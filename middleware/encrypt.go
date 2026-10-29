package middleware

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"

	mq "github.com/iqhive/iqmq"
)

// HeaderKeyID names the key a sealed body was sealed under.
//
// It is deliberately not in the reserved iqmq- namespace: the specification reserves that
// prefix for the core's own headers and Headers.Validate refuses it to
// callers, and this middleware is a caller.
const HeaderKeyID = "encryption-key-id"

// KeyProvider supplies the AEAD a message is sealed with or opened under
// (the specification's "external key provider").
//
// Implementations must be safe for concurrent use. Returning a provider
// rather than a key is what makes rotation possible: Current names the key
// new messages are sealed with, and For still answers for the keys older
// messages were sealed with.
type KeyProvider interface {
	// Current returns the key to seal with and the id to record on the
	// message.
	Current(ctx context.Context) (aead cipher.AEAD, id string, err error)
	// For returns the key with the given id, which is what the message
	// recorded. It returns ErrNoKey for an id it does not have.
	For(ctx context.Context, id string) (cipher.AEAD, error)
}

// StaticKey is a KeyProvider holding one key. It is what a program with no
// rotation needs, and what the tests use.
//
// The zero value is not usable; NewStaticKey returns a ready one. It is safe
// for concurrent use.
type StaticKey struct {
	aead cipher.AEAD
	id   string
}

// NewStaticKey returns a provider for one AES-GCM key. The key must be 16 or
// 32 bytes (AES-128 or AES-256); id is recorded on every message it seals and
// may be empty.
func NewStaticKey(key []byte, id string) (*StaticKey, error) {
	a, err := NewAESGCM(key)
	if err != nil {
		return nil, err
	}
	return &StaticKey{aead: a, id: id}, nil
}

// Current implements KeyProvider.
func (s *StaticKey) Current(context.Context) (cipher.AEAD, string, error) {
	return s.aead, s.id, nil
}

// For implements KeyProvider. It answers for its own id and for the empty id,
// which is what a message sealed before any id was configured carries, and
// refuses anything else with ErrNoKey.
func (s *StaticKey) For(_ context.Context, id string) (cipher.AEAD, error) {
	if id == s.id || id == "" {
		return s.aead, nil
	}
	return nil, ErrNoKey
}

// NewAESGCM returns an AES-GCM AEAD for key, which must be 16 or 32 bytes.
//
// AES-GCM is the choice because it is in the standard library, is an AEAD —
// so a tampered ciphertext fails to open rather than decrypting to rubbish —
// and needs no third-party dependency, which the root module does not have
// (ADR-002). The specification's "no custom cryptography" is why there is nothing else
// here.
func NewAESGCM(key []byte) (cipher.AEAD, error) {
	switch len(key) {
	case 16, 32:
	default:
		return nil, ErrInvalidConfig
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// EncryptConfig configures Encrypt and PublishEncrypt.
type EncryptConfig struct {
	// Keys supplies the AEAD. A nil provider makes both middlewares
	// pass-through, so that encryption can be switched on by configuration
	// without changing the chain.
	Keys KeyProvider
	// AdditionalData is authenticated but not encrypted: a receiver that
	// supplies different additional data cannot open the message.
	//
	// It is empty by default, and it is a footgun to bind it to the topic
	// even though that would be stronger: a dead-lettered or forwarded
	// message arrives on a DIFFERENT topic, and a consumer of the
	// dead-letter queue would then be unable to open anything. A caller who
	// does not forward messages should set it.
	AdditionalData []byte
	// RequireSealed refuses a delivery whose body is not sealed, rather than
	// passing it through. It is off by default so that a topic can be
	// migrated to encryption without dropping the messages already in it,
	// and it should be switched on once the migration is done: a consumer
	// that accepts unsealed messages accepts anything a publisher without
	// the key can write.
	RequireSealed bool
}

// Encrypt opens a sealed inbound body (AEAD).
//
// A body is sealed when the message carries the encryption-key-id header. Its
// layout is nonce || ciphertext || tag, with the nonce the AEAD's own size,
// so the whole thing is self-describing given the key.
//
// # What AEAD buys and what nonce reuse would cost
//
// Opening either returns the plaintext or fails. A body that was modified in
// any way — one bit of ciphertext, one bit of the tag, a truncation, a
// different key — fails with ErrNotAuthentic and never reaches the handler.
// That is the whole reason the specification says AEAD rather than "encryption".
//
// The failure mode of GCM is nonce reuse: two messages sealed with the same
// key and the same nonce leak the exclusive-or of their plaintexts and, worse,
// let an attacker forge tags. PublishEncrypt therefore draws every nonce from
// crypto/rand and never from a counter, which is what makes it safe across
// processes and restarts that share a key. Two seals of the same plaintext
// are different bodies, and the test asserts it.
//
// The opened body, and a header set without the key id, replace the
// delivery's own for the rest of the chain; neither is changed in place. The
// runner keeps the sealed message it received, so a message the handler fails
// is dead-lettered sealed and still carrying its key id, and a strictly
// ordered retry opens it again.
//
// A nil Keys returns the handler unchanged.
func Encrypt(cfg EncryptConfig) mq.Middleware {
	if cfg.Keys == nil {
		return func(next mq.Handler) mq.Handler { return next }
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			id, sealed := d.Headers.Get(HeaderKeyID)
			if !sealed {
				if cfg.RequireSealed {
					return ErrNotSealed
				}
				return next.Handle(ctx, d)
			}
			aead, err := cfg.Keys.For(ctx, string(id))
			if err != nil {
				return err
			}
			plain, err := open(aead, d.Body, cfg.AdditionalData)
			if err != nil {
				return err
			}
			// Both are replaced, never changed in place: the runner
			// dead-letters and retries the sealed message it received.
			d.Body = plain
			d.Headers = headersWithout(&d.Headers, HeaderKeyID)
			return next.Handle(ctx, d)
		})
	}
}

// PublishEncrypt seals an outbound body (AEAD).
//
// Every message gets a fresh random nonce, and every message is sealed: a
// key-id header the caller set is not proof that the body is sealed, so it is
// replaced rather than trusted.
//
// The caller's messages are not sealed: the sealed copies, with their own
// headers, go to the next publish function, so a message published again is
// sealed again rather than mistaken for one this middleware already sealed.
// What the publisher stamps on a copy comes back to the caller's message,
// as it would without this middleware: an ID (PublisherConfig.AutoID, ID) and
// a Properties.CreatedAt (PublisherConfig.Timestamp, Timestamp), each only
// where the caller's message had none. A caller that publishes again after
// an ambiguous failure then sends the same ID, which is what a deduplicating
// consumer keys on.
//
// A nil Keys returns the publish function unchanged.
func PublishEncrypt(cfg EncryptConfig) mq.PublishMiddleware {
	if cfg.Keys == nil {
		return func(next mq.PublishFunc) mq.PublishFunc { return next }
	}
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message, opts []mq.PublishOption) error {
			aead, id, err := cfg.Keys.Current(ctx)
			if err != nil {
				return err
			}
			sealed := make([]*mq.Message, len(ms))
			for i, m := range ms {
				if m == nil {
					continue
				}
				body, err := seal(aead, m.Body, cfg.AdditionalData)
				if err != nil {
					return err
				}
				c := *m
				c.Body = body
				c.Headers = m.Headers.Clone()
				c.Headers.Set(HeaderKeyID, []byte(id))
				sealed[i] = &c
			}
			err = next(ctx, topic, sealed, opts)
			// The stamps are written before any I/O, so they come back
			// whatever the outcome: a failed publication is the one whose
			// ID the caller needs for the retry.
			stampBack(ms, sealed)
			return err
		}
	}
}

// stampBack gives each of the caller's messages the ID and CreatedAt the
// publish chain stamped on its sealed copy, where the caller left them empty.
func stampBack(ms, sealed []*mq.Message) {
	for i, m := range ms {
		c := sealed[i]
		if c == nil {
			continue
		}
		if m.ID == "" {
			m.ID = c.ID
		}
		if m.Properties.CreatedAt.IsZero() {
			m.Properties.CreatedAt = c.Properties.CreatedAt
		}
	}
}

// seal returns nonce || aead.Seal(plaintext).
func seal(aead cipher.AEAD, plain, aad []byte) ([]byte, error) {
	n := aead.NonceSize()
	out := make([]byte, n, n+len(plain)+aead.Overhead())
	if _, err := rand.Read(out[:n]); err != nil {
		return nil, err
	}
	return aead.Seal(out, out[:n], plain, aad), nil
}

// open reverses seal.
func open(aead cipher.AEAD, sealed, aad []byte) ([]byte, error) {
	n := aead.NonceSize()
	if len(sealed) < n+aead.Overhead() {
		return nil, ErrNotSealed
	}
	plain, err := aead.Open(nil, sealed[:n], sealed[n:], aad)
	if err != nil {
		// cipher.AEAD's own error says only "message authentication
		// failed"; ErrNotAuthentic carries CodeCorruptMessage so that the
		// runner and a driver classify it the same way, and the cause is
		// kept for a caller that wants it.
		return nil, errors.Join(ErrNotAuthentic, err)
	}
	return plain, nil
}
