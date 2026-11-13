package encrypt

import (
	"context"
	"crypto/rand"
	"errors"
	"sync/atomic"
)

// Config configures [Sealer], [Opener] and the two middlewares.
type Config struct {
	// Keys supplies the keys. Required.
	Keys Keyring
	// AdditionalData is authenticated but not encrypted, on top of the
	// envelope's own header. A reader that supplies different additional
	// data cannot open the message.
	//
	// It is empty by default, and binding it to the topic is a footgun even
	// though it would be stronger: a dead-lettered or forwarded message
	// arrives on a DIFFERENT topic, and a dead-letter consumer would then be
	// unable to open anything — including, especially, the messages it most
	// needs to read. A deployment that never forwards should set it.
	AdditionalData []byte
	// RequireSealed refuses an unsealed inbound body rather than passing it
	// through.
	//
	// Off by default so a topic can be migrated to encryption without
	// dropping the messages already in it. Switch it on when the migration
	// is done: a consumer that accepts unsealed bodies accepts anything a
	// publisher without the key can write, which makes the encryption an
	// integrity control that an attacker can opt out of. A nil body is
	// unsealed too, and is refused unless AllowNilBody is set.
	RequireSealed bool
	// AllowNilBody lets a delivery with a nil body through under
	// RequireSealed, UNAUTHENTICATED.
	//
	// It is off by default, and what it costs is the reason: anyone who can
	// publish to the topic can send a message with no body, and whatever key
	// and headers it carries, and every broker lets them, so with it on such
	// a message reaches the handler whether or not its publisher held a key.
	// It is for a compacted Kafka topic whose deletes are tombstones, which
	// [PublishEncrypt] leaves nil because an envelope would give the record a
	// value that log compaction keeps for ever. A body-less message that must
	// be authenticated is published with an empty, non-nil body instead,
	// which is sealed. Without RequireSealed every unsealed body is passed
	// through, a nil one included, and this field changes nothing.
	AllowNilBody bool
}

// refusesUnsealed reports whether [Decrypt] under cfg refuses an unsealed
// body: everything under RequireSealed, except a nil body AllowNilBody lets
// through.
func refusesUnsealed(cfg Config, body []byte) bool {
	return cfg.RequireSealed && (body != nil || !cfg.AllowNilBody)
}

func (c Config) validate() error {
	if c.Keys == nil {
		return ErrInvalidConfig
	}
	return nil
}

// Sealer seals bodies. It is safe for concurrent use.
type Sealer struct{ cfg Config }

// NewSealer returns a sealer, or [ErrInvalidConfig] for a nil keyring.
func NewSealer(cfg Config) (*Sealer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Sealer{cfg: cfg}, nil
}

// Seal returns the envelope for plain.
//
// The nonce is drawn from crypto/rand for every call, so two seals of one
// plaintext under one key are different envelopes — [TestTwoSealsDiffer]
// asserts it, because a deterministic nonce is the one implementation mistake
// here that breaks the construction outright rather than degrading it.
//
// A body that is already an envelope is [ErrAlreadySealed] rather than sealed
// again. Compression's equivalent case is a silent pass-through, and the
// difference is deliberate: an already-compressed body is harmless and an
// already-sealed one means two layers of key management have been configured
// by accident, which the caller wants to hear about.
func (s *Sealer) Seal(ctx context.Context, plain []byte) ([]byte, error) {
	if Sealed(plain) {
		return nil, ErrAlreadySealed
	}
	key, err := s.cfg.Keys.Current(ctx)
	if err != nil {
		return nil, err
	}
	if len(key.ID) > MaxKeyIDLen {
		return nil, ErrKeyIDTooLong
	}
	d, ok := suiteOf(key.Suite)
	if !ok {
		return nil, ErrUnknownSuite
	}
	if key.AEAD == nil || key.AEAD.NonceSize() != d.nonceSize || key.AEAD.Overhead() != d.overhead {
		// The keyring handed back an AEAD whose sizes are not the suite's,
		// so an envelope written with it could not be parsed back. Better a
		// configuration error than an unopenable message in the broker.
		return nil, ErrInvalidConfig
	}

	out := make([]byte, 0, FixedHeaderLen+len(key.ID)+d.nonceSize+len(plain)+d.overhead)
	out, aadEnd := appendHeader(out, key.Suite, []byte(key.ID))
	// The nonce is appended and then used in place, so it is written once
	// and read from the envelope it will travel in: a separate nonce buffer
	// is how the two can disagree.
	nonceStart := len(out)
	out = out[:nonceStart+d.nonceSize]
	if _, err := rand.Read(out[nonceStart:]); err != nil {
		return nil, err
	}
	aad := out[:aadEnd]
	if len(s.cfg.AdditionalData) > 0 {
		aad = append(append(make([]byte, 0, aadEnd+len(s.cfg.AdditionalData)), out[:aadEnd]...), s.cfg.AdditionalData...)
	}
	return key.AEAD.Seal(out, out[nonceStart:nonceStart+d.nonceSize], plain, aad), nil
}

// Opener opens envelopes. It is safe for concurrent use.
type Opener struct {
	cfg Config
	// lastID is the key id the previous Open converted to a string for
	// [Keyring.For]. An envelope's id aliases the delivery's bytes and the
	// interface takes a string, so the conversion escaped and allocated on
	// every message; reusing the last one makes it free for every message
	// between two rotations. It is a cache, not state:
	// a racing Store only costs the next Open one conversion.
	lastID atomic.Pointer[string]
}

// NewOpener returns an opener, or [ErrInvalidConfig] for a nil keyring.
func NewOpener(cfg Config) (*Opener, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Opener{cfg: cfg}, nil
}

// Open returns the plaintext of an envelope.
//
// The order is: parse the layout (no key needed, nothing allocated), look the
// key up by the suite and id the envelope NAMES, then open with the
// envelope's own header as additional data. The last step is what makes a
// tampered suite byte or key id fail rather than redirect: the id the lookup
// used is inside the data the tag covers, so an attacker who rewrites it
// cannot also produce a tag for the rewrite.
//
// The AEAD the keyring hands back is checked as [Sealer.Seal] checks its own:
// a nil one, or one whose nonce size or tag overhead is not the suite's, is
// [ErrInvalidConfig] rather than a panic inside the primitive. The plaintext
// of an envelope sealed from an empty body is an empty, non-nil slice, so a
// handler that reads nil as "no body" never sees one nobody sent.
func (o *Opener) Open(ctx context.Context, sealed []byte) ([]byte, error) {
	e, err := ParseEnvelope(sealed)
	if err != nil {
		return nil, err
	}
	aead, err := o.cfg.Keys.For(ctx, e.Suite, o.keyID(e.KeyID))
	if err != nil {
		return nil, err
	}
	if d, ok := suiteOf(e.Suite); !ok || aead == nil || aead.NonceSize() != d.nonceSize || aead.Overhead() != d.overhead {
		return nil, ErrInvalidConfig
	}
	aad := e.AAD
	if len(o.cfg.AdditionalData) > 0 {
		aad = append(append(make([]byte, 0, len(e.AAD)+len(o.cfg.AdditionalData)), e.AAD...), o.cfg.AdditionalData...)
	}
	plain, err := aead.Open(nil, e.Nonce, e.Ciphertext, aad)
	if err != nil {
		// cipher.AEAD's own error says only "message authentication
		// failed". ErrNotAuthentic carries CodeCorruptMessage so the runner
		// and a driver classify it identically, and the cause is joined for
		// a caller that wants it. The five distinguishable causes are not
		// distinguished: see ErrNotAuthentic.
		return nil, errors.Join(ErrNotAuthentic, err)
	}
	if plain == nil {
		// cipher.AEAD.Open with a nil destination returns nil for an empty
		// plaintext, and nil is "no body".
		plain = []byte{}
	}
	return plain, nil
}

// keyID returns id as a string, reusing the previous Open's conversion when
// the bytes are the same. The comparison does not allocate; only a change of
// key does.
func (o *Opener) keyID(id []byte) string {
	if len(id) == 0 {
		return ""
	}
	if last := o.lastID.Load(); last != nil && *last == string(id) {
		return *last
	}
	s := string(id)
	o.lastID.Store(&s)
	return s
}
