package claimcheck

import (
	"context"
	"crypto/sha256"
	"errors"

	mq "bitbucket.org/iqhive/iqmq"
)

// Defaults.
const (
	// DefaultThreshold is the body size at or above which a publication is
	// checked into the store.
	//
	// 64 KiB is below every shipped driver's limit with room to spare, which
	// is the property that matters: a threshold above a broker's own maximum
	// would let a message be refused by the driver before this middleware
	// had a chance to park it, which is the one configuration that makes the
	// package useless.
	DefaultThreshold = 64 << 10
	// DefaultMaxPayload bounds what a consumer will fetch for one message.
	// the cap, applied to a body that arrives from a store rather than
	// from a broker.
	DefaultMaxPayload = 64 << 20
)

// Config configures the two middlewares.
type Config struct {
	// Store holds the payloads. Required.
	Store Store
	// Threshold is the smallest body that gets checked in. Zero takes
	// [DefaultThreshold]; negative is refused.
	//
	// A body under it is published as it is, so a topic carries a mixture of
	// checked and unchecked messages and the consumer tells them apart by
	// the reference's magic. That is deliberate: checking in a 200-byte
	// message costs two round trips to save nothing.
	Threshold int
	// MaxPayload bounds a fetched payload. Zero takes
	// [DefaultMaxPayload]; negative is refused. There is no unlimited.
	MaxPayload int64
	// Digest is the algorithm recorded in each reference. The zero value is
	// [DigestSHA256]; [DigestNone] must be named explicitly.
	Digest Digest
	// RequireDigest refuses an inbound reference that carries no digest.
	//
	// Off by default only so that a deployment which deliberately chose
	// [DigestNone] can read its own messages. Anything else should set it:
	// without a digest the payload is an unauthenticated body from an
	// external system, and a consumer that accepts one accepts whatever is
	// at that key now.
	RequireDigest bool
	// AllowForeignStore accepts a reference naming a store other than
	// [Store.Name].
	//
	// Off by default, which refuses with [ErrWrongStore]. On is for the
	// migration between two buckets, where the consumer reads the new one
	// and must still fetch from references written against the old -- and it
	// is honest about what it then does: it asks its own store for the key
	// regardless of which store the reference named, so it is only correct
	// while both names address the same objects.
	AllowForeignStore bool

	// digestNamed distinguishes "the caller did not set Digest" from "the
	// caller asked for DigestNone", so that the zero value of an exported
	// field can mean the SAFE default rather than the absent one. It is
	// unexported and set only by [WithoutDigest], which is what makes
	// DigestNone unreachable by forgetting a field. The alternative -- a
	// *Digest, or a second exported bool -- moves the awkwardness into every
	// caller's code, and this package's default has to be the safe one.
	digestNamed bool
}

func (c Config) resolve() (Config, error) {
	if c.Store == nil || c.Threshold < 0 || c.MaxPayload < 0 {
		return Config{}, ErrInvalidConfig
	}
	if c.Threshold == 0 {
		c.Threshold = DefaultThreshold
	}
	if c.MaxPayload == 0 {
		c.MaxPayload = DefaultMaxPayload
	}
	if c.Digest == DigestNone && !c.digestNamed {
		c.Digest = DigestSHA256
	}
	if c.Digest != DigestNone && c.Digest != DigestSHA256 {
		return Config{}, ErrUnknownDigest
	}
	if c.Store.Name() == "" || len(c.Store.Name()) > MaxStoreLen {
		return Config{}, ErrInvalidConfig
	}
	return c, nil
}

// WithoutDigest returns cfg with [DigestNone] selected deliberately.
//
// It exists so that DigestNone cannot be reached by forgetting to set a
// field. A caller who writes it has said, in the source, that the payload
// store is trusted to return the bytes that were put there.
func WithoutDigest(cfg Config) Config {
	cfg.Digest = DigestNone
	cfg.digestNamed = true
	return cfg
}

// PublishCheckIn parks a large outbound body in the store and replaces it
// with a reference.
//
// A body under [Config.Threshold] is left alone. A body that is already a
// reference is left alone: it has been checked in by something else, and
// checking it in again would park a reference and leave the payload
// unreachable.
//
// A store that cannot be reached FAILS THE PUBLICATION. Nothing is published,
// which is the asymmetry the package documentation explains: a reference to
// an object that was never uploaded is a message that can never be handled.
func PublishCheckIn(cfg Config) (mq.PublishMiddleware, error) {
	c, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message) error {
			for _, m := range ms {
				if m == nil || len(m.Body) < c.Threshold || IsRef(m.Body) {
					continue
				}
				ref, err := checkIn(ctx, c, m.Body)
				if err != nil {
					return err
				}
				m.Body = ref
			}
			return next(ctx, topic, ms)
		}
	}, nil
}

func checkIn(ctx context.Context, c Config, body []byte) ([]byte, error) {
	var sum []byte
	if c.Digest == DigestSHA256 {
		h := sha256.Sum256(body)
		sum = h[:]
	}
	key, err := c.Store.Put(ctx, body)
	if err != nil {
		return nil, err
	}
	return AppendRef(nil, c.Store.Name(), key, uint64(len(body)), c.Digest, sum)
}

// CheckOut fetches a checked-in payload and puts it back in the delivery
//
// A body that is not a reference is passed through: a topic carries both, by
// construction, because a body under the threshold is published as it is.
//
// The order of the checks is the contract:
//
//  1. parse the reference, which allocates nothing;
//  2. refuse a reference whose store is not this consumer's
//     ([ErrWrongStore]) or which carries no digest when one is required
//     ([ErrDigestRequired]) — both before any I/O;
//  3. compare the RECORDED size with the limit and refuse
//     [ErrTooLarge] — before the fetch, so an oversized payload costs no
//     transfer;
//  4. fetch, bounded again by the limit, because the recorded size is a
//     claim;
//  5. verify the fetched payload's length and digest against the reference.
//
// Step 5 is what makes the payload something other than unauthenticated
// remote input, and step 3 is the cap applied where it is cheapest.
//
// A payload the store reports GONE is returned as [mq.DeadLetter] of
// [ErrPayloadGone]: the handler does not run, and the runner applies its
// failure policy on the first attempt -- the message is dead-lettered with the
// reference as its body when a dead-letter topic is configured, and the runner
// halts with a *[mq.PoisonError] when none is -- instead of spending its
// retries and their backoff on an answer that cannot change. errors.Is still
// finds ErrPayloadGone and [mq.CodeOf] still reports CodePoisonMessage.
// Every other failure is returned as it is, so the
// runner's configured backoff and bound decide.
func CheckOut(cfg Config) (mq.Middleware, error) {
	c, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if !IsRef(d.Body) {
				return next.Handle(ctx, d)
			}
			body, err := checkOut(ctx, c, d.Body)
			if err != nil {
				if errors.Is(err, ErrPayloadGone) {
					// The store answered and the object does not exist.
					// No retry can change that, so the runner is told to
					// apply its failure policy NOW -- dead-letter when a
					// topic is configured, halt otherwise -- rather than
					// after MaxAttempts backoffs that hold the lane for
					// nothing.
					return mq.DeadLetter(err)
				}
				return err
			}
			d.Body = body
			return next.Handle(ctx, d)
		})
	}, nil
}

func checkOut(ctx context.Context, c Config, b []byte) ([]byte, error) {
	ref, err := ParseRef(b)
	if err != nil {
		return nil, err
	}
	if !c.AllowForeignStore && string(ref.Store) != c.Store.Name() {
		return nil, ErrWrongStore
	}
	if c.RequireDigest && ref.Digest == DigestNone {
		return nil, ErrDigestRequired
	}
	if ref.Size > uint64(c.MaxPayload) {
		return nil, ErrTooLarge
	}
	payload, err := c.Store.Get(ctx, ref.KeyString(), c.MaxPayload)
	if err != nil {
		return nil, err
	}
	if err := ref.verify(payload); err != nil {
		return nil, err
	}
	return payload, nil
}
