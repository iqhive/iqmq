package inbox_test

import (
	"context"
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/x/inbox"
)

// FuzzOnceIsOnce is a differential fuzz of the store against the simplest
// possible model of it: a set of keys and a rule.
//
// The op script is the fuzzer's byte string: each byte selects a key from a
// small alphabet and decides whether that delivery's handler succeeds. The
// model is four lines; the store is the thing under test. The invariant is the
// package's entire promise -- a key whose handler has EVER succeeded never
// runs the handler again, and a key whose handler has only failed always does.
//
// A small key alphabet is deliberate: the interesting inputs are the ones that
// revisit a key after a failure, and a fuzzer given 256 distinct keys would
// almost never do that.
func FuzzOnceIsOnce(f *testing.F) {
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{0x80, 0x00, 0x81, 0x01})
	f.Add([]byte{0x80, 0x80, 0x80, 0x00, 0x00})
	f.Add([]byte("abcabc"))
	f.Add([]byte{0x40, 0x40, 0x00, 0x00}) // panic, panic, then commit and a duplicate

	f.Fuzz(func(t *testing.T, script []byte) {
		s, err := inbox.NewMemoryStore(inbox.MemoryOptions{MaxEntries: 16})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		ctx := context.Background()
		fail := errors.New("the handler failed")

		// The model: committed[key] is true once a handler has succeeded.
		committed := map[string]bool{}
		for _, op := range script {
			key := string(rune('a' + int(op&0x07)))
			handlerFails := op&0x80 != 0
			// A panic is a third way for a handler to end, and the model
			// treats it as a failure: nothing committed, so nothing is
			// recorded.
			handlerPanics := op&0x40 != 0 && !handlerFails

			ran := false
			panicked := false
			var gotRan bool
			var gotErr error
			func() {
				defer func() {
					if recover() != nil {
						panicked = true
					}
				}()
				gotRan, gotErr = s.Once(ctx, key, func(context.Context) error {
					ran = true
					if handlerPanics {
						panic("the handler panicked")
					}
					if handlerFails {
						return fail
					}
					return nil
				})
			}()
			if panicked {
				if !handlerPanics {
					t.Fatalf("key %q: Once panicked although the handler did not", key)
				}
				if committed[key] {
					t.Fatalf("key %q: the handler ran (and panicked) for a key whose handler had already succeeded", key)
				}
				if s.Has(key) {
					t.Fatalf("key %q: recorded although the handler panicked; the redelivery would be acked without the work being done", key)
				}
				continue
			}
			if handlerPanics && ran {
				t.Fatalf("key %q: the handler ran and was due to panic, but Once returned (%v, %v)", key, gotRan, gotErr)
			}
			if errors.Is(gotErr, inbox.ErrFull) {
				// The bound was reached. Refusing is the documented
				// behaviour and the model says nothing about it.
				if ran || gotRan {
					t.Fatalf("key %q: Once reported ErrFull but ran=%v gotRan=%v", key, ran, gotRan)
				}
				continue
			}

			switch {
			case committed[key]:
				// The model says duplicate: fn must not run.
				if ran {
					t.Fatalf("key %q: the handler ran again for a key whose handler had already succeeded", key)
				}
				if gotRan || gotErr != nil {
					t.Fatalf("key %q: Once = (%v, %v), want (false, nil) for a duplicate", key, gotRan, gotErr)
				}
			case handlerFails:
				// The model says run and roll back.
				if !ran {
					t.Fatalf("key %q: the handler did not run for a key that has never succeeded", key)
				}
				if gotRan {
					t.Fatalf("key %q: Once reported that fn ran and committed, but fn failed", key)
				}
				if !errors.Is(gotErr, fail) {
					t.Fatalf("key %q: Once = %v, want the handler's error", key, gotErr)
				}
				if s.Has(key) {
					t.Fatalf("key %q: recorded although the handler failed; the redelivery would be acked without the work being done", key)
				}
			default:
				// The model says run and commit.
				if !ran || !gotRan || gotErr != nil {
					t.Fatalf("key %q: Once = (%v, %v) with ran=%v, want a committed run", key, gotRan, gotErr, ran)
				}
				if !s.Has(key) {
					t.Fatalf("key %q: not recorded although the handler succeeded", key)
				}
				committed[key] = true
			}
		}
		// And the store's final contents are exactly the model's.
		for key, want := range committed {
			if s.Has(key) != want {
				t.Fatalf("key %q: store Has=%v, model says %v", key, s.Has(key), want)
			}
		}
		if s.Len() != len(committed) {
			t.Fatalf("the store holds %d key(s), the model %d", s.Len(), len(committed))
		}
	})
}

// FuzzMiddlewareNeverAcksUnhandledWork is the middleware's half of the same
// property, stated as the one thing it must never do: return nil for a
// delivery whose handler has never succeeded.
//
// Returning nil is an ACK. An ack for work nobody did is the only way this
// package can lose a message, and it is reachable through every policy
// combination, so the fuzzer chooses those too.
func FuzzMiddlewareNeverAcksUnhandledWork(f *testing.F) {
	f.Add([]byte{0, 0}, uint8(0), uint8(0))
	f.Add([]byte{0x80, 0x00}, uint8(1), uint8(1))
	f.Add([]byte{0x40, 0x40}, uint8(0), uint8(1))

	f.Fuzz(func(t *testing.T, script []byte, unavail, missing uint8) {
		s, err := inbox.NewMemoryStore(inbox.MemoryOptions{MaxEntries: 16})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		cfg := inbox.Config{
			Store:         s,
			Key:           inbox.KeyFromHeader(keyHeader),
			OnUnavailable: inbox.Unavailable(int(unavail) % 2),
			OnMissingKey:  inbox.MissingKey(int(missing) % 2),
		}
		m, err := inbox.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		fail := errors.New("the handler failed")
		var current struct {
			fails bool
		}
		succeeded := map[string]bool{}
		h := m.Wrap()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			if current.fails {
				return fail
			}
			return nil
		}))

		for _, op := range script {
			key := string(rune('a' + int(op&0x07)))
			current.fails = op&0x80 != 0
			keyed := op&0x40 == 0

			d := delivery("")
			if keyed {
				d = delivery(key)
			}
			err := h.Handle(context.Background(), d)
			if err == nil && !current.fails {
				if keyed {
					succeeded[key] = true
				}
				continue
			}
			if err != nil {
				continue
			}
			// err == nil AND the handler was set to fail. The only way
			// that is correct is a duplicate: the handler was not called
			// because this key's work already succeeded.
			if !keyed || !succeeded[key] {
				t.Fatalf("key %q (keyed=%v): the middleware acked a delivery whose handler fails and which is not a duplicate of any success", key, keyed)
			}
		}
		// Counts are internally consistent: everything is accounted for.
		c := m.Counts()
		total := c.Processed + c.Duplicate + c.Failed + c.NoKey + c.Unavailable
		if c.NoKey > 0 && cfg.OnMissingKey == inbox.MissingKeyProcess {
			// An unkeyed delivery under MissingKeyProcess is counted in
			// NoKey and then runs the handler, so it is not double
			// counted -- the handler's outcome is not recorded anywhere.
			// Nothing to assert beyond the count being reachable.
			_ = total
		}
		if total < uint64(len(script)) && len(script) > 0 {
			t.Fatalf("Counts = %+v account for %d of %d deliveries", c, total, len(script))
		}
	})
}
