package claimcheck_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/claimcheck"
	"github.com/iqhive/iqmq/x/outbox"
)

func store(tb testing.TB) *claimcheck.MemoryStore {
	tb.Helper()
	s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{Name: "bucket-1"})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

// pipeline returns the publish and consume halves over one store, which is
// the shape every case below uses.
func pipeline(tb testing.TB, cfg claimcheck.Config) (mq.PublishMiddleware, mq.Middleware) {
	tb.Helper()
	pm, err := claimcheck.PublishCheckIn(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	mw, err := claimcheck.CheckOut(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	return pm, mw
}

// publish runs one body through the publish half and returns what would have
// gone on the wire.
func publish(tb testing.TB, pm mq.PublishMiddleware, body []byte) ([]byte, error) {
	tb.Helper()
	var out []byte
	err := pm(func(_ context.Context, _ string, ms []*mq.Message, _ []mq.PublishOption) error {
		out = ms[0].Body
		return nil
	})(context.Background(), "t", []*mq.Message{{Body: body}}, nil)
	return out, err
}

// handle runs one wire body through the consume half and returns what the
// handler saw.
func handle(tb testing.TB, mw mq.Middleware, wire []byte) ([]byte, error) {
	tb.Helper()
	var seen []byte
	ran := false
	h := mw(mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		ran = true
		seen = bytes.Clone(d.Body)
		return nil
	}))
	err := h.Handle(context.Background(), &mq.Delivery{Message: mq.Message{Body: wire}})
	if err != nil && ran {
		tb.Fatal("the handler ran AND the middleware failed; a failed check-out must not reach the handler")
	}
	if err == nil && !ran {
		tb.Fatal("the middleware succeeded without running the handler")
	}
	return seen, err
}

func TestRoundTrip(t *testing.T) {
	s := store(t)
	cfg := claimcheck.Config{Store: s, Threshold: 16}
	pm, mw := pipeline(t, cfg)

	body := bytes.Repeat([]byte("payload "), 512)
	wire, err := publish(t, pm, body)
	if err != nil {
		t.Fatal(err)
	}
	if !claimcheck.IsRef(wire) {
		t.Fatal("the publication was not checked in")
	}
	if len(wire) >= len(body) {
		t.Errorf("the reference is %d bytes for a %d-byte payload", len(wire), len(body))
	}
	ref, err := claimcheck.ParseRef(wire)
	if err != nil {
		t.Fatal(err)
	}
	if string(ref.Store) != "bucket-1" || ref.Size != uint64(len(body)) {
		t.Errorf("ref = %+v", ref)
	}
	if want := sha256.Sum256(body); !bytes.Equal(ref.Sum, want[:]) {
		t.Error("the reference's digest is not the payload's")
	}
	if s.Len() != 1 {
		t.Errorf("the store holds %d payload(s), want 1", s.Len())
	}

	seen, err := handle(t, mw, wire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, body) {
		t.Fatalf("the handler saw %d bytes, want the original %d", len(seen), len(body))
	}
}

func TestSmallBodyIsNotCheckedIn(t *testing.T) {
	s := store(t)
	pm, mw := pipeline(t, claimcheck.Config{Store: s, Threshold: 1024})
	body := []byte("small enough to publish as it is")
	wire, err := publish(t, pm, body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, body) {
		t.Error("a body under the threshold was checked in")
	}
	if s.Len() != 0 {
		t.Errorf("the store holds %d payload(s) for an unchecked publication", s.Len())
	}
	// And the consume half passes it through, which is what makes a mixed
	// topic work.
	seen, err := handle(t, mw, wire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, body) {
		t.Error("an unchecked body did not reach the handler intact")
	}
}

// TestAnUnreachableStoreFailsThePublication is the first answer: fail
// closed. A reference to an object that was never uploaded is a message that
// can never be handled.
func TestAnUnreachableStoreFailsThePublication(t *testing.T) {
	s := store(t)
	pm, err := claimcheck.PublishCheckIn(claimcheck.Config{Store: s, Threshold: 16})
	if err != nil {
		t.Fatal(err)
	}
	s.SetUnavailable(true)

	reached := false
	perr := pm(func(context.Context, string, []*mq.Message, []mq.PublishOption) error {
		reached = true
		return nil
	})(context.Background(), "t", []*mq.Message{{Body: bytes.Repeat([]byte("x"), 64)}}, nil)

	if !errors.Is(perr, claimcheck.ErrStoreUnavailable) {
		t.Errorf("Publish = %v, want ErrStoreUnavailable", perr)
	}
	if reached {
		t.Error("the publication reached the broker although the payload was never stored")
	}
}

// TestAnUnreachableStoreRetriesTheDelivery is the second answer: a fetch
// that could not be made is retryable and the message is NOT acked.
//
// The assertion is on the CODE and not only on the sentinel, because the code
// is what the runner acts on: CodeUnavailable is retried, and anything that
// classified as poison would dead-letter a good message for a transient
// outage.
func TestAnUnreachableStoreRetriesTheDelivery(t *testing.T) {
	s := store(t)
	cfg := claimcheck.Config{Store: s, Threshold: 16}
	pm, mw := pipeline(t, cfg)
	wire, err := publish(t, pm, bytes.Repeat([]byte("x"), 64))
	if err != nil {
		t.Fatal(err)
	}

	s.SetUnavailable(true)
	_, herr := handle(t, mw, wire)
	if !errors.Is(herr, claimcheck.ErrStoreUnavailable) {
		t.Fatalf("Handle = %v, want ErrStoreUnavailable", herr)
	}
	if got := mq.CodeOf(herr); got != mq.CodeUnavailable {
		t.Errorf("code = %v, want CodeUnavailable so the runner RETRIES rather than dead-letters", got)
	}

	// And it succeeds once the store is back, which is what makes the retry
	// worth taking.
	s.SetUnavailable(false)
	if _, err := handle(t, mw, wire); err != nil {
		t.Errorf("the delivery still fails after the store recovered: %v", err)
	}
}

// TestAGonePayloadIsPoisonNotRetried is the third answer, and the sharp
// one: a store that ANSWERED and said there is no such object is permanent.
//
// Retrying it forever pins a lane on a message that will never succeed. The
// code must therefore be CodePoisonMessage, so the runner's failure policy
// dead-letters it (or Halts where no DLQ is configured, per ADR-011) rather
// than looping.
func TestAGonePayloadIsPoisonNotRetried(t *testing.T) {
	s := store(t)
	cfg := claimcheck.Config{Store: s, Threshold: 16}
	pm, mw := pipeline(t, cfg)
	wire, err := publish(t, pm, bytes.Repeat([]byte("x"), 64))
	if err != nil {
		t.Fatal(err)
	}

	s.SetGone(true)
	_, herr := handle(t, mw, wire)
	if !errors.Is(herr, claimcheck.ErrPayloadGone) {
		t.Fatalf("Handle = %v, want ErrPayloadGone", herr)
	}
	if got := mq.CodeOf(herr); got != mq.CodePoisonMessage {
		t.Errorf("code = %v, want CodePoisonMessage so the runner stops retrying", got)
	}
	// The two failures must be DISTINCT, which is the mistake a store
	// implementation is most likely to make.
	if errors.Is(herr, claimcheck.ErrStoreUnavailable) {
		t.Error("ErrPayloadGone also matches ErrStoreUnavailable; a permanent failure would be retried for ever")
	}
	if !errors.Is(claimcheck.ErrStoreUnavailable, claimcheck.ErrStoreUnavailable) {
		t.Error("the sentinel does not match itself")
	}
	if errors.Is(claimcheck.ErrStoreUnavailable, claimcheck.ErrPayloadGone) {
		t.Error("ErrStoreUnavailable also matches ErrPayloadGone; a transient failure would be dead-lettered")
	}
	// And the reference survives into the failure path, so which payload
	// went missing is diagnosable from the dead letter.
	ref, perr := claimcheck.ParseRef(wire)
	if perr != nil {
		t.Fatal(perr)
	}
	if !strings.Contains(ref.String(), "bucket-1/") {
		t.Errorf("Ref.String() = %q, which does not name the store and key", ref.String())
	}
}

// TestASettledClaimMayOutliveItsPayload is the documented consequence of the
// consumer never deleting, written down as a test so that anyone who adds a
// Delete call has to delete this too.
//
// A redelivery after the store's lifecycle policy has expired the payload is
// ErrPayloadGone. That is the cost of the pattern. The test that matters
// alongside it is the next one: a redelivery BEFORE the TTL must succeed, and
// it is delete-on-ack that would break it.
func TestASettledClaimMayOutliveItsPayload(t *testing.T) {
	s := store(t)
	cfg := claimcheck.Config{Store: s, Threshold: 16}
	pm, mw := pipeline(t, cfg)
	wire, err := publish(t, pm, bytes.Repeat([]byte("x"), 64))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle(t, mw, wire); err != nil {
		t.Fatalf("the first delivery failed: %v", err)
	}
	// A redelivery of the SAME message must still work: at-least-once is
	// the substrate's guarantee, and the store still holds the payload
	// because nothing here deleted it.
	if _, err := handle(t, mw, wire); err != nil {
		t.Fatalf("a redelivery failed, which is what delete-on-ack would cause: %v", err)
	}
	if s.Len() != 1 {
		t.Errorf("the store holds %d payload(s) after a successful delivery, want 1: something deleted it", s.Len())
	}
	// And after the lifecycle policy expires it, the redelivery is poison.
	s.SetGone(true)
	if _, err := handle(t, mw, wire); !errors.Is(err, claimcheck.ErrPayloadGone) {
		t.Errorf("a redelivery after the TTL = %v, want ErrPayloadGone", err)
	}
}

// TestStoreHasNoDeleteMethod pins the interface decision, because it is the
// kind of thing a later change adds without reading the documentation.
func TestStoreHasNoDeleteMethod(t *testing.T) {
	type deleter interface {
		Delete(ctx context.Context, key string) error
	}
	var s claimcheck.Store = store(t)
	if _, ok := s.(deleter); ok {
		t.Error("the store has a Delete method; the package documentation explains why the consumer must never delete a payload, and an interface with Delete on it is an invitation to call it after a successful handler")
	}
}

func TestDigestMismatchIsDetected(t *testing.T) {
	s := store(t)
	cfg := claimcheck.Config{Store: s, Threshold: 16}
	pm, mw := pipeline(t, cfg)
	body := bytes.Repeat([]byte("the real payload"), 8)
	wire, err := publish(t, pm, body)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := claimcheck.ParseRef(wire)
	if err != nil {
		t.Fatal(err)
	}
	// Somebody else wrote the bucket: same key, different bytes, same
	// length so that the size check cannot be what catches it.
	other := bytes.Repeat([]byte("a fake payload!!"), 8)
	if len(other) != len(body) {
		t.Fatalf("the fixture lengths differ (%d vs %d), so this would test the size check", len(other), len(body))
	}
	replaceObject(t, s, ref.KeyString(), other)

	if _, err := handle(t, mw, wire); !errors.Is(err, claimcheck.ErrDigestMismatch) {
		t.Errorf("Handle = %v, want ErrDigestMismatch: the store returned bytes the publisher never put there", err)
	}

	// Without a digest, the same substitution is undetectable. That is the
	// cost of DigestNone, stated as a test so the default cannot be changed
	// without noticing it.
	noDigest := claimcheck.WithoutDigest(claimcheck.Config{Store: s, Threshold: 16})
	_, mw2 := pipeline(t, noDigest)
	pm2, err := claimcheck.PublishCheckIn(noDigest)
	if err != nil {
		t.Fatal(err)
	}
	wire2, err := publish(t, pm2, body)
	if err != nil {
		t.Fatal(err)
	}
	ref2, err := claimcheck.ParseRef(wire2)
	if err != nil {
		t.Fatal(err)
	}
	if ref2.Digest != claimcheck.DigestNone || len(ref2.Sum) != 0 {
		t.Fatalf("WithoutDigest still recorded %v", ref2.Digest)
	}
	replaceObject(t, s, ref2.KeyString(), other)
	seen, err := handle(t, mw2, wire2)
	if err != nil {
		t.Fatalf("DigestNone: %v", err)
	}
	if !bytes.Equal(seen, other) {
		t.Fatal("the substituted payload did not reach the handler")
	}
	t.Log("with DigestNone a substituted payload reaches the handler undetected, which is why DigestSHA256 is the default and WithoutDigest has to be written out")
}

// TestSizeMismatchIsDetectedWithoutADigest: the size check is the one
// verification DigestNone still gets, so a truncated object is caught even
// there.
func TestSizeMismatchIsDetectedWithoutADigest(t *testing.T) {
	s := store(t)
	cfg := claimcheck.WithoutDigest(claimcheck.Config{Store: s, Threshold: 16})
	pm, mw := pipeline(t, cfg)
	body := bytes.Repeat([]byte("x"), 64)
	wire, err := publish(t, pm, body)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := claimcheck.ParseRef(wire)
	if err != nil {
		t.Fatal(err)
	}
	replaceObject(t, s, ref.KeyString(), body[:32])
	if _, err := handle(t, mw, wire); !errors.Is(err, claimcheck.ErrSizeMismatch) {
		t.Errorf("Handle = %v, want ErrSizeMismatch", err)
	}
}

// replaceObject overwrites a stored payload, which is what an attacker with
// write access to the bucket, or an operator restoring a backup, does.
//
// It goes through Put and a fresh store rather than reaching into the
// MemoryStore's map, because the map is unexported and a test that needed it
// would be asserting on an implementation detail. A small shim store is the
// honest way to say "the store returns different bytes for this key".
func replaceObject(tb testing.TB, s *claimcheck.MemoryStore, key string, payload []byte) {
	tb.Helper()
	if _, err := s.Get(context.Background(), key, 1<<20); err != nil {
		tb.Fatalf("the key %q is not in the store, so the substitution would prove nothing: %v", key, err)
	}
	s.Overwrite(key, payload)
}

// ---------------------------------------------------------------- refusals

func TestParseRefRefusals(t *testing.T) {
	good := func() []byte {
		b, err := claimcheck.AppendRef(nil, "bucket-1", "obj-1", 64, claimcheck.DigestNone, nil)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{"nil", nil, claimcheck.ErrNotRef},
		{"short", []byte("IQK"), claimcheck.ErrNotRef},
		{"header only", good()[:claimcheck.HeaderLen], claimcheck.ErrNotRef},
		{"wrong magic", append([]byte("IQK2"), good()[4:]...), claimcheck.ErrNotRef},
		{"reserved flag", func() []byte { b := good(); b[4] = 1; return b }(), claimcheck.ErrBadFlags},
		{"unknown digest", func() []byte { b := good(); b[5] = 9; return b }(), claimcheck.ErrUnknownDigest},
		{"store length past the end", func() []byte {
			b := good()
			binary.BigEndian.PutUint16(b[14:16], 0xFFFF)
			return b
		}(), claimcheck.ErrNotRef},
		{"key length past the end", func() []byte {
			b := good()
			binary.BigEndian.PutUint16(b[16:18], 0xFFFF)
			return b
		}(), claimcheck.ErrNotRef},
		{"trailing bytes", append(good(), 'x'), claimcheck.ErrNotRef},
		{"digest claimed but absent", func() []byte { b := good(); b[5] = byte(claimcheck.DigestSHA256); return b }(), claimcheck.ErrNotRef},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := claimcheck.ParseRef(c.body); !errors.Is(err, c.want) {
				t.Errorf("ParseRef = %v, want %v", err, c.want)
			}
		})
	}
}

func TestAppendRefRefusals(t *testing.T) {
	sum := make([]byte, sha256.Size)
	cases := map[string]struct {
		store, key string
		digest     claimcheck.Digest
		sum        []byte
	}{
		"empty store":   {"", "k", claimcheck.DigestNone, nil},
		"empty key":     {"s", "", claimcheck.DigestNone, nil},
		"long store":    {strings.Repeat("s", claimcheck.MaxStoreLen+1), "k", claimcheck.DigestNone, nil},
		"long key":      {"s", strings.Repeat("k", claimcheck.MaxKeyLen+1), claimcheck.DigestNone, nil},
		"sum with none": {"s", "k", claimcheck.DigestNone, sum},
		"none with sha": {"s", "k", claimcheck.DigestSHA256, nil},
		"short sum":     {"s", "k", claimcheck.DigestSHA256, sum[:16]},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := claimcheck.AppendRef(nil, c.store, c.key, 1, c.digest, c.sum); !errors.Is(err, claimcheck.ErrInvalidConfig) {
				t.Errorf("AppendRef = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestWrongStoreAndRequiredDigest(t *testing.T) {
	s := store(t)
	t.Run("wrong store", func(t *testing.T) {
		_, mw := pipeline(t, claimcheck.Config{Store: s, Threshold: 16})
		foreign, err := claimcheck.AppendRef(nil, "somebody-elses-bucket", "obj-1", 4, claimcheck.DigestNone, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := handle(t, mw, foreign); !errors.Is(err, claimcheck.ErrWrongStore) {
			t.Errorf("Handle = %v, want ErrWrongStore", err)
		}
	})
	t.Run("allowed foreign store", func(t *testing.T) {
		cfg := claimcheck.WithoutDigest(claimcheck.Config{Store: s, Threshold: 16, AllowForeignStore: true})
		pm, mw := pipeline(t, cfg)
		body := bytes.Repeat([]byte("y"), 64)
		wire, err := publish(t, pm, body)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := claimcheck.ParseRef(wire)
		if err != nil {
			t.Fatal(err)
		}
		// Rewrite the reference to name another store of the same length.
		foreign, err := claimcheck.AppendRef(nil, "bucket-9", ref.KeyString(), ref.Size, claimcheck.DigestNone, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := handle(t, mw, foreign); err != nil {
			t.Errorf("AllowForeignStore did not accept a foreign reference: %v", err)
		}
	})
	t.Run("required digest", func(t *testing.T) {
		cfg := claimcheck.Config{Store: s, Threshold: 16, RequireDigest: true}
		_, mw := pipeline(t, cfg)
		noDigest, err := claimcheck.AppendRef(nil, "bucket-1", "obj-1", 4, claimcheck.DigestNone, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := handle(t, mw, noDigest); !errors.Is(err, claimcheck.ErrDigestRequired) {
			t.Errorf("Handle = %v, want ErrDigestRequired", err)
		}
	})
}

// TestAnOversizedReferenceIsRefusedBeforeTheFetch: the cap, applied where
// it costs nothing. A reference claiming more than the limit must not produce
// a Get at all.
func TestAnOversizedReferenceIsRefusedBeforeTheFetch(t *testing.T) {
	s := &countingStore{MemoryStore: store(t)}
	cfg := claimcheck.WithoutDigest(claimcheck.Config{Store: s, Threshold: 16, MaxPayload: 1024})
	_, mw := pipeline(t, cfg)
	huge, err := claimcheck.AppendRef(nil, s.Name(), "obj-1", 1<<40, claimcheck.DigestNone, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle(t, mw, huge); !errors.Is(err, claimcheck.ErrTooLarge) {
		t.Fatalf("Handle = %v, want ErrTooLarge", err)
	}
	if s.gets != 0 {
		t.Errorf("the store saw %d Get(s) for a reference claiming 1 TiB; the recorded size must be checked first", s.gets)
	}
}

// TestTheRecordedSizeIsStillOnlyAClaim: a reference that UNDER-states its
// size must not let an oversized payload through, because the store is what
// decides how many bytes exist. The limit is passed to Get and the fetched
// payload is checked again.
func TestTheRecordedSizeIsStillOnlyAClaim(t *testing.T) {
	s := store(t)
	cfg := claimcheck.WithoutDigest(claimcheck.Config{Store: s, Threshold: 16, MaxPayload: 1024})
	_, mw := pipeline(t, cfg)
	// Park 4 KiB, then hand the consumer a reference claiming 16 bytes.
	key, err := s.Put(context.Background(), bytes.Repeat([]byte("z"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	liar, err := claimcheck.AppendRef(nil, s.Name(), key, 16, claimcheck.DigestNone, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, herr := handle(t, mw, liar)
	if herr == nil {
		t.Fatal("a reference under-stating its size produced a body the handler accepted")
	}
	if !errors.Is(herr, claimcheck.ErrTooLarge) && !errors.Is(herr, claimcheck.ErrSizeMismatch) {
		t.Errorf("Handle = %v, want ErrTooLarge (the fetch was bounded) or ErrSizeMismatch (the payload was checked)", herr)
	}
	t.Logf("an under-stating reference was refused with %v", herr)
}

func TestConfigRefusals(t *testing.T) {
	s := store(t)
	for name, cfg := range map[string]claimcheck.Config{
		"no store":            {},
		"negative threshold":  {Store: s, Threshold: -1},
		"negative MaxPayload": {Store: s, MaxPayload: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := claimcheck.PublishCheckIn(cfg); !errors.Is(err, claimcheck.ErrInvalidConfig) {
				t.Errorf("PublishCheckIn = %v, want ErrInvalidConfig", err)
			}
			if _, err := claimcheck.CheckOut(cfg); !errors.Is(err, claimcheck.ErrInvalidConfig) {
				t.Errorf("CheckOut = %v, want ErrInvalidConfig", err)
			}
		})
	}
	t.Run("unknown digest", func(t *testing.T) {
		cfg := claimcheck.Config{Store: s, Digest: 9}
		if _, err := claimcheck.CheckOut(cfg); !errors.Is(err, claimcheck.ErrUnknownDigest) {
			t.Errorf("CheckOut = %v, want ErrUnknownDigest", err)
		}
	})
	t.Run("the default digest is SHA-256", func(t *testing.T) {
		pm, err := claimcheck.PublishCheckIn(claimcheck.Config{Store: s, Threshold: 16})
		if err != nil {
			t.Fatal(err)
		}
		wire, err := publish(t, pm, bytes.Repeat([]byte("d"), 64))
		if err != nil {
			t.Fatal(err)
		}
		ref, err := claimcheck.ParseRef(wire)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Digest != claimcheck.DigestSHA256 {
			t.Errorf("the zero Config recorded digest %v, want DigestSHA256: DigestNone must not be reachable by forgetting a field", ref.Digest)
		}
	})
}

func TestMemoryStore(t *testing.T) {
	t.Run("negative MaxEntries", func(t *testing.T) {
		if _, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{MaxEntries: -1}); !errors.Is(err, claimcheck.ErrInvalidConfig) {
			t.Error("a negative MaxEntries was accepted")
		}
	})
	t.Run("bound is refused not evicted", func(t *testing.T) {
		s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{MaxEntries: 2})
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if _, err := s.Put(context.Background(), []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		_, err = s.Put(context.Background(), []byte("x"))
		if !errors.Is(err, claimcheck.ErrStoreUnavailable) {
			t.Errorf("the third Put = %v, want ErrStoreUnavailable: an eviction would turn a memory bound into poison messages", err)
		}
		if errors.Is(err, claimcheck.ErrTooLarge) || mq.CodeOf(err) != mq.CodeUnavailable {
			t.Errorf("the third Put = %v (code %v), want CodeUnavailable alone: a full store is not an oversized payload, and x/outbox makes a CodeMessageTooLarge row poison", err, mq.CodeOf(err))
		}
		if s.Len() != 2 {
			t.Errorf("the store holds %d, want 2", s.Len())
		}
	})
	t.Run("payloads are copied in and out", func(t *testing.T) {
		s := store(t)
		body := []byte("mutable")
		key, err := s.Put(context.Background(), body)
		if err != nil {
			t.Fatal(err)
		}
		body[0] = 'X' // the publisher reused its buffer
		got, err := s.Get(context.Background(), key, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "mutable" {
			t.Errorf("Get returned %q; Put aliased the caller's slice", got)
		}
		got[0] = 'Y'
		again, err := s.Get(context.Background(), key, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if string(again) != "mutable" {
			t.Errorf("Get returned %q; a previous Get's result aliased the store", again)
		}
	})
	t.Run("close is idempotent and closes the door", func(t *testing.T) {
		s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Put(context.Background(), []byte("x")); err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			if err := s.Close(); err != nil {
				t.Errorf("Close #%d = %v", i+1, err)
			}
		}
		if _, err := s.Put(context.Background(), []byte("x")); !errors.Is(err, claimcheck.ErrClosed) {
			t.Errorf("Put after Close = %v, want ErrClosed", err)
		}
		if _, err := s.Get(context.Background(), "obj-1", 1<<20); !errors.Is(err, claimcheck.ErrClosed) {
			t.Errorf("Get after Close = %v, want ErrClosed", err)
		}
	})
	t.Run("Get respects the max", func(t *testing.T) {
		s := store(t)
		key, err := s.Put(context.Background(), bytes.Repeat([]byte("x"), 100))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(context.Background(), key, 99); !errors.Is(err, claimcheck.ErrTooLarge) {
			t.Error("Get ignored its max")
		}
		if _, err := s.Get(context.Background(), key, 100); err != nil {
			t.Errorf("Get refused a payload exactly at the max: %v", err)
		}
	})
}

func TestErrorsCarryTheirCode(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want mq.ErrorCode
	}{
		{"ErrNotRef", claimcheck.ErrNotRef, mq.CodeCorruptMessage},
		{"ErrBadFlags", claimcheck.ErrBadFlags, mq.CodeCorruptMessage},
		{"ErrDigestMismatch", claimcheck.ErrDigestMismatch, mq.CodeCorruptMessage},
		{"ErrSizeMismatch", claimcheck.ErrSizeMismatch, mq.CodeCorruptMessage},
		{"ErrUnknownDigest", claimcheck.ErrUnknownDigest, mq.CodeUnsupported},
		{"ErrTooLarge", claimcheck.ErrTooLarge, mq.CodeMessageTooLarge},
		{"ErrPayloadGone", claimcheck.ErrPayloadGone, mq.CodePoisonMessage},
		{"ErrStoreUnavailable", claimcheck.ErrStoreUnavailable, mq.CodeUnavailable},
		{"ErrWrongStore", claimcheck.ErrWrongStore, mq.CodeInvalidConfig},
		{"ErrDigestRequired", claimcheck.ErrDigestRequired, mq.CodeInvalidConfig},
		{"ErrInvalidConfig", claimcheck.ErrInvalidConfig, mq.CodeInvalidConfig},
		{"ErrClosed", claimcheck.ErrClosed, mq.CodeClosed},
	} {
		if got := mq.CodeOf(c.err); got != c.want {
			t.Errorf("CodeOf(%s) = %v, want %v", c.name, got, c.want)
		}
		if !strings.Contains(c.err.Error(), "iqmq/x/claimcheck") {
			t.Errorf("%s does not name its package: %q", c.name, c.err.Error())
		}
	}
}

// countingStore counts the Gets that reach the store, so that "refused
// before the fetch" can be asserted rather than assumed.
type countingStore struct {
	*claimcheck.MemoryStore
	gets int
}

func (s *countingStore) Get(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	s.gets++
	return s.MemoryStore.Get(ctx, key, maxBytes)
}

// TestADefinitelyRefusedPublicationDiscardsItsPayload: a publication the
// broker definitely refused gives the message its body back and drops the
// payload, so a retry checks it in again and the failed attempt does not
// hold the store's bound until the TTL. An ambiguous one keeps both.
func TestADefinitelyRefusedPublicationDiscardsItsPayload(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 64)
	refused := errors.New("refused")
	for _, tc := range []struct {
		name    string
		err     error
		discard bool
	}{
		{name: "definite", err: refused, discard: true},
		{name: "ambiguous", err: &mq.OpError{Operation: "publish", Ambiguous: true, Err: refused}},
		{name: "cancelled", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := store(t)
			pm, err := claimcheck.PublishCheckIn(claimcheck.Config{Store: s, Threshold: 16})
			if err != nil {
				t.Fatal(err)
			}
			m := &mq.Message{Body: body}
			perr := pm(func(context.Context, string, []*mq.Message, []mq.PublishOption) error {
				return tc.err
			})(context.Background(), "t", []*mq.Message{m}, nil)
			if !errors.Is(perr, tc.err) {
				t.Fatalf("Publish = %v, want %v", perr, tc.err)
			}
			if tc.discard {
				if s.Len() != 0 || !bytes.Equal(m.Body, body) {
					t.Fatalf("after a definite refusal the store holds %d payload(s) and the body is a reference: %v; want 0 and the original body", s.Len(), claimcheck.IsRef(m.Body))
				}
				return
			}
			if s.Len() != 1 || !claimcheck.IsRef(m.Body) {
				t.Fatalf("after a %s failure the store holds %d payload(s), want 1 and the reference kept: the message may be in the broker", tc.name, s.Len())
			}
		})
	}
}

// TestAPartialBatchDiscardsOnlyTheRefusedPayloads: the accepted item's payload
// is referenced by a message in the broker and must stay.
func TestAPartialBatchDiscardsOnlyTheRefusedPayloads(t *testing.T) {
	s := store(t)
	pm, err := claimcheck.PublishCheckIn(claimcheck.Config{Store: s, Threshold: 16})
	if err != nil {
		t.Fatal(err)
	}
	accepted := &mq.Message{Body: bytes.Repeat([]byte("a"), 64)}
	refused := &mq.Message{Body: bytes.Repeat([]byte("r"), 64)}
	perr := pm(func(context.Context, string, []*mq.Message, []mq.PublishOption) error {
		return &mq.BatchError{Errs: []error{nil, errors.New("refused")}}
	})(context.Background(), "t", []*mq.Message{accepted, refused}, nil)
	if perr == nil {
		t.Fatal("Publish = nil, want the batch error")
	}
	if !claimcheck.IsRef(accepted.Body) || claimcheck.IsRef(refused.Body) {
		t.Fatalf("accepted is a reference: %v, refused is a reference: %v; want true and false", claimcheck.IsRef(accepted.Body), claimcheck.IsRef(refused.Body))
	}
	if s.Len() != 1 {
		t.Fatalf("the store holds %d payload(s), want 1: the accepted one", s.Len())
	}
}

// TestARefusedPublicationGivesTheFileStoreItsBytesBack is the reported
// defect: orphaned payloads counted against MaxBytes until the TTL.
func TestARefusedPublicationGivesTheFileStoreItsBytesBack(t *testing.T) {
	s, err := claimcheck.NewFileStore(claimcheck.FileOptions{Name: "files-1", Dir: t.TempDir(), MaxBytes: 100, SweepEvery: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	pm, err := claimcheck.PublishCheckIn(claimcheck.Config{Store: s, Threshold: 16})
	if err != nil {
		t.Fatal(err)
	}
	refuse := pm(func(context.Context, string, []*mq.Message, []mq.PublishOption) error {
		return errors.New("refused")
	})
	for i := range 3 {
		if err := refuse(context.Background(), "t", []*mq.Message{{Body: bytes.Repeat([]byte("x"), 64)}}, nil); err == nil || errors.Is(err, claimcheck.ErrTooLarge) {
			t.Fatalf("attempt %d = %v, want the broker's refusal: a refused attempt kept its bytes", i, err)
		}
	}
	if got := s.Bytes(); got != 0 {
		t.Fatalf("Bytes = %d after refused publications, want 0", got)
	}
}

// TestAFullStoreDoesNotPoisonOutboxRows is the reported defect end to end: a
// store with no room answered Put with ErrTooLarge, CodeMessageTooLarge, and
// x/outbox reads that code as the row's own fault, so a relay publishing
// through PublishCheckIn took perfectly good rows out of rotation as poison
// for as long as the store stayed full. The row is not at fault, so it must
// stay in rotation however many claims the full store costs it.
func TestAFullStoreDoesNotPoisonOutboxRows(t *testing.T) {
	ctx := context.Background()
	s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Put(ctx, []byte("the payload that fills the store")); err != nil {
		t.Fatal(err)
	}
	pm, err := claimcheck.PublishCheckIn(claimcheck.Config{Store: s, Threshold: 16})
	if err != nil {
		t.Fatal(err)
	}
	var published int
	publish := pm(func(context.Context, string, []*mq.Message, []mq.PublishOption) error {
		published++
		return nil
	})
	rows, err := outbox.NewMemoryStore(outbox.MemoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rows.Close() })
	id, err := rows.Append(outbox.Row{Topic: "t", Body: bytes.Repeat([]byte("x"), 64)})
	if err != nil {
		t.Fatal(err)
	}
	const maxAttempts = 2
	relay, err := outbox.New(outbox.Config{Store: rows, Publish: publish, MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	for range maxAttempts + 3 {
		if _, err := relay.Drain(ctx); err != nil {
			t.Fatalf("Drain: %v", err)
		}
	}
	if st := relay.Stats(); st.Poison != 0 {
		t.Fatalf("the relay made %d row(s) poison because the claim-check store was full: %+v", st.Poison, st)
	}
	if errors.Is(rows.FailureOf(id), outbox.ErrPoisonRow) || rows.Unsent() != 1 {
		t.Fatalf("the row left rotation (failure %v, %d unsent), want it still waiting for room", rows.FailureOf(id), rows.Unsent())
	}
	if published != 0 {
		t.Fatalf("the broker saw %d publication(s), want none: the publication fails closed while the payload cannot be stored", published)
	}
}
