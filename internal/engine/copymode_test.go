package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/payload"
)

// canaryByte is the pattern the use-after-free canary writes over a released
// zero-copy payload. It is not 0 and not the pool's own poison, so a test can
// tell "scrubbed by ZeroOnRelease", "recycled by the pool" and "poisoned by
// the receive canary" apart.
const canaryByte = 0xDE

// scrubbed reports whether the engine scrubbed b as it released it: zeroed by
// ZeroOnRelease, or filled with the canary, which an iqmq_debug build writes
// instead for every aliasing receive mode because it is the stronger of the
// two scrubs (ReceiveMode.poisons).
func scrubbed(b []byte) bool {
	want := byte(0)
	if debugCanary {
		want = canaryByte
	}
	for _, v := range b {
		if v != want {
			return false
		}
	}
	return true
}

// bodyEngine builds an engine over byte-slice payloads with the copy hooks
// wired: Clone copies the body, Zero scrubs it and Poison writes the canary.
func bodyEngine(t *testing.T, cfg Config[[]byte]) *Engine[[]byte] {
	t.Helper()
	cfg.Clone = func(b []byte) []byte {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	cfg.Zero = func(b []byte) { clear(b) }
	cfg.Poison = func(b []byte) {
		for i := range b {
			b[i] = canaryByte
		}
	}
	if cfg.DefaultTopology.Partitions == 0 {
		cfg.DefaultTopology.Partitions = 1
	}
	if cfg.DefaultTopology.CapacityMessages == 0 {
		cfg.DefaultTopology.CapacityMessages = 16
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// bodyRig builds a topic, a subscription and a consumer over byte bodies.
func bodyRig(t *testing.T, e *Engine[[]byte]) (*Topic[[]byte], *Subscription[[]byte], *Consumer[[]byte]) {
	t.Helper()
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return top, sub, c
}

// TestCopyModeSafe: a safe publication copies the body, hands the caller's own
// back at once, and is unaffected by the caller mutating it afterwards.
func TestCopyModeSafe(t *testing.T) {
	e := bodyEngine(t, Config[[]byte]{})
	top, _, c := bodyRig(t, e)
	body := []byte{1, 2, 3, 4}
	released := 0
	if err := top.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: len(body), Mode: CopySafe,
		Release: func([]byte) { released++ },
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if released != 1 {
		t.Fatalf("a safe publication released %d times before delivery, want 1", released)
	}
	// The caller owns its buffer again the instant Publish returns.
	for i := range body {
		body[i] = 0xFF
	}
	dst := make([]Claim[[]byte], 1)
	n, err := c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("fetched %d records", n)
	}
	if !bytes.Equal(dst[0].Payload, []byte{1, 2, 3, 4}) {
		t.Fatalf("the delivery aliases the caller's buffer: %v", dst[0].Payload)
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("a safe publication released %d times, want exactly 1", released)
	}
	// A copying publication with no Clone is refused rather than silently
	// sharing the caller's buffer.
	plain, err := New(Config[[]byte]{DefaultTopology: TopologyConfig{Partitions: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = plain.Close() }()
	pt, err := plain.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pt.Subscribe("s", SubscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := pt.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: len(body), Mode: CopySafe,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a safe publication with no Clone = %v, want ErrInvalidConfig", err)
	}
}

// TestCopyModeOwnedReleasesOnce: an owned payload is released exactly once,
// after the last subscription has settled it, and ZeroOnRelease scrubs it.
func TestCopyModeOwnedReleasesOnce(t *testing.T) {
	e := bodyEngine(t, Config[[]byte]{})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	subs := make([]*Subscription[[]byte], 2)
	cons := make([]*Consumer[[]byte], 2)
	for i := range subs {
		s, err := top.Subscribe(string(rune('a'+i)), SubscriptionConfig{})
		if err != nil {
			t.Fatal(err)
		}
		c, err := s.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		subs[i], cons[i] = s, c
	}
	body := []byte{1, 2, 3, 4}
	released := 0
	if err := top.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: len(body), Mode: CopyOwned,
		Release: func([]byte) { released++ },
	}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[[]byte], 1)
	for i, c := range cons {
		n, err := c.Fetch(poll(t), dst)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("consumer %d fetched %d records", i, n)
		}
		if released != 0 {
			t.Fatalf("the payload was released while subscription %d still held it", i)
		}
		if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatal(err)
		}
	}
	if released != 1 {
		t.Fatalf("an owned payload was released %d times, want 1", released)
	}
	// ZeroOnRelease scrubbed the caller's buffer: the storage was the
	// engine's to scrub.
	if !scrubbed(body) {
		t.Fatalf("ZeroOnRelease left %v behind", body)
	}
}

// TestCopyModeBorrowedIsNotScrubbed: a borrowed payload belongs to the
// caller, so the engine notifies completion and scrubs nothing.
func TestCopyModeBorrowedIsNotScrubbed(t *testing.T) {
	e := bodyEngine(t, Config[[]byte]{})
	top, _, c := bodyRig(t, e)
	body := []byte{1, 2, 3, 4}
	done := 0
	if err := top.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: len(body), Mode: CopyBorrowed,
		Release: func([]byte) { done++ },
	}); err != nil {
		t.Fatal(err)
	}
	if done != 0 {
		t.Fatal("a borrow completed before the record was settled")
	}
	dst := make([]Claim[[]byte], 1)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if done != 1 {
		t.Fatalf("a borrow completed %d times, want 1", done)
	}
	if !bytes.Equal(body, []byte{1, 2, 3, 4}) {
		t.Fatalf("the engine scrubbed a borrowed payload: %v", body)
	}
}

// TestReceiveZeroCopyCanary: under ReceiveZeroCopy a settled delivery is no
// longer alive and its payload carries the canary, which is the
// use-after-free diagnosis of the memory engine.
func TestReceiveZeroCopyCanary(t *testing.T) {
	e := bodyEngine(t, Config[[]byte]{Receive: ReceiveZeroCopy})
	top, _, c := bodyRig(t, e)
	body := []byte{1, 2, 3, 4}
	if err := top.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: len(body), Mode: CopyOwned, Release: func([]byte) {},
	}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[[]byte], 1)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cl := dst[0]
	if !cl.Alive() {
		t.Fatal("a live delivery reports itself dead")
	}
	if !bytes.Equal(cl.Payload, []byte{1, 2, 3, 4}) {
		t.Fatalf("the zero-copy delivery does not alias the published body: %v", cl.Payload)
	}
	if err := c.Ack(cl.Env, cl.Gen); err != nil {
		t.Fatal(err)
	}
	if cl.Alive() {
		t.Fatal("a settled delivery still reports itself alive")
	}
	for _, b := range cl.Payload {
		if b != canaryByte {
			t.Fatalf("the released payload is %v, want the canary", cl.Payload)
		}
	}
}

// TestReceiveCopyIsIndependent: under ReceiveCopy the consumer's payload
// survives the settlement, because it is its own.
func TestReceiveCopyIsIndependent(t *testing.T) {
	e := bodyEngine(t, Config[[]byte]{Receive: ReceiveCopy})
	top, _, c := bodyRig(t, e)
	if err := top.Publish(context.Background(), Publish[[]byte]{
		Payload: []byte{1, 2, 3, 4}, Size: 4, Mode: CopyOwned, Release: func([]byte) {},
	}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[[]byte], 1)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cl := dst[0]
	if err := c.Ack(cl.Env, cl.Gen); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cl.Payload, []byte{1, 2, 3, 4}) {
		t.Fatalf("a copied delivery was scrubbed by the settlement: %v", cl.Payload)
	}
	// A copying receive with no Clone is refused.
	if _, err := New(Config[[]byte]{Receive: ReceiveCopy}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ReceiveCopy without a Clone = %v, want ErrInvalidConfig", err)
	}
}

// TestCopyModeStrings pins the benchmark dimension values and the parsers.
func TestCopyModeStrings(t *testing.T) {
	for _, m := range CopyModes() {
		got, ok := ParseCopyMode(m.String())
		if !ok || got != m {
			t.Fatalf("ParseCopyMode(%q) = %v, %v", m.String(), got, ok)
		}
		if !m.Valid() || m.Ident() == "CopyMode(?)" {
			t.Fatalf("copy mode %v has no identifier", m)
		}
	}
	for _, m := range ReceiveModes() {
		got, ok := ParseReceiveMode(m.String())
		if !ok || got != m {
			t.Fatalf("ParseReceiveMode(%q) = %v, %v", m.String(), got, ok)
		}
		if !m.Valid() || m.Ident() == "ReceiveMode(?)" {
			t.Fatalf("receive mode %v has no identifier", m)
		}
	}
	if CopyMode(9).Valid() || CopyMode(9).String() != "copymode(?)" {
		t.Fatal("an undefined copy mode is valid")
	}
	if ReceiveMode(9).Valid() || ReceiveMode(9).String() != "receivemode(?)" {
		t.Fatal("an undefined receive mode is valid")
	}
	for _, m := range IdleModes() {
		if !m.Valid() || m.Ident() == "IdleMode(?)" || m.String() == "idle(?)" {
			t.Fatalf("idle mode %v has no name", m)
		}
	}
	if IdleMode(9).Valid() {
		t.Fatal("an undefined idle mode is valid")
	}
	for _, k := range StartPositions() {
		if !k.Valid() || k.String() == "start(?)" {
			t.Fatalf("start position %v has no name", k)
		}
	}
}

// TestCopyModeDefault: an engine's Config.CopyMode is what a publication that
// names none is admitted under, and the Safe kind set publishes safely.
func TestCopyModeDefault(t *testing.T) {
	safe := bodyEngine(t, Config[[]byte]{Kinds: KindSet{Safe: true}})
	top, _, c := bodyRig(t, safe)
	body := []byte{7, 7}
	released := 0
	if err := top.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: 2, Release: func([]byte) { released++ },
	}); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("the Safe kind set did not publish safely: released %d", released)
	}
	dst := make([]Claim[[]byte], 1)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if &dst[0].Payload[0] == &body[0] {
		t.Fatal("the Safe kind set delivered the caller's own buffer")
	}
	// An engine configured for borrowed publication uses it by default.
	borrow := bodyEngine(t, Config[[]byte]{CopyMode: CopyBorrowed})
	bt, _, bc := bodyRig(t, borrow)
	own := []byte{9, 9}
	done := 0
	if err := bt.Publish(context.Background(), Publish[[]byte]{
		Payload: own, Size: 2, Release: func([]byte) { done++ },
	}); err != nil {
		t.Fatal(err)
	}
	if done != 0 {
		t.Fatal("a borrowed default completed at publication")
	}
	if _, err := bc.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := bc.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if done != 1 || own[0] != 9 {
		t.Fatalf("borrowed default: completed %d times, payload %v", done, own)
	}
}

// TestZeroOnReleaseScrubsPooledPayload: ZeroOnRelease reaches every payload
// the engine owns, including the clones a safe publication left behind.
func TestZeroOnReleaseScrubsPooledPayload(t *testing.T) {
	e := bodyEngine(t, Config[[]byte]{})
	top, _, c := bodyRig(t, e)
	body := []byte{5, 5, 5, 5}
	if err := top.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: 4, Mode: CopySafe,
	}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[[]byte], 1)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	clone := dst[0].Payload
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if !scrubbed(clone) {
		t.Fatalf("the engine's own clone was not scrubbed: %v", clone)
	}
	if !bytes.Equal(body, []byte{5, 5, 5, 5}) {
		t.Fatalf("a safe publication scrubbed the caller's buffer: %v", body)
	}
}

// TestPayloadEngineStillBuilds keeps the payload representation the adapter
// uses compiling against the copy-mode surface.
func TestPayloadEngineStillBuilds(t *testing.T) {
	e, err := New(Config[*payload.Payload]{
		Clone:           func(p *payload.Payload) *payload.Payload { return p.Clone() },
		DefaultTopology: TopologyConfig{Partitions: 1},
		CopyMode:        CopySafe,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}
