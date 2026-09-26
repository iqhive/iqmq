package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// The ownership fuzz targets of the memory engine and plan. Their
// oracle is the ownership contract itself rather than internal/model/memq:
// what is being checked is who owns a payload and when, which the model
// deliberately abstracts away — it tracks a release ledger, not the bytes.
//
// Each target drives a fuzz-chosen op script over an engine of byte-slice
// payloads and holds it to the contract after every step:
//
//   - FuzzEngineOwnedRelease: a payload the engine took is released exactly
//     once and a payload it refused is never released by it.
//   - FuzzEngineAliasing: a safe publication is never aliased by a delivery
//     and never shared between two subscriptions; an owned or borrowed one is
//     aliased rather than needlessly copied.
//   - FuzzEngineZeroCopy: a zero-copy delivery is alive and readable until it
//     is settled, and dead and poisoned afterwards.

// copyBody is the body length every ownership script publishes.
const copyBody = 8

// copySubs and copyConsumers bound the shape of an ownership script.
const (
	copySubs      = 2
	copyConsumers = 2
	copyHandles   = 8
)

// pubState is one publication an ownership script made.
type pubState struct {
	// body is the buffer the caller published and, for a safe publication,
	// still owns.
	body []byte
	// want is the bytes the engine must deliver: the body as it was at the
	// instant of publication.
	want []byte
	// mode is the ownership contract it was published under.
	mode CopyMode
	// taken reports that the engine admitted the publication and therefore
	// owes exactly one release; refused reports that it did not and owes
	// none.
	taken   bool
	refused bool
	// released counts the release callbacks the engine issued.
	released int
}

// copyHandle is a delivery an ownership script is holding.
type copyHandle struct {
	claim Claim[[]byte]
	cons  int
	pub   int
	// settled reports that the script has settled the delivery, after which
	// the payload of a zero-copy receive is no longer the script's to read.
	settled bool
}

// copyHarness drives one ownership script.
type copyHarness struct {
	t    testing.TB
	eng  *Engine[[]byte]
	top  *Topic[[]byte]
	subs []*Subscription[[]byte]
	cons []*Consumer[[]byte]
	pubs []*pubState
	held []copyHandle
	dst  []Claim[[]byte]
	poll context.Context
	mode CopyMode
	recv ReceiveMode
	// fanout reports that the topic has more than one subscription, which
	// makes a record outlive the first settlement of it.
	fanout bool
}

// newCopyHarness builds an engine wired with the copy hooks and the requested
// contract.
func newCopyHarness(t testing.TB, mode CopyMode, recv ReceiveMode, subs int) *copyHarness {
	t.Helper()
	eng, err := New(Config[[]byte]{
		CopyMode: mode,
		Receive:  recv,
		Clone: func(b []byte) []byte {
			out := make([]byte, len(b))
			copy(out, b)
			return out
		},
		Poison: func(b []byte) {
			for i := range b {
				b[i] = canaryByte
			}
		},
		DefaultTopology: TopologyConfig{
			Partitions: 1, CapacityMessages: 8, AckDeadline: 8 * tick,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	top, err := eng.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	h := &copyHarness{
		t: t, eng: eng, top: top, mode: mode, recv: recv,
		dst: make([]Claim[[]byte], 4), fanout: subs > 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.poll = ctx
	for i := range subs {
		s, err := top.Subscribe(string(rune('a'+i)), SubscriptionConfig{})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		h.subs = append(h.subs, s)
		for range copyConsumers {
			c, err := s.Attach(ConsumerConfig{})
			if err != nil {
				t.Fatalf("Attach: %v", err)
			}
			h.cons = append(h.cons, c)
		}
	}
	return h
}

// close shuts the engine down, which releases everything it still holds.
func (h *copyHarness) close() { _ = h.eng.Close() }

// publish makes one publication with a fuzz-chosen body.
func (h *copyHarness) publish(fill byte) {
	h.t.Helper()
	// The first two bytes are the publication index, so that no two
	// publications ever carry the same content: the aliasing checks identify
	// a delivery by what it holds, and two identical bodies would make them
	// compare a delivery with the wrong caller's buffer.
	n := len(h.pubs)
	body := make([]byte, copyBody)
	body[0], body[1] = byte(n), byte(n>>8)
	for i := 2; i < len(body); i++ {
		body[i] = fill + byte(i)
	}
	want := make([]byte, len(body))
	copy(want, body)
	p := &pubState{body: body, want: want, mode: h.mode}
	h.pubs = append(h.pubs, p)
	err := h.top.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: len(body), Mode: h.mode,
		Release: func(b []byte) {
			p.released++
			if p.released > 1 {
				h.t.Fatalf("publication %d released %d times", len(h.pubs)-1, p.released)
			}
			if h.mode == CopyBorrowed && !bytes.Equal(b, p.want) {
				h.t.Fatalf("the engine scrubbed a borrowed payload: %v, want %v", b, p.want)
			}
		},
	})
	switch {
	case err == nil, errors.Is(err, errcode.ErrPartial):
		p.taken = true
	default:
		p.refused = true
		if p.released != 0 {
			h.t.Fatalf("a refused publication released %d payloads", p.released)
		}
	}
	if p.taken && h.mode.Copies() && p.released != 1 {
		// A safe publication hands the caller's own payload straight back:
		// the engine kept a copy, so there is nothing left of the caller's.
		h.t.Fatalf("a safe publication released %d times before delivery", p.released)
	}
}

// mutate scribbles over the buffer of a publication the caller still owns,
// which is only the safe ones: an owned or borrowed payload belongs to the
// engine until it is released, and writing to it would be the caller's bug.
func (h *copyHarness) mutate(i int) {
	h.t.Helper()
	if len(h.pubs) == 0 || !h.mode.Copies() {
		return
	}
	p := h.pubs[i%len(h.pubs)]
	for j := range p.body {
		p.body[j] = 0xEE
	}
}

// findPub returns the publication a delivered payload belongs to, by the
// index its first two bytes carry.
func (h *copyHarness) findPub(b []byte) int {
	if len(b) < 2 {
		return -1
	}
	i := int(b[0]) | int(b[1])<<8
	if i >= len(h.pubs) || !bytes.Equal(b, h.pubs[i].want) {
		return -1
	}
	return i
}

// fetch claims records for one consumer and checks the payload contract of
// every delivery it produced.
func (h *copyHarness) fetch(ci, max int) {
	h.t.Helper()
	if len(h.cons) == 0 {
		return
	}
	ci %= len(h.cons)
	if max < 1 {
		max = 1
	}
	if max > len(h.dst) {
		max = len(h.dst)
	}
	n, err := h.cons[ci].Fetch(h.poll, h.dst[:max])
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errcode.ErrClosed) {
		h.t.Fatalf("Fetch: %v", err)
	}
	for i := range n {
		cl := h.dst[i]
		pi := h.findPub(cl.Payload)
		if pi < 0 {
			h.t.Fatalf("delivered %v, which no publication produced", cl.Payload)
		}
		p := h.pubs[pi]
		if !cl.Alive() {
			h.t.Fatalf("a fresh delivery of publication %d reports itself dead", pi)
		}
		h.checkAlias(pi, p, cl)
		h.hold(copyHandle{claim: cl, cons: ci, pub: pi})
	}
}

// checkAlias holds the delivery to the aliasing half of the copy contract: a
// safe publication is delivered as a copy the caller cannot reach, and an
// owned or borrowed one is delivered by reference, because copying it would
// be work the mode exists to avoid.
func (h *copyHarness) checkAlias(pi int, p *pubState, cl Claim[[]byte]) {
	h.t.Helper()
	if h.recv == ReceiveCopy {
		if aliases(cl.Payload, p.body) {
			h.t.Fatalf("a copying receive delivered publication %d by reference", pi)
		}
		return
	}
	if h.mode.Copies() {
		if aliases(cl.Payload, p.body) {
			h.t.Fatalf("a safe publication %d was delivered by reference", pi)
		}
		return
	}
	if !aliases(cl.Payload, p.body) {
		h.t.Fatalf("an owned publication %d was copied on delivery", pi)
	}
}

// aliases reports whether two slices share their backing array.
func aliases(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return unsafe.SliceData(a) == unsafe.SliceData(b)
}

// hold records a delivery, evicting the oldest when the table is full.
func (h *copyHarness) hold(hd copyHandle) {
	if len(h.held) < copyHandles {
		h.held = append(h.held, hd)
		return
	}
	copy(h.held, h.held[1:])
	h.held[len(h.held)-1] = hd
}

// settle acks or nacks a held delivery and checks what the settlement did to
// the payload.
func (h *copyHarness) settle(idx int, ack bool) {
	h.t.Helper()
	if len(h.held) == 0 {
		return
	}
	i := idx % len(h.held)
	hd := &h.held[i]
	if hd.settled {
		return
	}
	c := h.cons[hd.cons]
	var err error
	if ack {
		err = c.Ack(hd.claim.Env, hd.claim.Gen)
	} else {
		err = c.Nack(hd.claim.Env, hd.claim.Gen, 0)
	}
	if err != nil {
		// A lease the engine reclaimed first is not a contract violation:
		// the settlement simply lost the race, which the sentinels name.
		return
	}
	hd.settled = true
	if hd.claim.Alive() {
		h.t.Fatalf("a settled delivery of publication %d still reports itself alive", hd.pub)
	}
	if ack {
		// Only a terminal settlement releases the record. A negative one
		// hands it back to its lane, where the payload is still live for
		// the next delivery, so the canary must not have been written.
		h.checkAfterSettle(hd)
	}
}

// checkAfterSettle holds an acknowledged zero-copy delivery to the
// use-after-free half of the contract: once the record is released the
// payload carries the canary, so a consumer that reads it sees the diagnosis
// rather than plausible data. A record another subscription is still holding
// is not released yet and must not be poisoned, which is why the fan-out runs
// check only what they can know.
func (h *copyHarness) checkAfterSettle(hd *copyHandle) {
	h.t.Helper()
	if h.recv != ReceiveZeroCopy || h.fanout {
		return
	}
	p := h.pubs[hd.pub]
	if p.mode.Copies() {
		// The engine delivered its own clone; the canary went over that.
		if !poisoned(hd.claim.Payload) {
			h.t.Fatalf("a settled zero-copy delivery of publication %d was not poisoned: %v",
				hd.pub, hd.claim.Payload)
		}
		return
	}
	if p.mode == CopyBorrowed {
		// A borrowed payload is the caller's storage: the engine never
		// scrubs it, whatever the receive mode is.
		if !bytes.Equal(p.body, p.want) {
			h.t.Fatalf("the engine poisoned a borrowed payload: %v", p.body)
		}
		return
	}
	if !poisoned(p.body) {
		h.t.Fatalf("a settled zero-copy delivery of owned publication %d was not poisoned: %v",
			hd.pub, p.body)
	}
}

// poisoned reports whether every byte of b carries the canary.
func poisoned(b []byte) bool {
	for _, v := range b {
		if v != canaryByte {
			return false
		}
	}
	return len(b) > 0
}

// advance moves the bubble clock, which expires leases and lets the engine
// reclaim what a script never settled.
func (h *copyHarness) advance(d time.Duration) {
	if d <= 0 {
		return
	}
	time.Sleep(d)
	synctest.Wait()
}

// check holds the whole run to the release contract.
func (h *copyHarness) check() {
	h.t.Helper()
	for i, p := range h.pubs {
		if p.released > 1 {
			h.t.Fatalf("publication %d released %d times", i, p.released)
		}
		if p.refused && p.released != 0 {
			h.t.Fatalf("refused publication %d was released by the engine", i)
		}
	}
}

// checkClosed holds the run to the contract once the engine has closed: every
// payload the engine took has been handed back exactly once, and nothing it
// refused has.
func (h *copyHarness) checkClosed() {
	h.t.Helper()
	h.check()
	for i, p := range h.pubs {
		if p.taken && p.released != 1 {
			h.t.Fatalf("publication %d was taken by the engine and released %d times", i, p.released)
		}
	}
}

// copyScript is the op script the ownership targets share.
func copyScript() *fuzzutil.Script[*copyHarness] {
	return &fuzzutil.Script[*copyHarness]{
		MaxSteps: 256,
		Steps: []fuzzutil.Step[*copyHarness]{
			{Name: "publish", Weight: 6, Run: func(_ testing.TB, r *fuzzutil.Reader, h *copyHarness) {
				h.publish(r.Byte())
			}},
			{Name: "fetch", Weight: 5, Run: func(_ testing.TB, r *fuzzutil.Reader, h *copyHarness) {
				h.fetch(r.Choice(copySubs*copyConsumers), r.Int(1, 4))
			}},
			{Name: "ack", Weight: 5, Run: func(_ testing.TB, r *fuzzutil.Reader, h *copyHarness) {
				h.settle(r.Choice(copyHandles), true)
			}},
			{Name: "nack", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *copyHarness) {
				h.settle(r.Choice(copyHandles), false)
			}},
			{Name: "mutate", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *copyHarness) {
				h.mutate(r.Choice(copyHandles))
			}},
			{Name: "advance", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *copyHarness) {
				h.advance(time.Duration(r.Int(1, 12)) * tick)
			}},
		},
		Check: func(_ testing.TB, h *copyHarness) { h.check() },
	}
}

// copySeeds authors readable ownership scripts.
func copySeeds(sc *fuzzutil.Script[*copyHarness]) [][]byte {
	seeds := make([][]byte, 0, 3)
	// Publish, deliver, settle.
	w := fuzzutil.NewWriter()
	writeCopyConfig(w, 0, 0, 1)
	sc.WriteStep(w, "publish")
	w.Byte(1)
	sc.WriteStep(w, "fetch")
	w.Choice(copySubs*copyConsumers, 0)
	w.Int(1, 4, 2)
	sc.WriteStep(w, "ack")
	w.Choice(copyHandles, 0)
	seeds = append(seeds, w.Data())
	// Publish, mutate the caller's buffer, then deliver: a safe publication
	// must be unaffected.
	w = fuzzutil.NewWriter()
	writeCopyConfig(w, 1, 0, 1)
	sc.WriteStep(w, "publish")
	w.Byte(2)
	sc.WriteStep(w, "mutate")
	w.Choice(copyHandles, 0)
	sc.WriteStep(w, "fetch")
	w.Choice(copySubs*copyConsumers, 0)
	w.Int(1, 4, 1)
	seeds = append(seeds, w.Data())
	// Publish and let the lease expire rather than settling it.
	w = fuzzutil.NewWriter()
	writeCopyConfig(w, 2, 2, 2)
	sc.WriteStep(w, "publish")
	w.Byte(3)
	sc.WriteStep(w, "fetch")
	w.Choice(copySubs*copyConsumers, 0)
	w.Int(1, 4, 1)
	sc.WriteStep(w, "advance")
	w.Int(1, 12, 12)
	seeds = append(seeds, w.Data())
	return seeds
}

// writeCopyConfig mirrors copyConfigFrom.
func writeCopyConfig(w *fuzzutil.Writer, mode, recv, subs int) {
	w.Choice(len(CopyModes()), mode)
	w.Choice(len(ReceiveModes()), recv)
	w.Int(1, copySubs, subs)
}

// copyConfigFrom decodes the ownership shape from the head of a script.
func copyConfigFrom(r *fuzzutil.Reader) (CopyMode, ReceiveMode, int) {
	mode := CopyModes()[r.Choice(len(CopyModes()))]
	recv := ReceiveModes()[r.Choice(len(ReceiveModes()))]
	return mode, recv, r.Int(1, copySubs)
}

// runCopyScript drives one ownership script inside a synctest bubble.
func runCopyScript(t *testing.T, sc *fuzzutil.Script[*copyHarness], data []byte,
	force func(*CopyMode, *ReceiveMode),
) {
	t.Helper()
	r := fuzzutil.NewReader(data)
	mode, recv, subs := copyConfigFrom(r)
	if force != nil {
		force(&mode, &recv)
	}
	rest := data[len(data)-r.Remaining():]
	t.Logf("ownership mode=%v receive=%v subs=%d", mode, recv, subs)
	synctest.Test(t, func(t *testing.T) {
		h := newCopyHarness(t, mode, recv, subs)
		// The engine is closed even when a check fails, so that a failing
		// script reports the failure rather than the bubble deadlock its
		// abandoned scheduler goroutine would otherwise produce.
		defer h.close()
		sc.Run(t, rest, h)
		h.check()
		h.close()
		h.checkClosed()
	})
}

// FuzzEngineOwnedRelease holds the engine to the release half of the
// ownership contract over every copy mode: a payload the engine took is
// released exactly once — after the last subscription settles it, after a
// lease expiry that drops it, or when the engine closes — and a payload it
// refused is never released by it, because that publication is the one the
// caller may retry.
func FuzzEngineOwnedRelease(f *testing.F) {
	sc := copyScript()
	for _, s := range copySeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runCopyScript(t, sc, data, nil)
	})
}

// FuzzEngineAliasing holds the engine to the aliasing half: a safe
// publication is never delivered by reference and never affected by the
// caller mutating its buffer afterwards, while an owned or borrowed one is
// delivered by reference rather than needlessly copied. The receive mode is
// held at ReceiveShared, where aliasing is the engine's own choice rather
// than the mode's.
func FuzzEngineAliasing(f *testing.F) {
	sc := copyScript()
	for _, s := range copySeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runCopyScript(t, sc, data, func(_ *CopyMode, recv *ReceiveMode) {
			*recv = ReceiveShared
		})
	})
}

// FuzzEngineZeroCopy holds the engine to the use-after-free canary: a
// zero-copy delivery is alive and carries the published bytes until it is
// settled, and dead and poisoned afterwards — except for a borrowed payload,
// which is the caller's storage and is never scrubbed.
func FuzzEngineZeroCopy(f *testing.F) {
	sc := copyScript()
	for _, s := range copySeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runCopyScript(t, sc, data, func(_ *CopyMode, recv *ReceiveMode) {
			*recv = ReceiveZeroCopy
		})
	})
}
