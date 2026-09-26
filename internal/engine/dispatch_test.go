package engine

import (
	"strings"
	"sync"
	"testing"

	"github.com/iqhive/iqmq/internal/queue"
)

// TestDispatchKindsList announces — how the engine calls its
// lane's ready queue, which points
// at it.
//
// The family is deliberately not part of TestKindsList: that list enumerates
// the kinds an *engine* is built with, one per family, selected at
// construction. A dispatch mechanism is selected at compile time by a build
// tag instead, so there is nothing in KindSet to sweep and no auto.go to
// generate; the bake-off produces a report and ADR-017 records whether the
// default moves, exactly as it does for the per-publication copy modes of.
func TestDispatchKindsList(t *testing.T) {
	parts := make([]string, 0, len(Dispatches()))
	for _, d := range Dispatches() {
		parts = append(parts, d.String()+"="+d.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestDispatchProperties(t *testing.T) {
	if len(Dispatches()) != int(dispatchCount) {
		t.Fatalf("Dispatches() = %v", Dispatches())
	}
	seen := map[string]bool{}
	for _, d := range Dispatches() {
		if !d.Valid() || d.Ident() == "" || d.String() == "" {
			t.Fatalf("%v: basic properties", d)
		}
		if seen[d.String()] {
			t.Fatalf("%v: duplicate slug", d)
		}
		seen[d.String()] = true
	}
	if got := dispatchCount.String(); got != "dispatch(?)" {
		t.Fatalf("out-of-range dispatch named %q", got)
	}
	if got := dispatchCount.Ident(); got != "" {
		t.Fatalf("out-of-range dispatch has ident %q", got)
	}
	if !LaneDispatch().Valid() {
		t.Fatalf("this build reports dispatch %v", LaneDispatch())
	}
	// The build tag and the mechanism it selects must agree, so that a build
	// cannot silently report a binding it does not have.
	switch LaneDispatch() {
	case DispatchInterface:
		if LaneDispatchTag() != "" {
			t.Fatalf("the interface build reports tag %q", LaneDispatchTag())
		}
	case DispatchConcrete:
		if LaneDispatchTag() != "iqmq_queue_mpmc" {
			t.Fatalf("the concrete build reports tag %q", LaneDispatchTag())
		}
	case DispatchNative:
		if LaneDispatchTag() != "iqmq_queue_native" {
			t.Fatalf("the native build reports tag %q", LaneDispatchTag())
		}
	case dispatchCount:
		t.Fatalf("this build reports dispatch %d", LaneDispatch())
	}
	t.Logf("lane dispatch: %s (tag %q)", LaneDispatch(), LaneDispatchTag())
}

// TestReadyQueueBindsEveryKind checks that whatever this build is bound to,
// every kind the engine may be configured with still works
// through the lane's ready set. A tag that narrowed the configuration rather
// than the fast path would make the differential tests below it meaningless.
func TestReadyQueueBindsEveryKind(t *testing.T) {
	for _, k := range QueueKinds() {
		t.Run(k.String(), func(t *testing.T) {
			r := newReadyQueue[int](k, 8)
			if r.Len() != 0 {
				t.Fatalf("a fresh queue holds %d", r.Len())
			}
			if _, ok := r.TryPop(); ok {
				t.Fatal("an empty queue popped a value")
			}
			for i := range 8 {
				if !r.TryPush(i) {
					t.Fatalf("push %d was refused with room for 8", i)
				}
			}
			if got := r.Len(); got != 8 {
				t.Fatalf("Len = %d after 8 pushes", got)
			}
			got := make([]bool, 8)
			for range 8 {
				v, ok := r.TryPop()
				if !ok {
					t.Fatal("pop found nothing after 8 pushes")
				}
				if got[v] {
					t.Fatalf("value %d popped twice", v)
				}
				got[v] = true
			}
			if _, ok := r.TryPop(); ok {
				t.Fatal("a drained queue popped a value")
			}
		})
	}
}

// TestNativeRingMatchesTheKindItMirrors is the differential that holds the
// open-coded ring to the kind it duplicates: the same script of
// pushes and pops must produce the same answers from queue.MPMC and from
// nativeRing, in the same order, with the same capacity.
func TestNativeRingMatchesTheKindItMirrors(t *testing.T) {
	for _, capacity := range []int{1, 2, 8, 13} {
		ref := queue.NewMPMC[int](capacity)
		got := newNativeRing[int](capacity)
		if ref.Cap() != got.Cap() {
			t.Fatalf("capacity %d: reference holds %d, native holds %d",
				capacity, ref.Cap(), got.Cap())
		}
		x := uint64(capacity)*6364136223846793005 + 1
		for step := range 4096 {
			x = x*6364136223846793005 + 1442695040888963407
			if x>>60%2 == 0 {
				v := int(x % 1024)
				a, b := ref.TryPush(v), got.TryPush(v)
				if a != b {
					t.Fatalf("capacity %d step %d: push %v vs %v", capacity, step, a, b)
				}
			} else {
				va, a := ref.TryPop()
				vb, b := got.TryPop()
				if a != b || va != vb {
					t.Fatalf("capacity %d step %d: pop (%d,%v) vs (%d,%v)",
						capacity, step, va, a, vb, b)
				}
			}
			if ref.Len() != got.Len() {
				t.Fatalf("capacity %d step %d: len %d vs %d",
					capacity, step, ref.Len(), got.Len())
			}
		}
	}
}

// TestNativeRingConcurrent checks the open-coded ring under -race: nothing is
// lost, nothing is duplicated and the capacity is respected.
func TestNativeRingConcurrent(t *testing.T) {
	const (
		producers = 4
		consumers = 4
		perP      = 512
	)
	r := newNativeRing[int](64)
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make([]int, producers*perP)
	wg.Add(producers + consumers)
	for p := range producers {
		go func() {
			defer wg.Done()
			for i := range perP {
				for !r.TryPush(p*perP + i) {
				}
			}
		}()
	}
	done := make(chan struct{})
	for range consumers {
		go func() {
			defer wg.Done()
			for {
				v, ok := r.TryPop()
				if !ok {
					select {
					case <-done:
						if v, ok := r.TryPop(); ok {
							mu.Lock()
							seen[v]++
							mu.Unlock()
							continue
						}
						return
					default:
						continue
					}
				}
				mu.Lock()
				seen[v]++
				mu.Unlock()
			}
		}()
	}
	go func() {
		for {
			mu.Lock()
			n := 0
			for _, c := range seen {
				n += c
			}
			mu.Unlock()
			if n == producers*perP {
				close(done)
				return
			}
		}
	}()
	wg.Wait()
	for v, n := range seen {
		if n != 1 {
			t.Fatalf("value %d was delivered %d times", v, n)
		}
	}
	if r.Len() != 0 {
		t.Fatalf("the ring holds %d values after the run", r.Len())
	}
}
