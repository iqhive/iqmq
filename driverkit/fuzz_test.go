package driverkit_test

import (
	"context"
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// FuzzPushAdapter drives the ring with an op script and checks it against a
// reference model: a plain slice with the same bound.
//
// The invariants after every step are the ones a push adapter exists for. The
// ring never holds more than its capacity, whatever the order of pushes and
// fetches; a non-blocking push succeeds exactly when the model says there is
// room; a fetch returns the deliveries in the order they were pushed, which is
// what a broker's own ordering guarantee rests on; and the byte accounting comes
// back to zero when the ring empties, so a long-lived source cannot drift into
// refusing everything.
func FuzzPushAdapter(f *testing.F) {
	seed := func(steps func(w *fuzzutil.Writer)) {
		w := fuzzutil.NewWriter()
		steps(w)
		f.Add(w.Data())
	}
	seed(func(w *fuzzutil.Writer) {
		for range 4 {
			w.Choice(3, 0)
			w.Int(0, 32, 8)
		}
		w.Choice(3, 1)
		w.Int(1, 8, 4)
	})
	seed(func(w *fuzzutil.Writer) {
		w.Choice(3, 0)
		w.Int(0, 32, 1)
		w.Choice(3, 2)
	})
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 1, 2, 3})

	f.Fuzz(func(t *testing.T, data []byte) {
		ctx := context.Background()
		r := fuzzutil.NewReader(data)
		const capacity, maxBytes = 8, 64
		p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: capacity, MaxBytes: maxBytes})
		if err != nil {
			t.Fatalf("NewPushAdapter: %v", err)
		}
		var model []int // the bodies in the ring, oldest first
		modelBytes := 0
		next := 0
		dst := make([]mq.DriverDelivery, capacity)
		for !r.Exhausted() {
			switch r.Choice(3) {
			case 0: // a non-blocking push
				size := r.Int(0, 32)
				room := len(model) < capacity && (len(model) == 0 || modelBytes+size <= maxBytes)
				d := mq.DriverDelivery{Message: &mq.Message{Body: make([]byte, size)}, Token: uint64(next)}
				ok, perr := p.TryPush(d)
				if perr != nil {
					t.Fatalf("TryPush: %v", perr)
				}
				if ok != room {
					t.Fatalf("TryPush reported room=%v, the model says %v (len %d, bytes %d, size %d)",
						ok, room, len(model), modelBytes, size)
				}
				if ok {
					model = append(model, next)
					modelBytes += size
					next++
				}
			case 1: // a fetch of up to n
				n := r.Int(1, capacity)
				if len(model) == 0 {
					continue // a fetch on an empty ring would block
				}
				got, ferr := p.Fetch(ctx, dst[:n])
				if ferr != nil {
					t.Fatalf("Fetch: %v", ferr)
				}
				want := min(n, len(model))
				if got != want {
					t.Fatalf("Fetch returned %d, want %d", got, want)
				}
				for i := range got {
					if dst[i].Token != uint64(model[i]) {
						t.Fatalf("Fetch returned %d at %d, want %d: the ring reordered", dst[i].Token, i, model[i])
					}
					modelBytes -= len(dst[i].Message.Body)
				}
				model = model[got:]
			case 2: // drain everything the ring holds
				var drained []int
				n := p.Drain(func(d *mq.DriverDelivery) { drained = append(drained, int(d.Token)) })
				if n != len(model) {
					t.Fatalf("Drain reported %d, the model holds %d", n, len(model))
				}
				for i := range drained {
					if drained[i] != model[i] {
						t.Fatalf("Drain handed back %v, the model holds %v", drained, model)
					}
				}
				model, modelBytes = nil, 0
			}
			if got := p.Len(); got != len(model) {
				t.Fatalf("Len is %d, the model holds %d", got, len(model))
			}
			if got := p.Len(); got > capacity {
				t.Fatalf("the ring holds %d, past its capacity of %d", got, capacity)
			}
			if len(model) == 0 && p.Bytes() != 0 {
				t.Fatalf("an empty ring accounts for %d bytes: the accounting has drifted", p.Bytes())
			}
			if p.Blocked() != 0 {
				t.Fatalf("a single-goroutine script left %d pushes parked", p.Blocked())
			}
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := p.Fetch(ctx, dst); !errors.Is(err, mq.ErrClosed) && len(model) == 0 {
			t.Fatalf("Fetch on a closed, empty adapter: %v", err)
		}
	})
}

// FuzzTokenTable drives the slot table with an op script and checks the state
// machine against a reference model.
//
// The invariant that matters most is the one a model can see and a unit test
// cannot: no token ever resolves to a delivery other than the one it was claimed
// for. That is what a generation exists for, and a table that reused a slot
// without bumping it would settle the wrong message.
func FuzzTokenTable(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	w := fuzzutil.NewWriter()
	for range 3 {
		w.Choice(5, 0)
	}
	w.Choice(5, 1)
	w.Choice(5, 4)
	f.Add(w.Data())

	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		const size = 4
		tbl, err := driverkit.NewTokenTable(size)
		if err != nil {
			t.Fatalf("NewTokenTable: %v", err)
		}
		type live struct {
			token uint64
			id    int
			state driverkit.SettleState
		}
		var held []live
		// dead holds every token the table has released, settled or
		// invalidated. None of them may ever resolve again: a token that came
		// back to life would name whatever delivery took its slot, which is
		// how a stale settlement acknowledges someone else's message.
		var dead []uint64
		next := 0
		for !r.Exhausted() {
			switch r.Choice(5) {
			case 0: // claim
				id := next
				tok, ok := tbl.Claim(id)
				if ok != (len(held) < size) {
					t.Fatalf("Claim reported %v with %d of %d slots live", ok, len(held), size)
				}
				if ok {
					held = append(held, live{token: tok, id: id, state: driverkit.StateActive})
					next++
				}
			case 1: // settle
				if len(held) == 0 {
					continue
				}
				i := r.Int(0, len(held)-1)
				_, serr := tbl.Settle(held[i].token)
				switch held[i].state {
				case driverkit.StateActive:
					if serr != nil {
						t.Fatalf("Settle of an active delivery: %v", serr)
					}
					held[i].state = driverkit.StateSettling
				case driverkit.StateSettling:
					if !errors.Is(serr, mq.ErrSettlementInProgress) {
						t.Fatalf("a racing settlement: %v", serr)
					}
				case driverkit.StateUnknown:
					if !errors.Is(serr, mq.ErrSettlementUnknown) {
						t.Fatalf("a settlement of an unknown delivery: %v", serr)
					}
				case driverkit.StateFree, driverkit.StateSettled:
					if serr == nil {
						t.Fatal("a settled delivery was settled again")
					}
				}
			case 2: // complete
				if len(held) == 0 {
					continue
				}
				i := r.Int(0, len(held)-1)
				if held[i].state != driverkit.StateSettling {
					continue
				}
				if cerr := tbl.Complete(held[i].token, nil); cerr != nil {
					t.Fatalf("Complete: %v", cerr)
				}
				dead = append(dead, held[i].token)
				held = append(held[:i], held[i+1:]...)
			case 3: // release
				if len(held) == 0 {
					continue
				}
				i := r.Int(0, len(held)-1)
				if rerr := tbl.Release(held[i].token); rerr != nil {
					t.Fatalf("Release: %v", rerr)
				}
				dead = append(dead, held[i].token)
				held = append(held[:i], held[i+1:]...)
			case 4: // invalidate everything
				if n := tbl.InvalidateAll(); n != len(held) {
					t.Fatalf("InvalidateAll reported %d, the model holds %d", n, len(held))
				}
				for _, h := range held {
					if _, rerr := tbl.Resolve(h.token); !errors.Is(rerr, mq.ErrLeaseLost) {
						t.Fatalf("a token from before the invalidation still resolves: %v", rerr)
					}
					dead = append(dead, h.token)
				}
				held = nil
			}
			if got := tbl.Live(); got != len(held) {
				t.Fatalf("Live is %d, the model holds %d", got, len(held))
			}
			// A token the table has given up never comes back to life.
			for _, tok := range dead {
				if ref, rerr := tbl.Resolve(tok); rerr == nil {
					t.Fatalf("token %d was released and resolves again, to %v: a settlement carrying it would land on the delivery that took its slot", tok, ref)
				} else if !errors.Is(rerr, mq.ErrLeaseLost) {
					t.Fatalf("a dead token resolved with %v, want ErrLeaseLost", rerr)
				}
			}
			// No token ever names another delivery's slot.
			for _, h := range held {
				ref, rerr := tbl.Resolve(h.token)
				if rerr != nil {
					t.Fatalf("a live token stopped resolving: %v", rerr)
				}
				if h.state != driverkit.StateUnknown && ref != any(h.id) {
					t.Fatalf("token %d resolves to %v, want %d: a slot was reused under a live token", h.token, ref, h.id)
				}
			}
		}
	})
}
