package nsqfake

import (
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// FuzzFakeChannel holds the MODEL to nsqd's own accounting.
//
// It matters because everything this driver's tests assert about a stale
// settlement, a delayed requeue or an expired lease is asserted against this
// model rather than against nsqd: a model that mis-accounts makes every one of
// those tests meaningless, and a model that silently swallowed a settlement
// would make the driver's lease check untestable — the tests would pass with
// the check removed.
//
// The invariant is conservation. Every copy of a message a channel has ever
// been given is in exactly one of four places — pending, deferred, in flight,
// or finished — and the counts of the verbs that moved it between them add up.
// The state machine is driven directly rather than through a consumer, so the
// target is deterministic: a goroutine delivering in the background would make
// the invariant true only at moments the fuzzer cannot name.
func FuzzFakeChannel(f *testing.F) {
	seedChannel(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		s := New()
		const topic, channel = "t", "c"
		s.EnsureChannel(topic, channel)
		c := s.channelLocked(topic, channel)
		if c == nil {
			t.Fatal("EnsureChannel created nothing")
		}
		deadline := time.Duration(r.Int(1, 60)) * time.Second
		maxInFlight := r.Int(1, 8)

		// held is what "a consumer" is holding: the identifier and the
		// attempt each delivery was made under, which is what a settlement
		// names and is the whole reason a stale one is detectable at all.
		type handle struct {
			id      gonsq.MessageID
			attempt uint16
		}
		var held []handle
		put := 0

		now := func() time.Time { return time.Now().Add(s.offset) }
		check := func(step string) {
			t.Helper()
			total := len(c.pending) + len(c.deferred) + len(c.inflight) + c.finished
			if total != put {
				t.Fatalf("after %s: %d pending + %d deferred + %d in flight + %d finished = %d, "+
					"but %d were put: a copy of a message went missing or was invented",
					step, len(c.pending), len(c.deferred), len(c.inflight), c.finished, total, put)
			}
			for id, m := range c.inflight {
				if m.id != id {
					t.Fatalf("after %s: the in-flight map is keyed by %v and holds %v", step, id, m.id)
				}
				if m.attempt == 0 {
					t.Fatalf("after %s: an in-flight delivery reports attempt 0; nsqd counts from one", step)
				}
			}
			if c.crossSettled < 0 || c.crossTouched < 0 || c.lateSettled < 0 {
				t.Fatalf("after %s: a counter went negative", step)
			}
		}

		for steps := r.Int(0, 48); steps > 0; steps-- {
			switch r.Choice(7) {
			case 0: // publish
				c.put(&msgState{id: newID(), body: []byte("b")}, 0, now())
				put++
				check("publish")
			case 1: // publish with a delay
				c.put(&msgState{id: newID(), body: []byte("b")}, time.Duration(r.Int(1, 120))*time.Second, now())
				put++
				check("deferred publish")
			case 2: // deliver
				if m, attempt := c.take(now(), maxInFlight, deadline); m != nil {
					held = append(held, handle{id: m.id, attempt: attempt})
				}
				check("deliver")
			case 3: // finish
				if len(held) > 0 {
					i := r.Choice(len(held))
					h := held[i]
					c.settle(h.id, h.attempt, false, 0, now())
					held = append(held[:i], held[i+1:]...)
				}
				check("finish")
			case 4: // requeue, sometimes with a delay
				if len(held) > 0 {
					i := r.Choice(len(held))
					h := held[i]
					c.settle(h.id, h.attempt, true, time.Duration(r.Int(0, 30))*time.Second, now())
					held = append(held[:i], held[i+1:]...)
				}
				check("requeue")
			case 5: // touch
				if len(held) > 0 {
					h := held[r.Choice(len(held))]
					c.touch(h.id, h.attempt, now(), deadline)
				}
				check("touch")
			default: // advance the clock
				s.offset += time.Duration(r.Int(0, 300)) * time.Second
				c.expireLocked(now())
				check("advance")
			}
		}

		// A settlement for a delivery the channel no longer has in flight is
		// nsqd's E_..._FAILED: it must be COUNTED and must move nothing.
		before := len(c.inflight) + len(c.pending) + c.finished
		c.settle(newID(), 1, false, 0, now())
		if after := len(c.inflight) + len(c.pending) + c.finished; after != before {
			t.Fatalf("a settlement for an unknown identifier moved the channel from %d to %d", before, after)
		}
		if c.lateSettled == 0 {
			t.Fatal("a settlement for an unknown identifier was not counted as late")
		}
		check("a late settlement")
	})
}

// seedChannel commits the inputs that reach each transition, including the
// three the driver's correctness rests on: a delivery superseded by a
// redelivery, a settlement for the superseded attempt, and a touch for it.
func seedChannel(f *testing.F) {
	seeds := []func(*fuzzutil.Writer){
		// Publish, deliver, finish.
		func(w *fuzzutil.Writer) {
			w.Int(0, 48, 3)
			w.Choice(7, 0)
			w.Choice(7, 2)
			w.Choice(7, 3)
			w.Choice(1, 0)
		},
		// Publish, deliver, requeue with a delay, advance, deliver again.
		func(w *fuzzutil.Writer) {
			w.Int(0, 48, 6)
			w.Choice(7, 0)
			w.Choice(7, 2)
			w.Choice(7, 4)
			w.Choice(1, 0)
			w.Int(0, 30, 10)
			w.Choice(7, 6)
			w.Int(0, 300, 60)
			w.Choice(7, 2)
			w.Choice(7, 3)
			w.Choice(1, 0)
		},
		// Publish, deliver, let the lease expire, deliver again, then settle
		// the SUPERSEDED attempt: the cross-settlement the driver must never
		// produce and the model must therefore be able to.
		func(w *fuzzutil.Writer) {
			w.Int(0, 48, 5)
			w.Choice(7, 0)
			w.Choice(7, 2)
			w.Choice(7, 6)
			w.Int(0, 300, 120)
			w.Choice(7, 2)
			w.Choice(7, 3)
			w.Choice(2, 0)
		},
		// A touch, and then a touch of a superseded attempt.
		func(w *fuzzutil.Writer) {
			w.Int(0, 48, 6)
			w.Choice(7, 0)
			w.Choice(7, 2)
			w.Choice(7, 5)
			w.Choice(1, 0)
			w.Choice(7, 6)
			w.Int(0, 300, 300)
			w.Choice(7, 2)
			w.Choice(7, 5)
			w.Choice(2, 0)
		},
		// A deferred publication that never comes due.
		func(w *fuzzutil.Writer) {
			w.Int(0, 48, 2)
			w.Choice(7, 1)
			w.Int(1, 120, 120)
			w.Choice(7, 2)
		},
	}
	for _, seed := range seeds {
		w := fuzzutil.NewWriter()
		w.Int(1, 60, 30)
		w.Int(1, 8, 4)
		seed(w)
		f.Add(w.Data())
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
}
