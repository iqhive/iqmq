package psfake

import (
	"context"
	"errors"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// FuzzFakeServerInvariants drives the fake through op scripts and holds it to
// the invariants a Pub/Sub subscription really has.
//
// The fake is what the driver's L1 tests and its critical tests read their
// answers from, so a fake that lost a message, delivered one twice at once, or
// accepted a settlement for an identifier it no longer held would make those
// tests prove the wrong thing — quietly, because every one of them would still
// pass. This is the oracle for the oracle.
//
// The invariants, each of them the service's:
//
//   - CONSERVATION. Every message published to a subscription is either in its
//     backlog, outstanding at a consumer, or acknowledged. Nothing else.
//   - EXCLUSIVITY. A message is never in the backlog and outstanding at the
//     same time, which is what an acknowledgement deadline buys.
//   - SETTLEMENT IDENTITY. A settlement names an acknowledgement identifier the
//     subscription is currently holding, and one it is not is refused with
//     ErrAckIDNotReachable — which is what the driver maps to ErrLeaseLost and
//     what the whole local lease check exists for.
//   - A NACK RETURNS THE MESSAGE and an ACK does not.
func FuzzFakeServerInvariants(f *testing.F) {
	for _, seed := range serverSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		s := New("p")
		s.CreateTopic("t")
		s.CreateSubscription(psapi.SubscriptionSpec{
			Subscription: "s", Topic: "t", AckDeadline: time.Minute,
		})
		pub, err := s.Publisher(psapi.PublisherConfig{Topic: "t", Ordering: r.Bool()})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		ctx := context.Background()

		// published counts what the subscription has ever been given, and
		// acked what it has finished with. held is the deliveries a consumer
		// is holding, by acknowledgement identifier.
		published, acked := 0, 0
		held := map[string]psapi.Settlement{}
		// stale are identifiers a consumer once held and no longer does.
		var stale []psapi.Settlement

		for step := 0; step < 64 && !r.Exhausted(); step++ {
			switch r.Choice(6) {
			case 0: // publish
				if _, perr := pub.Publish(ctx, &psapi.Outgoing{Data: r.Bytes(8)}).Get(ctx); perr == nil {
					published++
				}
			case 1: // deliver one message, if there is one
				if s.Backlog("s") == 0 {
					continue
				}
				in := deliverOne(t, s)
				if _, dup := held[keyOf(in)]; dup {
					t.Fatalf("the fake delivered one acknowledgement identifier twice at once: %q", keyOf(in))
				}
				held[keyOf(in)] = in.Settle
			case 2: // ack one held delivery
				k, settle := takeOne(held)
				if settle == nil {
					continue
				}
				if aerr := settle.AckWithResult(ctx); aerr != nil {
					t.Fatalf("acknowledging a held delivery %q: %v", k, aerr)
				}
				acked++
				stale = append(stale, settle)
			case 3: // nack one held delivery
				k, settle := takeOne(held)
				if settle == nil {
					continue
				}
				if nerr := settle.NackWithResult(ctx); nerr != nil {
					t.Fatalf("declining a held delivery %q: %v", k, nerr)
				}
				stale = append(stale, settle)
			case 4: // settle a delivery nobody holds any more
				if len(stale) == 0 {
					continue
				}
				settle := stale[r.Int(0, len(stale)-1)]
				if aerr := settle.AckWithResult(ctx); !errors.Is(aerr, psapi.ErrAckIDNotReachable) {
					t.Fatalf("a settlement for an identifier the subscription no longer holds reported %v, want ErrAckIDNotReachable", aerr)
				}
			default: // the acknowledgement deadline passes
				n := s.Expire()
				if n != len(held) {
					t.Fatalf("Expire redelivered %d of %d outstanding deliveries", n, len(held))
				}
				// Every identifier a consumer held is now unreachable: the
				// message is back in the backlog and will be delivered with a
				// new one.
				for _, settle := range held {
					stale = append(stale, settle)
				}
				held = map[string]psapi.Settlement{}
			}

			backlog, outstanding := s.Backlog("s"), s.Outstanding("s")
			if outstanding != len(held) {
				t.Fatalf("step %d: the subscription reports %d outstanding and a consumer holds %d",
					step, outstanding, len(held))
			}
			if backlog+outstanding+acked != published {
				t.Fatalf("step %d: %d published, %d in the backlog, %d outstanding, %d acknowledged: a message was lost or invented",
					step, published, backlog, outstanding, acked)
			}
		}
	})
}

// deliverOne takes exactly one message out of the subscription.
//
// It runs Receive with a context it cancels from inside the callback, which is
// the library's own documented way to stop a receive and is what makes this a
// single-delivery step rather than a loop.
func deliverOne(t *testing.T, s *Server) *psapi.Incoming {
	t.Helper()
	sub, err := s.Subscription(psapi.SubscriptionConfig{Subscription: "s"})
	if err != nil {
		t.Fatalf("Subscription: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got *psapi.Incoming
	err = sub.Receive(ctx, func(_ context.Context, in *psapi.Incoming) {
		if got == nil {
			got = in
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if got == nil {
		t.Fatal("Receive returned with nothing delivered from a non-empty backlog")
	}
	return got
}

// keyOf is the delivery's acknowledgement identifier, which is what the fake
// mints per delivery.
func keyOf(in *psapi.Incoming) string {
	if rec, ok := in.Native.(*record); ok {
		return rec.ackID
	}
	return in.ID
}

// takeOne removes an arbitrary held delivery, or reports none.
func takeOne(held map[string]psapi.Settlement) (string, psapi.Settlement) {
	for k, v := range held {
		delete(held, k)
		return k, v
	}
	return "", nil
}

// serverSeeds are the committed regression inputs: one per step of the script,
// so that a step the fuzzer stops reaching is a seed that still does.
func serverSeeds() [][]byte {
	return [][]byte{
		// Publish, deliver, ack — the ordinary path, and the one the
		// conservation invariant is checked against.
		script(false, 0, 1, 2),
		// Publish, deliver, nack, deliver again — the redelivery the
		// driver's stale-settlement defence is built on.
		script(false, 0, 1, 3, 1, 2),
		// Publish, deliver, expire, deliver — the same thing through the
		// acknowledgement deadline rather than through a nack.
		script(false, 0, 1, 5, 1, 2),
		// A settlement for an identifier nobody holds any more.
		script(false, 0, 1, 2, 4),
		// Two messages in flight at once, then both settled.
		script(true, 0, 0, 1, 1, 2, 3),
	}
}

// script renders one op sequence in the encoding [FuzzFakeServerInvariants]
// reads.
func script(ordering bool, ops ...int) []byte {
	w := fuzzutil.NewWriter()
	w.Bool(ordering)
	for _, op := range ops {
		w.Choice(6, op)
		switch op {
		case 0:
			w.Bytes(8, []byte("x"))
		case 4:
			w.Int(0, 0, 0)
		}
	}
	return w.Data()
}
