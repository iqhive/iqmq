package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
)

// FuzzFakePeer holds the scripted peer to the invariants every critical test in
// `drivers/amqp10` rests on.
//
// It is here because the fake is not a test helper in the ordinary sense: it is
// the ONLY thing that produces a detach while a transfer is unresolved, a
// disposition for a delivery nobody handed out, a connection that dies with
// links attached — the cases the ambiguity and the staleness guard are
// about. A driver test that passed against a fake with a defect of its own
// would prove nothing, and the defect would be in the last place anybody looks.
//
// So the target drives a random script of the operations a driver performs and
// asserts four things the peer must never do, in every interleaving:
//
//  1. **Nothing is lost or invented, and the BAND is the ambiguity.** The
//     peer records at least every transfer `Send` returned nil for and at most
//     every transfer that was attempted — and the gap between the two is
//     exactly the set of publications whose outcome nobody knows. The target
//     was written with the tighter invariant ("recorded == accepted") and its
//     own sixth seed refuted it in twenty milliseconds: a HELD transfer whose
//     disposition never arrives returns an error and the frame has still left,
//     which is the case `TestCriticalAmbiguousPublishWhenDispositionNeverArrives`
//     exists for. A fake that dropped it would make that test vacuous.
//     Separately, every message delivered to a receiver arrives at most once.
//  2. **A detached or closed link refuses, and says which.** A send or a
//     disposition after a detach is an error and never a silent success: a
//     fake that accepted one would let a driver settle into a void.
//  3. **The credit accounting cannot go negative and cannot exceed what was
//     issued.** Manual credit is the whole reason `credit.go` exists
//     , and a peer that delivered more than its credit
//     would hide the very drift the driver counts.
//  4. **Nothing panics and nothing deadlocks.** Every call is made with a
//     context that is already bounded, so a peer that blocked for ever would
//     fail here rather than hang a driver test for ten minutes.
//
// The script is the fuzzer's bytes, one operation per byte, which is
// `internal/fuzzutil`'s shape without the dependency: this package's import
// list is the client library and the standard library, and a fake that pulled
// in a root-module helper would be a fake the root module could not be built
// without.
func FuzzFakePeer(f *testing.F) {
	f.Add([]byte{opSession, opSender, opSend, opSend, opResolveHeld})
	f.Add([]byte{opSession, opReceiver, opCredit, opDeliver, opReceive, opAccept})
	f.Add([]byte{opSession, opSender, opSend, opDetachSender, opSend})
	f.Add([]byte{opSession, opReceiver, opDeliver, opReceive, opDetachReceiver, opAccept})
	f.Add([]byte{opSession, opSender, opSend, opKillConn, opSend, opSession})
	f.Add([]byte{opSession, opReceiver, opCredit, opCredit, opDeliver, opDeliver, opReceive, opReceive})
	f.Add([]byte{opSession, opSender, opHoldNext, opSend, opDetachSender})
	f.Add([]byte{opSession, opCloseSession, opSender})
	f.Add([]byte{opRefuseAttach, opSession, opSender, opAllowAttach, opSender})
	f.Add([]byte{opSession, opReceiver, opDrain, opCredit, opDrain})
	f.Add([]byte{})
	// The fuzzer's own find: a second attach on one session, which makes the
	// counters belong to a link that no longer exists.
	f.Add([]byte("Z8(8"))

	f.Fuzz(func(t *testing.T, script []byte) {
		runFakeScript(t, script)
	})
}

// The script's operations. They are bytes so that the corpus is readable in a
// hex dump and stable across a change to the set.
const (
	opSession byte = iota
	opCloseSession
	opSender
	opReceiver
	opSend
	opHoldNext
	opResolveHeld
	opDetachSender
	opDetachReceiver
	opDeliver
	opReceive
	opAccept
	opCredit
	opDrain
	opKillConn
	opCloseConn
	opRefuseAttach
	opAllowAttach
	opCount
)

// sendBounded performs one transfer, bounding the context only when the peer's
// script will HOLD this transfer's disposition.
//
// The bound is in a function of its own because the cancel has to run when the
// send returns and not when the script ends: a `defer` inside the loop would
// hold one timer per operation for the whole input, which is the leak
// `gocritic`'s deferInLoop names and which a fuzzer's million executions would
// make real.
func sendBounded(ctx context.Context, snd *FakeSender, m *amqp.Message, holding bool) error {
	if !holding {
		return snd.Send(ctx, m, nil)
	}
	// A held transfer's disposition never arrives on its own and this script is
	// one goroutine, so nothing could resolve it and a plain context would park
	// here for ever.
	sctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	return snd.Send(sctx, m, nil)
}

// runFakeScript is the target's body, in a function of its own so that a
// regression can be replayed from a table test as well as from the corpus.
func runFakeScript(t *testing.T, script []byte) {
	t.Helper()
	const addr = "/queues/q"
	fake := NewFake(map[string]any{"product": "fuzz", "version": "1.0.0"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := fake.Dial(ctx, "amqp://peer", nil)
	if err != nil {
		t.Fatalf("the first dial failed with no script: %v", err)
	}
	fc, ok := conn.(*FakeConn)
	if !ok {
		t.Fatalf("Dial returned %T, want *FakeConn", conn)
	}

	var (
		sess      Session
		snd       *FakeSender
		rcv       *FakeReceiver
		sentOK    int
		delivered int
		received  int
		attempted int
		credit    uint32
		// planned and transfers are the model of the peer's send script, which
		// is POSITIONAL: entry i answers the peer's (i+1)th transfer ever, and
		// every Send reaches the script -- a detached sender's included, because
		// the peer reads its plan before it looks at the link. So transfer
		// number transfers+1 is held exactly when it is within the planned
		// entries, and this target plans nothing but holds.
		//
		// It used to be one boolean, set by a hold and cleared by the next send,
		// which is right for one hold and wrong for two in a row: the first send
		// was bounded and consumed one entry, the flag went false, and the second
		// send met the second held entry with an unbounded context and parked for
		// ever. The fuzzer reported that as a hang with no assertion (nightly
		// run 35551100054, seed 9e8c0108705b9a78, whose script plans two holds
		// before its first two sends); the peer was doing what its script said.
		planned   int
		transfers int
	)
	for i, op := range script {
		switch op % opCount {
		case opSession:
			s, serr := fc.NewSession(ctx, nil)
			if serr == nil {
				sess = s
			}
		case opCloseSession:
			if sess != nil {
				_ = sess.Close(ctx)
				sess, snd, rcv = nil, nil, nil
			}
		case opSender:
			if sess == nil {
				continue
			}
			s, aerr := sess.NewSender(ctx, addr, nil)
			if aerr == nil {
				fs, isFake := s.(*FakeSender)
				if !isFake {
					t.Fatalf("NewSender returned %T", s)
				}
				// The counters are per LINK and `Sent()` is the link's own, so
				// a second attach starts them again. The fuzzer found this in
				// the target rather than in the fake, with the script
				// `session sender send sender`: the accepted transfer belonged
				// to the first sender and the assertion was reading the
				// second's empty list. The input is a committed seed, because
				// a bookkeeping error in a target is as good at hiding a
				// defect as one in the code.
				snd, sentOK, attempted = fs, 0, 0
			}
		case opReceiver:
			if sess == nil {
				continue
			}
			r, aerr := sess.NewReceiver(ctx, addr, &amqp.ReceiverOptions{Credit: -1})
			if aerr == nil {
				fr, isFake := r.(*FakeReceiver)
				if !isFake {
					t.Fatalf("NewReceiver returned %T", r)
				}
				rcv, credit, delivered, received = fr, 0, 0, 0
			}
		case opHoldNext:
			fake.PlanSends(SendPlan{Hold: true})
			planned++
		case opSend:
			if snd == nil {
				continue
			}
			// A HELD transfer's disposition never arrives on its own, and this
			// script is one goroutine — so nothing could resolve it and a
			// plain context would park here for ever. The bound is applied
			// only to a transfer the script holds, so an ordinary one still
			// runs unbounded and a peer that parked it would hang here, which
			// is invariant (4) doing its job.
			detached := snd.Detached()
			if !detached {
				attempted++
			}
			held := transfers < planned
			transfers++
			serr := sendBounded(ctx, snd, &amqp.Message{Data: [][]byte{{byte(i)}}}, held)
			switch {
			case serr == nil && detached:
				t.Fatalf("op %d: a detached sender accepted a transfer; a driver would settle into a void", i)
			case serr == nil:
				sentOK++
			}
		case opResolveHeld:
			if snd != nil {
				_ = snd.ResolveHeld(1, nil)
			}
		case opDetachSender:
			if snd != nil {
				snd.Detach(errors.New("fuzz: detached"))
			}
		case opDetachReceiver:
			if rcv != nil {
				rcv.Detach(errors.New("fuzz: detached"))
			}
		case opDeliver:
			if rcv != nil {
				rcv.Deliver(&amqp.Message{Data: [][]byte{{byte(i)}}})
				delivered++
			}
		case opReceive:
			if rcv == nil {
				continue
			}
			// A bounded context, so a receiver with nothing to hand over
			// returns instead of parking: the target is about the peer's
			// bookkeeping and not about its blocking.
			rctx, rcancel := context.WithCancel(ctx)
			rcancel()
			m, rerr := rcv.Receive(rctx, nil)
			if rerr == nil && m != nil {
				received++
			}
		case opAccept:
			if rcv == nil {
				continue
			}
			detached := rcv.Detached()
			derr := rcv.Dispose(ctx, &amqp.Message{}, Disposition{Outcome: OutcomeAccepted})
			if derr == nil && detached {
				t.Fatalf("op %d: a detached receiver accepted a disposition", i)
			}
		case opCredit:
			if rcv == nil {
				continue
			}
			if cerr := rcv.IssueCredit(1); cerr == nil {
				credit++
			}
		case opDrain:
			if rcv != nil {
				dctx, dcancel := context.WithCancel(ctx)
				dcancel()
				_ = rcv.DrainCredit(dctx, nil)
			}
		case opKillConn:
			fc.Kill(errors.New("fuzz: connection lost"))
		case opCloseConn:
			_ = fc.Close()
		case opRefuseAttach:
			fake.RefuseAttach(addr, errors.New("fuzz: refused"))
		case opAllowAttach:
			fake.AllowAttach(addr)
		}

		// (3) The credit accounting, after every operation rather than at the
		// end: a peer that went over its credit and came back would pass an
		// end-of-script check.
		if rcv != nil {
			if got := rcv.Credit(); got > credit {
				t.Fatalf("op %d: the receiver holds %d credit and %d was issued", i, got, credit)
			}
			if q := rcv.Queued(); q < 0 {
				t.Fatalf("op %d: the receiver queues %d messages", i, q)
			}
		}
	}

	// (1) Nothing lost, nothing invented, and the band between them is the
	// ambiguity the specification makes a driver report rather than resolve.
	if snd != nil {
		got := len(snd.Sent())
		switch {
		case got < sentOK:
			t.Fatalf("the sender returned nil for %d transfers and the peer recorded only %d: a publication "+
				"this driver would report CONFIRMED never reached the peer", sentOK, got)
		case got > attempted:
			t.Fatalf("the peer recorded %d transfers and only %d were attempted: the fake invented one, "+
				"which would make a driver's duplicate accounting wrong in the safe-looking direction",
				got, attempted)
		}
	}
	if received > delivered {
		t.Fatalf("the receiver handed out %d deliveries and was given %d: a delivery arrived twice, "+
			"which would make a driver's duplicate-detection test vacuous", received, delivered)
	}

	// (2) A closed connection refuses, and closing twice is not an error.
	_ = fc.Close()
	if err := fc.Close(); err != nil {
		t.Fatalf("closing a closed connection reported %v; the specification makes Close idempotent", err)
	}
	if _, serr := fc.NewSession(ctx, nil); serr == nil {
		t.Fatal("a closed connection opened a session")
	}
}
