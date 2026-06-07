package stompsrv

import (
	"bytes"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
)

// The server's own invariants, pinned deterministically.
//
// `FuzzSTOMPServer` covers the accounting — a dense frame log, a receipt
// answered exactly once, an `ack` identifier never reused — and it cannot
// cover an ORDERING, because whether a SUBSCRIBE has been read yet is not
// observable from the client side without racing the server's reader. These
// three are the orderings, and each one is a facility the module above depends
// on: a delivery that must be refused, a receipt that must be held, and a
// refusal that must be one-shot.

const testBudget = 5 * time.Second

// live is how many subscriptions on a destination are attached, waited for.
func awaitLive(t *testing.T, srv *Server, dest string, want int) {
	t.Helper()
	err := srv.wait("the live subscriptions on "+dest+" reaching the expected count", testBudget, func() bool {
		n := 0
		for _, sub := range srv.subs {
			if sub.dest == dest && sub.live {
				n++
			}
		}
		return n == want
	})
	if err != nil {
		t.Fatalf("%v (Live(%q) is %d, want %d)", err, dest, srv.Live(dest), want)
	}
}

// TestDeliverRefusesAnUnsubscribedSubscription is the ordering the fuzz target
// cannot pin.
//
// A MESSAGE frame naming a subscription the client has unsubscribed is one
// go-stomp routes to nobody: `processLoop` looks the `subscription` header up
// in its own map and logs "ignored MESSAGE for subscription" when it is not
// there. So a server that delivered anyway would let a test assert on a
// delivery no consumer ever saw — and every settlement test in the module
// above reads exactly that.
func TestDeliverRefusesAnUnsubscribedSubscription(t *testing.T) {
	t.Parallel()
	srv, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	c := newScriptedClient(t, srv)
	t.Cleanup(c.stop)
	const dest = "orders::orders.workers"

	c.write(frame.New(frame.CONNECT, frame.AcceptVersion, "1.2", frame.Host, "localhost"))
	c.write(frame.New(frame.SUBSCRIBE, frame.Destination, dest, frame.Ack, "client-individual", frame.Id, "1"))
	awaitLive(t, srv, dest, 1)
	if _, err := srv.Deliver(dest, Message{Body: []byte("one")}); err != nil {
		t.Fatalf("Deliver to a live subscription: %v", err)
	}

	c.write(frame.New(frame.UNSUBSCRIBE, frame.Id, "1", frame.Receipt, "1"))
	awaitLive(t, srv, dest, 0)
	if _, err := srv.Deliver(dest, Message{Body: []byte("two")}); err == nil {
		t.Fatalf("Deliver to %q succeeded after the client unsubscribed. The MESSAGE frame would name a "+
			"subscription go-stomp has no channel for, so it would be logged and dropped -- and a test asserting "+
			"on that delivery would be asserting about nothing", dest)
	}
	// And the other direction: a fresh subscription on the same destination
	// takes deliveries again, so the refusal is about the subscription's
	// state and not about the destination being poisoned.
	c.write(frame.New(frame.SUBSCRIBE, frame.Destination, dest, frame.Ack, "client-individual", frame.Id, "2"))
	awaitLive(t, srv, dest, 1)
	if _, err := srv.Deliver(dest, Message{Body: []byte("three")}); err != nil {
		t.Fatalf("Deliver after re-subscribing: %v. A requeue on this driver is an unsubscribe and a "+
			"re-subscribe, so this is the path every negative settlement takes", err)
	}
}

// TestHoldReceiptsHoldsExactlyWhatItWasAsked is the ambiguous window as a
// facility, and it is the one thing in this package no broker will do on
// request.
//
// A publication whose RECEIPT never arrives is AMBIGUOUS and nothing else: the
// frame left the socket, so the broker may have the message. The hold has to
// be exact — one held publication and the next one answered — or
// `TestCriticalPublicationWithNoReceiptIsAmbiguous` would be green about a
// sink that had simply stopped working.
func TestHoldReceiptsHoldsExactlyWhatItWasAsked(t *testing.T) {
	t.Parallel()
	srv, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	c := newScriptedClient(t, srv)
	t.Cleanup(c.stop)

	c.write(frame.New(frame.CONNECT, frame.AcceptVersion, "1.2", frame.Host, "localhost"))
	srv.HoldReceipts(1)
	for _, id := range [...]string{"r1", "r2", "r3"} {
		f := frame.New(frame.SEND, frame.Destination, "orders", frame.Receipt, id)
		f.Header.Set(frame.ContentLength, "1")
		f.Body = []byte("b")
		c.write(f)
	}
	// quiesce first waits, with a bound, for the RECEIPT of a barrier frame
	// written after the three SENDs. The reader answers in order, so the two
	// answered publications were answered while the connection was LIVE: a
	// server that held every answer until it closed fails there. Then quiesce
	// ends the conversation and waits for the server to finish it, so the
	// count read afterwards is final and a third answer the reader wrote is
	// counted exactly, where the old wait-for-two-then-pause could only hope
	// it had arrived by then.
	//
	// What it cannot see is a held receipt released LATER from another
	// goroutine, after the barrier's has arrived: the half-close ends the
	// connection first and the late write is lost. That takes a window of
	// time to observe, and `TestCriticalPublicationWithNoReceiptIsAmbiguous`
	// in the module above fails on it.
	c.quiesce()
	if n := srv.Count(frame.SEND); n != 3 {
		t.Fatalf("the server read %d SEND frames, want 3", n)
	}
	if got := c.receipts(); got != 2 {
		t.Errorf("the server answered %d of three receipted publications, want 2: one held and two answered. A "+
			"hold that took more than it was asked for would make every publication after it look ambiguous, and "+
			"one that took fewer would make the window unreachable", got)
	}
}

// TestQuiesceOutlastsTheServersOwnPauses holds the fuzz target's instrument to
// what its final checks assume: when quiesce returns, the server has read every
// frame the client wrote and the client has counted every answer.
//
// The script is FuzzSTOMPServer's nightly crasher e51022daa3222b23, with the
// UNSUBSCRIBE's deliberate pause widened from five milliseconds to a hundred so
// that a quiesce which merely waits for a lull cannot straddle it by luck. The
// quiesce that watched for three quiet milliseconds returned during the pause,
// before the SEND was read, in 199 rounds of 200 at the default five; here it
// would return every time. The barrier quiesce writes is not counted in the
// frames or the receipts: it is the instrument's, not the script's.
func TestQuiesceOutlastsTheServersOwnPauses(t *testing.T) {
	t.Parallel()
	srv, err := New(Options{UnsubscribeReceiptDelay: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	c := newScriptedClient(t, srv)
	t.Cleanup(c.stop)

	c.write(frame.New(frame.ABORT, frame.Transaction, "t1"))
	c.write(frame.New(frame.CONNECT, frame.AcceptVersion, "1.2", frame.Host, "localhost"))
	c.write(frame.New(frame.SUBSCRIBE, frame.Destination, "orders::orders.workers", frame.Ack, "client-individual",
		frame.Id, "0", frame.Receipt, "r90"))
	c.write(frame.New(frame.UNSUBSCRIBE, frame.Id, "never-subscribed"))
	send := frame.New(frame.SEND, frame.Destination, "orders::orders.workers", frame.Receipt, "r34")
	send.Header.Set(frame.ContentLength, "5")
	send.Body = []byte(".hX1(")
	c.write(send)

	c.quiesce()
	frames, owed := receiptsOwed(srv)
	if frames != 5 {
		t.Fatalf("quiesce returned with %d of the 5 frames read: the checks after it would have judged a log that "+
			"was still growing", frames)
	}
	if got := c.receipts(); owed != 2 || got != 2 {
		t.Fatalf("after quiesce the server owed %d receipts and the client had counted %d, want 2 and 2", owed, got)
	}
}

// TestTheBarrierIsWrittenOnlyAtAFrameBoundary holds quiesce's decision to write
// its barrier frame to the only case in which the server can read it as a
// frame: the bytes the script sent end between frames.
//
// Written after half a frame, the barrier becomes part of it -- the rest of an
// unterminated body, bytes of a short `content-length`, the tail of a command
// line -- and its RECEIPT never comes, so quiesce would fail a correct server.
// Not written after a whole frame, quiesce loses the one check that the answers
// arrived while the connection was live.
func TestTheBarrierIsWrittenOnlyAtAFrameBoundary(t *testing.T) {
	var whole bytes.Buffer
	if err := frame.NewWriter(&whole).Write(frame.New(frame.SEND, frame.Destination, "d", frame.Receipt, "r1")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		sent string
		want bool
	}{
		{"nothing sent", "", true},
		{"a whole frame", whole.String(), true},
		{"heart-beats after a whole frame", whole.String() + "\n\r\n\n", true},
		{"an unterminated body", whole.String() + "SEND\ndestination:d\n\nbody-with-no-null", false},
		{"a short content-length", "SEND\ndestination:d\ncontent-length:99\n\nshort", false},
		{"half a command line", whole.String() + "SEN", false},
		{"headers with no blank line", "SEND\ndestination:d\n", false},
		{"a frame the reader refuses", "TELEPORT\n\n\x00", false},
	} {
		c := &scriptedClient{t: t}
		c.sent.WriteString(tc.sent)
		if got := c.barrierParsesAlone(); got != tc.want {
			t.Errorf("%s: barrierParsesAlone = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestRefuseSubscribeIsOneShot pins the choice
// [Server.RefuseSubscribe]'s doc comment makes.
//
// A standing refusal turns the driver's reconnect loop into a
// subscribe-refuse-redial spin for as long as the test runs, and what a test
// wants to observe is the refusal reaching a CALLER rather than the loop's
// behaviour under it. So the armed refusal fires once and the next SUBSCRIBE
// is accepted — and because an ERROR frame is fatal to the connection on both
// brokers, the second one arrives on a second connection.
func TestRefuseSubscribeIsOneShot(t *testing.T) {
	t.Parallel()
	srv, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	const dest = "orders::orders.workers"
	srv.RefuseSubscribe(dest, "AMQ339001 Destination does not exist "+dest)

	first := newScriptedClient(t, srv)
	t.Cleanup(first.stop)
	first.write(frame.New(frame.CONNECT, frame.AcceptVersion, "1.2", frame.Host, "localhost"))
	first.write(frame.New(frame.SUBSCRIBE, frame.Destination, dest, frame.Ack, "client-individual", frame.Id, "1"))
	if err := srv.WaitSubscribed(dest, 1, testBudget); err != nil {
		t.Fatalf("the server did not read the first SUBSCRIBE: %v", err)
	}
	// The refusal closes the connection, so the subscription it recorded is
	// retired with it -- which is what makes a refused SUBSCRIBE observable
	// as a SUBSCRIBE that happened AND a subscription that is not live.
	awaitLive(t, srv, dest, 0)

	second := newScriptedClient(t, srv)
	t.Cleanup(second.stop)
	second.write(frame.New(frame.CONNECT, frame.AcceptVersion, "1.2", frame.Host, "localhost"))
	second.write(frame.New(frame.SUBSCRIBE, frame.Destination, dest, frame.Ack, "client-individual", frame.Id, "1"))
	awaitLive(t, srv, dest, 1)
	if got := srv.Accepted(); got != 2 {
		t.Errorf("the server accepted %d connections, want 2: an ERROR frame is fatal to the connection, so the "+
			"re-subscription has to arrive on a new one", got)
	}
	if got := srv.Subscribes(dest); got != 2 {
		t.Errorf("the server recorded %d SUBSCRIBE frames on %s, want 2. The refused one has to be in the log: a "+
			"test proving the driver sent it reads exactly that", got, dest)
	}
}

// TestADeclaredBodyIsBoundedBeforeItIsBelieved is the memory bound
// [MaxDeclaredBody] exists for, and it is the defect behind issues #53, #59 and
// #63 — three nightly crashers for `FuzzSTOMPServer`, three weeks running, one
// cause.
//
// None of those three inputs reproduces anything, and that is the point: each
// run reported `fuzzing process hung or terminated unexpectedly while
// minimizing: EOF`, which is what Go's coordinator says when a WORKER PROCESS
// dies (`internal/fuzz/worker.go:270`). It then records whatever input it was
// minimizing at that moment, so the committed crasher names the wrong input
// every time. What killed the worker is here: forty-six bytes that declare a
// body and send none made the server allocate the declaration — four
// gibibytes, `Header.ContentLength` parsing a 32-bit value — and the fuzz
// target runs eight worker processes at once.
//
// So this is NOT a test about a message that is too big. It is a test that a
// NUMBER in a frame cannot decide how much this server allocates, which is why
// it asserts about allocation and not only about the refusal.
func TestADeclaredBodyIsBoundedBeforeItIsBelieved(t *testing.T) {
	srv, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	c := newScriptedClient(t, srv)
	t.Cleanup(c.stop)

	// A quarter of a gibibyte rather than the 4294967295 the fuzzer found: the
	// mechanism is linear in the declaration, so a regression should cost a run
	// 256 MiB rather than four gibibytes.
	const declared = 256 << 20
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	c.raw([]byte("SEND\ndestination:d\ncontent-length:" + strconv.Itoa(declared) + "\n\n"))

	// The refusal, which is also what makes the allocation measurable: an
	// unguarded reader records no violation at all here, because it blocks in
	// the body read until the socket ends and then reports the EOF, which
	// `isGone` keeps out of the violation log.
	if err := srv.wait("the over-large declaration being refused", testBudget, func() bool {
		return len(srv.violations) > 0
	}); err != nil {
		t.Fatalf("%v. A frame declaring %d bytes of body has to be refused before it is believed, and refused "+
			"the way everything else this server will not guess at is: a recorded violation and the socket",
			err, declared)
	}
	if got := srv.Violations()[0].Error(); !strings.Contains(got, strconv.Itoa(declared)) {
		t.Errorf("the violation is %q, which does not name the %d bytes that were declared. A violation that does "+
			"not say what was refused says a frame was refused and not why", got, declared)
	}
	if got := len(srv.Frames()); got != 0 {
		t.Errorf("the server recorded %d frames for a frame it refused to read, want 0", got)
	}

	runtime.ReadMemStats(&after)
	// Sixteen mebibytes is slack for everything else the exchange allocates;
	// the defect allocates the whole declaration, so the two are not close.
	const slack = 16 << 20
	if grew := after.TotalAlloc - before.TotalAlloc; grew > slack {
		t.Errorf("declaring %d bytes of body allocated %d bytes, more than the %d this exchange needs. A client's "+
			"number decides this server's memory, which under the fuzz target is eight worker processes allocating "+
			"it at once — and a worker the kernel kills is a crasher recorded against whichever input the "+
			"coordinator happened to be minimizing", declared, grew, slack)
	}
}

// TestABodyWithinTheBoundIsStillRead is the other half of [MaxDeclaredBody]:
// the guard must not have turned "a body this server reads" into "a body this
// server refuses". A SEND the driver really sends is the case that matters, and
// `frame.Writer` writes a `content-length` for every one of them.
func TestABodyWithinTheBoundIsStillRead(t *testing.T) {
	srv, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	c := newScriptedClient(t, srv)
	t.Cleanup(c.stop)
	c.write(frame.New(frame.CONNECT, frame.AcceptVersion, "1.2", frame.Host, "localhost"))

	body := bytes.Repeat([]byte("x"), 64<<10)
	send := frame.New(frame.SEND, frame.Destination, "d")
	send.Body = body
	c.write(send)
	if err := srv.WaitFrames(frame.SEND, 1, testBudget); err != nil {
		t.Fatalf("%v: a %d-byte body is within MaxDeclaredBody (%d) and has to arrive", err, len(body), MaxDeclaredBody)
	}
	if got := srv.FramesOf(frame.SEND)[0].Body; !bytes.Equal(got, body) {
		t.Errorf("the server recorded %d body bytes, want %d", len(got), len(body))
	}
	if got := srv.Violations(); len(got) != 0 {
		t.Errorf("the server recorded %v for a frame it was meant to read", got)
	}
}
