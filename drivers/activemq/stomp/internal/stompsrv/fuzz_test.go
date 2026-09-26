package stompsrv

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzSTOMPServer drives the server through a scripted sequence of client
// frames and holds it to its OWN invariants.
//
// It exists for the reason `drivers/amqp10/internal/client`'s peer fuzz target
// exists, and the reason is not that this server is production code: **a
// defect in the peer makes every critical test in the module above it vacuous,
// in the last place anybody looks.** `drivers/activemq/stomp`'s whole test
// suite asserts what the driver put on the wire by reading this server's frame
// log, so a server that silently dropped a frame, recorded one twice, answered
// a receipt it should have held, or delivered to a subscription that had been
// unsubscribed would turn "the driver sent no NACK frame" into a statement
// about this file rather than about the driver.
//
// So every assertion here is the server's and not a driver's:
//
//   - it never panics, whatever arrives on the socket — including bytes that
//     are not a frame at all;
//   - the frame log is DENSE and ORDERED: every recorded frame's Seq is its
//     own 1-based index, and its Conn is a connection the server accepted;
//   - it records only commands a CLIENT sends, so its own vocabulary
//     (CONNECTED, MESSAGE, RECEIPT, ERROR) can never appear in the log;
//   - it answers a receipted frame exactly once, and answers nothing for a
//     frame that carried no `receipt` header;
//   - it never delivers to a subscription that has been unsubscribed, or to a
//     connection that is gone;
//   - a protocol violation is RECORDED rather than guessed at, and an
//     ordinary disconnection is not recorded as one;
//   - `Close` is idempotent, and every goroutine it started has stopped by the
//     time it returns — which the package's TestMain checks for the whole run
//     and which is the invariant a test server most easily breaks.
//
// The client side is a raw socket and `frame.Writer` rather than
// `gostomp.Conn`, for two reasons. The library would refuse to send most of
// what this target needs to send — a SUBSCRIBE with no id, an ACK naming an
// identifier nobody issued, a frame with no terminator at all — and the
// library's own two permanent goroutine leaks would then be reported as this
// server's.
func FuzzSTOMPServer(f *testing.F) {
	seedSTOMPServer(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		srv, err := New(Options{ServerHeader: "ActiveMQ-Artemis/2.44.0"})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		c := newScriptedClient(t, srv)
		s := &serverScript{srv: srv, c: c}

		serverSteps().Run(t, data, s)

		// The final checks need the server to have read everything the
		// client wrote and the client to have counted everything the server
		// answered. quiesce half-closes the socket and waits for the
		// server's own close, so both are over rather than probably over.
		c.quiesce()
		s.checkSettled(t)
		s.checkRefusals(t)
		s.checkReceipts(t)
		s.checkViolations(t)

		c.stop()
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		// Idempotent, and checked on every input rather than in a test of its
		// own: a Close that was not would be a second listener close and a
		// second wg.Wait on goroutines that have already gone.
		if err := srv.Close(); err != nil {
			t.Errorf("the second Close reported %v", err)
		}
	})
}

// serverScript is what the steps act on.
type serverScript struct {
	srv *Server
	c   *scriptedClient
	// issued are the `ack` identifiers the server has handed out, so that a
	// settlement can name one that really exists. A fuzzer that invented a
	// fresh identifier every step would never send an ACK the server could
	// match, which is the case the accounting is about.
	issued []string
	// subIDs are the subscription ids the client has used.
	subIDs []string
}

// dests are the destinations the script draws from: an Artemis fully qualified
// queue name, a Classic virtual-topic consumer queue, and the empty string,
// which is a destination no broker accepts and which the server must therefore
// handle without guessing.
var dests = [...]string{"orders::orders.workers", "/queue/Consumer.workers.VirtualTopic.orders", ""}

// serverSteps is the op script. Every step reads a FIXED sequence of values
// from the reader, with no short-circuiting, so that [seedSTOMPServer] can
// mirror it exactly — a seed whose encoder and decoder disagree is a seed that
// exercises something nobody chose.
func serverSteps() *fuzzutil.Script[*serverScript] {
	return &fuzzutil.Script[*serverScript]{
		MaxSteps: 32,
		Steps: []fuzzutil.Step[*serverScript]{
			{Name: "connect", Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				creds, login, passcode := r.Bool(), r.String(8), r.String(8)
				fr := frame.New(frame.CONNECT, frame.AcceptVersion, "1.2", frame.Host, "localhost")
				if creds {
					fr.Header.Set(frame.Login, login)
					fr.Header.Set(frame.Passcode, passcode)
				}
				s.c.write(fr)
			}},
			{Name: "subscribe", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				dest, hasID, id, hasReceipt, receipt := dests[r.Choice(len(dests))], r.Bool(),
					strconv.Itoa(r.Int(0, 4)), r.Bool(), strconv.Itoa(r.Int(0, 99))
				fr := frame.New(frame.SUBSCRIBE, frame.Destination, dest, frame.Ack, "client-individual")
				if hasID {
					// A SUBSCRIBE with NO id is a frame go-stomp never sends
					// (it allocates one) and a broker refuses; the server has
					// to record it rather than index a subscription on "".
					fr.Header.Set(frame.Id, id)
					s.subIDs = append(s.subIDs, id)
				}
				if hasReceipt {
					fr.Header.Set(frame.Receipt, "r"+receipt)
				}
				s.c.write(fr)
			}},
			{Name: "send", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				dest, body, hasReceipt, receipt := dests[r.Choice(len(dests))], r.Bytes(16),
					r.Bool(), strconv.Itoa(r.Int(0, 99))
				fr := frame.New(frame.SEND, frame.Destination, dest)
				fr.Header.Set(frame.ContentLength, strconv.Itoa(len(body)))
				fr.Body = body
				if hasReceipt {
					fr.Header.Set(frame.Receipt, "r"+receipt)
				}
				s.c.write(fr)
			}},
			{Name: "ack", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				s.settle(r, frame.ACK)
			}},
			{Name: "nack", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				s.settle(r, frame.NACK)
			}},
			{Name: "unsubscribe", Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				known, hasReceipt := r.Bool(), r.Bool()
				fr := frame.New(frame.UNSUBSCRIBE)
				id := "never-subscribed"
				if known && len(s.subIDs) > 0 {
					id = s.subIDs[len(s.subIDs)-1]
				}
				fr.Header.Set(frame.Id, id)
				if hasReceipt {
					// go-stomp tags an UNSUBSCRIBE with a receipt of its own,
					// from inside its process loop, so a receipted one is the
					// normal case rather than the odd one -- and a server
					// that did not answer it stalls every requeue for thirty
					// seconds.
					fr.Header.Set(frame.Receipt, id)
				}
				s.c.write(fr)
			}},
			{Name: "transaction", Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				which, hasReceipt, receipt := r.Choice(3), r.Bool(), strconv.Itoa(r.Int(0, 99))
				fr := frame.New([...]string{frame.BEGIN, frame.COMMIT, frame.ABORT}[which],
					frame.Transaction, "t1")
				if hasReceipt {
					fr.Header.Set(frame.Receipt, "r"+receipt)
				}
				s.c.write(fr)
			}},
			{Name: "raw", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				// The only way to send what is not a frame. An empty command,
				// a header with no colon, a `content-length` that disagrees
				// with the body, a NUL inside a header value and a frame with
				// no terminator are all unreachable through `frame.Writer`,
				// which encodes a valid frame by construction -- and they are
				// exactly the inputs a server must refuse rather than guess
				// at.
				s.c.raw(r.Bytes(64))
			}},
			{Name: "deliver", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *serverScript) {
				dest, body, redelivered := dests[r.Choice(len(dests))], r.Bytes(8), r.Bool()
				ids, err := s.srv.Deliver(dest, Message{Body: body, Redelivered: redelivered})
				// Whether a subscription is live at this instant is not
				// observable from here without racing the server's own
				// reader: the SUBSCRIBE frame may be written and not yet
				// read. So what is asserted is what does not depend on the
				// timing -- that a refusal names one of the two reasons the
				// server has, and that a success hands out exactly one
				// identifier per message and never reuses one.
				// `TestDeliverRefusesAnUnsubscribedSubscription` is where the
				// ordering itself is pinned, deterministically.
				if err != nil {
					switch {
					case strings.Contains(err.Error(), "nothing is subscribed to"),
						strings.Contains(err.Error(), "connection under the subscription"):
					default:
						t.Fatalf("Deliver to %q was refused with %v, which is neither of the two reasons this "+
							"server has. A third reason is a refusal a test cannot tell from a defect", dest, err)
					}
					return
				}
				if len(ids) != 1 {
					t.Fatalf("Deliver of one message to %q returned %d identifiers", dest, len(ids))
				}
				if ids[0] == "" {
					t.Fatalf("Deliver to %q handed out an empty `ack` identifier, which a STOMP 1.2 ACK frame "+
						"would name as its `id` and no broker could match", dest)
				}
				for _, seen := range s.issued {
					if seen == ids[0] {
						t.Fatalf("Deliver handed out the `ack` identifier %q twice. SettledIDs would then be "+
							"ambiguous, and \"no ACK reached the server for THAT delivery\" -- which is what two "+
							"of the module's critical tests assert -- would mean nothing", ids[0])
					}
				}
				s.issued = append(s.issued, ids[0])
			}},
			{Name: "disconnect", Run: func(_ testing.TB, r *fuzzutil.Reader, s *serverScript) {
				hasReceipt := r.Bool()
				fr := frame.New(frame.DISCONNECT)
				if hasReceipt {
					fr.Header.Set(frame.Receipt, "bye")
				}
				s.c.write(fr)
			}},
		},
		Check: func(t testing.TB, s *serverScript) { s.checkLog(t) },
	}
}

// settle writes an ACK or a NACK naming a known identifier, one nobody issued,
// or nothing at all.
func (s *serverScript) settle(r *fuzzutil.Reader, cmd string) {
	kind, unknown := r.Choice(3), strconv.Itoa(r.Int(0, 99))
	fr := frame.New(cmd)
	switch {
	case kind == 0 && len(s.issued) > 0:
		fr.Header.Set(frame.Id, s.issued[len(s.issued)-1])
	case kind == 1:
		fr.Header.Set(frame.Id, "never-issued-"+unknown)
	default:
		// A settlement with no `id` at all is a frame no broker can act on,
		// and the server has to record it as a violation rather than index a
		// settlement on "".
	}
	s.c.write(fr)
}

// checkLog is the invariant every step has to leave true.
func (s *serverScript) checkLog(t testing.TB) {
	t.Helper()
	frames := s.srv.Frames()
	acks, nacks := 0, 0
	for i, got := range frames {
		if got.Seq != i+1 {
			t.Fatalf("the frame log is not dense: frame %d carries Seq %d. Every assertion in the module above "+
				"this one reads the log by position, and Server.Commands answers from a sequence number", i, got.Seq)
		}
		if got.Conn < 1 || got.Conn > s.srv.Accepted() {
			t.Fatalf("frame %s arrived on connection %d and the server has accepted %d. A test about a "+
				"reconnection is a test about WHICH connection a frame arrived on", got, got.Conn, s.srv.Accepted())
		}
		if !readerCommand(got.Command) {
			t.Fatalf("the server recorded a frame with the command %q, which `frame.Reader` refuses outright: a "+
				"frame that could not be read cannot be in the log", got.Command)
		}
		switch got.Command {
		case frame.ACK:
			acks++
		case frame.NACK:
			nacks++
		}
	}
	// The SettledIDs cross-check is NOT here, and the fuzzer is why: `Check`
	// runs after every step, `Frames` and `SettledIDs` are two separate lock
	// acquisitions, and the log can grow between them -- so a one-step "ack"
	// script reported one settled identifier against a snapshot holding no
	// ACK frame. The comparison belongs where the log has stopped changing.
	_, _ = acks, nacks
}

// checkSettled holds [Server.SettledIDs] to the frame log, which is what two
// of the module's critical tests read as "no ACK reached the server for THAT
// delivery".
//
// It runs after the reader has drained, because the two accessors take the
// server's lock separately and a growing log makes them disagree for reasons
// that are the TEST's rather than the server's.
func (s *serverScript) checkSettled(t testing.TB) {
	t.Helper()
	frames := s.srv.Frames()
	for _, cmd := range [...]string{frame.ACK, frame.NACK} {
		var want []string
		for _, got := range frames {
			if got.Command == cmd {
				want = append(want, got.Get(frame.Id))
			}
		}
		ids := s.srv.SettledIDs(cmd)
		if len(ids) != len(want) {
			t.Fatalf("SettledIDs(%s) has %d entries and the log holds %d %s frames. \"The driver sent no NACK "+
				"frame\" is the module's headline assertion and it reads exactly this", cmd, len(ids), len(want), cmd)
		}
		for i := range want {
			if ids[i] != want[i] {
				t.Fatalf("SettledIDs(%s)[%d] is %q and the log's %s frame at that position names %q. A settlement "+
					"accessor that did not agree with the log would make every settlement assertion in the module "+
					"above this one read a different frame from the one it names", cmd, i, ids[i], cmd, want[i])
			}
		}
	}
}

// checkRefusals holds the server to REFUSING a command that is its own
// vocabulary rather than acting on it.
//
// `frame.Reader` accepts CONNECTED, MESSAGE, RECEIPT and ERROR as well as the
// client's commands, so a client really can send one. Recording it is right --
// the log is what arrived -- and acting on it would not be: a MESSAGE frame
// that reached `handle`'s SEND arm would publish on the client's behalf.
//
// It runs after the reader has drained, and the fuzzer is why: `record` and
// `violate` are two separate lock acquisitions in that order, so a per-step
// check can see the frame before the violation and fail for a reason that is
// the TEST's timing rather than the server's behaviour.
func (s *serverScript) checkRefusals(t testing.TB) {
	t.Helper()
	for _, got := range s.srv.Frames() {
		if clientCommand(got.Command) {
			continue
		}
		if len(s.srv.Violations()) == 0 {
			t.Fatalf("the server recorded a %s frame from the CLIENT and logged no violation for it. That command "+
				"is the server's own vocabulary, so acting on one would let a client publish, deliver or answer "+
				"its own receipt", got.Command)
		}
	}
}

// checkReceipts holds the server to answering a receipted frame exactly once.
func (s *serverScript) checkReceipts(t testing.TB) {
	t.Helper()
	_, want := receiptsOwed(s.srv)
	if got := s.c.receipts(); got != want {
		t.Fatalf("the server answered %d receipts for %d receipted frames it read. A receipt answered twice is a "+
			"RECEIPT frame go-stomp routes to a channel it has already closed, and one never answered is a "+
			"publisher parked until its timeout -- which is exactly the window Server.HoldReceipts exists to "+
			"open deliberately", got, want)
	}
}

// checkViolations holds the violation log to naming something.
func (s *serverScript) checkViolations(t testing.TB) {
	t.Helper()
	for _, v := range s.srv.Violations() {
		if v == nil || strings.TrimSpace(v.Error()) == "" {
			t.Fatal("the server recorded a violation with nothing in it, which says a frame was refused and not why")
		}
		if isGone(v) {
			t.Fatalf("the server recorded an ordinary disconnection as a violation: %v. Every test in the module "+
				"above this one closes its client, so each would then fail in its cleanup", v)
		}
	}
}

// answersReceipt reports whether the server answers a `receipt` header on this
// command.
//
// It is not every command. A CONNECT is answered with CONNECTED and a frame
// the server refuses is answered with ERROR and the socket, so a receipt on
// either is owed nothing -- and a receipt accounting that counted them would
// wait for an answer that is never coming.
func answersReceipt(cmd string) bool {
	switch cmd {
	case frame.SEND, frame.SUBSCRIBE, frame.UNSUBSCRIBE, frame.ACK, frame.NACK,
		frame.BEGIN, frame.COMMIT, frame.ABORT, frame.DISCONNECT:
		return true
	default:
		return false
	}
}

// receiptsOwed is how many answers the log says the server owes, over the
// frames the SCRIPT sent: quiesce's barrier frame is the instrument's own and is
// accounted for there.
func receiptsOwed(srv *Server) (frames, owed int) {
	for _, f := range srv.Frames() {
		if isBarrier(f.Command, f.Get(frame.Receipt)) {
			continue
		}
		frames++
		if f.Has(frame.Receipt) && answersReceipt(f.Command) {
			owed++
		}
	}
	return frames, owed
}

// readerCommand reports whether `frame.Reader` accepts the command at all. A
// frame it refuses never reaches the server's own handler, so one in the log
// would mean the log was written by something other than the reader.
func readerCommand(cmd string) bool {
	switch cmd {
	case frame.CONNECTED, frame.MESSAGE, frame.RECEIPT, frame.ERROR:
		return true
	default:
		return clientCommand(cmd)
	}
}

// clientCommand reports whether a command is one a CLIENT sends, and therefore
// one the server may act on.
func clientCommand(cmd string) bool {
	switch cmd {
	case frame.CONNECT, frame.STOMP, frame.SEND, frame.SUBSCRIBE, frame.UNSUBSCRIBE,
		frame.ACK, frame.NACK, frame.BEGIN, frame.COMMIT, frame.ABORT, frame.DISCONNECT:
		return true
	default:
		return false
	}
}

// seedSTOMPServer is the committed corpus, authored through the script's own
// encoder so that a seed names the steps it means.
//
// Each one is a case worth keeping, and the files under
// `testdata/fuzz/FuzzSTOMPServer/` hold the same scripts: `f.Add` keeps them
// readable and in sync with the decoder, and the files are what
// `make fuzz-seeds` and the fuzz manifest count.
func seedSTOMPServer(f *testing.F) {
	for _, s := range serverSeeds() {
		f.Add(s.data)
	}
}

// serverSeed is one named seed.
type serverSeed struct {
	name string
	data []byte
}

// serverSeeds are the scripts worth committing.
func serverSeeds() []serverSeed {
	sc := serverSteps()
	build := func(fn func(w *fuzzutil.Writer)) []byte {
		w := fuzzutil.NewWriter()
		fn(w)
		return w.Data()
	}
	connect := func(w *fuzzutil.Writer) {
		sc.WriteStep(w, "connect")
		w.Bool(false)
		w.String(8, "")
		w.String(8, "")
	}
	subscribe := func(w *fuzzutil.Writer, id int, receipt bool) {
		sc.WriteStep(w, "subscribe")
		w.Choice(len(dests), 0)
		w.Bool(true)
		w.Int(0, 4, id)
		w.Bool(receipt)
		w.Int(0, 99, 1)
	}
	send := func(w *fuzzutil.Writer, body string, receipt bool) {
		sc.WriteStep(w, "send")
		w.Choice(len(dests), 0)
		w.Bytes(16, []byte(body))
		w.Bool(receipt)
		w.Int(0, 99, 2)
	}
	deliver := func(w *fuzzutil.Writer, redelivered bool) {
		sc.WriteStep(w, "deliver")
		w.Choice(len(dests), 0)
		w.Bytes(8, []byte("b"))
		w.Bool(redelivered)
	}
	settle := func(w *fuzzutil.Writer, cmd string, kind int) {
		sc.WriteStep(w, cmd)
		w.Choice(3, kind)
		w.Int(0, 99, 7)
	}
	unsubscribe := func(w *fuzzutil.Writer, known, receipt bool) {
		sc.WriteStep(w, "unsubscribe")
		w.Bool(known)
		w.Bool(receipt)
	}
	raw := func(w *fuzzutil.Writer, b string) {
		sc.WriteStep(w, "raw")
		w.Bytes(64, []byte(b))
	}
	disconnect := func(w *fuzzutil.Writer, receipt bool) {
		sc.WriteStep(w, "disconnect")
		w.Bool(receipt)
	}

	return []serverSeed{
		// The whole ordinary life of a consumer, which is what every test in
		// the module above does: connect, subscribe, take a delivery,
		// acknowledge it, unsubscribe, disconnect.
		{"the-ordinary-consumer", build(func(w *fuzzutil.Writer) {
			connect(w)
			subscribe(w, 1, false)
			deliver(w, false)
			settle(w, "ack", 0)
			unsubscribe(w, true, true)
			disconnect(w, true)
		})},
		// The requeue this driver's headline finding rests on: the
		// subscription is reset and the broker redelivers, marked.
		{"the-requeue-and-its-redelivery", build(func(w *fuzzutil.Writer) {
			connect(w)
			subscribe(w, 1, false)
			deliver(w, false)
			unsubscribe(w, true, true)
			subscribe(w, 2, false)
			deliver(w, true)
			settle(w, "ack", 0)
		})},
		// A NACK frame, which is the one verb that really sends one.
		{"a-nack-frame", build(func(w *fuzzutil.Writer) {
			connect(w)
			subscribe(w, 1, false)
			deliver(w, false)
			settle(w, "nack", 0)
		})},
		// A settlement naming an identifier nobody issued, which both brokers
		// accept silently and which the server must record against that id.
		{"a-settlement-nobody-issued", build(func(w *fuzzutil.Writer) {
			connect(w)
			subscribe(w, 1, false)
			settle(w, "ack", 1)
			settle(w, "nack", 1)
		})},
		// A settlement with no `id` at all: a frame no broker can act on, and
		// a violation rather than a settlement of "".
		{"a-settlement-with-no-id", build(func(w *fuzzutil.Writer) {
			connect(w)
			subscribe(w, 1, false)
			settle(w, "ack", 2)
		})},
		// A delivery to a subscription that has gone, which must be refused
		// rather than written to nobody.
		{"deliver-after-unsubscribe", build(func(w *fuzzutil.Writer) {
			connect(w)
			subscribe(w, 1, false)
			unsubscribe(w, true, true)
			deliver(w, false)
		})},
		// An UNSUBSCRIBE for an id nobody subscribed, which is what a
		// reconnection race produces.
		{"unsubscribe-nobody-subscribed", build(func(w *fuzzutil.Writer) {
			connect(w)
			unsubscribe(w, false, true)
		})},
		// A receipted SEND and an unreceipted one on one connection, which is
		// the accounting the receipt invariant is about.
		{"receipted-and-unreceipted-sends", build(func(w *fuzzutil.Writer) {
			connect(w)
			send(w, "one", true)
			send(w, "two", false)
			send(w, "three", true)
		})},
		// The empty destination, which no broker accepts.
		{"the-empty-destination", build(func(w *fuzzutil.Writer) {
			connect(w)
			sc.WriteStep(w, "subscribe")
			w.Choice(len(dests), 2)
			w.Bool(true)
			w.Int(0, 4, 1)
			w.Bool(true)
			w.Int(0, 99, 1)
		})},
		// A SUBSCRIBE with no id, which go-stomp never sends and a broker
		// refuses: the server must not index a subscription on "".
		{"subscribe-with-no-id", build(func(w *fuzzutil.Writer) {
			connect(w)
			sc.WriteStep(w, "subscribe")
			w.Choice(len(dests), 0)
			w.Bool(false)
			w.Int(0, 4, 0)
			w.Bool(false)
			w.Int(0, 99, 0)
			deliver(w, false)
		})},
		// A transaction's frames, which this driver never sends and which
		// Classic refuses to transact a poison-ack in at all.
		{"a-transaction", build(func(w *fuzzutil.Writer) {
			connect(w)
			sc.WriteStep(w, "transaction")
			w.Choice(3, 0)
			w.Bool(true)
			w.Int(0, 99, 3)
		})},
		// --- the malformed frames, none of which `frame.Writer` can produce
		{"raw-empty-command", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "\n\n\x00")
		})},
		{"raw-header-with-no-colon", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "SEND\ndestination\n\n\x00")
		})},
		{"raw-content-length-disagrees", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "SEND\ndestination:d\ncontent-length:99\n\nshort\x00")
		})},
		// The shape three nightly crashers were really about (#53, #59, #63):
		// a frame that DECLARES a body and sends none. `frame.Reader` believes
		// the declaration before it reads a byte, so without
		// [MaxDeclaredBody] these forty-six bytes allocate four gibibytes in
		// the server's reader and four more in `barrierParsesAlone` — which
		// over eight fuzz workers is a process the kernel kills, and a crasher
		// recorded against whichever input was being minimized.
		{"raw-content-length-beyond-the-bound", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "SEND\ndestination:d\ncontent-length:4294967295\n\n")
		})},
		{"raw-nul-inside-a-header-value", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "SEND\ndestination:d\nk:a\x00b\n\n\x00")
		})},
		{"raw-no-terminator", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "SEND\ndestination:d\n\nbody-with-no-null")
		})},
		{"raw-unknown-command", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "TELEPORT\ndestination:d\n\n\x00")
		})},
		{"raw-a-server-command-from-the-client", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "MESSAGE\ndestination:d\nsubscription:1\nack:a\n\n\x00")
		})},
		{"raw-nothing-but-heart-beats", build(func(w *fuzzutil.Writer) {
			connect(w)
			raw(w, "\n\n\n\n")
		})},
		{"raw-before-the-connect", build(func(w *fuzzutil.Writer) {
			raw(w, "SEND\ndestination:d\n\n\x00")
		})},
	}
}

// TestServerSeedsAreCommitted is what keeps the corpus files and the seeds in
// this file the same set.
//
// A seed authored with `f.Add` runs in `go test -run '^Fuzz'` and a file under
// `testdata/fuzz/` runs there too, and the two can drift apart silently: an
// `f.Add` seed nobody committed is a case the fuzz manifest does not count,
// and a committed file whose script the decoder no longer understands is a
// case that exercises something nobody chose. This checks that every seed has
// its file.
func TestServerSeedsAreCommitted(t *testing.T) {
	t.Parallel()
	for _, s := range serverSeeds() {
		path := seedPath(s.name)
		raw, err := os.ReadFile(path)
		got := string(raw)
		if err != nil {
			t.Errorf("the seed %q has no committed file at %s: %v. Write it with the corpus format's "+
				"`go test fuzz v1` header and one `[]byte(...)` line", s.name, path, err)
			continue
		}
		if want := corpusFile(s.data); got != want {
			t.Errorf("the committed file %s does not hold the script %q names:\n got %q\nwant %q",
				path, s.name, got, want)
		}
	}
}

// corpusFile renders one seed in Go's fuzz-corpus format.
//
// The format is a version line and one Go literal per fuzz argument, parsed
// with `go/parser` — so `[]byte(<quoted>)` is what a single-`[]byte` target's
// file holds.
func corpusFile(data []byte) string {
	return "go test fuzz v1\n[]byte(" + strconv.Quote(string(data)) + ")\n"
}

// seedPath is where a seed's committed file lives.
func seedPath(name string) string {
	return filepath.Join("testdata", "fuzz", "FuzzSTOMPServer", name)
}

// TestWriteServerSeeds regenerates the committed corpus from [serverSeeds].
//
// It is gated on an environment variable and skips otherwise, because a test
// that wrote to the repository on every run would make the corpus a thing
// nobody reviewed. Run it when a step's argument sequence changes:
//
//	IQMQ_WRITE_SEEDS=1 GOWORK=off go test -run TestWriteServerSeeds ./internal/stompsrv
//
// and then read the diff: a seed whose bytes moved because the DECODER moved
// is a seed that now exercises something nobody chose, which is what
// [TestServerSeedsAreCommitted] fails on until this has been run.
func TestWriteServerSeeds(t *testing.T) {
	if os.Getenv("IQMQ_WRITE_SEEDS") == "" {
		t.Skip("set IQMQ_WRITE_SEEDS=1 to rewrite the committed fuzz corpus from serverSeeds()")
	}
	dir := filepath.Join("testdata", "fuzz", "FuzzSTOMPServer")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	for _, s := range serverSeeds() {
		if err := os.WriteFile(seedPath(s.name), []byte(corpusFile(s.data)), 0o644); err != nil {
			t.Fatalf("writing the seed %q: %v", s.name, err)
		}
	}
	t.Logf("wrote %d seeds to %s", len(serverSeeds()), dir)
}

// scriptedClient is one raw socket to the server, with a goroutine draining
// whatever the server sends back.
//
// The responses are COUNTED rather than parsed, because what the invariants
// need is how many receipts came back and not what was in them: a RECEIPT
// frame carries the receipt id and nothing else, which is the fact
// `Sink.fillReceipt` rests on.
type scriptedClient struct {
	t    *testing.T
	conn net.Conn
	w    *frame.Writer
	wg   sync.WaitGroup
	once sync.Once
	// sent is every byte the server accepted from this client, in order, so
	// that quiesce can ask the server's own frame reader what the stream it
	// has been handed will parse as. Only the test goroutine touches it.
	sent bytes.Buffer
	// done is closed when drain returns: the server has ended the
	// connection.
	done chan struct{}
	// barrierSeen is closed when the RECEIPT for quiesce's barrier frame
	// arrives.
	barrierSeen chan struct{}

	mu        sync.Mutex
	rx        int
	barrierRx int
	err       error
}

// newScriptedClient dials the server and starts draining it.
func newScriptedClient(t *testing.T, srv *Server) *scriptedClient {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server: %v", err)
	}
	// Every read and write is bounded, because every refusal this target
	// exercises is "the server said nothing or closed the socket" and the
	// second half of that is a hang without a deadline.
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	c := &scriptedClient{t: t, conn: conn, done: make(chan struct{}), barrierSeen: make(chan struct{})}
	c.w = frame.NewWriter(io.MultiWriter(conn, &c.sent))
	c.wg.Add(1)
	go c.drain()
	return c
}

// drain reads the server's frames until the socket ends.
func (c *scriptedClient) drain() {
	defer c.wg.Done()
	defer close(c.done)
	r := frame.NewReader(c.conn)
	for {
		f, err := r.Read()
		if err != nil {
			c.mu.Lock()
			c.err = err
			c.mu.Unlock()
			return
		}
		if f == nil {
			continue // a heart-beat newline, which is not a frame
		}
		c.mu.Lock()
		switch {
		case f.Command != frame.RECEIPT:
		case f.Header.Get(frame.ReceiptId) == barrierReceipt:
			c.barrierRx++
			if c.barrierRx == 1 {
				close(c.barrierSeen)
			}
		default:
			c.rx++
		}
		c.mu.Unlock()
	}
}

// receipts is how many RECEIPT frames the server has sent for the script's
// frames, not counting quiesce's barrier.
func (c *scriptedClient) receipts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rx
}

// write sends one frame and ignores a socket the server has closed: an ERROR
// frame is fatal to the connection on both brokers, so a script that keeps
// writing afterwards has met the server's own contract rather than broken it.
func (c *scriptedClient) write(f *frame.Frame) { _ = c.w.Write(f) }

// raw writes arbitrary bytes, which is the only way to send what is not a
// frame.
func (c *scriptedClient) raw(b []byte) {
	n, _ := c.conn.Write(b)
	c.sent.Write(b[:n])
}

// The barrier frame quiesce writes: an ABORT, which the server answers and acts
// on no further, carrying a receipt no script step can produce (a step's
// receipts are "r<n>", a subscription id or "bye").
const (
	barrierReceipt = "stompsrv-quiesce-barrier"
	barrierTx      = "stompsrv-quiesce-barrier"
)

// barrierBudget bounds the wait for the barrier's RECEIPT on a live connection.
// A server that answers in microseconds uses none of it; one that holds its
// answers uses all of it, and fails.
const barrierBudget = 10 * time.Second

// isBarrier reports whether a frame is quiesce's barrier.
func isBarrier(command, receipt string) bool {
	return command == frame.ABORT && receipt == barrierReceipt
}

// barrierFrame is a fresh copy of the barrier.
func barrierFrame() *frame.Frame {
	return frame.New(frame.ABORT, frame.Transaction, barrierTx, frame.Receipt, barrierReceipt)
}

// quiesce returns once the server has read everything the client wrote, has
// answered every receipt it owes WHILE THE CONNECTION WAS LIVE, and every
// answer has been counted. It asks rather than watches for a lull, in two
// steps.
//
// First a barrier: a frame with a receipt of its own, written after the
// script's last byte, waited for on the live socket. The server's reader
// handles frames in order and writes each RECEIPT before it reads the next
// frame, so when the barrier's RECEIPT arrives every answer owed before it has
// already arrived too. A server that held its answers -- until the connection
// ended, or behind a buffer nothing flushes -- holds the barrier's as well, and
// fails here after [barrierBudget] instead of passing on the answers it writes
// as it closes. The barrier is skipped when the conversation cannot carry it:
// when the server has already ended the connection (a DISCONNECT, an ERROR, a
// frame it refused), and when the bytes the script wrote end part-way through a
// frame, so that the barrier would become part of it. The second is decided by
// running `frame.Reader` -- the parser the server reads with -- over exactly
// the bytes sent, followed by the barrier: the barrier is written only if that
// reader returns it as a frame of its own.
//
// Then the client half-closes its socket. The server's reader handles
// everything up to that end of input, reads it, and closes its own side, which
// is the end of input [scriptedClient.drain] returns on after every frame the
// server wrote. When drain has returned the log is final and so is the receipt
// count, with no timing in the argument. Only the WRITE half is closed: a full
// local Close drops whatever the kernel's receive buffer still holds, which is
// exactly the receipts being counted. The socket's deadline bounds the wait.
//
// What this cannot see is an answer the server releases LATER from another
// goroutine: a held SEND whose RECEIPT turns up a few milliseconds after the
// barrier's arrives after the barrier and is lost when the server closes.
// Catching that needs a window of time and is left to the tests of the module
// above, whose ambiguous-publication tests fail on it.
//
// It used to watch for a lull -- three one-millisecond observations in which
// the log had not grown and every receipt it named had arrived -- and a lull is
// also what the server's deliberate UNSUBSCRIBE delay
// ([DefaultUnsubscribeReceiptDelay], five milliseconds) looks like. The nightly
// crasher e51022daa3222b23 is ABORT, CONNECT, a receipted SUBSCRIBE, an
// UNSUBSCRIBE and a receipted SEND: quiesce returned during the pause, before
// the SEND was read (199 rounds in 200, measured), so the receipt check
// usually saw a log without it and passed vacuously -- and when the checks ran
// late enough to see the SEND logged but not yet answered, it reported "1
// receipts for 2 receipted frames". The server answered both every time; the
// instrument was racing it.
func (c *scriptedClient) quiesce() {
	c.t.Helper()
	if c.barrierParsesAlone() {
		select {
		case <-c.done:
		default:
			c.write(barrierFrame())
			timer := time.NewTimer(barrierBudget)
			select {
			case <-c.barrierSeen:
			case <-c.done:
				// The server ended the connection before answering: it
				// closed after a frame the script sent, which the barrier
				// then followed into a closed socket.
			case <-timer.C:
				c.t.Fatalf("the server did not answer the barrier frame's receipt within %v on a live "+
					"connection. Every frame before it had been read, so the answers it owed are being held -- "+
					"to the end of the connection, or behind a write nothing flushes -- and a publisher waiting "+
					"on one would park until its own timeout", barrierBudget)
			}
			timer.Stop()
		}
	}
	if tc, ok := c.conn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	} else {
		c.t.Fatalf("the scripted client's socket is a %T, which cannot be half-closed", c.conn)
	}
	c.wg.Wait()
	c.mu.Lock()
	twice := c.barrierRx > 1
	c.mu.Unlock()
	if twice {
		c.t.Fatalf("the server answered the barrier frame's receipt %d times", c.barrierRx)
	}
}

// barrierParsesAlone reports whether the barrier, written now, would reach the
// server as a frame of its own: whether `frame.Reader`, reading the bytes this
// client has sent followed by the barrier, returns the barrier as its last
// frame. It is false when the script's bytes end inside a frame -- an
// unterminated body, a short `content-length`, half a command line -- which the
// barrier would complete, extend or corrupt, and when they hold something the
// reader refuses before the barrier is reached.
func (c *scriptedClient) barrierParsesAlone() bool {
	var b bytes.Buffer
	if err := frame.NewWriter(&b).Write(barrierFrame()); err != nil {
		c.t.Fatalf("encoding the barrier frame: %v", err)
	}
	// Through the server's own guard, and not because this reader is a server:
	// it is handed the script's raw bytes, so without the guard a `raw` step
	// that DECLARES a body of four gibibytes makes the test goroutine allocate
	// it here as well as the server's reader allocating it there — two
	// allocations per exec, in the same process, from forty-six bytes.
	r := frame.NewReader(newBoundedFrames(io.MultiReader(bytes.NewReader(c.sent.Bytes()), &b)))
	var last *frame.Frame
	for {
		f, err := r.Read()
		if err != nil {
			break
		}
		if f != nil {
			last = f
		}
	}
	return last != nil && len(last.Body) == 0 && last.Header.Len() == 2 &&
		isBarrier(last.Command, last.Header.Get(frame.Receipt)) && last.Header.Get(frame.Transaction) == barrierTx
}

// close shuts the socket once.
func (c *scriptedClient) close() { c.once.Do(func() { _ = c.conn.Close() }) }

// stop closes the client's socket and reports a response the codec could not
// read, which would mean the server had written a frame no go-stomp
// `processLoop` could accept.
func (c *scriptedClient) stop() {
	c.close()
	c.wg.Wait()
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	switch {
	case err == nil, errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed),
		errors.Is(err, io.ErrUnexpectedEOF):
	case errors.Is(err, frame.ErrInvalidCommand), errors.Is(err, frame.ErrInvalidFrameFormat):
		c.t.Errorf("the server sent a frame its own codec cannot read: %v. Everything it writes goes to a "+
			"go-stomp `processLoop`, which answers an unreadable frame by closing the connection", err)
	default:
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			c.t.Errorf("reading from the server timed out: %v", err)
		}
	}
}
