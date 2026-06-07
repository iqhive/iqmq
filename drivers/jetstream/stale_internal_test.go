package jetstream

import (
	"context"
	"errors"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// FuzzNATSStaleAck is the op-script fuzz of this driver's one safety-critical
// piece of bookkeeping.
//
// The hazard it exists for is specific to this broker. JetStream acknowledges by
// reply subject, and the server's AckExplicit path deletes its pending entry for
// the message's STREAM SEQUENCE without looking at which delivery the reply came
// from, so the reply subject of an expired delivery still works — acking it
// acknowledges the record a later delivery now owns. Nothing in the client or
// the server reports the loss, so the guard has to be the driver's, and the only
// thing that can hold a guard like this over every interleaving is a fuzz target
// with a model beside it.
//
// The model is two maps: which token currently owns each record, and which
// records have been settled. The invariant is one sentence — a settlement that
// SUCCEEDS must name the token that currently owns its record — and every
// failure mode the hazard has is a violation of it: a superseded token that
// settles, a settled record that settles twice, a slot reused under a live
// token.
//
// It runs against the real [source] bookkeeping and the real [settler], with a
// scripted message in place of the wire: what is being checked is which
// settlements reach the wire at all.
func FuzzNATSStaleAck(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0})                            // deliver, ack
	f.Add([]byte{0, 0, 0, 0, 1, 0})                      // deliver, redeliver, ack the first
	f.Add([]byte{0, 0, 0, 0, 1, 1})                      // deliver, redeliver, ack the second
	f.Add([]byte{0, 0, 0, 0, 1, 0, 1, 1})                // both, in order
	f.Add([]byte{0, 0, 1, 0, 0, 0, 1, 0})                // settle, redeliver, settle again
	f.Add([]byte{0, 0, 2, 0})                            // deliver, extend
	f.Add([]byte{0, 0, 0, 0, 2, 0})                      // deliver, redeliver, extend the first
	f.Add([]byte{0, 0, 3, 0})                            // deliver, nack
	f.Add([]byte{0, 0, 0, 1, 0, 2, 1, 0, 3, 1, 2, 2})    // a mixed script over two records
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4}) // fill the table, then sweep
	f.Fuzz(func(t *testing.T, script []byte) {
		const slots = 4
		src := newScriptedSource(t, slots)
		set := src.set

		// The model. live[seq] is the token that currently owns the record;
		// settled[seq] records a terminal settlement.
		live := map[uint64]uint64{}
		settled := map[uint64]bool{}
		// history is every token ever handed out, with the record it named,
		// so that the script can reach a superseded one.
		type issued struct {
			token uint64
			seq   uint64
		}
		var history []issued

		ctx := context.Background()
		for i := 0; i+1 < len(script); i += 2 {
			op, arg := script[i]%5, script[i+1]
			switch op {
			case 0: // deliver or redeliver a record
				seq := uint64(arg%3) + 1
				h := &held{msg: &scriptedMsg{}, seq: seq, delivered: 1}
				token, err := src.claim(h)
				if err != nil {
					// The table is full, which is the only refusal claim
					// has. Nothing changes in the model.
					continue
				}
				live[seq] = token
				settled[seq] = false
				history = append(history, issued{token: token, seq: seq})
			case 1, 3: // settle a delivery, positively or negatively
				if len(history) == 0 {
					continue
				}
				h := history[int(arg)%len(history)]
				var err error
				if op == 1 {
					err = set.Ack(ctx, &mq.DriverDelivery{Token: h.token})
				} else {
					err = set.Nack(ctx, &mq.DriverDelivery{Token: h.token}, mq.NackOptions{})
				}
				if err == nil {
					// THE INVARIANT. A settlement that succeeded reached
					// the wire, and on this broker the wire takes it: it
					// had better have named the delivery that owns the
					// record.
					if cur, ok := live[h.seq]; !ok || cur != h.token {
						t.Fatalf("a settlement of token %d succeeded for record %d, whose current token is %d (present %v): on JetStream that acknowledges the record another delivery owns ",
							h.token, h.seq, cur, ok)
					}
					if settled[h.seq] {
						t.Fatalf("record %d was settled twice", h.seq)
					}
					settled[h.seq] = true
					delete(live, h.seq)
					continue
				}
				// A refusal is one of the answers and never a bare
				// failure: a caller cannot act on "something went wrong".
				if !isSettlementAnswer(err) {
					t.Fatalf("a refused settlement of token %d reported %v (code %v), which is not one of the answers",
						h.token, err, mq.CodeOf(err))
				}
			case 2: // extend a lease
				if len(history) == 0 {
					continue
				}
				h := history[int(arg)%len(history)]
				err := set.Extend(ctx, &mq.DriverDelivery{Token: h.token}, time.Second)
				if err == nil {
					if cur, ok := live[h.seq]; !ok || cur != h.token {
						t.Fatalf("an extension of token %d succeeded for record %d, whose current token is %d (present %v): the handler would keep working on a delivery someone else owns",
							h.token, h.seq, cur, ok)
					}
					continue
				}
				if !isSettlementAnswer(err) {
					t.Fatalf("a refused extension reported %v (code %v)", err, mq.CodeOf(err))
				}
			case 4: // close: every outstanding delivery is naked
				src.nakOutstanding()
				for seq, token := range live {
					if err := set.Ack(ctx, &mq.DriverDelivery{Token: token}); err == nil {
						t.Fatalf("token %d for record %d settled after the source swept it", token, seq)
					}
				}
				live = map[uint64]uint64{}
			}
		}
		// Whatever the script did, the sweep is safe to run and leaves
		// nothing settleable behind.
		src.nakOutstanding()
		for _, h := range history {
			if err := set.Ack(ctx, &mq.DriverDelivery{Token: h.token}); err == nil {
				t.Fatalf("token %d settled after the final sweep", h.token)
			}
		}
	})
}

// TestStaleAckSeedsAreAScript is the seed corpus as a test, so that `make
// fuzz-seeds` really exercises the invariant under -race rather than only
// compiling it.
func TestStaleAckSeedsAreAScript(t *testing.T) {
	src := newScriptedSource(t, 2)
	h1 := &held{msg: &scriptedMsg{}, seq: 7, delivered: 1}
	first, err := src.claim(h1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	h2 := &held{msg: &scriptedMsg{}, seq: 7, delivered: 2}
	second, err := src.claim(h2)
	if err != nil {
		t.Fatalf("claim of the redelivery: %v", err)
	}
	if first == second {
		t.Fatal("a redelivery took the same token as the delivery it replaced")
	}
	ctx := context.Background()
	if err := src.set.Ack(ctx, &mq.DriverDelivery{Token: first}); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("acking the superseded delivery reported %v, want ErrLeaseLost", err)
	}
	if err := src.set.Ack(ctx, &mq.DriverDelivery{Token: second}); err != nil {
		t.Fatalf("acking the current delivery: %v", err)
	}
	if got := h2.msg.(*scriptedMsg).acks; got != 1 {
		t.Errorf("the current delivery was acked %d times, want 1", got)
	}
	if got := h1.msg.(*scriptedMsg).acks; got != 0 {
		t.Errorf("the superseded delivery was acked %d times, want 0: the ack reached the wire", got)
	}
	if got := src.b.StaleSettlements(); got != 1 {
		t.Errorf("StaleSettlements = %d, want 1", got)
	}
}

// isSettlementAnswer reports whether err is one of the answers the specification gives a
// settlement that did not happen.
func isSettlementAnswer(err error) bool {
	switch mq.CodeOf(err) {
	case mq.CodeLeaseLost, mq.CodeAlreadySettled, mq.CodeSettlementInProgress,
		mq.CodeSettlementUnknown, mq.CodeResourceExhausted, mq.CodeUnsupported,
		mq.CodeInvalidArgument, mq.CodeClosed:
		return true
	default:
		return false
	}
}

// newScriptedSource builds a source with the real token table, the real
// stream-sequence index and the real settler, and no connection at all.
//
// The bookkeeping is what is under test, so the wire is replaced and nothing
// else is: a fuzz target that reached a server would run a hundred inputs a
// second instead of a million, and the interleavings that matter are the
// bookkeeping's.
func newScriptedSource(tb testing.TB, slots int) *source {
	tb.Helper()
	table, err := driverkit.NewTokenTable(slots)
	if err != nil {
		tb.Fatalf("NewTokenTable: %v", err)
	}
	push, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: slots})
	if err != nil {
		tb.Fatalf("NewPushAdapter: %v", err)
	}
	b := &Backend{cfg: Config{}.resolve(), errs: newErrorMapper()}
	// The health reporter is the real one: TestDoubleNakIsNotABackgroundFailure
	// asserts on what a background failure does to it, and a nil reporter
	// would make that assertion a panic rather than a check.
	b.health = driverkit.NewHealth(b.onHealth)
	b.health.Ready()
	src := &source{
		b:            b,
		push:         push,
		table:        table,
		topic:        "scripted",
		subscription: "workers",
		ackWait:      time.Second,
		seqs:         map[uint64]uint64{},
		closing:      make(chan struct{}),
	}
	src.set = &settler{src: src}
	return src
}

// scriptedMsg is a [github.com/nats-io/nats.go/jetstream.Msg] that records the
// settlement verbs instead of sending them.
//
// It is the whole of the wire in FuzzNATSStaleAck: what the target checks is
// which settlements reach a message at all, so a message that counts them is
// exactly the observation needed.
type scriptedMsg struct {
	acks, naks, terms, progress int
}

func (m *scriptedMsg) Metadata() (*js.MsgMetadata, error) {
	return &js.MsgMetadata{Sequence: js.SequencePair{Stream: 1, Consumer: 1}, NumDelivered: 1}, nil
}

func (m *scriptedMsg) Data() []byte           { return nil }
func (m *scriptedMsg) Headers() natsgo.Header { return nil }
func (m *scriptedMsg) Subject() string        { return "scripted" }
func (m *scriptedMsg) Reply() string          { return "$JS.ACK.scripted" }

func (m *scriptedMsg) Ack() error                       { m.acks++; return nil }
func (m *scriptedMsg) DoubleAck(context.Context) error  { m.acks++; return nil }
func (m *scriptedMsg) Nak() error                       { m.naks++; return nil }
func (m *scriptedMsg) NakWithDelay(time.Duration) error { m.naks++; return nil }
func (m *scriptedMsg) InProgress() error                { m.progress++; return nil }
func (m *scriptedMsg) Term() error                      { m.terms++; return nil }
func (m *scriptedMsg) TermWithReason(string) error      { m.terms++; return nil }

// compile-time proof that the scripted message is the client library's own
// interface, so the settler under test is the real one.
var _ js.Msg = (*scriptedMsg)(nil)

// TestDoubleNakIsNotABackgroundFailure holds the one rule a closing source needs
// of the error mapping.
//
// The consume callback and the closing sweep can both reach one message: the
// callback pushes it into the ring, the sweep naks it, the push then fails
// because the ring is closed and the callback naks it too. The client library
// answers the second nak with ErrMsgAlreadyAckd, which is not a failure —
// somebody already settled the delivery, which is what the nak wanted — and
// reporting it would make a Close degrade the client's health and count a
// consume error.
func TestDoubleNakIsNotABackgroundFailure(t *testing.T) {
	src := newScriptedSource(t, 2)
	// A message that answers every settlement the way a message somebody has
	// already settled does.
	settled := &settledMsg{}
	src.nak(settled)
	if got := src.b.ConsumeErrors(); got != 0 {
		t.Errorf("a nak of an already-settled delivery reported %d consume errors, want 0", got)
	}
	if got := src.b.DegradedTransitions(); got != 0 {
		t.Errorf("a nak of an already-settled delivery degraded the client %d times, want 0", got)
	}
	// A nak that fails for a reason that is NOT "already settled" still
	// reaches health: a check that swallowed everything would hide a broken
	// link.
	src.nak(&brokenMsg{})
	if got := src.b.ConsumeErrors(); got != 1 {
		t.Errorf("a nak that failed for another reason reported %d consume errors, want 1", got)
	}
}

// settledMsg answers every settlement with the client library's
// "already acknowledged".
type settledMsg struct{ scriptedMsg }

func (m *settledMsg) Nak() error { return js.ErrMsgAlreadyAckd }

// brokenMsg answers with a failure that is nobody's normal close path.
type brokenMsg struct{ scriptedMsg }

func (m *brokenMsg) Nak() error { return natsgo.ErrMaxPayload }

var (
	_ js.Msg = (*settledMsg)(nil)
	_ js.Msg = (*brokenMsg)(nil)
)
