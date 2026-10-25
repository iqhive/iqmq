package amqp10_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
)

// TestSettlerSurfaces is the compile-time claim as a run-time check. Both
// absences are the protocol's or the library's rather than this driver's taste,
// and claiming either would be worse than not having it: the core asserts them
// once, when the source is opened, and records the answer in the capability
// table.
func TestSettlerSurfaces(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	set := f.source(mq.SourceConfig{Prefetch: 2}).Settler()
	if _, ok := set.(mq.BatchSettler); ok {
		t.Error("the settler claims mq.BatchSettler; go-amqp's range disposition is unexported, so a batch here " +
			"would be a loop and FeatureBatchSettle would advertise a round trip it does not save")
	}
	if _, ok := set.(mq.DeadlineExtender); ok {
		t.Error("the settler claims mq.DeadlineExtender; AMQP 1.0 grants no lease, so there is no deadline to extend")
	}
}

// TestSettlerVerbsBecomeTheRightOutcomes is the table over AMQP 1.0's four
// terminal outcomes, and none of the four is the others.
func TestSettlerVerbsBecomeTheRightOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		settle  func(context.Context, mq.Settler, *mq.DriverDelivery) error
		outcome client.Outcome
		check   func(*testing.T, client.Disposition)
	}{
		{
			name: "Ack",
			settle: func(ctx context.Context, s mq.Settler, d *mq.DriverDelivery) error {
				return s.Ack(ctx, d)
			},
			outcome: client.OutcomeAccepted,
		},
		{
			name: "Nack(requeue)",
			settle: func(ctx context.Context, s mq.Settler, d *mq.DriverDelivery) error {
				return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionRequeue})
			},
			// `modified{delivery-failed}` and NOT `released`: a caller that
			// nacks has tried and failed, so Attempt has to rise or
			// MaxAttempts is compared against a number that never moves and a
			// poison message is retried for ever. `released` is what the
			// DRIVER gives back — the close path's own release, which is a
			// different function for exactly this reason.
			outcome: client.OutcomeModified,
			check: func(t *testing.T, d client.Disposition) {
				if !d.DeliveryFailed {
					t.Error("a requeue did not count the attempt: the redelivery would arrive with the same " +
						"delivery-count, so the message contract's MaxAttempts would never fire and a poison " +
						"message would be retried for ever")
				}
				if len(d.Annotations) != 0 {
					t.Errorf("a plain requeue carries %d annotation(s); the delay's annotation belongs to a "+
						"DELAYED requeue and a broker reading one here would hold the message back", len(d.Annotations))
				}
			},
		},
		{
			name: "Nack(requeue) with a delay",
			settle: func(ctx context.Context, s mq.Settler, d *mq.DriverDelivery) error {
				return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionRequeue, Delay: 90 * time.Second})
			},
			outcome: client.OutcomeModified,
			check: func(t *testing.T, d client.Disposition) {
				if !d.DeliveryFailed {
					t.Error("a delayed requeue did not count the attempt, although the profile asked it to")
				}
				got, ok := d.Annotations["x-opt-redelivery-delay"].(int64)
				if !ok || got != 90_000 {
					t.Errorf("the delay reached the `modified` outcome as %v, want 90000 milliseconds under the "+
						"profile's own annotation", d.Annotations)
				}
			},
		},
		{
			name: "Nack(drop)",
			settle: func(ctx context.Context, s mq.Settler, d *mq.DriverDelivery) error {
				return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDrop, Reason: "poison"})
			},
			outcome: client.OutcomeRejected,
			check: func(t *testing.T, d client.Disposition) {
				if d.Error == nil {
					t.Fatal("the rejection carries no condition, so a broker's dead-letter queue records no reason")
				}
				if !contains(d.Error.Description, "poison") {
					t.Errorf("the rejection's description is %q, which does not carry the caller's reason",
						d.Error.Description)
				}
			},
		},
		{
			name: "Nack(deadletter)",
			settle: func(ctx context.Context, s mq.Settler, d *mq.DriverDelivery) error {
				return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDeadLetter})
			},
			outcome: client.OutcomeRejected,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := open(t, testProfile(), aq.Config{})
			src := f.source(mq.SourceConfig{Prefetch: 2})
			rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")
			if err := tc.settle(context.Background(), src.Settler(), &d); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			disps := rcv.Dispositions()
			if len(disps) != 1 {
				t.Fatalf("the peer received %d disposition(s), want 1", len(disps))
			}
			if got := disps[0].Disposition.Outcome; got != tc.outcome {
				t.Fatalf("%s became the `%v` outcome, want `%v`", tc.name, got, tc.outcome)
			}
			if tc.check != nil {
				tc.check(t, disps[0].Disposition)
			}
		})
	}
}

// TestSettlerRefusesWhatItCannotDoAndSettlesNothing is the rule outright: "a
// driver that cannot honour the action asked for returns ErrUnsupported and
// settles nothing", so the delivery is still settleable and the message is not
// lost.
//
// The second half is the one a driver gets wrong. A driver that had already
// moved the slot to settling would leave the delivery unsettleable for ever, so
// the check is that the SAME delivery can still be acknowledged afterwards.
func TestSettlerRefusesWhatItCannotDoAndSettlesNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile func() aq.Profile
		opts    mq.NackOptions
	}{
		{
			name: "a delayed requeue on a profile that cannot hold one back",
			profile: func() aq.Profile {
				p := testProfile()
				p.NackDelay = aq.NackDelayRule{}
				return p
			},
			opts: mq.NackOptions{Action: mq.NackActionRequeue, Delay: time.Minute},
		},
		{
			name:    "a delay beyond the profile's ceiling",
			profile: testProfile,
			opts:    mq.NackOptions{Action: mq.NackActionRequeue, Delay: 2 * time.Hour},
		},
		{
			name: "a dead-letter route on a profile that has none",
			profile: func() aq.Profile {
				p := testProfile()
				p.Capabilities = func(aq.Facts) []aq.Claim { return []aq.Claim{aq.Withdraw(mq.FeatureDeadLetter)} }
				return p
			},
			opts: mq.NackOptions{Action: mq.NackActionDeadLetter},
		},
		{
			name:    "an action outside the enumeration",
			profile: testProfile,
			opts:    mq.NackOptions{Action: mq.NackAction(99)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := open(t, tc.profile(), aq.Config{})
			src := f.source(mq.SourceConfig{Prefetch: 2})
			rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")
			err := src.Settler().Nack(context.Background(), &d, tc.opts)
			if err == nil {
				t.Fatalf("%s was accepted", tc.name)
			}
			if len(rcv.Dispositions()) != 0 {
				t.Fatalf("%s settled something at the peer; the specification says a driver that cannot honour the action "+
					"settles NOTHING", tc.name)
			}
			// And the delivery really is still settleable, which is the half a
			// driver that had already taken the slot would have broken.
			if aerr := src.Settler().Ack(context.Background(), &d); aerr != nil {
				t.Fatalf("the delivery could no longer be acknowledged after the refusal: %v; a refusal that took "+
					"the slot leaves the delivery unsettleable for ever", aerr)
			}
		})
	}
}

// TestSettlerSecondSettlementIsRefused is the exactly-once rule at the
// driver's own layer: "a second terminal action after success returns
// ErrAlreadySettled (not silently ignored — catches bugs)".
func TestSettlerSecondSettlementIsRefused(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	err := src.Settler().Ack(context.Background(), &d)
	if !errors.Is(err, mq.ErrLeaseLost) && !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("a second Ack = %v, want ErrAlreadySettled or ErrLeaseLost; the specification forbids ignoring it silently", err)
	}
	if got := len(rcv.Dispositions()); got != 1 {
		t.Fatalf("the peer received %d dispositions for one delivery", got)
	}
}

// TestSettlerFailureLeavesTheDeliverySettleable is the settlement state machine
// a DEFINITE settlement rejection returns the delivery to active, and
// the caller may try again.
func TestSettlerFailureLeavesTheDeliverySettleable(t *testing.T) {
	f := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleSecond})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")

	// A disposition the peer refuses with a condition, which is a definite
	// answer rather than an unknown one.
	rcv.Detach(&amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondNotFound}})
	err := src.Settler().Ack(context.Background(), &d)
	if err == nil {
		t.Fatal("the settlement succeeded on a detached link")
	}
	assertCode(t, err, mq.CodeNotFound, "a disposition the peer refused with a condition")
	// A definite refusal leaves the delivery settleable, so a retry is
	// possible — and reports the same thing, because the link is still gone.
	if again := src.Settler().Ack(context.Background(), &d); again == nil {
		t.Fatal("the retried settlement succeeded on a link that is still gone")
	}
}

// TestSettlerUnknownOutcomeIsSticky is the stickiness, and it reaches the
// credit window: a delivery whose settlement outcome nobody knows keeps its
// slot for ever, because a slot reused under a token whose fate nobody knows is
// how an unknown settlement becomes a successful one by accident.
func TestSettlerUnknownOutcomeIsSticky(t *testing.T) {
	f := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleSecond})
	src := f.source(mq.SourceConfig{Prefetch: 4})
	rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")

	// A link that simply closed, with no condition: under `second` the peer
	// confirms a settlement, so a failure may be a settlement it took and a
	// confirmation nobody heard.
	rcv.Detach(&amqp.LinkError{})
	err := src.Settler().Ack(context.Background(), &d)
	assertCode(t, err, mq.CodeSettlementUnknown, "a settlement whose confirmation never arrived")
	if !mq.IsAmbiguous(err) {
		t.Fatalf("the unknown settlement is not reported as ambiguous: %v", err)
	}
	stats := src.CreditStats()
	if stats.Unknown != 1 {
		t.Fatalf("the credit window records %d unknown outcome(s), want 1: the specification makes an unknown outcome sticky, "+
			"and a window that freed the slot would let another delivery take a token whose fate nobody knows",
			stats.Unknown)
	}
	if stats.Held != 1 {
		t.Fatalf("the credit window holds %d deliver(ies), want the unknown one to keep its slot", stats.Held)
	}
	// A second attempt is refused as unknown rather than retried, which is
	// what "nothing later turns it into a success" means.
	if again := src.Settler().Ack(context.Background(), &d); !errors.Is(again, mq.ErrSettlementUnknown) {
		t.Fatalf("a second settlement of an unknown delivery = %v, want ErrSettlementUnknown", again)
	}
}

// TestSettlerDeadLetterRouteOrdersThePublicationFirst is the rule: "a failed
// or ambiguous dead-letter publish never acknowledges the original".
//
// The order is the contract. A rejection that ran first would destroy or
// dead-letter the message before its copy was durable, so a failure in between
// would lose it outright.
func TestSettlerDeadLetterRouteOrdersThePublicationFirst(t *testing.T) {
	p := testProfile()
	p.DeadLetterAddress = func(topic, sub string) (string, error) { return "DLQ." + topic, nil }

	t.Run("the publication succeeds, then the rejection", func(t *testing.T) {
		f := open(t, p, aq.Config{})
		src := f.source(mq.SourceConfig{Prefetch: 2})
		rcv, d := deliverAndFetch(t, f, src, "tag-1", "poison")
		if err := src.Settler().Nack(context.Background(), &d,
			mq.NackOptions{Action: mq.NackActionDeadLetter, Reason: "unparseable"}); err != nil {
			t.Fatalf("Reject: %v", err)
		}
		// The copy reached the profile's address.
		var dlq *client.FakeSender
		for _, s := range f.peer.LastConn().Senders() {
			if s.Address() == "DLQ.topic" {
				dlq = s
			}
		}
		if dlq == nil {
			t.Fatal("no sending link attached to the profile's dead-letter address")
		}
		sent := dlq.Sent()
		if len(sent) != 1 {
			t.Fatalf("the dead-letter address received %d message(s), want 1", len(sent))
		}
		if got := string(sent[0].Data[0]); got != "poison" {
			t.Fatalf("the dead-lettered copy carries %q", got)
		}
		if got, ok := sent[0].Annotations[aq.AnnotationDeadLetterReason].(string); !ok || got != "unparseable" {
			t.Fatalf("the copy's reason annotation is %v; a dead-letter destination with no reason in it is a queue "+
				"somebody has to guess about", sent[0].Annotations[aq.AnnotationDeadLetterReason])
		}
		if _, ok := sent[0].Annotations[aq.AnnotationDeadLetterAttempt]; !ok {
			t.Fatalf("the copy carries no attempt annotation: %v", sent[0].Annotations)
		}
		// And only then was the original rejected.
		disps := rcv.Dispositions()
		if len(disps) != 1 || disps[0].Disposition.Outcome != client.OutcomeRejected {
			t.Fatalf("the original was settled with %+v, want one `rejected`", disps)
		}
	})

	t.Run("the publication fails, so nothing is settled", func(t *testing.T) {
		peer := newPeer()
		peer.RefuseAttach("DLQ.topic", &amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondNotFound}})
		f := openWith(t, p, aq.Config{}, peer)
		src := f.source(mq.SourceConfig{Prefetch: 2})
		rcv, d := deliverAndFetch(t, f, src, "tag-1", "must-not-vanish")
		err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
		if err == nil {
			t.Fatal("the settlement succeeded while the dead-letter publication was refused: the message has been " +
				"rejected on the source address and written nowhere ")
		}
		if len(rcv.Dispositions()) != 0 {
			t.Fatalf("the original was settled although the copy was never written: %+v", rcv.Dispositions())
		}
		// The delivery is still settleable, which is the whole point: a
		// definite refusal leaves it so.
		if aerr := src.Settler().Ack(context.Background(), &d); aerr != nil {
			t.Fatalf("the delivery could no longer be settled: %v", aerr)
		}
	})

	t.Run("the publication is not accepted, so nothing is settled", func(t *testing.T) {
		peer := newPeer()
		peer.PlanSends(client.SendPlan{State: &amqp.StateRejected{
			Error: &amqp.Error{Condition: amqp.ErrCondResourceLimitExceeded},
		}})
		f := openWith(t, p, aq.Config{}, peer)
		src := f.source(mq.SourceConfig{Prefetch: 2})
		rcv, d := deliverAndFetch(t, f, src, "tag-1", "must-not-vanish")
		err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
		if err == nil {
			t.Fatal("the settlement succeeded while the peer refused the dead-letter copy")
		}
		assertCode(t, err, mq.CodeResourceExhausted, "a dead-letter copy the peer refused")
		if len(rcv.Dispositions()) != 0 {
			t.Fatalf("the original was settled although the copy was refused: %+v", rcv.Dispositions())
		}
	})
}

// TestSettlerDeadLetterLinkFailureKeepsTheDeliverySettleable is a dead-letter
// link that died after it was attached. The original was never settled with the
// broker, so the failure must be a definite, retryable refusal — not a lost
// lease or an unknown outcome, either of which strands the delivery and its
// credit slot — and the dead link must not be reused by the next attempt.
// TestSettlerDeadLetterLinkAsksForThePublishersNodeKind is the dead-letter
// copy on Artemis under queue addressing: the activemq driver puts the
// `queue` capability in Publisher.TargetCapabilities, and a dead-letter link
// that did not ask for it too had its copy routed multicast on an
// auto-created address, with no queue behind it, while the original was
// rejected as if the copy had landed.
func TestSettlerDeadLetterLinkAsksForThePublishersNodeKind(t *testing.T) {
	p := testProfile()
	p.DeadLetterAddress = func(topic, sub string) (string, error) { return "DLQ." + topic, nil }
	cfg := aq.Config{Publisher: aq.PublisherOptions{TargetCapabilities: []string{"queue"}}}
	f := open(t, p, cfg)
	src := f.source(mq.SourceConfig{Prefetch: 2})
	_, d := deliverAndFetch(t, f, src, "tag-1", "poison")
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	var dlq *client.FakeSender
	for _, s := range f.peer.LastConn().Senders() {
		if s.Address() == "DLQ.topic" {
			dlq = s
		}
	}
	if dlq == nil {
		t.Fatal("no sending link attached to the profile's dead-letter address")
	}
	if got := dlq.TargetCapabilities(); !slices.Equal(got, []string{"queue"}) {
		t.Fatalf("the dead-letter link asked for target capabilities %v, want the publisher's [queue]", got)
	}
}

func TestSettlerDeadLetterLinkFailureKeepsTheDeliverySettleable(t *testing.T) {
	for _, mode := range []aq.ReceiverSettleMode{aq.ReceiverSettleFirst, aq.ReceiverSettleSecond} {
		t.Run(mode.String(), func(t *testing.T) {
			p := testProfile()
			p.DeadLetterAddress = func(topic, sub string) (string, error) { return "DLQ." + topic, nil }
			f := open(t, p, aq.Config{ReceiverSettle: mode})
			src := f.source(mq.SourceConfig{Prefetch: 2})
			dl := mq.NackOptions{Action: mq.NackActionDeadLetter}

			// The first dead-letter attaches the link; then the broker takes it away.
			rcv, d0 := deliverAndFetch(t, f, src, "tag-0", "a")
			if err := src.Settler().Nack(context.Background(), &d0, dl); err != nil {
				t.Fatalf("first dead-letter: %v", err)
			}
			dlqs := func() []*client.FakeSender {
				var out []*client.FakeSender
				for _, s := range f.peer.LastConn().Senders() {
					if s.Address() == "DLQ.topic" {
						out = append(out, s)
					}
				}
				return out
			}
			if n := len(dlqs()); n != 1 {
				t.Fatalf("%d dead-letter link(s) attached, want 1", n)
			}
			dlqs()[0].Detach(&amqp.LinkError{})

			rcv.Deliver(message("tag-1", "b"))
			d1 := fetchOne(t, src, budget)
			err := src.Settler().Nack(context.Background(), &d1, dl)
			if err == nil {
				t.Fatal("the dead-letter succeeded over a detached link")
			}
			if c := mq.CodeOf(err); c == mq.CodeLeaseLost || c == mq.CodeSettlementUnknown || mq.IsAmbiguous(err) {
				t.Fatalf("a failed dead-letter copy = %v (code %v, ambiguous %v), want a definite refusal: "+
					"the original was never settled", err, c, mq.IsAmbiguous(err))
			}
			var oe *mq.OpError
			if !asOpError(err, &oe) || !oe.Retryable {
				t.Fatalf("a failed dead-letter copy is not retryable: %v", err)
			}
			if len(rcv.Dispositions()) != 1 {
				t.Fatalf("the original was settled although its copy failed: %+v", rcv.Dispositions())
			}

			// The delivery is still settleable, and settling it frees its slot.
			if aerr := src.Settler().Nack(context.Background(), &d1, mq.NackOptions{Action: mq.NackActionRequeue}); aerr != nil {
				t.Fatalf("the delivery could no longer be settled: %v", aerr)
			}
			if st := src.CreditStats(); st.Held != 0 || st.Unknown != 0 {
				t.Fatalf("credit window after settling = %+v, want nothing held", st)
			}

			// The next dead-letter reattaches rather than reusing the dead link.
			rcv.Deliver(message("tag-2", "c"))
			d2 := fetchOne(t, src, budget)
			if err := src.Settler().Nack(context.Background(), &d2, dl); err != nil {
				t.Fatalf("dead-letter after the link died: %v", err)
			}
			if got := dlqs(); len(got) != 2 || len(got[1].Sent()) != 1 {
				t.Fatalf("the dead-letter did not go out on a fresh link (%d link(s))", len(got))
			}
		})
	}
}

// TestSettlerRefusesANilDelivery keeps the surface total: a settlement that
// names no delivery is a caller's mistake and not a panic.
func TestSettlerRefusesANilDelivery(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	set := f.source(mq.SourceConfig{Prefetch: 2}).Settler()
	assertCode(t, set.Ack(context.Background(), nil), mq.CodeInvalidArgument, "Ack with no delivery")
	assertCode(t, set.Nack(context.Background(), nil, mq.NackOptions{}), mq.CodeInvalidArgument,
		"Nack with no delivery")
	// A delivery a caller built itself names no slot, so it is a lost lease
	// rather than a settlement of whatever occupies slot zero.
	assertIs(t, set.Ack(context.Background(), &mq.DriverDelivery{}), mq.ErrLeaseLost,
		"Ack of a delivery a caller built itself")
}

// contains is strings.Contains under a name the table above reads with.
func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

// indexOf is strings.Index, spelled out so that this file imports no more than
// it needs.
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
