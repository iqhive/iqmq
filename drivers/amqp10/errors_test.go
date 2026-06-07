package amqp10_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/drivers/amqp10/internal/client"
)

// The rule table, driven by the values go-amqp really produces.
//

// linkErr is a link the peer detached with a condition, which is how go-amqp
// reports a refusal that costs the link.
func linkErr(cond amqp.ErrCond) error {
	return &amqp.LinkError{RemoteErr: &amqp.Error{Condition: cond, Description: "the peer said so"}}
}

// connErr is a connection the peer closed with a condition.
func connErr(cond amqp.ErrCond) error {
	return &amqp.ConnError{RemoteErr: &amqp.Error{Condition: cond}}
}

// sessErr is a session the peer ended with a condition.
func sessErr(cond amqp.ErrCond) error {
	return &amqp.SessionError{RemoteErr: &amqp.Error{Condition: cond}}
}

// TestErrorMappingConditions holds every AMQP 1.0 condition this table claims
// to know, in each of the four shapes go-amqp delivers it in.
//
// The four shapes are the point. A condition may arrive as a bare *amqp.Error
// (a disposition's rejection), or wrapped in a *LinkError, a *SessionError or a
// *ConnError (the peer detaching, ending or closing). A table that matched only
// the bare form would classify the same refusal three different ways depending
// on which scope the peer chose to tear down.
func TestErrorMappingConditions(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	for _, tc := range []struct {
		cond      amqp.ErrCond
		code      mq.ErrorCode
		retryable bool
	}{
		{amqp.ErrCondUnauthorizedAccess, mq.CodeUnauthorized, false},
		{amqp.ErrCondNotFound, mq.CodeNotFound, false},
		{amqp.ErrCondResourceDeleted, mq.CodeNotFound, true},
		{amqp.ErrCondPreconditionFailed, mq.CodeConflict, false},
		{amqp.ErrCondResourceLocked, mq.CodeConflict, true},
		{amqp.ErrCondNotAllowed, mq.CodeForbidden, false},
		{amqp.ErrCondNotImplemented, mq.CodeUnsupported, false},
		{amqp.ErrCondIllegalState, mq.CodeConflict, true},
		{amqp.ErrCondMessageSizeExceeded, mq.CodeMessageTooLarge, false},
		{amqp.ErrCondResourceLimitExceeded, mq.CodeResourceExhausted, true},
		{amqp.ErrCondTransferLimitExceeded, mq.CodeResourceExhausted, true},
		{amqp.ErrCondWindowViolation, mq.CodeResourceExhausted, true},
		{amqp.ErrCondDecodeError, mq.CodeCorruptMessage, false},
		{amqp.ErrCondInvalidField, mq.CodeInvalidArgument, false},
		{amqp.ErrCondFramingError, mq.CodeInvalidConfig, false},
		{amqp.ErrCondFrameSizeTooSmall, mq.CodeInvalidConfig, false},
		{amqp.ErrCondStolen, mq.CodeLeaseLost, false},
		{amqp.ErrCondDetachForced, mq.CodeUnavailable, true},
		{amqp.ErrCondConnectionForced, mq.CodeUnavailable, true},
		{amqp.ErrCondLinkRedirect, mq.CodeUnavailable, true},
		{amqp.ErrCondConnectionRedirect, mq.CodeUnavailable, true},
		{amqp.ErrCondErrantLink, mq.CodeUnavailable, true},
		{amqp.ErrCondHandleInUse, mq.CodeUnavailable, true},
		{amqp.ErrCondUnattachedHandle, mq.CodeUnavailable, true},
		{amqp.ErrCondInternalError, mq.CodeUnavailable, true},
	} {
		for shape, build := range map[string]func(amqp.ErrCond) error{
			"bare":    func(c amqp.ErrCond) error { return &amqp.Error{Condition: c} },
			"link":    linkErr,
			"session": sessErr,
			"conn":    connErr,
		} {
			name := string(tc.cond) + "/" + shape
			t.Run(name, func(t *testing.T) {
				native := build(tc.cond)
				got := aq.MapForTest(f.b, mq.OpFetch, native)
				if code := mq.CodeOf(got); code != tc.code {
					t.Fatalf("%s mapped to %v, want %v (%v)", name, code, tc.code, got)
				}
				if mq.IsRetryable(got) != tc.retryable {
					t.Fatalf("%s mapped to retryable=%v, want %v", name, mq.IsRetryable(got), tc.retryable)
				}
				// The specification: "native errors reachable". A mapper that threw the
				// peer's own answer away would leave a caller with a code and
				// no diagnosis.
				var aerr *amqp.Error
				if !errors.As(got, &aerr) || aerr.Condition != tc.cond {
					t.Fatalf("%s lost the peer's own error: %v", name, got)
				}
				// And the driver's name is on it, which is what makes an error
				// from two drivers in one program tell a caller which broker
				// refused.
				var oerr *mq.OpError
				if !errors.As(got, &oerr) || oerr.Driver != "amqp10test" {
					t.Fatalf("%s carries no driver name: %v", name, got)
				}
			})
		}
	}
}

// TestErrorMappingTransportWithNoCondition is the case the condition table
// cannot answer: a link, session or connection that simply closed, with no
// remote error at all. go-amqp's Unwrap returns nil for those, so nothing in the
// condition table matches and the transport rules are what classify them.
func TestErrorMappingTransportWithNoCondition(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	for name, native := range map[string]error{
		"link":    &amqp.LinkError{},
		"session": &amqp.SessionError{},
		"conn":    &amqp.ConnError{},
	} {
		t.Run(name, func(t *testing.T) {
			got := aq.MapForTest(f.b, mq.OpFetch, native)
			assertCode(t, got, mq.CodeUnavailable, name+" with no condition")
			if !mq.IsRetryable(got) {
				t.Fatalf("%s with no condition is not retryable: a reconnection is what answers it", name)
			}
			if !aq.IsTransportGone(native) {
				t.Fatalf("IsTransportGone(%T) = false; a profile's own rules need the same predicate the core uses", native)
			}
		})
	}
}

// TestErrorMappingPublishAmbiguity is the central distinction on this
// protocol.
//
// Once a transfer has been written, a link or connection that dies is a failure
// to HEAR the disposition rather than a failure to write the transfer — so the
// outcome is Ambiguous. The same failure on a FETCH is a plain retry: nothing a
// fetch does can have been accepted in a way a retry would duplicate.
func TestErrorMappingPublishAmbiguity(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	native := &amqp.LinkError{}
	pub := aq.MapForTest(f.b, mq.OpPublish, native)
	if !mq.IsAmbiguous(pub) {
		t.Fatalf("a dead link on a publish is not ambiguous: %v. The transfer had been handed to the sender, so the "+
			"peer may hold the message and the specification makes reporting it as definite a disqualifier", pub)
	}
	fetch := aq.MapForTest(f.b, mq.OpFetch, native)
	if mq.IsAmbiguous(fetch) {
		t.Fatalf("a dead link on a FETCH is ambiguous: %v. Nothing a fetch does can have been accepted in a way a "+
			"retry would duplicate, and reporting one as ambiguous would make a caller reconcile a read", fetch)
	}
}

// TestErrorMappingSentAlreadySurvivesTheRuleTable is the defect,
// checked on this driver's own table.
//
// [bitbucket.org/iqhive/iqmq/driverkit.ErrorMapper.MapNet] computes the transport
// classification FIRST so that a matching rule can refine it but not contradict
// it. The entirely reasonable link rule every driver writes —
// {Match: isTransportGone, Code: CodeUnavailable, Retryable: true} — would
// otherwise throw the sentAlready argument away and defeat the whole call.
func TestErrorMappingSentAlreadySurvivesTheRuleTable(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	for name, native := range map[string]error{
		"a deadline after the write":      context.DeadlineExceeded,
		"a cancellation after the write":  context.Canceled,
		"an EOF after the write":          io.EOF,
		"a closed socket after the write": net.ErrClosed,
	} {
		t.Run(name, func(t *testing.T) {
			sent := aq.MapNetForTest(f.b, mq.OpPublish, native, true)
			if !mq.IsAmbiguous(sent) {
				t.Fatalf("%s is not ambiguous with sentAlready set: %v", name, sent)
			}
			notSent := aq.MapNetForTest(f.b, mq.OpPublish, native, false)
			if mq.IsAmbiguous(notSent) {
				t.Fatalf("%s is ambiguous with sentAlready CLEAR: %v. A failure before the write is a clean failure "+
					"the caller may retry, and reporting it as ambiguous makes every retry a reconciliation",
					name, notSent)
			}
		})
	}
	// A server REPLY that a rule matches is definite however the caller
	// reached it: the peer processed the command and refused it.
	reply := aq.MapNetForTest(f.b, mq.OpPublish, &amqp.Error{Condition: amqp.ErrCondNotAllowed}, true)
	if mq.IsAmbiguous(reply) {
		t.Fatalf("a peer's own refusal is reported as ambiguous with sentAlready set: %v. The peer ANSWERED, so the "+
			"outcome is definite however the request reached it", reply)
	}
}

// TestErrorMappingSettlementDependsOnTheSettleMode is the mirror image of the
// publish path, and the one place a configuration changes a classification.
//
// Under ReceiverSettleFirst a disposition IS the settlement: a write that failed
// means the peer never got it, the delivery is still the node's and it will be
// redelivered — a lost lease, which tells the caller the message is coming back.
// Under ReceiverSettleSecond the peer confirms, so a failure may be a settlement
// the peer took and a confirmation nobody heard — an unknown outcome, sticky for
// ever.
func TestErrorMappingSettlementDependsOnTheSettleMode(t *testing.T) {
	first := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleFirst})
	second := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleSecond})
	native := &amqp.LinkError{}
	for _, op := range []string{mq.OpAck, mq.OpNack} {
		got := aq.MapForTest(first.b, op, native)
		assertCode(t, got, mq.CodeLeaseLost, "a dead link on "+op+" under receiversettle=first")
		got = aq.MapForTest(second.b, op, native)
		assertCode(t, got, mq.CodeSettlementUnknown, "a dead link on "+op+" under receiversettle=second")
		if !mq.IsAmbiguous(got) {
			t.Fatalf("a dead link on %s under receiversettle=second is not ambiguous: %v", op, got)
		}
	}
}

// TestErrorMappingOutcomes holds the three non-accepting terminal outcomes,
// which are three different answers rather than one.
func TestErrorMappingOutcomes(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	for _, tc := range []struct {
		name      string
		state     amqp.DeliveryState
		code      mq.ErrorCode
		retryable bool
		ambiguous bool
	}{
		{"released", &amqp.StateReleased{}, mq.CodeUnavailable, true, false},
		{"modified", &amqp.StateModified{}, mq.CodeUnsupported, false, false},
		{"received", &amqp.StateReceived{}, mq.CodeSettlementUnknown, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := publishWithState(t, tc.state)
			if err == nil {
				t.Fatalf("a %s outcome was reported as a successful publication", tc.name)
			}
			assertCode(t, err, tc.code, "a "+tc.name+" outcome")
			// The retryable column is the DRIVER's judgement and lives on the
			// *OpError; the predicate is that judgement after the specification's
			// precedence, where ambiguity wins. Both are
			// asserted, and they have to be: the `received` row is ambiguous,
			// so reading the column through mq.IsRetryable alone could no
			// longer fail -- a rule that marked this outcome retryable would
			// still be reported not retryable by the predicate, and the row
			// would pass. Review round 1 of this branch found that and
			// reproduced it by adding `Retryable: true` to the received rule
			// in errors.go, which left the whole module green.
			var oe *mq.OpError
			if !errors.As(err, &oe) {
				t.Fatalf("a %s outcome is not an *mq.OpError: %v", tc.name, err)
			}
			if oe.Retryable != tc.retryable {
				t.Fatalf("a %s outcome is MARKED retryable=%v, want %v", tc.name, oe.Retryable, tc.retryable)
			}
			if got, want := mq.IsRetryable(err), tc.retryable && !tc.ambiguous; got != want {
				t.Fatalf("a %s outcome reports IsRetryable=%v, want %v (flag %v, ambiguous %v)",
					tc.name, got, want, tc.retryable, tc.ambiguous)
			}
			if mq.IsAmbiguous(err) != tc.ambiguous {
				t.Fatalf("a %s outcome is ambiguous=%v, want %v", tc.name, mq.IsAmbiguous(err), tc.ambiguous)
			}
			var oerr *aq.OutcomeError
			if !errors.As(err, &oerr) || oerr.Outcome != tc.name {
				t.Fatalf("a %s outcome does not carry its own name: %v", tc.name, err)
			}
		})
	}
	_ = f
}

// publishWithState publishes one message to a peer that answers with the
// delivery state given, and returns the publication's error.
func publishWithState(tb testing.TB, state amqp.DeliveryState) error {
	tb.Helper()
	peer := client.NewFake(testFacts())
	peer.PlanSends(client.SendPlan{State: state})
	f := openWith(tb, testProfile(), aq.Config{}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	return snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
}

// TestErrorMappingUnclassifiedIsNotRetryable is the honest default: "an
// error that carries no judgement is not retryable".
func TestErrorMappingUnclassifiedIsNotRetryable(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	got := aq.MapForTest(f.b, mq.OpFetch, errors.New("something nobody classified"))
	assertCode(t, got, mq.CodeUnknown, "an unclassified error")
	if mq.IsRetryable(got) {
		t.Fatal("an unclassified error is retryable; the specification makes retryability something a driver has to judge")
	}
}

// TestConditionIsReachable holds the exported accessor, because the condition is
// the only thing an AMQP 1.0 peer really says about a refusal and a caller
// should not have to know which of three wrappers it arrived in.
func TestConditionIsReachable(t *testing.T) {
	for name, native := range map[string]error{
		"bare": &amqp.Error{Condition: amqp.ErrCondNotFound},
		"link": linkErr(amqp.ErrCondNotFound),
		"conn": connErr(amqp.ErrCondNotFound),
	} {
		got, ok := aq.Condition(native)
		if !ok || got != string(amqp.ErrCondNotFound) {
			t.Errorf("Condition(%s) = (%q, %v), want (amqp:not-found, true)", name, got, ok)
		}
	}
	if _, ok := aq.Condition(&amqp.LinkError{}); ok {
		t.Error("Condition reported a condition for a link that closed with none")
	}
	if _, ok := aq.Condition(nil); ok {
		t.Error("Condition reported a condition for a nil error")
	}
}

// TestProfileErrorRulesComeFirst is the seam's half of the error table: a
// broker-specific rule is the SPECIFIC one and must shadow the core's general
// answer, not the other way round.
//
// The case is a real one. Both brokers this core was written for answer some
// refusals with `amqp:internal-error` and a description only they can
// interpret, which the core maps to a retryable CodeUnavailable because that is
// the honest general answer. A profile that knows better has to be able to say
// so, and a table that appended the profile's rules would leave the core's
// general answer shadowing the specific one.
func TestProfileErrorRulesComeFirst(t *testing.T) {
	p := testProfile()
	p.ErrorRules = func() map[string][]driverkit.Rule {
		return map[string][]driverkit.Rule{
			mq.OpFetch: {{
				Match: func(err error) bool {
					c, ok := aq.Condition(err)
					return ok && c == string(amqp.ErrCondInternalError)
				},
				Code: mq.CodeForbidden,
			}},
		}
	}
	f := open(t, p, aq.Config{})
	native := linkErr(amqp.ErrCondInternalError)
	// The profile's rule wins on the operation it names.
	assertCode(t, aq.MapForTest(f.b, mq.OpFetch, native), mq.CodeForbidden,
		"a condition the profile claims, on the operation it claims it for")
	// And the core's answer stands everywhere the profile said nothing, which
	// is what keeps a profile from having to restate the whole table.
	assertCode(t, aq.MapForTest(f.b, mq.OpPublish, native), mq.CodeUnavailable,
		"the same condition on an operation the profile said nothing about")
}

// TestAPeerThatDoesNotSpeakAMQP10IsUnsupported is the one direction of the specification's
// "never silently substituted" that a profile's version gate cannot reach.
//
// A broker whose AMQP 1.0 support is absent answers the protocol HEADER with
// another protocol's, so the handshake fails before any OPEN frame — and a
// version gate reads `product` and `version` out of an OPEN frame. There is no
// `*amqp.Error` and no transport wrapper either: go-amqp's own error is
// unexported and unwrapped, which is why this is the one rule in the table
// that matches on a string.
//
// `the capability matrix` found it against
// rabbitmq:3.13-management-alpine, whose image does not load the AMQP 1.0
// plugin, verbatim:
//
//	the AMQP 1.0 profile was refused by server 3.13.7 with rabbitmq: open:
//	Unknown: unexpected protocol version 0.9.1, want ErrUnsupported: a caller
//	distinguishes "this broker is too old" from "the dial failed"
//
// CodeUnknown is what a caller cannot act on.
func TestAPeerThatDoesNotSpeakAMQP10IsUnsupported(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	// The library's text, as go-amqp really renders it for a peer that
	// answered with the AMQP 0-9-1 header.
	raw := errors.New("unexpected protocol version 0.9.1")
	got := aq.MapForTest(f.b, mq.OpOpen, raw)
	if code := mq.CodeOf(got); code != mq.CodeUnsupported {
		t.Fatalf("a peer that does not speak AMQP 1.0 maps to %v, want %v: %v", code, mq.CodeUnsupported, got)
	}
	if !errors.Is(got, mq.ErrUnsupported) {
		t.Errorf("the refusal does not wrap ErrUnsupported, so a caller's errors.Is misses it: %v", got)
	}
	var oe *mq.OpError
	if errors.As(got, &oe) && oe.Retryable {
		t.Error("the refusal is retryable; a broker that cannot speak the protocol will not start speaking it")
	}
}
