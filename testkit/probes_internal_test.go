package testkit

import (
	"context"
	"errors"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestEveryProbeSaysWhereARefusalWouldCome holds the probe table to the one
// decision this increment makes it take.
//
// `capability/<F>/refused` reads "the driver says it cannot do this; prove it
// refuses", and it can only be evidence about F if the kit knows WHICH of the
// probe's five or six portable calls is F's. Before this, it knew none of
// them: any ErrUnsupported from anywhere in the probe was read as the refusal,
// so a driver whose consumer open answered ErrUnsupported had every negative
// claim in the suite recorded as refuted by observation.
//
// A probe therefore names the call (refusal) or says the feature has none
// (noRefusal), and a probe that says neither is one nobody decided about.
// There is no default, on purpose: a new probe added without the decision
// fails here rather than silently reporting a refusal it never located.
func TestEveryProbeSaysWhereARefusalWouldCome(t *testing.T) {
	probes := exercises()
	if len(probes) < 25 {
		t.Fatalf("the probe table holds %d entries; that is fewer than the inventory has probes, so this test is checking a table that is not the suite's", len(probes))
	}
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		ex, ok := probes[f]
		if !ok {
			continue
		}
		switch {
		case ex.refusal == "" && ex.noRefusal == "":
			t.Errorf("%v's probe says neither which call a driver without the feature has to refuse nor that there is none: the negative direction cannot tell a refusal OF THE FEATURE from a refusal of something the probe did on the way past", f)
		case ex.refusal != "" && ex.noRefusal != "":
			t.Errorf("%v's probe names a refusal point (%q) AND says the feature has none (%q); exactly one of the two is true", f, ex.refusal, ex.noRefusal)
		case ex.noRefusal != "" && len(ex.noRefusal) < 40:
			t.Errorf("%v's probe says the feature has no call to refuse with the reason %q, which is too short to be one: the reason is published in the report as why the denial can only ever be unrefuted", f, ex.noRefusal)
		case ex.refusal != "" && !strings.ContainsAny(ex.refusal, "."):
			t.Errorf("%v's probe names its refusal point as %q; it has to name the portable call in the API's own spelling, because the name is what a reader of the report matches against the surface they called", f, ex.refusal)
		}
	}
}

// TestAMarkedRefusalIsNotConfusedWithAnythingElse pins refusedAt's one rule:
// it marks a plain error as coming from the feature's own call and passes every
// already-classified observation through untouched.
//
// A skip, a violation and a divergence are each a decided answer. Wrapping one
// in a refusal would claim it came from a refusal, and the classifier reads
// refusals first — so a divergence wrapped as a refusal would be reported as
// "the driver answered the call with something that is not a refusal", which is
// a FAILURE for a driver that did nothing wrong.
func TestAMarkedRefusalIsNotConfusedWithAnythingElse(t *testing.T) {
	if got := refusedAt("Delivery.Ack", nil); got != nil {
		t.Errorf("refusedAt marked a nil answer as a refusal: %v", got)
	}
	for _, tc := range []struct {
		what string
		err  error
	}{
		{"a skip", Skip("the harness cannot build this")},
		{"a violation", violation("the order was %v", "reversed")},
		{"a divergence", Diverged("the call was accepted and nothing arrived")},
	} {
		// IDENTITY is the claim, not equality: refusedAt has to hand
		// back the very error it was given. errors.Is would pass for
		// the wrapping this asserts against -- a refusalError unwraps
		// to its inner error -- so the comparison is the check.
		//nolint:errorlint // see above: errors.Is cannot express "the same error, unwrapped"
		if got := refusedAt("Delivery.Ack", tc.err); got != tc.err {
			t.Errorf("refusedAt rewrote %s into %v; it is already a classified observation", tc.what, got)
		}
		if _, is := isRefusal(refusedAt("Delivery.Ack", tc.err)); is {
			t.Errorf("%s reads as a refusal after marking, so the classifier would report it against the refusal contract", tc.what)
		}
	}
	marked := refusedAt("Delivery.Ack", mq.ErrUnsupported)
	ref, is := isRefusal(marked)
	if !is {
		t.Fatalf("a plain error was not marked: %v", marked)
	}
	if ref.at != "Delivery.Ack" {
		t.Errorf("the mark names %q", ref.at)
	}
	if !strings.Contains(marked.Error(), "Delivery.Ack") {
		t.Errorf("the rendered error is %q and does not name the call", marked.Error())
	}
	if _, isDiv := isDivergence(marked); isDiv {
		t.Error("a marked refusal reads as a divergence")
	}
}

// TestTheFiveAnswersOfANegativeBehaviourProbe is refusedBehaviour's whole
// table, driven directly.
//
// The self-tests in the testkit self-tests reach these five answers through
// a real driver with an injected defect, which is what proves the probes
// produce them. This is the classifier itself, so that a sixth answer — or a
// reshuffle of the five — cannot be added without a decision: the two that
// used to be one are the last two rows, and they are the defect.
func TestTheFiveAnswersOfANegativeBehaviourProbe(t *testing.T) {
	const call = "Delivery.Nack with a NackDelay"
	behaviour := exercise{refusal: call}
	noCall := exercise{noRefusal: "the difference shows up only in what arrives and there is nothing to refuse"}
	for _, tc := range []struct {
		name    string
		ex      exercise
		err     error
		want    Outcome
		wantOne bool // the result's evidence is one-sided
		says    string
	}{
		{
			name: "the feature worked and is not claimed",
			ex:   behaviour, err: nil, want: OutcomeFail,
			says: "the feature worked anyway",
		},
		{
			name: "the feature's own call was refused",
			ex:   behaviour, err: refusedAt(call, mq.ErrUnsupported), want: OutcomePass,
			says: "the denial is proven by a refusal of " + call,
		},
		{
			name: "a settlement on a source with no settler",
			ex:   behaviour, err: refusedAt(call, mq.ErrNotSettleable), want: OutcomePass,
			says: "the denial is proven by a refusal of " + call,
		},
		{
			name: "the probe could not run",
			ex:   behaviour, err: Skip("the harness cannot build this topology"), want: OutcomeSkip,
			says: "the harness cannot build this topology",
		},
		{
			name: "the call was accepted and the driver did something else",
			ex:   noCall, err: Diverged("both names are one subscription, so the readers compete"), want: OutcomeSkip,
			wantOne: true,
			says:    "did not refuse",
		},
		{
			name: "the feature's own call answered something that is not a refusal",
			ex:   behaviour, err: refusedAt(call, &mq.OpError{Operation: "nack", Code: mq.CodeUnavailable}), want: OutcomeFail,
			says: "neither the feature working nor a refusal",
		},
		{
			name: "a refusal from a step that is not the feature's call",
			ex:   behaviour, err: mq.ErrUnsupported, want: OutcomeSkip,
			says: "did not reach the call",
		},
		{
			name: "an unrelated failure with the call never named",
			ex:   behaviour, err: failf("the connection went away"), want: OutcomeFail,
			says: "neither refused " + call,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{}
			got := refusedBehaviour(e, mq.FeatureDelayedRedelivery, tc.ex, tc.err)
			var out Outcome
			_, skipped := IsSkip(got)
			switch {
			case got == nil:
				out = OutcomePass
			case skipped:
				out = OutcomeSkip
			default:
				out = OutcomeFail
			}
			if out != tc.want {
				t.Fatalf("outcome %q, want %q: %v", out, tc.want, got)
			}
			note := withNotes(render(got), e.notes)
			if !strings.Contains(note, tc.says) {
				t.Errorf("the answer reads %q, want it to say %q", note, tc.says)
			}
			if e.oneSided != tc.wantOne {
				t.Errorf("one-sided evidence is %v, want %v: the report's kind is what tells a reader whether one run could settle this", e.oneSided, tc.wantOne)
			}
		})
	}
}

// TestAnEmptyReceiveIsNotABrokenOne pins the distinction every divergence
// rests on.
//
// A capability probe reports "the driver accepted the call and did something
// else" from what it saw arrive, and the two things it can see — nothing, and
// something that should not have come — are both also what a BROKEN receive
// looks like from one line up. A probe that read every failed receive as the
// broker's shape would report a lost message as an unrefuted denial, which is
// the same class of mistake as the defect it is part of fixing.
func TestAnEmptyReceiveIsNotABrokenOne(t *testing.T) {
	empty := &noDeliveryError{inner: context.DeadlineExceeded}
	arrived := &arrivedError{what: "a delivery arrived when none should have"}
	broken := failf("Receive reported %v, want the deadline", mq.ErrClosed)

	if !isNoDelivery(empty) || isNoDelivery(broken) || isNoDelivery(arrived) {
		t.Error("isNoDelivery does not separate an empty receive from a broken one")
	}
	if !isUnexpectedArrival(arrived) || isUnexpectedArrival(broken) || isUnexpectedArrival(empty) {
		t.Error("isUnexpectedArrival does not separate an unwanted delivery from a broken receive")
	}
	// And neither is mistaken for one of the classified answers.
	for _, err := range []error{empty, arrived} {
		if _, is := isRefusal(err); is {
			t.Errorf("%v reads as a refusal", err)
		}
		if _, is := isDivergence(err); is {
			t.Errorf("%v reads as a divergence; a probe has to NAME the observation, because the same empty receive is also a lost message", err)
		}
		if _, is := IsSkip(err); is {
			t.Errorf("%v reads as a skip", err)
		}
	}
	if !errors.Is(empty, context.DeadlineExceeded) {
		t.Error("the empty receive lost the error the receive reported")
	}
}
