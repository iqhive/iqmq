package confirm

import (
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzRabbitConfirm drives a [Tracker] with an operation script and holds it to
// the invariants a publisher's correctness rests on.
//
// The model it is compared against is a plain map of tag to state, maintained
// beside the tracker with no cumulative-confirm cleverness at all — which is
// the point: the tracker's whole difficulty is the `multiple` flag, the
// out-of-order arrival and the return-before-ack sequence, and the oracle that
// catches a mistake in any of them is one that applies the protocol's rules
// the slow, obvious way.
//
// The four invariants, each of which is a real failure mode:
//
//  1. **Exactly one terminal outcome per publication.** A publication resolved
//     twice is a caller told two different things about one message; one never
//     resolved is a leaked window slot, and the symptom is a publisher that
//     slowly stops publishing.
//  2. **A cumulative confirm resolves exactly the tags at or below it that
//     were still outstanding**, and nothing above it. Resolving one tag too
//     many acknowledges a publication the broker has not confirmed.
//  3. **A returned publication resolves as Rejected, never Confirmed.** the specification's
//     "Mandatory + NotifyReturn = Rejected": the broker took the message and
//     threw it away, and reporting that as a success is the most expensive
//     mistake here.
//  4. **A detached tracker resolves everything as Ambiguous and then nothing
//     at all.** a close may have been preceded by the broker
//     taking every one of those publications, and a late confirm frame names
//     tags that belong to the next channel.
func FuzzRabbitConfirm(f *testing.F) {
	seed := func(steps ...string) {
		w := fuzzutil.NewWriter()
		for _, s := range steps {
			script.WriteStep(f, w, s)
		}
		f.Add(w.Data())
	}
	seed("submit", "ack")
	seed("submit", "submit", "submit", "ackmultiple")
	seed("submit", "returned", "ack")
	seed("submit", "submit", "returned", "ackmultiple")
	seed("submit", "nack")
	seed("submit", "submit", "nackmultiple")
	seed("submit", "detach", "ack", "submit")
	seed("submit", "ackstale")
	seed("submit", "submit", "ackstale", "ackmultiple")
	seed("submit", "returnedonly", "ack")
	seed("submit", "submit", "returnedonly", "ackmultiple")
	seed("detach", "detach")

	f.Fuzz(func(t *testing.T, data []byte) {
		sut := &confirmSUT{
			tr:       NewTracker(8),
			state:    map[uint64]Result{},
			returned: map[uint64]bool{},
			tokens:   map[string]uint64{},
			seen:     map[uint64]int{},
		}
		script.Run(t, data, sut)
		// The invariants run after every step through Script.Check; this is
		// the destructive half, which has to come last: closing the channel
		// resolves whatever is left, and it resolves it ambiguously.
		sut.checkClose(t)
	})
}

// confirmSUT is the tracker under test beside the plain model of it.
type confirmSUT struct {
	tr *Tracker
	// state is the model: every tag ever issued, and what the protocol's
	// rules say its outcome is.
	state map[uint64]Result
	// returned records which tags came back through `basic.return`.
	returned map[uint64]bool
	// tokens maps a correlation token to its tag, so that a return can be
	// attributed the way the driver does it.
	tokens map[string]uint64
	// live is the outstanding tags in issue order, which is the model's own
	// copy of what a cumulative confirm has to resolve.
	live     []uint64
	next     uint64
	detached bool
	// seen counts terminal outcomes per tag, which is invariant (1).
	seen map[uint64]int
}

// script is the operation script both directions are driven by.
var script = fuzzutil.Script[*confirmSUT]{
	MaxSteps: 96,
	Steps: []fuzzutil.Step[*confirmSUT]{
		{Name: "submit", Weight: 6, Run: opSubmit},
		{Name: "ack", Weight: 4, Run: opAck},
		{Name: "ackmultiple", Weight: 3, Run: opAckMultiple},
		{Name: "nack", Weight: 2, Run: opNack},
		{Name: "nackmultiple", Weight: 1, Run: opNackMultiple},
		{Name: "returned", Weight: 2, Run: opReturned},
		{Name: "returnedonly", Weight: 1, Run: opReturnedOnly},
		{Name: "ackstale", Weight: 1, Run: opAckStale},
		{Name: "detach", Weight: 1, Run: opDetach},
	},
	Check: func(t testing.TB, s *confirmSUT) { s.invariants(t) },
}

// opSubmit publishes, with a correlation token derived from the tag so that a
// return can find it.
func opSubmit(t testing.TB, r *fuzzutil.Reader, s *confirmSUT) {
	token := ""
	if r.Bool() {
		token = tokenOf(int(s.next))
	}
	tag, err := s.tr.Submit(token)
	switch {
	case s.detached:
		if err == nil {
			t.Fatalf("a detached tracker issued tag %d", tag)
		}
		return
	case err != nil:
		// The only other refusal is a duplicate token, and the tokens are
		// derived from a counter, so it cannot happen. If it does, the
		// tracker is releasing tokens it should keep or vice versa.
		t.Fatalf("Submit refused a publication under token %q: %v", token, err)
	}
	s.next++
	if tag != s.next {
		t.Fatalf("Submit returned tag %d, want %d: delivery tags start at 1 and are consecutive", tag, s.next)
	}
	s.state[tag] = Pending
	s.live = append(s.live, tag)
	if token != "" {
		s.tokens[token] = tag
	}
}

// opAck confirms one outstanding tag, chosen from the model's own live set so
// that the common case is exercised rather than only the stale one.
func opAck(t testing.TB, r *fuzzutil.Reader, s *confirmSUT) {
	tag, ok := s.pick(r)
	if !ok {
		return
	}
	s.expect(t, s.tr.Ack(tag, false), s.resolveOne(tag, Confirmed, false))
}

// opAckMultiple confirms cumulatively, which is the protocol's `multiple=true`
// and the tracker's hardest path.
func opAckMultiple(t testing.TB, r *fuzzutil.Reader, s *confirmSUT) {
	tag, ok := s.pick(r)
	if !ok {
		return
	}
	s.expect(t, s.tr.Ack(tag, true), s.resolveUpTo(tag, Confirmed, false))
}

// opNack is the definite negative.
func opNack(t testing.TB, r *fuzzutil.Reader, s *confirmSUT) {
	tag, ok := s.pick(r)
	if !ok {
		return
	}
	requeue := r.Bool()
	s.expect(t, s.tr.Nack(tag, false, requeue), s.resolveOne(tag, Rejected, requeue))
}

// opNackMultiple is the cumulative negative.
func opNackMultiple(t testing.TB, r *fuzzutil.Reader, s *confirmSUT) {
	tag, ok := s.pick(r)
	if !ok {
		return
	}
	requeue := r.Bool()
	s.expect(t, s.tr.Nack(tag, true, requeue), s.resolveUpTo(tag, Rejected, requeue))
}

// opReturned is `basic.return` attributed by the correlation token.
func opReturned(t testing.TB, r *fuzzutil.Reader, s *confirmSUT) {
	if len(s.live) == 0 {
		return
	}
	tag := s.live[r.Choice(len(s.live))]
	token := s.tokenFor(tag)
	got := s.tr.Returned(token)
	want := !s.detached && token != ""
	if got != want {
		t.Fatalf("Returned(%q) = %v, want %v (detached=%v)", token, got, want, s.detached)
	}
	if want {
		s.returned[tag] = true
	}
}

// opReturnedOnly is `basic.return` attributed by there being exactly one
// candidate, which is the reference kind's route.
func opReturnedOnly(t testing.TB, _ *fuzzutil.Reader, s *confirmSUT) {
	got := s.tr.ReturnedOnly()
	want := !s.detached && len(s.live) == 1
	if got != want {
		t.Fatalf("ReturnedOnly = %v with %d outstanding (detached=%v), want %v", got, len(s.live), s.detached, want)
	}
	if want {
		s.returned[s.live[0]] = true
	}
}

// opAckStale is a confirm frame naming a tag this tracker never issued, which
// is what a frame still in the socket when the channel died looks like.
func opAckStale(t testing.TB, r *fuzzutil.Reader, s *confirmSUT) {
	tag := s.next + uint64(r.Int(1, 1000))
	if out := s.tr.Ack(tag, r.Bool()); len(out) != 0 {
		t.Fatalf("the tracker applied a confirm for tag %d, above the %d it has issued, and resolved %d publication(s)", tag, s.next, len(out))
	}
}

// opDetach closes the channel.
func opDetach(t testing.TB, _ *fuzzutil.Reader, s *confirmSUT) {
	out := s.tr.Detach()
	if s.detached {
		if len(out) != 0 {
			t.Fatalf("a second Detach resolved %d publication(s)", len(out))
		}
		return
	}
	want := make([]Outcome, 0, len(s.live))
	for _, tag := range s.live {
		want = append(want, Outcome{Tag: tag, Token: s.tokenFor(tag), Result: Ambiguous, Returned: s.returned[tag]})
	}
	s.detached = true
	s.live = s.live[:0]
	for _, o := range want {
		s.state[o.Tag] = Ambiguous
	}
	s.compare(t, "detach", out, want)
}

// pick chooses an outstanding tag from the model's live set. It reports false
// when there is nothing outstanding, which is the step doing nothing rather
// than the script failing: a confirm frame for a channel with no publications
// in flight is a frame a broker does not send.
func (s *confirmSUT) pick(r *fuzzutil.Reader) (uint64, bool) {
	if s.detached || len(s.live) == 0 {
		return 0, false
	}
	return s.live[r.Choice(len(s.live))], true
}

// resolveOne is the model's answer for a single confirm.
func (s *confirmSUT) resolveOne(tag uint64, result Result, requeue bool) []Outcome {
	for i, live := range s.live {
		if live != tag {
			continue
		}
		s.live = append(s.live[:i], s.live[i+1:]...)
		return []Outcome{s.outcome(tag, result, requeue)}
	}
	return nil
}

// resolveUpTo is the model's answer for a cumulative confirm: this tag and
// every lower one that is still outstanding, in issue order, and nothing
// above it.
func (s *confirmSUT) resolveUpTo(tag uint64, result Result, requeue bool) []Outcome {
	var out []Outcome
	kept := s.live[:0]
	for _, live := range s.live {
		if live <= tag {
			out = append(out, s.outcome(live, result, requeue))
			continue
		}
		kept = append(kept, live)
	}
	s.live = kept
	return out
}

// outcome builds the model's expected outcome for one tag, applying the
// return-beats-ack rule.
func (s *confirmSUT) outcome(tag uint64, result Result, requeue bool) Outcome {
	token := s.tokenFor(tag)
	if s.returned[tag] {
		result, requeue = Rejected, false
	}
	s.state[tag] = result
	return Outcome{Tag: tag, Token: token, Result: result, Requeue: requeue, Returned: s.returned[tag]}
}

// tokenFor is the correlation token a tag was submitted with, or the empty
// string when it was submitted without one. The model keeps the index the
// other way round because that is the direction a `basic.return` is resolved
// in, so this is a scan; the scripts are short enough that it costs nothing.
func (s *confirmSUT) tokenFor(tag uint64) string {
	for tok, tg := range s.tokens {
		if tg == tag {
			return tok
		}
	}
	return ""
}

// expect compares the tracker's answer with the model's and counts the
// terminal outcomes.
func (s *confirmSUT) expect(t testing.TB, got, want []Outcome) {
	s.compare(t, "confirm", got, want)
}

// compare is the comparison both call sites share.
func (s *confirmSUT) compare(t testing.TB, what string, got, want []Outcome) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s resolved %d publications, the model says %d (%v against %v)", what, len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s outcome %d is %+v, the model says %+v", what, i, got[i], want[i])
		}
		s.seen[got[i].Tag]++
		if s.seen[got[i].Tag] > 1 {
			t.Fatalf("tag %d was resolved %d times: a publication has exactly one outcome", got[i].Tag, s.seen[got[i].Tag])
		}
	}
}

// invariants is the reconciliation that runs after every step. It reads the
// tracker and changes nothing.
func (s *confirmSUT) invariants(t testing.TB) {
	t.Helper()
	if got := s.tr.Outstanding(); got != len(s.live) {
		t.Fatalf("the tracker holds %d outstanding publications and the model holds %d", got, len(s.live))
	}
	if got := s.tr.Detached(); got != s.detached {
		t.Fatalf("the tracker reports detached=%v and the model says %v", got, s.detached)
	}
	// Every tag ever issued has either resolved exactly once or is still
	// outstanding. Nothing is in neither set, which is the leak.
	live := map[uint64]bool{}
	for _, tag := range s.live {
		live[tag] = true
	}
	for tag, state := range s.state {
		switch {
		case state.Terminal():
			if s.seen[tag] != 1 {
				t.Fatalf("tag %d is terminal in the model but was resolved %d times", tag, s.seen[tag])
			}
			if live[tag] {
				t.Fatalf("tag %d is both resolved and outstanding", tag)
			}
			if s.returned[tag] && state == Confirmed {
				t.Fatalf("tag %d came back through basic.return and resolved as Confirmed: the broker took it and threw it away (the driver documentation)", tag)
			}
		case !live[tag]:
			t.Fatalf("tag %d is neither resolved nor outstanding: its window slot is leaked", tag)
		}
	}
}

// checkClose is the destructive half of the reconciliation: closing the
// channel resolves whatever is left, it resolves it ambiguously, and a confirm
// that arrives afterwards resolves nothing at all.
func (s *confirmSUT) checkClose(t testing.TB) {
	t.Helper()
	rest := s.tr.Detach()
	if !s.detached && len(rest) != len(s.live) {
		t.Fatalf("the final Detach resolved %d of %d outstanding publications", len(rest), len(s.live))
	}
	for _, o := range rest {
		if o.Result != Ambiguous {
			t.Fatalf("the close resolved tag %d as %v, want %v", o.Tag, o.Result, Ambiguous)
		}
	}
	// The staleness guard: the tags this tracker issued belong to a channel
	// that no longer exists, and a frame still in the socket must not
	// resolve the publication that takes one of them on the next channel.
	if out := s.tr.Ack(s.next, true); len(out) != 0 {
		t.Fatalf("a detached tracker applied a cumulative confirm and resolved %d publication(s)", len(out))
	}
	if _, err := s.tr.Submit(""); err == nil {
		t.Fatal("a detached tracker accepted a publication")
	}
}
