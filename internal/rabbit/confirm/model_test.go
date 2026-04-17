package confirm

import "testing"

// TestModel runs every kind through the reference simulation.
//
// The three kinds do not agree on an answer, so there is no single oracle to
// compare them against. What the model does instead is hold each of them to
// the claims its own documentation makes — the window it promises to respect,
// the number of times it promises to block, and how much a channel close can
// leave ambiguous — plus the invariants every kind owes whatever it costs:
// every publication gets exactly one terminal outcome, no publication is
// resolved twice, and nothing is ever reported Confirmed that the broker did
// not confirm.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("batch=1", func(t *testing.T) { modelBatch(t, k, 1) })
			t.Run("batch=16", func(t *testing.T) { modelBatch(t, k, 16) })
			t.Run("batch=128", func(t *testing.T) { modelBatch(t, k, 128) })
			t.Run("cumulative-confirms", func(t *testing.T) { modelCumulative(t, k) })
			t.Run("close-is-ambiguous", func(t *testing.T) { modelClose(t, k) })
			t.Run("mandatory-return-is-rejected", func(t *testing.T) { modelReturn(t, k) })
			t.Run("window-is-respected", func(t *testing.T) { modelWindow(t, k) })
		})
	}
}

// modelPublications is how many messages a model run publishes. It is large
// enough that the window fills several times over and small enough that the
// whole model is milliseconds.
const modelPublications = 512

// modelBatch drives one kind at one batch width and checks the shared
// invariants and the kind's own cost claim.
func modelBatch(t *testing.T, k Kind, batch int) {
	s := newSim(t, k, simConfig{
		Publications: modelPublications,
		Batch:        batch,
		PublishCost:  1,
		RTT:          1000,
		AckEvery:     1,
	})
	s.run()

	// The shared invariants. A kind that loses or duplicates a publication
	// is caught here whatever it costs.
	if s.resolved != modelPublications {
		t.Fatalf("%d of %d publications were resolved: every publication has exactly one outcome", s.resolved, modelPublications)
	}
	if s.confirmed != modelPublications {
		t.Fatalf("%d of %d publications were confirmed, %d rejected and %d ambiguous: the broker acked every one of them",
			s.confirmed, modelPublications, s.rejected, s.ambiguous)
	}
	if out := s.tr.Outstanding(); out != 0 {
		t.Fatalf("%d publication(s) were still outstanding after the flush", out)
	}

	// The kind's own cost claim, as an upper bound. The bound is PER BATCH,
	// because a blocking strategy owes its caller an outcome at every batch
	// boundary: a windowed strategy publishing one message at a time is a
	// per-message strategy, which is a fact about the workload and not a
	// defect in the kind.
	if bound := MaxStalls(s.st, modelPublications, batch); s.stalls > bound {
		t.Fatalf("the %v strategy blocked %d times publishing %d messages in batches of %d, over its own bound of %d",
			k, s.stalls, modelPublications, batch, bound)
	}

	// And the one exact claim, which is what separates the reference kind
	// from the other two: a per-message strategy waits for every single
	// publication. A bound alone would let a kind that never blocked pass as
	// "per-message".
	if k.Resolve() == PerMessage && s.stalls != modelPublications {
		t.Fatalf("the per-message strategy blocked %d times publishing %d messages: it owes one wait per publication",
			s.stalls, modelPublications)
	}

	// The throughput claim the criterion is read at. A per-message
	// strategy pays a round trip per message; a pipelined one pays one to
	// fill the pipeline, so its elapsed time is far below the round trip
	// times the message count. The threshold is a quarter of the serial
	// cost, which is loose enough not to be a performance assertion and
	// tight enough that a windowed strategy that had silently become
	// per-message would fail it.
	serial := int64(modelPublications) * (1 + 1000)
	switch {
	case k.Resolve() == PerMessage:
		if s.elapsed() < serial {
			t.Fatalf("the per-message strategy finished in %d virtual microseconds, under the %d a round trip per message costs: it cannot have waited for each",
				s.elapsed(), serial)
		}
	case batch > 1:
		if s.elapsed() > serial/4 {
			t.Fatalf("the %v strategy took %d virtual microseconds for %d publications at batch=%d, which is not far enough below the per-message cost of %d: the pipeline is not filling",
				k, s.elapsed(), modelPublications, batch, serial)
		}
	}
}

// modelCumulative drives the same workload with the broker coalescing confirms,
// which is the `multiple=true` form.
//
// It is a separate subtest because it exercises the part of the tracker that is
// easiest to get wrong and hardest to notice: a driver that resolved only the
// named tag of a cumulative ack would leak every other publication in the
// group, and the symptom is not an error — it is a publisher that slowly stops
// publishing because its window never drains, and then reports a window's
// worth of ambiguous outcomes when the channel closes.
func modelCumulative(t *testing.T, k Kind) {
	for _, every := range []int{1, 8, 64} {
		s := newSim(t, k, simConfig{
			Publications: modelPublications,
			Batch:        128,
			PublishCost:  1,
			RTT:          1000,
			AckEvery:     every,
		})
		s.run()
		if s.resolved != modelPublications {
			t.Fatalf("ackEvery=%d: %d of %d publications were resolved", every, s.resolved, modelPublications)
		}
		if s.confirmed != modelPublications {
			t.Fatalf("ackEvery=%d: %d of %d publications were confirmed", every, s.confirmed, modelPublications)
		}
		// The broker sent fewer frames than there were publications, which
		// is what makes this subtest different from the one above. Without
		// it the coalescing could be off and the test would still pass.
		switch {
		case k.Resolve() == PerMessage:
			// A per-message strategy cannot benefit from a cumulative
			// confirm and this is where that shows: with one publication
			// outstanding there is never a second one for the broker to
			// coalesce it with, so the frame count is the publication
			// count however aggressively the broker batches. It is the
			// third cost of the reference kind, beside the round trip and
			// the throughput, and it is asserted rather than described so
			// that a kind which had silently started pipelining would fail
			// here.
			if s.frames != modelPublications {
				t.Fatalf("ackEvery=%d: the broker sent %d frames for %d per-message publications, want one each: a strategy with one publication outstanding gives the broker nothing to coalesce",
					every, s.frames, modelPublications)
			}
		case every > 1:
			if s.frames >= modelPublications {
				t.Fatalf("ackEvery=%d: the broker sent %d frames for %d publications, so nothing was coalesced and this subtest proves nothing",
					every, s.frames, modelPublications)
			}
		}
	}
}

// modelClose closes the channel with publications outstanding, which is the specification's
// ambiguity and the specification's "ambiguity accuracy".
//
// What it checks in both directions: every outstanding publication becomes
// exactly one Ambiguous outcome and never a failed one, the count never exceeds
// the kind's own MaxAmbiguous, and the reference kind really does bound it at
// one — which is the property a caller chooses per-message confirms FOR.
func modelClose(t *testing.T, k Kind) {
	s := newSim(t, k, simConfig{
		Publications: modelPublications,
		// A batch wider than the run, so no batch boundary is reached and
		// nothing is flushed before the close.
		Batch:       modelPublications + 1,
		PublishCost: 1,
		RTT:         1 << 40, // a confirm that never arrives inside the run
		AckEvery:    1,
		DetachAtEnd: true,
	})
	s.run()

	if s.resolved != modelPublications {
		t.Fatalf("%d of %d publications were resolved by the close: a close resolves everything it was holding", s.resolved, modelPublications)
	}
	if s.rejected != 0 {
		t.Fatalf("%d publication(s) were reported Rejected by a channel close: the broker may have taken them, so the outcome is Ambiguous (the message contract)", s.rejected)
	}
	if bound := s.st.MaxAmbiguous(); s.ambiguous > bound {
		t.Fatalf("the %v strategy left %d publications ambiguous, over its own bound of %d", k, s.ambiguous, bound)
	}
	if k.Resolve() == PerMessage && s.ambiguous > 1 {
		t.Fatalf("the per-message strategy left %d publications ambiguous: at most one publication is ever outstanding", s.ambiguous)
	}
	if k.Resolve() != PerMessage && s.ambiguous < 2 {
		// The other two kinds pay for their throughput here, and a subtest
		// that measured one would not be measuring the cost at all.
		t.Fatalf("the %v strategy left only %d publication(s) ambiguous: with a window of %d and no confirms arriving it should be holding the whole window",
			k, s.ambiguous, s.st.Window())
	}
	// A detached tracker is terminal in both directions: it takes no new
	// publication and applies no late confirm.
	if _, err := s.tr.Submit(""); err == nil {
		t.Fatal("a detached tracker accepted a publication")
	}
	if out := s.tr.Ack(1, true); len(out) != 0 {
		t.Fatalf("a detached tracker applied a late cumulative confirm and resolved %d publication(s): those tags belong to the next channel", len(out))
	}
}

// modelReturn is the specification's "Mandatory + NotifyReturn = Rejected".
//
// A publication that no queue is bound for produces a `basic.return` and then a
// `basic.ack`: the broker took it and threw it away. Reporting that ack as a
// success is the most expensive mistake this package can make, because the
// caller then believes a message is in a queue that never received it.
func modelReturn(t *testing.T, k Kind) {
	st, err := New(k, Config{})
	if err != nil {
		t.Fatal(err)
	}
	tr := NewTracker(st.Window())

	// Two publications, of which the first is returned.
	tokens := st.NeedsCorrelation()
	first, err := tr.Submit(tokenIf(tokens, "p0"))
	if err != nil {
		t.Fatal(err)
	}
	if !tokens {
		// The reference kind has one publication outstanding, so the return
		// is attributable with no token at all — which is the whole reason
		// it needs none.
		if !tr.ReturnedOnly() {
			t.Fatal("ReturnedOnly refused a tracker with exactly one outstanding publication")
		}
		out := tr.Ack(first, false)
		if len(out) != 1 || out[0].Result != Rejected || !out[0].Returned {
			t.Fatalf("the ack after a return resolved to %+v, want one Rejected outcome marked Returned", out)
		}
		return
	}

	second, err := tr.Submit("p1")
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Returned("p0") {
		t.Fatal("Returned did not find the token the publication was submitted with")
	}
	// A cumulative ack covering both: the returned one is Rejected and the
	// other is Confirmed, from ONE frame. A driver that applied the frame's
	// verdict to both would report the unroutable message as delivered.
	out := tr.Ack(second, true)
	if len(out) != 2 {
		t.Fatalf("the cumulative ack resolved %d publications, want 2", len(out))
	}
	byTag := map[uint64]Outcome{}
	for _, o := range out {
		byTag[o.Tag] = o
	}
	if got := byTag[first]; got.Result != Rejected || !got.Returned {
		t.Errorf("the returned publication resolved to %+v, want Rejected and marked Returned", got)
	}
	if got := byTag[second]; got.Result != Confirmed || got.Returned {
		t.Errorf("the routed publication resolved to %+v, want Confirmed", got)
	}
	// And a return naming a token nobody holds is reported rather than
	// silently attributed to somebody.
	if tr.Returned("p9") {
		t.Error("Returned accepted a token no outstanding publication carries")
	}
}

// modelWindow checks that the strategy's window is never exceeded, which is the bound on a pending set and the reason [Async] has a window at all.
func modelWindow(t *testing.T, k Kind) {
	st, err := New(k, Config{Window: 8})
	if err != nil {
		// PerMessage refuses a configured window, which is itself the claim.
		if k.Resolve() == PerMessage {
			return
		}
		t.Fatal(err)
	}
	s := newSim(t, k, simConfig{
		Publications: 200,
		PublishCost:  1,
		RTT:          1000,
		AckEvery:     1,
		Window:       8,
	})
	for i := range s.cfg.Publications {
		s.publish(i)
	}
	s.drain()
	maxOut := s.maxOut
	if maxOut > st.Window() {
		t.Fatalf("the %v strategy held %d publications outstanding with a window of %d", k, maxOut, st.Window())
	}
	if st.Window() > 1 && maxOut < st.Window() {
		// The other direction: a strategy that never filled its window did
		// not pipeline, and this subtest would pass for a strategy that had
		// silently become per-message.
		t.Fatalf("the %v strategy never held more than %d publications outstanding with a window of %d: it is not pipelining", k, maxOut, st.Window())
	}
}

// tokenIf returns the token when the strategy needs one and the empty string
// when it does not, so that the model drives each kind the way a driver would.
func tokenIf(need bool, token string) string {
	if need {
		return token
	}
	return ""
}
