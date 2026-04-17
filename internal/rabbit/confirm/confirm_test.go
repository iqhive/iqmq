package confirm

import (
	"errors"
	"strings"
	"sync"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/testguard"
)

// TestKindsList records the family's kinds in the test log, which is what the
// bake-off's own lint reads to check that every kind it measures exists.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, kindCount)
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
	if len(parts) != int(kindCount)-1 {
		t.Fatalf("Kinds() listed %d of %d concrete kinds", len(parts), kindCount-1)
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds this family
// (front-matter `auto: {confirm: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../../..", "confirm")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds confirm yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds confirm to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestKindNames holds the slug and identifier tables to the enumeration: a
// kind whose slug is empty is one a benchmark name and a DSN parameter cannot
// express, and a slug that round-trips to a different kind is a configuration
// silently changing meaning.
func TestKindNames(t *testing.T) {
	seen := map[string]Kind{}
	for k := Auto; k < kindCount; k++ {
		slug, ident := k.String(), k.Ident()
		if slug == "" || ident == "" {
			t.Fatalf("kind %d has slug %q and identifier %q", k, slug, ident)
		}
		if prev, dup := seen[slug]; dup {
			t.Fatalf("kinds %v and %v share the slug %q", prev, k, slug)
		}
		seen[slug] = k
		got, ok := ParseKind(slug)
		if !ok || got != k {
			t.Fatalf("ParseKind(%q) = (%v, %v), want (%v, true)", slug, got, ok, k)
		}
	}
	if _, ok := ParseKind("nonesuch"); ok {
		t.Fatal("ParseKind accepted a slug no kind has")
	}
	if k := kindCount; k.Valid() || k.String() != "kind(?)" || k.Ident() != "" {
		t.Fatalf("a kind outside the enumeration reports valid=%v slug=%q ident=%q", k.Valid(), k.String(), k.Ident())
	}
}

// TestConfigValidation holds each refusal to an input that triggers it and one
// that does not.
func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind Kind
		cfg  Config
		want error
	}{
		{"default", Windowed, Config{}, nil},
		{"explicit window", Windowed, Config{Window: 8}, nil},
		{"per-message default", PerMessage, Config{}, nil},
		{"per-message window of one", PerMessage, Config{Window: 1}, nil},
		{"unknown kind", kindCount, Config{}, ErrInvalidKind},
		{"negative window", Windowed, Config{Window: -1}, ErrInvalidWindow},
		{"absurd window", Windowed, Config{Window: MaxWindow + 1}, ErrInvalidWindow},
		{"per-message with a window", PerMessage, Config{Window: 8}, ErrWindowWithPerMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate(tc.kind)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
			if tc.want != nil && mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Errorf("the refusal carries code %v, want %v", mq.CodeOf(err), mq.CodeInvalidConfig)
			}
			st, nerr := New(tc.kind, tc.cfg)
			if (nerr == nil) != (tc.want == nil) {
				t.Fatalf("New = (%v, %v), and Validate said %v", st, nerr, err)
			}
		})
	}
}

// TestPerMessageRefusesAConfiguredWindow states the refusal on its own,
// because it is the one that is a judgement rather than a bound: the
// alternative was to ignore the value, and an ignored value is one a caller
// reads back out of their configuration and believes.
func TestPerMessageRefusesAConfiguredWindow(t *testing.T) {
	if _, err := New(PerMessage, Config{Window: 64}); !errors.Is(err, ErrWindowWithPerMessage) {
		t.Fatalf("New(PerMessage, window 64) = %v, want ErrWindowWithPerMessage", err)
	}
	st, err := New(PerMessage, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Window() != 1 {
		t.Fatalf("the per-message window is %d, want 1", st.Window())
	}
}

// TestStrategyClaimsDiffer is the check that keeps the family from becoming
// three names for one behaviour.
//
// Every pair of kinds has to differ in at least one of the four properties a
// caller chooses by, and the two that matter most are asserted outright: only
// PerMessage bounds the ambiguity at one, and only Async never blocks.
func TestStrategyClaimsDiffer(t *testing.T) {
	type claims struct {
		window, ambiguous int
		correlate, blocks bool
	}
	got := map[Kind]claims{}
	for _, k := range Kinds() {
		st, err := New(k, Config{})
		if err != nil {
			t.Fatal(err)
		}
		got[k] = claims{st.Window(), st.MaxAmbiguous(), st.NeedsCorrelation(), st.Blocks()}
	}
	for a := range got {
		for b := range got {
			if a != b && got[a] == got[b] {
				t.Errorf("the %v and %v strategies make identical claims %+v: the family has two names for one behaviour", a, b, got[a])
			}
		}
	}
	if got[PerMessage].ambiguous != 1 {
		t.Errorf("PerMessage bounds the ambiguity at %d, want 1", got[PerMessage].ambiguous)
	}
	if got[PerMessage].correlate {
		t.Error("PerMessage asks for a correlation token, which it does not need: one publication is outstanding, so a basic.return has one candidate")
	}
	if !got[Windowed].correlate || !got[Async].correlate {
		t.Error("a windowed or asynchronous strategy does not ask for a correlation token: basic.return carries no delivery tag, so the return would be attributed to the wrong publication")
	}
	if got[Async].blocks {
		t.Error("the asynchronous strategy blocks the publisher")
	}
	if !got[PerMessage].blocks || !got[Windowed].blocks {
		t.Error("a per-message or windowed strategy does not block, so it has no outcome to return from a synchronous Publish")
	}
}

// TestTrackerIssuesTagsInOrder is the one ordering rule the protocol gives no
// second chance on: the broker numbers publications in the order their frames
// reach it, so the tag has to be taken under the lock that admits the
// publication.
func TestTrackerIssuesTagsInOrder(t *testing.T) {
	tr := NewTracker(8)
	for want := uint64(1); want <= 100; want++ {
		got, err := tr.Submit("")
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("Submit returned tag %d, want %d: RabbitMQ's first delivery tag on a confirm channel is 1 and they are consecutive", got, want)
		}
	}
}

// TestTrackerConcurrentSubmitAndConfirm is the race the driver really has: the
// publisher submits from the caller's goroutine and the client library
// delivers confirms on its own.
//
// It runs under -race in every `make ci`, and what it asserts is not absence of
// a data race but the accounting: every publication gets exactly one terminal
// outcome even when the two sides interleave.
func TestTrackerConcurrentSubmitAndConfirm(t *testing.T) {
	const n = 2000
	tr := NewTracker(64)
	var mu sync.Mutex
	seen := map[uint64]int{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// The confirmer walks the tags it knows have been issued, acking
		// cumulatively, which is the shape a broker under load produces.
		for tag := uint64(1); tag <= n; tag++ {
			for tr.Next() <= tag {
				// Wait for the publisher to reach this tag without
				// spinning on the lock: Gosched is what makes the
				// interleaving wide under -race.
				runtimeGosched()
			}
			for _, o := range tr.Ack(tag, tag%7 == 0) {
				mu.Lock()
				seen[o.Tag]++
				mu.Unlock()
			}
		}
	}()
	for range n {
		if _, err := tr.Submit(""); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	for _, o := range tr.Detach() {
		mu.Lock()
		seen[o.Tag]++
		mu.Unlock()
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("%d of %d publications were resolved", len(seen), n)
	}
	for tag, count := range seen {
		if count != 1 {
			t.Fatalf("tag %d was resolved %d times", tag, count)
		}
	}
}

// TestTrackerDiscardsATagItNeverIssued is the publisher-side staleness guard.
//
// A confirm frame that was already in the socket when the channel died names a
// tag on the OLD channel; the new channel's tags restart at 1. Applying it
// would resolve a publication with another publication's outcome, which is the disqualifier in the publish direction.
func TestTrackerDiscardsATagItNeverIssued(t *testing.T) {
	tr := NewTracker(8)
	tag, err := tr.Submit("")
	if err != nil {
		t.Fatal(err)
	}
	if out := tr.Ack(tag+5, false); len(out) != 0 {
		t.Fatalf("the tracker applied a confirm for tag %d, which it never issued, and resolved %d publication(s)", tag+5, len(out))
	}
	if out := tr.Ack(0, false); len(out) != 0 {
		t.Fatal("the tracker applied a confirm for tag 0, which is not a valid delivery tag")
	}
	if out := tr.Ack(tag+5, true); len(out) != 0 {
		t.Fatalf("the tracker applied a CUMULATIVE confirm above the highest tag it issued and resolved %d publication(s): the frame belongs to another channel", len(out))
	}
	// And the real tag still resolves, so the guard is not simply refusing
	// everything.
	if out := tr.Ack(tag, false); len(out) != 1 || out[0].Result != Confirmed {
		t.Fatalf("the tracker did not resolve its own tag: %+v", out)
	}
}

// TestTrackerRefusesADuplicateToken is the case that would make a
// `basic.return` attributable to either of two publications, which is worse
// than not attributing it at all.
func TestTrackerRefusesADuplicateToken(t *testing.T) {
	tr := NewTracker(8)
	first, err := tr.Submit("p0")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Submit("p0"); !errors.Is(err, ErrDuplicateToken) {
		t.Fatalf("the second publication under token %q was accepted: %v", "p0", err)
	}
	// The refusal must not have consumed a tag: the broker numbers what it
	// receives, and a driver that burned a tag on a refused publication
	// would be one ahead of the broker for ever afterwards.
	if next := tr.Next(); next != first+1 {
		t.Fatalf("a refused publication consumed a delivery tag: next=%d, want %d", next, first+1)
	}
	// And the token is reusable once the publication it named has resolved.
	if out := tr.Ack(first, false); len(out) != 1 {
		t.Fatalf("resolving the first publication gave %+v", out)
	}
	if _, err := tr.Submit("p0"); err != nil {
		t.Fatalf("the token was not released when its publication resolved: %v", err)
	}
}

// TestReturnedOnlyRefusesAnAmbiguousAttribution is the negative direction of
// the reference kind's "no correlation token needed" claim: it holds only
// while exactly one publication is outstanding, and the tracker says so rather
// than guessing.
func TestReturnedOnlyRefusesAnAmbiguousAttribution(t *testing.T) {
	tr := NewTracker(8)
	if tr.ReturnedOnly() {
		t.Error("ReturnedOnly attributed a return with nothing outstanding")
	}
	if _, err := tr.Submit(""); err != nil {
		t.Fatal(err)
	}
	if !tr.ReturnedOnly() {
		t.Error("ReturnedOnly refused a return with exactly one publication outstanding")
	}
	if _, err := tr.Submit(""); err != nil {
		t.Fatal(err)
	}
	if tr.ReturnedOnly() {
		t.Error("ReturnedOnly attributed a return with two publications outstanding: the frame names neither, so one of them would be told the other's outcome")
	}
}

// TestDetachIsIdempotentAndOrdered pins the two properties a caller
// reconciling a batch depends on.
func TestDetachIsIdempotentAndOrdered(t *testing.T) {
	tr := NewTracker(8)
	for range 5 {
		if _, err := tr.Submit(""); err != nil {
			t.Fatal(err)
		}
	}
	out := tr.Detach()
	if len(out) != 5 {
		t.Fatalf("Detach resolved %d of 5 outstanding publications", len(out))
	}
	for i, o := range out {
		if o.Tag != uint64(i+1) {
			t.Fatalf("outcome %d names tag %d: Detach reports in publication order", i, o.Tag)
		}
		if o.Result != Ambiguous {
			t.Fatalf("tag %d resolved to %v, want %v", o.Tag, o.Result, Ambiguous)
		}
	}
	if again := tr.Detach(); len(again) != 0 {
		t.Fatalf("a second Detach resolved %d publication(s)", len(again))
	}
	resolved, ambiguous := tr.Stats()
	if resolved != 5 || ambiguous != 5 {
		t.Fatalf("Stats = (%d, %d), want (5, 5)", resolved, ambiguous)
	}
	if !errors.Is(mq.ErrClosed, mq.ErrClosed) || mq.CodeOf(ErrDetached) != mq.CodeClosed {
		t.Errorf("ErrDetached carries code %v, want %v", mq.CodeOf(ErrDetached), mq.CodeClosed)
	}
}

// TestNackIsDefiniteAndNotAmbiguous is the distinction a caller acts on: a
// `basic.nack` means the broker did not take the publication, so a retry
// duplicates nothing, and it must not be reported with the same code as a
// channel that closed.
func TestNackIsDefiniteAndNotAmbiguous(t *testing.T) {
	tr := NewTracker(8)
	tag, err := tr.Submit("")
	if err != nil {
		t.Fatal(err)
	}
	out := tr.Nack(tag, false, false)
	if len(out) != 1 || out[0].Result != Rejected {
		t.Fatalf("Nack resolved to %+v, want one Rejected outcome", out)
	}
	if out[0].Returned {
		t.Error("a nacked publication is marked Returned, which says the broker took it and threw it away")
	}
}

// TestUnsubmitKeepsTheTagSequenceAlignedWithTheLibrary is the case that would
// mis-attribute every confirm after it.
//
// amqp091-go increments its own published counter before it writes the frame
// and rolls it back when the write fails, so a driver that assigned a tag and
// then failed to publish would be one ahead of the library for ever: the
// library's next successful publication would be tag N and the driver would
// believe it was N+1. Nothing would crash; every confirm from then on would
// resolve the wrong publication.
func TestUnsubmitKeepsTheTagSequenceAlignedWithTheLibrary(t *testing.T) {
	tr := NewTracker(8)
	first, err := tr.Submit("")
	if err != nil {
		t.Fatal(err)
	}
	refused, err := tr.Submit("p1")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Unsubmit(refused); err != nil {
		t.Fatalf("Unsubmit(%d) = %v, want nil: it is the tag issued last", refused, err)
	}
	// The next publication gets the tag the refused one had, which is what
	// the library will give it.
	again, err := tr.Submit("p2")
	if err != nil {
		t.Fatal(err)
	}
	if again != refused {
		t.Fatalf("after taking back tag %d the next publication got tag %d: the library would have given it %d", refused, again, refused)
	}
	// Only the last tag can be taken back. The earlier one has been written.
	if err := tr.Unsubmit(first); err == nil {
		t.Fatalf("Unsubmit(%d) was accepted although tag %d is outstanding after it: that publication has already been written", first, again)
	}
	if err := tr.Unsubmit(again + 7); err == nil {
		t.Fatal("Unsubmit accepted a tag the tracker never issued")
	}
	// And the token the refused publication held is released, so it can be
	// reused.
	if _, err := tr.Submit("p1"); err != nil {
		t.Fatalf("the token of a taken-back publication was not released: %v", err)
	}
	// A detached tracker refuses it, because the publication's outcome is
	// then ambiguous rather than absent.
	tr.Detach()
	if err := tr.Unsubmit(tr.Next() - 1); err == nil {
		t.Fatal("a detached tracker accepted Unsubmit: those publications are ambiguous, not absent")
	}
}
