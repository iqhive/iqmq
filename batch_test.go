package iqmq_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// The batch-publication contract and, and the fuzz target the
// plan names for it.
//
// One rule decides everything here: a per-item result is aligned with the
// caller's messages. A driver with native batching gets the batch split into
// broker-sized pieces and one that has none gets a call per message, and the
// two have to report the same thing — index for index — because otherwise a
// batch publish reports one message's outcome for another, which is how an
// accepted message is lost in silence.
//
// The second rule is the specification: every message's outcome is one of accepted,
// definitely rejected and AMBIGUOUS, and the batch's error must not report an
// ambiguous message as rejected, nor a message that never left the process as
// ambiguous. A batch none of whose messages was accepted used to collapse to
// its first error whatever the others were, so a batch whose first message was
// refused and whose last may have reached the broker reported "refused",
// mq.IsAmbiguous was false, and a caller who republished the batch published
// the last message twice.
//
// The fake broker answers the way a driver does, and that is part of what is
// under test : every refusal and every ambiguous answer is
// a FRESH *mq.OpError built per message by the real driverkit mapper, and a
// refusal and an ambiguous answer carry the SAME code. A fixture whose
// refusals were one shared errors.New value could not tell the rule apart
// from "compare codes" or "compare identity", and could not see IsAmbiguous
// stop at the first *OpError in a batch.

// batchOutcomeKind is what a batch publication's single error says about the
// batch as a whole.
type batchOutcomeKind uint8

const (
	// batchAllAccepted is a nil error: every message was accepted.
	batchAllAccepted batchOutcomeKind = iota
	// batchPartial is a *BatchError: some were.
	batchPartial
	// batchNoneAccepted is a single error: none were, and every message has
	// the same outcome -- all definitely failed, or all ambiguous.
	batchNoneAccepted
	// batchNoneAcceptedMixed is a *BatchError although none was accepted:
	// some definitely failed and some are ambiguous, and one error cannot
	// say both.
	batchNoneAcceptedMixed
)

func (k batchOutcomeKind) String() string {
	switch k {
	case batchAllAccepted:
		return "all accepted (nil)"
	case batchPartial:
		return "partial (*BatchError)"
	case batchNoneAcceptedMixed:
		return "none accepted, outcomes mixed (*BatchError)"
	default:
		return "none accepted (a single error)"
	}
}

// batchFate is what the fake broker does with one message of a batch.
type batchFate uint8

const (
	// fateAccept stores the message and says so.
	fateAccept batchFate = iota
	// fateRefuse refuses the message before anything is written: nothing is
	// stored, and the answer is a definite CodeUnavailable.
	fateRefuse
	// fateAmbiguous stores the message and loses the answer: the
	// connection died after the broker took it. The answer is an ambiguous
	// CodeUnavailable -- the same code as a refusal.
	fateAmbiguous
	// fateCodeAmbiguous stores the message and answers with ambiguity in
	// the CODE alone (CodeAmbiguous, no flag), which is the shape
	// drivers/redis's short WAITAOF had.
	fateCodeAmbiguous
	// fateFatal stores the message and answers with a post-send deadline:
	// ambiguous, and FATAL, so the batch cannot continue. The broker is
	// gone afterwards, and every later message of the batch definitely
	// did not reach it.
	fateFatal
	// fateStopped is not an input: it is the reference's name for a message
	// after a fatal one, whatever its own fate was.
	fateStopped
)

func (f batchFate) String() string {
	switch f {
	case fateAccept:
		return "accept"
	case fateRefuse:
		return "refuse"
	case fateAmbiguous:
		return "ambiguous"
	case fateCodeAmbiguous:
		return "code-ambiguous"
	case fateFatal:
		return "fatal"
	default:
		return "stopped"
	}
}

// ambiguous reports whether the fate leaves the message's outcome unknown.
func (f batchFate) ambiguous() bool {
	return f == fateAmbiguous || f == fateCodeAmbiguous || f == fateFatal
}

// batchTally is how many messages the reference gives each outcome.
type batchTally struct{ accepted, refused, ambiguous, stopped int }

// failed is how many messages the batch's error must report as failed.
func (t batchTally) failed() int { return t.refused + t.ambiguous + t.stopped }

// definite is how many failed messages definitely did not reach the broker.
func (t batchTally) definite() int { return t.refused + t.stopped }

// batchReference is the ONE oracle both driver shapes are checked against:
// what the outcome of publishing a batch with these fates must be, and the
// fate each message really has once a fatal one has stopped the batch.
//
// It is deliberately trivial. The implementation splits, re-aligns and folds;
// the reference counts. A reference that shared any of that machinery could
// agree with a defect in it.
func batchReference(fates []batchFate) (batchOutcomeKind, batchTally, []batchFate) {
	var t batchTally
	effective := make([]batchFate, len(fates))
	stopped := false
	for i, f := range fates {
		if stopped {
			f = fateStopped
		}
		effective[i] = f
		switch f {
		case fateAccept:
			t.accepted++
		case fateRefuse:
			t.refused++
		case fateStopped:
			t.stopped++
		default:
			t.ambiguous++
		}
		if f == fateFatal {
			stopped = true
		}
	}
	switch {
	case t.failed() == 0:
		return batchAllAccepted, t, effective
	case t.accepted > 0:
		return batchPartial, t, effective
	case t.definite() > 0 && t.ambiguous > 0:
		return batchNoneAcceptedMixed, t, effective
	default:
		return batchNoneAccepted, t, effective
	}
}

// errBatchRefused is the native cause every refusal wraps, so that
// errors.Is reaches it through the driver's *mq.OpError. A refusal is a
// per-message outcome and not a fatal one, so a batch carries on past it.
var errBatchRefused = errors.New("the native client refused this message")

// batchMapper is the fake driver's error mapper: the real one, with the
// ErrorMapper every driver uses.
var batchMapper = &driverkit.ErrorMapper{Driver: "fake"}

// batchAnswer is the fake broker's answer for message i with fate f. Every
// call builds a fresh value.
func batchAnswer(i int, f batchFate) error {
	switch f {
	case fateRefuse:
		// Refused before the request left: definite, CodeUnavailable.
		return batchMapper.MapNet(mq.OpPublish, "orders", "", fmt.Errorf("message %d: %w: %w", i, errBatchRefused, io.EOF), false)
	case fateAmbiguous:
		// The connection died after the write: ambiguous, CodeUnavailable.
		return batchMapper.MapNet(mq.OpPublish, "orders", "", fmt.Errorf("message %d: %w", i, io.EOF), true)
	case fateCodeAmbiguous:
		return &mq.OpError{Driver: "fake", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeAmbiguous}
	case fateFatal:
		// A deadline after the write: ambiguous, and fatal to the batch.
		return batchMapper.MapNet(mq.OpPublish, "orders", "", fmt.Errorf("message %d: %w", i, context.DeadlineExceeded), true)
	default:
		return nil
	}
}

// fatesOf turns a refusal pattern into fates with no ambiguity in them.
func fatesOf(fails []bool) []batchFate {
	out := make([]batchFate, len(fails))
	for i, f := range fails {
		if f {
			out[i] = fateRefuse
		}
	}
	return out
}

// runBatchCase publishes one batch with one pattern of fates through one
// driver shape and checks everything the contract fixes: the outcome's kind,
// the index alignment of a per-item result and the outcome of every
// item in it, which error a single-error outcome is, which messages reached
// the broker and in what order, and how many native calls it took.
func runBatchCase(tb testing.TB, batching bool, limit int, fates []batchFate) {
	tb.Helper()
	if len(fates) == 0 {
		return
	}
	b := newFakeBroker(fakeConfig{batching: batching, maxBatch: limit}, "orders")
	cl := mq.NewClient(b)
	defer func() {
		if err := cl.Close(); err != nil {
			tb.Errorf("closing the client: %v", err)
		}
	}()
	ctx := context.Background()
	// A subscription has to exist for the accepted messages to land
	// anywhere, which is what makes "the broker holds what was accepted" a
	// check on the publication and not on the counter.
	if _, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		tb.Fatalf("opening the consumer: %v", err)
	}
	p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		tb.Fatalf("opening the publisher: %v", err)
	}
	indexOf := func(m *mq.Message) int {
		i, cerr := strconv.Atoi(m.ID)
		if cerr != nil || i < 0 || i >= len(fates) {
			return -1
		}
		return i
	}
	// answers records what the broker said about each message it was asked
	// about, so a single-error outcome can be held to BEING one of them. The
	// hooks run under the broker's lock, and so does every read of dead.
	answers := make([]error, len(fates))
	dead := false
	b.setRefuse(func(_ int, m *mq.Message) error {
		i := indexOf(m)
		switch {
		case i < 0:
			return nil
		case dead:
			answers[i] = mq.ErrClosed
			return mq.ErrClosed
		case fates[i] == fateRefuse:
			answers[i] = batchAnswer(i, fateRefuse)
			return answers[i]
		}
		return nil
	})
	b.setLose(func(_ int, m *mq.Message) error {
		i := indexOf(m)
		if i < 0 || !fates[i].ambiguous() {
			return nil
		}
		answers[i] = batchAnswer(i, fates[i])
		if fates[i] == fateFatal {
			dead = true
		}
		return answers[i]
	})
	ms := make([]*mq.Message, len(fates))
	for i := range ms {
		ms[i] = &mq.Message{ID: strconv.Itoa(i), Body: []byte("b")}
	}

	err = p.PublishBatch(ctx, ms)
	wantKind, want, effective := batchReference(fates)

	// Whatever its shape, the batch's error is ambiguous exactly when one of
	// its messages is, and it reaches a refusal exactly when one of
	// them was refused.
	if err != nil {
		if got := mq.IsAmbiguous(err); got != (want.ambiguous > 0) {
			tb.Fatalf("PublishBatch(%v) = %v: IsAmbiguous = %v, want %v -- %d of the messages may be in the broker",
				fates, err, got, want.ambiguous > 0, want.ambiguous)
		}
		if got := errors.Is(err, errBatchRefused); got != (want.refused > 0) {
			tb.Fatalf("PublishBatch(%v) = %v: errors.Is(refused) = %v, want %v", fates, err, got, want.refused > 0)
		}
	}
	var be *mq.BatchError
	switch {
	case err == nil:
		if wantKind != batchAllAccepted {
			tb.Fatalf("PublishBatch(%v) = nil, want %v", fates, wantKind)
		}
	case errors.As(err, &be):
		if wantKind != batchPartial && wantKind != batchNoneAcceptedMixed {
			tb.Fatalf("PublishBatch(%v) = %v (a *BatchError), want %v", fates, err, wantKind)
		}
		if len(be.Errs) != len(fates) {
			tb.Fatalf("PublishBatch(%v) reported %d results for %d messages: the results are not aligned with the batch",
				fates, len(be.Errs), len(fates))
		}
		for i, itemErr := range be.Errs {
			checkBatchItem(tb, fates, i, itemErr, effective[i], batching, answers)
		}
		if be.Failed() != want.failed() {
			tb.Fatalf("PublishBatch(%v) reported %d failures, want %d", fates, be.Failed(), want.failed())
		}
		if !errors.Is(err, mq.ErrPartial) {
			tb.Fatalf("PublishBatch(%v) = %v does not match ErrPartial", fates, err)
		}
	default:
		if wantKind != batchNoneAccepted {
			tb.Fatalf("PublishBatch(%v) = %v (a single error), want %v", fates, err, wantKind)
		}
		if errors.Is(err, mq.ErrPartial) {
			tb.Fatalf("PublishBatch(%v) = %v matches ErrPartial although nothing was accepted", fates, err)
		}
		// The single error IS the first message's own answer, unchanged:
		// the error handling a caller has for one publication is what it
		// gets, and nothing the core made up stands in for it.
		if err != answers[0] { //nolint:errorlint // identity: the driver's own answer for message 0
			tb.Fatalf("PublishBatch(%v) = %v, want the broker's own answer for message 0, %v", fates, err, answers[0])
		}
	}

	// Conservation: the broker holds exactly the messages it took -- the
	// accepted ones and the ambiguous ones, whose answers were lost after
	// the broker had them -- in publication order.
	counts := b.counts("orders", "billing")
	held := want.accepted + want.ambiguous
	if counts.published != held {
		tb.Fatalf("PublishBatch(%v): the broker holds %d messages, want %d", fates, counts.published, held)
	}
	sent := b.sentSeqs()
	if len(sent) != held {
		tb.Fatalf("PublishBatch(%v): the native client took %d messages, want %d", fates, len(sent), held)
	}
	for i := 1; i < len(sent); i++ {
		if sent[i] <= sent[i-1] {
			tb.Fatalf("PublishBatch(%v): the native client saw the batch out of order: %v", fates, sent)
		}
	}
	// And the publisher counted the same thing the error said.
	st := p.Stats()
	if st.Published != int64(want.accepted) || st.Failed != int64(want.failed()) {
		tb.Fatalf("PublishBatch(%v): Stats = %+v, want %d published and %d failed", fates, st, want.accepted, want.failed())
	}
	if st.Pending != 0 {
		tb.Fatalf("PublishBatch(%v): Stats.Pending = %d after the batch, want 0", fates, st.Pending)
	}
	// One native call per broker-sized piece for a batching driver, one per
	// message for a driver without batching. It is the cost side and
	// the reason the optional contract exists. A fatal answer ends the batch
	// at a point that depends on how the shape grouped it -- a loop stops at
	// the message, a native piece finishes and the next piece finds the
	// broker gone -- so the count is held only for batches without one.
	if want.stopped > 0 || effective[len(effective)-1] == fateFatal {
		return
	}
	wantCalls := len(fates)
	if batching {
		size := limit
		if size <= 0 || size > len(fates) {
			size = len(fates)
		}
		wantCalls = (len(fates) + size - 1) / size
	}
	if got := b.nativeCalls(); got != wantCalls {
		tb.Fatalf("PublishBatch(%v) took %d native calls, want %d", fates, got, wantCalls)
	}
}

// checkBatchItem holds one entry of a per-item result to the message's real
// fate.
func checkBatchItem(tb testing.TB, fates []batchFate, i int, itemErr error, fate batchFate, batching bool, answers []error) {
	tb.Helper()
	if (itemErr != nil) != (fate != fateAccept) {
		tb.Fatalf("PublishBatch(%v) reported %v for message %d (%v): the results are aligned with something other than the messages",
			fates, itemErr, i, fate)
	}
	if itemErr == nil {
		return
	}
	if got := mq.IsAmbiguous(itemErr); got != fate.ambiguous() {
		tb.Fatalf("PublishBatch(%v) reported %v for message %d, whose fate was %v: IsAmbiguous = %v",
			fates, itemErr, i, fate, got)
	}
	if got := errors.Is(itemErr, errBatchRefused); got != (fate == fateRefuse) {
		tb.Fatalf("PublishBatch(%v) reported %v for message %d, whose fate was %v: errors.Is(refused) = %v",
			fates, itemErr, i, fate, got)
	}
	if fate == fateStopped {
		return
	}
	// A message the broker answered for carries a broker answer, and not one
	// the core made up. Through a loop it is the message's own answer; a
	// native piece whose messages all had one outcome answers with its first
	// message's, which the core gives to the whole piece.
	if !batching {
		if itemErr != answers[i] { //nolint:errorlint // identity: the broker's own answer for this message
			tb.Fatalf("PublishBatch(%v) reported %v for message %d, want the broker's own answer %v", fates, itemErr, i, answers[i])
		}
		return
	}
	for _, a := range answers {
		if a != nil && itemErr == a { //nolint:errorlint // identity: one of the broker's own answers
			return
		}
	}
	tb.Fatalf("PublishBatch(%v) reported %v for message %d: it is none of the broker's own answers", fates, itemErr, i)
}

// batchPatterns are the deterministic refusal patterns both shapes are
// checked against. They are fixed here rather than per shape so that neither
// can be excused by a pattern of its own.
var batchPatterns = [][]bool{
	{false},
	{true},
	{false, false},
	{true, false},
	{false, true},
	{true, true},
	{false, true, false, true, false},
	{true, false, false, false, true},
	{false, false, false, false, false, false, false},
	{true, true, true, true, true, true, true},
	{false, true, true, false, true, false, false, true, false},
}

// batchAmbiguityPatterns are the patterns with an ambiguous or fatal message
// in them, one letter per message: '.' accepted, 'R' refused, '?' stored with
// its answer lost, 'C' stored with an answer ambiguous only by its code, 'F'
// stored with a fatal, ambiguous answer after which the broker is gone. The
// first is the case the defect was reported with: the first message refused
// and the last one ambiguous, which collapsed to "refused". The second is the
// one FuzzBatchResults found on its own in 0.3 s, the other way round.
var batchAmbiguityPatterns = []string{
	"RRR?", "?R", "R?", "??", "?", "???", "???R", "R?R?R?R",
	".?", "?.", ".?R", "R.?", "?..", "..?..R..",
	"RC", "CR", "CC", "C?", ".CR", "R?C",
	"F", "F..", "RF.", "?F.", ".F.", "RRF", "..F", "F?R", ".?FR.",
}

// fatesFrom reads a pattern of batchAmbiguityPatterns.
func fatesFrom(s string) []batchFate {
	out := make([]batchFate, len(s))
	for i, c := range s {
		switch c {
		case 'R':
			out[i] = fateRefuse
		case '?':
			out[i] = fateAmbiguous
		case 'C':
			out[i] = fateCodeAmbiguous
		case 'F':
			out[i] = fateFatal
		}
	}
	return out
}

// allBatchPatterns is every deterministic pattern as fates.
func allBatchPatterns() [][]batchFate {
	out := make([][]batchFate, 0, len(batchPatterns)+len(batchAmbiguityPatterns))
	for _, fails := range batchPatterns {
		out = append(out, fatesOf(fails))
	}
	for _, s := range batchAmbiguityPatterns {
		out = append(out, fatesFrom(s))
	}
	return out
}

// TestBatchResults checks every pattern through both driver shapes at
// several broker maxima, including maxima that split the batch.
func TestBatchResults(t *testing.T) {
	for _, limit := range []int{0, 1, 2, 3, 8} {
		for _, batching := range []bool{false, true} {
			name := "loop"
			if batching {
				name = "native"
			}
			t.Run(name+"/limit="+strconv.Itoa(limit), func(t *testing.T) {
				for _, fates := range allBatchPatterns() {
					runBatchCase(t, batching, limit, fates)
				}
			})
		}
	}
}

// TestBatchEmptyAndNil pins the edges: an empty batch is a no-op and a nil
// message is refused before any I/O, with nothing published.
func TestBatchEmptyAndNil(t *testing.T) {
	cl, b := openFake(t, fakeConfig{batching: true})
	ctx := context.Background()
	p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.PublishBatch(ctx, nil); err != nil {
		t.Fatalf("an empty batch = %v, want nil", err)
	}
	ms := []*mq.Message{{Body: []byte("a")}, nil, {Body: []byte("c")}}
	if err := p.PublishBatch(ctx, ms); !errors.Is(err, mq.ErrInvalidTopic) && err == nil {
		t.Fatal("a batch with a nil message was accepted")
	}
	if got := b.nativeCalls(); got != 0 {
		t.Fatalf("a rejected batch made %d native calls, want 0: the specification puts validation before any I/O", got)
	}
	if got := b.counts("orders", "billing"); got.published != 0 {
		t.Fatalf("a rejected batch published %d messages", got.published)
	}
}

// TestBatchFatalErrorAlignsTheRest pins the alignment rule at its hardest
// point: when the sink cannot continue — a closed sink, a finished context —
// every message the batch never reached is refused with the same error
// rather than left looking accepted.
func TestBatchFatalErrorAlignsTheRest(t *testing.T) {
	for _, batching := range []bool{false, true} {
		name := "loop"
		if batching {
			name = "native"
		}
		t.Run(name, func(t *testing.T) {
			b := newFakeBroker(fakeConfig{batching: batching, maxBatch: 1}, "orders")
			cl := mq.NewClient(b)
			defer func() { _ = cl.Close() }()
			ctx := context.Background()
			if _, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
				t.Fatal(err)
			}
			p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
			if err != nil {
				t.Fatal(err)
			}
			// The third message closes the sink under the batch, which is
			// the fatal case: everything after it was never offered.
			b.setRefuse(func(seq int, _ *mq.Message) error {
				if seq >= 3 {
					return mq.ErrClosed
				}
				return nil
			})
			ms := make([]*mq.Message, 6)
			for i := range ms {
				ms[i] = &mq.Message{ID: strconv.Itoa(i), Body: []byte("b")}
			}
			err = p.PublishBatch(ctx, ms)
			var be *mq.BatchError
			if !errors.As(err, &be) {
				t.Fatalf("PublishBatch = %v, want a *BatchError: two messages were accepted", err)
			}
			if len(be.Errs) != len(ms) {
				t.Fatalf("PublishBatch reported %d results for %d messages", len(be.Errs), len(ms))
			}
			for i, itemErr := range be.Errs {
				if i < 2 {
					if itemErr != nil {
						t.Fatalf("message %d was accepted and reported %v", i, itemErr)
					}
					continue
				}
				if itemErr == nil {
					t.Fatalf("message %d was never offered to the broker and is reported as accepted: %v", i, be.Errs)
				}
				if !errors.Is(itemErr, mq.ErrClosed) {
					t.Fatalf("message %d = %v, want ErrClosed", i, itemErr)
				}
			}
			if got := b.counts("orders", "billing"); got.published != 2 {
				t.Fatalf("the broker holds %d messages, want 2", got.published)
			}
		})
	}
}

// Fuzz limits keep a decoded batch case inside a fuzzing budget.
const (
	fuzzMaxBatch = 24
	fuzzMaxLimit = 8
)

// batchCase is one decoded batch publication.
type batchCase struct {
	// batching selects the driver shape.
	batching bool
	// limit is the broker's advertised maximum batch, zero for none.
	limit int
	// fates is what the broker does with each message.
	fates []batchFate
}

// decodeBatchCase turns fuzz input into a batch case.
//
// Each dimension is read AFTER the ones that came before it, so an input
// written before a dimension existed decodes to the case it always named: an
// exhausted input reads false and zero, and those are the old meanings. The
// order is: the shape and the limit; the size; one "failed" flag per message;
// one "ambiguous" flag per failed message; one "by code alone" flag per
// ambiguous message; and the position of a fatal message, zero for none.
func decodeBatchCase(data []byte) batchCase {
	r := fuzzutil.NewReader(data)
	c := batchCase{
		batching: r.Bool(),
		limit:    r.Int(0, fuzzMaxLimit),
	}
	// At least one message: an exhausted input decodes to the low end of
	// every range, and a zero-length batch is a case that returns before it
	// has published anything. Starting the range at one is what keeps a
	// minimised input a real batch rather than a no-op — the fuzzer found
	// nothing at 870,000 executions a second because almost none of them
	// published anything.
	n := r.Int(1, fuzzMaxBatch)
	c.fates = make([]batchFate, n)
	for i := range c.fates {
		if r.Bool() {
			c.fates[i] = fateRefuse
		}
	}
	for i := range c.fates {
		if c.fates[i] == fateRefuse && r.Bool() {
			c.fates[i] = fateAmbiguous
		}
	}
	for i := range c.fates {
		if c.fates[i] == fateAmbiguous && r.Bool() {
			c.fates[i] = fateCodeAmbiguous
		}
	}
	if at := r.Int(0, n); at > 0 {
		c.fates[at-1] = fateFatal
	}
	return c
}

// encodeBatchCase is decodeBatchCase's mirror, so the scripted patterns can
// be seeded. It encodes the first fatal message; a pattern has at most one.
func encodeBatchCase(c batchCase) []byte {
	w := fuzzutil.NewWriter()
	w.Bool(c.batching)
	w.Int(0, fuzzMaxLimit, c.limit)
	w.Int(1, fuzzMaxBatch, len(c.fates))
	for _, f := range c.fates {
		w.Bool(f != fateAccept)
	}
	for _, f := range c.fates {
		if f != fateAccept {
			w.Bool(f == fateAmbiguous || f == fateCodeAmbiguous)
		}
	}
	for _, f := range c.fates {
		if f == fateAmbiguous || f == fateCodeAmbiguous {
			w.Bool(f == fateCodeAmbiguous)
		}
	}
	at := 0
	for i, f := range c.fates {
		if f == fateFatal {
			at = i + 1
			break
		}
	}
	w.Int(0, len(c.fates), at)
	return w.Data()
}

// FuzzBatchResults is the index-alignment and outcome-conservation
// target over both SPI contracts.
//
// Arbitrary batch sizes and arbitrary patterns of fates go through one
// publisher and through the reference model, and the outcome, the per-message
// results and their outcomes, the messages the native client took, their
// order and the number of native calls all have to agree. A batch larger than
// the broker's maximum is split, so a contract that aligned its results with
// the split rather than with the messages fails here — which is the defect
// the target exists for (x/apibench/a36). A message whose answer was
// lost after the broker took it is the second dimension, one whose ambiguity
// is in its code alone the third, and a fatal answer that ends the batch the
// fourth: an outcome that reported an ambiguous message as refused, or a
// message that never left as ambiguous, is caught here too.
func FuzzBatchResults(f *testing.F) {
	for _, fates := range allBatchPatterns() {
		for _, limit := range []int{0, 1, 3} {
			for _, batching := range []bool{false, true} {
				f.Add(encodeBatchCase(batchCase{batching: batching, limit: limit, fates: fates}))
			}
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		c := decodeBatchCase(data)
		runBatchCase(t, c.batching, c.limit, c.fates)
	})
}

// TestBatchCaseRoundTrip pins that the seed encoder is the decoder's mirror,
// so a seed names the case it was written for.
func TestBatchCaseRoundTrip(t *testing.T) {
	cases := allBatchPatterns()
	for _, limit := range []int{0, 1, 3, 8} {
		for _, batching := range []bool{false, true} {
			for _, fates := range cases {
				want := batchCase{batching: batching, limit: limit, fates: fates}
				got := decodeBatchCase(encodeBatchCase(want))
				if got.batching != want.batching || got.limit != want.limit || len(got.fates) != len(want.fates) {
					t.Fatalf("case %+v decoded as %+v", want, got)
				}
				for i := range got.fates {
					if got.fates[i] != want.fates[i] {
						t.Fatalf("case %+v decoded as %+v", want, got)
					}
				}
			}
		}
	}
}
