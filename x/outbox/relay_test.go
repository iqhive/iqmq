package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/outbox"
)

// sink records what a relay published and can be told to fail.
type sink struct {
	published []*mq.Message
	// fail, when set, is called with each message and its error returned.
	fail func(m *mq.Message) error
}

func (s *sink) publish(_ context.Context, _ string, ms []*mq.Message, _ []mq.PublishOption) error {
	for _, m := range ms {
		if s.fail != nil {
			if err := s.fail(m); err != nil {
				return err
			}
		}
		s.published = append(s.published, m)
	}
	return nil
}

func (s *sink) bodies() []string {
	out := make([]string, 0, len(s.published))
	for _, m := range s.published {
		out = append(out, string(m.Body))
	}
	return out
}

func newStore(tb testing.TB, opt outbox.MemoryOptions) *outbox.MemoryStore {
	tb.Helper()
	s, err := outbox.NewMemoryStore(opt)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

func append3(tb testing.TB, s *outbox.MemoryStore) []string {
	tb.Helper()
	ids := make([]string, 0, 3)
	for _, body := range []string{"one", "two", "three"} {
		id, err := s.Append(outbox.Row{Topic: "orders", Body: []byte(body)})
		if err != nil {
			tb.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestDrainPublishesAndMarks(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	ids := append3(t, s)
	sk := &sink{}
	r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish})
	if err != nil {
		t.Fatal(err)
	}
	n, err := r.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("Drain published %d, want 3", n)
	}
	if got := sk.bodies(); strings.Join(got, ",") != "one,two,three" {
		t.Errorf("published %v, want one,two,three in row order", got)
	}
	if s.Unsent() != 0 {
		t.Errorf("%d row(s) unsent", s.Unsent())
	}
	if got := s.Sent(); len(got) != 3 {
		t.Errorf("Sent() = %v, want all three of %v", got, ids)
	}
	st := r.Stats()
	if st.Claimed != 3 || st.Sent != 3 || st.Failed != 0 || st.Ambiguous != 0 {
		t.Errorf("Stats = %+v", st)
	}
	// A second pass finds nothing: marking works.
	if n, err := r.Drain(context.Background()); err != nil || n != 0 {
		t.Errorf("the second Drain = (%d, %v), want (0, nil)", n, err)
	}
}

// TestAnAmbiguousPublishLeavesTheRowUnsent is the first of the package
// documentation's two rules, and the only way this pattern can LOSE a
// message.
//
// the ambiguous outcome is a publication whose acceptance is unknown.
// Marking it sent turns "we do not know" into "we are done". Leaving it
// unsent produces a duplicate, which at-least-once already promised.
func TestAnAmbiguousPublishLeavesTheRowUnsent(t *testing.T) {
	// Both halves of the surface: an error carrying CodeAmbiguous, and
	// one whose *OpError sets the Ambiguous flag. A relay that checked only
	// one would mark half of them sent.
	for name, ambiguous := range map[string]error{
		"code":    ambiguousByCode(),
		"OpError": ambiguousByFlag(),
	} {
		t.Run(name, func(t *testing.T) {
			if mq.CodeOf(ambiguous) != mq.CodeAmbiguous && !mq.IsAmbiguous(ambiguous) {
				t.Fatalf("the fixture %v is not ambiguous by either route, so this test would prove nothing", ambiguous)
			}
			s := newStore(t, outbox.MemoryOptions{})
			append3(t, s)
			sk := &sink{fail: func(m *mq.Message) error {
				if string(m.Body) == "two" {
					return ambiguous
				}
				return nil
			}}
			r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			if s.Unsent() == 0 {
				t.Fatal("every row was marked sent although one publication was ambiguous")
			}
			if st := r.Stats(); st.Ambiguous != 1 {
				t.Errorf("Stats.Ambiguous = %d, want 1: an ambiguous publication must be counted separately from a failure, because it is what explains a duplicate", st.Ambiguous)
			}
			// The ambiguous row must NOT have been recorded as failed: it
			// may well have been accepted, and a dead-letter column saying
			// it failed would be a lie.
			if err := s.FailureOf("row-2"); err != nil {
				t.Errorf("the ambiguous row was recorded as failed with %v; its outcome is unknown, not failed", err)
			}
			// And a second pass republishes it once the broker answers,
			// which is the duplicate this pattern deliberately prefers to a
			// loss. (The publication may well have been accepted the first
			// time -- that is what "ambiguous" means -- so this second
			// delivery is the duplicate, and the consumer's inbox is what
			// absorbs it.)
			sk.fail = nil
			if _, err := r.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			if s.Unsent() != 0 {
				t.Errorf("%d row(s) still unsent after a second pass", s.Unsent())
			}
		})
	}
}

// TestAPartialBatchMarksOnlyWhatWasPublished is the second rule: a batch
// whose second row fails marks the first and releases the rest.
//
// The rest is released UNATTEMPTED. Concurrency 1 preserves row order, and
// publishing row three past a failed row two put it on the broker first, so
// the relay used to reorder exactly when a publication failed.
func TestAPartialBatchMarksOnlyWhatWasPublished(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	append3(t, s)
	sk := &sink{fail: func(m *mq.Message) error {
		if string(m.Body) == "two" {
			return errors.New("the broker refused this one")
		}
		return nil
	}}
	r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish})
	if err != nil {
		t.Fatal(err)
	}
	n, err := r.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("Drain published %d, want 1", n)
	}
	// Row one went; row two did not, and row three waits behind it. A
	// relay that marked the whole claim because the call returned would
	// report Unsent()==0 and row two would never be published.
	if got := s.Sent(); strings.Join(got, ",") != "row-1" {
		t.Errorf("Sent() = %v, want [row-1]: the failed row must not be marked, and the row after it must not overtake it", got)
	}
	if got := sk.bodies(); strings.Join(got, ",") != "one" {
		t.Errorf("published %v, want [one]: row three was published ahead of the row that failed before it", got)
	}
	if s.Unsent() != 2 {
		t.Errorf("%d row(s) unsent, want 2", s.Unsent())
	}
	// Released rather than left locked, so the next pass takes them at once,
	// and in order.
	sk.fail = nil
	if n, err := r.Drain(context.Background()); err != nil || n != 2 {
		t.Errorf("the second Drain = (%d, %v), want (2, nil): released rows must be claimable immediately", n, err)
	}
	if got := sk.bodies(); strings.Join(got, ",") != "one,two,three" {
		t.Errorf("published %v, want [one two three]", got)
	}
	if s.Unsent() != 0 {
		t.Errorf("%d row(s) still unsent", s.Unsent())
	}
}

func TestAPoisonRowIsNotRetriedForEver(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	if _, err := s.Append(outbox.Row{Topic: "orders", Body: []byte("poison")}); err != nil {
		t.Fatal(err)
	}
	sk := &sink{fail: func(*mq.Message) error { return rowFault("always fails") }}
	r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if _, err := r.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	st := r.Stats()
	if st.Poison == 0 {
		t.Error("Stats.Poison = 0: a row that fails identically on every attempt was retried without bound, and everything behind it makes no progress")
	}
	if got := s.FailureOf("row-1"); !errors.Is(got, outbox.ErrPoisonRow) {
		t.Errorf("FailureOf = %v, want ErrPoisonRow recorded against the row", got)
	}
	// And the publisher stopped being called: the point of the bound is to
	// stop spending attempts, not only to count them.
	if len(sk.published) != 0 {
		t.Errorf("the sink saw %d successful publications", len(sk.published))
	}
	t.Logf("after 6 passes with MaxAttempts=3: %+v", st)
}

// TestPoisonRowsAtTheHeadDoNotStallTheRelay is the bound's other half: a row
// over MaxAttempts must leave rotation and not merely be counted. The relay
// used to release it like any unsent row and the store kept it claimable, so
// a claim's worth of poison rows at the head of the table came back first on
// every pass and nothing behind them was ever published.
func TestPoisonRowsAtTheHeadDoNotStallTheRelay(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	for _, body := range []string{"poison", "poison", "good"} {
		if _, err := s.Append(outbox.Row{Topic: "orders", Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	sk := &sink{fail: func(m *mq.Message) error {
		if string(m.Body) == "poison" {
			return rowFault("always fails")
		}
		return nil
	}}
	r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, Batch: 2, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := r.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.Sent(); strings.Join(got, ",") != "row-3" {
		t.Fatalf("Sent() = %v, want [row-3]: the poison rows at the head kept every claim", got)
	}
	for _, id := range []string{"row-1", "row-2"} {
		if got := s.FailureOf(id); !errors.Is(got, outbox.ErrPoisonRow) {
			t.Errorf("FailureOf(%s) = %v, want ErrPoisonRow", id, got)
		}
		if n := s.Attempts(id); n != 2 {
			t.Errorf("%s was claimed %d times, want 2: a poison row must not be claimed again", id, n)
		}
	}
}

func TestARowWithNoTopicIsRecordedNotPublished(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	if _, err := s.Append(outbox.Row{Body: []byte("no topic")}); err != nil {
		t.Fatal(err)
	}
	sk := &sink{}
	r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := r.Drain(context.Background()); err != nil || n != 0 {
		t.Errorf("Drain = (%d, %v), want (0, nil)", n, err)
	}
	if len(sk.published) != 0 {
		t.Error("a row with no topic was published to some default topic")
	}
	if got := s.FailureOf("row-1"); !errors.Is(got, outbox.ErrNoTopic) {
		t.Errorf("FailureOf = %v, want ErrNoTopic", got)
	}
}

// TestTwoRelaysClaimDisjointRows is the store contract the relay's
// correctness rests on.
func TestTwoRelaysClaimDisjointRows(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{LockFor: time.Hour})
	for range 10 {
		if _, err := s.Append(outbox.Row{Topic: "t", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.Claim(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Claim(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 4 || len(second) != 4 {
		t.Fatalf("claims returned %d and %d rows, want 4 each", len(first), len(second))
	}
	seen := map[string]bool{}
	for _, r := range append(first, second...) {
		if seen[r.ID] {
			t.Fatalf("row %s was claimed twice; two relays would publish every row twice", r.ID)
		}
		seen[r.ID] = true
	}
}

// TestALockLapses is the other half: a relay that dies holding a claim must
// not strand its rows for ever.
func TestALockLapses(t *testing.T) {
	clk := newFakeClock()
	s := newStore(t, outbox.MemoryOptions{LockFor: time.Minute, Clock: clk})
	if _, err := s.Append(outbox.Row{Topic: "t", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if rows, err := s.Claim(context.Background(), 1); err != nil || len(rows) != 1 {
		t.Fatalf("the first claim = (%d rows, %v)", len(rows), err)
	}
	// The relay "dies" here: nothing marks or releases.
	if rows, err := s.Claim(context.Background(), 1); err != nil || len(rows) != 0 {
		t.Fatalf("a locked row was claimed again immediately: (%d rows, %v)", len(rows), err)
	}
	clk.advance(2 * time.Minute)
	rows, err := s.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatal("the lock did not lapse; a crashed relay strands its rows for ever")
	}
	if rows[0].Attempts != 2 {
		t.Errorf("Attempts = %d after two claims, want 2: the attempt count is what tells a poison row from an unlucky one", rows[0].Attempts)
	}
}

func TestConcurrencyOneKeepsRowOrder(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	for i := range 50 {
		if _, err := s.Append(outbox.Row{Topic: "t", Body: []byte{byte('a' + i%26), byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	sk := &sink{}
	r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sk.published) != 50 {
		t.Fatalf("published %d, want 50", len(sk.published))
	}
	for i, m := range sk.published {
		if m.Body[1] != byte(i) {
			t.Fatalf("message %d carries sequence %d: Concurrency 1 must preserve row order", i, m.Body[1])
		}
	}
}

func TestRunStopsOnContextAndOnClose(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		s := newStore(t, outbox.MemoryOptions{})
		sk := &sink{}
		r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, Interval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := r.Run(ctx); err != nil {
			t.Errorf("Run = %v, want nil: a cancelled context is a clean stop and not a relay failure", err)
		}
	})
	t.Run("close", func(t *testing.T) {
		s := newStore(t, outbox.MemoryOptions{})
		sk := &sink{}
		r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, Interval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- r.Run(context.Background()) }()
		// Close is idempotent and stops Run.
		for i := range 3 {
			if err := r.Close(); err != nil {
				t.Errorf("Close #%d = %v", i+1, err)
			}
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run after Close = %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after Close")
		}
		if _, err := r.Drain(context.Background()); !errors.Is(err, outbox.ErrClosed) {
			t.Errorf("Drain after Close = %v, want ErrClosed", err)
		}
	})
	t.Run("a dead store is reported, not looped on", func(t *testing.T) {
		s := newStore(t, outbox.MemoryOptions{})
		if _, err := s.Append(outbox.Row{Topic: "t", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		s.SetUnavailable(true)
		sk := &sink{}
		r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, Interval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- r.Run(context.Background()) }()
		select {
		case err := <-done:
			if err == nil {
				t.Error("Run returned nil against a store that cannot be reached; a permanently broken database would hide behind a process that looks healthy")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return against an unavailable store")
		}
	})
}

func TestAppendCopiesTheCallersSlices(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	body := []byte("original")
	key := []byte("k1")
	var h mq.Headers
	h.Add("a", []byte("1"))
	if _, err := s.Append(outbox.Row{Topic: "t", Body: body, Key: key, Headers: h}); err != nil {
		t.Fatal(err)
	}
	// The caller reuses its buffers, which the specification allows.
	copy(body, "MUTATED!")
	copy(key, "XX")
	h.Set("a", []byte("9"))

	rows, err := s.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(rows[0].Body) != "original" {
		t.Errorf("Body = %q; Append aliased the caller's slice", rows[0].Body)
	}
	if string(rows[0].Key) != "k1" {
		t.Errorf("Key = %q; Append aliased the caller's slice", rows[0].Key)
	}
	if v, _ := rows[0].Headers.Get("a"); string(v) != "1" {
		t.Errorf("header a = %q; Append aliased the caller's Headers", v)
	}
}

func TestConfigRefusals(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	sk := &sink{}
	for name, tc := range map[string]struct {
		cfg  outbox.Config
		want error
	}{
		"no store":             {outbox.Config{Publish: sk.publish}, outbox.ErrNoStore},
		"no publisher":         {outbox.Config{Store: s}, outbox.ErrNoPublisher},
		"negative batch":       {outbox.Config{Store: s, Publish: sk.publish, Batch: -1}, outbox.ErrInvalidConfig},
		"negative interval":    {outbox.Config{Store: s, Publish: sk.publish, Interval: -1}, outbox.ErrInvalidConfig},
		"negative attempts":    {outbox.Config{Store: s, Publish: sk.publish, MaxAttempts: -1}, outbox.ErrInvalidConfig},
		"negative concurrency": {outbox.Config{Store: s, Publish: sk.publish, Concurrency: -1}, outbox.ErrInvalidConfig},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := outbox.New(tc.cfg); !errors.Is(err, tc.want) {
				t.Errorf("New = %v, want %v", err, tc.want)
			}
		})
	}
	if _, err := outbox.NewMemoryStore(outbox.MemoryOptions{LockFor: -1}); !errors.Is(err, outbox.ErrInvalidConfig) {
		t.Error("a negative LockFor was accepted")
	}
}

// ---------------------------------------------------------------- headers

func TestHeaderRoundTrip(t *testing.T) {
	cases := map[string]func() mq.Headers{
		"empty": func() mq.Headers { return mq.Headers{} },
		"one":   func() mq.Headers { var h mq.Headers; h.Add("a", []byte("1")); return h },
		"empty value": func() mq.Headers {
			var h mq.Headers
			h.Add("a", nil)
			return h
		},
		"duplicates": func() mq.Headers {
			var h mq.Headers
			h.Add("dup", []byte("first"))
			h.Add("dup", []byte("second"))
			h.Add("other", []byte("x"))
			h.Add("dup", []byte("third"))
			return h
		},
		"binary value": func() mq.Headers {
			var h mq.Headers
			h.Add("bin", []byte{0, 1, 0xff})
			return h
		},
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			in := mk()
			got, err := outbox.DecodeHeaders(outbox.EncodeHeaders(in))
			if err != nil {
				t.Fatal(err)
			}
			if got.Len() != in.Len() {
				t.Fatalf("decoded %d header(s), want %d", got.Len(), in.Len())
			}
			// Order AND duplicates, which is why this format is not JSON.
			var wantK, gotK []string
			var wantV, gotV [][]byte
			for k, v := range in.All() {
				wantK, wantV = append(wantK, k), append(wantV, v)
			}
			for k, v := range got.All() {
				gotK, gotV = append(gotK, k), append(gotV, v)
			}
			if strings.Join(gotK, ",") != strings.Join(wantK, ",") {
				t.Errorf("keys = %v, want %v in order", gotK, wantK)
			}
			for i := range wantV {
				if !bytes.Equal(gotV[i], wantV[i]) {
					t.Errorf("value %d = %q, want %q", i, gotV[i], wantV[i])
				}
			}
		})
	}
}

// TestDecodeHeadersRefusals: the blob comes out of a database column, so it
// is input and every length in it is a claim.
func TestDecodeHeadersRefusals(t *testing.T) {
	var h mq.Headers
	h.Add("a", []byte("1"))
	good := outbox.EncodeHeaders(h)
	cases := map[string][]byte{
		"three bytes":                {0, 0, 0},
		"count with no pairs":        {0, 0, 0, 1},
		"four billion pairs":         {0xFF, 0xFF, 0xFF, 0xFF},
		"key length past the end":    {0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0},
		"value length past the end":  {0, 0, 0, 1, 0, 0, 0, 1, 'a', 0xFF, 0xFF, 0xFF, 0xFF},
		"truncated":                  good[:len(good)-1],
		"trailing bytes":             append(bytes.Clone(good), 'x'),
		"count lower than the pairs": {0, 0, 0, 0, 0, 0, 0, 1, 'a', 0, 0, 0, 1, '1'},
	}
	for name, blob := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := outbox.DecodeHeaders(blob)
			if !errors.Is(err, outbox.ErrBadHeaders) {
				t.Errorf("DecodeHeaders = (%d headers, %v), want ErrBadHeaders", got.Len(), err)
			}
			if got.Len() != 0 {
				t.Errorf("a refused blob still produced %d header(s)", got.Len())
			}
		})
	}
	// nil and empty are not errors: a row with no headers has a NULL column.
	for name, blob := range map[string][]byte{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			if got, err := outbox.DecodeHeaders(blob); err != nil || got.Len() != 0 {
				t.Errorf("DecodeHeaders(%s) = (%d, %v), want (0, nil)", name, got.Len(), err)
			}
		})
	}
}

func TestErrorsCarryTheirCode(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want mq.ErrorCode
	}{
		{"ErrNoTopic", outbox.ErrNoTopic, mq.CodeInvalidArgument},
		{"ErrNoStore", outbox.ErrNoStore, mq.CodeInvalidConfig},
		{"ErrNoPublisher", outbox.ErrNoPublisher, mq.CodeInvalidConfig},
		{"ErrInvalidConfig", outbox.ErrInvalidConfig, mq.CodeInvalidConfig},
		{"ErrBadHeaders", outbox.ErrBadHeaders, mq.CodeCorruptMessage},
		{"ErrPoisonRow", outbox.ErrPoisonRow, mq.CodePoisonMessage},
		{"ErrClosed", outbox.ErrClosed, mq.CodeClosed},
	} {
		if got := mq.CodeOf(c.err); got != c.want {
			t.Errorf("CodeOf(%s) = %v, want %v", c.name, got, c.want)
		}
		if !strings.Contains(c.err.Error(), "iqmq/x/outbox") {
			t.Errorf("%s does not name its package: %q", c.name, c.err.Error())
		}
	}
}
