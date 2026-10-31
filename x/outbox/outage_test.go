package outbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/outbox"
)

// unavailable is what a publisher reports while its broker is down.
func unavailable() error {
	return &mq.OpError{Operation: "publish", Code: mq.CodeUnavailable, Retryable: true, Err: errPublish}
}

// TestAnOutageDoesNotPoisonRows: the store counts claims, and a claim while
// the broker is down is not the row's failure. The relay used to compare the
// claim count with MaxAttempts before publishing, so an outage that outlasted
// MaxAttempts passes dead-lettered every row it touched, for good.
func TestAnOutageDoesNotPoisonRows(t *testing.T) {
	for name, down := range map[string]func() error{
		"unavailable": unavailable,
		"ambiguous":   ambiguousByCode,
		"deadline":    func() error { return context.DeadlineExceeded },
		// Not the row's fault either: credentials revoked or a topic
		// being recreated fail every row alike, and so does a raw network
		// error from a custom PublishFunc, which carries no code at all.
		// The relay used to count these against the row, and Attempts
		// had grown through the outage, so the first one dead-lettered it.
		"unauthorized": func() error { return &mq.OpError{Operation: "publish", Code: mq.CodeUnauthorized, Err: errPublish} },
		"forbidden":    func() error { return &mq.OpError{Operation: "publish", Code: mq.CodeForbidden, Err: errPublish} },
		"not found":    func() error { return &mq.OpError{Operation: "publish", Code: mq.CodeNotFound, Err: errPublish} },
		"uncoded":      func() error { return errors.New("dial tcp 10.0.0.1:9092: connect: connection refused") },
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t, outbox.MemoryOptions{})
			append3(t, s)
			sk := &sink{fail: func(*mq.Message) error { return down() }}
			r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, MaxAttempts: 3})
			if err != nil {
				t.Fatal(err)
			}
			for range 10 {
				if _, err := r.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if st := r.Stats(); st.Poison != 0 {
				t.Fatalf("Stats.Poison = %d after an outage: rows the broker never refused were taken out of rotation", st.Poison)
			}
			sk.fail = nil
			if _, err := r.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := sk.bodies(); strings.Join(got, ",") != "one,two,three" {
				t.Errorf("published %v once the broker was back, want [one two three]", got)
			}
		})
	}
}

// TestARefusedRowDoesNotHoldBackTheRowsBehindIt: a broker that refuses one
// message for what it is fails it identically on every attempt, so the row is
// poison once it is over the bound and the rows behind it go out. A driver
// reports a header it cannot carry as CodeUnsupported, and the relay used to
// blame the broker for that: the row stayed at the head of every ordered
// claim, and nothing behind it was ever published.
func TestARefusedRowDoesNotHoldBackTheRowsBehindIt(t *testing.T) {
	for name, code := range map[string]mq.ErrorCode{
		"unsupported":      mq.CodeUnsupported,
		"invalid argument": mq.CodeInvalidArgument,
		"too large":        mq.CodeMessageTooLarge,
		"corrupt":          mq.CodeCorruptMessage,
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t, outbox.MemoryOptions{})
			append3(t, s)
			sk := &sink{fail: func(m *mq.Message) error {
				if string(m.Body) == "one" {
					return &mq.OpError{Operation: "publish", Code: code, Err: errPublish}
				}
				return nil
			}}
			r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, MaxAttempts: 3})
			if err != nil {
				t.Fatal(err)
			}
			for range 10 {
				if _, err := r.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if st := r.Stats(); st.Poison != 1 {
				t.Errorf("Stats.Poison = %d, want the refused row taken out of rotation", st.Poison)
			}
			if got := sk.bodies(); strings.Join(got, ",") != "two,three" {
				t.Errorf("published %v, want [two three]: the refused row held back the rows behind it", got)
			}
		})
	}
}

// TestARowTheMemoryBrokerCanNeverFitStaysInRotation pins where the memory
// broker's never-fit refusal stops. The broker refuses a record larger than
// any lane of the topic can ever hold with an ErrQueueFull that
// mq.IsRetryable reports as not retryable, because every retry is refused the
// same way. The relay decides by the code (brokerDown), and reads
// CodeResourceExhausted as the broker being down: the row is never poison,
// and on the ordered path nothing behind it is published. Making it a row
// fault needs a decision this test is the place to record: a code of its own
// for the refusal, or brokerDown honouring an explicit not-retryable mark,
// which would also poison the rows other drivers refuse that way while the
// broker is the problem, such as Pulsar's exceeded backlog quota.
func TestARowTheMemoryBrokerCanNeverFitStaysInRotation(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestConfig(memory.Config{
		DefaultTopology: memory.TopologyConfig{CapacityBytes: 8},
	}))
	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	s := newStore(t, outbox.MemoryOptions{})
	for _, body := range [][]byte{make([]byte, 64), []byte("two")} {
		if _, err := s.Append(outbox.Row{Topic: "orders", Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	publish := func(ctx context.Context, topic string, ms []*mq.Message, opts []mq.PublishOption) error {
		for _, m := range ms {
			if err := client.Publish(ctx, topic, m, opts...); err != nil {
				return err
			}
		}
		return nil
	}
	r, err := outbox.New(outbox.Config{Store: s, Publish: publish, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if _, err := r.Drain(ctx); err != nil {
			t.Fatal(err)
		}
	}
	got := s.FailureOf("row-1")
	if !errors.Is(got, mq.ErrQueueFull) || mq.IsRetryable(got) || mq.CodeOf(got) != mq.CodeResourceExhausted {
		t.Fatalf("the row's failure = %v (code %v, IsRetryable %v), want the never-fit refusal: "+
			"CodeResourceExhausted and not retryable", got, mq.CodeOf(got), mq.IsRetryable(got))
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatal(err)
	}
	if st := r.Stats(); st.Poison != 0 || st.Sent != 0 || snap.Published != 0 {
		t.Fatalf("Stats = %+v and the subscription was handed %d records: the code decides, so want the "+
			"row kept in rotation and nothing behind it published until the decision changes it",
			st, snap.Published)
	}
}

// TestARowOverTheBoundIsPoisonOnlyOnItsOwnFailure: a row that is over the
// bound because of an outage gets its attempt, and only a failure that is the
// row's own takes it out of rotation.
func TestARowOverTheBoundIsPoisonOnlyOnItsOwnFailure(t *testing.T) {
	s := newStore(t, outbox.MemoryOptions{})
	if _, err := s.Append(outbox.Row{Topic: "orders", Body: []byte("big")}); err != nil {
		t.Fatal(err)
	}
	sk := &sink{fail: func(*mq.Message) error { return unavailable() }}
	r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := r.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if st := r.Stats(); st.Poison != 0 {
		t.Fatalf("Stats.Poison = %d: the row was poisoned by claims made while the broker was down", st.Poison)
	}
	sk.fail = func(*mq.Message) error {
		return &mq.OpError{Operation: "publish", Code: mq.CodeMessageTooLarge, Err: errPublish}
	}
	if _, err := r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := s.FailureOf("row-1"); !errors.Is(got, outbox.ErrPoisonRow) {
		t.Errorf("FailureOf = %v, want ErrPoisonRow once the row failed for a reason of its own past the bound", got)
	}
}

// TestRunBacksOffWhileTheBrokerIsDown: a pass that met a broker that was down
// waits before the next, and longer each time. Run used to go straight back
// whenever a pass published anything, so one row for a dead topic behind a
// row that succeeded was claimed and failed as fast as the store answered.
func TestRunBacksOffWhileTheBrokerIsDown(t *testing.T) {
	clk := newFakeClock()
	s := newStore(t, outbox.MemoryOptions{Clock: clk})
	append3(t, s)
	sk := &sink{fail: func(m *mq.Message) error {
		if string(m.Body) != "one" {
			return unavailable()
		}
		return nil
	}}
	const interval = time.Second
	r, err := outbox.New(outbox.Config{Store: s, Publish: sk.publish, Interval: interval, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run = %v", err)
		}
	}()

	waitFor(t, "the first pass to start waiting", func() bool { return clk.pending() == 1 })
	if p := r.Stats().Passes; p != 1 {
		t.Fatalf("Passes = %d before the clock moved, want 1: a pass that met a down broker must wait", p)
	}
	// The second wait is two intervals: one interval fires nothing.
	clk.advance(interval)
	waitFor(t, "the second pass to start waiting", func() bool { return r.Stats().Passes == 2 && clk.pending() == 1 })
	clk.advance(interval)
	time.Sleep(20 * time.Millisecond)
	if p := r.Stats().Passes; p != 2 {
		t.Fatalf("Passes = %d one interval into the second wait, want 2: consecutive outages must back off", p)
	}
	clk.advance(interval)
	waitFor(t, "the third pass", func() bool { return r.Stats().Passes == 3 })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
