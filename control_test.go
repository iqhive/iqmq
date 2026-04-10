package iqmq_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/clock"
)

// TestControlErrors holds the control errors: what a handler returns to
// override the configured failure policy, and what the runner reads back.
func TestControlErrors(t *testing.T) {
	boom := errors.New("handler failed")
	cases := []struct {
		name  string
		err   error
		want  mq.Directive
		delay time.Duration
	}{
		{"RetryAfter", mq.RetryAfter(boom, 5*time.Second), mq.DirectiveRetryAfter, 5 * time.Second},
		{"DeadLetter", mq.DeadLetter(boom), mq.DirectiveDeadLetter, 0},
		{"Discard", mq.Discard(boom), mq.DirectiveDiscard, 0},
		{"Halt", mq.Halt(boom), mq.DirectiveHalt, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, delay := mq.DirectiveOf(c.err)
			if got != c.want || delay != c.delay {
				t.Fatalf("DirectiveOf = (%v, %v), want (%v, %v)", got, delay, c.want, c.delay)
			}
			// The handler's own error has to stay reachable: it is what gets
			// logged, counted and written into the dead-letter provenance
			// headers.
			if !errors.Is(c.err, boom) {
				t.Fatal("the handler's error is not reachable through the control error")
			}
			if !strings.Contains(c.err.Error(), boom.Error()) {
				t.Fatalf("the message dropped the handler's error: %q", c.err.Error())
			}
			var ce *mq.ControlError
			if !errors.As(c.err, &ce) || ce.Directive != c.want {
				t.Fatalf("errors.As did not find the directive: %v", c.err)
			}
		})
	}

	t.Run("NoDirective", func(t *testing.T) {
		for _, err := range []error{nil, boom, mq.ErrClosed, errors.Join(boom, mq.ErrLeaseLost)} {
			if got, delay := mq.DirectiveOf(err); got != mq.DirectiveNone || delay != 0 {
				t.Errorf("DirectiveOf(%v) = (%v, %v), want (DirectiveNone, 0)", err, got, delay)
			}
		}
	})

	t.Run("SurvivesWrapping", func(t *testing.T) {
		inner := mq.RetryAfter(boom, time.Second)
		wrapped := errors.Join(errors.New("middleware"), inner)
		if got, d := mq.DirectiveOf(wrapped); got != mq.DirectiveRetryAfter || d != time.Second {
			t.Fatalf("DirectiveOf through a wrap = (%v, %v)", got, d)
		}
	})

	t.Run("OutermostWins", func(t *testing.T) {
		// A middleware that overrides a handler's instruction is the one that
		// decided, so the outermost directive is the answer.
		inner := mq.RetryAfter(boom, time.Second)
		outer := mq.DeadLetter(inner)
		if got, _ := mq.DirectiveOf(outer); got != mq.DirectiveDeadLetter {
			t.Fatalf("DirectiveOf = %v, want the outermost directive DirectiveDeadLetter", got)
		}
	})

	t.Run("NegativeDelayIsZero", func(t *testing.T) {
		if _, d := mq.DirectiveOf(mq.RetryAfter(boom, -time.Second)); d != 0 {
			t.Fatalf("a negative delay came back as %v, want 0", d)
		}
	})

	t.Run("NilHandlerError", func(t *testing.T) {
		// A handler may ask for a directive without an error of its own.
		err := mq.Discard(nil)
		if got, _ := mq.DirectiveOf(err); got != mq.DirectiveDiscard {
			t.Fatalf("DirectiveOf = %v", got)
		}
		if err.Error() == "" {
			t.Fatal("a directive with no wrapped error renders as an empty message")
		}
	})

	t.Run("CodeComesFromTheWrappedError", func(t *testing.T) {
		// A directive is an instruction, not a failure category, so it does
		// not invent a code — except Halt with nothing wrapped, which is the poison-message outcome.
		if got := mq.CodeOf(mq.DeadLetter(mq.ErrMessageTooLarge)); got != mq.CodeMessageTooLarge {
			t.Errorf("CodeOf(DeadLetter(ErrMessageTooLarge)) = %v, want CodeMessageTooLarge", got)
		}
		if got := mq.CodeOf(mq.Discard(errors.New("plain"))); got != mq.CodeUnknown {
			t.Errorf("CodeOf(Discard(plain)) = %v, want CodeUnknown", got)
		}
		if got := mq.CodeOf(mq.Halt(nil)); got != mq.CodePoisonMessage {
			t.Errorf("CodeOf(Halt(nil)) = %v, want CodePoisonMessage ", got)
		}
		if !errors.Is(mq.Halt(nil), mq.ErrPoisonMessage) {
			t.Error("Halt(nil) does not match ErrPoisonMessage")
		}
	})

	t.Run("DirectiveNames", func(t *testing.T) {
		seen := map[string]bool{}
		for _, d := range []mq.Directive{
			mq.DirectiveNone, mq.DirectiveRetryAfter, mq.DirectiveDeadLetter,
			mq.DirectiveDiscard, mq.DirectiveHalt,
		} {
			if !d.Valid() || d.String() == "" || d.String() == "directive(?)" {
				t.Errorf("directive %d has no name", d)
			}
			if seen[d.String()] {
				t.Errorf("duplicate directive name %q", d.String())
			}
			seen[d.String()] = true
		}
		if mq.Directive(99).Valid() || mq.Directive(99).String() != "directive(?)" {
			t.Error("an out-of-range directive is not handled")
		}
	})
}

// TestValidateTopic checks that the root's validator is the portable grammar
// and that it reports the code.
func TestValidateTopic(t *testing.T) {
	// The portable grammar is segments of alphanumerics, dots,
	// underscores and hyphens, joined by "/".
	for _, name := range []string{
		"orders", "orders.eu", "a", "orders.eu.west-1",
		"tenant/ns/orders", "a_b-c.d",
	} {
		if err := mq.ValidateTopic(name); err != nil {
			t.Errorf("ValidateTopic(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range []string{
		"", "/orders", "orders/", "a//b", "orders eu", "orders+eu",
		".", "..", "a/./b", strings.Repeat("x", 256),
	} {
		err := mq.ValidateTopic(name)
		if err == nil {
			t.Errorf("ValidateTopic(%q) = nil, want a failure", name)
			continue
		}
		if !errors.Is(err, mq.ErrInvalidTopic) {
			t.Errorf("ValidateTopic(%q) = %v, does not match ErrInvalidTopic", name, err)
		}
		if got := mq.CodeOf(err); got != mq.CodeInvalidArgument {
			t.Errorf("ValidateTopic(%q) code = %v, want CodeInvalidArgument", name, got)
		}
	}
}

type clockAdapter struct{ c clock.Clock }

func (a clockAdapter) Now() time.Time { return a.c.Now() }

func (a clockAdapter) NewTimer(d time.Duration) mq.Timer { return a.c.NewTimer(d) }

// TestClockInterface pins that the root's Clock and Timer are satisfiable by
// internal/clock through the adapter, and that a Timer's Go 1.23 semantics
// survive the crossing.
func TestClockInterface(t *testing.T) {
	// internal/clock's Timer satisfies the root's Timer directly: the method
	// sets are identical and neither mentions the other interface.
	var _ mq.Timer = clock.Real{}.NewTimer(time.Second)

	fake := clock.NewFake(time.Unix(100, 0))
	var c mq.Clock = clockAdapter{fake}
	if got := c.Now(); !got.Equal(time.Unix(100, 0)) {
		t.Fatalf("Now = %v, want the fake clock's 100s", got)
	}
	tm := c.NewTimer(time.Second)
	if tm.C() == nil {
		t.Fatal("C returned a nil channel")
	}
	fake.Advance(2 * time.Second)
	select {
	case got := <-tm.C():
		if !got.Equal(time.Unix(101, 0)) {
			t.Fatalf("the timer fired at %v, want 101s", got)
		}
	default:
		t.Fatal("the timer did not fire after the clock passed its deadline")
	}
	if tm.Stop() {
		t.Fatal("Stop after firing reported that it stopped the timer")
	}
	if !tm.Reset(time.Second) {
		t.Log("Reset on a fired timer reports it had not been active, which is time.Timer's own answer")
	}

	// And the root's interfaces are satisfiable by a caller who never sees
	// internal/clock at all, which is the point of declaring them.
	var _ mq.Clock = stoppedClock{}
}

// stoppedClock is a Clock a caller outside the module could write.
type stoppedClock struct{}

func (stoppedClock) Now() time.Time { return time.Unix(0, 0) }

func (stoppedClock) NewTimer(time.Duration) mq.Timer { return neverTimer{} }

type neverTimer struct{}

func (neverTimer) C() <-chan time.Time      { return nil }
func (neverTimer) Stop() bool               { return false }
func (neverTimer) Reset(time.Duration) bool { return false }

// TestPublishMiddlewareComposes checks that the publish-side types
// compose in the documented order. There is no Chain for them at v0 — the
// handler-side Chain is settled with Delivery at — so the composition a
// caller writes is the composition tested.
func TestPublishMiddlewareComposes(t *testing.T) {
	var order []string
	mw := func(name string) mq.PublishMiddleware {
		return func(next mq.PublishFunc) mq.PublishFunc {
			return func(ctx context.Context, topic string, ms []*mq.Message) error {
				order = append(order, "->"+name)
				err := next(ctx, topic, ms)
				order = append(order, "<-"+name)
				return err
			}
		}
	}
	var base mq.PublishFunc = func(_ context.Context, topic string, ms []*mq.Message) error {
		order = append(order, "publish "+topic)
		if len(ms) != 1 {
			t.Fatalf("the batch reached the sink with %d messages", len(ms))
		}
		return nil
	}
	chained := mw("a")(mw("b")(base))
	if err := chained(context.Background(), "orders", []*mq.Message{{Body: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	want := []string{"->a", "->b", "publish orders", "<-b", "<-a"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", order, want)
	}
}
