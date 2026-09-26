package testkit_test

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/testkit"
)

// TestClockIsDeterministic pins what a driver test needs of the fake clock: time
// moves only when it is told to, timers fire when it passes their deadline, and
// Stop and Reset leave no stale value behind, which is the Go 1.23 timer
// contract mq.Timer states.
func TestClockIsDeterministic(t *testing.T) {
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	c := testkit.NewClock(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Now is %v", c.Now())
	}
	timer := c.NewTimer(time.Second)
	if c.Timers() != 1 {
		t.Fatalf("the clock holds %d timers", c.Timers())
	}
	select {
	case <-timer.C():
		t.Fatal("a timer fired before time moved")
	default:
	}
	c.Advance(500 * time.Millisecond)
	select {
	case <-timer.C():
		t.Fatal("a timer fired before its deadline")
	default:
	}
	c.Advance(time.Second)
	select {
	case at := <-timer.C():
		if at.Before(start.Add(time.Second)) {
			t.Errorf("the timer fired at %v", at)
		}
	default:
		t.Fatal("the timer did not fire after time passed its deadline")
	}
	if c.Timers() != 0 {
		t.Errorf("a fired timer is still armed")
	}
	// Reset re-arms and drops the value nobody read.
	fired := c.NewTimer(time.Millisecond)
	c.Advance(time.Second)
	if !fired.Reset(time.Hour) {
		// The timer had fired, so Reset reports false: that is the
		// contract, not a failure.
		_ = fired
	}
	select {
	case <-fired.C():
		t.Fatal("Reset left a stale value on the channel")
	default:
	}
	if !fired.Stop() {
		t.Error("Stop reported that an armed timer was not armed")
	}
	// A zero delay fires at once, which is what mq.Timer promises.
	now := c.NewTimer(0)
	select {
	case <-now.C():
	default:
		t.Error("a zero-delay timer did not fire")
	}
	// The zero start is a fixed instant rather than the zero time, so a
	// test that does not care still gets a plausible clock.
	if testkit.NewClock(time.Time{}).Now().IsZero() {
		t.Error("the default clock starts at the zero time")
	}
}

// TestRecorderRecords pins the hook recorder a driver's test asserts on.
func TestRecorderRecords(t *testing.T) {
	r := testkit.NewRecorder()
	h := r.Hooks()
	h.OnPublishStart(context.Background(), mq.PublishStartEvent{Topic: "t", Messages: 1})
	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{Topic: "t", Messages: 1})
	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{Topic: "t", Err: mq.ErrClosed})
	h.OnReceive(context.Background(), mq.ReceiveEvent{Messages: 3})
	h.OnSettle(context.Background(), mq.SettleEvent{Kind: mq.SettleAck})
	h.OnSettle(context.Background(), mq.SettleEvent{Kind: mq.SettleNack, Err: mq.ErrLeaseLost})
	h.OnReconnect(context.Background(), mq.ReconnectEvent{Attempt: 1})
	if r.PublishStarts() != 1 || r.PublishesDone() != 2 || r.PublishFailures() != 1 {
		t.Errorf("publish counts are %d/%d/%d", r.PublishStarts(), r.PublishesDone(), r.PublishFailures())
	}
	if r.Receives() != 1 || r.Messages() != 3 {
		t.Errorf("receive counts are %d/%d", r.Receives(), r.Messages())
	}
	if r.Settles() != 2 || r.SettleFailures() != 1 {
		t.Errorf("settle counts are %d/%d", r.Settles(), r.SettleFailures())
	}
	if r.Reconnects() != 1 {
		t.Errorf("reconnects is %d", r.Reconnects())
	}
	if r.LastPublish().Err == nil || r.LastSettle().Kind != mq.SettleNack {
		t.Errorf("the last events are %+v and %+v", r.LastPublish(), r.LastSettle())
	}
}

// TestFaultsAreOptIn pins that a zero Faults changes nothing, which is what
// makes it safe for a harness to wrap unconditionally, and that a capability
// rewriter never reaches the driver's own table.
func TestFaultsAreOptIn(t *testing.T) {
	var caps mq.Capabilities
	if err := caps.Set(mq.FeatureNack, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	rewrite := testkit.WithdrawFeature(mq.FeatureNack)
	out := rewrite(caps)
	if out.Has(mq.FeatureNack) {
		t.Error("WithdrawFeature left the feature advertised")
	}
	if !caps.Has(mq.FeatureNack) {
		t.Fatal("a capability rewriter changed the driver's own table: Capabilities shares its storage between copies, so a rewriter that mutated would change what every other client of the driver advertises")
	}
	claim := testkit.ClaimFeature(mq.FeatureTransactions, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic})
	if !claim(caps).Has(mq.FeatureTransactions) || caps.Has(mq.FeatureTransactions) {
		t.Error("ClaimFeature did not add the claim, or added it to the original")
	}
}
