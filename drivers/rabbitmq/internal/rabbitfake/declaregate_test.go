package rabbitfake_test

import (
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestDeclareGateStepsOneDeclarationAtATime holds the gate to the two things a
// test using it depends on: a declaration parks and does NOT take effect until
// it is stepped, and the broker is readable while one is parked.
//
// An instrument nobody has watched fail is assumed not to work, and this one
// exists to make a mid-replay state assertable — so a gate that let everything
// through would make the test that uses it vacuous rather than red.
func TestDeclareGateStepsOneDeclarationAtATime(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	conn, err := broker.Dialer().Dial(t.Context(), []string{"fake://broker"}, client.DialConfig{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	gate := broker.HoldDeclarations()
	defer gate.Release()

	errs := make(chan error, 2)
	go func() {
		_, derr := ch.QueueDeclare("first", true, false, false, false, nil)
		errs <- derr
		_, derr = ch.QueueDeclare("second", true, false, false, false, nil)
		errs <- derr
	}()

	waitUntil(t, func() bool { return gate.Parked() == 1 })
	if got := len(broker.Queues()); got != 0 {
		t.Fatalf("the broker holds %d queues with the first declaration parked, want 0: a gate that parks after the declaration cannot show a mid-replay state", got)
	}

	gate.Step()
	waitUntil(t, func() bool { return gate.Passed() == 1 && gate.Parked() == 1 })
	if _, ok := broker.Queue("first"); !ok {
		t.Error("the stepped declaration did not take effect")
	}
	if _, ok := broker.Queue("second"); ok {
		t.Error("the parked declaration took effect: the gate is not gating")
	}

	gate.Release()
	for range 2 {
		if derr := <-errs; derr != nil {
			t.Fatalf("QueueDeclare: %v", derr)
		}
	}
	if _, ok := broker.Queue("second"); !ok {
		t.Error("the released declaration did not take effect")
	}
	if got := gate.Parked(); got != 0 {
		t.Errorf("%d declarations are still parked after Release", got)
	}

	// A released gate parks nothing, so a test that has finished stepping does
	// not have to keep stepping.
	if _, derr := ch.QueueDeclare("third", true, false, false, false, nil); derr != nil {
		t.Fatalf("QueueDeclare after Release: %v", derr)
	}
}

// TestDeclareGateIsReleasedByClose is the half that keeps a test which forgets
// to release from hanging for the whole package's timeout.
func TestDeclareGateIsReleasedByClose(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	conn, err := broker.Dialer().Dial(t.Context(), []string{"fake://broker"}, client.DialConfig{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	gate := broker.HoldDeclarations()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = ch.QueueDeclare("parked", true, false, false, false, nil)
	}()
	waitUntil(t, func() bool { return gate.Parked() == 1 })
	_ = broker.Close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close left a declaration parked at the gate")
	}
}

// waitUntil polls for a condition another goroutine establishes.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the condition did not hold within 10s")
}
