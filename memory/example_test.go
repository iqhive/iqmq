package memory_test

import (
	"context"
	"fmt"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// Example is the memory driver's own Open: the direct typed form, with the
// driver's Config rather than a DSN.
//
// AutoCreate is false by default, the same as every network driver, because
// The specification forbids Open from creating production topology. A test wants the
// opposite, which is what NewForTest is for.
func Example() {
	ctx := context.Background()
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Shutdown(ctx) }()

	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("hello")}); err != nil {
		panic(err)
	}
	fmt.Println(client.Info().Name, "is up")
	// Output: memory is up
}

// Example_dsn is the other form: a DSN, for a program whose broker is chosen
// by configuration rather than by an import.
//
// The typed Config is what production should use — a DSN is a string and a
// typo in one is a runtime failure — but a DSN is what a deployment can hand a
// process, and every driver parses its own.
func Example_dsn() {
	ctx := context.Background()
	client, err := memory.OpenDSN(ctx, "memory://?autocreate=true")
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Shutdown(ctx) }()
	fmt.Println("profile:", client.Info().Profile == "")
	// Output: profile: true
}

// Example_testing is the memory-testing workflow: a broker whose clock only
// moves when the test says so, which is what makes a delay assertable without
// a sleep.
//
// Broker.Advance is the only way time moves, and it is deliberately the
// broker's rather than the clock's: moving the clock alone would leave the
// broker in a state no real passage of time produces, because the sweeps a
// move implies — times to live, retained ages, lease deadlines — and the
// consumer wake-ups they cause happen inside Advance.
//
// A test that would rather not drive time at all runs under testing/synctest
// instead: the default wall clock follows the bubble clock, and a forgotten
// Close surfaces as a bubble deadlock. In a real test, memory.NewForTest gives
// all of this plus the cleanup and the leak check in one call.
func Example_testing() {
	ctx := context.Background()
	broker, err := memory.NewBroker(memory.Config{
		AutoCreate: true,
		Clock:      memory.NewTestClock(time.Time{}),
	})
	if err != nil {
		panic(err)
	}
	client, err := memory.OpenWithClient(ctx, broker, memory.ClientOwned)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		panic(err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("later")}, mq.Delay(time.Hour)); err != nil {
		panic(err)
	}

	// A poll, not a wait: Drain is bounded by its context and nothing else,
	// so asking it for a message that is not there yet with a live context
	// would block for ever.
	poll, cancel := context.WithCancel(ctx)
	cancel()
	before, _ := memory.Drain(poll, consumer, 1)
	fmt.Println("before the hour:", len(before))

	if err := broker.Advance(time.Hour); err != nil {
		panic(err)
	}
	after, err := memory.Drain(ctx, consumer, 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("after the hour: %d (%s)\n", len(after), after[0].Body)
	// Output:
	// before the hour: 0
	// after the hour: 1 (later)
}
