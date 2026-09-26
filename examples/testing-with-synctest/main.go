// Command testing-with-synctest is iqmq's deterministic-test story: a
// retrying consumer whose backoff is hours long, tested in milliseconds.
//
// There are two ways to make time pass in an iqmq test and this module shows
// both, because they answer different questions:
//
//   - A [testing/synctest] bubble with memory.NewForTest(t,
//     memory.TestRealClock()). The bubble's fake clock drives package time, and
//     the memory driver on the wall clock follows it, so every timer in the
//     runner and in the broker moves when the bubble decides nothing can make
//     progress. Nothing in the test names a duration it then has to wait for,
//     and a handle nobody closed surfaces as a bubble deadlock instead of as a
//     leaked goroutine nobody notices.
//   - memory.NewForTest(t) and Broker.Advance. The broker's clock is a
//     TestClock and moves only when the test says so, which is what a test of
//     the BROKER's own time wants — a lease that expires, a message past its
//     time to live — because Advance runs the sweeps the move implies before it
//     returns.
//
// What neither of them is: time.Sleep. A test that sleeps is slow when it
// passes and flaky when the host is busy, and this repository's own rule
// (the specification) is "fake clocks instead of sleeps".
//
// The program itself runs the same workflow on the real clock with a
// millisecond backoff, so that `go run .` finishes. The backoff is a parameter
// precisely so the tests can make it an hour.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

func main() {
	if err := demo(context.Background(), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// demo is main's body as a function that returns, so that log.Fatal is reached
// after the deferred shutdown has run rather than instead of it.
func demo(ctx context.Context, out io.Writer) error {
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()
	// Five milliseconds, so that the program a reader runs by hand takes
	// fifteen and not three hours. The tests pass an hour.
	return chargeWorkflow(ctx, out, client, 5*time.Millisecond)
}

// errCardDeclined is the transient failure the workflow retries. The specification retries
// a handler that returns any error; what makes this one transient is that the
// third attempt succeeds.
var errCardDeclined = errors.New("the card issuer is not answering")

// attemptsBeforeSuccess is how many attempts fail before one works. Three
// attempts total means two backoff waits, which is what the tests assert the
// elapsed time against.
const attemptsBeforeSuccess = 2

// chargeWorkflow publishes one charge, lets a handler fail it twice and
// succeed on the third attempt, and returns once it has succeeded.
//
// base is the first retry delay. The backoff is declared with NoJitter and an
// explicit Max, which is what makes the elapsed time a number a test can
// assert: the default is full jitter capped at DefaultRetryMax, so a caller
// naming an hour and nothing else would be capped to thirty seconds and then
// drawn at random inside it.
func chargeWorkflow(ctx context.Context, out io.Writer, client *mq.Client, base time.Duration) error {
	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "charges", Subscription: "billing"})
	if err != nil {
		return err
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	var attempts atomic.Int64
	var once sync.Once
	settled := make(chan struct{})
	go func() {
		_ = consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			n := attempts.Add(1)
			_, _ = fmt.Fprintf(out, "attempt %d for charge %s (broker attempt %d)\n", n, d.ID, d.Info().Attempt)
			if n <= attemptsBeforeSuccess {
				return errCardDeclined
			}
			_, _ = fmt.Fprintf(out, "charge %s captured\n", d.ID)
			once.Do(func() { close(settled) })
			return nil
		}),
			mq.WithConcurrency(1),
			mq.WithMaxAttempts(attemptsBeforeSuccess+1),
			mq.WithRetryBackoff(mq.ExponentialBackoff{
				Base:   base,
				Max:    24 * time.Hour,
				Growth: 2,
				Jitter: mq.NoJitter,
			}))
	}()

	if err := client.Publish(ctx, "charges", &mq.Message{ID: "ch-1", Body: []byte(`{"amount":1250}`)}); err != nil {
		return err
	}
	select {
	case <-settled:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
