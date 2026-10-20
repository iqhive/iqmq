// Command sqs-fifo is Amazon SQS's FIFO profile through
// github.com/iqhive/iqmq/drivers/sqs: message groups, deduplication, the
// visibility timeout, and what a blocked group looks like from the API.
//
// # The four things a FIFO queue does that a standard one does not
//
//   - **Order within a group.** mq.Message.Key becomes MessageGroupId, and the
//     service delivers one group in order. Across groups there is no order at
//     all, which is the whole design: a FIFO queue is as parallel as it has
//     groups.
//   - **Deduplication.** mq.DedupKey becomes MessageDeduplicationId, and the
//     service discards a second publication of the same identifier for five
//     minutes. The window is the service's and is not configurable. A
//     publication with NO DedupKey is given a unique identifier by the driver,
//     so it is never deduplicated by accident — which also means a retried
//     publication without one publishes a second message.
//   - **One in flight per group.** While a message of group G is invisible,
//     the service delivers nothing else from G. That is what makes the order
//     hold under concurrency, and it is why a receive can come back EMPTY from
//     a queue that is not: see blockedGroup below.
//   - **The visibility timeout as the lease.** A receive makes a message
//     invisible for Config.AckDeadline and returns a receipt handle. Nobody
//     settles it, the window passes, the message comes back. Everything above
//     rests on that one mechanism.
//
// # The lease the driver refuses to lose
//
// The service will accept a DeleteMessage for a receipt handle whose message
// another consumer now holds: there is no API that asks "do I still hold
// this?", the delete does not fail, and the message is gone. So the only place
// the refusal can happen is in the driver, BEFORE the call — and this driver
// compares the delivery's visibility deadline, as it recorded it at the
// receive, against now on the injected clock, and answers mq.ErrLeaseLost.
//
// It is a LOCAL view and it is deliberately pessimistic in both directions:
// it cannot see a window another process shortened, and it refuses on a window
// this process believes has passed even where the service would still have
// taken the delete. The cost is a redelivery; the alternative is deleting
// somebody else's work. leasePhase below walks a message through all three
// states — granted, extended, lost.
//
// # Why `pastTheWindow` is a parameter
//
// Nothing in this program sleeps. A visibility window that has to pass is the
// one thing a test cannot wait for without either a real second or a clock it
// controls, so the step that moves past it is the caller's: against a broker
// it is a wait, and against the in-process server of
// github.com/iqhive/iqmq/drivers/sqs/sqsfake it is Server.Advance, which
// expires the lease in microseconds and runs the sweeps the move implies. It
// is the same shape examples/testing-with-synctest uses for a retry backoff.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
)

// topicBase is the logical topic. A FIFO queue's name must end in ".fifo" and
// the driver appends it: topic "orders" is queue "orders.fifo" on the FIFO
// profile and queue "orders" on the standard one, which is why the two
// profiles are two clients and never one (the specification makes a capability table
// constant per client).
const topicBase = "orders"

// newTopic returns a topic name nothing else is using, and it is the one piece
// of this program that exists purely so that it can be run twice.
//
// A FIFO queue makes a shared name expensive in two ways a standard queue does
// not. A message left behind by an earlier run is delivered to this one, where
// it belongs to no group this program knows and breaks every assertion about
// what is in the queue; and the deduplication window is FIVE MINUTES and is
// not configurable, so a second run inside it has its first publication
// DISCARDED as a duplicate of the earlier run's. Both happened while this
// example was being written, against a real broker, in that order.
//
// The queue is deleted at the end, best-effort: a development broker should
// not accumulate one queue per run, and a failure to delete is not a failure
// of anything this program is demonstrating.
func newTopic() string {
	return topicBase + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// The message groups. Two that carry an order's lifecycle and one the lease
// demonstration owns, so that nothing in leasePhase can disturb the ordering
// claim.
const (
	groupA     = "acct-1"
	groupB     = "acct-2"
	groupLease = "acct-9"
)

// lifecycle is groupA's messages, in the order the service must deliver them.
var lifecycle = []string{"created", "paid", "shipped"}

// The two visibility windows this program uses, and why there are two.
//
// A visibility window is per CONSUMER — mq.ConsumerConfig.AckDeadline, which
// this driver turns into the receive's VisibilityTimeout — and the two halves
// of this example want opposite things from it:
//
//   - blockedGroup holds a message unsettled while it proves that the group is
//     blocked, so its window has to outlast every poll it makes. A window that
//     lapsed mid-proof would make the held message visible again, deliver it,
//     and turn the demonstration into a redelivery test. That is exactly what
//     happened when both halves shared a two-second window: against a real
//     broker the third poll produced the message the first one was still
//     holding.
//   - leasePhase has to WAIT the window out on a real broker, so a long one is
//     a long test.
//
// So they use two consumers. Nothing about that is a workaround: a service
// whose handlers take a minute and whose audit reader takes a second
// configures exactly this way.
const (
	// groupWindow outlasts three nothingPoll waits with room to spare.
	groupWindow = 20 * time.Second
	// leaseWindow is short because the broker arm really waits for it. The
	// service's own default is thirty seconds, and a program that waited for
	// that would look like a hang.
	leaseWindow = 2 * time.Second
	// nothingPoll is how long "nothing arrived" is given to be wrong. It is
	// three times Config.WaitTime, so the driver's pollers get three long
	// polls in: a zero is the service saying nothing rather than this program
	// not having asked.
	nothingPoll = 3 * time.Second
)

// extension is how much further leasePhase moves the window with
// Delivery.Extend.
const extension = 6 * time.Second

func main() {
	if err := demo(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// demo is main's body as a function that returns, so that os.Exit is reached
// after the deferred shutdown has run rather than instead of it.
func demo() error {
	addr := os.Getenv("IQMQ_ELASTICMQ_ADDR")
	if addr == "" {
		// conformance/harness's convention: IQMQ_<BROKER>_ADDR names an
		// externally managed broker as host:port. ElasticMQ's default port is
		// 9324 and it is the emulator this project's SQS evidence is taken
		// against.
		addr = "127.0.0.1:9324"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := open(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()
	return run(ctx, os.Stdout, client, sleepPast)
}

// open dials an SQS-compatible endpoint at addr on the FIFO profile.
//
// The region is the emulator's own name rather than a real one: the SDK needs
// a region to sign a request and no emulator checks it, so naming a real one
// would suggest this program says something about that region. For the managed
// service, drop Endpoint and InsecureTransport, put a real region here, and
// use sqs.OpenWithClient with an *sqs.Client built from the SDK's default
// credential chain — the chain lives in a module this driver deliberately does
// not depend on.
func open(ctx context.Context, addr string) (*mq.Client, error) {
	endpoint := addr
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	return sq.Open(ctx, Config(func(cfg *sq.Config) {
		cfg.Endpoint = endpoint
		cfg.InsecureTransport = true
	}))
}

// Config is the configuration both arms share, so that the broker run and the
// in-process run cannot drift apart in anything but the endpoint and the
// clock.
//
// It is exported because the test builds its own client over the in-process
// server and must not fork the settings to do it.
func Config(mutate ...func(*sq.Config)) sq.Config {
	//nolint:gosec // G101: the pair below is not a credential; an emulator accepts any pair and the SDK will not sign a request without one
	cfg := sq.Config{
		Region: "elasticmq",
		// Any credentials are accepted by both emulators. They are supplied
		// anyway because the SDK will not sign a request without them.
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "not-a-real-secret-and-not-a-real-key",
		// The specification forbids Open from creating production topology, and AutoCreate
		// is the caller saying "this is a development queue, make it": the
		// driver then creates orders.fifo with the FIFO attribute set.
		AutoCreate: true,
		FIFO:       true,
		// One message per receive, so that every step below is one message and
		// the blocked-group demonstration is unambiguous.
		MaxMessages: 1,
		// A one-second long poll. Every step that expects NOTHING to arrive
		// waits for the poll to come back, and the service's maximum of twenty
		// seconds would make those steps twenty seconds long.
		WaitTime:    time.Second,
		AckDeadline: groupWindow,
		// The margin the local lease check refuses inside. It is named rather
		// than left to the default because leasePhase asserts on the refusal:
		// a guard of a quarter second means a settlement in the last quarter
		// second of a window is refused, which a reader should see is a
		// deliberate trade and not a rounding error.
		LeaseGuard: 250 * time.Millisecond,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg
}

// sleepPast waits d out on the wall clock.
//
// It is the only wait in this module and it is the subject rather than a
// synchronisation: the visibility window is a real duration on a real broker,
// and there is no other way to be on the far side of it. The in-process arm
// passes sqsfake's Server.Advance instead.
func sleepPast(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run is the whole program.
//
// out is an io.Writer rather than the log package so that the test can read
// what this printed and compare it byte for byte. A service would use
// log/slog.
func run(ctx context.Context, out io.Writer, cl *mq.Client, pastTheWindow func(context.Context, time.Duration) error) error {
	info := cl.Info()
	// The mapping rule, shown on the logical name rather than on the
	// run-unique one below, so that a reader sees the rule and not a
	// timestamp. QueueName is a pure function: "/" becomes "-", the prefix is
	// prepended and ".fifo" is appended once.
	queue, err := sq.QueueName(topicBase, "", true)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "driver %s profile %q: topic %s is queue %s\n",
		info.Name, info.Profile, topicBase, queue)
	// The capability table is the client's, not the driver's: seven claims
	// differ between the two SQS profiles, and ordering by key is one of them.
	// A program that read "sqs" and planned against the standard profile's
	// table would be planning against a different queue.
	caps := cl.Capabilities()
	_, _ = fmt.Fprintf(out, "claims: ordering by key %v, deduplication %v\n",
		caps.Has(mq.FeatureOrderedByKey), caps.Has(mq.FeatureDeduplication))

	// A topic of this run's own: see newTopic.
	topic := newTopic()
	defer dropTopic(ctx, cl, topic)

	consumer, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "fulfilment"})
	if err != nil {
		return err
	}
	published, err := publish(ctx, cl, topic)
	if err != nil {
		return err
	}
	if err := blockedGroup(ctx, out, consumer, published); err != nil {
		return err
	}
	// The first consumer is shut down BEFORE the second is opened, and that
	// is not tidiness. SourceConfig.Subscription is IGNORED by this driver: a
	// queue has exactly one set of messages, so every consumer of one topic
	// COMPETES for them whatever its subscription says. Two consumers of
	// "orders" are two competitors and not two subscriptions — two
	// independent subscriptions need github.com/iqhive/iqmq/drivers/snssqs,
	// where the topic is an SNS topic and the subscription is a queue. Left
	// open, this one would steal the lease phase's message about half the
	// time, which is how this example first failed.
	if err := consumer.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutting the first consumer down: %w", err)
	}
	return leasePhase(ctx, out, cl, topic, pastTheWindow)
}

// publish sends five publications of which the service keeps four, and returns
// how many it sent.
//
// The duplicate is the point. Both copies carry the same MessageDeduplicationId
// — mq.DedupKey — and the service discards the second for five minutes. The
// proof is arithmetic rather than an API call: the queue yields four messages
// below, so exactly one publication was discarded.
func publish(ctx context.Context, cl *mq.Client, topic string) (int, error) {
	pub, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		return 0, err
	}
	// The publisher is not closed here: the specification makes Client.Shutdown close
	// every handle the client opened, publishers before consumers drain.
	sent := 0
	send := func(group, event string) error {
		sent++
		// Key becomes MessageGroupId. A FIFO queue REQUIRES one, so a keyless
		// publication is sent under sqs.DefaultGroupID — which puts every
		// keyless message in ONE group, in order, one in flight at a time.
		// That is a throughput cliff rather than an error, and it is why a
		// FIFO publication without a key is almost always a mistake.
		m := &mq.Message{
			Key:  []byte(group),
			Body: []byte(event),
			ID:   group + "/" + event,
		}
		// DedupKey becomes MessageDeduplicationId. Without one the driver
		// generates a unique identifier per publication, so a retry would
		// publish a second message; with one, a retry is discarded by the
		// service. It is the difference between at-least-once and
		// once-per-identifier publication.
		return pub.Publish(ctx, m, mq.DedupKey(group+"/"+event))
	}
	for _, event := range lifecycle {
		if err := send(groupA, event); err != nil {
			return sent, fmt.Errorf("publishing %s/%s: %w", groupA, event, err)
		}
	}
	// The duplicate: the same group, the same event, therefore the same dedup
	// identifier.
	if err := send(groupA, lifecycle[0]); err != nil {
		return sent, err
	}
	if err := send(groupB, lifecycle[0]); err != nil {
		return sent, err
	}
	return sent, nil
}

// blockedGroup drains the queue one message at a time and prints the three
// things the draining shows: the order inside a group, a group that is blocked
// by its own in-flight message, and the deduplication.
func blockedGroup(ctx context.Context, out io.Writer, consumer *mq.ConsumerHandle, published int) error {
	// 1. The head of groupA, held UNSETTLED. Everything below depends on it
	//    staying in flight.
	first, err := receive(ctx, consumer)
	if err != nil {
		return err
	}
	if got := string(first.Key); got != groupA {
		return fmt.Errorf("the first message came from group %q, want %q: the oldest publication is the head of the queue", got, groupA)
	}
	if got := string(first.Body); got != lifecycle[0] {
		return fmt.Errorf("the first message of %s is %q, want %q", groupA, got, lifecycle[0])
	}

	// 2. With groupA's head in flight, the next receive cannot produce
	//    groupA's second message — and it does not block either, because
	//    groupB is a different group. THIS is the sentence the example exists
	//    to print.
	other, err := receive(ctx, consumer)
	if err != nil {
		return err
	}
	if got := string(other.Key); got != groupB {
		return fmt.Errorf("while %s/%s was in flight a receive produced group %q; a FIFO group must hold its later messages until the one in flight is settled", groupA, lifecycle[0], got)
	}
	_, _ = fmt.Fprintf(out, "in flight: %s/%s held unsettled, so the next receive produced %s/%s instead of %s's second message\n",
		groupA, lifecycle[0], groupB, string(other.Body), groupA)
	if err := other.Ack(); err != nil {
		return fmt.Errorf("acking %s: %w", string(other.Key), err)
	}

	// 3. And now nothing at all: groupA is blocked and groupB is empty, while
	//    the queue still holds groupA's two remaining messages. A receive that
	//    returns nothing is what a blocked group looks like through the
	//    portable API — there is no error, no code and no signal, which is
	//    worth knowing before somebody reads an empty poll as an empty queue.
	blocked, err := receiveNothing(ctx, consumer)
	if err != nil {
		return err
	}
	if blocked != 0 {
		return fmt.Errorf("a receive produced %d message(s) while %s was blocked and %s was empty", blocked, groupA, groupB)
	}
	_, _ = fmt.Fprintf(out, "blocked: with %s in flight and %s empty, a receive produced nothing though the queue still holds %d message(s)\n",
		groupA, groupB, len(lifecycle)-1)

	// 4. Settle the head, and groupA moves again — in order.
	if err := first.Ack(); err != nil {
		return fmt.Errorf("acking the head of %s: %w", groupA, err)
	}
	seen := []string{string(first.Body)}
	for range len(lifecycle) - 1 {
		d, err := receive(ctx, consumer)
		if err != nil {
			return err
		}
		if got := string(d.Key); got != groupA {
			return fmt.Errorf("the queue produced group %q, want %q: nothing else is left", got, groupA)
		}
		seen = append(seen, string(d.Body))
		if err := d.Ack(); err != nil {
			return err
		}
	}
	for i, event := range lifecycle {
		if seen[i] != event {
			return fmt.Errorf("group %s was delivered as %v, want %v", groupA, seen, lifecycle)
		}
	}
	_, _ = fmt.Fprintf(out, "order: %s delivered %s, in publication order\n", groupA, strings.Join(seen, ","))

	// 5. The arithmetic that proves the deduplication. One more receive must
	//    produce nothing: every publication that survived has been settled.
	left, err := receiveNothing(ctx, consumer)
	if err != nil {
		return err
	}
	if left != 0 {
		return fmt.Errorf("%d message(s) are still in the queue: the duplicate publication was NOT discarded, so deduplication is not doing what this example says it does", left)
	}
	delivered := len(lifecycle) + 1
	_, _ = fmt.Fprintf(out, "dedup: %d publications produced %d messages; the second copy of dedup id %s/%s was discarded by the service\n",
		published, delivered, groupA, lifecycle[0])
	return nil
}

// leasePhase walks one message through all three states of a visibility
// window: granted, extended, and lost.
func leasePhase(ctx context.Context, out io.Writer, cl *mq.Client, topic string,
	pastTheWindow func(context.Context, time.Duration) error,
) error {
	// Its own consumer, with the short window: an acknowledgement deadline is
	// per consumer (AckDeadline), and the two halves of this example
	// want opposite things from it. See groupWindow.
	consumer, err := cl.Consumer(ctx, mq.ConsumerConfig{
		Topic: topic, Subscription: "lease-demo", AckDeadline: leaseWindow,
	})
	if err != nil {
		return err
	}
	defer func() { _ = consumer.Shutdown(ctx) }()

	m := &mq.Message{Key: []byte(groupLease), Body: []byte("lease-demo"), ID: groupLease + "/lease-demo"}
	if err := cl.Publish(ctx, topic, m, mq.DedupKey(groupLease+"/lease-demo")); err != nil {
		return err
	}
	d, err := receive(ctx, consumer)
	if err != nil {
		return err
	}
	info := d.Info()
	if info.LeaseExpires.IsZero() {
		return errors.New("the delivery reports no lease; a FIFO receive always grants a visibility window")
	}
	// Rounded to the second, because the printed line must be the same on
	// every run: the remaining window is the configured one minus however
	// long the receive took.
	_, _ = fmt.Fprintf(out, "lease: granted for about %v, this consumer's own AckDeadline\n", info.LeaseExpires.Sub(info.ReceivedAt).Round(time.Second))

	// Extend moves the broker's window. It is NOT a settlement: it takes no
	// generation from the state machine, so it neither wins nor loses the
	// "exactly one settlement wins" race, and the delivery is exactly as
	// settleable afterwards.
	//
	// What it does NOT do is tell the caller what it bought. DeliveryInfo is
	// the snapshot the driver handed over at the receive, so LeaseExpires
	// still reports the original deadline after a successful extension, and
	// the SPI's Extend returns only an error — there is no
	// "the window now ends at" anywhere in the API. A caller that needs to
	// know has to assume the documented minimum, which is why the wait below
	// is computed from the configuration rather than read from the delivery.
	if err := d.Extend(extension); err != nil {
		return fmt.Errorf("extending the lease: %w", err)
	}
	if moved := d.Info().LeaseExpires; !moved.Equal(info.LeaseExpires) {
		return fmt.Errorf("Info().LeaseExpires moved from %v to %v after Extend: the API has grown a way to read an extended deadline, so the specification is closed and this example should read it", info.LeaseExpires, moved)
	}
	_, _ = fmt.Fprintf(out, "lease: extended by %v, and Info().LeaseExpires still reports the original deadline — a DeliveryInfo is the snapshot the receive handed over \n", extension)

	// And now the window passes with nothing settled. The wait is the
	// original window plus the extension plus a second, which is the
	// documented minimum and a second: an extension moves the deadline at
	// least `extension` beyond the one it had, so the window cannot end
	// before leaseWindow+extension after the receive.
	//
	// It used to be deliberately longer than it had to be, because this
	// driver's Extend set the visibility timeout to `extension` from NOW
	// rather than `extension` beyond the deadline and the real window ended
	// sooner than the minimum. It does not any more, and the wait is now the
	// bound rather than an over-wait (closing, and TestExtendDoesNotShortenThisDriversLease).
	if err := pastTheWindow(ctx, leaseWindow+extension+time.Second); err != nil {
		return err
	}
	err = d.Ack()
	if !errors.Is(err, mq.ErrLeaseLost) {
		return fmt.Errorf("a settlement after the visibility window reported the wrong error, want mq.ErrLeaseLost; the driver's local lease check is what stops a delete removing another consumer's work: %w", err)
	}
	_, _ = fmt.Fprintln(out, "lease: a settlement after the window reports ErrLeaseLost, refused by the driver before the delete leaves the process")

	// The message is therefore still in the queue, and it comes back as a
	// redelivery. An unsettled delivery is not a lost message; it is a later
	// one.
	//
	// The loop is not defensive decoration. The driver's pollers keep
	// fetching while the consumer is open, so during the wait above this
	// message was received, left unsettled, expired and received again, once
	// per window — its ATTEMPT COUNT therefore grows with wall time, and one
	// or two of those deliveries are sitting in the prefetch buffer with
	// windows that have already lapsed. Settling one of those reports
	// ErrLeaseLost exactly as the first settlement did, which is correct and
	// is why the loop asks for the next one instead of failing. It is also
	// the mechanism a redrive policy's maxReceiveCount counts: a message
	// nobody settles reaches the dead-letter queue on a timer, not on a
	// decision.
	for {
		again, err := receive(ctx, consumer)
		if err != nil {
			return fmt.Errorf("the message did not come back after its window passed: %w", err)
		}
		back := again.Info()
		if !back.Redelivered || back.Attempt < 2 {
			return fmt.Errorf("the message came back as attempt %d (redelivered=%v), want attempt 2 or more and redelivered", back.Attempt, back.Redelivered)
		}
		err = again.Ack()
		if err == nil {
			// The attempt NUMBER is deliberately not printed: it is a
			// function of how long the wait above really took, and an
			// example whose expected output contained it would fail at
			// random on a busy machine.
			_, _ = fmt.Fprintf(out, "redelivery: %s came back marked as a redelivery, on a later attempt, and was settled\n", again.ID)
			return nil
		}
		if !errors.Is(err, mq.ErrLeaseLost) {
			return fmt.Errorf("settling the redelivery: %w", err)
		}
	}
}

// receive takes exactly one delivery, bounded so that a message that never
// arrives is an error rather than a hang.
//
// The bound is what makes every step above falsifiable: a check that hangs
// instead of failing is worse than no check, because the log says "timed out"
// and names no assertion.
func receive(ctx context.Context, c *mq.ConsumerHandle) (*mq.Delivery, error) {
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(rctx, dst)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, fmt.Errorf("a bounded receive returned %d messages, want 1", n)
	}
	return &dst[0], nil
}

// receiveNothing is the opposite: it reports how many messages a bounded
// receive produced, and expects the answer to be none.
//
// The bound is nothingPoll, and a deadline that passes is the ANSWER here
// rather than a failure — which is why context.DeadlineExceeded is not an
// error, and why this function is the slowest part of the example.
func receiveNothing(ctx context.Context, c *mq.ConsumerHandle) (int, error) {
	rctx, cancel := context.WithTimeout(ctx, nothingPoll)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(rctx, dst)
	// Anything that did arrive is settled, so that a failing check does not
	// leave a message on a lease nobody will release. The slice is trimmed
	// with min rather than indexed to n because the specification makes n the count
	// Receive filled, and a driver that over-reported it would index past the
	// slice — a driver defect this example should not turn into a panic.
	for i := range dst[:min(n, len(dst))] {
		_ = dst[i].Ack()
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return n, err
	}
	return n, nil
}

// dropTopic deletes the run's queue, best-effort.
//
// It is deliberately not an error: the destructive operations are explicit
// and this one is the program tidying up after itself, not part of what it
// demonstrates. A broker that refuses it leaves one queue behind, which a
// reader can see and delete; a program that failed here would be failing for
// something that has nothing to do with FIFO.
func dropTopic(ctx context.Context, cl *mq.Client, topic string) {
	adm, err := cl.Admin()
	if err != nil {
		return
	}
	_ = adm.DeleteTopic(ctx, topic)
}
