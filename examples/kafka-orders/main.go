// Command kafka-orders is a partitioned order pipeline over
// github.com/iqhive/iqmq/drivers/kafka: keyed publishing, a consumer group
// with concurrency, settlement the handler does itself, retry into a
// dead-letter topic, and a shutdown that leaves nothing in flight.
//
// # What it is really about
//
// Per-key order under concurrency. Kafka gives order inside a PARTITION and
// nothing across partitions, so "every event of one order arrives in order" is
// a property of two decisions together:
//
//   - the PRODUCER puts every event of an order in one partition, because it
//     publishes them under one key (Message.Key). Change the key and the
//     guarantee is gone, whatever the consumer does.
//   - the CONSUMER serialises a partition. The ordering machinery reads
//     DeliveryInfo.Partition and routes one partition to one lane, which is
//     the runner's default — so raising the concurrency to four processes four
//     partitions at once and never two events of one order at once.
//
// Both halves are checked below, and the test breaks each of them to prove the
// check can see it.
//
// # Retry, and what "explicit settlement" means beside it
//
// One order in the batch cannot be processed. Its handler returns an error
// every time, so the runner retries it with the configured backoff and
// bound and then dead-letters it.
//
// The other nine deliveries are acknowledged BY THE HANDLER, with
// Delivery.Ack, and the runner does not overrule a delivery the handler
// settled. That is worth showing beside the dead-letter route because
// the two are often assumed to be alternatives: mq.WithSettlement(
// mq.SettlementManual) really is an alternative — it makes the runner settle
// nothing, and with it the retry and dead-letter routes below would never run
// — while a handler that settles a delivery and returns nil keeps both.
//
// # Two things this example found, and why it takes the route it takes
//
// The dead-letter route here is mq.WithNativeDeadLetter() — the DRIVER's own,
// which for Kafka is a publication to Config.NackConfig.DeadLetterTopic — and
// not the mq.WithDeadLetter(topic). That is not a preference:
//
//  1. **mq.WithDeadLetter could not work on this driver, and now can.**
//     the route publishes a copy of the message stamped with five
//     provenance headers, all under the reserved iqmq- prefix, through the
//     ordinary publish path. This driver's Representable refused ANY header
//     under that prefix — the rule about a caller's headers, applied to the
//     core's own — so every dead-letter publication was refused before any
//     I/O, counted as a DeadLetterFailure, and the message was requeued and
//     redelivered for ever; five other drivers carried the same check.
//     driverkit.ReservedHeaderAllowed now admits exactly
//     those five keys and nothing else, on all twelve drivers, so the core
//     route is available here. This program keeps the
//     native route because that is one settlement and no publish-then-ack
//     window, and because finding 2 below is about the native route and is
//     what the example prints.
//     TestTheCoreDeadLetterRouteWorksOnThisDriver is the evidence, and it
//     fails the day the refusal comes back.
//  2. **The native route carries no reason and no attempt count.** It settles
//     with Nack(NackDeadLetter()), which names neither, so the driver stamps
//     the four origin headers and neither iqmq-attempts nor iqmq-failure. The
//     dead-lettered message says where it came from and not why it died.
//
// Both are printed rather than hidden: an example that quietly configured
// around a defect would be documentation for a library that does not exist.
// The first of the two is now fixed, and it was the pinned TEST rather than
// this paragraph that noticed — which is the reason the finding was written as
// a test in the first place.
//
// # The Kafka-specific part
//
// An acknowledgement completes an offset; the COMMIT that makes it durable
// happens on a ticker (Config.CommitInterval), at a rebalance and at Close.
// The at-least-once window is "everything completed since the last commit", so
// a process that dies in between redelivers those records. Backend.Commit
// closes that window to zero at a point of the caller's choosing, and this
// program calls it once the batch is done — which is what a checkpoint looks
// like.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// The topology. Three partitions is the smallest number that makes the
// ordering claim mean anything: with one partition every key is trivially in
// order, and the check would pass against a producer that ignored the key.
const (
	topicOrders  = "orders"
	topicDLQ     = "orders.dlq"
	subscription = "fulfilment"
	partitions   = 3
)

// The events of one order, in the order they must be seen.
var lifecycle = []string{"created", "paid", "shipped"}

// The order keys. Three that work and one that cannot be processed at all.
var (
	goodKeys = []string{"ord-1001", "ord-1002", "ord-1003"}
	// badKey's first event fails every attempt. It is one event and not three
	// because the interesting thing about it is the attempt count, and a
	// strictly ordered lane holds the rest of a key behind its first failure
	// anyway.
	badKey = "ord-bad"
)

// maxAttempts is the attempt bound: two failures and a third that is also a
// failure, after which the failure policy decides. It is named so that the
// printed attempt count and this number cannot drift apart.
const maxAttempts = 3

// errUnprocessable is what badKey's handler returns. It is an ordinary error:
// The specification retries a handler that returns any error, and what makes this one
// terminal is the attempt bound rather than the error's type. A handler that
// knows a message is poison says so with mq.DeadLetter(err) and skips the
// remaining attempts.
var errUnprocessable = errors.New("the order has no shipping address")

func main() {
	if err := demo(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// demo is main's body as a function that returns, so that os.Exit is reached
// after the deferred shutdown has run rather than instead of it.
func demo() error {
	brokers := os.Getenv("IQMQ_KAFKA_ADDR")
	if brokers == "" {
		// conformance/harness's convention: IQMQ_<BROKER>_ADDR names an
		// externally managed broker. Kafka's may be a comma-separated seed
		// list, which is why it is split rather than used whole.
		brokers = "127.0.0.1:9092"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client, backend, err := open(ctx, strings.Split(brokers, ","))
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()
	return run(ctx, os.Stdout, client, backend)
}

// open dials the cluster and returns the portable client and the driver's
// backend.
//
// Both, because two things this program does are the driver's own:
// Backend.Commit has no portable equivalent (nothing asks a broker to
// checkpoint a group), and mq.Client.Native() answers the *kgo.Client rather
// than the backend, so there is no route from a client back to the driver's
// type. That is deliberate in the SPI (ADR-003) and it means a program that
// needs both opens both.
func open(ctx context.Context, brokers []string) (*mq.Client, *kq.Backend, error) {
	backend, err := kq.OpenBackend(ctx, kq.Config{
		Brokers:  brokers,
		ClientID: "iqmq-kafka-orders",
		// AutoCreateTopics plus Partitions is how EnsureTopic below gets
		// three partitions: mq.TopicSpec carries only the portable
		// intersection — the name — and the count is the driver's own
		// configuration. A topic Kafka auto-creates on a
		// produce gets the CLUSTER's default instead, and a topic's partition
		// count is fixed when it is created, so a program that publishes
		// before it ensures is a program whose ordering story depends on a
		// broker setting it never looked at.
		// The DRIVER's dead-letter destination, not the core's. See the
		// package comment: mq.WithDeadLetter is refused by this driver.
		NackConfig:        nack.Config{DeadLetterTopic: topicDLQ},
		AutoCreateTopics:  true,
		Partitions:        partitions,
		ReplicationFactor: 1, // a single-broker cluster; -1 asks for the cluster default
		// Short intervals so that this program finishes while a reader is
		// watching. A service leaves them alone: the defaults are 250 ms for
		// the commit ticker and 45 s for the session.
		CommitInterval:    50 * time.Millisecond,
		SessionTimeout:    6 * time.Second,
		RebalanceTimeout:  10 * time.Second,
		HeartbeatInterval: 500 * time.Millisecond,
		FetchMaxWait:      50 * time.Millisecond,
		Linger:            -1, // no linger: this program publishes twelve records and waits
	})
	if err != nil {
		return nil, nil, err
	}
	return mq.NewClient(backend), backend, nil
}

// observation is what the handler recorded about one delivery.
type observation struct {
	// events is the event names of this key, in the order they were handled.
	events []string
	// partitions is every partition this key's events arrived on. More than
	// one means the producer did not keep the key together.
	partitions []int32
	// handled is how many times the handler was CALLED for this key. It is
	// the only attempt count an application can have when the retry happens
	// in process: see brokerAttempt.
	handled int
	// brokerAttempt is the highest DeliveryInfo.Attempt seen for this key —
	// the BROKER's count, which is not the runner's. A Kafka nack under the
	// default block-partition strategy holds the record in process and hands
	// it back to the handler without re-fetching it, so this stays at 1
	// however many times the specification retries. Nothing in the frozen API carries the
	// runner's own attempt number to a handler; the dead-letter route stamps
	// it on iqmq-attempts, and a handler that needs it counts for itself, as
	// this one does.
	brokerAttempt int
	// ackedByHandler counts the deliveries this key's handler settled itself.
	ackedByHandler int
}

// ledger is every key's observations, guarded because four workers write it.
type ledger struct {
	mu sync.Mutex
	m  map[string]*observation
}

func newLedger() *ledger { return &ledger{m: map[string]*observation{}} }

// record adds one delivery to the ledger.
func (l *ledger) record(key, event string, part int32, brokerAttempt int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	o := l.m[key]
	if o == nil {
		o = &observation{}
		l.m[key] = o
	}
	o.handled++
	// The event list records each event ONCE, however many attempts it takes:
	// the ordering claim is about the sequence of events, and appending a
	// retry would make a retried lane look like a reordered one.
	if !slices.Contains(o.events, event) {
		o.events = append(o.events, event)
	}
	if !slices.Contains(o.partitions, part) {
		o.partitions = append(o.partitions, part)
	}
	o.brokerAttempt = max(o.brokerAttempt, brokerAttempt)
}

// acked records that the handler settled a delivery of key itself.
func (l *ledger) acked(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if o := l.m[key]; o != nil {
		o.ackedByHandler++
	}
}

// snapshot copies the ledger, so the printing below reads a value nothing is
// still writing to.
func (l *ledger) snapshot() map[string]observation {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]observation, len(l.m))
	for k, o := range l.m {
		out[k] = observation{
			events:         slices.Clone(o.events),
			partitions:     slices.Clone(o.partitions),
			handled:        o.handled,
			brokerAttempt:  o.brokerAttempt,
			ackedByHandler: o.ackedByHandler,
		}
	}
	return out
}

// run is the whole pipeline.
//
// out is an io.Writer rather than the log package so that the test can read
// what this printed and compare it byte for byte. A service would use
// log/slog.
func run(ctx context.Context, out io.Writer, cl *mq.Client, backend *kq.Backend) error {
	if err := ensureTopics(ctx, cl); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "topology: %s with %d partition(s), dead letters to %s\n", topicOrders, partitions, topicDLQ)

	consumer, err := cl.Consumer(ctx, mq.ConsumerConfig{
		Topic:        topicOrders,
		Subscription: subscription,
		// StartEarliest so that a run against a broker that already has the
		// topic reads the records this run publishes rather than joining at
		// the end of a log somebody else wrote. A service picks the position
		// its semantics need; for a group that has committed offsets before,
		// the committed position wins either way.
		Start: mq.StartEarliest(),
	})
	if err != nil {
		return err
	}

	l := newLedger()
	// done is closed once every delivery this program expects has been
	// handled: the nine good events plus maxAttempts attempts of the bad one.
	// Counting is what ends the program — nothing here sleeps, and a message
	// that never arrives ends it through ctx instead.
	wantDeliveries := len(lifecycle)*len(goodKeys) + maxAttempts
	done := make(chan struct{})
	var once sync.Once
	var handled int
	var handledMu sync.Mutex

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	runErr := make(chan error, 1)
	go func() {
		runErr <- consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			key := string(d.Key)
			event, _ := d.Headers.Get("event")
			info := d.Info()
			l.record(key, string(event), info.Partition, int(info.Attempt))

			// counted says this delivery's BOOKKEEPING is finished, and it
			// is called at the end of each branch rather than here. It used
			// to be called before the acknowledgement below, so the last
			// good delivery closed `done` while its own l.acked had not run
			// yet: the main goroutine went on to shut down and read the
			// ledger, and reported "the handler acked 8 deliveries itself,
			// want 9". On a loaded host that lost the race often enough to
			// make this example's own test the most frequent failure in
			// `make ci`.
			counted := func() {
				handledMu.Lock()
				defer handledMu.Unlock()
				handled++
				if handled >= wantDeliveries {
					// once.Do is not defensive decoration: the specification redelivers
					// a message whose handler failed, so a handler that
					// closes a channel has to be written as though it may
					// be called again.
					once.Do(func() { close(done) })
				}
			}

			if key == badKey {
				// No settlement: the runner owns the failure, and what it
				// does with it is the retry-then-dead-letter policy
				// configured below.
				counted()
				return errUnprocessable
			}
			// Explicit settlement. The runner does not overrule a delivery
			// the handler settled, so returning nil after this is not
			// a second acknowledgement — it is how the handler says the work
			// succeeded. A handler that acked and then returned an error
			// would keep the acknowledgement and be counted as a failure,
			// which is occasionally what a caller means.
			if err := d.Ack(); err != nil {
				return err
			}
			l.acked(key)
			counted()
			return nil
		}),
			// Four workers over three partitions. The ordering option is
			// WithOrderingByKey with no argument, which selects the specification's
			// StrictOrdering: a failed delivery blocks ITS LANE until it is
			// settled or terminally routed, and the other lanes keep moving —
			// which is why the three good orders finish while ord-bad is
			// still being retried.
			mq.WithConcurrency(4),
			mq.WithOrderingByKey(),
			mq.WithMaxAttempts(maxAttempts),
			// NoJitter and an explicit Max make the retry schedule a number
			// rather than a draw: the default is full jitter capped at
			// ADR-011's 30 s, and a program that wants to finish while a
			// reader watches names its own.
			mq.WithRetryBackoff(mq.ExponentialBackoff{
				Base:   20 * time.Millisecond,
				Max:    200 * time.Millisecond,
				Growth: 2,
				Jitter: mq.NoJitter,
			}),
			// The dead-letter destination is the RUNNER's, not the topology's.
			// Both routes exist and they are not the same
			// thing: Config.Nack with nack.RetryTopic makes the DRIVER move a
			// record to <topic>.retry.<n>, while this is the specification's own route and
			// stamps the provenance headers — iqmq-origin-topic,
			// iqmq-attempts and iqmq-failure among them — that an operator
			// reading orders.dlq needs.
			mq.WithNativeDeadLetter(),
			mq.WithFailurePolicy(mq.FailureDeadLetter),
			// Two seconds is plenty for twelve records and is named because
			// the default is thirty: a shutdown that waits for a handler
			// nobody is going to see finish is a shutdown a reader reads as a
			// hang.
			mq.WithDrainTimeout(2*time.Second))
	}()

	if err := publish(ctx, cl); err != nil {
		return err
	}
	select {
	case <-done:
	case err := <-runErr:
		return fmt.Errorf("the runner stopped before the batch was handled: %w", err)
	case <-ctx.Done():
		return fmt.Errorf("only %d of %d deliveries arrived: %w", handled, wantDeliveries, ctx.Err())
	}

	if err := report(out, l.snapshot()); err != nil {
		return err
	}
	if err := readDeadLetter(ctx, out, cl); err != nil {
		return err
	}

	// The checkpoint. Everything acknowledged above has completed in its
	// partition's tracker; this is what puts the resulting watermark in the
	// group coordinator, so a process that dies after this line redelivers
	// nothing.
	if err := backend.Commit(ctx); err != nil {
		return fmt.Errorf("committing the group's offsets: %w", err)
	}
	_, _ = fmt.Fprintf(out, "commit: offsets committed explicitly, so the at-least-once window is zero here (%d commit(s) in this run)\n", min(backend.Commits(), 1))

	return shutdown(ctx, out, consumer, runErr)
}

// ensureTopics creates both topics through the portable administrative API.
//
// The specification forbids Open from creating production topology, so this is explicit,
// and it happens BEFORE the first publication because a Kafka topic's
// partition count is fixed at creation.
func ensureTopics(ctx context.Context, cl *mq.Client) error {
	adm, err := cl.Admin()
	if err != nil {
		return err
	}
	for _, t := range []string{topicOrders, topicDLQ} {
		if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: t}); err != nil {
			return fmt.Errorf("ensuring %s: %w", t, err)
		}
	}
	return nil
}

// publish sends every order's events through one bound publisher.
//
// The publisher is bound to its destination (ADR-021): the topic is resolved
// once, at construction, instead of per call. It is not closed here — the specification
// makes Client.Shutdown close every handle the client opened, publishers
// before consumers drain, and a publisher closed by hand first is a publisher
// the runner's dead-letter route can no longer reach.
func publish(ctx context.Context, cl *mq.Client) error {
	pub, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topicOrders})
	if err != nil {
		return err
	}
	// The bad order first, so that its retries overlap the good ones and the
	// "other lanes keep moving" claim is being exercised rather than asserted
	// after the fact.
	if err := pub.Publish(ctx, order(badKey, lifecycle[0], 1)); err != nil {
		return err
	}
	for _, key := range goodKeys {
		for i, event := range lifecycle {
			// One key, one partition, in publication order. THIS is the
			// producer's half of the ordering guarantee: the driver hashes
			// Message.Key to a partition, so every event of one order lands
			// in the same log and Kafka keeps them in the order they were
			// appended.
			if err := pub.Publish(ctx, order(key, event, i+1)); err != nil {
				return fmt.Errorf("publishing %s/%s: %w", key, event, err)
			}
		}
	}
	return nil
}

// order builds one event of one order.
func order(key, event string, seq int) *mq.Message {
	m := &mq.Message{
		Key:  []byte(key),
		Body: []byte(fmt.Sprintf(`{"order":%q,"event":%q,"seq":%d}`, key, event, seq)),
		ID:   key + "/" + event,
	}
	// The event name is a header rather than only a body field so that the
	// handler can read it without decoding, which is what a real pipeline
	// does for a routing decision.
	m.Headers.Set("event", []byte(event))
	return m
}

// report prints what the handler observed and fails if either half of the
// ordering guarantee did not hold.
//
// Sorted by key, because four workers finish in whatever order the cluster
// hands the partitions over and an example whose output depends on that is an
// example that fails at random.
func report(out io.Writer, obs map[string]observation) error {
	keys := make([]string, 0, len(obs))
	for k := range obs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	totalAcked := 0
	for _, key := range keys {
		o := obs[key]
		totalAcked += o.ackedByHandler
		if key == badKey {
			if o.handled != maxAttempts {
				return fmt.Errorf("the handler was called %d time(s) for %s, want %d: the retry policy did not run", o.handled, key, maxAttempts)
			}
			_, _ = fmt.Fprintf(out,
				"key %s: %d handler attempts of %s, every one failed (the broker's own attempt counter stayed at %d: the retry never left this process)\n",
				key, o.handled, o.events[0], o.brokerAttempt)
			continue
		}
		if o.handled != len(lifecycle) {
			return fmt.Errorf("the handler was called %d time(s) for %s, want %d: a good order was retried", o.handled, key, len(lifecycle))
		}
		// The consumer's half: one lane per key, so the events of one key are
		// never handled out of order however many workers there are.
		if !slices.Equal(o.events, lifecycle) {
			return fmt.Errorf("key %s was handled as %v, want %v: the events of one order were reordered", key, o.events, lifecycle)
		}
		// The producer's half: one key, one partition. Without it the events
		// are in three logs and no consumer can put them back in order.
		if len(o.partitions) != 1 {
			return fmt.Errorf("key %s arrived on partitions %v; a keyed publication puts one key in one partition", key, o.partitions)
		}
		_, _ = fmt.Fprintf(out, "key %s: %s in one partition, in order\n", key, strings.Join(o.events, ","))
	}
	if want := len(lifecycle) * len(goodKeys); totalAcked != want {
		return fmt.Errorf("the handler acked %d deliveries itself, want %d", totalAcked, want)
	}
	_, _ = fmt.Fprintf(out, "the handler acked %d deliveries itself; the runner overruled none\n", totalAcked)
	return nil
}

// readDeadLetter reads the one message that ended up in the dead-letter topic
// and says exactly what provenance is on it.
//
// "Exactly" is the point. The specification names five provenance headers and this driver's
// route writes four of them; the fifth — iqmq-failure, the reason the message
// died — is NOT there, and finding that out was one of the two things this
// example produced. See the package comment and README.md.
func readDeadLetter(ctx context.Context, out io.Writer, cl *mq.Client) error {
	dlq, err := cl.Consumer(ctx, mq.ConsumerConfig{
		Topic:        topicDLQ,
		Subscription: "dlq-audit",
		Start:        mq.StartEarliest(),
	})
	if err != nil {
		return err
	}
	defer func() { _ = dlq.Shutdown(ctx) }()

	// Bounded, because a check that hangs instead of failing is worse than no
	// check: the log says "timed out" and names no assertion.
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := dlq.Receive(rctx, dst)
	if err != nil || n == 0 {
		return fmt.Errorf("nothing arrived in %s: %w", topicDLQ, err)
	}
	d := &dst[0]
	if got := string(d.Key); got != badKey {
		return fmt.Errorf("the dead-letter topic holds key %q, want %q", got, badKey)
	}
	// The four headers that ARE there. Their values are checked rather than
	// printed where a value is deterministic, and checked for PRESENCE where
	// it is not: the partition a key hashes to and the offset a record landed
	// at differ between runs, and an example whose output contained either
	// would fail at random.
	for _, want := range []struct {
		key, value string
	}{
		{kq.HeaderOriginTopic, topicOrders},
		{kq.HeaderOriginSubscription, subscription},
	} {
		got, ok := d.Headers.Get(want.key)
		if !ok || string(got) != want.value {
			return fmt.Errorf("the dead-lettered message carries %s=%q, want %q", want.key, got, want.value)
		}
	}
	for _, key := range []string{kq.HeaderOriginPartition, kq.HeaderOriginOffset} {
		if _, ok := d.Headers.Get(key); !ok {
			return errors.New("the dead-lettered message carries no " + key + " header")
		}
	}
	// And the two that are NOT, with the check written so that the day they
	// start arriving this example FAILS and says so, rather than quietly
	// keeping a stale claim in its output.
	//
	// Both are missing for the same reason: this route settles with
	// Nack(NackDeadLetter()) and that option carries neither a reason nor an
	// attempt count, so the driver's stamp — which writes iqmq-attempts only
	// when the plan names an attempt and iqmq-failure only when it names a
	// reason — writes neither. The handler's error, and the number of times
	// it was called, are the two things an operator reading this topic most
	// wants.
	for _, key := range []string{mq.HeaderAttempts, mq.HeaderFailure} {
		if v, ok := d.Headers.Get(key); ok {
			return fmt.Errorf("the dead-lettered message now carries %s=%q: the native dead-letter route has grown provenance it used not to have, so the specification is closed and this example's output has to be updated", key, v)
		}
	}

	_, _ = fmt.Fprintf(out, "dead-lettered %s, with origin topic %s, subscription %s, partition and offset\n",
		d.ID, topicOrders, subscription)
	_, _ = fmt.Fprintf(out, "...and with neither %s nor %s: Nack(NackDeadLetter()) carries no attempt count and no reason \n",
		mq.HeaderAttempts, mq.HeaderFailure)
	return d.Ack()
}

// shutdown stops the runner gracefully and prints what the consumer counted.
//
// This is the graceful trio in the order it is meant to be used, and the
// order is the whole point:
//
//  1. Drain stops receiving and waits for the handlers already in flight. The
//     runner stops as it would for a shutdown — Run returns once its lanes
//     have drained — and the handle stays OPEN, so nothing has been closed
//     underneath a goroutine that is still working.
//  2. Run's return is then read. What a DRAINED runner returns is
//     context.Canceled — the internal fetch context's error, not the
//     caller's: Drain cancels the fetch, the handle is not closed, and
//     nothing maps that cancellation back to the drain that caused it. So a
//     caller cannot tell "I drained you" from "my own context died", and a
//     shutdown, which does have a named answer (ErrClosed), is the only one
//     of the three that says what happened. Both are
//     accepted below; anything else — a *PoisonError, say — would mean the
//     failure policy could not place a message.
//  3. Shutdown closes the consumer, which requeues or abandons whatever was
//     never settled.
//
// Cancelling the run context instead of draining is the abrupt form, and
// closing the handle first is the one that makes Run return ErrClosed with
// handlers still running. InFlight back to zero is the assertion: a closed
// consumer holds nothing, because its source has requeued or abandoned what
// it had.
func shutdown(ctx context.Context, out io.Writer, consumer *mq.ConsumerHandle, runErr <-chan error) error {
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := consumer.Drain(sctx); err != nil {
		return fmt.Errorf("draining the consumer: %w", err)
	}
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run: %w", err)
	}
	if err := consumer.Shutdown(sctx); err != nil {
		return fmt.Errorf("consumer shutdown: %w", err)
	}
	s := consumer.Stats()
	if s.InFlight != 0 {
		return fmt.Errorf("%d deliveries are still in flight after Shutdown; nothing may be left unsettled", s.InFlight)
	}
	_, _ = fmt.Fprintf(out,
		"shutdown: run drained cleanly; received %d, acked %d, dead-lettered %d, settle failures %d, in flight %d\n",
		s.Received, s.Acked, s.DeadLettered, s.SettleFailures, s.InFlight)
	return nil
}
