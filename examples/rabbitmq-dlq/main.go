// Command rabbitmq-dlq is the dead-letter destination, BOTH ways, over
// RabbitMQ (bitbucket.org/iqhive/iqmq/drivers/rabbitmq) and over the
// in-process memory broker.
//
// # There are two dead-letter routes and they are not equivalent
//
// The CORE route -- mq.WithDeadLetter(topic) -- is iqmq's own. When a
// message's attempts are exhausted the runner PUBLISHES A COPY of it to the
// named topic through the ordinary publish path, stamped with the specification's
// provenance headers: iqmq-origin-topic, iqmq-origin-subscription,
// iqmq-attempts, iqmq-failure and iqmq-first-delivered-at. Then it acks the
// original. The copy is an ordinary message that any consumer can read, and
// the reason the handler gave is IN it.
//
// The NATIVE route -- mq.WithNativeDeadLetter() -- asks the broker. The
// runner settles with Nack(NackDeadLetter()) and the driver translates that
// into whatever the broker has: on RabbitMQ a `basic.reject` with requeue
// false, which the queue's `x-dead-letter-exchange` routes onward. Nothing is
// republished, the message is moved by the broker, and it arrives with the
// broker's own provenance (`x-death`) and without iqmq's.
//
// # Which route this example uses, and why that sentence has to be here
//
// It uses BOTH, on both arms, because both work -- and until that was a property of these two drivers rather than of the library.
// The specification recorded that mq.WithDeadLetter could NOT work on six of
// the twelve shipped drivers -- drivers/kafka, drivers/kafka/kafkago,
// drivers/nats, drivers/jetstream, drivers/amqp10 and drivers/pulsar --
// because their Representable refused ANY header under
// mq.ReservedHeaderPrefix, which is every one of the five headers the core
// route stamps: the publication was refused before any I/O, the runner
// requeued the message and counted a DeadLetterFailures, and the message was
// redelivered for ever. driverkit.ReservedHeaderAllowed now admits exactly
// those five and nothing else, in every driver, so the core route is
// available on all twelve.
//
// The sentence stays because the two routes still differ, and now the
// difference is only about what the dead-lettered message CARRIES: the core
// route stamps a reason and an attempt count, the native route carries the
// broker's provenance and neither of those , and
// ConsumerStats.DeadLettered counts only the native one.
// Where the broker has no dead-letter feature there is still no native route
// at all. So this program prints which route it took rather than leaving a
// reader to guess what arrived.
//
// # What the native route does not carry
//
// runner.nativeDeadLetter settles with Nack(NackDeadLetter()), which names
// neither the failure nor the attempt count -- so a natively dead-lettered
// message says where it came from, at best, and never why it died. This
// program asserts that absence, on both arms, because it is the practical
// difference between the two routes: the core route's copy is
// self-describing and the native route's is not.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	rb "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
)

// The topics. Two source topics, because the two routes need two runners with
// different options, and one dead-letter topic that both of them land in.
const (
	topicCore   = "orders-core"
	topicNative = "orders-native"
	topicDLQ    = "orders-dlq"
)

// subscription is the subscription name every consumer uses.
const subscription = "fulfilment"

// maxAttempts is how many handler attempts a message gets before the specification routes
// it. Three, because one would not show a retry and ten would make the
// example slow.
const maxAttempts = 3

// poison is the body every message in this example carries. There is exactly
// one message per route and it always fails: a dead-letter example whose
// messages sometimes succeed is an example of something else.
const poison = "unprocessable"

// failure is what the handler reports. It is the string that must survive
// into iqmq-failure on the core route, so it is distinctive enough that a
// match cannot be a coincidence.
const failure = "the order references a product that does not exist"

// backoff is the retry delay. It is short because the example really waits
// for the attempts to be exhausted, and it is not zero because a backoff of
// zero would make the retry a busy loop in a program a reader may run.
const backoff = 50 * time.Millisecond

// newSuffix makes this run's topics unique.
//
// A durable RabbitMQ exchange and a quorum queue outlive the process, so a
// second run of a shared name inherits the first run's messages -- including
// the dead letters, which is exactly what this program counts. It is the lesson in a different broker.
func newSuffix() string { return "-" + strconv.FormatInt(time.Now().UnixNano(), 36) }

func main() {
	if err := demo(); err != nil {
		log.Fatal(err)
	}
}

// demo is main's body as a function that returns.
func demo() error {
	addr := os.Getenv("IQMQ_RABBITMQ_ADDR")
	if addr == "" {
		// conformance/harness's convention: IQMQ_<BROKER>_ADDR names an
		// externally managed broker as host:port. 5672 is AMQP's port.
		addr = "127.0.0.1:5672"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	suffix := newSuffix()
	client, err := open(ctx, addr, suffix)
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()
	return run(ctx, os.Stdout, client, suffix)
}

// open dials RabbitMQ.
func open(ctx context.Context, addr, suffix string) (*mq.Client, error) {
	return rb.Open(ctx, Config(suffix, func(cfg *rb.Config) { cfg.Addrs = []string{addr} }))
}

// Config is the configuration the broker arm uses, exported so that the test
// cannot fork it.
//
// Declare and DeadLetter are the two settings this example cannot do without,
// and both are OFF by default because the specification forbids a driver from creating
// topology behind a caller's back. DeadLetterTopic is what makes the native
// route CONSUMABLE: without it a rejected message lands in `<queue>.dlq`,
// which is a queue and not a topic, so nothing can read it through mq.Client
// and the whole second half of this example would have to be `rabbitmqctl`.
func Config(suffix string, mutate ...func(*rb.Config)) rb.Config {
	cfg := rb.Config{
		Declare:    true,
		DeadLetter: true,
		// The named topic has to exist before a message is rejected:
		// RabbitMQ DROPS a dead-lettered message whose dead-letter exchange
		// is not there, silently. run() opens a consumer
		// on it first for exactly that reason.
		DeadLetterTopic: topicDLQ + suffix,
		Prefix:          "iqmq-example",
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg
}

// route names one of the two dead-letter routes.
type route struct {
	// name is what the printed line calls it.
	name string
	// topic is the source topic this route's message is published to.
	topic string
	// option is the RunOption that selects the route.
	option mq.RunOption
	// wantIQMQHeaders says whether the dead-lettered message must carry
	// the specification's own provenance. The core route stamps it; the native route
	// cannot.
	wantIQMQHeaders bool
	// The settlement counters this route leaves behind, which are DIFFERENT
	// for the two routes and are the example's sharpest finding. See
	// The specification: ConsumerStats.DeadLettered counts "negative
	// settlements that asked for the dead-letter destination", and the core
	// route makes none -- it publishes a copy and ACKS the original -- so it
	// is invisible in that counter.
	wantAcked, wantNacked, wantDeadLettered int64
	// note is the sentence the printed line ends with.
	note string
}

// run is the whole program.
func run(ctx context.Context, out io.Writer, cl *mq.Client, suffix string) error {
	caps := cl.Capabilities()
	// The claim, and only the claim. A driver that does not make it cannot
	// take the native route at all.
	//
	// The LEVEL and the SCOPE are deliberately not printed, and that is a
	// finding rather than a simplification: the memory broker advertises
	// Conditional at mq.ScopeTopic ("TopologyConfig.DeadLetterTopic, which
	// must already exist") and RabbitMQ advertises Native at mq.ScopeGroup
	// ("routed to the exchange of Config.DeadLetterTopic"), so the two agree
	// about the feature and about nothing else. And OWN route depends
	// on something the table does not report at all -- whether the driver's
	// Representable accepts mq.ReservedHeaderPrefix -- which is why every
	// line below is about what happened rather than about what was
	// advertised. That last clause is now about a
	// property every driver has rather than six of twelve, because
	// driverkit.ReservedHeaderAllowed made the core's own five provenance
	// keys admissible everywhere ; what the table still
	// does not report is which route carries what.
	_, _ = fmt.Fprintf(out, "claims: dead letter %v\n", caps.Has(mq.FeatureDeadLetter))

	dlq := topicDLQ + suffix
	// The dead-letter consumer comes FIRST. On RabbitMQ the dead-letter
	// exchange has to exist before a message is rejected or the broker drops
	// it silently , and opening a consumer is what
	// declares it.
	dlqConsumer, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: dlq, Subscription: subscription})
	if err != nil {
		return fmt.Errorf("opening the dead-letter consumer: %w", err)
	}
	defer func() { _ = dlqConsumer.Shutdown(ctx) }()

	routes := []route{
		{
			name:  "core",
			topic: topicCore + suffix,
			// the specification's own route: publish a stamped copy of the message to the
			// named topic, then ack the original.
			option:          mq.WithDeadLetter(dlq),
			wantIQMQHeaders: true,
			// One ack (the original, after the copy was published) and two
			// nacks (the first two failed attempts).
			wantAcked: 1, wantNacked: maxAttempts - 1, wantDeadLettered: 0,
			note: "the copy was published and the original ACKED, so ConsumerStats.DeadLettered counts none of it ",
		},
		{
			name:  "native",
			topic: topicNative + suffix,
			// The broker's route: Nack(NackDeadLetter()) and let the
			// x-dead-letter-exchange move the message.
			option:          mq.WithNativeDeadLetter(),
			wantIQMQHeaders: false,
			// Three nacks, the last of which asked for the dead-letter
			// destination, and no ack at all.
			wantAcked: 0, wantNacked: maxAttempts, wantDeadLettered: 1,
			note: "the last nack asked the broker to move the message, which is the only shape ConsumerStats.DeadLettered counts",
		},
	}
	for i := range routes {
		if err := exercise(ctx, out, cl, &routes[i]); err != nil {
			return err
		}
	}
	// Both dead letters are now in the one dead-letter topic, and this is
	// where they are read: a dead-letter destination is a place a caller
	// reads FROM, which is the whole reason Config.DeadLetterTopic exists.
	return collect(ctx, out, dlqConsumer, routes)
}

// exercise publishes one poison message, runs it until its attempts are
// exhausted, and prints what the runner counted.
func exercise(ctx context.Context, out io.Writer, cl *mq.Client, r *route) error {
	consumer, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: r.topic, Subscription: subscription})
	if err != nil {
		return err
	}
	id := r.name + "/" + poison
	if err := cl.Publish(ctx, r.topic, &mq.Message{Body: []byte(poison), ID: id}); err != nil {
		return fmt.Errorf("publishing to %s: %w", r.topic, err)
	}

	var (
		mu       sync.Mutex
		attempts int
		done     = make(chan struct{})
		once     sync.Once
	)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runErr := make(chan error, 1)
	go func() {
		runErr <- consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, _ *mq.Delivery) error {
			mu.Lock()
			attempts++
			last := attempts == maxAttempts
			mu.Unlock()
			// `done` is closed AFTER this handler's own accounting, and the
			// ROUTING still has to happen after the handler returns: the
			// runner decides to dead-letter on the way out. So the wait below
			// is on the dead-lettered message arriving, not on this channel,
			// and this channel only says "stop expecting more attempts".
			// Reading a counter off an event raised inside the operation that
			// increments it is the defect.
			if last {
				once.Do(func() { close(done) })
			}
			return errors.New(failure)
		}),
			mq.WithMaxAttempts(maxAttempts),
			mq.WithRetryBackoff(mq.ConstantBackoff(backoff)),
			r.option,
		)
	}()

	select {
	case <-done:
	case err := <-runErr:
		return fmt.Errorf("the %s run ended after %d attempt(s): %w", r.name, attemptsOf(&mu, &attempts), err)
	case <-ctx.Done():
		return ctx.Err()
	}

	// Shutdown drains, which is what finishes the dead-letter routing of the
	// delivery whose handler has just returned, and Run then returns. Only
	// after that are the runner's counters final.
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := consumer.Shutdown(sctx); err != nil {
		return fmt.Errorf("shutting the %s consumer down: %w", r.name, err)
	}
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, mq.ErrClosed) {
		return fmt.Errorf("the %s run ended with %w", r.name, err)
	}

	s := consumer.Stats()
	got := attemptsOf(&mu, &attempts)
	if got != maxAttempts {
		return fmt.Errorf("the %s route's handler ran %d time(s), want %d: mq.WithMaxAttempts is what decides when the specification routes", r.name, got, maxAttempts)
	}
	if s.DeadLetterFailures != 0 {
		return fmt.Errorf("the %s route reported %d dead-letter failure(s): ADR-011 requires a failed dead-letter publication to be requeued and counted, and this is that count — on six drivers the core route fails here for EVERY message ",
			r.name, s.DeadLetterFailures)
	}
	if s.SettleFailures != 0 {
		return fmt.Errorf("the %s route reported %d settle failure(s)", r.name, s.SettleFailures)
	}
	if s.Acked != r.wantAcked || s.Nacked != r.wantNacked || s.DeadLettered != r.wantDeadLettered {
		return fmt.Errorf("the %s route left acked=%d nacked=%d dead-lettered=%d, want %d/%d/%d: the two routes settle DIFFERENTLY and that is what this example is about",
			r.name, s.Acked, s.Nacked, s.DeadLettered, r.wantAcked, r.wantNacked, r.wantDeadLettered)
	}
	if s.InFlight != 0 {
		return fmt.Errorf("%d deliveries are still in flight after the %s route's Shutdown", s.InFlight, r.name)
	}
	_, _ = fmt.Fprintf(out, "%s route: %d handler attempt(s), then acked %d nacked %d dead-lettered %d — %s\n",
		r.name, got, s.Acked, s.Nacked, s.DeadLettered, r.note)
	return nil
}

// attemptsOf reads the shared counter under its mutex.
func attemptsOf(mu *sync.Mutex, n *int) int {
	mu.Lock()
	defer mu.Unlock()
	return *n
}

// collect reads both dead letters out of the dead-letter topic and checks what
// each route's message carries.
func collect(ctx context.Context, out io.Writer, consumer *mq.ConsumerHandle, routes []route) error {
	byID := map[string]*mq.Message{}
	for range routes {
		d, err := receive(ctx, consumer)
		if err != nil {
			return fmt.Errorf("reading the dead-letter topic: %w", err)
		}
		// Cloned before the ack: the specification borrows a received body and header
		// storage until the delivery is settled, and this program settles
		// before it reads.
		m := d.CloneMessage()
		if err := d.Ack(); err != nil {
			return fmt.Errorf("acking a dead letter: %w", err)
		}
		byID[identify(m)] = m
	}
	// And nothing else: two poison messages produce two dead letters, and a
	// third would mean one of them was routed twice.
	if n, err := receiveNothing(ctx, consumer); err != nil {
		return err
	} else if n != 0 {
		return fmt.Errorf("the dead-letter topic holds %d more message(s) than the two this program produced", n)
	}

	for i := range routes {
		r := &routes[i]
		m, ok := byID[r.name]
		if !ok {
			have := make([]string, 0, len(byID))
			for k := range byID {
				have = append(have, k)
			}
			return fmt.Errorf("the %s route's message never reached the dead-letter topic; what arrived was %v", r.name, have)
		}
		if got := string(m.Body); got != poison {
			return fmt.Errorf("the %s route's dead letter carries body %q, want %q: the specification routes the message and does not rewrite it", r.name, got, poison)
		}
		if err := checkProvenance(out, r, m); err != nil {
			return err
		}
	}
	return nil
}

// identify says which route a dead letter came from.
//
// The core route's copy carries iqmq-origin-topic; the native route's message
// carries whatever the broker put on it and, on a broker that puts nothing,
// only its own identifier. Both routes preserve mq.Message.ID, so the
// identifier is what this reads -- deliberately, because a route that is
// identified by the header it is being TESTED for cannot fail that test.
func identify(m *mq.Message) string {
	name, _, ok := strings.Cut(m.ID, "/")
	if !ok {
		return m.ID
	}
	return name
}

// checkProvenance holds each route to what it carries, and prints it.
func checkProvenance(out io.Writer, r *route, m *mq.Message) error {
	origin, hasOrigin := m.Headers.Get(mq.HeaderOriginTopic)
	_, hasSub := m.Headers.Get(mq.HeaderOriginSubscription)
	_, hasAttempts := m.Headers.Get(mq.HeaderAttempts)
	reason, hasReason := m.Headers.Get(mq.HeaderFailure)

	if r.wantIQMQHeaders {
		switch {
		case !hasOrigin || !hasSub:
			return fmt.Errorf("the %s route's dead letter carries origin-topic=%v origin-subscription=%v, want both: the specification stamps five provenance headers and this driver's Representable accepted them",
				r.name, hasOrigin, hasSub)
		case !hasAttempts:
			return fmt.Errorf("the %s route's dead letter carries no %s", r.name, mq.HeaderAttempts)
		case !hasReason:
			return fmt.Errorf("the %s route's dead letter carries no %s", r.name, mq.HeaderFailure)
		case string(reason) != failure:
			return fmt.Errorf("the %s route's dead letter says it failed with %q, want %q", r.name, reason, failure)
		}
		// The attempt count is NOT asserted as a number. It is the header
		// The specification writes from DeliveryInfo.Attempt, which is the BROKER's
		// counter -- and where the retry never leaves the process that
		// counter does not move, so it reads 1 however many times the runner
		// retried. Printing it would make this example's
		// expected output a statement about one driver's nack strategy.
		_, _ = fmt.Fprintf(out, "%s route: the copy carries origin topic %s, its subscription, an attempt count and the handler's reason %q\n",
			r.name, string(origin), failure)
		return nil
	}

	// The native route. This is the assertion, and it is the interesting one:
	// the message MOVED and it does not say why.
	if hasAttempts || hasReason {
		return fmt.Errorf("the %s route's dead letter carries %s=%v and %s=%v; if the native route has learned to carry a reason and an attempt count then the specification is closed and this example should stop saying it has not",
			r.name, mq.HeaderAttempts, hasAttempts, mq.HeaderFailure, hasReason)
	}
	_, _ = fmt.Fprintf(out, "%s route: the broker moved the message, and it carries neither %s nor %s — Nack(NackDeadLetter()) names neither \n",
		r.name, mq.HeaderAttempts, mq.HeaderFailure)
	return nil
}

// receive takes exactly one delivery, bounded so that a message that never
// arrives is an error rather than a hang.
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

// receiveNothing reports how many messages a bounded receive produced, and
// expects none. A deadline that passes is the ANSWER here rather than a
// failure.
func receiveNothing(ctx context.Context, c *mq.ConsumerHandle) (int, error) {
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(rctx, dst)
	for i := range dst[:min(n, len(dst))] {
		_ = dst[i].Ack()
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return n, err
	}
	return n, nil
}
