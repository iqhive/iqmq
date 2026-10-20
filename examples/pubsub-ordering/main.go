// Command pubsub-ordering is per-key ordering through the portable API, run
// unchanged on two drivers: the in-process memory broker and Google Cloud
// Pub/Sub's ordering keys through
// github.com/iqhive/iqmq/drivers/gcppubsub.
//
// # Ordering by key is THREE decisions, not one
//
//   - The BROKER has to keep an order, and the scope it keeps it at is a
//     capability rather than a promise of this library. mq.FeatureOrderedByKey
//     at mq.ScopeKey is the claim; Pub/Sub makes it only with
//     Config.Ordering set, which turns on the client library's
//     EnableMessageOrdering and the subscription's own flag, and the memory
//     broker makes it natively.
//   - The PUBLISHER has to put every event of one entity under one
//     mq.Message.Key, which becomes Pub/Sub's ordering key. Drop the key and
//     the service orders nothing; no consumer can put the events back.
//   - The RUNNER has to serialise a key. The specification routes by hash(key) % lanes and
//     one lane is one handler at a time, so mq.WithConcurrency(4) processes
//     four LANES at once and never two events of one key at once.
//
// # What the option does not do
//
// mq.WithOrdering names how the runner ROUTES. It is not a request the broker
// answers, and nothing refuses it for a scope the profile does not have: this
// program asks for mq.OrderingByPartition on a driver whose capability table
// says "Pub/Sub has no partitions", and the Run starts. Every delivery then
// carries partition 0 and the whole subscription collapses onto one lane,
// which is a throughput cliff rather than an error. The capability table is
// what a caller has to read; the option is only what to do with what arrives.
//
// # Why the parallelism is not asserted
//
// The obvious assertion — "with four handlers and three keys, three keys were
// in flight at once" — cannot be made deterministic. Lanes are hash(key) %
// lanes, so two of three keys may share a lane whatever the concurrency is,
// and whether two keys overlap in time depends on the scheduler. So this
// program asserts the half that IS a guarantee, per key and per delivery, and
// says the other half in prose rather than printing a number that would fail
// at random on a busy machine.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	ps "github.com/iqhive/iqmq/drivers/gcppubsub"
)

// The entity keys and the lifecycle every one of them goes through. Three
// keys and three events is nine publications, which is small enough to print
// and large enough that a lost or reordered event is visible.
var (
	keys      = []string{"ord-1001", "ord-1002", "ord-1003"}
	lifecycle = []string{"created", "paid", "shipped"}
)

// subscription is the subscription name both arms use.
const subscription = "fulfilment"

// handlers is how many lanes the runner is given. It is more than the number
// of keys on purpose: the point of the demonstration is that four handlers do
// not break a key's order.
const handlers = 4

// topicBase is the logical topic; newTopic makes it unique per run.
const topicBase = "orders"

// newTopic returns a topic name nothing else is using.
//
// A shared name is expensive against a broker that outlives the run: a message
// an earlier run left behind is delivered to this one, where it belongs to no
// key this program knows and breaks every assertion about what arrived. It is
// the lesson the specification recorded against a FIFO queue, and it costs one
// line here.
func newTopic() string {
	return topicBase + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func main() {
	if err := demo(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// demo is main's body as a function that returns, so the deferred shutdown
// runs before os.Exit ends the process.
func demo() error {
	addr := os.Getenv("IQMQ_PUBSUB_ADDR")
	if addr == "" {
		// conformance/harness's convention: IQMQ_<BROKER>_ADDR names an
		// externally managed broker as host:port. 8085 is the Pub/Sub
		// emulator's default.
		addr = "127.0.0.1:8085"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := open(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()
	return run(ctx, os.Stdout, client)
}

// open dials a Pub/Sub emulator at addr.
//
// For the real service, drop Endpoint and InsecureTransport, put a real
// project here, and let the client library find credentials the usual way.
// Config.Ordering stays: it is what makes the capability table claim
// FeatureOrderedByKey, and a publisher built without it REFUSES every
// publication that carries a key.
func open(ctx context.Context, addr string) (*mq.Client, error) {
	return ps.Open(ctx, Config(func(cfg *ps.Config) { cfg.Endpoint = addr }))
}

// Config is the configuration the broker arm uses. It is exported so the test
// cannot fork the settings.
func Config(mutate ...func(*ps.Config)) ps.Config {
	cfg := ps.Config{
		ProjectID: "iqmq-example",
		// The emulator speaks plain HTTP/2 and has no credentials.
		InsecureTransport: true,
		// The specification forbids Open from creating production topology; AutoCreate is
		// the caller saying "this is a development project, make it".
		AutoCreate: true,
		// The one setting this example is about. Without it Pub/Sub orders
		// nothing and the publisher refuses a keyed publication outright.
		Ordering: true,
		// Short, because every "did anything else arrive" step waits for it.
		AckDeadline: 10 * time.Second,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg
}

// observation is what the handler recorded about one key.
type observation struct {
	// events are the bodies in the order they were handled.
	events []string
	// maxInFlight is the most deliveries of THIS key that were being handled
	// at the same moment. The guarantee is that it is 1.
	maxInFlight int
}

// ledger collects the observations. Every field is written under the mutex,
// including the counters: a handler runs on `handlers` goroutines at once and
// this is the part of the program the race detector is pointed at.
type ledger struct {
	mu       sync.Mutex
	byKey    map[string]*observation
	inFlight map[string]int
	// live and liveMax are the in-flight count across ALL keys, which is
	// what distinguishes "four lanes" from "one lane". It is an observation
	// and not a guarantee -- see the package documentation -- except under
	// Ordering that collapses everything onto one lane, where its maximum is
	// exactly 1 however much concurrency is configured.
	live    int
	liveMax int
	handled int
	// done is closed by the delivery that completes the set, and it is closed
	// only AFTER this ledger's own accounting for that delivery has finished.
	// Closing it first is the defect: the main goroutine wakes
	// up, shuts the consumer down and reads a count the last handler had not
	// finished writing.
	done chan struct{}
	once sync.Once
	want int
}

func newLedger(want int) *ledger {
	return &ledger{
		byKey:    map[string]*observation{},
		inFlight: map[string]int{},
		done:     make(chan struct{}),
		want:     want,
	}
}

// enter records the start of a delivery and returns the key's in-flight count.
func (l *ledger) enter(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	o := l.obsLocked(key)
	l.inFlight[key]++
	if l.inFlight[key] > o.maxInFlight {
		o.maxInFlight = l.inFlight[key]
	}
	l.live++
	if l.live > l.liveMax {
		l.liveMax = l.live
	}
}

// leave records the end of a delivery, appends the event, and closes done when
// the set is complete.
func (l *ledger) leave(key, event string) {
	l.mu.Lock()
	o := l.obsLocked(key)
	o.events = append(o.events, event)
	l.inFlight[key]--
	l.live--
	l.handled++
	complete := l.handled == l.want
	l.mu.Unlock()
	// Outside the lock, and AFTER every write above: see the done field.
	if complete {
		l.once.Do(func() { close(l.done) })
	}
}

func (l *ledger) obsLocked(key string) *observation {
	o, ok := l.byKey[key]
	if !ok {
		o = &observation{}
		l.byKey[key] = o
	}
	return o
}

// LiveMax is the most deliveries that were being handled at one moment,
// across every key.
func (l *ledger) LiveMax() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.liveMax
}

// snapshot copies the observations out.
func (l *ledger) snapshot() map[string]observation {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]observation, len(l.byKey))
	for k, o := range l.byKey {
		out[k] = observation{events: slices.Clone(o.events), maxInFlight: o.maxInFlight}
	}
	return out
}

// run is the whole program.
func run(ctx context.Context, out io.Writer, cl *mq.Client) error {
	_, err := runWith(ctx, out, cl, nil)
	return err
}

// runWith is run with extra RunOptions, and it hands the ledger back.
//
// The seam exists for one test: TestOrderingByPartitionCollapsesEveryKeyOntoOneLane
// runs this program with mq.WithOrdering(mq.OrderingByPartition) on a
// single-partition topic, where the lane count is 1 and the cross-key
// concurrency is therefore exactly 1 -- which is the cost of asking for a
// scope the topology cannot spread. The example itself passes nil.
func runWith(ctx context.Context, out io.Writer, cl *mq.Client, extra []mq.RunOption) (*ledger, error) {
	caps := cl.Capabilities()
	sup := caps.Support(mq.FeatureOrderedByKey)
	// The claim, and its SCOPE. The specification makes a capability table constant per
	// client, so this is the one honest place to read what the broker will
	// keep — and it is what a caller must read instead of assuming that
	// asking for an order produces one.
	_, _ = fmt.Fprintf(out, "claims: ordering by key %v at scope %s\n",
		caps.Has(mq.FeatureOrderedByKey), sup.Scope)

	topic := newTopic()
	defer dropTopic(ctx, cl, topic)
	consumer, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: subscription})
	if err != nil {
		return nil, err
	}
	if err := publish(ctx, cl, topic); err != nil {
		return nil, err
	}

	l := newLedger(len(keys) * len(lifecycle))
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runErr := make(chan error, 1)
	opts := append([]mq.RunOption{
		// Four lanes. The specification routes by hash(key) % lanes, so this is four keys
		// at a time at most and never two events of one key.
		mq.WithConcurrency(handlers),
		// Strict ordering: a failed delivery blocks its lane until it is
		// settled or terminally routed, so nothing behind it overtakes its
		// redelivery. Nothing fails in this program; the option is here
		// because a program that keeps an order and retries needs both
		// halves.
		mq.WithOrderingByKey(),
	}, extra...)
	go func() {
		runErr <- consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			key := string(d.Key)
			l.enter(key)
			l.leave(key, string(d.Body))
			// Returning nil acks under the default settlement mode. The
			// runner's ack happens AFTER this returns, which is why nothing
			// below reads ConsumerStats until Run has returned.
			return nil
		}), opts...)
	}()

	select {
	case <-l.done:
	case err := <-runErr:
		return nil, fmt.Errorf("the run ended before every event arrived: %w", err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Every delivery has been handled and this ledger's accounting for all of
	// them is complete. Shutdown drains and settles, and Run then returns —
	// which is the point at which the runner's own counters are final.
	// Reading them before this is how "acked 8, want 9"
	// happens.
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := consumer.Shutdown(sctx); err != nil {
		return nil, fmt.Errorf("shutting the consumer down: %w", err)
	}
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, mq.ErrClosed) {
		return nil, fmt.Errorf("the run ended with %w", err)
	}

	if err := report(out, l.snapshot()); err != nil {
		return nil, err
	}
	s := consumer.Stats()
	if s.InFlight != 0 {
		return nil, fmt.Errorf("%d deliveries are still in flight after Shutdown; nothing may be left unsettled", s.InFlight)
	}
	_, _ = fmt.Fprintf(out, "shutdown: received %d, acked %d, in flight %d, settle failures %d\n",
		s.Received, s.Acked, s.InFlight, s.SettleFailures)
	return l, nil
}

// publish sends every key's lifecycle, keyed.
func publish(ctx context.Context, cl *mq.Client, topic string) error {
	pub, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		return err
	}
	// Not closed here: the specification makes Client.Shutdown close every handle the
	// client opened, publishers before consumers drain.
	for _, event := range lifecycle {
		for _, key := range keys {
			// Interleaved on purpose: created for all three keys, then paid
			// for all three. A program that published one key at a time would
			// produce the right answer even with no ordering at all, because
			// the arrival order would already be the publication order.
			m := &mq.Message{Key: []byte(key), Body: []byte(event), ID: key + "/" + event}
			if err := pub.Publish(ctx, m); err != nil {
				return fmt.Errorf("publishing %s/%s: %w", key, event, err)
			}
		}
	}
	return nil
}

// report checks the observations and prints them.
//
// It is a function over a map so that main_test.go can hand it fabricated
// observations and watch each check fail: a check nobody has seen fail is
// assumed not to work.
func report(out io.Writer, seen map[string]observation) error {
	if len(seen) != len(keys) {
		return fmt.Errorf("%d key(s) were delivered, want %d: %v", len(seen), len(keys), sortedKeys(seen))
	}
	for _, key := range keys {
		o, ok := seen[key]
		if !ok {
			return fmt.Errorf("key %s was never delivered; the keys seen were %v", key, sortedKeys(seen))
		}
		if !slices.Equal(o.events, lifecycle) {
			return fmt.Errorf("key %s was delivered as %v, want %v: every event of one key has to arrive in publication order",
				key, o.events, lifecycle)
		}
		if o.maxInFlight != 1 {
			return fmt.Errorf("key %s had %d deliveries in flight at once, want 1: the specification routes a key to one lane and one lane is one handler at a time",
				key, o.maxInFlight)
		}
		_, _ = fmt.Fprintf(out, "order: %s delivered %s\n", key, strings.Join(o.events, ","))
	}
	_, _ = fmt.Fprintf(out, "concurrency: %d handlers over %d key(s), and no key ever had two deliveries in flight at once\n",
		handlers, len(keys))
	return nil
}

// sortedKeys is the keys of seen, in order, for an error message that is the
// same on every run.
func sortedKeys(seen map[string]observation) []string {
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// dropTopic deletes the run's topic and subscription, best-effort.
func dropTopic(ctx context.Context, cl *mq.Client, topic string) {
	adm, err := cl.Admin()
	if err != nil {
		return
	}
	_ = adm.DeleteSubscription(ctx, topic, subscription)
	_ = adm.DeleteTopic(ctx, topic)
}
