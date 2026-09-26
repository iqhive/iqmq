package iqmq_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// The script bounds. They are small on purpose: a fuzz input that takes a
// second to run is an input the corpus cannot afford many of, and every
// interesting interleaving of this runner is reachable inside them.
const (
	fuzzMaxMessages    = 12
	fuzzMaxKeys        = 4
	fuzzMaxConcurrency = 4
	fuzzMaxAttempts    = 3
	fuzzRunDeadline    = 10 * time.Second
)

// The handler outcomes a scripted delivery can produce. Every one of the specification's
// dispositions is here, because the disposition is what decides where the
// message ends up and conservation is what the target checks.
const (
	outcomeAck = iota
	outcomeError
	outcomeRetryAfter
	outcomeDiscard
	outcomeDeadLetter
	outcomeHalt
	outcomePanic
	outcomeCount
)

// dispatchScript is one scripted run of the runner.
type dispatchScript struct {
	messages    int
	keys        int
	concurrency int
	ordering    int
	orderMode   int
	maxAttempts int
	batch       bool
	deadLetter  bool
	native      bool
	extend      bool
	delayed     bool
	countless   bool
	manual      bool
	outcomes    []int
}

// decodeDispatchScript reads a script out of a fuzz input.
func decodeDispatchScript(data []byte) dispatchScript {
	r := fuzzutil.NewReader(data)
	sc := dispatchScript{
		messages:    r.Int(1, fuzzMaxMessages),
		keys:        r.Int(1, fuzzMaxKeys),
		concurrency: r.Int(1, fuzzMaxConcurrency),
		ordering:    r.Choice(3),
		orderMode:   r.Choice(4),
		maxAttempts: r.Int(1, fuzzMaxAttempts),
		batch:       r.Bool(),
		deadLetter:  r.Bool(),
		native:      r.Bool(),
		extend:      r.Bool(),
		delayed:     r.Bool(),
		countless:   r.Bool(),
		manual:      r.Bool(),
	}
	n := r.Int(1, fuzzMaxMessages*fuzzMaxAttempts)
	for range n {
		sc.outcomes = append(sc.outcomes, r.Choice(outcomeCount))
	}
	return sc
}

// encodeDispatchScript is decodeDispatchScript's inverse, so that a
// hand-written seed reproduces the script it names.
func encodeDispatchScript(sc dispatchScript) []byte {
	w := fuzzutil.NewWriter()
	w.Int(1, fuzzMaxMessages, sc.messages)
	w.Int(1, fuzzMaxKeys, sc.keys)
	w.Int(1, fuzzMaxConcurrency, sc.concurrency)
	w.Choice(3, sc.ordering)
	w.Choice(4, sc.orderMode)
	w.Int(1, fuzzMaxAttempts, sc.maxAttempts)
	for _, b := range []bool{sc.batch, sc.deadLetter, sc.native, sc.extend, sc.delayed, sc.countless, sc.manual} {
		w.Bool(b)
	}
	w.Int(1, fuzzMaxMessages*fuzzMaxAttempts, len(sc.outcomes))
	for _, o := range sc.outcomes {
		w.Choice(outcomeCount, o)
	}
	return w.Data()
}

// runOptions turns a script into the runner's options, dropping the ones the
// configuration cannot honour so that the target exercises the runner rather
// than resolveRun's refusals — which have tests of their own.
func (sc dispatchScript) runOptions() []mq.RunOption {
	opts := []mq.RunOption{
		mq.WithConcurrency(sc.concurrency),
		mq.WithMaxAttempts(sc.maxAttempts),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)),
		mq.WithDrainTimeout(time.Second),
	}
	switch sc.orderMode {
	case 0:
		opts = append(opts, mq.WithOrdering(mq.Ordering(sc.ordering)))
	case 1:
		opts = append(opts, mq.WithOrderingByKey(mq.OrderingStrict))
	case 2:
		opts = append(opts, mq.WithOrderingByKey(mq.OrderingRelaxed))
	default:
		opts = append(opts, mq.WithOrderingByKey(mq.OrderingStopOnFailure))
	}
	switch {
	case sc.native:
		opts = append(opts, mq.WithNativeDeadLetter())
	case sc.deadLetter:
		opts = append(opts, mq.WithDeadLetter("dead"))
	}
	if sc.extend {
		opts = append(opts, mq.WithLeaseExtension(50*time.Millisecond))
	}
	if sc.manual {
		opts = append(opts, mq.WithSettlement(mq.SettlementManual))
	}
	return opts
}

// laneWatch reports two deliveries of one key handled at the same time, which
// The specification forbids under OrderingByKey and OrderingByPartition.
type laneWatch struct {
	mu    sync.Mutex
	live  map[string]int
	clash atomic.Bool
}

func newLaneWatch() *laneWatch { return &laneWatch{live: map[string]int{}} }

func (w *laneWatch) enter(key string) {
	w.mu.Lock()
	w.live[key]++
	if w.live[key] > 1 {
		w.clash.Store(true)
	}
	w.mu.Unlock()
}

func (w *laneWatch) leave(key string) {
	w.mu.Lock()
	w.live[key]--
	w.mu.Unlock()
}

// FuzzDispatcher is the target for the runner: an arbitrary runner
// configuration and an arbitrary sequence of handler outcomes, checked
// against the invariants the API states.
//
// What it looks for is a message that ends up nowhere. The conservation
// over the fake broker — published == acked + ready + in flight + dropped +
// dead-lettered — is the one property that no combination of ordering mode,
// failure policy, dead-letter route, panic, halt or shutdown may break, and disqualifies an implementation that loses an accepted message outside
// a documented failure boundary. Beside it: the per-key exclusion, which
// no concurrency may break, and the in-flight counter returning to zero,
// which is the terminal-release invariant.
func FuzzDispatcher(f *testing.F) {
	seeds := []dispatchScript{
		{messages: 3, keys: 1, concurrency: 1, maxAttempts: 1, outcomes: []int{outcomeAck}},
		{messages: 4, keys: 2, concurrency: 2, maxAttempts: 2, outcomes: []int{outcomeError, outcomeAck}},
		{messages: 2, keys: 1, concurrency: 1, maxAttempts: 1, orderMode: 1, outcomes: []int{outcomeError, outcomeAck}},
		{messages: 2, keys: 2, concurrency: 2, maxAttempts: 1, orderMode: 3, outcomes: []int{outcomeError}},
		{messages: 3, keys: 1, concurrency: 1, maxAttempts: 1, deadLetter: true, outcomes: []int{outcomeDeadLetter}},
		{messages: 3, keys: 2, concurrency: 2, maxAttempts: 1, native: true, outcomes: []int{outcomeError}},
		{messages: 2, keys: 1, concurrency: 1, maxAttempts: 2, outcomes: []int{outcomePanic, outcomeAck}},
		{messages: 4, keys: 2, concurrency: 2, maxAttempts: 1, batch: true, outcomes: []int{outcomeAck, outcomeDiscard}},
		{messages: 4, keys: 1, concurrency: 1, maxAttempts: 1, outcomes: []int{outcomeHalt}},
		{messages: 5, keys: 3, concurrency: 3, maxAttempts: 2, ordering: 1, extend: true, outcomes: []int{outcomeRetryAfter, outcomeAck}},
		{messages: 5, keys: 3, concurrency: 3, maxAttempts: 2, ordering: 2, countless: true, outcomes: []int{outcomeAck}},
		{messages: 3, keys: 1, concurrency: 1, maxAttempts: 1, manual: true, outcomes: []int{outcomeAck}},
	}
	for _, sc := range seeds {
		f.Add(encodeDispatchScript(sc))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		sc := decodeDispatchScript(data)
		runDispatchScript(t, sc)
	})
}

// runDispatchScript runs one script and checks the invariants.
func runDispatchScript(t *testing.T, sc dispatchScript) {
	t.Helper()
	if len(sc.outcomes) == 0 {
		return
	}
	cfg := fakeConfig{
		deadLetter: sc.deadLetter || sc.native,
		delayed:    sc.delayed,
		extend:     sc.extend,
		countless:  sc.countless,
	}
	b := newFakeBroker(cfg, "orders", "dead")
	cl := mq.NewClient(b)
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	for i := range sc.messages {
		id := strconv.Itoa(i)
		m := &mq.Message{ID: id, Key: []byte("k" + strconv.Itoa(i%sc.keys)), Body: []byte("b")}
		if err := cl.Publish(context.Background(), "orders", m); err != nil {
			t.Fatalf("publishing %s: %v", id, err)
		}
	}

	watch := newLaneWatch()
	var calls atomic.Int64
	handled := make(map[string]bool)
	var mu sync.Mutex
	one := func(_ context.Context, d *mq.Delivery) error {
		key := string(d.Key)
		watch.enter(key)
		defer watch.leave(key)
		mu.Lock()
		handled[d.ID] = true
		mu.Unlock()
		n := int(calls.Add(1)) - 1
		switch sc.outcomes[n%len(sc.outcomes)] {
		case outcomeError:
			return errors.New("scripted failure")
		case outcomeRetryAfter:
			return mq.RetryAfter(errors.New("scripted retry"), 0)
		case outcomeDiscard:
			return mq.Discard(errors.New("scripted discard"))
		case outcomeDeadLetter:
			return mq.DeadLetter(errors.New("scripted dead letter"))
		case outcomeHalt:
			return mq.Halt(errors.New("scripted halt"))
		case outcomePanic:
			panic("scripted panic")
		default:
			if sc.manual {
				_ = d.Ack()
			}
			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), fuzzRunDeadline)
	done := make(chan error, 1)
	go func() {
		if sc.batch {
			done <- c.RunBatch(ctx, batchOf(one), sc.batchOptions()...)
			return
		}
		done <- c.Run(ctx, mq.HandlerFunc(one), sc.runOptions()...)
	}()

	// The runner ends by itself when it halts; otherwise it is stopped once
	// every message has reached a handler, or once the deadline says it never
	// will. Both are ends the script may legitimately reach.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		seen := len(handled)
		mu.Unlock()
		if seen >= sc.messages || len(done) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(fuzzRunDeadline):
		t.Fatal("the runner never returned")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("closing the consumer: %v", err)
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("closing the client: %v", err)
	}
	counts := b.counts("orders", "billing")
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v (script %+v)", counts, sc)
	}
	if got := c.Stats().InFlight; got != 0 {
		t.Fatalf("InFlight = %d after close, want 0 (terminal release)", got)
	}
	ordered := sc.orderMode != 0 || mq.Ordering(sc.ordering) != mq.OrderingUnordered
	if ordered && watch.clash.Load() {
		t.Fatalf("two deliveries of one key were handled at once under %v (script %+v)", mq.Ordering(sc.ordering), sc)
	}
}

// batchOptions is runOptions without the ones RunBatch refuses: a batch
// runner cannot hold a batch for a strictly ordered retry ,
// so the script's strict arm becomes the relaxed one.
func (sc dispatchScript) batchOptions() []mq.RunOption {
	if sc.orderMode == 1 {
		sc.orderMode = 2
	}
	return sc.runOptions()
}

// batchFunc adapts a Handler to a BatchHandler by calling it once per
// delivery and returning the index-aligned *BatchError.
type batchFunc func(ctx context.Context, d *mq.Delivery) error

func batchOf(f batchFunc) mq.BatchHandler { return batchAdapter{f: f} }

type batchAdapter struct{ f batchFunc }

// HandleBatch implements mq.BatchHandler.
func (a batchAdapter) HandleBatch(ctx context.Context, ds []*mq.Delivery) error {
	errs := make([]error, len(ds))
	failed := false
	for i, d := range ds {
		errs[i] = a.f(ctx, d)
		if errs[i] != nil {
			failed = true
		}
	}
	if !failed {
		return nil
	}
	return &mq.BatchError{Errs: errs}
}
