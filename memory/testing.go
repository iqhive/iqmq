package memory

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestOption is one setting NewForTest takes.
//
// It is a value token and not a closure, for the reason the specification decided for
// the publication options (ADR-006, Value): a call site names what it wants
// and allocates nothing to do it. The zero TestOption names no setting and is
// reported as a test failure rather than applied.
type TestOption struct {
	field testField
	cfg   Config
	eng   EngineConfig
	topo  TopologyConfig
	names []string
	flag  bool
}

// testField names the setting a TestOption carries. The zero value names
// none, which is what makes a stray TestOption{} a failure rather than a
// silent no-op.
type testField uint8

const (
	testFieldNone testField = iota
	testFieldConfig
	testFieldEngine
	testFieldTopology
	testFieldTopics
	testFieldRealClock

	testFieldCount
)

// TestConfig is the configuration NewForTest starts from. AutoCreate and the
// deterministic clock are applied over it, so a caller sets the fields it
// cares about and keeps the test defaults for the rest.
func TestConfig(cfg Config) TestOption {
	return TestOption{field: testFieldConfig, cfg: cfg}
}

// TestEngine pins the variant kind of every family, which is how a
// differential test sweeps the engine: one subtest per kind set, each one a
// broker of its own.
func TestEngine(eng EngineConfig) TestOption {
	return TestOption{field: testFieldEngine, eng: eng}
}

// TestTopology sets the topology every topic is created with.
func TestTopology(topo TopologyConfig) TestOption {
	return TestOption{field: testFieldTopology, topo: topo}
}

// TestTopics creates the named topics before the client is returned, so that
// a test can run with AutoCreate off — which is what a network driver does —
// without writing the admin calls.
func TestTopics(names ...string) TestOption {
	return TestOption{field: testFieldTopics, names: names}
}

// TestRealClock leaves the broker on the wall clock instead of the
// deterministic one.
//
// It is what a test inside a testing/synctest bubble wants: the wall clock
// follows the bubble clock there, so time is deterministic anyway and a
// forgotten Close surfaces as a bubble deadlock, which is the free leak
// detection. Broker.Advance then reports ErrUnsupported, because the broker
// no longer owns a clock it can move.
func TestRealClock() TestOption {
	return TestOption{field: testFieldRealClock, flag: true}
}

// NewForTest opens a client and its broker for a test (the memory engine).
//
// It differs from [Open] in exactly three ways, and all three are what a test
// wants rather than what a service wants: AutoCreate is on (ADR-016, row
// The specification), the clock is a deterministic [TestClock] so that delays, times to
// live and lease expiries move only when the test says so, and the client is
// closed on cleanup — which is the leak check, because a client that still
// holds an open consumer fails to close and says so.
//
// It fails the test rather than returning an error: a test double that could
// not be built is not a condition a test recovers from.
func NewForTest(t testing.TB, opts ...TestOption) (*mq.Client, *Broker) {
	t.Helper()
	cfg := Config{}
	real := false
	var topics []string
	for i := range opts {
		o := &opts[i]
		switch o.field {
		case testFieldConfig:
			cfg = o.cfg
		case testFieldEngine:
			cfg.Engine = o.eng
		case testFieldTopology:
			cfg.DefaultTopology = o.topo
		case testFieldTopics:
			topics = append(topics, o.names...)
		case testFieldRealClock:
			real = o.flag
		case testFieldNone, testFieldCount:
			t.Fatal("memory.NewForTest: test option names no setting")
		default:
			t.Fatal("memory.NewForTest: test option names no setting")
		}
	}
	cfg.AutoCreate = true
	if !real && cfg.Clock == nil {
		cfg.Clock = NewTestClock(testStart)
	}
	if real {
		cfg.Clock = nil
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("memory.NewForTest: %v", err)
	}
	client := b.client(true)
	for _, name := range topics {
		if _, err := b.eng.CreateTopic(name, b.topo); err != nil {
			t.Fatalf("memory.NewForTest: create topic %q: %v", name, err)
		}
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("memory.NewForTest: closing the client: %v", err)
		}
	})
	return client, b
}

// testStart is the instant a test broker's clock starts at. It is a round
// wall-clock time rather than the epoch so that a timestamp printed by a
// failing test reads as a time, and it is fixed so that two runs of one test
// print the same one.
var testStart = mustParse("2026-01-01T00:00:00Z")

// mustParse is testStart's constructor. A malformed constant is a programming
// error in this file and nowhere else.
func mustParse(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic("iqmq/memory: bad test start time: " + err.Error())
	}
	return t
}

// Drain receives and acknowledges deliveries until it has limit of them or
// ctx is done, and returns the messages it took.
//
// It is the affordance a test writes over and over: publish, take what came
// out, assert on it.
//
// ctx is what bounds it, and a ctx that is never done does not bound it.
// Receive blocks until a delivery arrives, so Drain with context.Background()
// and a limit the broker will not reach blocks for ever — which is the case a
// test that expected fewer messages than it asked for actually hits. Pass a
// ctx with a deadline, or an already-cancelled one to poll:
//
//   - cancelled ctx, nothing ready: (nil, context.Canceled)
//   - cancelled ctx, limit messages ready: (the messages, nil), because the
//     fetch is satisfied before the cancellation is read
//   - ctx with a deadline, fewer than limit ready: what it took by the
//     deadline, and the deadline's error
//
// So the error is part of the answer rather than a failure: a partial return
// with context.DeadlineExceeded is "this is all there was".
//
// Before this comment said Drain "stops as soon as a receive would block,
// so it never hangs a test that expected fewer messages than it got", which is
// the opposite of what it does. It was found by writing the memory-testing
// example against it.
func Drain(ctx context.Context, c *mq.ConsumerHandle, limit int) ([]*mq.Message, error) {
	if limit <= 0 {
		return nil, nil
	}
	out := make([]*mq.Message, 0, limit)
	buf := make([]mq.Delivery, min(limit, 64))
	for len(out) < limit {
		n, err := c.Receive(ctx, buf[:min(len(buf), limit-len(out))])
		for i := range n {
			out = append(out, buf[i].Clone())
			if aerr := buf[i].Ack(); aerr != nil && err == nil {
				err = aerr
			}
		}
		if err != nil {
			return out, err
		}
		if n == 0 {
			return out, nil
		}
	}
	return out, nil
}
