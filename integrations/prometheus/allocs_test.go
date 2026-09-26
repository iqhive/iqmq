package prometheus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"

	mq "github.com/iqhive/iqmq"

	iqmqprom "github.com/iqhive/iqmq/integrations/prometheus"
)

// TestAllocs says what this integration costs per event, exactly (the specification's
// "disabled hooks/observer path: 0 allocs, ≤ 1 predictable branch", and rule 2 for the enabled one).
//
// An observability integration must not change what it observes. A hook that
// allocates per message turns the path it measures into a different path, so
// both halves are pinned here:
//
//   - DISABLED is the zero mq.Hooks — what a client built without
//     mq.WithHooks holds. The core builds no event, reads no clock and calls
//     nothing, so the cost is zero by construction and this test measures the
//     shape rather than this package.
//   - ENABLED is every hook and the handler recorder. Each is zero
//     allocations per event: the per-series children are resolved once, on
//     the first event for a label combination, and afterwards found through
//     one atomic load and one map lookup on a comparable key.
//
// The one path that is NOT zero is the first event for a new label
// combination, which builds fourteen prometheus children. That is bounded by
// Cardinality.MaxSeries and is what the cardinality control is for.
func TestAllocs(t *testing.T) {
	if allocsRaceDetector {
		t.Skip("the race detector perturbs allocation counts; `make allocs` runs without it")
	}

	reg := prom.NewPedanticRegistry()
	m, err := iqmqprom.New(iqmqprom.Config{Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	h := m.Hooks()
	rec := m.Recorder()
	batchErr := &mq.BatchError{Errs: []error{nil, errors.New("no")}}

	// The disabled half: a client with no hooks holds the zero value, and
	// every field is nil. Calling nothing costs nothing, and what this row
	// pins is that a nil field is what the core finds.
	var off mq.Hooks
	if off.OnPublishDone != nil || off.OnReceive != nil || off.OnSettle != nil || off.OnReconnect != nil {
		t.Fatal("the zero mq.Hooks is not empty")
	}

	cases := []struct {
		name string
		run  func()
		want float64
		why  string
	}{
		{
			name: "disabled/every hook nil", want: 0,
			why: "the disabled observer path: the core builds no event at all",
			run: func() {
				if off.OnReceive != nil {
					off.OnReceive(context.Background(), mq.ReceiveEvent{})
				}
			},
		},
		{
			name: "OnPublishDone/success", want: 0,
			why: "one atomic load, one map lookup, one Add and two Observes",
			run: func() {
				h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
					Topic: "orders", Messages: 1, Bytes: 64, Duration: time.Millisecond,
				})
			},
		},
		{
			name: "OnPublishDone/whole-batch failure", want: 0,
			why: "the failure path resolves the same children",
			run: func() {
				h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
					Topic: "orders", Messages: 4, Bytes: 64,
					Duration: time.Millisecond, Err: mq.ErrQueueFull,
				})
			},
		},
		{
			name: "OnPublishDone/partial batch", want: 0,
			why: "errors.As against a *mq.BatchError allocates nothing",
			run: func() {
				h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
					Topic: "orders", Messages: 2, Bytes: 64,
					Duration: time.Millisecond, Err: batchErr,
				})
			},
		},
		{
			name: "OnReceive", want: 0,
			why: "one counter Add",
			run: func() {
				h.OnReceive(context.Background(), mq.ReceiveEvent{
					Topic: "orders", Subscription: "billing", Messages: 8,
					Duration: time.Millisecond,
				})
			},
		},
		{
			name: "OnSettle/ack", want: 0,
			why: "one histogram Observe",
			run: func() {
				h.OnSettle(context.Background(), mq.SettleEvent{
					Topic: "orders", Subscription: "billing",
					Kind: mq.SettleAck, Duration: time.Millisecond,
				})
			},
		},
		{
			name: "OnSettle/failed ack", want: 0,
			why: "one Observe and one Inc",
			run: func() {
				h.OnSettle(context.Background(), mq.SettleEvent{
					Topic: "orders", Subscription: "billing", Kind: mq.SettleAck,
					Duration: time.Millisecond, Err: mq.ErrAlreadySettled,
				})
			},
		},
		{
			name: "OnSettle/extend", want: 0,
			why: "one Observe and one Inc on the extension counter",
			run: func() {
				h.OnSettle(context.Background(), mq.SettleEvent{
					Topic: "orders", Subscription: "billing",
					Kind: mq.SettleExtend, Duration: time.Millisecond,
				})
			},
		},
		{
			name: "HandlerDuration", want: 0,
			why: "the outcome switch picks a cached observer; nothing is formatted",
			run: func() {
				rec.HandlerDuration("orders", "billing", "success", time.Millisecond)
			},
		},
	}

	// Every case is run once first, so that the label combination is admitted
	// and what AllocsPerRun measures is the steady state and not the first
	// event's fourteen children.
	for _, c := range cases {
		c.run()
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := testing.AllocsPerRun(200, c.run); got != c.want {
				t.Errorf("%s: %.1f allocs/op, want %.0f (%s)", c.name, got, c.want, c.why)
			}
		})
	}
}

// TestAllocsReconnectIsAlsoZero covers the one hook whose label values come
// from the EVENT rather than from the program's configuration, so its child
// cannot be resolved into a cache ahead of time.
//
// It costs nothing anyway: client_golang hashes the label values on the stack
// and finds an existing child without allocating. The row is separate from
// TestAllocs because the reason it is zero is upstream's and not this
// package's — if a client_golang release starts allocating in
// WithLabelValues, this is the test that says so, and iqmq_reconnects_total
// is the one family where that would matter. A reconnection is not a message
// path, so the fix would be a note rather than a cache keyed on a driver name
// the event supplies.
func TestAllocsReconnectIsAlsoZero(t *testing.T) {
	if allocsRaceDetector {
		t.Skip("the race detector perturbs allocation counts")
	}
	reg := prom.NewPedanticRegistry()
	m, err := iqmqprom.New(iqmqprom.Config{Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	h := m.Hooks()
	ev := mq.ReconnectEvent{Driver: "kafka", Attempt: 1}
	h.OnReconnect(context.Background(), ev)
	if got := testing.AllocsPerRun(200, func() { h.OnReconnect(context.Background(), ev) }); got != 0 {
		t.Errorf("OnReconnect = %.1f allocs/op, want 0 "+
			"(client_golang's label lookup used not to allocate for an existing child)", got)
	}
}
