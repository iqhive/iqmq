// Package hooks measures the observer row:
//
//	Disabled hooks/observer path | 0 allocs, <= 1 predictable branch
//
// and, beside it, what the two shipped observability integrations cost when
// they are switched on. It exists because that row was the only figure
// with no benchmark anywhere in the repository: `TestAllocs` in
// integrations/prometheus and integrations/otel pins their allocation counts
// exactly -- zero per event for every Prometheus hook, three to five for an
// OpenTelemetry span under a non-recording tracer -- so the claim "an
// observability integration must not change what it observes" was established
// for allocations and merely asserted for time.
//
// The driver under all three arms is a Backend that discards and allocates
// nothing per message, which is the same device the runner-dispatch row is
// measured over: what is left in the number is the core's own publication path
// plus whatever the installed hooks add. That is what makes the DIFFERENCE
// between the arms the observer path's cost, and the `disabled` arm's
// allocs/op the figure rather than the memory driver's.
//
// Two limits worth knowing before reading a number from it. Only
// `OnPublishStart` and `OnPublishDone` are on this path: `OnReceive` and
// `OnSettle` need a source that hands out deliveries and are not covered here.
// And the specification's "<= 1 predictable branch" is not a thing a benchmark can
// measure; the allocation half is.
package hooks

import (
	"context"
	"fmt"

	mq "github.com/iqhive/iqmq"
)

// Backend is the smallest Backend the SPI allows that can accept a
// publication: one sink, which discards, and no source.
type Backend struct {
	caps mq.Capabilities
}

// NewBackend builds the backend and its capability table, or fails when the
// table cannot be built.
func NewBackend() (Backend, error) {
	var caps mq.Capabilities
	if err := caps.Set(mq.FeatureAtLeastOnce, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic}); err != nil {
		return Backend{}, fmt.Errorf("hooks: capabilities: %w", err)
	}
	return Backend{caps: caps}, nil
}

// Info names the driver in every event the hooks observe.
func (Backend) Info() mq.DriverInfo {
	return mq.DriverInfo{Name: "hooksbench", DriverVersion: "v0"}
}

// Capabilities advertises the least a publisher needs, so that nothing in the
// core takes an optional path this benchmark is not about.
func (b Backend) Capabilities() mq.Capabilities { return b.caps }

// OpenSink returns the discarding sink.
func (Backend) OpenSink(context.Context, mq.SinkConfig) (mq.Sink, error) { return sink{}, nil }

// OpenSource refuses: this package measures the publication path.
func (Backend) OpenSource(context.Context, mq.SourceConfig) (mq.Source, error) {
	return nil, mq.ErrUnsupported
}

// Ping always succeeds.
func (Backend) Ping(context.Context) error { return nil }

// Native exposes nothing.
func (Backend) Native() any { return nil }

// Close releases nothing.
func (Backend) Close() error { return nil }

// sink accepts every publication and allocates nothing.
type sink struct{}

func (sink) Publish(context.Context, *mq.Message, *mq.PublishOptions) error { return nil }

func (sink) Close() error { return nil }
