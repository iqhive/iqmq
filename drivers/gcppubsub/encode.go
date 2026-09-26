package gcppubsub

import (
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// Encoder is this driver's mapping, exported for the tests that
// measure it.
//
// It exists because the mapping is the one part of a driver whose cost is
// worth a number (the specification asks a driver for its own) and whose refusals are
// worth asserting without a broker — and because the alternative was an
// unexported hook the tests reached through a file in the driver's package,
// which is a surface either way. It takes a
// [testing.TB] so that it cannot be built from production code by accident:
// the driver's own sinks and sources use the unexported encoder directly.
type Encoder struct{ enc *encoder }

// NewEncoder builds the mapping for one publisher or consumer, as the driver
// would build it.
func NewEncoder(tb testing.TB, cfg Config, mode mq.MetadataMode) *Encoder {
	tb.Helper()
	resolved := cfg.resolve()
	caps, err := capabilities(resolved)
	if err != nil {
		tb.Fatalf("capabilities: %v", err)
	}
	enc, err := newEncoder(resolved, caps, mode)
	if err != nil {
		tb.Fatalf("newEncoder: %v", err)
	}
	return &Encoder{enc: enc}
}

// Publication is one message in the shape the service takes it.
type Publication struct {
	// Out is what goes to the client library: the data, the attributes and
	// the ordering key.
	Out psapi.Outgoing
	// Enveloped reports that Out.Data is an iqmq envelope rather than the
	// caller's payload.
	Enveloped bool
	// Bytes is the accounted size: the data, plus every attribute name and
	// value, plus the ordering key — which is what the service's
	// ten-megabyte limit counts.
	Bytes int
}

// Encode turns one message and its resolved publication options into the
// service's shape, refusing everything that can be refused before any I/O.
func (e *Encoder) Encode(m *mq.Message, o *mq.PublishOptions) (Publication, error) {
	out, err := e.enc.encode(m, o)
	if err != nil {
		return Publication{}, err
	}
	// A conversion rather than a field-by-field copy, and that is the
	// stricter choice: it compiles only while the internal shape and the
	// exported one are identical, so a field added to the encoder's own
	// struct and not to this one is a build failure rather than a value the
	// exported view silently omits.
	return Publication(out), nil
}

// Dropped is how many headers this encoder has discarded under
// MetadataLenientDrop, which is the metric.
func (e *Encoder) Dropped() int64 { return e.enc.Dropped() }
