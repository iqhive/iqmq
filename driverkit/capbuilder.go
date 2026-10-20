package driverkit

import (
	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// CapabilityBuilder builds a driver's capability table and refuses a dishonest
// one (ADR-007).
//
// The validation is the point. The specification requires a conditional support to state its
// condition and an emulation to say what it is, and a table that does not is a
// table a caller cannot act on: "conditional" with no note tells a program that
// something might work, under conditions nobody wrote down. The builder refuses
// to produce one, so the omission is a construction failure in the driver's own
// tests rather than an empty cell in the generated matrix.
//
// It is a construction-time helper: the specification makes capabilities constant per client,
// so a driver builds the table once, at open, and never touches it again.
type CapabilityBuilder struct {
	caps mq.Capabilities
	errs []error
	seen [mq.FeatureCount]bool
}

// NewCapabilities returns an empty builder. The table it produces advertises
// nothing until something is set, which is the honest default: an unadvertised
// feature is Unsupported and an unadvertised limit is unknown.
func NewCapabilities() *CapabilityBuilder { return &CapabilityBuilder{} }

// Native advertises a feature the broker provides itself.
func (b *CapabilityBuilder) Native(f mq.Feature, scope mq.Scope) *CapabilityBuilder {
	return b.set(f, mq.Support{Level: mq.Native, Scope: scope})
}

// Conditional advertises a feature that needs a profile or an option, naming the
// condition. The specification requires the condition, so an empty note is refused.
func (b *CapabilityBuilder) Conditional(f mq.Feature, scope mq.Scope, note string) *CapabilityBuilder {
	if note == "" {
		b.errs = append(b.errs, errConditionalNote)
		return b
	}
	return b.set(f, mq.Support{Level: mq.Conditional, Scope: scope, Note: note})
}

// EmulatedVolatile advertises a feature iqmq emulates in process, naming what
// the emulation is. It NEVER satisfies a durability requirement, so the
// note is what tells a reader why.
func (b *CapabilityBuilder) EmulatedVolatile(f mq.Feature, scope mq.Scope, note string) *CapabilityBuilder {
	if note == "" {
		b.errs = append(b.errs, errEmulationNote)
		return b
	}
	return b.set(f, mq.Support{Level: mq.EmulatedVolatile, Scope: scope, Note: note})
}

// EmulatedDurable advertises a feature iqmq emulates using durable broker
// features, naming the emulation.
func (b *CapabilityBuilder) EmulatedDurable(f mq.Feature, scope mq.Scope, note string) *CapabilityBuilder {
	if note == "" {
		b.errs = append(b.errs, errEmulationNote)
		return b
	}
	return b.set(f, mq.Support{Level: mq.EmulatedDurable, Scope: scope, Note: note})
}

// Unsupported states that a feature is absent. It is the zero value, so calling
// it changes nothing about the table — what it changes is the record that the
// driver considered the feature, which [CapabilityBuilder.Missing] reports on.
func (b *CapabilityBuilder) Unsupported(f mq.Feature) *CapabilityBuilder {
	return b.set(f, mq.Support{Level: mq.Unsupported})
}

// Support records an arbitrary support, for a driver whose table is a table.
func (b *CapabilityBuilder) Support(f mq.Feature, s mq.Support) *CapabilityBuilder {
	switch s.Level {
	case mq.Conditional:
		return b.Conditional(f, s.Scope, s.Note)
	case mq.EmulatedVolatile:
		return b.EmulatedVolatile(f, s.Scope, s.Note)
	case mq.EmulatedDurable:
		return b.EmulatedDurable(f, s.Scope, s.Note)
	case mq.Unsupported, mq.Native:
	}
	return b.set(f, s)
}

// Limits records the numeric limits the driver advertises. Only what the driver
// really knows belongs here: an unadvertised limit is never an overloaded zero
func (b *CapabilityBuilder) Limits(l mq.Limits) *CapabilityBuilder {
	b.caps.SetLimits(l)
	return b
}

// set records one support and remembers that the feature was considered.
func (b *CapabilityBuilder) set(f mq.Feature, s mq.Support) *CapabilityBuilder {
	if !f.Valid() {
		b.errs = append(b.errs, errUnknownFeature)
		return b
	}
	if b.seen[f] {
		b.errs = append(b.errs, errFeatureTwice)
		return b
	}
	if err := b.caps.Set(f, s); err != nil {
		b.errs = append(b.errs, err)
		return b
	}
	b.seen[f] = true
	return b
}

// Missing returns the features the driver never mentioned, in inventory order.
//
// It is not an error: a table that says nothing about a feature says
// "unsupported", which is the safe answer. It is here so that a driver's own
// test can assert the whole inventory was considered, which is the difference
// between a feature a driver decided it does not have and one nobody looked at.
func (b *CapabilityBuilder) Missing() []mq.Feature {
	var out []mq.Feature
	for f := range mq.FeatureCount {
		if !b.seen[mq.Feature(f)] {
			out = append(out, mq.Feature(f))
		}
	}
	return out
}

// Build returns the table, or the first refusal.
func (b *CapabilityBuilder) Build() (mq.Capabilities, error) {
	if len(b.errs) > 0 {
		return mq.Capabilities{}, b.errs[0]
	}
	return b.caps, nil
}

// The builder's refusals.
var (
	errConditionalNote = errcode.New(errcode.InvalidConfig,
		"iqmq/driverkit: a conditional capability must state its condition (the message contract)")
	errEmulationNote = errcode.New(errcode.InvalidConfig,
		"iqmq/driverkit: an emulated capability must say what the emulation is (the message contract)")
	errUnknownFeature = errcode.New(errcode.InvalidArgument,
		"iqmq/driverkit: the feature is not in the inventory")
	errFeatureTwice = errcode.New(errcode.InvalidConfig,
		"iqmq/driverkit: the capability table advertises one feature twice")
)
