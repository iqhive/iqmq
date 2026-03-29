package engine

// Arch is one fan-out storage architecture of the engine: where a record
// lives between admission and delivery, and how many copies of it a
// publication makes (the memory engine).
//
// The four kinds are the ones the specification requires: the shared payload
// with per-subscription envelopes that the rest of the engine is shaped
// around, the shared log with a cursor per subscription, the full per
// subscription copy, and the channel baseline. They differ in three
// observable ways — how much a fan-out copies, whether one slow subscription
// holds capacity for the others, and how much a claim costs — and the
// bake-off (the specification) decides which one Auto names.
type Arch uint8

// The architectures. ArchAuto resolves to the benchmark-selected default
// recorded in auto_arch.go.
const (
	// ArchAuto selects the recorded bake-off winner.
	ArchAuto Arch = iota
	// PerSubEnvelopes shares one reference-counted payload record between
	// the subscriptions and gives each of them its own pooled envelope in
	// its own lane: fan-out copies no payload and every subscription has its
	// own capacity, so a slow subscription cannot hold up the others.
	PerSubEnvelopes
	// ChanFanout is the baseline: the ready set of a lane is a Go channel
	// rather than a kind of the queue family, so it measures what the engine
	// buys over the obvious implementation. Capacity is still per
	// subscription.
	ChanFanout
	// FullCopy gives every subscription an independent copy of the payload,
	// made with Config.Clone at publication time, and hands the publisher
	// its own payload back at once. It is what a fan-out costs when nothing
	// is shared.
	FullCopy
	// LogCursor stores one sequence-numbered log per topic partition with a
	// cursor per subscription and a reference count per slot: a publication
	// writes one slot however many subscriptions read it, but a slot is
	// reclaimed only once every subscription has settled it, so the slowest
	// subscription holds capacity for all of them (the memory engine).
	LogCursor

	archCount
)

var archNames = [archCount]string{"auto", "persubenvelopes", "chanfanout", "fullcopy", "logcursor"}

var archIdents = [archCount]string{"ArchAuto", "PerSubEnvelopes", "ChanFanout", "FullCopy", "LogCursor"}

// Arches returns the concrete architectures in declaration order. ArchAuto is
// not one of them.
func Arches() []Arch { return []Arch{PerSubEnvelopes, ChanFanout, FullCopy, LogCursor} }

// String returns the benchmark slug of the architecture.
func (a Arch) String() string {
	if a < archCount {
		return archNames[a]
	}
	return "arch(?)"
}

// Ident returns the Go identifier of the architecture, which is what the
// bake-off writes into auto_arch.go.
func (a Arch) Ident() string {
	if a < archCount {
		return archIdents[a]
	}
	return ""
}

// Valid reports whether a names an architecture or ArchAuto.
func (a Arch) Valid() bool { return a < archCount }

// Resolve maps ArchAuto to the recorded default and leaves every other
// architecture alone.
func (a Arch) Resolve() Arch {
	if a == ArchAuto {
		return ArchAutoKind
	}
	return a
}

// ParseArch maps a slug back to its architecture.
func ParseArch(s string) (Arch, bool) {
	for i, n := range archNames {
		if n == s {
			return Arch(i), true
		}
	}
	return ArchAuto, false
}

// Shared reports whether the architecture stores one record per topic
// partition for every subscription at once, so that capacity is shared and
// the slowest subscription bounds admission for all of them.
func (a Arch) Shared() bool { return a.Resolve() == LogCursor }

// Copies reports whether a publication makes an independent copy of the
// payload for every subscription, which requires Config.Clone.
func (a Arch) Copies() bool { return a.Resolve() == FullCopy }

// Ordered reports whether the architecture delivers the records of a
// partition to a subscription in one first-in-first-out sequence. The three
// per-subscription architectures do: their ready structure is a single queue
// that a requeued record re-enters at the tail. LogCursor does not: a record
// returned by a negative acknowledgement or a lease expiry leaves the shared
// log for the subscription's own redelivery queue, which is served first, so
// a redelivered record overtakes the log records behind it — the
// redelivery-first placement real log brokers use. Whether the engine as a
// whole is ordered also depends on the ready-queue kind and on the
// scheduler kind, which KindSet.Ordered folds in.
func (a Arch) Ordered() bool { return a.Resolve() != LogCursor }
