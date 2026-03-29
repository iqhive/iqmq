package engine

// CopyMode is the ownership contract of one publication (the memory engine,
// the specification A). It says who owns the payload the caller hands
// to Publish, and therefore what the engine may do with it and when the
// caller may touch it again.
//
// The three modes are the three answers to that question, and all three are
// implemented, model-tested, fuzzed and benchmarked: the bake-off measures
// what each costs, it does not choose between them, because a caller picks
// the one that matches the buffer it holds.
type CopyMode uint8

// The copy modes. The zero value is CopyDefault, which takes the engine's
// Config.CopyMode.
const (
	// CopyDefault leaves the mode to the engine's Config.CopyMode.
	CopyDefault CopyMode = iota
	// CopySafe copies the payload on admission and hands the caller's own
	// payload straight back: the engine retains nothing of it, so the caller
	// may reuse or mutate its buffer the instant Publish returns. It needs a
	// Config.Clone and is the mode the message contract makes the default of
	// the public API.
	CopySafe
	// CopyOwned transfers the payload to the engine. Publish.Release is
	// called exactly once, when the last subscription that was admitted has
	// settled the record or the record is dropped, and the caller must not
	// touch the payload after a Publish that reports admission. Config.Zero
	// (ZeroOnRelease) scrubs the payload before it is handed back, because
	// the storage is the engine's to scrub.
	CopyOwned
	// CopyBorrowed leaves the payload with the caller, which must keep it
	// valid and unmutated until the engine calls Publish.Release — the
	// completion notification of an asynchronous borrow. The engine copies
	// nothing and scrubs nothing: the storage is not the engine's, so
	// Config.Zero does not apply to it.
	CopyBorrowed

	copyModeCount
)

var copyModeNames = [copyModeCount]string{"default", "safe", "owned", "borrowed"}

var copyModeIdents = [copyModeCount]string{"CopyDefault", "CopySafe", "CopyOwned", "CopyBorrowed"}

// CopyModes returns the concrete copy modes in benchmark order.
func CopyModes() []CopyMode { return []CopyMode{CopySafe, CopyOwned, CopyBorrowed} }

// String returns the lower-case mode name, which is the benchmark dimension
// value of the "mode=" dimension.
func (m CopyMode) String() string {
	if m < copyModeCount {
		return copyModeNames[m]
	}
	return "copymode(?)"
}

// Ident returns the Go identifier of the mode.
func (m CopyMode) Ident() string {
	if m < copyModeCount {
		return copyModeIdents[m]
	}
	return "CopyMode(?)"
}

// Valid reports whether m names a mode or the default.
func (m CopyMode) Valid() bool { return m < copyModeCount }

// ParseCopyMode maps a slug back to its mode.
func ParseCopyMode(s string) (CopyMode, bool) {
	for i, n := range copyModeNames {
		if n == s {
			return CopyMode(i), true
		}
	}
	return CopyDefault, false
}

// Copies reports whether the mode makes the engine take its own copy of the
// payload, which is what lets the caller keep its buffer.
func (m CopyMode) Copies() bool { return m == CopySafe }

// Scrubs reports whether Config.Zero applies to a payload released under the
// mode. It does not for a borrowed payload: the storage belongs to the
// caller, and zeroing it would scribble on memory the engine was only lent.
func (m CopyMode) Scrubs() bool { return m != CopyBorrowed }

// ReceiveMode is how a delivery hands the payload to its consumer
// (the memory engine "zero-copy receive (opt-in; data invalid after
// settlement; debug mode poisons/verifies)").
type ReceiveMode uint8

// The receive modes.
const (
	// ReceiveShared hands the consumer the engine's own payload. It is the
	// default and it copies nothing: the payload is valid until the delivery
	// is settled, after which the record may be released or recycled.
	ReceiveShared ReceiveMode = iota
	// ReceiveCopy hands the consumer an independent copy, which stays valid
	// for ever and costs one Config.Clone per delivery.
	ReceiveCopy
	// ReceiveZeroCopy is ReceiveShared with the use-after-free canary armed:
	// the engine scrubs the payload with Config.Poison as it releases the
	// record, so a consumer that reads a settled delivery sees the canary
	// rather than plausible data. Claim.Alive reports whether the aliased
	// payload is still live.
	ReceiveZeroCopy

	receiveModeCount
)

var receiveModeNames = [receiveModeCount]string{"shared", "copy", "zerocopy"}

var receiveModeIdents = [receiveModeCount]string{"ReceiveShared", "ReceiveCopy", "ReceiveZeroCopy"}

// ReceiveModes returns the receive modes in benchmark order.
func ReceiveModes() []ReceiveMode {
	return []ReceiveMode{ReceiveShared, ReceiveCopy, ReceiveZeroCopy}
}

// String returns the lower-case mode name.
func (m ReceiveMode) String() string {
	if m < receiveModeCount {
		return receiveModeNames[m]
	}
	return "receivemode(?)"
}

// Ident returns the Go identifier of the mode.
func (m ReceiveMode) Ident() string {
	if m < receiveModeCount {
		return receiveModeIdents[m]
	}
	return "ReceiveMode(?)"
}

// Valid reports whether m names a receive mode.
func (m ReceiveMode) Valid() bool { return m < receiveModeCount }

// ParseReceiveMode maps a slug back to its mode.
func ParseReceiveMode(s string) (ReceiveMode, bool) {
	for i, n := range receiveModeNames {
		if n == s {
			return ReceiveMode(i), true
		}
	}
	return ReceiveShared, false
}

// Aliases reports whether a delivery hands out the engine's own payload
// rather than a copy of it.
func (m ReceiveMode) Aliases() bool { return m != ReceiveCopy }

// poisons reports whether a record released under this receive mode is
// scrubbed with Config.Poison. The zero-copy mode arms the canary in every
// build, because it is the mode whose contract says the payload dies at
// settlement; an iqmq_debug build arms it for the shared mode too, whose
// contract says the same thing without promising a diagnosis.
func (m ReceiveMode) poisons() bool {
	return m == ReceiveZeroCopy || (debugCanary && m == ReceiveShared)
}
