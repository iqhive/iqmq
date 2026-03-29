package engine

// Lane dispatch (the specification A; the API design).
//
// Every class-A internal is selected at construction through an interface, so
// the engine's hot path calls its ready queue through one. The specification says that the
// cost of that indirection is itself measured, and that "if significant, the
// default is bound via build tag while alternates remain reachable for
// testing". This file and the lane_bind_*.go set are that binding.
//
// The three mechanisms are the kinds of the family:
//
//   - interface: the reference. The lane holds a queue.Queue[T] and every
//     push and pop is an indirect call through its itab. Every kind is reachable, which is what the differential tests need.
//   - concrete: the lane holds *queue.MPMC[T]. The call is static and the
//     compiler may inline it across the package boundary. A kind other than
//     the bound one still works — it falls back to the interface — so the
//     tag narrows the fast path without narrowing the configuration.
//   - native: the ring is open-coded in this package, so there is no
//     cross-package call at all and the whole operation is visible to the
//     compiler at the call site. It is the floor the other two are measured
//     against.
//
// Which one a build uses is fixed at compile time by the iqmq_queue_<slug>
// tags and reported by LaneDispatch. BenchmarkAPI/Dispatch measures all three
// in one binary, whatever the build is bound to, so the bake-off does not
// need three of them.

// Dispatch identifies how the engine calls its lane's ready queue.
type Dispatch uint8

// The dispatch mechanisms. DispatchInterface is the reference.
const (
	// DispatchInterface calls through queue.Queue[T].
	DispatchInterface Dispatch = iota
	// DispatchConcrete calls the bound kind's concrete type directly.
	DispatchConcrete
	// DispatchNative calls a ring open-coded in this package.
	DispatchNative

	dispatchCount
)

var dispatchSlugs = [dispatchCount]string{"interface", "concrete", "native"}

var dispatchIdents = [dispatchCount]string{"DispatchInterface", "DispatchConcrete", "DispatchNative"}

// String returns the mechanism's slug, as used in benchmark names and ADRs.
func (d Dispatch) String() string {
	if d < dispatchCount {
		return dispatchSlugs[d]
	}
	return "dispatch(?)"
}

// Ident returns the mechanism's Go identifier.
func (d Dispatch) Ident() string {
	if d < dispatchCount {
		return dispatchIdents[d]
	}
	return ""
}

// Valid reports whether d names a mechanism.
func (d Dispatch) Valid() bool { return d < dispatchCount }

// Dispatches lists every mechanism in declaration order.
func Dispatches() []Dispatch {
	return []Dispatch{DispatchInterface, DispatchConcrete, DispatchNative}
}

// LaneDispatch returns the mechanism this build binds the lane's ready queue
// to. It is decided by the iqmq_queue_<slug> build tags: none of them binds
// the interface, iqmq_queue_mpmc binds queue.MPMC as a concrete type, and
// iqmq_queue_native binds this package's own ring.
func LaneDispatch() Dispatch { return laneDispatch }

// LaneDispatchTag returns the build tag that selected the binding, or the
// empty string for the untagged interface build.
func LaneDispatchTag() string { return laneDispatchTag }
