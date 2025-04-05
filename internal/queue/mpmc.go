package queue

// MPMC is the SeqMPMC kind named as a concrete type, so that a caller may
// hold one without an interface between it and its ring.
//
// It exists (the specification A, the API design):
// the engine binds its lane to this type under the iqmq_queue_mpmc build tag
// and calls it without dynamic dispatch, and BenchmarkAPI/Dispatch measures
// what that is worth against the interface. The slug is the one
// the performance budgets uses for this kind in BenchmarkMemory/Queue.
//
// Nothing else changes: New(SeqMPMC, capacity) returns the same
// implementation behind Queue, and the two are interchangeable.
type MPMC[T any] = seqMPMC[T]

// NewMPMC returns a bounded multi-producer, multi-consumer queue of the
// SeqMPMC kind as its concrete type. capacity is rounded up to a power of
// two, as it is for New.
func NewMPMC[T any](capacity int) *MPMC[T] { return newSeqMPMC[T](capacity) }
