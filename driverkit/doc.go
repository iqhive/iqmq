// Package driverkit is the toolbox a backend driver is written with
// (the SPI).
//
// Nothing here is required: a driver that implements
// [github.com/iqhive/iqmq.Backend] and nothing else works. What these helpers
// exist for is the handful of problems every driver has to solve identically
// and that are easy to solve wrongly in a way no test notices:
//
//   - [PushAdapter] turns a callback-delivering client into the pull Fetch the
//     SPI asks for, with real backpressure. A push adapter that buffers without
//     bound is the defect this type exists to prevent: the broker keeps handing
//     the driver messages, the ring keeps growing, and the process dies of
//     memory rather than slowing down.
//   - [TokenTable] hands out the packed token and generation a settlement is
//     resolved through, so a stale settlement is refused rather than applied to
//     the delivery that took its slot.
//   - [BatchSettler] coalesces settlements into the batched round trip a broker
//     charges per call for.
//   - [Reconnector] is the dial loop, with the jittered backoff the specification requires and
//     the generation bump a reconnection has to publish.
//   - [ErrorMapper] maps a native error to a portable code, a retryability and
//     an ambiguity, preserving the native error for errors.As.
//   - [CapabilityBuilder], [Health], [Ownership], [BaseBackend] and [Envelope]
//     are the smaller obligations: an honest capability table, the health state
//     machine, borrowed-versus-owned native clients, close-once bookkeeping, and
//     the envelope a header-less broker needs in order to carry the headers.
//     [BaseBackend.Close] runs the driver's teardown and the native client's
//     close with no lock of its own held, because every real client calls back
//     into its owner from inside its own Close and a callback that asked the
//     backend a question would otherwise deadlock.
//
// # Wrapping a settler
//
// driverkit's own test for this is a table rather than a case: every optional
// interface the specification defines against every wrapper in the package, both directions,
// with a source scan that fails when a wrapper is added and not measured.
//
// # Refusing before any I/O
//
// The other shape of silent damage is a publication the driver accepts and
// cannot read back. [Envelope.Check] is the guard: it answers, from the
// decoder's own limits and validation rules, whether the envelope
// [Envelope.Wrap] is about to produce will decode, and Wrap refuses anything
// it refuses. Without it a header key one byte over the bound, or an
// identifier that is not valid UTF-8, became a record that broke every
// subsequent Fetch on that subscription for ever — undeliverable and therefore
// unsettleable, made by the driver out of a publication it had accepted.
//
// # Hot paths
//
// This package is on the performance-critical list: fmt.Errorf, fmt.Sprintf,
// time.Now and context.WithTimeout are forbidden here. Errors are the
// preallocated values of the root package, and time comes from the
// [github.com/iqhive/iqmq.Clock] a helper is built with.
package driverkit
