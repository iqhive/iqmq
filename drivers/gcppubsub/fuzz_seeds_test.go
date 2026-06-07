package gcppubsub

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// The committed seeds. Each one names the shape it reaches, because a seed
// corpus nobody can read is one nobody maintains: `make fuzz-seeds` runs every
// input in testdata/fuzz under -race on every CI run, so these are regression
// tests with an opaque encoding rather than a starting point for the fuzzer
// alone.
//
// They are written with [fuzzutil.Writer] so that the encoding follows
// [genMessage]'s reads. A seed hand-written as a byte string drifts the moment
// a generator gains a branch, which is how a corpus stops covering what its
// comments say it covers.

// attributeSeeds are FuzzPubSubAttributes' seeds.
func attributeSeeds() [][]byte {
	return [][]byte{
		// A plain text message with two headers and no reserved name in
		// sight: the shape every other seed is a deviation from.
		attributeSeed(func(w *fuzzutil.Writer) {
			w.Bool(false) // ordering off
			w.Bool(false) // exactly-once off
			w.Choice(4, 2)
			w.Bytes(64, []byte("payload"))
			w.Choice(4, 2)
			w.Bytes(48, []byte("customer-7"))
			w.Bool(true)
			w.String(32, "app-1")
			w.Bool(false)
			w.Int(0, 6, 2)
			w.Choice(6, 5)
			w.String(16, "trace")
			w.Bytes(16, []byte("abc"))
			w.Choice(6, 5)
			w.String(16, "tenant")
			w.Bytes(16, []byte("acme"))
		}),
		// A nil body and a nil key, which is the distinction draws and
		// protobuf does not.
		attributeSeed(func(w *fuzzutil.Writer) {
			w.Bool(false)
			w.Bool(false)
			w.Choice(4, 0)
			w.Choice(4, 0)
			w.Bool(false)
			w.Bool(false)
			w.Int(0, 6, 0)
		}),
		// An empty body and an empty key, the other half of it.
		attributeSeed(func(w *fuzzutil.Writer) {
			w.Bool(false)
			w.Bool(false)
			w.Choice(4, 1)
			w.Choice(4, 1)
			w.Bool(false)
			w.Bool(false)
			w.Int(0, 6, 0)
		}),
		// A binary key with ordering ON, which is the base64 ordering-key
		// path: a key that is not valid UTF-8 cannot be an ordering key
		// verbatim, and encoding it keeps the guarantee where refusing it
		// would cost publish/key-roundtrip.
		attributeSeed(func(w *fuzzutil.Writer) {
			w.Bool(true)
			w.Bool(false)
			w.Choice(4, 2)
			w.Bytes(64, []byte("payload"))
			w.Choice(4, 2)
			w.Bytes(48, []byte{0x00, 0xff, 0x7f})
			w.Bool(false)
			w.Bool(false)
			w.Int(0, 6, 0)
		}),
		// A header colliding with the reserved iqmq- prefix, which every
		// metadata mode refuses.
		attributeSeed(func(w *fuzzutil.Writer) {
			w.Bool(false)
			w.Bool(false)
			w.Choice(4, 2)
			w.Bytes(64, []byte("x"))
			w.Choice(4, 0)
			w.Bool(false)
			w.Bool(false)
			w.Int(0, 6, 1)
			w.Choice(6, 0)
			w.String(8, "k")
			w.Bytes(8, []byte("v"))
		}),
		// A header key over the 256 bytes that BOTH the service's attribute
		// names and the envelope decoder bound, which is the case
		// MetadataAdaptive is not an answer to.
		attributeSeed(func(w *fuzzutil.Writer) {
			w.Bool(false)
			w.Bool(false)
			w.Choice(4, 2)
			w.Bytes(64, []byte("x"))
			w.Choice(4, 0)
			w.Bool(false)
			w.Bool(false)
			w.Int(0, 6, 1)
			w.Choice(6, 2)
			w.Int(1, 400, 300)
			w.Bytes(8, []byte("v"))
		}),
		// A repeated header key and a binary header value, the two the
		// envelope does fix.
		attributeSeed(func(w *fuzzutil.Writer) {
			w.Bool(false)
			w.Bool(false)
			w.Choice(4, 2)
			w.Bytes(64, []byte("x"))
			w.Choice(4, 0)
			w.Bool(false)
			w.Bool(false)
			w.Int(0, 6, 3)
			w.Choice(6, 3)
			w.Bytes(8, []byte("a"))
			w.Choice(6, 3)
			w.Bytes(8, []byte("b"))
			w.Choice(6, 4)
			w.String(16, "bin")
			w.Int(1, 8, 3)
		}),
		// Every typed property set, including a timestamp, which is the one
		// field whose round trip goes through a text format.
		attributeSeed(func(w *fuzzutil.Writer) {
			w.Bool(false)
			w.Bool(false)
			w.Choice(4, 2)
			w.Bytes(64, []byte("x"))
			w.Choice(4, 0)
			w.Bool(false)
			w.Bool(true)
			w.String(16, "application/json")
			w.String(8, "gzip")
			w.String(16, "corr-1")
			w.String(16, "cause-1")
			w.String(16, "replies")
			w.Bool(true)
			w.Uint32(1_600_000_000)
			w.Int(0, 6, 0)
		}),
	}
}

// attributeSeed renders one seed.
func attributeSeed(fn func(*fuzzutil.Writer)) []byte {
	w := fuzzutil.NewWriter()
	fn(w)
	return w.Data()
}

// errorSeeds are FuzzPubSubErrorMapping's seeds: one per rule the table has,
// so that a rule deleted by accident is a seed that stops reaching it rather
// than a code path nobody notices.
func errorSeeds() [][]byte {
	seeds := make([][]byte, 0, 16)
	ops := 8
	for op := range ops {
		for _, kind := range []int{1, 3} {
			for choice := range statusChoices {
				w := fuzzutil.NewWriter()
				w.Choice(ops, op)
				w.Choice(4, kind)
				w.Choice(statusChoices, choice)
				w.String(16, "topic")
				w.String(16, "sub")
				seeds = append(seeds, w.Data())
			}
		}
	}
	return seeds
}

// statusChoices is how many native errors [statusError] and [sentinelError]
// choose between. It is a constant so that the seed writer and the reader
// agree.
const statusChoices = 13

// statusError builds one gRPC status error from the fuzz input.
func statusError(r *fuzzutil.Reader) error {
	all := [statusChoices]codes.Code{
		codes.NotFound, codes.AlreadyExists, codes.Unauthenticated,
		codes.PermissionDenied, codes.Unimplemented, codes.InvalidArgument,
		codes.OutOfRange, codes.FailedPrecondition, codes.Canceled,
		codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded,
		codes.Aborted,
	}
	return status.Error(all[r.Choice(statusChoices)], "injected")
}

// sentinelError builds one of this driver's own native sentinels from the fuzz
// input.
func sentinelError(r *fuzzutil.Reader) error {
	all := [statusChoices]error{
		psapi.ErrAckIDNotReachable, psapi.ErrOrderingPaused, psapi.ErrNotFound,
		psapi.ErrAlreadyExists, mq.ErrClosed, mq.ErrLeaseLost,
		mq.ErrAlreadySettled, mq.ErrUnsupported, errNoStats,
		errNoDeadLetter, errExtendZero, errDelayTooLong,
		errors.New("unclassified"),
	}
	return all[r.Choice(statusChoices)]
}
