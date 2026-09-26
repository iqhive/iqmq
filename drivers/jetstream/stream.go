package jetstream

import (
	"context"
	"time"

	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
)

// ensureStream binds this client to its stream, creating it only when the
// configuration asked for that.
//
// The specification forbids a driver from provisioning topology behind the caller's back, so
// [Config.ManageStream] is off by default and a client whose stream does not
// exist fails at [Open] with CodeNotFound rather than quietly creating one. A
// deployment that wants the driver to own the stream says so, and then gets the
// stream this configuration describes.
//
// The returned [github.com/nats-io/nats.go/jetstream.StreamInfo] is what every
// conditional capability claim is read off, and it is what the SERVER said
// rather than what this client asked for: a 2.10 server ignores `allow_msg_ttl`
// and `allow_msg_schedules` outright — unknown JSON fields — so a stream created
// with both flags comes back with neither, and the capability table flips
// without a version comparison anywhere in this driver.
func (b *Backend) ensureStream(ctx context.Context) (js.Stream, *js.StreamInfo, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	if !b.cfg.ManageStream {
		stream, err := b.js.Stream(ctx, b.cfg.Stream)
		if err != nil {
			return nil, nil, b.errs.Map(mq.OpOpen, "", "", b.nameStream(err))
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return nil, nil, b.errs.Map(mq.OpOpen, "", "", b.nameStream(err))
		}
		return stream, info, nil
	}
	stream, err := b.js.CreateOrUpdateStream(ctx, b.streamConfig())
	if err != nil {
		return nil, nil, b.errs.Map(mq.OpOpen, "", "", b.nameStream(err))
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, nil, b.errs.Map(mq.OpOpen, "", "", b.nameStream(err))
	}
	return stream, info, nil
}

// nameStream puts the stream's name into an Open failure.
//
// The client library's own message is "nats: API error: code=404 err_code=10059
// description=stream not found", which says everything except WHICH stream —
// and a deployment with five of them, or one whose stream name comes from a DSN
// somebody else wrote, cannot act on that. The native error is kept underneath,
// so the error mapper's MatchIs still classifies it (CodeNotFound for
// ErrStreamNotFound) and a caller's errors.As still reaches the library's own
// type. Found by drivers/natsunified, whose ADR-016 test asserts that a DSN
// naming a stream that does not exist says which.
func (b *Backend) nameStream(err error) error {
	if err == nil {
		return nil
	}
	return errStreamOp{stream: b.cfg.Stream, err: err}
}

// errStreamOp names the stream an Open failure is about and wraps the native
// error.
type errStreamOp struct {
	stream string
	err    error
}

func (e errStreamOp) Error() string {
	return "stream " + e.stream + ": " + e.err.Error()
}

func (e errStreamOp) Unwrap() error { return e.err }

// streamConfig is the stream this configuration describes.
//
// The two version-gated flags are always asked for and never insisted on. A
// server that has them gives them; one that does not ignores the fields, and the
// capability table reads the difference off the stream rather than off a version
// string — which is the only way this can be right on a server whose version
// this driver has never heard of.
func (b *Backend) streamConfig() js.StreamConfig {
	cfg := js.StreamConfig{
		Name:        b.cfg.Stream,
		Description: "iqmq " + DriverName,
		Subjects:    b.cfg.StreamSubjects,
		Storage:     js.FileStorage,
		Retention:   js.LimitsPolicy,
		Duplicates:  b.cfg.DuplicateWindow,
		MaxAge:      b.cfg.MaxAge,
		MaxMsgs:     streamBound(b.cfg.MaxMsgs),
		MaxBytes:    streamBound(b.cfg.MaxBytes),
		// streamBound maps this driver's zero onto JetStream's -1, and
		// Validate has already refused a negative MaxMsgSize, so the
		// conversion is either the caller's own int32 or -1.
		MaxMsgSize:  int32(streamBound(int64(b.cfg.MaxMsgSize))),
		Replicas:    b.cfg.Replicas,
		AllowMsgTTL: true,
	}
	if b.cfg.Storage == StorageMemory {
		cfg.Storage = js.MemoryStorage
	}
	if !b.cfg.DisableSchedules {
		cfg.AllowMsgSchedules = true
	}
	return cfg
}

// streamBound maps this driver's "zero means the server's default" onto
// JetStream's "-1 means unlimited".
//
// The two spellings are not the same thing and conflating them is how a stream
// comes back with a bound nobody asked for: the server's default for MaxMsgs is
// -1, and sending a literal 0 would ask for a stream that stores no messages at
// all.
func streamBound(n int64) int64 {
	if n <= 0 {
		return -1
	}
	return n
}

// deliverPolicy maps the start positions onto a consumer's delivery policy.
//
// Every one of the four is reachable on this broker, which is what makes
// FeatureReplay, FeatureSeekByTime and FeatureSeekBySequence native claims rather
// than refusals. The policy only decides where a consumer with no committed
// position BEGINS: a durable that already has an ack floor resumes from it,
// which is what a durable subscription means.
func deliverPolicy(start mq.StartPosition) (js.DeliverPolicy, uint64, *time.Time, error) {
	switch start.Resolve().Kind() {
	case mq.StartKindLatest:
		return js.DeliverNewPolicy, 0, nil, nil
	case mq.StartKindEarliest:
		return js.DeliverAllPolicy, 0, nil, nil
	case mq.StartKindSequence:
		seq := start.Sequence()
		if seq == 0 {
			// Stream sequences begin at 1, so zero is not a position. The
			// server would answer with a bad-request round trip; refusing
			// here keeps the specification's "validate before I/O".
			return 0, 0, nil, errBadStartSequence
		}
		return js.DeliverByStartSequencePolicy, seq, nil, nil
	case mq.StartKindTime:
		at := start.Time()
		return js.DeliverByStartTimePolicy, 0, &at, nil
	default:
		return 0, 0, nil, errBadStartPosition
	}
}
