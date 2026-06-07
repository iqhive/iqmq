package stomp

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/activemq/protocol"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Sink is one topic's publishing handle.
//
// # The confirmation boundary is a RECEIPT
//
// A STOMP SEND frame has no answer. What the protocol offers instead is the
// `receipt` header: a client that sets one is answered with a RECEIPT frame
// once the broker has processed the frame, and a client that does not is
// answered with nothing at all. So the two boundaries are exactly those two
// cases:
//
//	ConfirmNone      no receipt: the frame is written and the call returns.
//	                 Local admission, which the specification says may be lossy.
//	ConfirmAccepted  a receipt: the broker has the message.
//	ConfirmDurable   a receipt on a `persistent: true` publication, which
//	                 both brokers write to their store before they answer.
//
// A receipt that never arrives is AMBIGUOUS and nothing else: the frame left
// the socket, so the broker may have the message, and a caller told "failed"
// republishes it. errors.go carries that in the rule table rather than at this
// call site, because `MapNet` can only OR in what `ClassifyNet` finds and
// go-stomp's errors are neither a net.Error nor an EOF.
type Sink struct {
	b     *Backend
	topic string
	names sinkNames
	cfg   mq.SinkConfig

	confirm  mq.ConfirmationMode
	receipts bool

	// mu guards the reusable buffers WHILE THEY ARE BEING FILLED. It cannot
	// guard them while they are in use: `Publish` releases it before
	// `conn.Send`, because with receipts on that call waits for a broker
	// round trip. So nothing the lock protects may escape it, and
	// `frameFor` returns a copy of the options rather than the scratch
	// itself -- see the comment there for the race that taught us.
	mu     sync.Mutex
	fields protocol.Fields
	opts   []func(*frame.Frame) error

	closed   atomic.Bool
	seq      atomic.Uint64
	proto    protocol.Protocol
	envelope *driverkit.Envelope
	// forced envelopes unconditionally, for the messages
	// `driverkit.Envelope` cannot see the need for. It is nil unless this
	// client's mode envelopes at all: under native-strict the refusal is
	// the contract and forcing one would be the silent substitution the specification
	// forbids.
	forced *driverkit.Envelope
}

var _ mq.Sink = (*Sink)(nil)

// sinkNames is the destination a sink publishes to, and the dead-letter
// destination a rejection goes to.
type sinkNames struct {
	publish    string
	deadLetter string
}

// OpenSink returns a publishing handle for one topic.
//
// It resolves the destination and refuses an unrepresentable configuration
// BEFORE any frame is written, which is the rule and matters here more than
// it looks: a STOMP broker answers a frame it refuses with an ERROR frame and
// then closes the connection, so one bad publication costs every other
// publisher on that connection its link.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	n, err := b.cfg.Names(cfg.Topic, "")
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", err)
	}
	p, err := protocol.New(protocol.STOMP)
	if err != nil {
		return nil, err
	}
	dest, err := p.Destination(n, protocol.Publish)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", err)
	}
	confirm := cfg.Confirmation.Resolve()
	if confirm == mq.ConfirmDurable && !b.Capabilities().Has(mq.FeatureDurableConfirm) {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", errNoDurableConfirm)
	}
	if confirm != mq.ConfirmNone && !b.cfg.Receipts {
		// The specification again: a publication that must be acknowledged needs the one
		// answer this protocol has, and a client configured without it
		// cannot have the boundary it asked for. Refusing at open is the
		// honest answer; publishing and reporting success would be a
		// promise the wire never made.
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", errNeedsReceipts)
	}
	env, err := newEnvelope(b.cfg, b.Capabilities())
	if err != nil {
		return nil, err
	}
	var forced *driverkit.Envelope
	if b.cfg.Metadata == mq.MetadataAdaptive {
		forced, err = newForcedEnvelope(b.Capabilities())
		if err != nil {
			return nil, err
		}
	}
	s := &Sink{
		b:        b,
		topic:    cfg.Topic,
		names:    sinkNames{publish: dest, deadLetter: n.DeadLetter},
		cfg:      cfg,
		confirm:  confirm,
		receipts: b.cfg.Receipts && confirm != mq.ConfirmNone,
		proto:    p,
		envelope: env,
		forced:   forced,
	}
	b.mu.Lock()
	b.sinks[s] = struct{}{}
	b.mu.Unlock()
	return s, nil
}

// Destination is the destination this sink publishes to, in the broker's own
// form. It is exported because a deployment debugging a virtual topic needs to
// see the name the driver derived rather than the one it asked for.
func (s *Sink) Destination() string { return s.names.publish }

// Publish sends one message and waits for whatever boundary the sink was
// opened with.
func (s *Sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if s.closed.Load() {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", errClosed)
	}
	if err := s.b.CheckOpen(ctx); err != nil {
		return err
	}
	// anything the protocol cannot carry is either wrapped
	// or refused HERE, before a frame is written, so a caller's mistake
	// cannot cost the connection.
	if err := s.envelope.Check(m); err != nil {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	var out *mq.Message
	// Two envelopes, because `driverkit.Envelope.Wrap` re-decides under
	// MetadataAdaptive: it calls its own `Needed` and hands the message
	// back unchanged when the answer is no, so a driver that knows better
	// cannot say so through it. The second envelope is one whose mode
	// always envelopes, used for exactly the messages `Needed` cannot see
	// (see [Sink.needsEnvelope]).
	switch {
	case s.envelope.Needed(m):
		w, err := s.envelope.Wrap(m)
		if err != nil {
			return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
		}
		out = w
	case s.forced != nil && s.needsEnvelope(m):
		w, err := s.forced.Wrap(m)
		if err != nil {
			return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
		}
		out = w
	default:
		// Everything else still goes through the configured envelope's own
		// `Wrap`, because that is the half that agrees with the source's
		// `Unwrap`: under MetadataPortableEnvelope it envelopes every
		// message, and under MetadataAdaptive it envelopes a body the read
		// side would claim as an envelope -- one that begins with the magic.
		// Deciding without it published both natively,
		// and this driver's own source then refused them. In the other two modes it hands the message back.
		w, err := s.envelope.Wrap(m)
		if err != nil {
			return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
		}
		out = w
	}
	conn, _, err := s.b.connection(ctx)
	if err != nil {
		return s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, false)
	}
	s.mu.Lock()
	body, opts, err := s.frameFor(out, o)
	if err != nil {
		s.mu.Unlock()
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	contentType := s.fields.ContentType
	s.mu.Unlock()
	if err := conn.Send(s.names.publish, contentType, body, opts...); err != nil {
		// The frame may have left the socket, so the failure's ambiguity is
		// the rule table's to decide: a receipt that never came and a
		// connection that died are both ambiguous on this path, and a
		// broker's ERROR frame is not.
		if isConnGone(err) {
			s.b.connectionLostFor(conn, err)
		}
		if isReceiptTimeout(err) {
			s.b.ambiguous.Add(1)
		}
		return s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, true)
	}
	s.fillReceipt(m)
	return nil
}

// needsEnvelope is what `driverkit.Envelope.Needed` cannot answer for this
// protocol, because neither thing it is about is a feature.
//
// `Needed` reads the capability table for the HEADERS -- native headers,
// binary values, duplicate keys -- and two of the promises are outside it:
//
//   - A message KEY. Neither ActiveMQ protocol has one, so this driver carries
//     it as the reserved entry `x-iqmq-key`, which over STOMP is a text frame
//     header -- and a key that is not valid UTF-8 cannot go in one. Measured
//     as the specification's `publish/key-roundtrip` failing on a key of "\x00\xff".
//   - A NIL body. The specification makes nil and empty observably different and a STOMP
//     frame body cannot express the difference: a frame with no body and a
//     frame with a zero-length one are the same bytes. Measured as
//     `publish/roundtrip-bodies` reporting "a nil body came back as an empty
//     one".
//
// Both are exactly what the envelope is for, and both are invisible to the
// helper -- so a driver that relied on `Needed` alone would refuse the first
// and silently change the second.
func (s *Sink) needsEnvelope(m *mq.Message) bool {
	if m == nil {
		return false
	}
	return (len(m.Key) > 0 && !utf8.Valid(m.Key)) || m.Body == nil
}

// PublishBatch publishes each message in turn (the emulated batch).
//
// One STOMP SEND frame carries one message and the library has no batched
// write, so this is a loop and the capability table says so: a failure
// part-way through leaves what it had already sent published, and the answer
// says so message by message (stoppedAt).
func (s *Sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	for i := range ms {
		if err := s.Publish(ctx, ms[i], o); err != nil {
			return s.stoppedAt(i, len(ms), err)
		}
	}
	return nil
}

// stoppedAt is the answer for a batch of n that stopped at message i with
// err: index-aligned, with nil for the published prefix, err itself at i, and
// a DEFINITE, retryable not-sent answer for every message after it, whose code
// is errcode.NotAttempted's -- err's own, unless err's code means an unknown
// outcome. stomp's receipt timeout is CodeSettlementUnknown, and the first
// version, which copied it, told every unsent message "may be in the broker".
//
// It used to be a *PartialBatchError{Published: i}, which is not
// index-aligned, so the core gave that one error to every message: the i
// messages already published read as failed, a caller who republished the
// definite ones duplicated them, and when err was ambiguous -- a receipt that
// never came -- the messages never sent read "may be in the broker". A
// failure at the first message with every other message never sent is one
// outcome when err is definite, and driverkit.CollapseBatch answers with err
// alone, as before.
func (s *Sink) stoppedAt(i, n int, err error) error {
	errs := make([]error, n)
	errs[i] = err
	if i+1 < n {
		notSent := &mq.OpError{
			Driver: DriverName, Operation: mq.OpPublish, Topic: s.topic,
			Code:      mq.ErrorCode(errcode.NotAttempted(errcode.Code(mq.CodeOf(err)))),
			Retryable: true,
			Err:       errcode.ErrNotAttempted,
		}
		for j := i + 1; j < n; j++ {
			errs[j] = notSent
		}
	}
	return driverkit.CollapseBatch(errs)
}

// Flush returns when every publication this sink has accepted is resolved,
// which for a synchronous sink is immediately: there is no window here. It is
// present because the specification requires it and answering it honestly is cheaper than
// explaining its absence.
func (s *Sink) Flush(context.Context) error {
	if s.closed.Load() {
		return errClosed
	}
	return nil
}

// Close releases the sink. It is idempotent.
//
// There is nothing to tell the broker: a SEND frame has no link and a
// publisher has no state on the broker at all, so what closes is the driver's
// own bookkeeping.
func (s *Sink) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.b.forget(nil, s)
	return nil
}

// connectionLost is called when the backend's connection died.
//
// A synchronous sink has nothing outstanding -- every publication is resolved
// before Publish returns -- so there is nothing to fail ambiguously here, and
// the next publication attaches to whatever connection the loop establishes.
// The method exists so that the backend's invalidation sequence has one shape
// for both handle kinds.
func (s *Sink) connectionLost() {}

// frameFor maps the message and builds the SEND frame's options.
//
// It runs under the sink's lock and reuses the sink's own buffers, so a
// publication costs no allocation for the mapping itself. The caller must hold
// s.mu.
func (s *Sink) frameFor(m *mq.Message, o *mq.PublishOptions) ([]byte, []func(*frame.Frame) error, error) {
	po := s.b.cfg.protocolOptions()
	po.Limits = s.b.Capabilities().Limits()
	if o != nil {
		po.Publish = *o
	}
	// STOMP's `expires` is an ABSOLUTE instant and TTL is a relative
	// duration, so the clock is read HERE and not in the shared mapping:
	// `internal/activemq/protocol` is pure and refuses a publication whose
	// expiry was not resolved for it.
	if o != nil && o.TTL > 0 {
		po.Expiry = s.b.now().Add(o.TTL)
	}
	if o != nil {
		switch {
		case o.Delay > 0:
			po.Delay = o.Delay
		case !o.DeliverAt.IsZero():
			// The same arithmetic in the other direction: the brokers'
			// STOMP header is a DELAY and the caller named an instant.
			d := o.DeliverAt.Sub(s.b.now())
			if d > 0 {
				po.Delay = d
				break
			}
			// A delivery time that has ALREADY PASSED is due now, so the
			// publication carries no delay at all -- and the instant is
			// cleared with it, because the mapping reads "DeliverAt set and
			// Delay zero" as "the driver did not resolve it" and refuses
			// the publication with ErrNeedsResolvedDelay. Clamping to zero
			// without clearing it turned the case a retry loop produces
			// most often -- a DeliverAt recomputed from a stored instant --
			// into a refusal naming a contract the caller cannot act on.
			po.Publish.DeliverAt = time.Time{}
			po.Publish.Delay = 0
		}
	}
	if s.receipts {
		po.Receipt = s.receiptID()
	}
	if err := s.proto.Carry(&s.fields, m, po); err != nil {
		return nil, nil, err
	}
	s.opts = s.opts[:0]
	s.opts = appendHeaders(s.opts, &s.fields, s.receipts)
	// A COPY, and this is the whole of the fix for a real data race.
	//
	// `s.opts` is scratch shared by every publication on this sink, and
	// `Publish` releases `s.mu` before it hands the slice to `conn.Send` --
	// it has to, because with receipts on, Send waits for a broker round
	// trip and holding the lock across that would turn N concurrent
	// publishers into N sequential ones. So the lock protected the BUILDING
	// of the buffer and not its USE: the next publication truncated and
	// refilled the same backing array while the previous one was still
	// writing it to the socket.
	//
	// `go test -race` reports it as two writes inside `appendHeaders`, and
	// `publish/concurrent-eight-goroutines` is the scenario that reaches
	// it -- on both STOMP profiles, because this file serves both. Nothing
	// had seen it, because every conformance suite was dying before it got
	// this far.
	//
	// It costs one allocation per publication and the pinned ceiling moved
	// from sixteen to seventeen to say so. A `sync.Pool` was tried and
	// measured WORSE -- eighteen -- because the pooled slice regrows where
	// the sink's own scratch had already settled at its capacity; buying
	// back one allocation was not worth a second mechanism in the publish
	// path. The scratch stays for exactly that capacity.
	out := make([]func(*frame.Frame) error, len(s.opts))
	copy(out, s.opts)
	return m.Body, out, nil
}

// receiptID is the identifier a publication asks the broker to answer with.
//
// go-stomp generates one of its own for `SendOpt.Receipt`, and this driver
// names its own instead so that the identifier is in the mapping's Fields and
// therefore visible to a test and to a log line. A receipt nobody can name is
// a round trip nobody can debug.
func (s *Sink) receiptID() string {
	return "iqmq-" + strconv.FormatUint(s.seq.Add(1), 36)
}

// fillReceipt records what the broker said, for a publisher that asked for
// receipts.
//
// STOMP's RECEIPT frame carries the receipt id and NOTHING else: no broker
// message identifier, no offset, no timestamp. So the receipt this driver can
// fill is the fact of acceptance, and MessageID is left EMPTY rather than
// filled with the identifier the publisher itself sent -- reporting a caller's
// own value back as the broker's would be inventing evidence.
func (s *Sink) fillReceipt(m *mq.Message) {
	if !s.cfg.Receipts || m == nil || m.Receipt == nil {
		return
	}
	// STOMP's RECEIPT frame carries the receipt id and nothing else, so what
	// this driver can report truthfully is the BOUNDARY and the time it was
	// reached. MessageID stays empty: the broker assigned none, and filling
	// it with the identifier the publisher itself sent would be reporting a
	// caller's own value back as the broker's.
	m.Receipt.Confirmation = s.confirm
	m.Receipt.ConfirmedAt = s.b.now()
}

// appendHeaders turns the mapped fields into SEND frame options.
//
// The order is fixed -- the protocol's own headers, then the extensible
// entries -- so that two publications of one message produce the same frame,
// which is what makes the wire reproducible for a test.
func appendHeaders(dst []func(*frame.Frame) error, f *protocol.Fields, receipts bool) []func(*frame.Frame) error {
	add := func(k, v string) {
		if v != "" {
			dst = append(dst, gostomp.SendOpt.Header(k, v))
		}
	}
	add("content-type", f.ContentType)
	add("correlation-id", f.CorrelationID)
	add("reply-to", f.ReplyTo)
	if len(f.GroupID) > 0 {
		add(protocol.PropertyGroupID, string(f.GroupID))
	}
	if f.Durable {
		add("persistent", "true")
	}
	if f.HasPriority {
		add("priority", strconv.Itoa(int(f.Priority)))
	}
	if !f.Expiry.IsZero() {
		add("expires", strconv.FormatInt(f.Expiry.UnixMilli(), 10))
	}
	if !f.Timestamp.IsZero() {
		add("timestamp", strconv.FormatInt(f.Timestamp.UnixMilli(), 10))
	}
	if f.Slot != protocol.NoDelay {
		delayKey, atKey := f.Slot.Keys()
		if f.Delay > 0 && delayKey != "" {
			add(delayKey, strconv.FormatInt(int64(f.Delay/time.Millisecond), 10))
		}
		if !f.DeliverAt.IsZero() && atKey != "" {
			add(atKey, strconv.FormatInt(f.DeliverAt.UnixMilli(), 10))
		}
	}
	// An APPLICATION header is written whatever its value, empty included,
	// which is what `add` above must not be used for. The presence-flag
	// pattern -- a header whose name is the message and whose value is "" --
	// is a header a caller set deliberately, and `Carry` accepts it and
	// reports nothing dropped, so skipping it here lost it in silence. The
	// skip is right for the driver's OWN optional fields above, where an
	// empty string means "this message has no correlation identifier".
	always := func(k, v string) {
		dst = append(dst, gostomp.SendOpt.Header(k, v))
	}
	for i := range f.Props {
		e := &f.Props[i]
		switch e.Kind {
		case protocol.Text:
			always(e.Key, e.Text)
		case protocol.Int:
			always(e.Key, strconv.FormatInt(e.Int, 10))
		case protocol.Binary:
			// The shared mapping refuses a binary value over this protocol
			// before it gets here, so this is unreachable; carrying the
			// bytes as text would be the mangling the refusal exists to
			// prevent.
			always(e.Key, string(e.Bin))
		}
	}
	if receipts && f.Receipt != "" {
		dst = append(dst, gostomp.SendOpt.Header("receipt", f.Receipt))
	}
	return dst
}

// newEnvelope is the wrapper for metadata the protocol cannot carry.
//
// The capability table it is given is the WIRE's and not the driver's, and the
// difference is a circularity the conformance suite found. `Envelope.Needed`
// asks the table whether the broker can carry a binary or a duplicate header,
// and this driver ADVERTISES both at `EmulatedDurable` precisely because the
// envelope carries them — so passing the advertised table made `Needed` answer
// "no envelope required", the mapping then refused the header, and
// `metadata/envelope-roundtrip` failed with "a STOMP frame header is text" on
// a publication the envelope existed to carry. Claiming the emulation stopped
// the emulation happening.
//
// So the two rows the envelope decides on are withdrawn for its own copy of
// the table. What is advertised to a CALLER is what the client can do; what the
// envelope reads is what the frame can do.
func newEnvelope(cfg Config, caps mq.Capabilities) (*driverkit.Envelope, error) {
	return driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         cfg.Metadata,
		Capabilities: wireCapabilities(caps),
		Limits:       nil,
	})
}

// newForcedEnvelope is an envelope whose mode always envelopes, for the
// messages `driverkit.Envelope.Needed` cannot see the need for.
//
// It exists because `Wrap` re-decides: under MetadataAdaptive it calls
// `Needed` itself and returns the message unchanged when the answer is no, so
// a driver that knows its protocol cannot carry something has no way to say so
// through the configured envelope. Under MetadataPortableEnvelope `Wrap`
// always encodes and this is never reached; under native-strict it must never
// be, because there the refusal IS the contract.
func newForcedEnvelope(caps mq.Capabilities) (*driverkit.Envelope, error) {
	return driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mq.MetadataPortableEnvelope,
		Capabilities: wireCapabilities(caps),
		Limits:       nil,
	})
}

// wireCapabilities is the advertised table with the emulated header rows
// withdrawn, so that the envelope decides from the protocol rather than from
// the driver's own claim about itself.
func wireCapabilities(caps mq.Capabilities) mq.Capabilities {
	b := driverkit.NewCapabilities()
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		switch f {
		case mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders:
			b.Unsupported(f)
		default:
			b.Support(f, caps.Support(f))
		}
	}
	b.Limits(caps.Limits())
	out, err := b.Build()
	if err != nil {
		// Unreachable: every row came from a table the builder already
		// accepted. Returning the original rather than panicking keeps a
		// future feature added to the inventory from crashing an open.
		return caps
	}
	return out
}
