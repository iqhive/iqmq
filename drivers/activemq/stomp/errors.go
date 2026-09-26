package stomp

import (
	"errors"
	"strconv"
	"strings"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The error table.
//
// # What a STOMP failure looks like, and why the table is mostly strings
//
// STOMP has no error codes. A broker that refuses something sends an ERROR
// frame whose `message` header is a sentence, and go-stomp turns that frame
// into a plain error carrying the sentence — so a driver that wants the specification's
// codes has to read the sentence. That is unpleasant and it is what the
// protocol gives: the alternative is to map every broker refusal to one code,
// which would make a missing destination and a bad selector the same error.
//
// The sentences are the brokers' own and are matched on their CODES where
// there is one. Artemis prefixes its own (`AMQ339001`), and Classic does not
// prefix anything — so the Classic rules match on a phrase and are ordered
// after the coded ones, which is the same "specific before general" ordering
// `drivers/amqp10`'s condition table has.
//
// # An ERROR frame is fatal to the connection
//
// Measured: a broker that answers an ERROR frame CLOSES the connection after
// it. So a rule here does not only classify a failure, it also decides whether
// the connection is gone -- and every rule below that names a broker refusal
// is marked retryable only when a retry could succeed on a NEW connection.
func newErrorMapper() *driverkit.ErrorMapper {
	// A destination that does not exist. Artemis with auto-creation disabled
	// refuses a SUBSCRIBE with "AMQ339001 Destination does not exist
	// <name>", measured; Classic creates the destination instead and never
	// says this.
	notFound := []driverkit.Rule{
		{Match: driverkit.MatchContains("AMQ339001"), Code: mq.CodeNotFound},
		{Match: driverkit.MatchContains("Destination does not exist"), Code: mq.CodeNotFound},
		{Match: driverkit.MatchContains("AMQ229017"), Code: mq.CodeNotFound},
	}
	// Authentication and authorisation.
	auth := []driverkit.Rule{
		{Match: driverkit.MatchContains("AMQ229031", "Unable to validate user"), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchContains("AMQ229032", "does not have permission"), Code: mq.CodeForbidden},
		{Match: driverkit.MatchContains("User name [null] is not authenticated"), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchContains("Not authorized"), Code: mq.CodeForbidden},
	}
	// A refusal the broker made about the frame itself, which a retry cannot
	// fix.
	shape := []driverkit.Rule{
		{Match: driverkit.MatchContains("Invalid selector", "Invalid subscription id", "invalid selector"), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchContains("Poison ack cannot be transacted"), Code: mq.CodeUnsupported},
		{Match: driverkit.MatchContains("Message exceeds", "exceeds the maximum"), Code: mq.CodeMessageTooLarge},
	}
	// The library's own refusals, which are the shape of a caller's mistake
	// rather than a broker's answer.
	// Neither of the library's two closed-connection errors is in this set,
	// and that is the correction forced. `ErrAlreadyClosed` and
	// `ErrClosedUnexpectedly` are the TRANSPORT's death, not the caller's
	// handle being closed -- a closed handle is refused by the driver's own
	// guard before any frame is written -- and a rule here would claim them
	// before `gone` does, which on the publish path is the difference
	// between "definitely failed" and "may have been taken". Measured:
	//
	//	the publication that met the connection's death reports activemq:
	//	publish topic=orders: Closed: connection already closed (code
	//	Closed), ambiguous=false
	//
	// `isConnGone` matches both sentinels, so removing them from here moves
	// them to `ambiguous(gone)` on publish and `lost(gone)` on a settlement
	// without loosening anything else.
	library := []driverkit.Rule{
		{Match: driverkit.MatchIs(gostomp.ErrInvalidCommand, gostomp.ErrInvalidFrameFormat), Code: mq.CodeCorruptMessage},
		{Match: driverkit.MatchIs(gostomp.ErrNackNotSupported), Code: mq.CodeUnsupported},
		// A send timeout is the one transport failure that is NOT ambiguous:
		// go-stomp's write channel either took the frame or it did not, and
		// this error is the case where it did not, so the frame never
		// reached the writer and the broker cannot have the message.
		{Match: driverkit.MatchIs(gostomp.ErrMsgSendTimeout), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(errNotConnected), Code: mq.CodeUnavailable, Retryable: true},
	}
	// A receipt that never arrived. It is AMBIGUOUS on the publish path and
	// nothing else: the frame left the socket, so the broker may have the
	// message and a caller told "failed" republishes it.
	receipt := []driverkit.Rule{
		{Match: isReceiptTimeout, Code: mq.CodeSettlementUnknown, Retryable: true, Ambiguous: true},
	}
	// The connection died. Every one of these is retryable on a new
	// connection, and on the publish path every one of them is ambiguous.
	gone := []driverkit.Rule{
		{Match: isConnGone, Code: mq.CodeUnavailable, Retryable: true},
	}
	answered := concat(notFound, auth, shape, library)
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			// The specification: a transfer that left the socket may have been taken, so
			// every transport failure on the publish path is ambiguous. It
			// is the RULE that carries the ambiguity and not the call site,
			// because `MapNet` can only OR in what `ClassifyNet` finds and
			// go-stomp's errors are neither a net.Error nor an EOF -- the
			// hole `drivers/amqp10`'s table records as the in
			// a new skin.
			mq.OpPublish: concat(answered, receipt, ambiguous(gone)),
			mq.OpFetch:   concat(answered, gone),
			// An ACK or a NACK is one frame with no answer unless receipts
			// are on. Under receipts a failure is an UNKNOWN settlement,
			// which the specification makes permanent; without them the frame either
			// went or the connection is gone, and a connection that is gone
			// means the broker will redeliver -- a lost lease.
			mq.OpAck:    concat(answered, receipt, lost(gone)),
			mq.OpNack:   concat(answered, receipt, lost(gone)),
			mq.OpExtend: concat(answered, gone),
			mq.OpOpen:   concat(answered, gone),
			mq.OpClose:  concat(answered, gone),
			mq.OpAdmin:  concat(answered, gone),
		},
		Fallback: concat(answered, gone),
	}
}

// concat joins rule sets in order, which is what makes "specific before
// general" a property of the table rather than of each call.
func concat(sets ...[]driverkit.Rule) []driverkit.Rule {
	n := 0
	for _, s := range sets {
		n += len(s)
	}
	out := make([]driverkit.Rule, 0, n)
	for _, s := range sets {
		out = append(out, s...)
	}
	return out
}

// ambiguous copies a rule set with the ambiguity set, for the publish path.
func ambiguous(in []driverkit.Rule) []driverkit.Rule {
	out := make([]driverkit.Rule, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Ambiguous = true
	}
	return out
}

// lost copies a rule set as a lost lease, for the settlement path.
//
// A settlement whose connection died did not reach the broker, so the broker
// still holds the delivery and will hand it to the next subscription: that is the lost lease rather than an unknown outcome, and the difference matters
// because an unknown outcome is permanent and a lost lease is not.
func lost(in []driverkit.Rule) []driverkit.Rule {
	out := make([]driverkit.Rule, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Code = mq.CodeLeaseLost
		out[i].Retryable = false
		out[i].Ambiguous = false
	}
	return out
}

// isConnGone reports whether err is the connection's death.
func isConnGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gostomp.ErrClosedUnexpectedly) || errors.Is(err, gostomp.ErrAlreadyClosed) {
		return true
	}
	s := err.Error()
	// Both strings, and they are NOT one string: "connection closed" is
	// go-stomp's own synthetic ERROR frame -- the one `processLoop` answers a
	// pending receipt with when its reader's channel closes -- and
	// "connection already closed" is `ErrAlreadyClosed`'s text. Neither
	// contains the other.
	return strings.Contains(s, "connection closed") ||
		strings.Contains(s, "connection already closed") ||
		strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "connection reset by peer") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "read timeout") ||
		strings.Contains(s, "EOF")
}

// isReceiptTimeout reports whether err is a receipt that never arrived.
func isReceiptTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errReceiptTimeout) {
		return true
	}
	return strings.Contains(err.Error(), "receipt timeout")
}

// PartialBatchError is a PublishBatch that stopped part-way.
//
// The type stays for the callers that name it.
//
// Deprecated: PublishBatch no longer returns it. It answers with an
// index-aligned *mq.BatchError, which says the same thing message by message
// and which the core can read.
type PartialBatchError struct {
	// Published is how many messages reached the broker.
	Published int
	// Err is why it stopped.
	Err error
}

// Error implements error.
func (e *PartialBatchError) Error() string {
	return "iqmq/activemq/stomp: " + strconv.Itoa(e.Published) +
		" of the batch's messages were published before it failed: " + e.Err.Error()
}

// Unwrap reaches both the partial sentinel and the underlying cause, so
// that errors.Is finds CodePartial and errors.As finds the broker's own
// answer.
func (e *PartialBatchError) Unwrap() []error { return []error{mq.ErrPartial, e.Err} }

// ErrorFrameError is a broker's ERROR frame, kept so that a caller can read
// the broker's own sentence with errors.As.
//
// It exists because the sentence is the only diagnosis STOMP offers: there is
// no code and no structured field, so a driver that dropped the text would
// leave an operator with "Unavailable" and nothing else.
type ErrorFrameError struct {
	// Message is the ERROR frame's `message` header.
	Message string
	// Body is the frame's body, which some brokers use for a stack trace.
	Body string
}

// Error implements error.
func (e *ErrorFrameError) Error() string {
	if e.Body == "" {
		return "iqmq/activemq/stomp: the broker sent an ERROR frame: " + e.Message
	}
	return "iqmq/activemq/stomp: the broker sent an ERROR frame: " + e.Message + ": " + e.Body
}

// The driver's own refusals.
var (
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: the capability table leaves a feature unanswered")
	errReceiptTimeout = errcode.New(errcode.SettlementUnknown,
		"iqmq/activemq/stomp: the broker's RECEIPT frame never arrived")
	errNoDelayedRedelivery = errcode.New(errcode.Unsupported,
		"iqmq/activemq/stomp: neither broker reads a delay on a negative settlement, and this driver's requeue is a subscription reset that the broker answers at once; Nack with no delay, or use drivers/activemq")
	errNoDeadLetterRoute = errcode.New(errcode.Unsupported,
		"iqmq/activemq/stomp: measured, a NACK frame on Artemis acknowledges the delivery rather than dead-lettering it; set Config.DeadLetterTopic for a route this driver publishes to")
	errNoExtend = errcode.New(errcode.Unsupported,
		"iqmq/activemq/stomp: STOMP grants no lease, so there is no deadline to extend: a delivery is this subscription's until the subscription goes away")
	errNoTopology = errcode.New(errcode.Unsupported,
		"iqmq/activemq/stomp: STOMP has no topology frame; use drivers/activemq's Admin against Artemis, or the broker's own console")
	errNilConn = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq/stomp: OpenWithClient was given no connection")
	errClosed = errcode.New(errcode.Closed, "iqmq/activemq/stomp: closed")
	// errNotConnected is the window between a connection's death and its
	// replacement. It is RETRYABLE and it is not ambiguous, and both halves
	// of that are provable: the dial loop is running, so a later attempt can
	// succeed, and no frame was written, so the broker cannot have the
	// message. It is the one transport-shaped failure on the publish path
	// that does NOT make ambiguous.
	errNotConnected = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: the connection is not established")
	errNoSubscription = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq/stomp: a source needs a subscription: a STOMP destination with no subscription is a queue every consumer competes for, which is not the fan-out")
	errTokenTableFull = errcode.New(errcode.ResourceExhausted,
		"iqmq/activemq/stomp: every settlement slot is in use; the prefetch bounds how many deliveries may be unsettled at once")
	errNotSettleable = errcode.New(errcode.NotSettleable,
		"iqmq/activemq/stomp: the delivery does not belong to this source")
)

// The sink's own refusals, which are both refused at OPEN rather than at the
// first publication: the specification validates before it starts work, and a STOMP broker
// answers a frame it refuses with an ERROR frame and then closes the
// connection, so one bad publication costs every other publisher on that
// connection its link.
var (
	errNoDurableConfirm = errcode.New(errcode.Unsupported,
		"iqmq/activemq/stomp: ConfirmDurable needs a durable boundary this configuration does not have; it is the RECEIPT for a persistent publication, and this client is configured NoReceipts")
	errNeedsReceipts = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: a confirmed publication needs a STOMP receipt, which is the only answer a SEND frame has, and this client is configured NoReceipts; publish with ConfirmNone or turn receipts on")
)

// errNoBackendForClient is a conformance harness asked to quiesce a client it
// did not open. It is a test-harness defect rather than a driver failure, and
// it has a sentinel so that the failure names the cause rather than showing up
// as a probe that read one delivery of eight.
var errNoBackendForClient = errcode.New(errcode.InvalidConfig,
	"iqmq/activemq/stomp: the conformance harness has no backend for that client")
