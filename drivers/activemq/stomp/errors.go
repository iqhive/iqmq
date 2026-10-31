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
// Measured against both pinned images: Artemis closes the socket after the
// ERROR frame it answers a refused SEND with, and Classic keeps it open after
// refusing a SEND it could read (its codec's refusal it closes) -- but
// go-stomp closes the connection itself on EVERY ERROR frame, whichever broker
// sent it, so to this driver the frame is fatal either way. So a rule here
// does not only classify a failure, it also decides whether the connection is
// gone -- and every rule below that names a broker refusal is marked retryable
// only when a retry could succeed on a NEW connection.
//
// # A refusal is a publication's only when it names that publication
//
// go-stomp hands an ERROR frame to every receipt it is waiting on, whatever
// frame the broker was refusing, so a refusal of a receipt-less SEND read as a
// refusal of the receipted one waiting behind it. Measured: a ConfirmNone
// publication the broker refused, and a ConfirmAccepted one right after it,
// reported the second as a definite failure naming the FIRST one's
// destination -- while Classic had stored it and Artemis had not read it. The
// sentence rules below are therefore reached only by an ERROR frame the
// publication can claim ([answers]); any other one is [strayErrorFrame],
// which `stray` claims first.
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
		// A refused login, which both brokers answer with a sentence and no
		// code (measured on the pinned images, with a wrong passcode and with
		// no login at all): Classic says "User name [iqmq] or password is
		// invalid." and Artemis "Security Error occurred: User name [artemis]
		// or password is invalid", with "[null]" for a CONNECT with no login.
		// Artemis's AMQ229031 goes to its own log and never reaches a STOMP
		// client, because its CONNECT handler catches the security exception
		// and answers with that sentence instead. Classic's AuthorizationBroker
		// adds "User is not authenticated." for a connection with no security
		// context, which refuses the CONNECT itself (through the producer
		// Classic's STOMP converter registers for it) on a broker with
		// authorisation and no authentication, from the source. The
		// rule matched "User name [null] is not authenticated", which no
		// pinned broker says, so a refused login mapped to CodeUnknown on both
		// and OpenBackend's dial loop retried it until its context ended
		// ([Backend.refusedForGood]).
		{Match: driverkit.MatchContains("or password is invalid", "User is not authenticated"), Code: mq.CodeUnauthorized},
		// Classic's refusal is its AuthorizationBroker's sentence, "User
		// reader is not authorized to write to: queue://denied" (measured on
		// the pinned image), and the same "is not authorized to" for read
		// from, create and remove. It is lower case mid-sentence, and the rule
		// matched only "Not authorized", so every Classic authorisation
		// refusal mapped to CodeUnknown. Both spellings are matched.
		{Match: driverkit.MatchContains("Not authorized", "not authorized"), Code: mq.CodeForbidden},
	}
	// A refusal the broker made about the frame itself, which a retry cannot
	// fix.
	shape := []driverkit.Rule{
		{Match: driverkit.MatchContains("Invalid selector", "Invalid subscription id", "invalid selector"), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchContains("Poison ack cannot be transacted"), Code: mq.CodeUnsupported},
		// Classic's codec refusing a frame over one of its size limits
		// (`StompWireFormat` and `StompCodec` in activemq-stomp 6.2.0). "The
		// maximum frame size was exceeded" is the one measured, for a frame
		// over the connector's `wireFormat.maxFrameSize`. The others are for
		// `wireFormat.maxDataLength`, a header line over 10 KiB and more than
		// 1,000 headers, and the specification calls too many or too large
		// headers a message too large as well. The rule matched only "Message
		// exceeds" and "exceeds the maximum", which none of these contain, so
		// the measured refusal mapped to CodeUnknown.
		//
		// "The maximum command length was exceeded" is left out on purpose.
		// go-stomp writes no command that long, and Classic says it about
		// bytes it could not read as the start of a frame (measured: the
		// oversized body, read again as a command after the frame-size
		// refusal), which says nothing about this message's size. Artemis
		// 2.44.0 adds no sentence: a body over its `stompMinLargeMessageSize`
		// becomes a large message, and one whose headers are that large too is
		// refused as "AMQ339031 Error handling send", which names no cause.
		// Whether a codec refusal is this publication's at all is [answers]'s
		// question, not this rule's.
		{Match: driverkit.MatchContains(
			"Message exceeds", "exceeds the maximum",
			"The maximum frame size was exceeded",
			"The maximum data length was exceeded",
			"The maximum header length was exceeded",
			"The maximum number of headers was exceeded",
		), Code: mq.CodeMessageTooLarge},
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
	// them to `gone` -- `lost(gone)` on a settlement -- without loosening
	// anything else. On publish `ErrAlreadyClosed` is the one exception, and
	// `unwritten` below says why: the code was the defect, and the ambiguity
	// that came with moving it was a second one.
	library := []driverkit.Rule{
		{Match: driverkit.MatchIs(gostomp.ErrInvalidCommand, gostomp.ErrInvalidFrameFormat), Code: mq.CodeCorruptMessage},
		{Match: driverkit.MatchIs(gostomp.ErrNackNotSupported), Code: mq.CodeUnsupported},
		// A send timeout is the one transport failure that is NOT ambiguous:
		// go-stomp's write channel either took the frame or it did not, and
		// this error is the case where it did not, so the frame never
		// reached the writer and the broker cannot have the message.
		{Match: driverkit.MatchIs(gostomp.ErrMsgSendTimeout), Code: mq.CodeUnavailable, Retryable: true},
		// A frame that gave up waiting for room in go-stomp's write queue
		// ([Backend.room]). No frame was queued, so it is definite on every
		// path, the publish path included, and retryable: the queue drains
		// once the broker reads again. MatchIs claims every
		// Backpressure-coded errcode sentinel, and these two are the only
		// ones that reach the table.
		{Match: driverkit.MatchIs(errWriteStalled, errWriteQueueFull), Code: mq.CodeBackpressure, Retryable: true},
		// A source or a client that will not dial again, by configuration.
		// It is ahead of `gone` because a budget's refusal carries the last
		// dial's failure and that text can read like a dead connection
		// ("connection reset by peer"), which `gone` calls retryable -- and a
		// caller would retry a handle that has stopped for good. It is
		// matched on the sentinels' SENTENCES and ahead of errNotConnected
		// because `errors.Is` is no help with either: an errcode sentinel is
		// Is every error of its code, so MatchIs would make errNotConnected
		// terminal and errNotConnected's own rule would make these retryable.
		{
			Match: driverkit.MatchContains(
				errSourceNoReconnect.Error(), errSourceRedialsSpent.Error(),
				errNoReconnect.Error(), errRedialsSpent.Error(), errBorrowedLost.Error(), errNoAddrsLost.Error(),
			),
			Code: mq.CodeUnavailable,
		},
		{Match: driverkit.MatchIs(errNotConnected), Code: mq.CodeUnavailable, Retryable: true},
	}
	// The library closed a connection that had nothing written to it: `Send`
	// answers `ErrAlreadyClosed` from its first check, before the frame is
	// built, so the broker cannot have the message. It is the connection's
	// death all the same -- `isConnGone` still matches it and the publish
	// path still reports it -- and it is retryable on the connection the dial
	// loop makes next. Only the ambiguity `ambiguous(gone)` would add is
	// wrong: a publication reported "may have been taken" that provably was
	// not is a duplicate the caller is told to fear for nothing, and is
	// counted in a window nothing was in.
	unwritten := []driverkit.Rule{
		{Match: driverkit.MatchIs(gostomp.ErrAlreadyClosed), Code: mq.CodeUnavailable, Retryable: true},
	}
	// A subscription this driver retired whose UNSUBSCRIBE was never
	// answered. go-stomp pushes the timeout into the subscription's channel
	// as a terminal message, the pump that reads it is a superseded one and
	// records it, and the fetch after the requeue reads it -- by which time
	// the requeue has dropped that connection and subscribed on a new one
	// ([Source.retire]). It is NOT in `gone`, because `retire` reads
	// `isConnGone` to decide whether the connection still needs dropping, and
	// a receipt that never came is exactly the case where it does.
	retired := []driverkit.Rule{
		{Match: isUnsubscribeTimeout, Code: mq.CodeUnavailable, Retryable: true},
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
	// An ERROR frame that closed the connection while a publication waited
	// for its RECEIPT, and that is not provably that publication's refusal.
	// The frame left the socket and nothing answered it, so it is the
	// connection's death on the publish path and nothing more: ambiguous,
	// and retryable on the next connection. It is FIRST, ahead of `answered`,
	// because the error carries the broker's sentence and a sentence rule
	// would otherwise claim it as the refusal it is not. OpNack has it too,
	// because a dead-letter copy's publication error is mapped again there.
	stray := []driverkit.Rule{
		{Match: isStrayErrorFrame, Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true},
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
			mq.OpPublish: concat(stray, answered, receipt, unwritten, ambiguous(gone)),
			mq.OpFetch:   concat(answered, gone, retired),
			// An ACK or a NACK is one frame with no answer unless receipts
			// are on. Under receipts a failure is an UNKNOWN settlement,
			// which the specification makes permanent; without them the frame either
			// went or the connection is gone, and a connection that is gone
			// means the broker will redeliver -- a lost lease.
			mq.OpAck:    concat(answered, receipt, lost(gone)),
			mq.OpNack:   concat(stray, answered, receipt, lost(gone)),
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
	if errors.Is(err, errSocketClosed) {
		return true
	}
	s := err.Error()
	// Both strings, and they are NOT one string: "connection closed" is
	// go-stomp's own synthetic ERROR frame -- the one `processLoop` answers a
	// pending receipt with when its reader's channel closes -- and
	// "connection already closed" is `ErrAlreadyClosed`'s text. Neither
	// contains the other.
	//
	// "write channel closed" is the third synthetic frame and the one a
	// source meets most: `MustDisconnect` closes the library's write channel,
	// and `processLoop` answers every registered channel with that sentence
	// when it reads the closure. A source's death notice disconnects inline
	// on the reader goroutine, BEFORE the reader closes its own channel, so
	// on a socket that died under a subscription this is the sentence the
	// pump reads -- every time. Left out, the fetch after the death reported
	// it as CodeUnknown and not retryable, and a consumer loop gave up on a
	// subscription the next fetch would have re-established.
	return strings.Contains(s, "connection closed") ||
		strings.Contains(s, "write channel closed") ||
		strings.Contains(s, "connection already closed") ||
		strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "connection reset by peer") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "read timeout") ||
		strings.Contains(s, "EOF")
}

// isUnsubscribeTimeout reports whether err is go-stomp's terminal message for a
// subscription whose UNSUBSCRIBE was never answered.
//
// It is a sentence and not a sentinel because the library builds it per
// subscription -- "Subscription <id>: <destination>: channel unsubscribe
// receipt timeout" -- and hands it over as a message on the channel, not as
// `ErrUnsubscribeReceiptTimeout`.
func isUnsubscribeTimeout(err error) bool {
	return err != nil && strings.Contains(err.Error(), "unsubscribe receipt timeout")
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
// leave an operator with "Unavailable" and nothing else. That is exactly the
// answer a publication gets when an ERROR frame that does not name it closes
// its connection, so such an error carries one.
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

// strayErrorFrame is an ERROR frame that reached a publication's receipt wait
// without provably being that publication's answer ([answers]).
//
// It is the publication's connection dying and nothing else. The frame may
// refuse a receipt-less SEND written before it, a frame whose own receipt
// already timed out, or the connection itself, and the publication's frame
// may have been stored (Classic, which processes it after the refusal) or
// never read (Artemis, which answers it "AMQ339010 Connection is not valid").
// So it is ambiguous, and the broker's sentence is kept beside it -- reachable
// with errors.As as an [*ErrorFrameError] -- for the operator who has to find
// which frame it did refuse.
type strayErrorFrame struct {
	frame ErrorFrameError
}

// Error implements error.
func (e *strayErrorFrame) Error() string {
	return "iqmq/activemq/stomp: an ERROR frame that does not name this publication's receipt closed the connection, " +
		"so the broker may or may not have the message: " + e.frame.Message
}

// Unwrap reaches the broker's own frame.
func (e *strayErrorFrame) Unwrap() error { return &e.frame }

// isStrayErrorFrame reports whether err is a [strayErrorFrame]. It is a type
// test and not a sentinel because an errcode sentinel is Is every error of its
// code, so a sentinel would claim errNotConnected's definite answer as well.
func isStrayErrorFrame(err error) bool {
	var s *strayErrorFrame
	return errors.As(err, &s)
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
	// that does NOT make ambiguous. Once the dial loop has stopped for good
	// the first half is false, and [Backend.connection] answers with one of
	// the four refusals below instead.
	errNotConnected = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: the connection is not established")
	// errDialTimedOut is a dial Config.DialTimeout ended while the caller's
	// own context still had time. It is the node failing to answer, not the
	// caller's deadline, so it is a retryable Unavailable like
	// errNotConnected, whose rule matches it by code ([Backend.dialOne]).
	errDialTimedOut = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: the broker did not answer the dial within Config.DialTimeout")
	// errNoReconnect, errBorrowedLost, errNoAddrsLost and errRedialsSpent are
	// the client's own connection gone for good: nothing will dial another,
	// so each is Unavailable and NOT retryable, like the source's two below.
	// Each names its cause, because "Unavailable" alone leaves an operator
	// guessing which setting stopped the client.
	errNoReconnect = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: this client's connection was lost and Config.NoReconnect makes that terminal, so it does not dial another")
	errBorrowedLost = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: the connection the caller lent this client was lost, and a borrowed connection is never replaced")
	errNoAddrsLost = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: the connection the caller gave this client was lost, and Config.Addrs is empty, so there is no address to dial a replacement at")
	errRedialsSpent = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: this client gave up re-dialling after Config.ReconnectAttempts consecutive failed attempts")
	errNoSubscription = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq/stomp: a source needs a subscription: a STOMP destination with no subscription is a queue every consumer competes for, which is not the fan-out")
	errTokenTableFull = errcode.New(errcode.ResourceExhausted,
		"iqmq/activemq/stomp: every settlement slot is in use; the prefetch bounds how many deliveries may be unsettled at once")
	errNotSettleable = errcode.New(errcode.NotSettleable,
		"iqmq/activemq/stomp: the delivery does not belong to this source")
	// errSourceNoReconnect and errSourceRedialsSpent are a source that will
	// not dial again because the configuration said so. Both are
	// Unavailable and NOT retryable: retrying cannot help a client that was
	// told to stop, which is the backend's own Degraded-and-stays answer.
	errSourceNoReconnect = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: this source's connection was lost and Config.NoReconnect makes that terminal, so it does not dial another")
	errSourceRedialsSpent = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: this source gave up re-dialling after Config.ReconnectAttempts consecutive failed attempts")
	// errForeignQueue is a subscription whose queue is bound to another
	// address than the one it names ([Source.foreign]). It is a Conflict and
	// not retryable: Artemis attaches by the queue's name alone, so every
	// retry attaches to the same queue.
	errForeignQueue = errcode.New(errcode.Conflict,
		"iqmq/activemq/stomp: the subscription's queue is bound to another address; Artemis attaches a STOMP consumer to a queue by its name alone, and an Artemis queue name is unique across the whole broker, so a queue of this name on another address belongs to another topic or application; nothing from it was handed out or acknowledged")
	// errSourceNeedsAddrs is a source on a client opened over a connection
	// the caller gave and no address to dial one of its own.
	errSourceNeedsAddrs = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: a source dials a connection of its own, because one STOMP connection's single reader would couple a full consumer buffer to every publisher's receipt; this client was opened over a connection the caller gave and Config.Addrs is empty, so there is nothing to dial; set Config.Addrs to the broker that connection reaches")
	// errSocketClosed is the death notice for a socket that was CLOSED rather
	// than one whose read or write failed: go-stomp closes the connection
	// itself when a heart-beat it was owed does not arrive and when the
	// broker sends an ERROR frame (watch.go).
	errSocketClosed = errcode.New(errcode.Unavailable,
		"iqmq/activemq/stomp: the STOMP library closed the connection")
	// errWriteStalled and errWriteQueueFull are a receipt-less SEND, an ACK
	// or a NACK refused before it was queued ([Backend.room]). The first is
	// a socket that took no bytes for Config.ReceiptTimeout, which is a
	// broker that has stopped reading; the second is a queue that stayed full
	// for Config.ReceiptTimeout of this frame's own wait, which says only
	// that, because go-stomp's process loop can hold the queue up with the
	// broker reading.
	errWriteStalled = errcode.New(errcode.Backpressure,
		"iqmq/activemq/stomp: the frame was not queued: the STOMP library's write queue is full and the broker has taken no bytes off this connection for Config.ReceiptTimeout")
	errWriteQueueFull = errcode.New(errcode.Backpressure,
		"iqmq/activemq/stomp: the frame was not queued: the STOMP library's write queue stayed full for Config.ReceiptTimeout")
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
