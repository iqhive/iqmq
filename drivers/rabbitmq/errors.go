package rabbitmq

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// newErrorMapper builds this driver rule table.
//
// Every rule matches on what the CLIENT LIBRARY produces and not on what the
// server sends, which is the lesson recorded in the maintenance notes: a
// driver's error table is usually written against the text a broker sends, and
// what reaches the mapper is whatever the library decoded it into. On this
// library that means two things.
//
// First, every protocol failure arrives as an [amqp.Error] — a struct with a
// numeric `Code` from the AMQP 0-9-1 specification, a `Reason` string, and two
// flags — so the rules match by `errors.As` and then by code. They deliberately
// do NOT match `amqp.ErrClosed` and its nine siblings with `errors.Is`: those
// are package-level `*amqp.Error` VALUES with no `Is` method, so `errors.Is`
// compares pointers and a server-sent 504 would not match the library's own
// `ErrClosed` even though both mean the same thing. Matching on the code
// catches both.
//
// Second, the same code means different things in different places, so the
// table is per operation. A 404 on a publish is a missing exchange, which is a
// topology error the caller has to fix; a 404 on a settlement is a queue that
// has been deleted under a consumer, which means the lease is gone. The specification
// forbids inferring retryability from an error type alone and this is why.
func newErrorMapper() *driverkit.ErrorMapper {
	// The rules that are the same wherever they appear. They are built once
	// and shared by every operation's list, so a code cannot be classified
	// two ways by accident.
	shared := []driverkit.Rule{
		// The library's OWN package-level error values come first, and the
		// order is the finding rather than a preference. Every one of them is
		// an `*amqp091.Error` carrying an AMQP reply code, and the codes
		// COLLIDE with the server-sent ones: ErrSASL, ErrCredentials and
		// ErrVhost are all 403, ErrChannelMax is 504 and ErrFieldType is
		// 502. A table that matched by code first therefore reported five of
		// the library's ten sentinels as something else -- "username or
		// password not allowed" as Forbidden rather than Unauthorized, and
		// "unsupported table field type" as a corrupt conversation rather
		// than a malformed argument. They are matched by IDENTITY, before the
		// codes, because only the value distinguishes them.
		{Match: driverkit.MatchIs(amqp.ErrSASL, amqp.ErrCredentials, amqp.ErrVhost), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchIs(amqp.ErrChannelMax), Code: mq.CodeResourceExhausted, Retryable: true},
		{Match: driverkit.MatchIs(amqp.ErrFieldType), Code: mq.CodeInvalidArgument},
		// 403 ACCESS_REFUSED covers the library's own ErrSASL,
		// ErrCredentials and ErrVhost as well as a server refusal, and the
		// distinction draws is whether the credentials were rejected
		// or the operation was: a client with no access to a vhost is
		// Unauthorized, and one refused a particular exchange is Forbidden.
		// The library gives no way to tell them apart beyond the reason, so
		// the driver reports Unauthorized for the three authentication
		// values and Forbidden for the rest.
		{Match: matchCode(amqp.AccessRefused), Code: mq.CodeForbidden},
		{Match: matchCode(amqp.ResourceLocked), Code: mq.CodeConflict, Retryable: true},
		{Match: matchCode(amqp.PreconditionFailed), Code: mq.CodeConflict},
		{Match: matchCode(amqp.ResourceError), Code: mq.CodeResourceExhausted, Retryable: true},
		{Match: matchCode(amqp.ContentTooLarge), Code: mq.CodeMessageTooLarge},
		{Match: matchCode(amqp.NotAllowed), Code: mq.CodeForbidden},
		{Match: matchCode(amqp.NotImplemented), Code: mq.CodeUnsupported},
		{Match: matchCode(amqp.InvalidPath), Code: mq.CodeInvalidConfig},
		// 320 CONNECTION_FORCED is the broker shutting the connection down:
		// a restart, or an operator closing it. It is retryable because the
		// driver's own dial loop is what answers it.
		{Match: matchCode(amqp.ConnectionForced), Code: mq.CodeUnavailable, Retryable: true},
		// 504 CHANNEL_ERROR is the library's own ErrClosed as well as a
		// server-sent channel error, and both mean the same thing to a
		// caller: this handle is gone.
		{Match: matchCode(amqp.ChannelError), Code: mq.CodeClosed, Retryable: true},
		// The three protocol-level failures. They are bugs rather than
		// conditions, so they are not retryable and they keep CodeUnknown's
		// honesty: something is wrong with the conversation, not with the
		// request.
		{Match: matchCode(amqp.FrameError, amqp.SyntaxError, amqp.UnexpectedFrame), Code: mq.CodeCorruptMessage},
		{Match: matchCode(amqp.CommandInvalid), Code: mq.CodeUnsupported},
		{Match: matchCode(amqp.InternalError), Code: mq.CodeUnknown, Retryable: true},
		// This driver's own refusals, which reach the mapper when a
		// publication is refused before any frame is written.
		{Match: matchOurCode(mq.CodeUnsupported), Code: mq.CodeUnsupported},
		{Match: matchOurCode(mq.CodeInvalidArgument), Code: mq.CodeInvalidArgument},
		{Match: matchOurCode(mq.CodeInvalidConfig), Code: mq.CodeInvalidConfig},
		{Match: matchOurCode(mq.CodeMessageTooLarge), Code: mq.CodeMessageTooLarge},
		{Match: matchOurCode(mq.CodeLeaseLost), Code: mq.CodeLeaseLost},
		{Match: matchOurCode(mq.CodeClosed), Code: mq.CodeClosed},
	}

	publish := []driverkit.Rule{
		// 312 NO_ROUTE is what a `mandatory` publication that no queue is
		// bound for comes back as, and it is Rejected: the broker
		// took the message and threw it away, so retrying will throw it
		// away again. It almost always means the topology is wrong.
		{Match: matchCode(amqp.NoRoute), Code: mq.CodeNotFound},
		// 313 NO_CONSUMERS is the `immediate` flag's refusal. RabbitMQ
		// removed support for `immediate` in 3.0 and this driver never sets
		// it, so reaching this rule means a broker sent a reply this driver
		// cannot have asked for; it is mapped rather than left unknown so
		// that the code a caller sees is not CodeUnknown.
		{Match: matchCode(amqp.NoConsumers), Code: mq.CodeNotFound},
		// 404 on a publish is a missing exchange.
		{Match: matchCode(amqp.NotFound), Code: mq.CodeNotFound},
	}
	settle := []driverkit.Rule{
		// 404 on a settlement is a queue that has gone under the consumer,
		// so the lease is gone with it and a retry cannot help.
		{Match: matchCode(amqp.NotFound), Code: mq.CodeLeaseLost},
		// 406 PRECONDITION_FAILED on a settlement is an unknown delivery
		// tag: the broker refuses an ack for a tag it did not issue, and
		// that is what a settlement carrying a tag from a closed channel
		// looks like from the server's side. ErrLeaseLost rather than
		// Conflict, because the delivery is gone and the caller can do
		// nothing about it.
		{Match: matchCode(amqp.PreconditionFailed), Code: mq.CodeLeaseLost},
	}
	consume := []driverkit.Rule{
		// 404 on a consume is a missing queue, which under the no-hidden-
		// creation rule is the expected answer for a subscription nobody has
		// declared.
		{Match: matchCode(amqp.NotFound), Code: mq.CodeNotFound},
		{Match: matchCode(amqp.ResourceLocked), Code: mq.CodeConflict},
	}
	open := []driverkit.Rule{
		{Match: matchCode(amqp.NotFound), Code: mq.CodeNotFound},
	}

	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(publish, shared),
			mq.OpAck:     concat(settle, shared),
			mq.OpNack:    concat(settle, shared),
			mq.OpFetch:   concat(consume, shared),
			mq.OpOpen:    concat(open, shared),
			mq.OpAdmin:   concat(open, shared),
		},
		Fallback: shared,
	}
}

// concat joins two rule lists, the specific before the shared, which is the
// order [driverkit.ErrorMapper] resolves them in.
func concat(specific, shared []driverkit.Rule) []driverkit.Rule {
	out := make([]driverkit.Rule, 0, len(specific)+len(shared))
	out = append(out, specific...)
	return append(out, shared...)
}

// matchCode returns a predicate that matches an [amqp.Error] with any of the
// given AMQP reply codes.
//
// It unwraps with errors.As rather than comparing values, which is the whole
// point: the library's own package-level errors are `*amqp.Error` values with
// no `Is` method, so `errors.Is(err, amqp.ErrClosed)` is a pointer comparison
// that a server-sent 504 fails. Both reach this predicate.
func matchCode(codes ...int) func(error) bool {
	return func(err error) bool {
		var aerr *amqp.Error
		if !errors.As(err, &aerr) {
			return false
		}
		for _, code := range codes {
			if aerr.Code == code {
				return true
			}
		}
		return false
	}
}

// matchOurCode matches this driver's own refusals, which carry a portable code
// already.
//
// They reach the mapper because a refusal made before any frame is written —
// an unrepresentable header, a delay longer than the strategy can express — is
// still returned through the same path as a broker's answer, and a driver that
// let it fall through to the transport classification would turn
// CodeUnsupported into CodeUnknown.
func matchOurCode(code mq.ErrorCode) func(error) bool {
	return func(err error) bool { return mq.CodeOf(err) == code }
}

// ReplyCode is the AMQP reply code an error carries, and whether it carries
// one at all.
//
// It is exported because the code is the only thing that distinguishes several
// failures a caller may want to act on differently — a 406 on a re-declaration
// with different arguments is a topology conflict an operator has to resolve,
// and a 406 on a settlement is a delivery tag the broker does not know — and the portable codes deliberately do not carry that much detail. A caller
// that needs it reads it here rather than parsing the error's text.
func ReplyCode(err error) (int, bool) {
	var aerr *amqp.Error
	if errors.As(err, &aerr) {
		return aerr.Code, true
	}
	return 0, false
}

// IsChannelException reports whether the error closed the channel it happened
// on.
//
// It matters more here than on most brokers. In AMQP 0-9-1 a channel exception
// takes the CHANNEL down, not just the operation: everything else in flight on
// it fails too, and on a publisher-confirm channel every publication whose
// confirm had not arrived becomes ambiguous. So the driver refuses as much as
// it can before writing a frame, and when the broker raises one anyway this is
// what tells the reconnection path to rebuild the channel rather than retry
// the call.
func IsChannelException(err error) bool {
	code, ok := ReplyCode(err)
	if !ok {
		return false
	}
	switch code {
	case amqp.ContentTooLarge, amqp.NoRoute, amqp.NoConsumers,
		amqp.AccessRefused, amqp.NotFound, amqp.ResourceLocked,
		amqp.PreconditionFailed:
		return true
	default:
		// The 5xx codes are CONNECTION exceptions: they take the whole
		// connection, not one channel, which is a different repair.
		return false
	}
}

// The driver's own refusals, made before any frame reaches the socket.
var (
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the source was opened with no prefetch; the core resolves the API's default before opening a source")
	errNoWildcards = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: a wildcard topic is not a subscribable name on this driver: a topic maps to its own durable exchange, so a pattern over topic names would have to span exchanges and a binding cannot. A topic exchange's wildcards are available inside one topic through Config.KeyAsRoutingKey and a binding of your own")
	errNoDeadLetterRoute = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: this subscription has no dead-letter route; set Config.DeadLetter (which needs Config.Declare) or dead-letter through the core's own policy")
	errDelayNeedsDeclare = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: a delayed redelivery needs a delay queue or the delayed-message exchange, and the API forbids creating either behind the caller's back; set Config.Declare")
	errNoTopology = errcode.New(errcode.NotFound,
		"iqmq/rabbitmq: the exchange or queue does not exist and the API forbids creating it behind the caller's back; set Config.Declare or provision it out of band")
	errClosedChannel = errcode.New(errcode.Closed,
		"iqmq/rabbitmq: the channel closed; every publication it was holding was reported ambiguous")
	errScheduledPublish = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: a scheduled publication needs the rabbitmq_delayed_message_exchange plugin, which is not part of the official RabbitMQ image")
	errPriorityWithoutQueue = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: a message priority is ignored by a queue that was not declared with x-max-priority; set Config.MaxPriority")
)

// The refusals the driver's own paths produce, beyond the mapped ones above.
var (
	errReconnecting = &mq.OpError{
		Driver: DriverName, Operation: mq.OpOpen,
		Code: mq.CodeUnavailable, Retryable: true,
		Err: errcode.New(errcode.Unavailable,
			"iqmq/rabbitmq: the connection is being re-established; the driver's own dial loop is retrying with backoff"),
	}
	// errChannelOpenTimeout is a broker that accepted the connection and then
	// did not answer `channel.open` within Config.DialTimeout.
	//
	// Retryable, and it really is: the connection is up, so the next attempt
	// costs one round trip rather than a dial. What it replaces is an
	// unbounded park -- see [Backend.channelOnWithin] for why that park was
	// not confined to the one goroutine doing the waiting.
	errChannelOpenTimeout = &mq.OpError{
		Driver: DriverName, Operation: mq.OpOpen,
		Code: mq.CodeUnavailable, Retryable: true,
		Err: errcode.New(errcode.Unavailable,
			"iqmq/rabbitmq: the broker did not answer channel.open within Config.DialTimeout; the connection is up, so a retry costs one round trip"),
	}
	// errDetached is a publisher whose channel has gone and IS being replaced:
	// a repair in flight, or a redial. Retryable, because something really is
	// working on it.
	//
	// It is separate from errReconnecting because that one names the DIAL
	// LOOP, and a channel exception leaves the connection up: no dial loop is
	// running, so a publisher refused with "the driver's own dial loop is
	// retrying with backoff" was told to wait for something that was not
	// happening, and -- before -- to wait for ever.
	//
	// It deliberately does NOT cover the third state, a repair that FAILED:
	// the repair is one-shot, so nothing is re-attaching then and nothing will
	// until the connection is replaced. This text said it was, which is the
	// original lie narrowed from "always" to "after a failed repair" -- 1,831
	// refusals in two seconds with the broker healthy, reached on a real
	// broker by the driver page's own example (publish to a deleted exchange
	// -> 404 -> the repair's declaration fails). That state has its own
	// refusal below.
	errDetached = &mq.OpError{
		Driver: DriverName, Operation: mq.OpPublish,
		Code: mq.CodeUnavailable, Retryable: true,
		Err: errcode.New(errcode.Unavailable,
			"iqmq/rabbitmq: this publisher's channel closed and the driver is attaching another"),
	}
	// errRepairFailed is the state after the one-shot repair did not install a
	// channel: no dial loop, no repair, and no route that will make one --
	// `sink.attach` is called from the open, from the repair and from the dial
	// loop, and Publish is none of those. So the only honest advice is the one
	// the driver page used to give and gives again: close this publisher and
	// open a new one.
	//
	// NOT Retryable. Retryable means a retry may succeed, and a retry
	// here fails identically until the connection happens to be replaced --
	// which on a channel exception may be never. Marking it retryable is how
	// the original defect told a caller to spin for ever.
	errRepairFailed = errcode.New(errcode.Unavailable,
		"iqmq/rabbitmq: this publisher's channel closed and the driver's one re-attach attempt failed; nothing will attach another until the connection is replaced, so close this publisher and open a new one")
	errNoDurableBoundary = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: ConfirmDurable needs durable quorum queues; a classic non-durable queue returns a publisher confirm once the broker holds the message in memory, which is not a durable boundary (the message contract)")
	errNoDeduplication = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: RabbitMQ de-duplicates nothing, so a DedupKey would be accepted and ignored; use the core's own de-duplication middleware")
	errQueueGone = errcode.New(errcode.NotFound,
		"iqmq/rabbitmq: the broker cancelled this consumer, which happens when the queue it was reading is deleted")
	errParkAmbiguous = errcode.New(errcode.SettlementUnknown,
		"iqmq/rabbitmq: the delayed redelivery's parked copy may or may not have been stored, so the original was left unacknowledged and the broker will redeliver it")
	errParkRefused = errcode.New(errcode.Unavailable,
		"iqmq/rabbitmq: the broker refused the delayed redelivery's parked copy, so the original was left unacknowledged")
	errNilConnection = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: OpenWithClient was given a nil connection")
	errNilDialer = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: BackendWithDialer was given a nil dialler")
	// errHandleDetached is what [Backend.Health] reports instead of Ready when
	// some open handle has lost its channel and is being re-attached. The specification
	// forbids a payload, a credential or an address in an error, and this
	// names none: it says which of the client's parts is not working.
	// A handle whose repair FAILED reports its own
	// failure instead, which is the more useful answer and the one that tells
	// the operator the client will not mend itself.
	errHandleDetached = errcode.New(errcode.Unavailable,
		"iqmq/rabbitmq: the connection is up and at least one publisher or consumer has no channel; the driver is re-attaching it")
	// errNoChannel is a handle with no channel for an operation that is not a
	// publication: a settlement's delayed-nack park, and the reply endpoint.
	//
	// Those three sites answered with errReconnecting, whose text names the
	// dial loop -- and `source.claimLost` nils the source's tracker on a
	// CHANNEL exception, with the connection up and no dial loop running, so
	// they told the lie on the consuming side. Review round 1
	// found that the ledger's claim that errReconnecting had "one honest
	// caller" was four fifths untrue.
	errNoChannel = errcode.New(errcode.Unavailable,
		"iqmq/rabbitmq: this handle has no channel right now; it is restored by the handle's own repair or by the next reconnection")
	errAdminNeedsDeclare = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: this client may not create topology (Config.Declare is off), so Admin can inspect and delete and not create")
)

// errNoSeek is a start position a queue cannot express. It is separate from
// the other refusals because it is the one a conformance run reads: it is what
// makes the capability table's Unsupported for FeatureSeekByTime and
// FeatureSeekBySequence provable in the negative direction rather than taken
// on trust.
var errNoSeek = errcode.New(errcode.Unsupported,
	"iqmq/rabbitmq: a queue has no history and no position, so there is nothing to seek to; StartLatest and StartEarliest are the same place and a time or a sequence is refused")
