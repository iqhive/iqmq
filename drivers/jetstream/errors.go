package jetstream

import (
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"

	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// newErrorMapper builds this driver rules.
//
// Rules are per operation because retryability is, and JetStream makes the
// distinction sharper than the Core profile did: a publication is a request and
// a response, so a failure reported after the request left the socket may still
// have been stored — the PubAck is what says otherwise — while the same failure
// reported by the write is definite. That is what
// [github.com/iqhive/iqmq/driverkit.ErrorMapper.MapNet]'s sentAlready argument
// carries, and getting it backwards is how a duplicate comes to look like a
// loss.
//
// The settlement operations have rules of their own, which the Core profile did
// not need at all: a message whose reply subject is gone cannot be acked, and
// reporting that as an unknown failure would leave the caller believing a
// redelivery might still be settled.
func newErrorMapper() *driverkit.ErrorMapper {
	auth := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrAuthorization, natsgo.ErrAuthExpired, natsgo.ErrAuthRevoked, natsgo.ErrAccountAuthExpired), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchIs(natsgo.ErrPermissionViolation), Code: mq.CodeForbidden},
	}
	link := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrConnectionClosed, natsgo.ErrInvalidConnection, js.ErrConnectionClosed), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(natsgo.ErrConnectionDraining), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(natsgo.ErrNoServers, natsgo.ErrDisconnected, natsgo.ErrStaleConnection, natsgo.ErrConnectionReconnecting), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(js.ErrServerShutdown, js.ErrNoHeartbeat, js.ErrConsumerLeadershipChanged), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(natsgo.ErrSlowConsumer), Code: mq.CodeResourceExhausted, Retryable: true},
		{Match: driverkit.MatchIs(natsgo.ErrMaxConnectionsExceeded, natsgo.ErrMaxAccountConnectionsExceeded, natsgo.ErrMaxSubscriptionsExceeded), Code: mq.CodeResourceExhausted, Retryable: true},
		// A TLS handshake the link ended is the link's failure and not the
		// configuration's, so it comes before the rule for ErrTLS.
		{Match: tlsTimedOut, Code: mq.CodeDeadlineExceeded, Retryable: true},
		{Match: tlsCut, Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(natsgo.ErrSecureConnRequired, natsgo.ErrSecureConnWanted, natsgo.ErrTLS, natsgo.ErrConnectionNotTLS), Code: mq.CodeUnavailable},
		{Match: driverkit.MatchIs(natsgo.ErrTimeout), Code: mq.CodeDeadlineExceeded, Retryable: true},
	}
	// The topology answers. A stream or consumer that is not there is
	// NotFound and not Unavailable: retrying the same call for ever against a
	// stream nobody created is the failure mode this distinction prevents.
	topology := []driverkit.Rule{
		{Match: driverkit.MatchIs(js.ErrStreamNotFound, js.ErrConsumerNotFound, js.ErrConsumerDoesNotExist, js.ErrMsgNotFound), Code: mq.CodeNotFound},
		// "no responders" on a JetStream API subject or a publish means no
		// stream is bound to the subject: the message was NOT stored, which
		// is a definite NotFound rather than an ambiguous round trip.
		{Match: driverkit.MatchIs(js.ErrNoStreamResponse, natsgo.ErrNoResponders), Code: mq.CodeNotFound, Retryable: true},
		{Match: driverkit.MatchIs(js.ErrStreamNameAlreadyInUse, js.ErrConsumerExists, js.ErrConsumerNameAlreadyInUse), Code: mq.CodeConflict},
		{Match: driverkit.MatchIs(js.ErrJetStreamNotEnabled, js.ErrJetStreamNotEnabledForAccount), Code: mq.CodeUnsupported},
		{Match: driverkit.MatchIs(js.ErrMaximumConsumersLimit), Code: mq.CodeResourceExhausted},
		{Match: driverkit.MatchIs(js.ErrConsumerDeleted), Code: mq.CodeNotFound, Retryable: true},
	}
	shape := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrBadSubject, natsgo.ErrInvalidMsg, natsgo.ErrInvalidArg), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(js.ErrInvalidSubject, js.ErrInvalidStreamName, js.ErrInvalidConsumerName, js.ErrStreamNameRequired), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(js.ErrInvalidOption, js.ErrBadRequest, js.ErrInvalidJetStreamResponse, js.ErrInvalidJSAck), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(natsgo.ErrNoDeadlineContext, natsgo.ErrBadTimeout, natsgo.ErrInvalidContext), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(natsgo.ErrHeadersNotSupported), Code: mq.CodeUnsupported},
		{Match: driverkit.MatchIs(natsgo.ErrBadHeaderMsg), Code: mq.CodeCorruptMessage},
		{Match: driverkit.MatchIs(js.ErrNotJSMessage), Code: mq.CodeCorruptMessage},
	}
	publish := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrMaxPayload, js.ErrMaxBytesExceeded), Code: mq.CodeMessageTooLarge},
		// The client's asynchronous window is full. Nothing was written, so
		// it is a definite refusal and it is the one place the specification's
		// backpressure code belongs on this driver.
		{Match: driverkit.MatchIs(js.ErrTooManyStalledMsgs), Code: mq.CodeBackpressure, Retryable: true},
		{Match: driverkit.MatchIs(natsgo.ErrReconnectBufExceeded), Code: mq.CodeBackpressure, Retryable: true},
		// The publication was written and its PubAck never came: the stream
		// may hold it. The specification makes that ambiguous, and a retry may
		// duplicate — which is what Nats-Msg-Id exists for.
		{Match: driverkit.MatchIs(js.ErrAsyncPublishTimeout), Code: mq.CodeDeadlineExceeded, Retryable: true, Ambiguous: true},
		{Match: driverkit.MatchIs(js.ErrJetStreamPublisherClosed), Code: mq.CodeSettlementUnknown, Ambiguous: true},
		// A message schedule or a per-message TTL the stream does not allow.
		// It is a capability answer and not a transport failure, so it is
		// Unsupported and never retried.
		{Match: driverkit.MatchIs(js.ErrMessageSchedulesDisabled), Code: mq.CodeUnsupported},
		{Match: driverkit.MatchIs(js.ErrSchedulePatternInvalid, js.ErrScheduleTargetInvalid, js.ErrScheduleTTLInvalid, js.ErrScheduleSourceInvalid, js.ErrScheduleRollupInvalid), Code: mq.CodeInvalidArgument},
		{Match: matchAPICode(jsErrCodeMsgTTLDisabled), Code: mq.CodeUnsupported},
		{Match: matchAPICode(js.JSErrCodeStreamWrongLastSequence), Code: mq.CodeConflict},
	}
	// Settlement. A reply subject that is gone, or a message the client has
	// already answered, is a LOST LEASE and not an unknown outcome: the
	// distinction is what tells a caller whether the message may still come
	// back.
	settle := []driverkit.Rule{
		{Match: driverkit.MatchIs(js.ErrMsgNoReply, js.ErrMsgNotBound), Code: mq.CodeLeaseLost},
		{Match: driverkit.MatchIs(js.ErrMsgAlreadyAckd), Code: mq.CodeAlreadySettled},
	}
	m := &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(publish, topology, shape, link, auth),
			mq.OpFetch:   concat(topology, shape, link, auth),
			mq.OpAck:     concat(settle, shape, link, auth),
			mq.OpNack:    concat(settle, publish, topology, shape, link, auth),
			mq.OpExtend:  concat(settle, shape, link, auth),
			mq.OpOpen:    concat(topology, shape, link, auth),
			mq.OpClose:   concat(link, auth),
		},
		Fallback: concat(topology, link, auth, shape),
	}
	return m
}

// jsErrCodeMsgTTLDisabled is the API error the server answers a per-message TTL
// with when the stream was not created with AllowMsgTTL.
//
// The client library has no sentinel for it — the schedule refusals got one and
// this did not — so the code is named here rather than matched on a message.
const jsErrCodeMsgTTLDisabled js.ErrorCode = 10166

// matchAPICode returns a [driverkit.Rule.Match] that matches a JetStream API
// error code, which is the typed surface the client does expose for every server
// refusal.
func matchAPICode(code js.ErrorCode) func(error) bool {
	return func(err error) bool {
		var apiErr *js.APIError
		if errors.As(err, &apiErr) {
			return apiErr.ErrorCode == code
		}
		return false
	}
}

// refusal is the permissions violation the server answered a publication to
// a subject pattern matches with, when it is the connection's last error and
// arrived after before was read from the connection; nil otherwise.
//
// It is how a publication whose PubAck never came, or a JetStream API request
// whose reply never came, learns why when the driver did not hear the refusal
// itself ([Backend.refuseWaits]): the caller replaced the error handler of a
// borrowed connection, say. nats.go keeps one error, so a later asynchronous
// error hides the refusal. The refusal names the subject, not the message, so
// where permission depends on more than the subject (a reload) a publication
// can be read as refused because another one to the subject was.
func (b *Backend) refusal(before error, pattern string) error {
	now := b.nc.LastError()
	if !errors.Is(now, natsgo.ErrPermissionViolation) || now == before { //nolint:errorlint // identity is what says the refusal is new
		return nil
	}
	if !refuses(now, pattern) {
		return nil
	}
	return now
}

// refuses reports whether the permissions violation err is the server's
// refusal of a publication to a subject pattern matches, in the server's own
// words. pattern is the subject itself for a publication, and a NATS wildcard
// pattern for the JetStream API requests a call makes ([Backend.apiRequest]).
func refuses(err error, pattern string) bool {
	msg := err.Error()
	i := strings.LastIndex(msg, refusedPublication)
	if i < 0 {
		return false
	}
	subject, uerr := strconv.Unquote(msg[i+len(refusedPublication):])
	return uerr == nil && subjectMatches(pattern, subject)
}

// refusedPublication is what the server's refusal of a publication says
// before the quoted subject, which ends it.
const refusedPublication = "Publish to "

// tlsHandshake reports whether err is a TLS handshake that failed.
//
// nats.go wraps every handshake failure in ErrTLS, a certificate or a TLS
// alert the peer refused along with a reset, an end of file or a deadline,
// and the rule for ErrTLS read each one as a refusal a retry cannot get past.
// A connection the server closes once the handshake has completed is not
// counted: it is how a proxy refuses a client certificate, so it stays with
// the rule for ErrTLS. nats.go wraps it the same way and says so only in its
// message, so that is what tells the two apart.
func tlsHandshake(err error) bool {
	return errors.Is(err, natsgo.ErrTLS) && !strings.Contains(err.Error(), tlsClosedAfterHandshake)
}

// tlsClosedAfterHandshake is how nats.go v1.55.0 describes a connection the
// server closed right after a TLS handshake that completed.
const tlsClosedAfterHandshake = "after TLS handshake"

// tlsTimedOut reports whether err is a TLS handshake a deadline ended, which
// is Open's when it has one.
func tlsTimedOut(err error) bool {
	var nerr net.Error
	return tlsHandshake(err) && errors.As(err, &nerr) && nerr.Timeout()
}

// tlsCut reports whether err is a TLS handshake the peer cut, with a reset or
// an end of file, as a server restarting or a load balancer dropping the
// connection does.
func tlsCut(err error) bool {
	return tlsHandshake(err) && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed))
}

// concat joins rule sets in priority order, most specific first.
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

// isSlowConsumer reports whether err is the asynchronous error nats.go raises
// when a subscription's pending limits are exceeded and the client has started
// dropping messages.
//
// It matters less here than on the Core profile — a dropped delivery is unacked,
// so JetStream redelivers it — but it is still the signal that the consumer is
// configured wrongly, so it reaches health.
func isSlowConsumer(err error) bool {
	return err != nil &&
		(errors.Is(err, natsgo.ErrSlowConsumer) || strings.Contains(err.Error(), "slow consumer"))
}

// isGone reports whether an operation failed because there was nothing left to
// operate on, which is not a failure of the operation that reports it.
//
// The first four are the connection's: it was closed underneath us. The last
// three are the DELIVERY's, and they are here for a reason a closing source runs
// into. The consume callback and the closing sweep can both reach one message —
// the callback pushes it into the ring, the sweep naks it, the push then fails
// because the ring is closed and the callback naks it too — and the client
// library answers the second nak with ErrMsgAlreadyAckd. That is not a failure:
// somebody already settled the delivery, which is what the nak wanted.
// Reporting it would make a Close degrade the client's health and count a
// consume error.
func isGone(err error) bool {
	if err == nil {
		return true
	}
	return errorsIsAny(err,
		natsgo.ErrConnectionClosed,
		natsgo.ErrBadSubscription,
		natsgo.ErrInvalidConnection,
		natsgo.ErrConnectionDraining,
		js.ErrConnectionClosed,
		js.ErrMsgAlreadyAckd,
		js.ErrMsgNotBound,
		js.ErrMsgNoReply,
	)
}

// errorsIsAny reports whether err matches any of the targets.
func errorsIsAny(err error, targets ...error) bool {
	for _, t := range targets {
		if errors.Is(err, t) {
			return true
		}
	}
	return false
}

// The driver's own refusals, for the conditions no native error names.
var (
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/jetstream: the capability table does not mention every feature of the inventory")
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig,
		"iqmq/jetstream: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/jetstream: the metadata mode is not one of the four")
	errNoSchedule = errcode.New(errcode.Unsupported,
		"iqmq/jetstream: this stream cannot schedule a publication; message schedules need a 2.12+ server and a stream created with AllowMsgSchedules (see docs/drivers/jetstream.md)")
	errNoTTL = errcode.New(errcode.Unsupported,
		"iqmq/jetstream: this stream cannot give a message a time to live; Nats-TTL needs a 2.11+ server and a stream created with AllowMsgTTL (see docs/drivers/jetstream.md)")
	errNoPriority = errcode.New(errcode.Unsupported,
		"iqmq/jetstream: JetStream has no message priority")
	errNoPartition = errcode.New(errcode.Unsupported,
		"iqmq/jetstream: JetStream has no partitions to publish to; a subject is the only partition there is")
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/jetstream: no dead-letter destination is configured, so Reject has nowhere to route the message; set Config.DeadLetterTopic (the message contract)")
	errNoDelayAndDeliverAt = errcode.New(errcode.InvalidArgument,
		"iqmq/jetstream: Delay and DeliverAt name the same control twice")
	errScheduleInPast = errcode.New(errcode.InvalidArgument,
		"iqmq/jetstream: the scheduled time is already past")
	errPatternReachesSchedules = errcode.New(errcode.InvalidArgument,
		"iqmq/jetstream: the consumer's pattern also matches Config.ScheduleSubject, so it would receive the driver's own schedule messages; narrow the pattern or set Config.DisableSchedules")
	errPrefetchTooLarge = errcode.New(errcode.InvalidConfig,
		"iqmq/jetstream: the prefetch does not fit a token table")
	errDurableAckPolicy = errcode.New(errcode.InvalidConfig,
		"iqmq/jetstream: the durable this subscription maps to has an AckPolicy other than explicit, which the server cannot change; delete the durable or name another subscription (see docs/drivers/jetstream.md)")
	errStreamMsgSize = errcode.New(errcode.InvalidConfig,
		"iqmq/jetstream: maxmsgsize does not fit a 32-bit stream ceiling")
	errTableFull = errcode.New(errcode.ResourceExhausted,
		"iqmq/jetstream: the source has more unsettled deliveries than its token table can hold")
	errClosedMidWait = errcode.New(errcode.Closed,
		"iqmq/jetstream: the backend closed while waiting for the server")
	errDeadLetterUnknownAtClose = errcode.New(errcode.SettlementUnknown,
		"iqmq/jetstream: the backend closed while the dead-letter copy waited for its PubAck, so whether the dead-letter subject's stream holds it is unknown; the original was not terminated")
	errDeadLetterUnknownAtCut = errcode.New(errcode.SettlementUnknown,
		"iqmq/jetstream: the connection closed while the dead-letter copy waited for its PubAck, so whether the dead-letter subject's stream holds it is unknown; the original was not terminated")
)

// The start-position refusals. The specification gives four positions and this broker has
// all four, so the only refusals left are the ones that name no position at all.
var (
	errBadStartSequence = errcode.New(errcode.InvalidArgument,
		"iqmq/jetstream: a start sequence of zero names no message; JetStream stream sequences begin at 1")
	errBadStartPosition = errcode.New(errcode.InvalidArgument,
		"iqmq/jetstream: the start position is not one of the four")
)

// errExtendRange is an extension by nothing, which is a mistake rather than a
// request. The core refuses it too; refusing here as well is what makes the
// driver's own surface answer the same way when it is used directly.
var errExtendRange = errcode.New(errcode.InvalidArgument,
	"iqmq/jetstream: a lease extension must be positive")

// errExtendTooLong is an extension `+WPI` cannot grant. The server resets the
// redelivery timer to the delivery's lease from when it reads the `+WPI`, which
// is the consumer's AckWait or its BackOff step for the delivery, so anything
// longer would be reported as moved and redelivered at the end of the lease.
var errExtendTooLong = errcode.New(errcode.InvalidArgument,
	"iqmq/jetstream: the extension is longer than the delivery's lease (the consumer's AckWait, or its BackOff step for this delivery); +WPI resets the redelivery timer to that lease from now and cannot move it further")
