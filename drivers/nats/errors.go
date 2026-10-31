package nats

import (
	"errors"
	"strconv"
	"strings"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// newErrorMapper builds this driver rules.
//
// Rules are per operation because retryability is. The one distinction no
// native error type can make is whether the request had already left the
// socket: nats.go's Publish writes into a buffer and a flush is what proves the
// server saw it, so a failure reported by the flush is ambiguous — the server
// may have taken every message before the link went away — while the same
// failure reported by the write is not. That is what
// [github.com/iqhive/iqmq/driverkit.ErrorMapper.MapNet]'s sentAlready argument
// carries, and getting it backwards is how a duplicate comes to look like a
// loss.
func newErrorMapper() *driverkit.ErrorMapper {
	auth := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrAuthorization, natsgo.ErrAuthExpired, natsgo.ErrAuthRevoked, natsgo.ErrAccountAuthExpired), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchIs(natsgo.ErrPermissionViolation), Code: mq.CodeForbidden},
	}
	link := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrConnectionClosed, natsgo.ErrInvalidConnection, errClosedMidRequest), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(natsgo.ErrConnectionDraining), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(natsgo.ErrNoServers, natsgo.ErrDisconnected, natsgo.ErrStaleConnection, natsgo.ErrConnectionReconnecting), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(natsgo.ErrSlowConsumer), Code: mq.CodeResourceExhausted, Retryable: true},
		{Match: driverkit.MatchIs(natsgo.ErrMaxConnectionsExceeded, natsgo.ErrMaxAccountConnectionsExceeded, natsgo.ErrMaxSubscriptionsExceeded), Code: mq.CodeResourceExhausted, Retryable: true},
		{Match: driverkit.MatchIs(natsgo.ErrSecureConnRequired, natsgo.ErrSecureConnWanted, natsgo.ErrTLS, natsgo.ErrConnectionNotTLS), Code: mq.CodeUnavailable},
		{Match: driverkit.MatchIs(natsgo.ErrTimeout), Code: mq.CodeDeadlineExceeded, Retryable: true},
	}
	shape := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrBadSubject, natsgo.ErrInvalidMsg, natsgo.ErrInvalidArg), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(natsgo.ErrNoDeadlineContext, natsgo.ErrBadTimeout, natsgo.ErrInvalidContext), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(natsgo.ErrBadQueueName, natsgo.ErrBadSubscription), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(natsgo.ErrHeadersNotSupported), Code: mq.CodeUnsupported},
		{Match: driverkit.MatchIs(natsgo.ErrBadHeaderMsg), Code: mq.CodeCorruptMessage},
	}
	publish := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrMaxPayload), Code: mq.CodeMessageTooLarge},
		// The client's own reconnect buffer is full: the publication was
		// never written, so it is a definite refusal and it is the one
		// place the backpressure code belongs on this driver.
		{Match: driverkit.MatchIs(natsgo.ErrReconnectBufExceeded), Code: mq.CodeBackpressure, Retryable: true},
	}
	request := []driverkit.Rule{
		// Nobody is subscribed: the request reached the server and the
		// server answered "no responders", which is a definite NotFound and
		// not an ambiguous round trip.
		{Match: driverkit.MatchIs(natsgo.ErrNoResponders), Code: mq.CodeNotFound, Retryable: true},
	}
	m := &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(publish, request, shape, link, auth),
			mq.OpFetch:   concat(shape, link, auth),
			mq.OpOpen:    concat(shape, link, auth),
			mq.OpClose:   concat(link, auth),
		},
		Fallback: concat(link, auth, shape),
	}
	return m
}

// failWritten maps the failure of a round trip made after its request was
// written: a confirmation flush, or a request waiting for its reply.
//
// nats.go clears every pending flush when the link drops, when it is told to
// reconnect and when it closes (processOpErr, ForceReconnect, close), and every
// pending request on the last two. A flush cut that way returns
// ErrConnectionClosed even while the connection is only reconnecting, and a
// request returns ErrDisconnected, or ErrConnectionClosed once the connection
// has closed. The rule set reads those as a definite Closed and a definite,
// retryable Unavailable, which is right for a write refused before anything
// left and wrong here: the bytes had gone, and the server may have taken them.
// So both are ambiguous, and while the client is reconnecting the answer is
// Unavailable rather than Closed, retryable for a caller who can de-duplicate,
// because the link is coming back.
func (b *Backend) failWritten(topic string, err error) error {
	mapped := b.errs.MapNet(mq.OpPublish, topic, "", err, true)
	var oe *mq.OpError
	cut := errors.Is(err, natsgo.ErrConnectionClosed) || errors.Is(err, natsgo.ErrDisconnected)
	if !cut || !errors.As(mapped, &oe) {
		return mapped
	}
	if !b.nc.IsClosed() {
		oe.Code, oe.Retryable = mq.CodeUnavailable, true
	}
	oe.Ambiguous = true
	return mapped
}

// refusal is the permissions violation the server answered a publication to
// subject with, or one with reply as its reply subject, when it arrived after
// before was read from the connection; nil otherwise.
//
// The server discards such a publication and says so with an -ERR that it
// writes before the PONG of the flush that follows. nats.go keeps it only as
// the connection's last error and in the asynchronous error handler, so a
// confirmation that waited only for the PONG reported the discarded message
// as accepted. The last error is set before the PONG is read, so a flush that
// has returned has already seen it, on any connection, dialled or handed
// over. It holds one error, so any asynchronous error arriving after the
// refusal and before the PONG hides it (a refusal of another subject, a
// refused subscription, a slow consumer), and health still reports it. The
// refusal names the subject, not the message: where permission depends on
// more than the subject (response permissions, a reload), a publication the
// server took can be reported refused because another one to the subject was.
func (b *Backend) refusal(before error, subject, reply string) error {
	now := b.nc.LastError()
	if !errors.Is(now, natsgo.ErrPermissionViolation) || now == before { //nolint:errorlint // identity is what says the refusal is new
		return nil
	}
	text := now.Error()
	if strings.HasSuffix(text, "Publish to "+strconv.Quote(subject)) ||
		(reply != "" && strings.HasSuffix(text, "Publish with Reply of "+strconv.Quote(reply))) {
		return now
	}
	return nil
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
// It is matched by identity where the client gives one and by message where it
// does not: nats.go reports the per-subscription case as ErrSlowConsumer and
// the connection-wide case through the same callback, and a driver that only
// matched the sentinel would report half of them as CodeUnknown.
func isSlowConsumer(err error) bool {
	return err != nil &&
		(errors.Is(err, natsgo.ErrSlowConsumer) || strings.Contains(err.Error(), "slow consumer"))
}

// The driver's own refusals, for the conditions no native error names.
var (
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/nats: the capability table does not mention every feature of the inventory")
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig,
		"iqmq/nats: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errNoDurableConfirm = errcode.New(errcode.Unsupported,
		"iqmq/nats: NATS Core has no durable boundary, so ConfirmDurable cannot be honoured (the message contract)")
	errNoSchedule = errcode.New(errcode.Unsupported,
		"iqmq/nats: NATS Core cannot delay or schedule a publication")
	errNoTTL = errcode.New(errcode.Unsupported,
		"iqmq/nats: NATS Core cannot give a message a time to live")
	errNoPriority = errcode.New(errcode.Unsupported,
		"iqmq/nats: NATS Core has no message priority")
	errNoDedup = errcode.New(errcode.Unsupported,
		"iqmq/nats: NATS Core has no publication de-duplication")
	errNoPartition = errcode.New(errcode.Unsupported,
		"iqmq/nats: NATS Core has no partitions to publish to")
	errNoReplySubject = errcode.New(errcode.InvalidArgument,
		"iqmq/nats: the delivery carries no reply subject to respond to")
	errClosedMidRequest = errcode.New(errcode.Closed,
		"iqmq/nats: the backend closed while the request waited for its reply")
	errMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/nats: the metadata mode is not one of the four")
)
