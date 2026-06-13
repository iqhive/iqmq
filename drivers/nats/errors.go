package nats

import (
	"errors"
	"strings"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// newErrorMapper builds this driver rules.
//
// Rules are per operation because retryability is. The one distinction no
// native error type can make is whether the request had already left the
// socket: nats.go's Publish writes into a buffer and a flush is what proves the
// server saw it, so a failure reported by the flush is ambiguous — the server
// may have taken every message before the link went away — while the same
// failure reported by the write is not. That is what
// [bitbucket.org/iqhive/iqmq/driverkit.ErrorMapper.MapNet]'s sentAlready argument
// carries, and getting it backwards is how a duplicate comes to look like a
// loss.
func newErrorMapper() *driverkit.ErrorMapper {
	auth := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrAuthorization, natsgo.ErrAuthExpired, natsgo.ErrAuthRevoked, natsgo.ErrAccountAuthExpired), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchIs(natsgo.ErrPermissionViolation), Code: mq.CodeForbidden},
	}
	link := []driverkit.Rule{
		{Match: driverkit.MatchIs(natsgo.ErrConnectionClosed, natsgo.ErrInvalidConnection), Code: mq.CodeClosed},
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
	errMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/nats: the metadata mode is not one of the four")
)
