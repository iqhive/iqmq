package redis

import (
	"errors"
	"net"
	"strings"

	"github.com/redis/rueidis"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/streams"
)

// newErrorMapper builds this driver rules.
//
// Redis reports almost everything as an error REPLY whose first word is the
// condition: NOGROUP, BUSYGROUP, NOSCRIPT, CROSSSLOT, MOVED, LOADING,
// READONLY, WRONGTYPE, NOAUTH, NOPERM, OOM. There are no typed errors to match
// on, so the rules match prefixes — and that is why [driverkit.MatchContains]
// is used with the prefix INCLUDING its trailing space or the whole word: a
// rule matching "NOAUTH" alone would also fire on a message containing the
// word, which is a code a caller would act on wrongly.
//
// The one distinction no reply text can make is whether the command had
// already reached the server. That is
// [github.com/iqhive/iqmq/driverkit.ErrorMapper.MapNet]'s sentAlready
// argument, and getting it backwards is how a duplicate comes to look like a
// loss: a publication whose XADD was written and whose reply never
// arrived MAY have been stored, so its outcome is Ambiguous; one that failed
// before the write did not.
func newErrorMapper() *driverkit.ErrorMapper {
	auth := []driverkit.Rule{
		{Match: driverkit.MatchContains("NOAUTH", "WRONGPASS"), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchContains("NOPERM"), Code: mq.CodeForbidden},
	}
	topology := []driverkit.Rule{
		// The consumer group or the key is gone. It is an operator action
		// (XGROUP DESTROY, a FLUSHDB, a key that expired) rather than a
		// transient failure, so it is definite and not retryable: retrying
		// would spin against a group that is not coming back.
		{Match: driverkit.MatchContains("NOGROUP"), Code: mq.CodeNotFound},
		// The stream itself is missing, which Redis does not answer with
		// NOGROUP from XGROUP or XINFO (see [noStream]). These came out as
		// CodeUnknown.
		{Match: driverkit.MatchContains(xgroupNoKey, xinfoNoKey), Code: mq.CodeNotFound},
		// The group already exists, which is what an idempotent create sees.
		{Match: driverkit.MatchContains("BUSYGROUP"), Code: mq.CodeConflict},
		{Match: driverkit.MatchContains("WRONGTYPE"), Code: mq.CodeInvalidArgument},
		// A key that is not where this client thought. rueidis follows MOVED
		// and ASK itself, so one that reaches us is a cluster that is still
		// settling: retryable.
		{Match: driverkit.MatchContains("MOVED", "ASK", "CLUSTERDOWN", "TRYAGAIN"), Code: mq.CodeUnavailable, Retryable: true},
		// A multi-key command whose keys are on two slots. It is a
		// programming error in the KEY NAMING and never transient, which is
		// why the driver checks SlotSafe before it sends one.
		{Match: driverkit.MatchContains("CROSSSLOT"), Code: mq.CodeUnsupported},
	}
	server := []driverkit.Rule{
		{Match: driverkit.MatchContains("LOADING"), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchContains("MASTERDOWN"), Code: mq.CodeUnavailable, Retryable: true},
		// A replica refusing a write: a failover in progress. Retryable, and
		// the health surface reports the degradation.
		{Match: driverkit.MatchContains("READONLY You can't write"), Code: mq.CodeUnavailable, Retryable: true},
		// Out of memory. The specification makes this backpressure rather than a plain
		// failure: the publication was refused because the broker is full,
		// which is the condition a caller backs off on.
		{Match: driverkit.MatchContains("OOM command not allowed"), Code: mq.CodeBackpressure, Retryable: true},
		{Match: driverkit.MatchContains("BUSY Redis is busy"), Code: mq.CodeResourceExhausted, Retryable: true},
		// The script cache is empty: the server restarted or somebody ran
		// SCRIPT FLUSH. It is the one error this driver recovers from itself,
		// by sending the script body and retrying, so it is retryable.
		{Match: driverkit.MatchContains("NOSCRIPT"), Code: mq.CodeUnavailable, Retryable: true},
	}
	// None of these six rules names the "ERR " the reply carries, and that is
	// the whole point of them. Redis prefixes a generic error reply with
	// "ERR ", and the two client libraries this driver runs behind present it
	// differently: rueidis STRIPS "ERR " from every error reply it decodes (a
	// kvrocks compatibility workaround in RedisMessage.Error) and go-redis
	// keeps it. So a rule matching "ERR unknown command" fired for
	// `drivers/redis/goredis` and never for rueidis -- and under rueidis, the
	// client this module was written for, all six rules were dead: an invalid
	// stream identifier, a wrong arity and an unsupported command each came
	// out as CodeUnknown. What made it invisible is that `errors_test.go`
	// feeds the mapper the reply TEXT, which is the text a server sends and
	// not the text either client hands over. The distinctive phrase is what
	// is matched, so both spellings fire and neither is a single word that
	// could appear in an unrelated message.
	shape := []driverkit.Rule{
		{Match: driverkit.MatchContains("Invalid stream ID"), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchContains("The ID specified in XADD is equal or smaller"), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchContains("wrong number of arguments", "syntax error"), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchContains("unknown command"), Code: mq.CodeUnsupported},
		{Match: driverkit.MatchContains("DB index is out of range", "SELECT is not allowed"), Code: mq.CodeInvalidConfig},
		{Match: driverkit.MatchContains("WAITAOF cannot be used"), Code: mq.CodeUnsupported},
	}
	// The link rules are only the client's own refusals, whose outcome is
	// definite whatever the caller thought, and every transport failure (a
	// timeout, a cancellation, an EOF, a *net.OpError) is left to
	// driverkit.ClassifyNet, where sentAlready decides the ambiguity.
	//
	// This used to be load-bearing rather than tidy: ErrorMapper.MapNet
	// consulted the rule table FIRST and only fell back to ClassifyNet when no
	// rule matched, so a rule that matched a transport failure threw away the
	// sentAlready argument the whole call exists for. A publication whose XADD
	// was written and whose reply never arrived was then reported as a
	// definite failure, the caller republished, and the first copy was stored
	// after all — a duplicate presented as a loss, which is the exact mistake
	// The specification names and calls a disqualifier.
	//
	// The kit now stops it: MapNet classifies the transport first and a
	// matching rule can refine the code and the retryability but cannot
	// contradict the ambiguity. So a driver written after
	// this may add a transport rule safely. This table stays as it is because
	// ClassifyNet already answers correctly for every condition it would have
	// named, and TestTransportFailureAfterAWriteIsAmbiguous holds both halves.
	//
	// go-redis's own refusals are matched by their messages, because this
	// module does not import go-redis and `drivers/redis/goredis` builds on
	// this table. A pool with no free connection (ErrPoolTimeout,
	// ErrPoolExhausted) is a local capacity limit a retry can get past, and a
	// closed client (ErrClosed) is what rueidis.ErrClosing is on the other
	// arm. They used to fall through to CodeUnknown, not retryable.
	//
	// A publication or a settlement that rueidis answers with ErrClosing may
	// have been written: rueidis's Close waits a second for the replies in
	// flight, then closes the connection and answers every command still
	// waiting for one with ErrClosing, the written ones included. The code
	// stays CodeClosed and the outcome is unknown. ClassifyNet does not read
	// ErrClosing as a transport failure, so sentAlready did not reach it, and
	// a publication whose XADD had run was reported as never made: a caller
	// that published it again stored it twice. A publication made once a
	// close has begun is refused before anything is sent (sink.check), so the
	// rule reaches a call that was in flight as the client closed. rueidis
	// also closes connections outside any close of the driver's, with the
	// same answer for what they held: a Sentinel failover closes the old
	// master's, and a cluster client one to a node its slot map dropped or
	// the old one after a MOVED to the same host. ErrClosing answers a call
	// made through a client its owner closed, or through a connection rueidis
	// had just closed, without writing it, and the driver cannot tell any of
	// these from the written case: unknown is the side a duplicate cannot
	// hide on.
	//
	// It is only for these operations. Opening, fetching and administration
	// have nothing a caller could repeat into a duplicate, and keep the link
	// rule's definite CodeClosed.
	inFlight := []driverkit.Rule{
		{Match: driverkit.MatchIs(rueidis.ErrClosing), Code: mq.CodeClosed, Ambiguous: true},
	}
	link := []driverkit.Rule{
		{Match: driverkit.MatchIs(rueidis.ErrClosing), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(rueidis.ErrNoAddr), Code: mq.CodeInvalidConfig},
		{Match: driverkit.MatchContains("redis: client is closed"), Code: mq.CodeClosed},
		{
			Match:     driverkit.MatchContains("redis: connection pool timeout", "redis: connection pool exhausted"),
			Code:      mq.CodeResourceExhausted,
			Retryable: true,
		},
	}
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(shape, topology, server, inFlight, link, auth),
			mq.OpFetch:   concat(topology, server, shape, link, auth),
			mq.OpAck:     concat(topology, server, shape, inFlight, link, auth),
			mq.OpNack:    concat(topology, server, shape, inFlight, link, auth),
			mq.OpExtend:  concat(topology, server, shape, inFlight, link, auth),
			mq.OpOpen:    concat(topology, shape, server, link, auth),
			mq.OpClose:   concat(link, auth),
			mq.OpAdmin:   concat(topology, shape, server, link, auth),
		},
		Fallback: concat(topology, server, shape, link, auth),
	}
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

// written reports whether a command whose round trip failed with err may
// have reached the server, which is MapNet's sentAlready for every command
// this driver sends.
//
// It is false only for a dial that connected nowhere: refused, unreachable,
// timed out or cut short by its context, or a name that did not resolve.
// Neither client sends a command this driver builds a second time (rueidis
// retries none of them, and on go-redis every one carries NoRetry, see
// drivers/redis/goredis), so a dial error is the answer for a command that
// never left the client, not for a written one whose retry could not
// reconnect. The exception is a rueidis client built with ConnLifetime, which
// sends again any command whose connection's lifetime ended while it waited
// for its reply; [OpenWithClient] says to leave it unset, because the driver
// cannot see it. A failure on a connection that was made, such as a reset or an
// EOF, keeps the command written. driverkit.ClassifyNet reads every dial
// error as possibly applied when sentAlready is true, so every publication
// and settlement while the server was down came back ambiguous and not
// retryable, and a settlement's unknown outcome was kept for good.
//
// A settlement made of two round trips is the one place a later dial can
// follow a write, and internal/streams marks that half-done move unknown
// itself (Consumer.deadLetterApart), whatever this says about the dial.
func written(err error) bool {
	oe, ok := errors.AsType[*net.OpError](err)
	return !ok || oe.Op != "dial"
}

// noStream reports whether err is the server saying the stream, or the group
// on it, does not exist.
//
// Redis checks the key before the group, and for a missing key it answers
// NOGROUP only from the commands that read a group (XREADGROUP, XPENDING,
// XCLAIM, XAUTOCLAIM). Every XGROUP subcommand answers "The XGROUP subcommand
// requires the key to exist", CREATE without MKSTREAM included, and XINFO
// answers "no such key" (measured on 7.4 and 8.2). The driver matched NOGROUP
// alone, which is what its in-memory server used to answer, so against a
// server a source opened with AutoCreate off on a missing stream was
// CodeUnknown rather than errNoAutoCreate, DeleteSubscription of a topic that
// is not there failed, and Purge and Stats were CodeUnknown. The texts are
// matched by containment because rueidis strips the reply's "ERR " and
// go-redis keeps it.
func noStream(err error) bool {
	if err == nil {
		return false
	}
	if streams.IsNoGroup(err) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, xgroupNoKey) || strings.Contains(msg, xinfoNoKey)
}

// xgroupNoKey and xinfoNoKey are the distinctive parts of Redis's answers for
// a missing stream ([noStream]).
const (
	xgroupNoKey = "The XGROUP subcommand requires the key to exist"
	xinfoNoKey  = "no such key"
)

// The driver's own refusals, for the conditions no server reply names.
var (
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/redis: the capability table does not mention every feature of the inventory")
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig,
		"iqmq/redis: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errNoDurableConfirm = errcode.New(errcode.Unsupported,
		"iqmq/redis: this server cannot define a durable boundary: WAITAOF needs Redis 7.2 or later and an append-only file, so ConfirmDurable cannot be honoured (the message contract)")
	errNoSchedule = errcode.New(errcode.Unsupported,
		"iqmq/redis: Redis Streams cannot delay or schedule a publication; a delayed REDELIVERY is the zset nack strategy's, and a delayed publication is not the same thing")
	errNoTTL = errcode.New(errcode.Unsupported,
		"iqmq/redis: a stream entry has no time to live; a stream is bounded by MAXLEN, which is the whole stream's retention and not one message's")
	errNoPriority = errcode.New(errcode.Unsupported,
		"iqmq/redis: Redis Streams has no message priority")
	errNoDedup = errcode.New(errcode.Unsupported,
		"iqmq/redis: Redis Streams has no publication de-duplication window")
	errNoReceipts = errcode.New(errcode.Unsupported,
		"iqmq/redis: this driver does not advertise FeatureReceipts, so a sink must not be opened with Receipts")
	errMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/redis: the metadata mode is not one of the four")
	errNoAutoCreate = errcode.New(errcode.NotFound,
		"iqmq/redis: the stream or the consumer group does not exist and AutoCreate is off (the API)")
	errBadStartPosition = errcode.New(errcode.Unsupported,
		"iqmq/redis: this start position has no Redis Streams equivalent")
	errBadDatabase = errcode.New(errcode.InvalidConfig,
		"iqmq/redis: the DSN path is the logical database number and is not one")
	errPartitionOutOfRange = errcode.New(errcode.InvalidArgument,
		"iqmq/redis: the publication names a partition this topic does not have")
	errNackDelay = errcode.New(errcode.Unsupported,
		"iqmq/redis: the bound nack strategy cannot hold a redelivery back, so a nack with a delay is refused and nothing is settled; nack=zsetmover is the strategy that honours a delay")
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/redis: the subscription has no dead-letter destination, so a Reject has nowhere to route (the message contract)")
	errExtendTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/redis: the extension is longer than the source's AckDeadline; it resets the entry's idle clock, and the reclaim sweep takes the entry back one AckDeadline after that")
)
