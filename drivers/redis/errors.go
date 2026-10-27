package redis

import (
	"github.com/redis/rueidis"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
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
	// The link rules are only the two conditions whose outcome is definite
	// whatever the caller thought, and every transport failure — a timeout, a
	// cancellation, an EOF, a *net.OpError — is left to
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
	link := []driverkit.Rule{
		{Match: driverkit.MatchIs(rueidis.ErrClosing), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(rueidis.ErrNoAddr), Code: mq.CodeInvalidConfig},
	}
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(shape, topology, server, link, auth),
			mq.OpFetch:   concat(topology, server, shape, link, auth),
			mq.OpAck:     concat(topology, server, shape, link, auth),
			mq.OpNack:    concat(topology, server, shape, link, auth),
			mq.OpExtend:  concat(topology, server, shape, link, auth),
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

// The driver's own refusals, for the conditions no server reply names.
var (
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/redis: the capability table does not mention every feature of the inventory")
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig,
		"iqmq/redis: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errNoDurableConfirm = errcode.New(errcode.Unsupported,
		"iqmq/redis: this server cannot define a durable boundary: WAITAOF needs Redis 7.0 or later and an append-only file, so ConfirmDurable cannot be honoured (the message contract)")
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
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/redis: the subscription has no dead-letter destination, so a Reject has nowhere to route (the message contract)")
	errExtendTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/redis: the extension is longer than the source's AckDeadline; it resets the entry's idle clock, and the reclaim sweep takes the entry back one AckDeadline after that")
)
