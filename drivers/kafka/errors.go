package kafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// newErrorMapper builds this driver rules.
//
// Rules are per operation because retryability is, and because AMBIGUITY is.
// The distinction this driver exists to get right is the produce path's: once
// a record has entered franz-go's buffer, a failure reported afterwards may
// have been a failure to hear the answer rather than a failure to write the
// record, and Kafka has a whole family of errors like that. Every one of them
// is marked Ambiguous below, which is what turns them into the specification's
// CodeSettlementUnknown rather than a definite refusal a caller would retry
// into a duplicate.
//
// The three that catch people out:
//
//   - NOT_ENOUGH_REPLICAS_AFTER_APPEND is, by its own name, an append that
//     happened. Reporting it as a failure is the textbook double publish.
//   - REQUEST_TIMED_OUT is the broker saying it did not finish in time, which
//     on a produce means the record may be in the leader's log already.
//   - kgo.ErrRecordTimeout and kgo.ErrRecordRetries are franz-go giving up on
//     a record it has been trying to send. It may have been written by an
//     attempt whose response never arrived.
//
// What the mapper never does is throw the native error away: the result wraps
// it, so errors.As still reaches a *kerr.Error and a caller who needs the
// broker's own code has it.
func newErrorMapper() *driverkit.ErrorMapper {
	auth := []driverkit.Rule{
		{Match: driverkit.MatchIs(kerr.SaslAuthenticationFailed, kerr.UnsupportedSaslMechanism, kerr.IllegalSaslState), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchIs(kerr.TopicAuthorizationFailed, kerr.GroupAuthorizationFailed, kerr.ClusterAuthorizationFailed, kerr.TransactionalIDAuthorizationFailed, kerr.DelegationTokenAuthorizationFailed), Code: mq.CodeForbidden},
	}
	link := []driverkit.Rule{
		{Match: driverkit.MatchIs(kgo.ErrClientClosed), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(kerr.BrokerNotAvailable, kerr.ReplicaNotAvailable, kerr.CoordinatorNotAvailable, kerr.NotCoordinator, kerr.CoordinatorLoadInProgress, kerr.ListenerNotFound), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(kerr.LeaderNotAvailable, kerr.NotLeaderForPartition, kerr.FencedLeaderEpoch, kerr.UnknownLeaderEpoch, kerr.PreferredLeaderNotAvailable, kerr.NewLeaderElected), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(kerr.NetworkException, kerr.KafkaStorageError), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(kerr.ThrottlingQuotaExceeded, kerr.GroupMaxSizeReached), Code: mq.CodeResourceExhausted, Retryable: true},
		{Match: driverkit.MatchIs(kgo.ErrMaxBuffered), Code: mq.CodeBackpressure, Retryable: true},
	}
	shape := []driverkit.Rule{
		{Match: driverkit.MatchIs(kerr.InvalidTopicException, kerr.InvalidPartitions, kerr.InvalidReplicationFactor, kerr.InvalidReplicaAssignment, kerr.InvalidConfig, kerr.InvalidGroupID, kerr.InvalidRequest), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(kerr.UnsupportedVersion, kerr.UnsupportedForMessageFormat, kerr.UnsupportedCompressionType, kerr.UnsupportedAssignor), Code: mq.CodeUnsupported},
		{Match: driverkit.MatchIs(kerr.PolicyViolation, kerr.TopicDeletionDisabled, kerr.SecurityDisabled), Code: mq.CodeForbidden},
		{Match: driverkit.MatchIs(kerr.CorruptMessage, kerr.InvalidRecord), Code: mq.CodeCorruptMessage},
	}
	// The produce path. Ambiguity is decided here and not by the caller,
	// which is what ClassifyNet's sentAlready argument cannot do on its own:
	// these are answers the broker gave, not transport failures.
	publish := []driverkit.Rule{
		{Match: driverkit.MatchIs(kerr.MessageTooLarge, kerr.RecordListTooLarge), Code: mq.CodeMessageTooLarge},
		// The broker wrote the record and could not replicate it far
		// enough. It is in the leader's log.
		{Match: driverkit.MatchIs(kerr.NotEnoughReplicasAfterAppend), Code: mq.CodeSettlementUnknown, Ambiguous: true, Retryable: true},
		// The broker refused before appending, so the record is not there.
		{Match: driverkit.MatchIs(kerr.NotEnoughReplicas), Code: mq.CodeUnavailable, Retryable: true},
		// The broker ran out of time. The append may have happened.
		{Match: driverkit.MatchIs(kerr.RequestTimedOut), Code: mq.CodeSettlementUnknown, Ambiguous: true, Retryable: true},
		// A topic the cluster does not know, as franz-go's timeout reports
		// it: with this as its last error. franz-go words it that way only
		// for a record still waiting for its topic's partitions, which it
		// never sent, so this is definite on any client. It comes before
		// the timeout's own rule.
		{Match: topicNeverResolved, Code: mq.CodeNotFound, Retryable: true},
		// franz-go gave up on a record it had been trying to send. A
		// record it never placed was not, whatever this says
		// ([neverSent]).
		{Match: driverkit.MatchIs(kgo.ErrRecordTimeout, kgo.ErrRecordRetries, kgo.ErrAborting), Code: mq.CodeSettlementUnknown, Ambiguous: true, Retryable: true},
		// The idempotent producer's sequence went wrong, which means the
		// client can no longer tell what the broker has.
		{Match: driverkit.MatchIs(kerr.OutOfOrderSequenceNumber, kerr.DuplicateSequenceNumber, kerr.UnknownProducerID, kerr.InvalidProducerEpoch, kerr.ProducerFenced), Code: mq.CodeSettlementUnknown, Ambiguous: true},
		{Match: driverkit.MatchIs(kerr.InvalidRequiredAcks), Code: mq.CodeInvalidArgument},
		// A topic the broker said it does not have. As one answer it is a
		// definite refusal; franz-go's retries of it are what make a
		// publication's outcome unknown ([unknownAfterRetries]).
		{Match: driverkit.MatchIs(kerr.UnknownTopicOrPartition, kerr.UnknownTopicID), Code: mq.CodeNotFound, Retryable: true},
	}
	// The consume and settle paths. A group error is a lost assignment, and the answer for a settlement that arrives after one is ErrLeaseLost.
	group := []driverkit.Rule{
		{Match: driverkit.MatchIs(kerr.IllegalGeneration, kerr.UnknownMemberID, kerr.RebalanceInProgress, kerr.FencedInstanceID, kerr.FencedMemberEpoch, kerr.StaleMemberEpoch, kerr.UnreleasedInstanceID), Code: mq.CodeLeaseLost, Retryable: true},
		{Match: driverkit.MatchIs(kerr.OffsetOutOfRange, kerr.OffsetMovedToTieredStorage), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(kerr.UnstableOffsetCommit), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(kerr.GroupSubscribedToTopic, kerr.NonEmptyGroup), Code: mq.CodeConflict},
		{Match: driverkit.MatchIs(kerr.GroupIDNotFound), Code: mq.CodeNotFound},
		{Match: driverkit.MatchIs(kerr.UnknownTopicOrPartition, kerr.UnknownTopicID), Code: mq.CodeNotFound, Retryable: true},
	}
	admin := []driverkit.Rule{
		{Match: driverkit.MatchIs(kerr.TopicAlreadyExists), Code: mq.CodeConflict},
		{Match: driverkit.MatchIs(kerr.UnknownTopicOrPartition, kerr.UnknownTopicID), Code: mq.CodeNotFound},
		{Match: driverkit.MatchIs(kerr.NotController), Code: mq.CodeUnavailable, Retryable: true},
	}
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(publish, shape, link, auth),
			mq.OpFetch:   concat(group, shape, link, auth),
			mq.OpAck:     concat(group, shape, link, auth),
			mq.OpNack:    concat(publish, group, shape, link, auth),
			mq.OpAdmin:   concat(admin, shape, link, auth),
			mq.OpOpen:    concat(group, shape, link, auth),
			mq.OpClose:   concat(link, auth),
		},
		Fallback: concat(link, auth, shape),
	}
}

// topicNeverResolved matches franz-go's record timeout with an unknown topic
// as its last error.
func topicNeverResolved(err error) bool {
	return errors.Is(err, kgo.ErrRecordTimeout) && unknownTopic(err)
}

// unknownAfterRetries reports a record franz-go failed with the broker's
// UNKNOWN_TOPIC_OR_PARTITION (or UNKNOWN_TOPIC_ID) on its own. franz-go
// retries that answer, and fails a record it placed with it only once it has
// had it too many times, so an attempt before those may have been appended
// (REQUEST_TIMED_OUT, then this). The rule reads the answer as the one
// refusal it is; for a record that may have been sent, the outcome is unknown.
// The same answer as a record timeout's last error is a record whose topic
// never resolved ([topicNeverResolved]).
func unknownAfterRetries(err error) bool {
	return unknownTopic(err) && !errors.Is(err, kgo.ErrRecordTimeout)
}

// closedHolding reports franz-go's Close failing a record it still held.
// Close fails every record in its buffer with kgo.ErrClientClosed, those whose
// produce request is already at the broker among them, and the broker appends
// such a record once it gets to the request. The link rule reads the error as
// a closed client, which is the whole answer for a record franz-go never
// placed; for one it may have sent, the outcome is unknown.
func closedHolding(err error) bool {
	return errors.Is(err, kgo.ErrClientClosed)
}

// unknownTopic reports the broker's answer for a topic it does not have.
func unknownTopic(err error) bool {
	return errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.UnknownTopicID)
}

// neverSent is the definite outcome of a publication or a nack's move whose
// record franz-go failed before placing it, for an error the publish rules
// call ambiguous. It keeps everything else the mapping said, the operation
// and the subscription among it.
//
// Those rules are written for a record that may be in a produce request, and
// MapNet keeps a rule's ambiguity whatever its caller says. A record still
// waiting for its topic's partitions is in no request. franz-go fails it with
// kgo.ErrRecordTimeout when they never load in time, with kgo.ErrAborting when
// the producer aborts its buffered records, and with the producer id's own
// fatal error when the producer has none it can use:
//
//   - the abort is the client cancelling the record, CodeCanceled;
//   - a producer whose epoch is old or that was fenced has been replaced by
//     another with its transactional id, and only a new client can publish
//     again: CodeConflict, not retryable;
//   - everything else is a cluster that did not take the record yet,
//     CodeUnavailable, retryable.
func neverSent(oe *mq.OpError) *mq.OpError {
	out := *oe
	out.Ambiguous = false
	switch {
	case errors.Is(oe.Err, kgo.ErrAborting):
		out.Code, out.Retryable = mq.CodeCanceled, false
	case errors.Is(oe.Err, kerr.InvalidProducerEpoch), errors.Is(oe.Err, kerr.ProducerFenced):
		out.Code, out.Retryable = mq.CodeConflict, false
	default:
		out.Code, out.Retryable = mq.CodeUnavailable, true
	}
	return &out
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

// The driver's own refusals, for the conditions no native error names.
var (
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/kafka: the capability table does not mention every feature of the inventory")
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig,
		"iqmq/kafka: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errNoSchedule = errcode.New(errcode.Unsupported,
		"iqmq/kafka: Kafka cannot delay or schedule a publication; a delay is the consumer's business and reaches it through the nack strategy")
	errNoTTL = errcode.New(errcode.Unsupported,
		"iqmq/kafka: Kafka has topic retention, not a per-message time to live")
	errNoPriority = errcode.New(errcode.Unsupported,
		"iqmq/kafka: Kafka has no message priority; a partition is a queue and not a heap")
	errNoDedup = errcode.New(errcode.Unsupported,
		"iqmq/kafka: Kafka's idempotent producer de-duplicates a producer's own retries, not an application's de-duplication key")
	errMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/kafka: the metadata mode is not one of the four")
	errNoSubscription = errcode.New(errcode.InvalidArgument,
		"iqmq/kafka: SourceConfig.Subscription is empty; a subscription is the consumer group and Kafka has no anonymous one")
	errBadGroup = errcode.New(errcode.InvalidArgument,
		"iqmq/kafka: a consumer group name carries a byte Kafka does not accept")
	errStopped = errcode.New(errcode.PoisonMessage,
		"iqmq/kafka: the nack strategy stopped this consumer; nothing after the refused record is consumed until it is reopened (the message contract, ADR-011)")
	errLeaseLost = errcode.New(errcode.LeaseLost,
		"iqmq/kafka: the partition was revoked or lost, so this settlement names a delivery another member now owns (the memory engine)")
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/kafka: no dead-letter topic is configured, so there is nowhere to route this delivery (Config.NackConfig.DeadLetterTopic)")
	errNotSettleable = errcode.New(errcode.InvalidArgument,
		"iqmq/kafka: the delivery did not come from this source")
	errRevokeTimeout = errcode.New(errcode.DeadlineExceeded,
		"iqmq/kafka: a revoked partition still had deliveries in flight when the bounded wait ran out; their offsets were not committed and will be redelivered (Config.RevokeWait)")
	errNilCallback = errcode.New(errcode.InvalidArgument,
		"iqmq/kafka: PublishAsync was given no callback")
	errDisconnected = errcode.New(errcode.Unavailable,
		"iqmq/kafka: a broker connection was lost; the client is reconnecting")
	errUnknownKind = errcode.New(errcode.InvalidConfig,
		"iqmq/kafka: the DSN names a variant kind that no family has")
)
