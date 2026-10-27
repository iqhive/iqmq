package kafkago

import (
	"errors"
	"io"
	"syscall"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// newErrorMapper builds this driver rules.
//
// Rules are per operation because retryability is, and because AMBIGUITY is.
// The distinction this driver exists to get right is the produce path's: once
// a record has entered the writer's batch, a failure reported afterwards may
// have been a failure to hear the answer rather than a failure to write the
// record, and Kafka has a whole family of errors like that. Every one of them
// is marked Ambiguous below, which is what turns them into the specification's
// CodeSettlementUnknown rather than a definite refusal a caller would retry
// into a duplicate.
//
// The table is the franz-go driver's judgements re-expressed over
// [kgo.Error], which is kafka-go's own enumeration of the same protocol error
// codes. Two drivers that classified the same broker answer differently would
// make a portable caller's retry policy depend on which client it happened to
// be built with, so TestCrossDriverErrorCodes asserts that the codes agree for
// every error code both clients name.
//
// The three that catch people out:
//
//   - NOT_ENOUGH_REPLICAS_AFTER_APPEND is, by its own name, an append that
//     happened. Reporting it as a failure is the textbook double publish.
//   - REQUEST_TIMED_OUT is the broker saying it did not finish in time, which
//     on a produce means the record may be in the leader's log already.
//   - a caller's own cancellation. kafka-go's WriteMessages returns ctx.Err()
//     the moment the context is done and lets the produce continue on the
//     writer's goroutine, so a cancelled publication is MORE likely to have
//     been written here than on a client that cancels the write. That case
//     reaches [driverkit.ErrorMapper.MapNet] with sentAlready true and comes
//     out ambiguous, which is the whole reason the produce path uses MapNet.
//
// What the mapper never does is throw the native error away: the result wraps
// it, so errors.As still reaches a [kgo.Error] and a caller who needs the
// broker's own code has it.
func newErrorMapper() *driverkit.ErrorMapper {
	auth := []driverkit.Rule{
		{Match: driverkit.MatchIs(kgo.SASLAuthenticationFailed, kgo.UnsupportedSASLMechanism, kgo.IllegalSASLState), Code: mq.CodeUnauthorized},
		{Match: driverkit.MatchIs(kgo.TopicAuthorizationFailed, kgo.GroupAuthorizationFailed, kgo.ClusterAuthorizationFailed, kgo.TransactionalIDAuthorizationFailed, kgo.DelegationTokenAuthorizationFailed, kgo.BrokerAuthorizationFailed), Code: mq.CodeForbidden},
	}
	link := []driverkit.Rule{
		// kafka-go reports a closed writer as io.ErrClosedPipe and a closed
		// group as its own sentinel; neither is a broker answer and both mean
		// ErrClosed.
		{Match: driverkit.MatchIs(io.ErrClosedPipe, kgo.ErrGroupClosed), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(kgo.BrokerNotAvailable, kgo.ReplicaNotAvailable, kgo.GroupCoordinatorNotAvailable, kgo.NotCoordinatorForGroup, kgo.GroupLoadInProgress, kgo.ListenerNotFound), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(kgo.LeaderNotAvailable, kgo.NotLeaderForPartition, kgo.FencedLeaderEpoch, kgo.UnknownLeaderEpoch, kgo.PreferredLeaderNotAvailable, kgo.EligibleLeadersNotAvailable), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(kgo.NetworkException, kgo.KafkaStorageError, kgo.LogDirNotFound), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(kgo.ThrottlingQuotaExceeded, kgo.GroupMaxSizeReached), Code: mq.CodeResourceExhausted, Retryable: true},
	}
	shape := []driverkit.Rule{
		{Match: driverkit.MatchIs(kgo.InvalidTopic, kgo.InvalidPartitionNumber, kgo.InvalidReplicationFactor, kgo.InvalidReplicaAssignment, kgo.InvalidConfiguration, kgo.InvalidGroupId, kgo.InvalidRequest, kgo.InvalidSessionTimeout, kgo.InvalidTimestamp), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(kgo.UnsupportedVersion, kgo.UnsupportedForMessageFormat, kgo.UnsupportedCompressionType, kgo.InconsistentGroupProtocol), Code: mq.CodeUnsupported},
		{Match: driverkit.MatchIs(kgo.PolicyViolation, kgo.TopicDeletionDisabled, kgo.SecurityDisabled), Code: mq.CodeForbidden},
		{Match: driverkit.MatchIs(kgo.InvalidMessage, kgo.InvalidRecord, kgo.InvalidMessageSize), Code: mq.CodeCorruptMessage},
	}
	// The produce path. Ambiguity is decided here and not by the caller,
	// which is what ClassifyNet's sentAlready argument cannot do on its own:
	// these are answers the broker gave, not transport failures.
	publish := []driverkit.Rule{
		// The writer's last attempt's answer, after it retried: an earlier
		// attempt may have been appended whatever this one says. First,
		// because the code underneath it is the last attempt's alone.
		{Match: matchRetriedOut, Code: mq.CodeSettlementUnknown, Ambiguous: true, Retryable: true},
		{Match: driverkit.MatchIs(kgo.MessageSizeTooLarge, kgo.RecordListTooLarge), Code: mq.CodeMessageTooLarge},
		// kafka-go's own refusal of a record larger than the batch bound,
		// made before anything is sent. It is a typed value rather than a
		// code, so it needs errors.As and not errors.Is.
		{Match: matchMessageTooLarge, Code: mq.CodeMessageTooLarge},
		// The broker wrote the record and could not replicate it far enough.
		// It is in the leader's log.
		{Match: driverkit.MatchIs(kgo.NotEnoughReplicasAfterAppend), Code: mq.CodeSettlementUnknown, Ambiguous: true, Retryable: true},
		// The broker refused before appending, so the record is not there.
		{Match: driverkit.MatchIs(kgo.NotEnoughReplicas), Code: mq.CodeUnavailable, Retryable: true},
		// The broker ran out of time. The append may have happened.
		{Match: driverkit.MatchIs(kgo.RequestTimedOut), Code: mq.CodeSettlementUnknown, Ambiguous: true, Retryable: true},
		// There is no idempotent producer on this client, so the sequence
		// errors below cannot be produced by ITS retries — but a broker can
		// still answer with them when another producer's transaction or
		// producer id is involved, and the honest answer is the same: the
		// client can no longer tell what the broker has.
		{Match: driverkit.MatchIs(kgo.OutOfOrderSequenceNumber, kgo.DuplicateSequenceNumber, kgo.UnknownProducerId, kgo.InvalidProducerEpoch, kgo.ProducerFenced), Code: mq.CodeSettlementUnknown, Ambiguous: true},
		{Match: driverkit.MatchIs(kgo.InvalidRequiredAcks), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(kgo.UnknownTopicOrPartition, kgo.UnknownTopicID), Code: mq.CodeNotFound, Retryable: true},
	}
	// The consume and settle paths. A group error is a lost assignment, and the answer for a settlement that arrives after one is ErrLeaseLost.
	group := []driverkit.Rule{
		{Match: driverkit.MatchIs(kgo.IllegalGeneration, kgo.UnknownMemberId, kgo.RebalanceInProgress, kgo.FencedInstanceID, kgo.MemberIDRequired, kgo.ErrGenerationEnded), Code: mq.CodeLeaseLost, Retryable: true},
		{Match: driverkit.MatchIs(kgo.OffsetOutOfRange, kgo.OffsetNotAvailable), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(kgo.InvalidCommitOffsetSize, kgo.OffsetMetadataTooLarge), Code: mq.CodeInvalidArgument},
		{Match: driverkit.MatchIs(kgo.UnstableOffsetCommit), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(kgo.GroupSubscribedToTopic, kgo.NonEmptyGroup), Code: mq.CodeConflict},
		{Match: driverkit.MatchIs(kgo.GroupIdNotFound), Code: mq.CodeNotFound},
		{Match: driverkit.MatchIs(kgo.UnknownTopicOrPartition, kgo.UnknownTopicID), Code: mq.CodeNotFound, Retryable: true},
	}
	admin := []driverkit.Rule{
		{Match: driverkit.MatchIs(kgo.TopicAlreadyExists), Code: mq.CodeConflict},
		{Match: driverkit.MatchIs(kgo.UnknownTopicOrPartition, kgo.UnknownTopicID), Code: mq.CodeNotFound},
		{Match: driverkit.MatchIs(kgo.NotController), Code: mq.CodeUnavailable, Retryable: true},
	}
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(publish, shape, link, auth, unnamed()),
			mq.OpFetch:   concat(group, shape, link, auth, unnamed()),
			mq.OpAck:     concat(group, shape, link, auth, unnamed()),
			mq.OpNack:    concat(publish, group, shape, link, auth, unnamed()),
			mq.OpAdmin:   concat(admin, shape, link, auth, unnamed()),
			mq.OpOpen:    concat(group, shape, link, auth, unnamed()),
			mq.OpClose:   concat(link, auth, unnamed()),
		},
		Fallback: concat(link, auth, shape, unnamed()),
	}
}

// mapSent classifies a failure from an operation whose request may already
// have reached the broker, and it is NOT simply
// [driverkit.ErrorMapper.MapNet].
//
// A [kgo.Error] is a BROKER REPLY: the broker processed the request and
// answered, so the outcome is definite unless a rule in this driver's table
// says otherwise — NOT_ENOUGH_REPLICAS_AFTER_APPEND and REQUEST_TIMED_OUT are
// the ones that say otherwise, and they say it by name. `MapNet` computes
// driverkit's transport classification first and ORs its ambiguity into
// whatever a matching rule said , which is right for a
// client whose protocol errors are not transport errors — and kafka-go's are:
// `kgo.Error` is an integer type with `Error`, `Timeout` and `Temporary`, so
// it satisfies [net.Error] and `ClassifyNet` reads every one of them on a send
// path as "the request may already have left the socket".
//
// The consequence, before this function existed, was that TOPIC_AUTHORIZATION_
// FAILED and MESSAGE_TOO_LARGE came out of a publish as the ambiguous
// outcome — "the record may be in the log, retry only if you can bear a
// duplicate" — for answers that mean the broker wrote nothing.
// TestCrossDriverErrorCodes and TestAmbiguousProduceOutcomes caught it on the
// merge that brought in.
//
// So a protocol code is mapped with `Map`, which ignores the transport
// classification and honours the rule table alone, and everything else — a
// cancellation, a deadline, an EOF, a genuine net.Error — is mapped with
// `MapNet` and sentAlready, which is what that argument is for.
func mapSent(m *driverkit.ErrorMapper, op, topic, sub string, err error) error {
	var pe kgo.Error
	if errors.As(err, &pe) {
		return m.Map(op, topic, sub, err)
	}
	return m.MapNet(op, topic, sub, err, true)
}

// retriedOut is a produce answer kafka-go's writer returned after retrying the
// batch, which is the only answer it keeps.
//
// The writer tries a batch again on a retriable answer, up to
// [Config.MaxAttempts] times, and hands back the last attempt's error and
// nothing of the attempts before it. A REQUEST_TIMED_OUT that was appended
// followed by a NOT_ENOUGH_REPLICAS therefore came out of a publish as the
// definite, retryable refusal while the record was in the log, and a caller's
// retry wrote a second copy. Once the writer has retried, an earlier attempt
// may have reached the log whatever the last one says.
type retriedOut struct{ err error }

func (e *retriedOut) Error() string { return e.err.Error() }

func (e *retriedOut) Unwrap() error { return e.err }

func matchRetriedOut(err error) bool {
	var e *retriedOut
	return errors.As(err, &e)
}

// afterAttempts marks the answer kafka-go's writer gave for a batch as
// reached by retrying, when it was.
//
// The answer alone says so. The writer stops at the first attempt whose
// answer it does not retry, so with more than one attempt allowed, an answer
// it does retry comes back only once every attempt has been spent. An answer
// it does not retry may still follow attempts it did, and nothing kafka-go
// returns tells the two apart, so that case keeps the last attempt's own
// classification.
func afterAttempts(err error, maxAttempts int) error {
	if err == nil || maxAttempts < 2 || !writerRetries(err) {
		return err
	}
	return &retriedOut{err: err}
}

// writerRetries is the test kafka-go's writer applies before it tries a batch
// again, copied from its isTemporary and isTransientNetworkError. It matches
// Temporary by interface, where the protocol-code matchers below match the
// type, because that is what the writer does: a context.DeadlineExceeded from
// its own WriteTimeout is retried like a retriable protocol code.
func writerRetries(err error) bool {
	var t interface{ Temporary() bool }
	if errors.As(err, &t) && t.Temporary() {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// matchMessageTooLarge recognises kafka-go's typed refusal of an oversized
// record, which carries the message rather than a protocol code.
func matchMessageTooLarge(err error) bool {
	var e kgo.MessageTooLargeError
	return errors.As(err, &e)
}

// matchTemporaryBrokerError and matchBrokerError are the CATCH-ALL for a
// protocol error code this driver's table does not name, and they exist
// because without them the fallback is actively wrong on this client.
//
// [kgo.Error] is an integer type with Error, Timeout AND Temporary methods, so
// it satisfies [net.Error] — and [github.com/iqhive/iqmq/driverkit.ClassifyNet]
// reads a net.Error as a TRANSPORT failure: retryable, and on a path with
// sentAlready true, AMBIGUOUS. So every one of the fifty-five protocol codes
// this table did not name came out of a publish as CodeSettlementUnknown with
// IsAmbiguous true — "the broker may have written it, retry if you can bear a
// duplicate" — for answers like NON_EMPTY_GROUP and INVALID_PRINCIPAL_TYPE
// that the broker gave INSTEAD of writing anything. A caller following that
// advice retries for ever and duplicates nothing, which is the mildest
// version; the sharp version is that a definite refusal was reported as an
// unknown outcome, which exists to keep apart.
// TestCrossDriverErrorCodeSurvey found it, and *kerr.Error does not implement
// Temporary, so the franz-go driver never had it.
//
// The judgement these two make is the honest one for a code nobody wrote a
// rule for: the BROKER ANSWERED, so the outcome is definite and never
// ambiguous. Kafka's own protocol documentation says which codes are
// retriable and kafka-go carries that list, so a retriable code is Unavailable
// and retryable and anything else is CodeUnknown — which the specification makes "carries
// no judgement, therefore not retryable".
//
// They match [kgo.Error] BY TYPE and never by interface, and the first attempt
// did the latter: an `interface{ error; Temporary() bool }` also matches
// context.DeadlineExceeded, whose own Temporary reports true — so a caller's
// deadline passing after the record was submitted came out as a definite
// retryable Unavailable instead of the ambiguous outcome, which is the one
// classification on the publish path that decides whether a caller may retry.
// TestCallerDeadlineAfterSubmissionIsAmbiguous found it. A rule that means
// "a protocol error code" has to name the protocol error code's type.
func matchTemporaryBrokerError(err error) bool {
	var e kgo.Error
	return errors.As(err, &e) && e.Temporary()
}

func matchBrokerError(err error) bool {
	var e kgo.Error
	return errors.As(err, &e)
}

// unnamed is the catch-all rule pair, last in every operation's list.
func unnamed() []driverkit.Rule {
	return []driverkit.Rule{
		{Match: matchTemporaryBrokerError, Code: mq.CodeUnavailable, Retryable: true},
		{Match: matchBrokerError, Code: mq.CodeUnknown},
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

// The driver's own refusals, for the conditions no native error names.
var (
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/kafkago: the capability table does not mention every feature of the inventory")
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig,
		"iqmq/kafkago: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errNoSchedule = errcode.New(errcode.Unsupported,
		"iqmq/kafkago: Kafka cannot delay or schedule a publication; a delay is the consumer's business and reaches it through the nack strategy")
	errNoTTL = errcode.New(errcode.Unsupported,
		"iqmq/kafkago: Kafka has topic retention, not a per-message time to live")
	errNoPriority = errcode.New(errcode.Unsupported,
		"iqmq/kafkago: Kafka has no message priority; a partition is a queue and not a heap")
	errNoDedup = errcode.New(errcode.Unsupported,
		"iqmq/kafkago: kafka-go's producer sends no producer id, so the broker de-duplicates nothing at all — not an application's key and not the client's own retries")
	errMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/kafkago: the metadata mode is not one of the four")
	errNoSubscription = errcode.New(errcode.InvalidArgument,
		"iqmq/kafkago: SourceConfig.Subscription is empty; a subscription is the consumer group and Kafka has no anonymous one")
	errBadGroup = errcode.New(errcode.InvalidArgument,
		"iqmq/kafkago: a consumer group name carries a byte Kafka does not accept")
	errStopped = errcode.New(errcode.PoisonMessage,
		"iqmq/kafkago: the nack strategy stopped this consumer; nothing after the refused record is consumed until it is reopened (the message contract, ADR-011)")
	errLeaseLost = errcode.New(errcode.LeaseLost,
		"iqmq/kafkago: the consumer group generation ended, so this settlement names a delivery another member now owns (the memory engine)")
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/kafkago: no dead-letter topic is configured, so there is nowhere to route this delivery (Config.NackConfig.DeadLetterTopic)")
	errNotSettleable = errcode.New(errcode.InvalidArgument,
		"iqmq/kafkago: the delivery did not come from this source")
	errNoLogBounds = errcode.New(errcode.Unavailable,
		"iqmq/kafkago: the broker answered no log bounds for an assigned partition, so its start offset cannot be resolved yet")
	errRevokeTimeout = errcode.New(errcode.DeadlineExceeded,
		"iqmq/kafkago: a generation ended while deliveries were still in flight and the bounded wait ran out; their offsets were not committed and will be redelivered (Config.RevokeWait)")
	errNilCallback = errcode.New(errcode.InvalidArgument,
		"iqmq/kafkago: PublishAsync was given no callback")
	errDisconnected = errcode.New(errcode.Unavailable,
		"iqmq/kafkago: the consumer group generation ended; the client is rejoining")
	errUnknownKind = errcode.New(errcode.InvalidConfig,
		"iqmq/kafkago: the DSN names a variant kind that no family has")
)
