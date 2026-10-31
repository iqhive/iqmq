package pulsar

import (
	"errors"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// Resulter is an error that carries one of pulsar-client-go's result codes.
//
// `*pulsar.Error` is the one the library produces. The driver matches on the
// INTERFACE rather than on that concrete type for two reasons: a library
// release that wrapped its errors in a new type would otherwise take every
// classification with it, and an in-process model has no way to construct a
// `*pulsar.Error` at all — the library exports the type and not a constructor,
// so a fake that could not report a result code could not exercise the error
// mapper.
type Resulter interface {
	error
	// Result is the Pulsar result code this error carries.
	Result() pulsar.Result
}

// ResultOf extracts pulsar-client-go's own result code from an error, if it
// carries one, or else the code of the broker refusal the error's text names.
//
// It is the only way to classify a Pulsar failure: the library's *pulsar.Error
// values are constructed per failure rather than being sentinels, so comparing
// pointers works for the fifteen package-level ones and for nothing else.
// `errors.As` reaches the code through the library's own `pkg/errors` wrapping
// as well, which is what a seek failure arrives inside.
//
// A refusal the broker sends in answer to a request carries no result code at
// all in pulsar-client-go v0.21: a CommandError reaches the caller as
// errors.New("server error: <Name>: <message>") (internal/connection.go,
// handleResponseError), a lookup's refusal as the bare name
// (internal/lookup_service.go), and a subscription's ERROR response as
// "<Name>: <message>" (consumer_partition.go). So against a real broker a
// second Exclusive consumer, a terminated topic and a missing one all matched
// no rule and read as CodeUnknown. The name is the broker's ServerError, which
// [brokerResult] translates into the code the library would have given it.
func ResultOf(err error) (pulsar.Result, bool) {
	var r Resulter
	if errors.As(err, &r) {
		return r.Result(), true
	}
	return brokerResult(refusalName(err))
}

// producerBusy is the broker's refusal of a producer whose name another
// producer on the topic already has. pulsar-client-go has no result code for
// it, so a rule matches it by name ([refusedAs]).
const producerBusy = "ProducerBusy"

// refusalName is the broker's own name for the refusal err reports as text,
// or "" when it reports none. Every error in err's tree is read, so a refusal
// wrapped on its way out is still found.
func refusalName(err error) string {
	if err == nil {
		return ""
	}
	if name := refusalIn(err.Error()); name != "" {
		return name
	}
	switch u := err.(type) { //nolint:errorlint // this is the unwrap step itself
	case interface{ Unwrap() error }:
		return refusalName(u.Unwrap())
	case interface{ Unwrap() []error }:
		for _, one := range u.Unwrap() {
			if name := refusalName(one); name != "" {
				return name
			}
		}
	}
	return ""
}

// refusalIn reads a refusal's name from one error's own text, in the three
// forms pulsar-client-go v0.21 writes (see [ResultOf]). A name the broker does
// not use is no refusal, so a text that merely starts with a word and a colon
// reads as none.
func refusalIn(text string) string {
	name, _, _ := strings.Cut(strings.TrimPrefix(text, "server error: "), ": ")
	if _, ok := brokerResult(name); ok || name == producerBusy {
		return name
	}
	return ""
}

// brokerResult is the result code for the broker's name for a refusal (its
// ServerError), and false for a name pulsar-client-go has no code for. The
// pairs are the ones the Pulsar C++ client makes (ClientConnection.cc,
// getResult), in the Go library's names.
func brokerResult(name string) (pulsar.Result, bool) {
	switch name {
	case "MetadataError":
		return pulsar.BrokerMetadataError, true
	case "PersistenceError":
		return pulsar.BrokerPersistenceError, true
	case "AuthenticationError":
		return pulsar.AuthenticationError, true
	case "AuthorizationError":
		return pulsar.AuthorizationError, true
	case "ConsumerBusy":
		return pulsar.ConsumerBusy, true
	case "ServiceNotReady":
		return pulsar.ServiceUnitNotReady, true
	case "ProducerBlockedQuotaExceededError":
		return pulsar.ProducerBlockedQuotaExceededError, true
	case "ProducerBlockedQuotaExceededException":
		return pulsar.ProducerBlockedQuotaExceededException, true
	case "ChecksumError":
		return pulsar.ChecksumError, true
	case "UnsupportedVersionError":
		return pulsar.UnsupportedVersionError, true
	case "TopicNotFound":
		return pulsar.TopicNotFound, true
	case "SubscriptionNotFound":
		return pulsar.SubscriptionNotFound, true
	case "ConsumerNotFound":
		return pulsar.ConsumerNotFound, true
	case "TooManyRequests":
		return pulsar.TooManyLookupRequestException, true
	case "TopicTerminatedError":
		return pulsar.TopicTerminated, true
	case "InvalidTopicName":
		return pulsar.InvalidTopicName, true
	case "IncompatibleSchema":
		return pulsar.SchemaFailure, true
	case "InvalidTxnStatus":
		return pulsar.InvalidStatus, true
	case "TransactionNotFound":
		return pulsar.TransactionNoFoundError, true
	case "ProducerFenced":
		return pulsar.ProducerFenced, true
	}
	return pulsar.Ok, false
}

// refusedAs matches a broker refusal by its name, for a refusal
// pulsar-client-go has no result code for.
func refusedAs(name string) func(error) bool {
	return func(err error) bool { return refusalName(err) == name }
}

// matchResult matches an error carrying any of the given Pulsar result codes.
func matchResult(rs ...pulsar.Result) func(error) bool {
	return func(err error) bool {
		got, ok := ResultOf(err)
		if !ok {
			return false
		}
		for _, r := range rs {
			if got == r {
				return true
			}
		}
		return false
	}
}

// PublishAdmitted reports whether a failed publication had already left this
// process when it failed, which is what decides the ambiguity.
//
// It is a whitelist of the refusals that happen BEFORE anything is written, and
// everything else is treated as admitted. That asymmetry is deliberate: calling
// a definite refusal ambiguous costs a caller one unnecessary de-duplication,
// and calling an ambiguous outcome definite costs it a lost message or a silent
// duplicate — which is the specification's "misclassifies an ambiguous publish".
//
// So a code is on the list only when pulsar-client-go v0.21 never uses it for a
// message it has written. ProducerClosed, TopicTerminated, ProducerFenced and
// ProducerBlockedQuotaExceededException are not: the producer fails every
// message still waiting for its receipt with them (failPendingMessages, on Close
// and when a reconnection is refused), and those messages are on the wire. They
// used to be listed, so a Publish the broker had stored and a Close then failed
// was reported as never sent. ProducerClosed is also the answer to a
// publication on a producer that had already closed, which never left; the
// code cannot tell the two apart, and the asymmetry above decides.
func PublishAdmitted(err error) bool {
	if err == nil {
		return false
	}
	r, ok := ResultOf(err)
	if !ok {
		// Neither the library's result code nor a refusal the broker
		// named: a validation refusal this driver made itself, or a
		// context error the caller's own
		// deadline produced. The first never reached the client; the
		// second is classified by driverkit.ClassifyNet against the
		// caller's own sentAlready, which the sink decides from where
		// the error came from (sink.failSend).
		return false
	}
	switch r {
	case pulsar.ProducerQueueIsFull, pulsar.ClientMemoryBufferIsFull,
		pulsar.MessageTooBig, pulsar.InvalidMessage,
		pulsar.AlreadyClosedError,
		pulsar.InvalidTopicName, pulsar.InvalidConfiguration,
		pulsar.SchemaFailure, pulsar.AddToBatchFailed,
		pulsar.InvalidBatchBuilderType,
		pulsar.ProducerBlockedQuotaExceededError,
		pulsar.ProducerNotInitialized:
		return false
	default:
		return true
	}
}

// newErrorMapper builds this driver rules.
//
// Rules are per operation because retryability is, and because one Pulsar
// result means different things on two operations: `TimeoutError` on a
// publication is a message that may be at the broker, and on a lookup it is a
// request that certainly is not.
//
// The order inside a set is the order of specificity, and the first two entries
// of the publication set are load-bearing:
//
//   - ProducerQueueIsFull is BACKPRESSURE and not a failure. The producer's
//     queue filled because this process is publishing faster than the broker
//     is accepting, the message was never admitted, and the specification gives the core a
//     mode for exactly that. Mapping it to CodeUnavailable — which is what a
//     transport rule would do — would make a caller's retry policy hammer a
//     broker that is keeping up perfectly well.
//   - ClientMemoryBufferIsFull is the same thing measured in bytes.
func newErrorMapper() *driverkit.ErrorMapper {
	backpressure := []driverkit.Rule{
		{Match: matchResult(pulsar.ProducerQueueIsFull), Code: mq.CodeBackpressure},
		{Match: matchResult(pulsar.ClientMemoryBufferIsFull), Code: mq.CodeBackpressure},
	}
	shape := []driverkit.Rule{
		{Match: matchResult(pulsar.MessageTooBig), Code: mq.CodeMessageTooLarge},
		// pulsar.ErrMetaTooLarge — a message whose METADATA exceeds the
		// broker's maximum — carries InvalidMessage rather than
		// MessageTooBig, so a message refused for its properties reads
		// as an invalid argument. It is the honest code: the body is
		// within the limit and what is too large is the header set,
		// which the limits describe and MaxMessageBytes does not.
		{Match: matchResult(pulsar.InvalidMessage), Code: mq.CodeInvalidArgument},
		{Match: matchResult(pulsar.InvalidTopicName), Code: mq.CodeInvalidArgument},
		{Match: matchResult(pulsar.ChecksumError, pulsar.CryptoError), Code: mq.CodeCorruptMessage},
		{Match: matchResult(pulsar.SchemaFailure, pulsar.InvalidBatchBuilderType), Code: mq.CodeInvalidConfig},
		{Match: matchResult(pulsar.InvalidConfiguration, pulsar.InvalidURL), Code: mq.CodeInvalidConfig},
		{Match: matchResult(pulsar.AddToBatchFailed), Code: mq.CodeInvalidArgument},
	}
	notFound := []driverkit.Rule{
		{Match: matchResult(pulsar.TopicNotFound, pulsar.SubscriptionNotFound,
			pulsar.ConsumerNotFound, pulsar.TransactionNoFoundError), Code: mq.CodeNotFound},
	}
	auth := []driverkit.Rule{
		{Match: matchResult(pulsar.AuthenticationError, pulsar.ErrorGettingAuthenticationData), Code: mq.CodeUnauthorized},
		{Match: matchResult(pulsar.AuthorizationError), Code: mq.CodeForbidden},
	}
	conflict := []driverkit.Rule{
		// An Exclusive or Failover subscription that already has its
		// consumer, a producer another process fenced or whose name
		// another producer holds, a terminated topic: different ways of
		// being told "somebody else has this", and every one of them is
		// permanent until the other side goes away rather than something
		// to retry now.
		{Match: matchResult(pulsar.ConsumerBusy), Code: mq.CodeConflict},
		{Match: matchResult(pulsar.ProducerFenced), Code: mq.CodeConflict},
		{Match: refusedAs(producerBusy), Code: mq.CodeConflict},
		{Match: matchResult(pulsar.TopicTerminated), Code: mq.CodeConflict},
		{Match: matchResult(pulsar.InvalidStatus), Code: mq.CodeConflict},
	}
	closed := []driverkit.Rule{
		{Match: matchResult(pulsar.AlreadyClosedError, pulsar.ConsumerClosed, pulsar.ProducerClosed), Code: mq.CodeClosed},
	}
	unsupported := []driverkit.Rule{
		{Match: matchResult(pulsar.OperationNotSupported, pulsar.UnsupportedVersionError,
			pulsar.TransactionCoordinatorNotEnabled), Code: mq.CodeUnsupported},
		// A seek on a multi-topic, pattern or partitioned consumer is
		// refused by the client itself and can never succeed on that
		// handle, so it is a missing feature rather than a failed call.
		{Match: matchResult(pulsar.SeekFailed), Code: mq.CodeUnsupported},
	}
	quota := []driverkit.Rule{
		{Match: matchResult(pulsar.ProducerBlockedQuotaExceededError,
			pulsar.ProducerBlockedQuotaExceededException), Code: mq.CodeResourceExhausted},
		{Match: matchResult(pulsar.TooManyLookupRequestException,
			pulsar.MaxConcurrentOperationsReached), Code: mq.CodeThrottled, Retryable: true},
	}
	link := []driverkit.Rule{
		{Match: matchResult(pulsar.ConnectError, pulsar.LookupError, pulsar.ReadError,
			pulsar.NotConnectedError, pulsar.BrokerMetadataError, pulsar.BrokerPersistenceError,
			pulsar.ServiceUnitNotReady, pulsar.ConsumerNotInitialized,
			pulsar.ProducerNotInitialized), Code: mq.CodeUnavailable, Retryable: true},
	}
	// A publication's timeout is ambiguous and a lookup's is not, so the two
	// timeouts are two rules in two sets rather than one rule in Fallback.
	publishTimeout := []driverkit.Rule{
		{Match: matchResult(pulsar.TimeoutError), Code: mq.CodeDeadlineExceeded, Ambiguous: true},
	}
	plainTimeout := []driverkit.Rule{
		{Match: matchResult(pulsar.TimeoutError), Code: mq.CodeDeadlineExceeded, Retryable: true},
	}
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(backpressure, shape, closed, conflict, quota, notFound, auth, publishTimeout, link),
			mq.OpFetch:   concat(closed, shape, conflict, notFound, auth, plainTimeout, link),
			mq.OpOpen:    concat(closed, conflict, unsupported, shape, notFound, auth, quota, plainTimeout, link),
			mq.OpClose:   concat(closed, plainTimeout, link),
			mq.OpAck:     concat(closed, unsupported, shape, notFound, plainTimeout, link),
			mq.OpNack:    concat(closed, unsupported, shape, notFound, plainTimeout, link),
			mq.OpExtend:  concat(closed, unsupported, notFound, plainTimeout, link),
			mq.OpAdmin:   concat(notFound, auth, conflict, shape, unsupported, quota, plainTimeout, link),
		},
		Fallback: concat(closed, shape, notFound, auth, conflict, unsupported, quota, plainTimeout, link),
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
