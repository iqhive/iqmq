package sqs

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// newErrorMapper builds this driver rule table.
//
// The rules are per operation because retryability is (the specification forbids inferring
// it from a native error type alone), and the ordering inside each set is
// specific before general. What the mapper never does is throw the native
// error away: the result wraps it, so errors.As still reaches a
// *types.QueueDoesNotExist or a *smithy.OperationError and a caller who needs
// the service's own answer has it.
//
// Two rules are worth reading in full.
//
// ReceiptHandleIsInvalid and MessageNotInflight are the service saying "the
// lease you are settling is not yours any more", and they map to ErrLeaseLost
// rather than to a retryable failure. A retry could not succeed — the handle
// will never become valid again — and reporting it as a plain failure would
// leave the core believing the delivery is still settleable.
//
// A throttle is retryable and NOT ambiguous on a receive and on an
// administrative call, and it is AMBIGUOUS on a publication: the service
// answers RequestThrottled after it has read the request, so a publication
// that was throttled may or may not have been accepted. The specification makes that
// distinction the driver's job, and ClassifyNet's sentAlready flag is the
// other half of it.
func newErrorMapper() *driverkit.ErrorMapper {
	notFound := driverkit.Rule{Match: matchAny(
		new(*types.QueueDoesNotExist), new(*types.ResourceNotFoundException),
	), Code: mq.CodeNotFound}
	badRequest := driverkit.Rule{Match: matchAny(
		new(*types.InvalidAddress), new(*types.InvalidAttributeName),
		new(*types.InvalidAttributeValue), new(*types.InvalidIdFormat),
		new(*types.InvalidBatchEntryId), new(*types.BatchEntryIdsNotDistinct),
		new(*types.EmptyBatchRequest), new(*types.TooManyEntriesInBatchRequest),
	), Code: mq.CodeInvalidArgument}
	unauthorized := driverkit.Rule{Match: matchAny(
		new(*types.InvalidSecurity), new(*types.KmsAccessDenied),
	), Code: mq.CodeUnauthorized}
	forbidden := driverkit.Rule{Match: matchCode(
		"AccessDenied", "AccessDeniedException", "AuthorizationError",
		"MissingAuthenticationToken", "SignatureDoesNotMatch",
		"InvalidClientTokenId", "UnrecognizedClientException",
	), Code: mq.CodeForbidden}
	unsupported := driverkit.Rule{Match: matchAny(
		new(*types.UnsupportedOperation),
	), Code: mq.CodeUnsupported}
	leaseLost := driverkit.Rule{Match: matchAny(
		new(*types.ReceiptHandleIsInvalid), new(*types.MessageNotInflight),
	), Code: mq.CodeLeaseLost}
	tooLarge := driverkit.Rule{Match: matchAny(
		new(*types.BatchRequestTooLong), new(*types.InvalidMessageContents),
	), Code: mq.CodeMessageTooLarge}
	overLimit := driverkit.Rule{Match: matchAny(
		new(*types.OverLimit),
	), Code: mq.CodeResourceExhausted, Retryable: true}
	conflict := driverkit.Rule{Match: matchAny(
		new(*types.QueueNameExists), new(*types.QueueDeletedRecently),
		new(*types.PurgeQueueInProgress),
	), Code: mq.CodeConflict}
	kmsDown := driverkit.Rule{Match: matchAny(
		new(*types.KmsDisabled), new(*types.KmsInvalidState),
		new(*types.KmsNotFound), new(*types.KmsOptInRequired),
		new(*types.KmsInvalidKeyUsage),
	), Code: mq.CodeUnavailable}
	// A throttled KMS key is a throttle, not a key that is unusable, and it
	// is definite on every operation, a publication included: the service
	// needs the key before it can store the message, which is why the rest
	// of the KMS family is definite as well.
	kmsThrottle := driverkit.Rule{Match: matchAny(
		new(*types.KmsThrottled),
	), Code: mq.CodeThrottled, Retryable: true}
	throttleRetry := driverkit.Rule{Match: throttled, Code: mq.CodeThrottled, Retryable: true}
	throttlePublish := driverkit.Rule{
		Match: throttled, Code: mq.CodeThrottled, Retryable: true, Ambiguous: true,
	}
	serverRetry := driverkit.Rule{Match: matchCode(
		"InternalError", "InternalFailure", "ServiceUnavailable", "RequestTimeout",
	), Code: mq.CodeUnavailable, Retryable: true}
	serverPublish := driverkit.Rule{Match: matchCode(
		"InternalError", "InternalFailure", "ServiceUnavailable", "RequestTimeout",
	), Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true}

	common := []driverkit.Rule{
		notFound, leaseLost, unauthorized, forbidden, unsupported,
		badRequest, tooLarge, overLimit, conflict, kmsDown, kmsThrottle,
	}
	publish := append(append([]driverkit.Rule{}, common...), throttlePublish, serverPublish)
	other := append(append([]driverkit.Rule{}, common...), throttleRetry, serverRetry)
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: publish,
			mq.OpFetch:   other,
			mq.OpAck:     other,
			mq.OpNack:    other,
			mq.OpExtend:  other,
			mq.OpOpen:    other,
			mq.OpClose:   other,
			mq.OpAdmin:   other,
		},
		Fallback: other,
	}
}

// matchAny returns a predicate matching any of the service's typed errors,
// by errors.As against each of the pointers it is given.
//
// The pointers are the destinations errors.As needs, and they are allocated
// once at construction: a rule's Match is on the error path rather than the
// hot path, but building a destination per call would allocate on every
// failure and the rule table is shared by every operation.
func matchAny(targets ...any) func(error) bool {
	return func(err error) bool {
		for _, t := range targets {
			if errors.As(err, t) {
				return true
			}
		}
		return false
	}
}

// matchCode returns a predicate matching a smithy API error code.
//
// It is the last resort the specification allows and it is here because the service
// reports a whole class of failures — throttling, an internal error, an
// authorisation refusal — with no generated type at all: the SDK surfaces them
// as a *smithy.GenericAPIError whose only distinguishing feature is its code
// string. docs/drivers/sqs.md's error table says which of this driver's
// mappings rest on one.
func matchCode(codes ...string) func(error) bool {
	return func(err error) bool {
		var api smithy.APIError
		if !errors.As(err, &api) {
			return false
		}
		got := api.ErrorCode()
		for _, c := range codes {
			if got == c {
				return true
			}
		}
		return false
	}
}

// throttled reports whether the error is the service asking for less load.
//
// It reads both surfaces: the generated *types.RequestThrottled and the
// smithy codes the service uses for the same condition on operations that do
// not declare it.
func throttled(err error) bool {
	var typed *types.RequestThrottled
	if errors.As(err, &typed) {
		return true
	}
	return matchCode(
		"RequestThrottled", "ThrottlingException", "Throttling",
		"TooManyRequestsException", "RequestThrottledException",
		"ProvisionedThroughputExceededException", "SlowDown",
	)(err)
}

// batchEntryError maps one failed batch entry to the model.
//
// A batch response reports a per-entry code and a sender-fault flag rather
// than a typed error, so the entry's outcome cannot go through the rule table
// at all. This is the translation, and its shape is the part that matters:
// SenderFault false means the SERVICE failed, which is retryable, and
// SenderFault true means the request was wrong, which is not. Reading them the
// same way would make a driver retry a malformed entry for ever, or give up on
// a transient failure.
func batchEntryError(op, topic, code, message string, senderFault bool) error {
	var c mq.ErrorCode
	retryable := false
	switch code {
	case "ReceiptHandleIsInvalid", "AWS.SimpleQueueService.MessageNotInflight":
		// The lease is gone: the message has been redelivered to somebody
		// else, or the handle is from a receive this consumer has
		// superseded. A retry can never succeed.
		return mq.ErrLeaseLost
	case "AWS.SimpleQueueService.NonExistentQueue":
		c = mq.CodeNotFound
	case "AWS.SimpleQueueService.BatchRequestTooLong", "InvalidMessageContents":
		c = mq.CodeMessageTooLarge
	case "AWS.SimpleQueueService.UnsupportedOperation":
		c = mq.CodeUnsupported
	case "RequestThrottled", "ThrottlingException":
		c, retryable = mq.CodeThrottled, true
	case "KmsThrottled", "KMS.ThrottlingException":
		// A throttled KMS key. The service reports it as the sender's
		// fault, but a retry can succeed.
		c, retryable = mq.CodeThrottled, true
	case "InternalError", "InternalFailure", "ServiceUnavailable":
		c, retryable = mq.CodeUnavailable, true
	default:
		if senderFault {
			c = mq.CodeInvalidArgument
		} else {
			c, retryable = mq.CodeUnavailable, true
		}
	}
	return &mq.OpError{
		Driver:    DriverName,
		Operation: op,
		Topic:     topic,
		Code:      c,
		Retryable: retryable,
		Err:       &entryError{code: code, message: message},
	}
}

// entryError is the native error a failed batch entry carries, so that
// errors.As still reaches the service's own code and message.
type entryError struct {
	code    string
	message string
}

// Error renders the service's answer for this entry. It carries no payload,
// which the specification forbids: the code and the service's message are the whole thing.
func (e *entryError) Error() string {
	if e.message == "" {
		return "sqs batch entry: " + e.code
	}
	return "sqs batch entry " + e.code + ": " + e.message
}

// ErrorCode is the service's own code, so a caller can match on it through
// the smithy interface the rest of the SDK uses.
func (e *entryError) ErrorCode() string { return e.code }

// ErrorMessage is the service's own message.
func (e *entryError) ErrorMessage() string { return e.message }

// ErrorFault says whose fault the service considered it.
func (e *entryError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

// compile-time proof that a failed entry is a smithy API error like any other.
var _ smithy.APIError = (*entryError)(nil)

func collapse(errs []error) error { return driverkit.CollapseBatch(errs) }
