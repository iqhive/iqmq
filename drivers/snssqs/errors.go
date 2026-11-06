package snssqs

import (
	"context"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// newErrorMapper builds this driver rule table for the SNS half.
//
// The consume half maps through `drivers/sqs`'s table, which is the point of
// composing that driver rather than reimplementing it: a receipt handle the
// service has invalidated becomes ErrLeaseLost there, once.
//
// The rules are per operation because retryability is (the specification forbids inferring
// it from a native error type alone), and the ordering inside each set is
// specific before general. What the mapper never does is throw the native
// error away: the result wraps it, so errors.As still reaches a
// *types.NotFoundException or a *smithy.OperationError.
//
// Two rules are worth reading in full.
//
// *types.NotFoundException is SNS's answer for a topic ARN that names nothing
// AND for a subscription ARN that names nothing, and it maps to CodeNotFound
// rather than to a retryable failure: neither will come into existence by
// being asked for again. A topic deleted underneath a publisher shows up here.
//
// A throttle is retryable and NOT ambiguous on an administrative call, and it
// is AMBIGUOUS on a publication: the service answers ThrottledException after
// it has read the request, so a publication that was throttled may or may not
// have been accepted — and on a fan-out an accepted one is already on its way
// to every queue. The specification makes that distinction the driver's job.
//
// An answer the SDK could not decode is read by its HTTP status, last
// ([undecodedReply]). A load balancer, an API gateway or a reverse proxy in
// front of SNS answers 502 or 504 with a page of its own, or 500 or 503 with
// no body, and a success can come back cut off; no code rule matches any of
// them. Unclaimed, they fell through to CodeUnknown, definite and not
// retryable, so a publication SNS had accepted and fanned out read as one
// that never happened, and an outage that passes read as a permanent failure.
// They are mapped as InternalError is: retryable, and ambiguous on a
// publication. Because [sendAttempts.record] reads each attempt by the
// publication rules, such an answer to an attempt the SDK then repeated makes
// the publication ambiguous whatever the last attempt met, a lookup that
// failed included.
func newErrorMapper() *driverkit.ErrorMapper {
	notFound := driverkit.Rule{Match: matchAny(
		new(*types.NotFoundException), new(*types.ResourceNotFoundException),
	), Code: mq.CodeNotFound}
	badRequest := driverkit.Rule{Match: matchAny(
		new(*types.InvalidParameterException), new(*types.InvalidParameterValueException),
		new(*types.InvalidBatchEntryIdException), new(*types.BatchEntryIdsNotDistinctException),
		new(*types.EmptyBatchRequestException), new(*types.TooManyEntriesInBatchRequestException),
		new(*types.ValidationException),
	), Code: mq.CodeInvalidArgument}
	unauthorized := driverkit.Rule{Match: matchAny(
		new(*types.InvalidSecurityException), new(*types.KMSAccessDeniedException),
	), Code: mq.CodeUnauthorized}
	forbidden := driverkit.Rule{Match: matchAny(
		new(*types.AuthorizationErrorException),
	), Code: mq.CodeForbidden}
	forbiddenCode := driverkit.Rule{Match: matchCode(
		"AccessDenied", "AccessDeniedException", "AuthorizationError",
		"MissingAuthenticationToken", "SignatureDoesNotMatch",
		"InvalidClientTokenId", "UnrecognizedClientException",
	), Code: mq.CodeForbidden}
	tooLarge := driverkit.Rule{Match: matchAny(
		new(*types.BatchRequestTooLongException),
	), Code: mq.CodeMessageTooLarge}
	exhausted := driverkit.Rule{Match: matchAny(
		new(*types.SubscriptionLimitExceededException), new(*types.TopicLimitExceededException),
		new(*types.FilterPolicyLimitExceededException), new(*types.ReplayLimitExceededException),
		new(*types.TagLimitExceededException),
	), Code: mq.CodeResourceExhausted}
	conflict := driverkit.Rule{Match: matchAny(
		new(*types.ConcurrentAccessException), new(*types.StaleTagException),
		new(*types.InvalidStateException),
	), Code: mq.CodeConflict}
	kmsDown := driverkit.Rule{Match: matchAny(
		new(*types.KMSDisabledException), new(*types.KMSInvalidStateException),
		new(*types.KMSNotFoundException), new(*types.KMSOptInRequired),
	), Code: mq.CodeUnavailable}
	// A throttled KMS key is a throttle, not a key that is unusable, and it
	// is definite on every operation, a publication included: SNS needs the
	// key before it can accept the message, which is why the rest of the KMS
	// family is definite as well.
	kmsThrottle := driverkit.Rule{Match: matchAny(
		new(*types.KMSThrottlingException),
	), Code: mq.CodeThrottled, Retryable: true}
	// An endpoint SNS has disabled is a subscriber that is gone, which is
	// neither this publisher's fault nor something a retry fixes.
	gone := driverkit.Rule{Match: matchAny(
		new(*types.EndpointDisabledException), new(*types.PlatformApplicationDisabledException),
	), Code: mq.CodeUnavailable}
	serverTyped := driverkit.Rule{Match: matchAny(
		new(*types.InternalErrorException),
	), Code: mq.CodeUnavailable, Retryable: true}
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
	publishTyped := driverkit.Rule{Match: matchAny(
		new(*types.InternalErrorException),
	), Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true}
	replyRetry := driverkit.Rule{Match: undecodedReply, Code: mq.CodeUnavailable, Retryable: true}
	replyPublish := driverkit.Rule{Match: undecodedReply, Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true}

	common := []driverkit.Rule{
		notFound, unauthorized, forbidden, forbiddenCode, badRequest,
		tooLarge, exhausted, conflict, kmsDown, kmsThrottle, gone,
	}
	publish := append(append([]driverkit.Rule{}, common...), publishTyped, throttlePublish, serverPublish, replyPublish)
	other := append(append([]driverkit.Rule{}, common...), serverTyped, throttleRetry, serverRetry, replyRetry)
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: publish,
			mq.OpOpen:    other,
			mq.OpClose:   other,
			mq.OpAdmin:   other,
		},
		Fallback: other,
	}
}

// sendAttempts watches the attempts the SDK makes at one publication, so that
// its failure is classified by all of them and not by the last alone. It is
// drivers/sqs's, copied: this module builds against a released drivers/sqs.
//
// The SDK's default retryer repeats a request after a connection reset, a
// throttle or a server error, so the attempt whose failure the call returns
// may not be the one SNS accepted. driverkit.ClassifyNet calls a name that did
// not resolve definite, and for the attempt that looked it up it is: that
// attempt wrote nothing. But when SNS accepted the first attempt and the
// connection was reset before its answer came back, the retry dials again,
// and its lookup can fail during the same incident. The publication was then
// reported as a definite, retryable failure while its copies were on their
// way to every subscribed queue, and a caller that repeated it sent each
// queue a second copy. A refusal the last attempt was given read as definite
// the same way.
//
// One is made per call. The SDK makes a call's attempts one after another on
// the caller's goroutine, so unknown needs no lock.
type sendAttempts struct {
	errs *driverkit.ErrorMapper
	// unknown reports that an attempt failed the way a publication SNS may
	// have accepted fails: the publication rules call its failure ambiguous.
	unknown bool
}

// option is the per-call SDK option that adds record to the request's stack.
// It is per call because a borrowed client's own options are not this
// driver's to change.
func (a *sendAttempts) option(o *sns.Options) {
	o.APIOptions = append(o.APIOptions, a.add)
}

// add puts record at the end of the finalize step. The SDK's retry loop is
// earlier in that step, so record runs once for every attempt.
func (a *sendAttempts) add(stack *middleware.Stack) error {
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("iqmq/snssqs.sendAttempts", a.record), middleware.After)
}

// record classifies one attempt's failure by the rules a publication's
// failure is classified by, as if it had been the only attempt.
func (a *sendAttempts) record(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
	out, md, err := next.HandleFinalize(ctx, in)
	if err != nil && mq.IsAmbiguous(a.errs.MapNet(mq.OpPublish, "", "", err, true)) {
		a.unknown = true
	}
	return out, md, err
}

// classify is mapped, the call's failure as the last attempt's error maps,
// made ambiguous when an attempt may have been accepted. The code and the
// retryability stay the last attempt's.
func (a *sendAttempts) classify(mapped error) error {
	var oe *mq.OpError
	if a.unknown && errors.As(mapped, &oe) {
		oe.Ambiguous = true
	}
	return mapped
}

// matchAny returns a predicate matching any of the service's typed errors, by
// errors.As against each of the pointers it is given.
//
// The pointers are the destinations errors.As needs and they are allocated
// once at construction: building a destination per call would allocate on
// every failure, and the rule table is shared by every operation.
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
// reports a whole class of failures — an internal error, an authorisation
// refusal — with no generated type at all: the SDK surfaces them as a
// *smithy.GenericAPIError whose only distinguishing feature is its code
// string. docs/drivers/snssqs.md's error table says which mappings rest on
// one.
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
// It reads both surfaces: the generated *types.ThrottledException and the
// smithy codes the service uses for the same condition on operations that do
// not declare it.
func throttled(err error) bool {
	var typed *types.ThrottledException
	if errors.As(err, &typed) {
		return true
	}
	return matchCode(
		"Throttled", "ThrottledException", "RequestThrottled", "ThrottlingException",
		"Throttling", "TooManyRequestsException", "RequestThrottledException", "SlowDown",
	)(err)
}

// undecodedReply reports whether the error is an HTTP answer that no code rule
// can read: a 500, 502, 503 or 504, whatever its body said, or a success
// whose body the SDK could not decode, such as one cut off on the way back.
// The four server errors are the ones the SDK's standard retryer retries,
// because a repeat can get past them; a 501 or a 505 is a refusal of the
// request's form, and an undecodable 4xx page is the refusal of something in
// front of SNS, so both are left as they were. It is the last rule of each
// set, so it decides only an answer no code rule recognised. It is
// drivers/sqs's, copied.
//
// A body read that a transport failure ended is left to
// driverkit.ClassifyNet, which owned it before this rule: the caller's
// context ending, Config.APITimeout passing, a read timeout, a reset or an
// EOF while the answer arrived. The SDK wraps each of those in the same
// response error, and the rule, consulted first, reported a cancelled call as
// the service being unavailable, and on an administrative call as retryable.
func undecodedReply(err error) bool {
	if code, _, _ := driverkit.ClassifyNet(err, false); code != mq.CodeUnknown {
		return false
	}
	re, ok := errors.AsType[*smithyhttp.ResponseError](err)
	if !ok {
		return false
	}
	switch status := re.HTTPStatusCode(); {
	case status == http.StatusInternalServerError, status == http.StatusBadGateway,
		status == http.StatusServiceUnavailable, status == http.StatusGatewayTimeout:
		return true
	case status >= 200 && status < 300:
		_, undecodable := errors.AsType[*smithy.DeserializationError](err)
		return undecodable
	}
	return false
}

// batchEntryError maps one failed PublishBatch entry to the model.
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
	case "NotFound", "NotFoundException":
		c = mq.CodeNotFound
	case "BatchRequestTooLong", "InvalidParameter", "InvalidParameterValue", "ValidationException":
		if code == "BatchRequestTooLong" {
			c = mq.CodeMessageTooLarge
		} else {
			c = mq.CodeInvalidArgument
		}
	case "AuthorizationError", "AccessDenied":
		c = mq.CodeForbidden
	case "Throttled", "ThrottledException", "ThrottlingException":
		c, retryable = mq.CodeThrottled, true
	case "KMSThrottling", "KMSThrottlingException":
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
		return "sns batch entry: " + e.code
	}
	return "sns batch entry " + e.code + ": " + e.message
}

// ErrorCode is the service's own code, so a caller can match on it through the
// smithy interface the rest of the SDK uses.
func (e *entryError) ErrorCode() string { return e.code }

// ErrorMessage is the service's own message.
func (e *entryError) ErrorMessage() string { return e.message }

// ErrorFault says whose fault the service considered it.
func (e *entryError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

// compile-time proof that a failed entry is a smithy API error like any other.
var _ smithy.APIError = (*entryError)(nil)

func collapse(errs []error) error { return driverkit.CollapseBatch(errs) }
