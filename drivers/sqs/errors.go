package sqs

import (
	"context"
	"crypto/md5" //nolint:gosec // the service's digest of a body is MD5; it checks transit, not integrity against an attacker
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

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
//
// An answer the SDK could not decode is read by its HTTP status, last
// ([undecodedReply]). A load balancer, an API gateway or a reverse proxy in
// front of the service answers 502 or 504 with a page of its own, or 500 or
// 503 with no body, and a success can come back cut off; no code rule matches
// any of them. Unclaimed, they fell through to CodeUnknown, definite and not
// retryable, so a publication the service had stored read as one that never
// happened, and an outage that passes read as a permanent failure. They are
// mapped as InternalError is: retryable, and ambiguous on a publication.
// Because [sendAttempts.record] reads each attempt by the publication rules,
// such an answer to an attempt the SDK then repeated makes the send ambiguous
// whatever the last attempt met, a lookup that failed included.
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
	replyRetry := driverkit.Rule{Match: undecodedReply, Code: mq.CodeUnavailable, Retryable: true}
	replyPublish := driverkit.Rule{Match: undecodedReply, Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true}

	common := []driverkit.Rule{
		notFound, leaseLost, unauthorized, forbidden, unsupported,
		badRequest, tooLarge, overLimit, conflict, kmsDown, kmsThrottle,
	}
	publish := append(append([]driverkit.Rule{}, common...), throttlePublish, serverPublish, replyPublish)
	other := append(append([]driverkit.Rule{}, common...), throttleRetry, serverRetry, replyRetry)
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

// sendAttempts watches the attempts the SDK makes at one send, so that a
// failed publication is classified by all of them and not by the last alone.
//
// The SDK's default retryer repeats a request after a connection reset, a
// throttle or a server error, so the attempt whose failure the call returns
// may not be the one the service stored. driverkit.ClassifyNet calls a name
// that did not resolve definite, and for the attempt that looked it up it is:
// that attempt wrote nothing. But when the service stored the first attempt
// and the connection was reset before its answer came back, the retry dials
// again, and its lookup can fail during the same incident. The publication
// was then reported as a definite, retryable failure while the message was on
// the queue, and a caller that repeated it put it there twice. A refusal the
// last attempt was given read as definite the same way.
//
// One is made per call. The SDK makes a call's attempts one after another on
// the caller's goroutine, so unknown needs no lock. It checks the digests of
// the call's answer as well ([digests]).
type sendAttempts struct {
	errs *driverkit.ErrorMapper
	// unknown reports that an attempt failed the way a publication that may
	// be on the queue fails: the publication rules call its failure
	// ambiguous.
	unknown bool
	digests
}

// option is the per-call SDK option that adds record to the request's stack
// and takes the digest check over. It is per call because a borrowed client's
// own options are not this driver's to change.
func (a *sendAttempts) option(o *sqs.Options) {
	o.APIOptions = append(o.APIOptions, a.add)
	a.digests.option(o)
}

// add puts record at the end of the finalize step. The SDK's retry loop is
// earlier in that step, so record runs once for every attempt.
func (a *sendAttempts) add(stack *middleware.Stack) error {
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("iqmq/sqs.sendAttempts", a.record), middleware.After)
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
// made ambiguous when an attempt may have left the message with the service.
// The code and the retryability stay the last attempt's.
func (a *sendAttempts) classify(mapped error) error {
	var oe *mq.OpError
	if a.unknown && errors.As(mapped, &oe) {
		oe.Ambiguous = true
	}
	return mapped
}

// digests takes over, for one call, the SDK's check of the MD5 digests the
// service returns for the bodies it stored or handed out, so that a digest
// that is not its body's fails that message alone.
//
// The SDK makes the check once the call has succeeded, outside its retry
// loop, and fails the whole call when one digest is wrong. A SendMessageBatch
// the service had stored, every entry of it, was then reported as a definite
// CodeUnknown failure, and a caller that repeated it put every entry on the
// queue twice. A ReceiveMessage whose messages the service had already made
// invisible handed none of them out, and the one message held up every
// receive that returned it.
//
// A client whose own options turn the check off keeps it off: on is false,
// and the driver checks nothing either.
type digests struct {
	// on reports that the client would have checked the digests.
	on bool
}

// option is the per-call SDK option that turns the client's check off for
// the call, remembering whether it was on.
func (d *digests) option(o *sqs.Options) {
	d.on = !o.DisableMessageChecksumValidation
	o.DisableMessageChecksumValidation = true
}

// altered reports whether digest, the service's for a body it stored or
// handed out, is not the MD5 digest of body. An answer without a digest is
// not checked, as the SDK does not check one.
func (d *digests) altered(body, digest *string) bool {
	if !d.on || body == nil || digest == nil {
		return false
	}
	sum := md5.Sum([]byte(*body)) //nolint:gosec // the service's digest is MD5, and it checks transit, not an attacker
	var want [2 * md5.Size]byte
	hex.Encode(want[:], sum[:])
	return string(want[:]) != *digest
}

// bodyAltered is the answer for a message the service stored under a digest
// that is not the digest of the body sent ([digests]). It is AMBIGUOUS: the
// message is on the queue, though perhaps not as it was sent, and a repeat
// would put it there twice. It is not retryable, because nothing says a
// repeat would arrive intact.
func bodyAltered(op, topic, id string) error {
	return &mq.OpError{
		Driver:    DriverName,
		Operation: op,
		Topic:     topic,
		Code:      mq.CodeCorruptMessage,
		Ambiguous: true,
		Err: errors.New("sqs: the service stored message " + id +
			" under an MD5 digest that is not the digest of the body sent"),
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

// undecodedReply reports whether the error is an HTTP answer that no code rule
// can read: a 500, 502, 503 or 504, whatever its body said, or a success
// whose body the SDK could not decode, such as one cut off on the way back.
// The four server errors are the ones the SDK's standard retryer retries,
// because a repeat can get past them; a 501 or a 505 is a refusal of the
// request's form, and an undecodable 4xx page is the refusal of something in
// front of the service, so both are left as they were. It is the last rule of
// each set, so it decides only an answer no code rule recognised.
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
