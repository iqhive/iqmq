package gcppubsub

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"github.com/iqhive/iqmq/internal/errcode"
)

// newErrorMapper builds this driver rule table.
//
// Pub/Sub speaks gRPC, so almost every failure arrives as a status code and the
// rules are a code table. The rules are per operation because retryability is (the specification forbids inferring it from a native error type alone), and what the
// mapper never does is throw the native error away: the result wraps it, so
// errors.As still reaches a *status.Status and a caller who needs the service's
// own answer has it.
//
// Three rules are worth reading in full.
//
// [psapi.ErrAckIDNotReachable] is the service saying "the acknowledgement you
// are sending names a lease that is not yours any more", and it maps to
// ErrLeaseLost rather than to a retryable failure: a retry cannot succeed,
// because the acknowledgement identifier will never become valid again, and
// reporting it as a plain failure would leave the core believing the delivery
// is still settleable.
//
// [psapi.ErrOrderingPaused] is the library refusing a publication because an
// EARLIER publication of the same ordering key failed. It is retryable —
// the sink resumes the key and the caller's retry then reaches the service —
// and it is not ambiguous, because a paused publication was never sent.
//
// RESOURCE_EXHAUSTED, UNAVAILABLE, DEADLINE_EXCEEDED and ABORTED are
// retryable everywhere and AMBIGUOUS on a publication only: the service
// answers them after it has read the request, so a publication that got one
// may or may not have been accepted. On a fetch or a settlement the same codes
// are a plain retry, because a fetch that failed delivered nothing and a
// settlement that failed leaves the message where it was. A Reject's copy to
// the dead-letter topic is a publication, so it is mapped by the publication
// rules although it is reported as the negative settlement
// ([source.mapDeadLetter]).
func newErrorMapper() *driverkit.ErrorMapper {
	leaseLost := driverkit.Rule{
		Match: driverkit.MatchIs(psapi.ErrAckIDNotReachable),
		Code:  mq.CodeLeaseLost,
	}
	orderingPaused := driverkit.Rule{
		Match: driverkit.MatchIs(psapi.ErrOrderingPaused),
		Code:  mq.CodeUnavailable, Retryable: true,
	}
	notFound := driverkit.Rule{
		Match: matchCodes(codes.NotFound), Code: mq.CodeNotFound,
	}
	conflict := driverkit.Rule{
		Match: matchCodes(codes.AlreadyExists), Code: mq.CodeConflict,
	}
	unauthenticated := driverkit.Rule{
		Match: matchCodes(codes.Unauthenticated), Code: mq.CodeUnauthorized,
	}
	forbidden := driverkit.Rule{
		Match: matchCodes(codes.PermissionDenied), Code: mq.CodeForbidden,
	}
	unimplemented := driverkit.Rule{
		Match: matchCodes(codes.Unimplemented), Code: mq.CodeUnsupported,
	}
	// The service reports a message over its ten-megabyte limit as
	// INVALID_ARGUMENT, so the size case is separated by the driver's own
	// pre-flight check (mapping.go) rather than by the status code; what is
	// left here is a genuine argument error.
	badRequest := driverkit.Rule{
		Match: matchCodes(codes.InvalidArgument, codes.OutOfRange),
		Code:  mq.CodeInvalidArgument,
	}
	precondition := driverkit.Rule{
		Match: matchCodes(codes.FailedPrecondition), Code: mq.CodeConflict,
	}
	cancelled := driverkit.Rule{
		Match: matchCodes(codes.Canceled), Code: mq.CodeCanceled,
	}
	transient := []codes.Code{
		codes.ResourceExhausted, codes.Unavailable,
		codes.DeadlineExceeded, codes.Aborted, codes.Internal, codes.Unknown,
	}
	retry := driverkit.Rule{
		Match: matchCodes(transient...), Code: mq.CodeUnavailable, Retryable: true,
	}
	retryAmbiguous := driverkit.Rule{
		Match: matchCodes(transient...),
		Code:  mq.CodeUnavailable, Retryable: true, Ambiguous: true,
	}
	exhausted := driverkit.Rule{
		Match: matchCodes(codes.ResourceExhausted),
		Code:  mq.CodeResourceExhausted, Retryable: true,
	}

	common := []driverkit.Rule{
		leaseLost, orderingPaused, notFound, conflict,
		unauthenticated, forbidden, unimplemented,
		badRequest, precondition, cancelled,
	}
	publish := append(append([]driverkit.Rule{}, common...), retryAmbiguous)
	fetch := append(append([]driverkit.Rule{}, common...), exhausted, retry)
	other := append(append([]driverkit.Rule{}, common...), retry)
	return &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: publish,
			mq.OpFetch:   fetch,
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

// matchCodes returns a predicate matching any of the gRPC status codes.
//
// status.Code unwraps a *status.Error and answers codes.Unknown for anything
// else, so the predicate is asked whether the error IS a status first: an
// ordinary error is not a service answer and must not be read as one.
func matchCodes(want ...codes.Code) func(error) bool {
	return func(err error) bool {
		st, ok := status.FromError(err)
		if !ok || st == nil {
			// status.FromError reports ok for a nil error and for an error
			// implementing GRPCStatus. Anything else — a wrapped sentinel,
			// a context error — is left to the mapper's own classification.
			var se interface{ GRPCStatus() *status.Status }
			if !errors.As(err, &se) {
				return false
			}
			st = se.GRPCStatus()
		}
		for _, c := range want {
			if st.Code() == c {
				return true
			}
		}
		return false
	}
}

// The driver's own refusals. Most are reachable before any I/O; the start
// position's (errNoHistory, errNoSeek, errArmWithoutRetention) come after the
// open has read the subscription or the topic, and errSeekLeftBehind after a
// create or a seek. The specification names no sentinel for these and
// errors.Is matches by code, so a caller compares mq.CodeOf(err).
var (
	errNilClient = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the client is nil")
	errNilRegistrar = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: Register was given a nil registrar")
	errNoScheduledPublish = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: Pub/Sub has no publication timer: a message is deliverable as soon as the service has stored it, so Delay and DeliverAt are refused before any I/O rather than silently ignored")
	errNoTTL = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: Pub/Sub expires messages by the SUBSCRIPTION's retention period and has no per-message time to live")
	errNoPriority = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: Pub/Sub delivers in arrival order within an ordering key and has no message priority")
	errNoPartition = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: Pub/Sub has no partitions; the ordering key is the ordering unit and is taken from Message.Key")
	errNoDedup = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: Pub/Sub does not deduplicate publications; a DedupKey would be accepted and ignored, which is worse than refusing it")
	errOrderingKeyTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the ordering key is longer than the service's 1024 bytes")
	errNoSeekBySequence = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: Pub/Sub has no per-message sequence number to seek to; a seek names a timestamp or a snapshot")
	errNoSeek = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: seeking needs messages kept to seek back over: Config.RetainAcked for the subscription's acknowledged messages, or message retention on the topic itself (Config.TopicRetention gives it to topics this driver creates), and this subscription has neither")
	errNoHistory = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: StartEarliest asks a new subscription for the topic's history, and this topic retains none: a Pub/Sub subscription begins at its own creation and reaches further back only by seeking into the topic's message retention. Set Config.TopicRetention (the topicretention DSN parameter) for topics this driver creates, or give the topic message retention where it is provisioned; or open the subscription with StartLatest")
	errSeekLeftBehind = errcode.New(errcode.Conflict,
		"iqmq/gcppubsub: the subscription for StartEarliest may exist without its seek into the topic's history: its create or its seek failed, and it was not removed (the deletion failed, or the create answered that it already existed, so it may be another consumer's). A subscription like that reads only from its own creation, and one that exists keeps its own position, so opening it at StartEarliest again would read none of the history: delete it, or open it with StartAt, first")
	errArmWithoutRetention = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: this client arm cannot create or read a topic's message retention, which Config.TopicRetention, StartEarliest on a new subscription and StartAt without Config.RetainAcked need. drivers/gcppubsub/v1compat up to v1.1.1 does not implement it: upgrade drivers/gcppubsub/v1compat to the release that matches this drivers/gcppubsub, or open with the v2 client")
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: this subscription has no dead-letter policy, so there is nowhere to reject a message to; set Config.DeadLetterTopic and MaxDeliveryAttempts")
	errNoDrop = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: a negative settlement cannot destroy a message on this broker; a nack makes it deliverable again and the subscription's dead-letter policy is what ends its life")
	errNackAction = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the negative settlement names no action of the message contract")
	errExtendZero = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: an extension of zero or less would end the lease, which is what a Nack means")
	errExtendBeyondBudget = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: the extension is longer than what is left of the client library's automatic extension budget (Config.MaxExtension), and this driver cannot send a ModifyAckDeadline of its own: the acknowledgement identifier the request needs is private to the client library's message type")
	errDelayWithDeadLetter = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: a redelivery delay names nothing on a dead-letter settlement: the message goes to the dead-letter topic and the original is acknowledged, so there is no redelivery to hold back")
	errDelayTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the redelivery delay is longer than what is left of the automatic extension budget, so the in-process hold could not keep the lease alive for it")
	errTooManyAttributes = errcode.New(errcode.MessageTooLarge,
		"iqmq/gcppubsub: the message needs more attributes than the service's hundred")
	errAttributeKeyTooLong = errcode.New(errcode.MessageTooLarge,
		"iqmq/gcppubsub: a header key is longer than the service's 256-byte attribute-name limit")
	errAttributeValueTooLong = errcode.New(errcode.MessageTooLarge,
		"iqmq/gcppubsub: a header value is longer than the service's 1024-byte attribute-value limit")
	errAttributeKeyInvalid = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: a header key is not valid UTF-8, and a Pub/Sub attribute name is a string")
	errAttributeValueBinary = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: a Pub/Sub attribute value is a STRING, so a binary header value has no native representation; MetadataAdaptive carries it in the portable envelope")
	errReservedAttribute = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the header key collides with one of this driver's reserved iqmq- attributes, or with the goog- prefix the service reserves for itself")
	errMessageTooLarge = errcode.New(errcode.MessageTooLarge,
		"iqmq/gcppubsub: the message is larger than the service's ten megabytes")
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the capability table does not mention every feature of the inventory")
	errWildcard = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: Pub/Sub has no topic patterns: a subscription names exactly one topic, and a server-side filter (Config.Filter) selects on a message's attributes rather than on the topic name")
	errUnknownTopic = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the topic name is empty")
	errTopicTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the derived topic identifier is longer than the service's 255 characters")
	errSubTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the derived subscription identifier is longer than the service's 255 characters")
	errNoSubscription = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: a source needs a subscription name; a Pub/Sub subscription is a named resource and two consumers of one topic with no name would be one subscription or two, with nothing to say which")
	errSubscriptionMissing = errcode.New(errcode.NotFound,
		"iqmq/gcppubsub: the subscription does not exist, and this driver creates one only when Config.AutoCreate is set; a streaming pull on a missing subscription receives nothing")
	errEnvelopeNeeded = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: the message carries metadata Pub/Sub attributes cannot represent (a binary header value, a repeated header key, or more attributes than the service takes); MetadataAdaptive or MetadataPortableEnvelope carries it in the portable envelope")
	errMetadataValueTooLong = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: the key, the identifier or a typed property is longer than the service's 1024-byte attribute-value limit; MetadataLenientDrop drops only headers, and MetadataAdaptive or MetadataPortableEnvelope carries it in the portable envelope")
)

// The DSN's and the administration surface's refusals.
var (
	errWrongScheme = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the DSN's scheme is not gcppubsub")
	errDSNHosts = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the DSN names more than one host; its authority is one endpoint, or one project when there is no path")
	errDSNProjectPort = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the DSN names a project with a port; a project has none, and an endpoint names its project in the path (gcppubsub://host:port/project)")
	errDSNProjectTwice = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the DSN names the project twice, in the authority or the path and in the project parameter")
	errDSNEndpointTwice = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the DSN names the endpoint twice, in the authority and in the endpoint parameter")
	errDSNHost = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the DSN's endpoint host decodes to a byte a gRPC target gives a meaning to -- \"@\", \"/\", \"?\", \"#\", \",\", a backslash, a space or a control character -- so it would dial somewhere the DSN does not name")
	errBadDuration = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: a DSN duration parameter is not a Go duration")
	errBadNumber = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: a DSN numeric parameter is not a number")
	errBadBool = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: a DSN flag parameter is not a boolean")
	errNilMessage = errcode.New(errcode.InvalidArgument,
		"iqmq/gcppubsub: the message is nil")
	errNoStats = errcode.New(errcode.Unsupported,
		"iqmq/gcppubsub: Pub/Sub does not report a subscription's backlog through its own API; the undelivered-message count and the oldest-unacknowledged-message age are Cloud Monitoring metrics, which is a different service with a different credential scope and a sampling delay of minutes")
)
