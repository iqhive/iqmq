package psv2

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// TestASettlementTheServiceRefusedIsNeverASuccess is the one function in this
// package a settlement's integrity rests on.
//
// On an exactly-once subscription the library answers an acknowledgement with a
// STATUS and, for some outcomes, no error at all. A translation that returned
// the error unchanged would hand the driver a nil for
// AcknowledgeStatusPermissionDenied, the driver would report the settlement
// done, and the service would redeliver the message it never acknowledged --
// which is the exactly-once claim broken by the layer that makes it. The specification
// names a settlement the caller believes and the broker does not as a
// disqualifier.
//
// InvalidAckID is the other half and is not the same failure: the identifier is
// gone, so it is a LEASE that was lost rather than a refusal, and it is wrapped
// in [psapi.ErrAckIDNotReachable] so the driver maps it to CodeLeaseLost. An
// operator sent to look at IAM for a redelivered message is looking in the
// wrong place.
func TestASettlementTheServiceRefusedIsNeverASuccess(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		st       pubsub.AcknowledgeStatus
		err      error
		wantNil  bool
		notReach bool
	}{
		{name: "success with no error", st: pubsub.AcknowledgeStatusSuccess, wantNil: true},
		{name: "invalid ack id", st: pubsub.AcknowledgeStatusInvalidAckID, notReach: true},
		{name: "permission denied", st: pubsub.AcknowledgeStatusPermissionDenied},
		{name: "failed precondition", st: pubsub.AcknowledgeStatusFailedPrecondition},
		{name: "other", st: pubsub.AcknowledgeStatusOther},
		{name: "a status this library does not have yet", st: pubsub.AcknowledgeStatus(9999)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ackStatus(tc.st, tc.err)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("a successful settlement reported %v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("the status %v with no error of its own reported SUCCESS; the driver would then report "+
					"the message settled, and the service would redeliver a message it never acknowledged ",
					tc.st)
			}
			if tc.notReach != errors.Is(got, psapi.ErrAckIDNotReachable) {
				t.Errorf("the status %v %s psapi.ErrAckIDNotReachable: an identifier that is gone is a lost lease, "+
					"and every other refusal is the service saying no -- an operator sent to IAM for a lost lease "+
					"is looking in the wrong place", tc.st,
					map[bool]string{true: "does not reach", false: "reaches"}[tc.notReach])
			}
		})
	}
}

// TestTheServicesOwnErrorSurvivesTheTranslation holds the other half of
// [ackStatus]: when the library DID give a reason, that reason travels, because
// the fallback sentinel says only that the settlement failed.
func TestTheServicesOwnErrorSurvivesTheTranslation(t *testing.T) {
	t.Parallel()
	own := errors.New("quota exceeded for project")
	for _, st := range []pubsub.AcknowledgeStatus{
		pubsub.AcknowledgeStatusInvalidAckID,
		pubsub.AcknowledgeStatusPermissionDenied,
		pubsub.AcknowledgeStatusFailedPrecondition,
		pubsub.AcknowledgeStatusOther,
	} {
		if got := ackStatus(st, own); !errors.Is(got, own) {
			t.Errorf("the status %v with a reason reported %v, which does not reach the service's own error: the "+
				"fallback says only that the settlement failed", st, got)
		}
	}
	// errOr is the choice itself, and a fallback that won over a real reason
	// would lose every diagnosis the service gives.
	fallback := errors.New("fallback")
	if got := errOr(own, fallback); !errors.Is(got, own) {
		t.Errorf("errOr chose %v over the service's own error", got)
	}
	if got := errOr(nil, fallback); !errors.Is(got, fallback) {
		t.Errorf("errOr with no error chose %v, want the fallback: a failed settlement must never render as a "+
			"success", got)
	}
}

// TestNotFoundAndAlreadyExistsAreTheTwoStatusesTopologyDependsOn is
// [translateStatus], and the pair is what makes the operations idempotent.
//
// EnsureTopic treats AlreadyExists as success and DeleteTopic treats NotFound
// as success. A translation that confused the two would make a create report
// failure for a topic that is there and a delete report success for one it
// never touched. Everything else travels unchanged, because a gRPC status this
// package does not name is one the driver's own rule table classifies.
func TestNotFoundAndAlreadyExistsAreTheTwoStatusesTopologyDependsOn(t *testing.T) {
	t.Parallel()
	if got := translateStatus(nil); got != nil {
		t.Errorf("translateStatus(nil) is %v, want nil", got)
	}
	notFound := status.Error(codes.NotFound, "no such topic")
	if got := translateStatus(notFound); !errors.Is(got, psapi.ErrNotFound) {
		t.Errorf("a NotFound status became %v, which does not reach psapi.ErrNotFound: a delete would then report "+
			"a failure for a topic that is already gone", got)
	} else if !errors.Is(got, notFound) {
		t.Error("the translation lost the gRPC status, so a caller cannot read the service's own message")
	}
	exists := status.Error(codes.AlreadyExists, "topic exists")
	if got := translateStatus(exists); !errors.Is(got, psapi.ErrAlreadyExists) {
		t.Errorf("an AlreadyExists status became %v, which does not reach psapi.ErrAlreadyExists: an idempotent "+
			"create would then report a failure for a topic that is there", got)
	}
	other := status.Error(codes.ResourceExhausted, "quota")
	got := translateStatus(other)
	if errors.Is(got, psapi.ErrNotFound) || errors.Is(got, psapi.ErrAlreadyExists) {
		t.Errorf("a ResourceExhausted status became %v, which one of the topology sentinels claims: a quota "+
			"failure reported as \"already exists\" is a create an operator believes succeeded", got)
	}
	if !errors.Is(got, other) {
		t.Errorf("a status this package does not name became %v rather than travelling unchanged", got)
	}
}

// TestAPausedOrderingKeyIsItsOwnAnswer is [translatePublish]. The library pauses
// an ordering key after a failure and every later publication on that key fails
// until the key is resumed, which is a state the CALLER has to clear -- so it
// cannot arrive as an ordinary publication failure a retry would fix.
func TestAPausedOrderingKeyIsItsOwnAnswer(t *testing.T) {
	t.Parallel()
	if got := translatePublish(nil); got != nil {
		t.Errorf("translatePublish(nil) is %v, want nil", got)
	}
	paused := pubsub.ErrPublishingPaused{OrderingKey: "tenant-7"}
	got := translatePublish(paused)
	if !errors.Is(got, psapi.ErrOrderingPaused) {
		t.Fatalf("a paused ordering key became %v, which does not reach psapi.ErrOrderingPaused: the caller would "+
			"retry for ever, because only ResumePublish clears it", got)
	}
	var native pubsub.ErrPublishingPaused
	if !errors.As(got, &native) || native.OrderingKey != "tenant-7" {
		t.Errorf("the translation lost the key, so the caller cannot say which one to resume: %v", got)
	}
	ordinary := errors.New("deadline exceeded")
	if errors.Is(translatePublish(ordinary), psapi.ErrOrderingPaused) {
		t.Error("an ordinary publication failure claims a paused ordering key, which would send a caller to " +
			"ResumePublish for a key that was never paused")
	}
}

// TestALeaseIsNeverRoundedDownToNothing is [wholeSeconds], and the direction is
// the whole point.
//
// The service takes whole seconds. Truncating 500ms to 0 is a subscription
// whose messages are immediately redeliverable — no lease at all, and a
// duplicate for every delivery. Rounding up is the safe direction, and only
// zero and below may answer zero.
func TestALeaseIsNeverRoundedDownToNothing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   time.Duration
		want int32
	}{
		{0, 0},
		{-time.Second, 0},
		{time.Nanosecond, 1},
		{500 * time.Millisecond, 1},
		{time.Second, 1},
		{time.Second + time.Nanosecond, 2},
		{90 * time.Second, 90},
		{10 * time.Minute, 600},
	} {
		if got := wholeSeconds(tc.in); got != tc.want {
			t.Errorf("wholeSeconds(%v) = %d, want %d: a sub-second deadline rounded DOWN is a subscription whose "+
				"messages are immediately redeliverable", tc.in, got, tc.want)
		}
	}
}

// TestAQualifiedNameIsPassedThroughAndAShortOneIsQualified holds the naming
// helpers, which are the difference between reaching a resource and creating a
// second one whose name contains the first.
//
// A caller may hand the driver either form: an unqualified "orders" or the
// service's own "projects/p/topics/orders". Qualifying an already-qualified
// name would produce "projects/p/topics/projects/p/topics/orders", which the
// service accepts as a topic nobody is subscribed to.
func TestAQualifiedNameIsPassedThroughAndAShortOneIsQualified(t *testing.T) {
	t.Parallel()
	if got := qualifyTopic("p", "orders"); got != "projects/p/topics/orders" {
		t.Errorf("qualifyTopic(p, orders) = %q", got)
	}
	full := "projects/other/topics/orders"
	if got := qualifyTopic("p", full); got != full {
		t.Errorf("qualifyTopic passed a qualified name through as %q; qualifying it again names a topic nobody is "+
			"subscribed to", got)
	}
	if got := qualifySub("p", "workers"); got != "projects/p/subscriptions/workers" {
		t.Errorf("qualifySub(p, workers) = %q", got)
	}
	fullSub := "projects/other/subscriptions/workers"
	if got := qualifySub("p", fullSub); got != fullSub {
		t.Errorf("qualifySub passed a qualified name through as %q", got)
	}
	// lastSegment is the inverse, and it is what the driver reports back to a
	// caller who asked in the short form.
	if got := lastSegment("projects/p/subscriptions/workers"); got != "workers" {
		t.Errorf("lastSegment = %q, want workers", got)
	}
	if got := lastSegment("workers"); got != "workers" {
		t.Errorf("lastSegment of an unqualified name is %q, want it unchanged", got)
	}
}

// TestAHandleWithNoResourceIsRefusedBeforeAnyRPC holds the two guards in
// [Client.Publisher] and [Client.Subscription].
//
// Neither does any I/O, so an empty name would otherwise become a handle whose
// first publication or first Receive fails with the service's own complaint
// about a name of "projects/p/topics/" — a diagnosis that names the project
// rather than the missing argument.
func TestAHandleWithNoResourceIsRefusedBeforeAnyRPC(t *testing.T) {
	t.Parallel()
	c := &Client{project: "p"}
	if _, err := c.Publisher(psapi.PublisherConfig{}); !errors.Is(err, errNoTopic) {
		t.Errorf("a publisher with no topic reported %v, want errNoTopic", err)
	}
	if _, err := c.Subscription(psapi.SubscriptionConfig{}); !errors.Is(err, errNoSubscription) {
		t.Errorf("a subscription handle with no subscription reported %v, want errNoSubscription", err)
	}
}

// TestTheWrappedClientReportsItsOwnIdentity pins the three constants the specification's
// health surface and the capability table read, because a driver that reported
// the v1 module while running v2 would make every version-conditioned claim in
// the table wrong.
func TestTheWrappedClientReportsItsOwnIdentity(t *testing.T) {
	t.Parallel()
	c := &Client{project: "my-project"}
	if got := c.Project(); got != "my-project" {
		t.Errorf("Project() = %q", got)
	}
	if got := c.Generation(); got != Generation {
		t.Errorf("Generation() = %q, want %q", got, Generation)
	}
	if got := c.ClientModule(); got != Module {
		t.Errorf("ClientModule() = %q, want %q", got, Module)
	}
	if got := c.ClientVersion(); got != Version {
		t.Errorf("ClientVersion() = %q, want %q", got, Version)
	}
	// Native is the escape hatch, and what matters is the TYPE it hands
	// back: a caller reaching for the library's own client type finds it, and
	// a driver that returned its own wrapper instead would make every such
	// assertion fail at run time rather than at compile time.
	if _, ok := c.Native().(*pubsub.Client); !ok {
		t.Errorf("Native() is a %T, want the library's own *pubsub.Client: the escape hatch is only usable if "+
			"a caller can assert the type the library documents", c.Native())
	}
}
