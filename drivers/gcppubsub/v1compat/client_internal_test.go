package v1compat

import (
	"errors"
	"testing"
	"time"

	//nolint:staticcheck // the deprecated v1 client library IS this package, and so is its test
	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// TestASettlementTheServiceRefusedIsNeverASuccess is this arm's copy of the one
// translation a settlement's integrity rests on, and it is tested here rather
// than only in the v2 arm because compares the two arms and a
// divergence in this function is a divergence in what a settlement MEANS.
//
// On an exactly-once subscription the library answers an acknowledgement with a
// STATUS and, for some outcomes, no error at all. Returning that error unchanged
// would hand the driver a nil for AcknowledgeStatusPermissionDenied, the driver
// would report the settlement done, and the service would redeliver a message it
// never acknowledged — the disqualifier, produced by the layer that makes
// the claim.
func TestASettlementTheServiceRefusedIsNeverASuccess(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		st       pubsub.AcknowledgeStatus
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
			got := ackStatus(tc.st, nil)
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
				t.Errorf("the status %v %s psapi.ErrAckIDNotReachable: an identifier that is gone is a lost lease "+
					"and every other refusal is the service saying no", tc.st,
					map[bool]string{true: "does not reach", false: "reaches"}[tc.notReach])
			}
		})
	}
	own := errors.New("quota exceeded for project")
	if got := ackStatus(pubsub.AcknowledgeStatusOther, own); !errors.Is(got, own) {
		t.Errorf("a refusal with a reason reported %v, which does not reach the service's own error", got)
	}
	fallback := errors.New("fallback")
	if got := errOr(own, fallback); !errors.Is(got, own) {
		t.Errorf("errOr chose %v over the service's own error", got)
	}
	if got := errOr(nil, fallback); !errors.Is(got, fallback) {
		t.Errorf("errOr with no error chose %v, want the fallback: a failed settlement must never render as a "+
			"success", got)
	}
}

// TestNotFoundAndAlreadyExistsAreTheTwoStatusesTopologyDependsOn is [translate],
// the pair the idempotence rests on: EnsureTopic treats AlreadyExists as
// success and DeleteTopic treats NotFound as success, so confusing them makes a
// create report failure for a topic that is there and a delete report success
// for one it never touched.
func TestNotFoundAndAlreadyExistsAreTheTwoStatusesTopologyDependsOn(t *testing.T) {
	t.Parallel()
	if got := translate(nil); got != nil {
		t.Errorf("translate(nil) is %v, want nil", got)
	}
	notFound := status.Error(codes.NotFound, "no such topic")
	if got := translate(notFound); !errors.Is(got, psapi.ErrNotFound) {
		t.Errorf("a NotFound status became %v, which does not reach psapi.ErrNotFound", got)
	} else if !errors.Is(got, notFound) {
		t.Error("the translation lost the gRPC status, so a caller cannot read the service's own message")
	}
	if got := translate(status.Error(codes.AlreadyExists, "topic exists")); !errors.Is(got, psapi.ErrAlreadyExists) {
		t.Errorf("an AlreadyExists status became %v, which does not reach psapi.ErrAlreadyExists", got)
	}
	other := status.Error(codes.ResourceExhausted, "quota")
	got := translate(other)
	if errors.Is(got, psapi.ErrNotFound) || errors.Is(got, psapi.ErrAlreadyExists) {
		t.Errorf("a ResourceExhausted status became %v, which one of the topology sentinels claims: a quota "+
			"failure reported as \"already exists\" is a create an operator believes succeeded", got)
	}
	if !errors.Is(got, other) {
		t.Errorf("a status this package does not name became %v rather than travelling unchanged", got)
	}
}

// TestALeaseIsNeverRoundedDownToNothing is [wholeSeconds]. The service takes
// whole seconds, and truncating 500ms to 0 is a subscription whose messages are
// immediately redeliverable — no lease at all, and a duplicate for every
// delivery. Only zero and below may answer zero.
func TestALeaseIsNeverRoundedDownToNothing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   time.Duration
		want int64
	}{
		{0, 0},
		{-time.Second, 0},
		{time.Nanosecond, 1},
		{500 * time.Millisecond, 1},
		{time.Second, 1},
		{time.Second + time.Nanosecond, 2},
		{90 * time.Second, 90},
	} {
		if got := wholeSeconds(tc.in); got != tc.want {
			t.Errorf("wholeSeconds(%v) = %d, want %d: a sub-second deadline rounded DOWN is a subscription whose "+
				"messages are immediately redeliverable", tc.in, got, tc.want)
		}
	}
}

// TestTheV1ArmTakesTheSHORTNameAndReportsTheShortNameBack is the naming
// difference between the two arms, and it is a difference in the
// LIBRARY rather than in this driver.
//
// The v1 API's Client.Topic takes a topic ID and qualifies it itself, so this
// arm must hand it the last segment; the v2 arm's Publisher takes the fully
// qualified name. Handing v1 a qualified name produces
// "projects/p/topics/projects/p/topics/orders", which the service accepts as a
// topic nobody is subscribed to — a publication that succeeds and is never
// received.
func TestTheV1ArmTakesTheSHORTNameAndReportsTheShortNameBack(t *testing.T) {
	t.Parallel()
	if got := lastSegment("projects/p/topics/orders"); got != "orders" {
		t.Errorf("lastSegment = %q, want orders: the v1 client qualifies the ID itself, and handing it a qualified "+
			"name names a topic nobody is subscribed to", got)
	}
	if got := lastSegment("orders"); got != "orders" {
		t.Errorf("lastSegment of an unqualified name is %q, want it unchanged", got)
	}
	if got := qualifyTopic("p", "orders"); got != "projects/p/topics/orders" {
		t.Errorf("qualifyTopic(p, orders) = %q", got)
	}
	full := "projects/other/topics/orders"
	if got := qualifyTopic("p", full); got != full {
		t.Errorf("qualifyTopic passed a qualified name through as %q; qualifying it again names a topic nobody is "+
			"subscribed to", got)
	}
}

// TestAHandleWithNoResourceIsRefusedBeforeAnyRPC holds the two guards in
// [Client.Publisher] and [Client.Subscription]. Neither does any I/O, so an
// empty name would otherwise become a handle whose first publication fails with
// the library's own complaint about an empty topic ID rather than with the
// missing argument.
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

// TestThisArmReportsTheV1LibraryAndNotTheV2One pins the three constants family
// the bakeoff report is keyed on. An arm that reported the other library's
// module or generation would make every row of the comparison name the wrong
// client, and the comparison is the whole reason both arms exist.
func TestThisArmReportsTheV1LibraryAndNotTheV2One(t *testing.T) {
	t.Parallel()
	c := &Client{project: "my-project"}
	if got := c.Project(); got != "my-project" {
		t.Errorf("Project() = %q", got)
	}
	if got := c.Generation(); got != "v1compat" {
		t.Errorf("Generation() = %q, want v1compat", got)
	}
	if got := c.ClientModule(); got != "cloud.google.com/go/pubsub" {
		t.Errorf("ClientModule() = %q, want the v1 module: a report naming pubsub/v2 while this arm answered "+
			"makes every row of the comparison name the wrong client", got)
	}
	if got := c.ClientVersion(); got != Version {
		t.Errorf("ClientVersion() = %q, want %q", got, Version)
	}
}
