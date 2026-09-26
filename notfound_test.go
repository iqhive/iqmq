package iqmq_test

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestNotFoundTellsATopicFromASubscription is the distinction the specification found
// missing: a driver reporting a missing subscription has only CodeNotFound to
// set, so a caller had to read the message to tell it from a missing topic.
//
// There is deliberately no ErrSubscriptionNotFound: the specification matches a sentinel
// by code, so a second sentinel over CodeNotFound would be true of both cases
// in both directions. The *OpError already names which of the two the
// operation was about, and these predicates read it.
func TestNotFoundTellsATopicFromASubscription(t *testing.T) {
	topicGone := &mq.OpError{
		Driver: "test", Operation: mq.OpPublish, Topic: "orders",
		Code: mq.CodeNotFound, Err: errors.New("no such topic"),
	}
	subGone := &mq.OpError{
		Driver: "test", Operation: mq.OpFetch, Topic: "orders", Subscription: "workers",
		Code: mq.CodeNotFound, Err: errors.New("no such subscription"),
	}

	if !mq.IsTopicNotFound(topicGone) {
		t.Errorf("IsTopicNotFound(a publisher's NotFound) = false; a NotFound that names no subscription is the topic")
	}
	if mq.IsSubscriptionNotFound(topicGone) {
		t.Errorf("IsSubscriptionNotFound(a publisher's NotFound) = true; it names no subscription")
	}
	if !mq.IsSubscriptionNotFound(subGone) {
		t.Errorf("IsSubscriptionNotFound(a consumer's NotFound naming a subscription) = false: this is the case " +
			"a caller could only tell by reading the message")
	}
	if mq.IsTopicNotFound(subGone) {
		t.Errorf("IsTopicNotFound(a NotFound that names a subscription) = true; the subscription is what was missing")
	}

	// The older behaviour is unchanged, and the predicates are why it is
	// still usable: errors.Is matches by code, so the sentinel is true of
	// both, which is exactly why one sentinel cannot carry the distinction.
	if !errors.Is(subGone, mq.ErrTopicNotFound) {
		t.Errorf("errors.Is(a missing subscription, ErrTopicNotFound) = false; the specification matches by code and both are CodeNotFound")
	}

	// Neither predicate answers yes to anything else.
	for name, err := range map[string]error{
		"a plain error":    errors.New("boom"),
		"another code":     &mq.OpError{Driver: "test", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeUnavailable, Err: errors.New("down")},
		"a nil error":      nil,
		"the sentinel odd": mq.ErrUnsupported,
	} {
		if mq.IsTopicNotFound(err) || mq.IsSubscriptionNotFound(err) {
			t.Errorf("%s: a NotFound predicate said yes (topic=%v sub=%v)", name,
				mq.IsTopicNotFound(err), mq.IsSubscriptionNotFound(err))
		}
	}
}

// adminFailingBackend declares the topology surface and cannot build it, which
// is what a driver with an unconfigured management endpoint does.
type adminFailingBackend struct {
	*fakeBroker
	err error
}

func (b adminFailingBackend) Admin() (mq.Admin, error) { return nil, b.err }

// TestAdminReportsWhyTheSurfaceIsMissing pins the other finding: NewClient
// discarded the error AdminBackend.Admin returned, so Client.Admin then
// answered ErrUnsupported -- "this driver has no topology" -- about a driver
// whose topology surface had failed to open.
func TestAdminReportsWhyTheSurfaceIsMissing(t *testing.T) {
	want := errors.New("the management endpoint is not configured")
	c := mq.NewClient(adminFailingBackend{fakeBroker: newFakeBroker(fakeConfig{}, "orders"), err: want})
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.Admin()
	if err == nil {
		t.Fatal("Admin() succeeded on a backend whose Admin() failed")
	}
	if errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("Admin() = %v, which says the driver HAS no topology surface; it has one and it could not be opened", err)
	}
	if !errors.Is(err, want) {
		t.Errorf("Admin() = %v, want the driver's own error %v", err, want)
	}
}
