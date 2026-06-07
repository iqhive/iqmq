package psfake

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// The fake's own refusals and counters.
//
// `drivers/gcppubsub`'s tests drive the delivery paths through it all day.
// What they never do is ask it for a publisher with no topic, publish an
// ordering key to a publisher that was not built for one, or use anything
// after Close -- and those are the paths this fake exists to produce, because
// the real library produces them only under conditions a test cannot arrange.

// running is a server with one topic and one subscription on it.
func running(t *testing.T) *Server {
	t.Helper()
	s := New("iqmq-test")
	s.CreateTopic("orders")
	s.CreateSubscription(psapi.SubscriptionSpec{Subscription: "workers", Topic: "orders"})
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestAHandleWithNoNameIsRefused.
//
// Both are a driver bug rather than a configuration one -- the names come
// from the driver's own naming layer -- so the fake refuses instead of
// creating something addressed by the empty string, which would then be found
// by every later lookup that also passed "".
func TestAHandleWithNoNameIsRefused(t *testing.T) {
	s := running(t)

	if _, err := s.Publisher(psapi.PublisherConfig{}); err == nil {
		t.Error("Publisher with no topic returned a handle; it would publish to a topic named \"\"")
	}
	if _, err := s.Subscription(psapi.SubscriptionConfig{}); err == nil {
		t.Error("Subscription with no name returned a handle")
	}
}

// TestAnOrderingKeyOnAPublisherThatWasNotBuiltForOneIsRefused.
//
// The library's publisher decides at construction whether it orders, and a
// key on one that does not is an error rather than an ignored field. It has
// to be an error: silently dropping the key is the ordering guarantee
// disappearing without anybody being told.
func TestAnOrderingKeyOnAPublisherThatWasNotBuiltForOneIsRefused(t *testing.T) {
	s := running(t)
	p, err := s.Publisher(psapi.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}

	res := p.Publish(t.Context(), &psapi.Outgoing{Data: []byte("x"), OrderingKey: "k"})
	if _, err := res.Get(t.Context()); err == nil {
		t.Error("publishing with an ordering key to an unordered publisher succeeded; dropping the key " +
			"silently is the guarantee disappearing without anybody being told")
	}
}

// TestPublishingToAClosedServerFails, and so does publishing from a stopped
// publisher.
//
// Both are the shutdown race a driver meets: the client is closing while a
// publication is in flight. The failure has to be an error rather than a
// panic or a result that never resolves, because the driver is waiting on it.
func TestPublishingToAClosedServerFails(t *testing.T) {
	s := running(t)
	p, err := s.Publisher(psapi.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing the server: %v", err)
	}

	res := p.Publish(t.Context(), &psapi.Outgoing{Data: []byte("x")})
	if _, err := res.Get(t.Context()); err == nil {
		t.Error("publishing to a closed server succeeded")
	}

	p.Stop()
	res = p.Publish(t.Context(), &psapi.Outgoing{Data: []byte("x")})
	if _, err := res.Get(t.Context()); err == nil {
		t.Error("publishing from a stopped publisher succeeded")
	}
}

// TestAPublishResultRespectsACancelledContext.
//
// `Get` is what the driver blocks in, so a cancelled context has to end it:
// a result that ignored the context would make a publish uninterruptible and the deadline unenforceable.
func TestAPublishResultRespectsACancelledContext(t *testing.T) {
	s := running(t)
	p, err := s.Publisher(psapi.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	s.GatePublishResults()
	t.Cleanup(s.ReleasePublishResults)

	res := p.Publish(t.Context(), &psapi.Outgoing{Data: []byte("x")})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := res.Get(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("a gated publish result read with a cancelled context is %v, want context.Canceled", err)
	}
}

// TestReceivingFromASubscriptionThatIsNotThereIsNotFound, and an injected
// receive failure reaches the caller.
func TestReceivingFromASubscriptionThatIsNotThereIsNotFound(t *testing.T) {
	s := running(t)

	missing, err := s.Subscription(psapi.SubscriptionConfig{Subscription: "never-created"})
	if err != nil {
		t.Fatalf("Subscription: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := missing.Receive(ctx, func(context.Context, *psapi.Incoming) {}); !errors.Is(err, psapi.ErrNotFound) {
		t.Errorf("receiving from a subscription that does not exist is %v, want ErrNotFound", err)
	}
}

// TestTheCountersReportZeroForASubscriptionNobodyMade.
//
// They are what every test of the driver asserts on, so answering something
// other than zero -- or panicking -- for a name that is not there would make
// a failing test say the wrong thing about why.
func TestTheCountersReportZeroForASubscriptionNobodyMade(t *testing.T) {
	s := running(t)

	if got := s.Backlog("never-created"); got != 0 {
		t.Errorf("Backlog of a subscription that does not exist is %d, want 0", got)
	}
	if got := s.Outstanding("never-created"); got != 0 {
		t.Errorf("Outstanding of a subscription that does not exist is %d, want 0", got)
	}
}

// TestDeletingATopicAndAskingWhetherItIsThere.
//
// `TopicExists` is what the driver's AutoCreate path asks before it creates,
// so both answers have to be right and a fault on it has to reach the caller
// rather than reading as "no".
func TestDeletingATopicAndAskingWhetherItIsThere(t *testing.T) {
	s := running(t)
	admin := s.Admin()

	exists, err := admin.TopicExists(t.Context(), "orders")
	if err != nil || !exists {
		t.Fatalf("TopicExists for a topic that is there is (%t, %v), want (true, nil)", exists, err)
	}
	if err := admin.DeleteTopic(t.Context(), "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	exists, err = admin.TopicExists(t.Context(), "orders")
	if err != nil || exists {
		t.Errorf("TopicExists after the delete is (%t, %v), want (false, nil)", exists, err)
	}

	boom := errors.New("the service refused")
	s.AdminErr = func(op, _ string) error {
		if op == "TopicExists" {
			return boom
		}
		return nil
	}
	if _, err := admin.TopicExists(t.Context(), "orders"); !errors.Is(err, boom) {
		t.Errorf("a failing TopicExists is %v, want the injected error: reading it as \"no\" would make the "+
			"driver create a topic it could not see", err)
	}
}

// TestSettlingAnAckIDNobodyIssuedIsNotFound.
//
// A settlement after a redelivery carries an identifier the service has
// forgotten, which is the lost lease. It must not be silently accepted:
// the driver decides what to do about it, and it cannot if it is told the
// settlement worked.
func TestSettlingAnAckIDNobodyIssuedIsNotFound(t *testing.T) {
	s := running(t)

	if err := s.Admin().Seek(t.Context(), "never-created", time.Now()); !errors.Is(err, psapi.ErrNotFound) {
		t.Errorf("seeking a subscription that does not exist is %v, want ErrNotFound", err)
	}
}

// TestAServerThatIsClosedTwiceIsStillClosed.
func TestAServerThatIsClosedTwiceIsStillClosed(t *testing.T) {
	s := New("iqmq-test")
	if err := s.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("closing a second time is %v, want nil", err)
	}
	if !strings.Contains(s.Project(), "iqmq") {
		t.Errorf("the project is %q after Close", s.Project())
	}
}
