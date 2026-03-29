package engine

import (
	"context"
	"errors"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// TestTopologyKindString names every profile, including the zero value and an
// undefined one.
func TestTopologyKindString(t *testing.T) {
	for k, want := range map[TopologyKind]string{
		TopologyDefault: "default",
		TopologyQueue:   "queue",
		TopologyTopic:   "topic",
		TopologyStream:  "stream",
		TopologyKind(9): "topology(?)",
	} {
		if got := k.String(); got != want {
			t.Fatalf("TopologyKind(%d).String() = %q, want %q", k, got, want)
		}
	}
}

// TestTopologyDefaultResolvesToTopic: the zero Kind takes the engine's
// default topology, and the package default behind that is TopologyTopic —
// fan-out to independent subscriptions (adr/ADR-017-assumptions.md,
// ).
func TestTopologyDefaultResolvesToTopic(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got := top.Topology().Kind; got != TopologyTopic {
		t.Fatalf("the default topology is %v, want topic", got)
	}
	a, err := top.Subscribe("a", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := top.Subscribe("b", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("a topic refused a second subscription: %v", err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Subscription[int64]{a, b} {
		if snap := s.Inspect(); snap.Published != 1 || snap.Ready != 1 {
			t.Fatalf("subscription %s: %+v", s.Name(), snap)
		}
	}
	// A topic named by an engine default is the same topic profile.
	e2 := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, Kind: TopologyQueue})
	q, err := e2.CreateTopic("q", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got := q.Topology().Kind; got != TopologyQueue {
		t.Fatalf("the engine default was not inherited: %v", got)
	}
}

// TestTopologyQueueTakesOneSubscription: a queue is competing consumers over
// one subscription, so a second, differently named Subscribe is a
// configuration error rather than a silent fan-out.
func TestTopologyQueueTakesOneSubscription(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	top, err := e.CreateTopic("q", TopologyConfig{Kind: TopologyQueue})
	if err != nil {
		t.Fatal(err)
	}
	if got := top.Topology().Kind; got != TopologyQueue {
		t.Fatalf("topology is %v", got)
	}
	a, err := top.Subscribe("a", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// The same name is the same subscription, however often it is asked for,
	// and any number of consumers may compete over it.
	again, err := top.Subscribe("a", SubscriptionConfig{})
	if err != nil || again != a {
		t.Fatalf("Subscribe(\"a\") again = %p, %v", again, err)
	}
	for range 2 {
		if _, err := a.Attach(ConsumerConfig{}); err != nil {
			t.Fatalf("a queue refused a competing consumer: %v", err)
		}
	}
	if _, err := top.Subscribe("b", SubscriptionConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a second subscription of a queue: %v, want ErrInvalidConfig", err)
	}
	if n := len(top.Subscriptions()); n != 1 {
		t.Fatalf("the queue holds %d subscriptions", n)
	}
	// Once the only subscription has gone the queue can be subscribed again:
	// what the profile forbids is two at once.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := top.Subscribe("b", SubscriptionConfig{}); err != nil {
		t.Fatalf("Subscribe after the only subscription closed: %v", err)
	}
}

// TestTopologyKindIsChecked: an undefined topology profile is refused rather
// than behaving like a topic, which would advertise semantics the engine does
// not implement. The stream profile is implemented and its own
// tests live in stream_test.go.
func TestTopologyKindIsChecked(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	if _, err := e.CreateTopic("s", TopologyConfig{Kind: TopologyKind(9)}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("CreateTopic with an undefined profile = %v, want ErrInvalidConfig", err)
	}
	if _, err := e.Topic("s"); !errors.Is(err, errcode.ErrTopicNotFound) {
		t.Fatalf("a refused topology created the topic anyway: %v", err)
	}
	// Retention on a topology that advertises no replay is refused too.
	if _, err := e.CreateTopic("r", TopologyConfig{
		Kind: TopologyTopic, Retention: Retention{MaxMessages: 4},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("retention on a topic = %v, want ErrInvalidConfig", err)
	}
	if _, err := New(Config[int64]{
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{Kind: TopologyKind(9)},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New with an undefined default profile: %v", err)
	}
}
