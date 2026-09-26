package amqp10_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	rmq10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
)

// budget bounds the waits in this file. It is generous because this host runs
// about fifty other people's containers and a reconnection is a dial loop with
// jittered backoff, not a synchronous call.
const budget = 5 * time.Second

// TestCriticalStaleDispositionIsRefused is the disqualifier "lets a stale
// ack settle a later redelivery", held against this profile.
//
// # Why it matters here specifically
//
// Detaching an AMQP 1.0 link returns everything unsettled on it to the queue,
// where RabbitMQ hands it to whichever consumer of that queue is next. So by
// the time a settlement arrives for a delivery whose link is gone, the message
// is the queue's again and may already be another consumer's — and the delivery
// id the disposition would name is one the NEW link may reuse for a different
// message. A driver that wrote the disposition anyway would settle a delivery
// this process never saw.
//
// # What this test proves, and what it cannot
//
// The guard is the shared core's: a delivery records the connection generation
// of the link it arrived on, takes a slot in a
// [github.com/iqhive/iqmq/driverkit.TokenTable], and a settlement whose token
// or whose generation is not current is refused with ErrLeaseLost before
// anything reaches the wire. What this profile could do to defeat it is supply
// a second settlement path — a [core.Profile.DeadLetterAddress], which makes
// the core publish a copy and then settle the original — and it supplies none
// (TestProfileShape).
//
// So what is checked here is that the guard is REACHABLE and INTACT through
// this profile's own backend: a settlement for a delivery this source does not
// hold is refused with ErrLeaseLost and counted, before and after a
// reconnection, and the connection generation really moves when the connection
// dies. What it cannot check is the same refusal for a delivery that arrived on
// a real link, because the scripted peer here cannot attach one: go-amqp
// implements the client role of a link, and the shared core's own scripted peer
// — which can — is in `drivers/amqp10/internal/client` and is unreachable from
// this module (peer_test.go says why, with Go's own message for the attempt).
// The core's TestCriticalStaleDispositionAfterDetach is that half, against the
// same guard.
func TestCriticalStaleDispositionIsRefused(t *testing.T) {
	p := newPeer(t, rabbitProps("4.1.0"))
	b := openBackend(t, testConfig(p))
	ctx := context.Background()

	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("opening the source: %v", err)
	}
	t.Cleanup(func() {
		if err := src.Close(); err != nil {
			t.Errorf("closing the source: %v", err)
		}
	})
	source, ok := src.(*core.Source)
	if !ok {
		t.Fatalf("the source is a %T and not the core's", src)
	}
	set := source.Settler()

	// Two tokens, which are the guard's two refusal paths: one whose slot
	// generation does not match the token, and one that matches a slot the
	// table has never handed out. Both are ErrLeaseLost, and a guard that
	// answered only one of them would let the other through.
	stale := []mq.DriverDelivery{
		{Token: 0},
		{Token: 1 << 32},
	}
	for _, d := range stale {
		if err := set.Ack(ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("the Ack of a delivery this source does not hold (token %d) = %v, want ErrLeaseLost", d.Token, err)
		}
		if err := set.Nack(ctx, &d, mq.NackOptions{}); !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("the Nack of a delivery this source does not hold (token %d) = %v, want ErrLeaseLost", d.Token, err)
		}
	}
	// And the guard discriminates rather than answering ErrLeaseLost to
	// everything: a nil delivery is a programming error in the core's caller
	// and must not be reported as a message that went back to the queue,
	// which is a thing a caller would otherwise wait for.
	if err := set.Ack(ctx, nil); errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("the Ack of a nil delivery = %v, which reaches ErrLeaseLost: a check that answers \"lost lease\" to "+
			"every input proves nothing about the ones that really are stale", err)
	} else if err == nil {
		t.Error("the Ack of a nil delivery succeeded")
	}
	if n := source.StaleSettlements(); n == 0 {
		t.Fatal("the source counted no stale settlement, so a deployment whose handlers outlive their links could " +
			"not see that it is happening")
	}
	before := source.StaleSettlements()

	// The connection dies. The supervisor bumps the generation before
	// anything else happens — which is what makes a stale settlement
	// detectable AT ONCE rather than one backoff later — and then re-dials.
	gen := b.Generation()
	p.killAll()
	if !core.AwaitGenerationForTest(b, gen, budget) {
		t.Fatalf("the connection generation did not move past %d after the connection died, so a delivery from the "+
			"dead link would still look current", gen)
	}
	if !core.AwaitReconnectForTest(b, 1, budget) {
		t.Fatal("the dial loop did not re-establish the connection")
	}
	if p.accepted() < 2 {
		t.Errorf("the peer accepted %d connection(s); the driver owns its dial loop and a lost connection has "+
			"to be re-dialled", p.accepted())
	}

	// And the guard still answers after the reconnection: a driver that
	// rebuilt its token table on re-dial and forgot the generation would
	// accept these.
	for _, d := range stale {
		if err := set.Ack(ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("after the reconnection, the Ack of a stale delivery (token %d) = %v, want ErrLeaseLost", d.Token, err)
		}
	}
	if source.StaleSettlements() <= before {
		t.Error("the settlements after the reconnection were not counted as stale")
	}
	// A settlement is never reported as a success it was not: the backend's
	// own counter has to agree with the source's, or a deployment reading one
	// of the two numbers would see nothing.
	if b.Stats().StaleSettlements == 0 {
		t.Error("the backend counted no stale settlement, so the condition has a name in one place and no number in " +
			"the other")
	}
}

// TestCriticalUnhonourableNackSettlesNothing is the rule for an action this
// profile cannot perform, and it is the other half of the same hazard.
//
// The specification says a driver that cannot honour the action asked for "returns
// ErrUnsupported and settles nothing", and the reason the second half matters
// as much as the first is the token table: a driver that took the delivery's
// slot from active to settling and THEN discovered it could not honour the
// action would leave the delivery unsettleable for ever — the caller cannot
// retry, the broker will not redeliver until the link goes, and the credit the
// slot holds is gone.
//
// Both refusals are this profile's own. A delayed requeue is refused because
// RabbitMQ has no AMQP 1.0 spelling for "hold this back" — a `modified`
// disposition makes the message available again at once — and a dead-letter
// nack is refused when the deployment has not said its queues have a
// dead-letter exchange, because the alternative is a `rejected` disposition
// that discards the message into nowhere.
func TestCriticalUnhonourableNackSettlesNothing(t *testing.T) {
	p := newPeer(t, rabbitProps("4.1.0"))
	b := openBackend(t, testConfig(p))
	ctx := context.Background()

	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("opening the source: %v", err)
	}
	t.Cleanup(func() {
		if err := src.Close(); err != nil {
			t.Errorf("closing the source: %v", err)
		}
	})
	source, ok := src.(*core.Source)
	if !ok {
		t.Fatalf("the source is a %T and not the core's", src)
	}
	set := source.Settler()
	before := source.StaleSettlements()
	delivery := mq.DriverDelivery{Token: 1 << 32}

	for name, o := range map[string]mq.NackOptions{
		"a requeue with a delay":                               {Action: mq.NackActionRequeue, Delay: time.Minute},
		"a dead-letter route this deployment has not declared": {Action: mq.NackActionDeadLetter},
	} {
		err := set.Nack(ctx, &delivery, o)
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("%s = %v, want ErrUnsupported", name, err)
		}
		if errors.Is(err, mq.ErrLeaseLost) {
			t.Errorf("%s was refused as a lost lease, which means the token table was consulted first: the specification wants "+
				"the refusal BEFORE the slot moves, so that the delivery is still settleable", name)
		}
	}
	if got := source.StaleSettlements(); got != before {
		t.Errorf("a refused action moved the stale count from %d to %d; an action the driver cannot honour must "+
			"settle nothing and touch nothing", before, got)
	}
	// The refusal really is the profile's and not the core's taste: with the
	// dead-letter route asserted, the same Nack reaches the settlement path
	// and is then refused for the ordinary reason — the delivery is not this
	// source's.
	withRoute := testConfig(p)
	withRoute.DeadLetter = true
	b2 := openBackend(t, withRoute)
	src2, err := b2.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("opening the second source: %v", err)
	}
	t.Cleanup(func() {
		if err := src2.Close(); err != nil {
			t.Errorf("closing the second source: %v", err)
		}
	})
	source2, ok := src2.(*core.Source)
	if !ok {
		t.Fatalf("the second source is a %T", src2)
	}
	err = source2.Settler().Nack(ctx, &delivery, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("with a dead-letter route asserted, the dead-letter Nack of a stale delivery = %v, want ErrLeaseLost: "+
			"the dead-letter refusal is a capability answer and must not stand in for the staleness guard", err)
	}
}

// TestCriticalDelayedPublicationIsRefusedBeforeAnyIO is the publish-side
// version of the same rule, and it is a claim this profile makes that the AMQP
// 0-9-1 driver does not.
//
// RabbitMQ has no AMQP 1.0 spelling of a delivery delay, and nothing in this
// repository has verified which section the delayed-message plugin would read
// one from. The specification forbids claiming a capability that cannot be proved, so the
// profile carries no delay rule at all — and the consequence a caller sees is
// that a delayed publication is refused before any I/O rather than sent
// without its delay, which would be a message delivered immediately by a
// publisher that asked for it in an hour.
func TestCriticalDelayedPublicationIsRefusedBeforeAnyIO(t *testing.T) {
	cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	profile, err := cfg.Profile()
	if err != nil {
		t.Fatalf("building the profile: %v", err)
	}
	if profile.Delay.Supported() {
		t.Fatal("the profile claims a delayed publication")
	}
	caps, err := rmq10.CapabilitiesForTest(cfg, "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	if caps.Has(mq.FeatureScheduledPublish) {
		t.Fatal("the capability table claims a scheduled publication, so the core would accept a delay and write it " +
			"nowhere")
	}
	if err := caps.Require(mq.RequireAtLeast(mq.FeatureScheduledPublish, mq.Conditional)); !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("a requirement for a scheduled publication = %v, want ErrUnsupported: a caller that needs one has to "+
			"be able to find out at construction", err)
	}
}
