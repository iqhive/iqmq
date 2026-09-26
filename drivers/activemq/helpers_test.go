package activemq_test

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	amq "github.com/iqhive/iqmq/drivers/activemq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/internal/activemq"
)

// The scripted peer is what makes every test in this package need no broker.
// Three of the behaviours this driver has to get right cannot be provoked from
// a container at all -- a disposition sent on a link that has since detached,
// a consumer cancelled while a fetch is parked, and a publication whose
// disposition never arrives -- and the fourth, a link detach with the
// connection still alive, is something no broker will do on request.

// artemisProps is what Artemis announces itself with in its open performative,
// measured against apache/activemq-artemis:2.44.0-alpine.
func artemisProps() map[string]any {
	return map[string]any{"product": activemq.ProductArtemis, "version": "2.44.0"}
}

// artemisConfig is a configuration for Artemis with no timeouts a test has to
// wait out.
func artemisConfig() amq.Config {
	return amq.Config{
		Broker:   activemq.Artemis,
		Addrs:    []string{"localhost:5672"},
		Username: "artemis",
		Password: "artemis",
		// The peer is not a socket, but the core's credential check runs
		// before the dial and is right to: PLAIN over a plaintext address
		// sends the password in clear. Saying so deliberately is what the
		// field is for.
		InsecurePlainSASL: true,
		OperationTimeout:  2 * time.Second,
		// A failed dial must not be retried in a unit test: the reconnect
		// loop would hold the test open for as long as the backoff says.
		NoReconnect: true,
	}
}

// openPeer opens a backend over a scripted peer and registers its close.
func openPeer(t *testing.T, cfg amq.Config, props map[string]any) (*core.Backend, *core.Peer) {
	t.Helper()
	p := core.NewPeer(props)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := amq.BackendWithPeer(ctx, cfg, p)
	if err != nil {
		t.Fatalf("BackendWithPeer: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, p
}

// addresses is the pair of destinations a topic and a subscription map to.
func addresses(t *testing.T, cfg amq.Config, topic, sub string) (sink, source string) {
	t.Helper()
	sink, source, err := amq.AddressForTest(cfg, topic, sub)
	if err != nil {
		t.Fatalf("AddressForTest(%q, %q): %v", topic, sub, err)
	}
	return sink, source
}

// tagged is a delivery the peer can hand over and a test can identify: the
// fake records a disposition against the message's own delivery tag, so a tag
// is how a test says WHICH delivery was settled rather than how many were.
func tagged(tag, body string) *amqp.Message {
	m := amqp.NewMessage([]byte(body))
	m.DeliveryTag = []byte(tag)
	m.Header = &amqp.MessageHeader{Durable: true}
	return m
}

// fetchOne fetches exactly one delivery within d, or fails the test.
func fetchOne(t *testing.T, src mq.Source, d time.Duration) mq.DriverDelivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	n, err := src.Fetch(ctx, dst)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 1 {
		t.Fatalf("Fetch returned %d deliveries, want 1", n)
	}
	return dst[0]
}

// warm attaches every source's link and issues its credit, which is what has
// to happen before the peer has a link to deliver to: the core attaches
// lazily, on the first fetch.
func warm(t *testing.T, b *core.Backend) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := core.WarmForTest(ctx, b); err != nil {
		t.Fatalf("warming the links: %v", err)
	}
}

// fetchWhileDelivering fetches through whatever link the driver attaches
// NEXT, which is how a test exercises re-attachment through the production
// path.
//
// The shape matters. A test that re-attached the link itself -- by warming it,
// or by reaching for the peer -- would pass against a driver that never
// recovers, because the test would have done the recovering. A caller only
// ever calls Fetch, so the fetch runs first and the delivery is handed to the
// peer once the peer has a link to hand it to.
func fetchWhileDelivering(t *testing.T, src mq.Source, p *core.Peer, addr string, msgs ...*amqp.Message) []mq.DriverDelivery {
	t.Helper()
	links := p.Receivers(addr)
	type result struct {
		n   int
		err error
	}
	dst := make([]mq.DriverDelivery, len(msgs))
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		n, err := src.Fetch(ctx, dst)
		done <- result{n: n, err: err}
	}()
	deadline := time.After(8 * time.Second)
	for p.Receivers(addr) <= links {
		select {
		case r := <-done:
			t.Fatalf("the fetch returned (%d, %v) without the driver attaching a new link: a link detach does not bump the connection generation, so a driver that reattaches only on a generation change holds the dead link for ever",
				r.n, r.err)
		case <-deadline:
			t.Fatalf("the driver never attached a new link on %s (still %d): a link detach does not bump the connection generation, so a driver that reattaches only on a generation change holds the dead link for ever",
				addr, p.Receivers(addr))
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := p.Deliver(addr, msgs...); err != nil {
		t.Fatalf("Deliver to the reattached link: %v", err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("the fetch on the reattached link failed: %v", r.err)
		}
		if r.n != len(msgs) {
			t.Fatalf("the fetch on the reattached link returned %d deliveries, want %d", r.n, len(msgs))
		}
	case <-time.After(8 * time.Second):
		t.Fatal("the fetch on the reattached link never returned")
	}
	return dst
}

// openSource opens a source on one subscription with a small prefetch.
func openSource(t *testing.T, b *core.Backend, topic, sub string, prefetch int) mq.Source {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: topic, Subscription: sub, Prefetch: prefetch})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// openSink opens a sink on one topic.
func openSink(t *testing.T, b *core.Backend, topic string, confirm mq.ConfirmationMode) mq.Sink {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: topic, Confirmation: confirm})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	return snk
}
