package nsq_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
	"bitbucket.org/iqhive/iqmq/drivers/nsq/internal/nsqapi"
	"bitbucket.org/iqhive/iqmq/drivers/nsq/nsqfake"
)

// TestCriticalDeferredRequeue is the first of the driver-specific critical
// tests for NSQ ("deferred requeue, backoff, lookupd changes").
//
// A requeue with a delay is nsqd's own delayed redelivery: REQ carries the
// delay, nsqd holds the message at the channel level, and the attempt count
// rises. What it must NOT be is an in-process timer holding an unacked
// message, which is what a driver without the native verb would have to do —
// and what classes as a semantics-preserving emulation the core owns, not
// the driver.
func TestCriticalDeferredRequeue(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("retry me")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Three requeues with rising delays, each of which must come back only
	// after its own delay and with its attempt count one higher.
	for i, delay := range []time.Duration{time.Second, 5 * time.Second, 30 * time.Second} {
		attempt := uint32(i + 1)
		d := fetchOne(t, src)
		if d.Info.Attempt != attempt {
			t.Fatalf("the delivery reports attempt %d, want %d", d.Info.Attempt, attempt)
		}
		if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Delay: delay}); err != nil {
			t.Fatalf("Nack(%v): %v", delay, err)
		}
		st := server.Stats("orders", "workers")
		if st.Deferred != 1 {
			t.Fatalf("attempt %d: the broker holds %d deferred and %d pending; the delay must be nsqd's",
				attempt, st.Deferred, st.Depth)
		}
		if st.BackoffAsked != 0 {
			t.Fatalf("attempt %d: the requeue asked go-nsq for its own backoff, which sets RDY to zero "+
				"for the whole subscription", attempt)
		}
		// Not yet.
		server.Advance(delay / 2)
		if st := server.Stats("orders", "workers"); st.Deferred != 1 {
			t.Fatalf("attempt %d: the message became deliverable at half its delay", attempt)
		}
		server.Advance(delay)
	}
	final := fetchOne(t, src)
	if final.Info.Attempt != 4 || !bytes.Equal(final.Message.Body, []byte("retry me")) {
		t.Fatalf("the fourth delivery is attempt %d of %q", final.Info.Attempt, final.Message.Body)
	}
	if err := src.Settler().Ack(context.Background(), &final); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestCriticalGoNSQRetriesAreOff is the second: go-nsq's own retry policy must
// be off, and both halves of it matter.
//
// `Config.MaxAttempts` defaults to five, and what go-nsq does when a message's
// Attempts passes it is call FINISH and log a warning — a message DISCARDED by
// the client library while the core was still retrying it. That is a loss of an
// accepted message outside any documented failure boundary, which the specification makes a
// disqualifier.
//
// `Config.MaxBackoffDuration` defaults to two minutes, and go-nsq's backoff
// sets RDY to zero for the whole subscription: one message's failure stops
// every other message. The specification gives redelivery accounting to exactly one layer.
func TestCriticalGoNSQRetriesAreOff(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("stubborn")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Past go-nsq's default MaxAttempts of five, twice over.
	for i := range 12 {
		d := fetchOne(t, src)
		if got := d.Info.Attempt; got != uint32(i+1) {
			t.Fatalf("delivery %d reports attempt %d; a message the client library gave up on "+
				"would have been finished and never delivered again", i+1, got)
		}
		if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{}); err != nil {
			t.Fatalf("Nack %d: %v", i+1, err)
		}
	}
	st := server.Stats("orders", "workers")
	if st.Finished != 0 {
		t.Fatalf("the broker finished %d message(s) and the core never acknowledged one: go-nsq gave up "+
			"on a message the core was still retrying, which is the specification's 'can lose an accepted message'",
			st.Finished)
	}
	if st.BackoffAsked != 0 {
		t.Fatalf("%d requeue(s) asked go-nsq for its own backoff", st.BackoffAsked)
	}
	if st.Requeued != 12 {
		t.Fatalf("the broker saw %d requeues for twelve negative settlements", st.Requeued)
	}
	d := fetchOne(t, src)
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestCriticalLookupdIsPreferredOverDirectAddresses is the third.
//
// A consumer connected straight to the nsqd addresses never learns about an
// nsqd that joins the cluster later, so on a cluster a direct connection
// silently consumes a SUBSET of the topic — every message on the nsqd nobody
// told it about is invisible. nsqlookupd is what closes that, so a
// configuration that has one uses it, and the preference is not a tie-break.
func TestCriticalLookupdIsPreferredOverDirectAddresses(t *testing.T) {
	for name, tc := range map[string]struct {
		lookupd []string
		want    string
	}{
		"with a lookupd":    {[]string{"http://lookupd:4161"}, "lookupd"},
		"without a lookupd": {nil, "direct"},
	} {
		t.Run(name, func(t *testing.T) {
			server := nsqfake.New()
			spy := &connectSpy{Producer: server.Producer(), server: server}
			cfg := nq.Config{
				Addrs:        []string{"nsqfake:4150"},
				LookupdAddrs: tc.lookupd,
				AckDeadline:  30 * time.Second,
				LeaseGuard:   time.Second,
				Clock:        server,
			}
			b, err := nq.BackendWithClient(context.Background(), spy, driverkit.ClientOwned, cfg)
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 2)
			if got := spy.how(); got != tc.want {
				t.Fatalf("the consumer connected %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCriticalARedeliveryAfterAConnectionLossIsNotSettledTwice is the fourth,
// and the one that costs correctness rather than throughput.
//
// nsqd makes every message a lost connection was holding available again at
// once, and the handles the previous consumer holds stay valid as far as nsqd
// is concerned. So the driver has to refuse a settlement for a delivery a later
// one has superseded, and the model counts every settlement that landed on the
// wrong delivery attempt.
func TestCriticalARedeliveryAfterAConnectionLossIsNotSettledTwice(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 8)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	const messages = 8
	for i := range messages {
		if err := snk.Publish(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	first := fetchN(t, src, messages)
	server.Redeliver("orders", "workers")
	second := fetchN(t, src, messages)

	// Every settlement from the first round has to be refused, and none of
	// them may reach the broker.
	for i := range first {
		if err := src.Settler().Ack(context.Background(), &first[i]); !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("the stale Ack of delivery %d answered %v, want ErrLeaseLost", i, err)
		}
	}
	for i := range second {
		if err := src.Settler().Ack(context.Background(), &second[i]); err != nil {
			t.Fatalf("the live Ack of delivery %d: %v", i, err)
		}
	}
	st := server.Stats("orders", "workers")
	if st.CrossSettled != 0 {
		t.Fatalf("%d settlement(s) landed on a later delivery than the one that sent them", st.CrossSettled)
	}
	if st.Finished != messages {
		t.Fatalf("the broker finished %d of %d messages", st.Finished, messages)
	}
	if n := b.Stale(); n < messages {
		t.Fatalf("the driver counted %d stale settlements for %d superseded deliveries", n, messages)
	}
}

// connectSpy wraps the in-process model and records which discovery route each
// consumer took, which is the only externally visible difference between a
// lookupd-based subscription and a direct one.
type connectSpy struct {
	// The publishing half is the model's own. The spy's Consumer method
	// shadows the promoted one, which is how the wrapper gets in.
	nsqapi.Producer
	server *nsqfake.Server

	mu    sync.Mutex
	taken []string
}

// Consumer wraps the model's consumer so that the connect call is observable.
func (s *connectSpy) Consumer(topic, channel string, maxInFlight int, deadline time.Duration) nsqapi.Consumer {
	return &spyConsumer{Consumer: s.server.Consumer(topic, channel, maxInFlight, deadline), spy: s}
}

func (s *connectSpy) record(route string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.taken = append(s.taken, route)
}

func (s *connectSpy) how() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.taken) != 1 {
		return strings.Join(s.taken, "+")
	}
	return s.taken[0]
}

type spyConsumer struct {
	nsqapi.Consumer
	spy *connectSpy
}

func (c *spyConsumer) ConnectToNSQLookupds(addrs []string) error {
	c.spy.record("lookupd")
	return c.Consumer.ConnectToNSQLookupds(addrs)
}

func (c *spyConsumer) ConnectToNSQDs(addrs []string) error {
	c.spy.record("direct")
	return c.Consumer.ConnectToNSQDs(addrs)
}
