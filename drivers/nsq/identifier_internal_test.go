package nsq

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/nsq/nsqfake"
)

// verbLog is a message delegate that records every frame written for its
// messages, with the nsqd each one went to.
type verbLog struct {
	mu    sync.Mutex
	verbs []string
}

func (v *verbLog) record(verb string, m *gonsq.Message) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.verbs = append(v.verbs, verb+" "+m.NSQDAddress)
}

func (v *verbLog) OnFinish(m *gonsq.Message) { v.record("FIN", m) }

func (v *verbLog) OnRequeue(m *gonsq.Message, _ time.Duration, _ bool) { v.record("REQ", m) }

func (v *verbLog) OnTouch(m *gonsq.Message) { v.record("TOUCH", m) }

func (v *verbLog) all() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return slices.Clone(v.verbs)
}

// TestAnIdentifierIsOnlyUniqueWithinOneNSQD holds the supersession rule to
// what an nsqd message identifier names.
//
// nsqd builds an identifier from a timestamp, its node ID and a sequence, so it
// is unique within one nsqd and not across them: two nsqds whose node IDs
// collide, and the default is a hash of the host name, issue the same
// identifier for two different messages published in the same tick. A
// consumer that discovers both through nsqlookupd took the second message for a
// redelivery of the first, refused the first one's Ack with ErrLeaseLost and
// sent its nsqd a REQ, so a message whose work was done came back.
func TestAnIdentifierIsOnlyUniqueWithinOneNSQD(t *testing.T) {
	server := nsqfake.New()
	b, err := BackendWithClient(context.Background(), server.Producer(), driverkit.ClientOwned, Config{
		Addrs:       []string{"nsqfake:4150"},
		AckDeadline: 30 * time.Second,
		LeaseGuard:  time.Second,
		Clock:       server,
	})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	opened, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 4, Metadata: mq.MetadataNativeStrict,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	src := opened.(*source)

	log := &verbLog{}
	var id gonsq.MessageID
	copy(id[:], "0000000000000001")
	deliver := func(nsqd, body string) mq.DriverDelivery {
		m := gonsq.NewMessage(id, []byte(body))
		m.Attempts = 1
		m.NSQDAddress = nsqd
		m.Delegate = log
		if err := src.handle(m); err != nil {
			t.Fatalf("handle: %v", err)
		}
		return wireFetch(t, src)
	}
	fromA := deliver("nsqd-a:4150", "order 17 from a")
	fromB := deliver("nsqd-b:4150", "order 99 from b")

	if err := src.Settler().Ack(context.Background(), &fromA); err != nil {
		t.Fatalf("the Ack of nsqd-a's message (%q) returned %v after nsqd-b delivered a different message (%q) "+
			"under the same identifier; frames written: %q", fromA.Message.Body, err, fromB.Message.Body, log.all())
	}
	if err := src.Settler().Ack(context.Background(), &fromB); err != nil {
		t.Fatalf("the Ack of nsqd-b's message: %v", err)
	}
	if got, want := log.all(), []string{"FIN nsqd-a:4150", "FIN nsqd-b:4150"}; !slices.Equal(got, want) {
		t.Fatalf("the frames written are %q, want %q", got, want)
	}
	if n := b.Stale(); n != 0 {
		t.Fatalf("Stale() = %d: a message from one nsqd was counted as superseded by another's", n)
	}
}
