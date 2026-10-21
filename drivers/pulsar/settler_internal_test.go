package pulsar

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
)

// nackCounter is a consumer that counts the negative acknowledgements written
// through it. Everything else is the embedded nil interface, which a test that
// reaches it fails on loudly.
type nackCounter struct {
	pulsar.Consumer
	nacks atomic.Int32
}

// NackID implements pulsar.Consumer.
func (c *nackCounter) NackID(pulsar.MessageID) { c.nacks.Add(1) }

// TestADeliveryPastItsGuardIsGivenBackAndForgotten holds the lease check's
// terminal transition: the sweeper only gives back an ACTIVE delivery, so a
// settlement that finds the guard passed and takes the delivery out of
// circulation has to give the message back and forget it itself, or the
// message is never redelivered and the in-flight map keeps it for ever.
func TestADeliveryPastItsGuardIsGivenBackAndForgotten(t *testing.T) {
	cons := &nackCounter{}
	src := &source{b: &Backend{}, cons: cons, lease: time.Minute, inflight: map[BatchPosition]*delivery{}}
	set := &settler{src: src}
	ref := &delivery{src: src, pos: BatchPosition{Entry: 1}}
	ref.expires.Store(1)
	src.supersede(ref)
	d := mq.DriverDelivery{Ref: ref}
	if err := set.Extend(t.Context(), &d, time.Second); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend past the guard returned %v, want ErrLeaseLost", err)
	}
	if n := cons.nacks.Load(); n != 1 {
		t.Fatalf("the lost delivery was given back %d times, want once", n)
	}
	src.mu.Lock()
	n := len(src.inflight)
	src.mu.Unlock()
	if n != 0 {
		t.Fatalf("the in-flight map holds %d lost deliveries, want 0", n)
	}
}
