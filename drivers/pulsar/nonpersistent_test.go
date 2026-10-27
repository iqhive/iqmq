package pulsar_test

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// zeroPositionClient is the in-process model with a non-persistent topic's
// identifiers. The broker keeps nothing for such a topic and dispatches every
// one of its messages as entry 0:0 (NonPersistentTopic.publishMessage creates
// each entry at ledger 0, entry 0), so two different messages carry the same
// position. The model numbers its entries, so this client rewrites the
// identifiers on the way to the driver and back on the way to the model.
type zeroPositionClient struct {
	pulsarapi.Client
	stop chan struct{}
	wg   sync.WaitGroup
}

func (c *zeroPositionClient) Subscribe(o pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	outer := o.MessageChannel
	inner := make(chan pulsar.ConsumerMessage, cap(outer))
	o.MessageChannel = inner
	cons, err := c.Client.Subscribe(o)
	if err != nil {
		return nil, err
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.stop:
				return
			case cm := <-inner:
				cm.Message = zeroPositionMessage{cm.Message}
				select {
				case outer <- cm:
				case <-c.stop:
					return
				}
			}
		}
	}()
	return zeroPositionConsumer{cons}, nil
}

// zeroPositionID is a model identifier reporting entry 0:0.
type zeroPositionID struct{ pulsar.MessageID }

func (zeroPositionID) LedgerID() int64 { return 0 }
func (zeroPositionID) EntryID() int64  { return 0 }

type zeroPositionMessage struct{ pulsar.Message }

func (m zeroPositionMessage) ID() pulsar.MessageID { return zeroPositionID{m.Message.ID()} }

// zeroPositionConsumer hands the model back its own identifiers.
type zeroPositionConsumer struct{ pulsar.Consumer }

func modelID(id pulsar.MessageID) pulsar.MessageID {
	if z, ok := id.(zeroPositionID); ok {
		return z.MessageID
	}
	return id
}

func (c zeroPositionConsumer) AckID(id pulsar.MessageID) error { return c.Consumer.AckID(modelID(id)) }
func (c zeroPositionConsumer) NackID(id pulsar.MessageID)      { c.Consumer.NackID(modelID(id)) }

// TestNonPersistentDeliveriesDoNotSupersedeEachOther is two different messages
// of a non-persistent topic, both at 0:0. The source keyed its in-flight
// deliveries by position, which on a persistent topic is what a redelivery
// shares with the delivery it replaces, so the second message superseded the
// first and the first one's Ack was refused with ErrLeaseLost.
func TestNonPersistentDeliveriesDoNotSupersedeEachOther(t *testing.T) {
	const topic = "non-persistent://public/default/volatile"
	server := pulsarfake.New()
	cl := &zeroPositionClient{Client: server.Client(), stop: make(chan struct{})}
	t.Cleanup(func() { close(cl.stop); cl.wg.Wait() })
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, rigConfig(server))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src := sourceOf(t, b, topic, "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, topic, mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("one")})
	publish(t, snk, &mq.Message{Body: []byte("two")})
	ds := fetchN(t, src, 2)
	for i := range ds {
		if err := src.Settler().Ack(context.Background(), &ds[i]); err != nil {
			t.Errorf("the Ack of %q answered %v (code %v), want nil", ds[i].Message.Body, err, mq.CodeOf(err))
		}
	}
	if n := b.Stale(); n != 0 {
		t.Errorf("%d deliveries were counted stale, want 0: neither message was redelivered", n)
	}
}
