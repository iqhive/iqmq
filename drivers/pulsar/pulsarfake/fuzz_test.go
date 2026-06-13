package pulsarfake

import (
	"context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// FuzzModel holds the model itself to the invariants a broker has, because
// every conclusion the driver's tests draw rests on them.
//
// It is a differential fuzz against ARITHMETIC rather than against a second
// implementation: the model's own accounting is what the driver's tests read,
// so the properties worth checking are the conservation laws. Every message
// that was published is settled, out with a consumer, waiting in the backlog or
// waiting for a delivery time — and nothing is settled that nobody asked to
// settle, which is the disqualifier counted.
//
// The op script is one byte per operation and reads no helper. The fake is a
// nested module's test support, and taking the root module's internal
// fuzz-script helper for it would make a change there able to break a driver's
// fake (the reasoning  recorded).
func FuzzModel(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 2})
	f.Add([]byte{0, 0, 1, 1, 3, 1, 2})
	f.Add([]byte{0, 0, 0, 0, 4, 1, 1, 2, 2})
	f.Add([]byte{0, 5, 0, 1, 6, 1, 2})
	f.Add([]byte{0, 0, 0, 1, 1, 1, 3, 4, 2})
	f.Fuzz(func(t *testing.T, script []byte) {
		const topic = "persistent://public/default/fuzzed"
		const sub = "workers"
		server := New(WithBatchSize(3), WithNackRedeliveryDelay(time.Second))
		cl := server.Client()
		prod, err := cl.CreateProducer(pulsar.ProducerOptions{
			Topic: topic, BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
		})
		if err != nil {
			t.Fatalf("CreateProducer: %v", err)
		}
		defer prod.Close()
		ch := make(chan pulsar.ConsumerMessage, 64)
		cons, err := cl.Subscribe(pulsar.ConsumerOptions{
			Topic: topic, SubscriptionName: sub, Type: pulsar.Shared,
			MessageChannel: ch, ReceiverQueueSize: 64,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			NackRedeliveryDelay:         time.Second,
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		defer cons.Close()

		published := 0
		var held []pulsar.Message
		acked := 0
		for _, op := range script {
			switch op % 7 {
			case 0: // publish
				key := string(rune('a' + int(op/7)%3))
				if _, err := prod.Send(context.Background(), &pulsar.ProducerMessage{
					Payload: []byte{op}, Key: key,
				}); err != nil {
					t.Fatalf("Send: %v", err)
				}
				published++
			case 1: // receive
				select {
				case cm := <-ch:
					held = append(held, cm.Message)
				default:
				}
			case 2: // acknowledge the oldest held delivery
				if len(held) == 0 {
					continue
				}
				m := held[0]
				held = held[1:]
				if err := cons.AckID(m.ID()); err != nil {
					t.Fatalf("AckID: %v", err)
				}
				acked++
			case 3: // give the oldest held delivery back
				if len(held) == 0 {
					continue
				}
				cons.NackID(held[0].ID())
				held = held[1:]
			case 4: // time passes
				server.Advance(time.Second)
			case 5: // the producer's open batch is sent
				server.Flush()
			case 6: // everything unsettled comes back
				server.Redeliver(topic, sub)
				held = nil
			}
			// THE CONSERVATION LAW, after every operation.
			st := server.Stats(topic, sub)
			total := server.Messages(topic)
			if total != published {
				t.Fatalf("the topic holds %d messages and %d were published", total, published)
			}
			settled := st.Acked + st.CrossSettled
			accounted := settled + server.InFlight(topic, sub) + server.Backlog(topic, sub)
			if accounted != published {
				t.Fatalf("%d published, %d settled, %d in flight, %d in the backlog: %d accounted for",
					published, settled, server.InFlight(topic, sub), server.Backlog(topic, sub), accounted)
			}
			// Nothing is settled that nobody asked to settle. Every
			// acknowledgement here uses the identifier the consumer
			// handed out, and batch-index acknowledgement is off, so
			// the client holds each frame back until its entry is
			// complete — which is the one safe combination.
			if st.CrossSettled != 0 {
				t.Fatalf("%d message(s) were settled without being acknowledged: the model settled a "+
					"sibling of a batched entry through a tracked identifier, which is the one "+
					"thing it must never do", st.CrossSettled)
			}
			if st.Acked > st.Delivered {
				t.Fatalf("%d acknowledged of %d delivered", st.Acked, st.Delivered)
			}
			if st.Acked != acked && st.Acked+st.CrossSettled != acked {
				// An acknowledgement of a partly complete batch
				// reaches the broker only once the batch is
				// complete, so the broker's count LAGS the
				// application's. It may never lead it.
				if st.Acked > acked {
					t.Fatalf("the broker acknowledged %d and the application %d; the broker cannot "+
						"be ahead", st.Acked, acked)
				}
			}
		}
	})
}
