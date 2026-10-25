package stomp_test

import (
	"slices"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// poisonBody is a body the portable envelope claims and cannot open: the
// magic and version are right and nothing after them is.
var poisonBody = []byte("IQMQ\x01\xff\xff\xff\xff")

// TestAnUndecodableDeliveryIsGivenBack is the consumer that stopped for good.
//
// A delivery the source could not decode used to stay unacknowledged with no
// way to settle it, and each one holds a message of the broker's flow-control
// window. `prefetch` of them stopped the subscription, and a reconnect
// redelivered them and stopped it again. Where the configuration has a
// dead-letter route the source now takes it: a copy to Config.DeadLetterTopic
// and then an ACK, or on Classic with no topic a NACK, which Classic
// poison-acks onto its own dead-letter strategy. Artemis with no topic has no
// frame that is not an acknowledgement, so there the delivery is still left
// alone, and the case below pins that it is not destroyed.
func TestAnUndecodableDeliveryIsGivenBack(t *testing.T) {
	for _, tc := range []struct {
		name       string
		broker     activemq.Broker
		deadLetter string
		// settle is the frame that must name the delivery, or "" for none.
		settle string
		copied bool
	}{
		{"classic/no-topic", activemq.Classic, "", "NACK", false},
		{"classic/topic", activemq.Classic, "poison", "ACK", true},
		{"artemis/topic", activemq.Artemis, "poison", "ACK", true},
		{"artemis/no-topic", activemq.Artemis, "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := brokerServer(t, tc.broker)
			cfg := settings(tc.broker)
			cfg.Addrs = []string{srv.Addr()}
			cfg.Metadata = mq.MetadataPortableEnvelope
			cfg.DeadLetterTopic = tc.deadLetter
			b := openBackend(t, cfg)
			src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
			ids := deliver(t, srv, consume, stompsrv.Message{Body: poisonBody})
			await(t, "the source to refuse the delivery", func() bool { return src.Undecodable() == 1 })
			if tc.settle != "" {
				await(t, "the "+tc.settle+" of the undecodable delivery", func() bool {
					return slices.Contains(srv.SettledIDs(tc.settle), ids[0])
				})
			}
			for _, cmd := range []string{"ACK", "NACK"} {
				if cmd != tc.settle && slices.Contains(srv.SettledIDs(cmd), ids[0]) {
					t.Fatalf("the undecodable delivery was settled with %s; want %q\n%s", cmd, tc.settle, frameLog(srv))
				}
			}
			// The source publishes nothing else, so any SEND is the copy. Its
			// body is not compared: the dead-letter sink wraps it in the
			// configured envelope like any other publication.
			copied := len(srv.FramesOf("SEND")) > 0
			if copied != tc.copied {
				t.Fatalf("a dead-letter copy was published = %v, want %v\n%s", copied, tc.copied, frameLog(srv))
			}
		})
	}
}
