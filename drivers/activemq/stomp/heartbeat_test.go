package stomp_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// TestAFullBufferDoesNotStopTheConnectionsHeartBeats pins that a caller who
// stops fetching does not stop its source's connection.
//
// go-stomp writes a connection's heart-beats from the same process loop that
// hands each MESSAGE to its subscription with a blocking send, and the
// source's pump used to be the reader of that subscription: a full buffer
// parked the pump, the loop blocked behind it, the heart-beats stopped, and
// the broker dropped the connection and redelivered everything the
// subscription had not acknowledged. Measured against the pinned Artemis with
// the default configuration and a consumer that held its 32 deliveries for
// eight seconds: 64 duplicates in 120 messages, and the acknowledgements
// written during the pause either returned nil and were lost with the socket
// or failed with `msg send timeout`.
//
// The server here does what both brokers do to a client whose heart-beats
// stop -- it closes the socket and says nothing -- and the deliveries stay
// inside what each broker's own window would let it send, because that window
// is the one bound the fix relies on:
//
//   - Artemis's window is frame BYTES, so forty small frames are a few
//     kilobytes of the default mebibyte and all of them arrive at a consumer
//     whose buffer holds two.
//   - Classic's window is the prefetch COUNT, so forty arrive only at a
//     consumer that asked for forty; a one-byte PrefetchBytes then leaves its
//     buffer room for one of them, which is the configuration that stalled the
//     pinned Classic: prefetch 64 and a byte budget, 28 duplicates in 120.
//
// Either way the deliveries outnumber the buffer, go-stomp's sixteen-deep
// channel and the frame its loop holds, so nothing but a reader that never
// waits on the caller keeps the loop running.
func TestAFullBufferDoesNotStopTheConnectionsHeartBeats(t *testing.T) {
	t.Parallel()
	const (
		// interval is the heart-beat the server asks for and the client
		// sends; silence is how long the server tolerates none, which is
		// thirty missed heart-beats rather than the brokers' two or so, so
		// that a loaded runner cannot make a drop out of a late one.
		interval = 20 * time.Millisecond
		silence  = 30 * interval
		total    = 40
	)
	cases := []struct {
		name          string
		server        string
		broker        activemq.Broker
		prefetch      int
		prefetchBytes int64
	}{
		{name: "artemis", server: activemq.ServerArtemis + "/2.44.0", broker: activemq.Artemis, prefetch: 2},
		{name: "classic", server: activemq.ServerClassic + "/6.2.0", broker: activemq.Classic, prefetch: total, prefetchBytes: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newServer(t, stompsrv.Options{
				ServerHeader:      tc.server,
				WantHeartBeats:    interval,
				DropSilentClients: silence,
			})
			cfg := settings(tc.broker)
			cfg.Addrs = []string{srv.Addr()}
			cfg.HeartbeatSend = interval
			cfg.PrefetchBytes = tc.prefetchBytes
			b := openBackend(t, cfg)
			src, consume := openSource(t, b, cfg, srv, "orders", "workers", tc.prefetch)
			// The source dials a connection of its own, and the backend's
			// heart-beats too, so the source's is named by the connection its
			// SUBSCRIBE arrived on.
			subs := srv.FramesOf(frame.SUBSCRIBE)
			if len(subs) != 1 {
				t.Fatalf("the server received %d SUBSCRIBE frames, want 1:%s", len(subs), frameLog(srv))
			}
			own := subs[0].Conn

			msgs := make([]stompsrv.Message, total)
			for i := range msgs {
				msgs[i] = body(strconv.Itoa(i))
			}
			deliver(t, srv, consume, msgs...)
			held := fetchN(t, src, 1)

			// The caller now fetches nothing for longer than the server's
			// silence, measured in the source connection's own heart-beats:
			// enough of them to span the silence one and a half times over.
			// A loop that stopped writing them never reaches the count.
			from := srv.HeartBeats(own)
			if err := srv.WaitHeartBeats(own, from+3*int(silence/interval)/2, budget); err != nil {
				t.Fatalf("the source's connection stopped heart-beating while its buffer was full: %v. The "+
					"server closed %d connection(s) for silence and the source has re-dialled %d time(s); a broker "+
					"does the same, and redelivers every delivery the subscription had not acknowledged",
					err, srv.SilentDrops(), src.Redials())
			}
			if n := srv.SilentDrops(); n != 0 {
				t.Fatalf("the server closed %d connection(s) for silence, want none", n)
			}

			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			settler := src.Settler()
			seen := map[string]int{}
			ack := func(d *mq.DriverDelivery) {
				t.Helper()
				if err := settler.Ack(ctx, d); err != nil {
					t.Fatalf("Ack of %q: %v. The connection it was delivered on was dropped while the "+
						"caller was not fetching", d.Message.Body, err)
				}
				seen[string(d.Message.Body)]++
			}
			for i := range held {
				ack(&held[i])
			}
			// Settled as they arrive: a token slot is freed only by a
			// settlement, so a caller that fetched everything first would wait
			// on the table, which is a different and deliberate wait.
			for len(seen) < total {
				for _, d := range fetchN(t, src, 1) {
					if d.Info.Redelivered {
						t.Errorf("delivery %q arrived marked redelivered", d.Message.Body)
					}
					ack(&d)
				}
			}
			if len(seen) != total {
				t.Fatalf("%d distinct deliveries arrived, want %d", len(seen), total)
			}
			if got := src.Redials(); got != 0 {
				t.Errorf("the source re-dialled %d time(s), want none", got)
			}
			// Every acknowledgement reached the server on the connection the
			// deliveries came in on, which is the one that settles them.
			if err := srv.WaitFrames(frame.ACK, total, budget); err != nil {
				t.Fatalf("%v:%s", err, frameLog(srv))
			}
			for _, f := range srv.FramesOf(frame.ACK) {
				if f.Conn != own {
					t.Errorf("an ACK arrived on connection %d, want %d, the one the deliveries came in on", f.Conn, own)
				}
			}
		})
	}
}
