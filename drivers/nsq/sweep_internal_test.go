package nsq

import (
	"context"
	"sync"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/nsq/nsqfake"
)

// heldRequeue is a message delegate whose FIRST requeue blocks until the test
// releases it, which is what holds the lease sweeper between its scan and the
// rest of its give-backs. Every later requeue passes straight through.
type heldRequeue struct {
	once    sync.Once
	first   chan gonsq.MessageID
	release chan struct{}
}

func (h *heldRequeue) OnFinish(*gonsq.Message) {}

func (h *heldRequeue) OnTouch(*gonsq.Message) {}

func (h *heldRequeue) OnRequeue(m *gonsq.Message, _ time.Duration, _ bool) {
	held := false
	h.once.Do(func() { held = true })
	if !held {
		return
	}
	h.first <- m.ID
	<-h.release
}

// signalRequeue reports that its message was requeued.
type signalRequeue struct{ done chan struct{} }

func (s signalRequeue) OnFinish(*gonsq.Message) {}

func (s signalRequeue) OnTouch(*gonsq.Message) {}

func (s signalRequeue) OnRequeue(*gonsq.Message, time.Duration, bool) { close(s.done) }

// TestTheSweeperCountsOnlyWhatItGaveBack holds the sweeper's stale count to
// the deliveries the sweeper itself took out of circulation.
//
// The sweeper picks what is due under the source's lock and gives it back
// outside it, so a settlement can take a delivery in between: one whose lease
// check passed before the scan and whose claim lands after it, or one the
// lease check itself takes as lost. The sweeper counted every delivery it had
// picked whether or not its own give-back won, so the first was counted stale
// although it was settled, and the second was counted twice. Each case holds
// the sweeper on its first give-back, settles the other due delivery the way
// the case describes, and lets it go. A third delivery, already lost, is
// answered after the due ones in the same sweep, which is how the test knows
// the sweep has finished with them.
func TestTheSweeperCountsOnlyWhatItGaveBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		take func(t *testing.T, src *source, d *delivery)
		want uint64
	}{
		{"settled after the scan", func(t *testing.T, _ *source, d *delivery) {
			// The settlement's lease check ran before the scan; this is
			// its claim landing after it.
			if !d.claim() {
				t.Fatal("the delivery was no longer active")
			}
		}, 1},
		{"taken by the lease check after the scan", func(t *testing.T, src *source, d *delivery) {
			dd := mq.DriverDelivery{Ref: d, Gen: d.gen}
			if err := src.settleFn.Ack(context.Background(), &dd); err == nil {
				t.Fatal("an Ack inside the lease guard succeeded")
			}
		}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const guard = time.Second
			server := nsqfake.New()
			b, err := BackendWithClient(context.Background(), server.Producer(), driverkit.ClientOwned, Config{
				Addrs:       []string{"nsqfake:4150"},
				AckDeadline: 30 * time.Second,
				LeaseGuard:  guard,
				Clock:       server,
			})
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			opened, err := b.OpenSource(context.Background(), mq.SourceConfig{
				Topic: "orders", Subscription: "workers", Prefetch: 4,
				Metadata: mq.MetadataNativeStrict,
			})
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			src := opened.(*source)

			gate := &heldRequeue{first: make(chan gonsq.MessageID, 1), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			// Close waits for the sweeper, so a failed case must not leave it held.
			t.Cleanup(func() {
				release()
				_ = src.Close()
			})
			swept := signalRequeue{done: make(chan struct{})}

			// Both due deliveries are inside the guard and short of their
			// deadline at any scan in the next guard less ten milliseconds,
			// and the sweeper scans every half guard.
			now := b.now()
			due := map[gonsq.MessageID]*delivery{}
			src.mu.Lock()
			for i, del := range []gonsq.MessageDelegate{gate, gate, swept} {
				var id gonsq.MessageID
				copy(id[:], "0000000000000000")
				id[15] = byte('a' + i)
				msg := gonsq.NewMessage(id, []byte("x"))
				msg.Delegate = del
				d := &delivery{msg: msg, id: id, src: src, gen: src.gen.Add(1), received: now}
				d.expires.Store(now.Add(guard - 10*time.Millisecond).UnixNano())
				if i == 2 {
					d.expires.Store(now.Add(time.Hour).UnixNano())
					d.state.Store(stateLost)
				} else {
					due[id] = d
				}
				src.inflight[id] = d
			}
			src.mu.Unlock()

			var first gonsq.MessageID
			select {
			case first = <-gate.first:
			case <-time.After(5 * time.Second):
				t.Fatal("the sweeper gave nothing back")
			}
			for id, d := range due {
				if id != first {
					tc.take(t, src, d)
				}
			}
			release()
			select {
			case <-swept.done:
			case <-time.After(5 * time.Second):
				t.Fatal("the sweeper did not finish the sweep")
			}
			if got := b.Stale(); got != tc.want {
				t.Fatalf("Stale() = %d, want %d: the sweeper counted a delivery it did not give back", got, tc.want)
			}
		})
	}
}
