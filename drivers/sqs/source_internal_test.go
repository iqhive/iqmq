package sqs

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// TestAReceiveAdmittedLateDoesNotSupersedeAFresherOne is two pollers
// receiving the same message. Poller A's answer holds it behind a message the
// full ring will not take, so A parks before admitting it while its window
// runs out. Poller B then receives the message again, with a fresh receipt and
// a whole window, and admits it. When the ring makes room, A admits its own
// copy, whose window closed long ago. The copy admitted last used to become
// the one the source vouched for, so B's fresh delivery was marked lost, Fetch
// dropped A's as expired and B's as lost, and the message sat invisible under
// B's receipt for a whole AckDeadline with nobody working on it. Had B's
// delivery been handed out already, its Ack was refused with ErrLeaseLost and
// the message was processed twice.
func TestAReceiveAdmittedLateDoesNotSupersedeAFresherOne(t *testing.T) {
	const deadline = 30 * time.Second
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	clk := &stepClock{at: server.Now()}
	b, err := BackendWithAPI(t.Context(), server, server, driverkit.ClientBorrowed, Config{
		Region: "fake-region-1", AccessKeyID: "AKIAFAKE", SecretAccessKey: "not-a-real-secret",
		AutoCreate: true, WaitTime: time.Second, AckDeadline: deadline, Clock: clk,
		Pollers: 2, MaxMessages: 1,
	})
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	url, err := b.res.URL(t.Context(), "t")
	if err != nil {
		t.Fatalf("URL: %v", err)
	}
	if _, err := server.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("d"),
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	receive := func() (*types.Message, time.Time) {
		t.Helper()
		out, err := server.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 1,
			VisibilityTimeout: visibilitySeconds(deadline), MessageAttributeNames: []string{"All"},
		})
		if err != nil || len(out.Messages) != 1 {
			t.Fatalf("ReceiveMessage: %d message(s), %v", len(out.Messages), err)
		}
		return &out.Messages[0], clk.Now()
	}
	stale, staleAt := receive()
	server.Advance(deadline + time.Second)
	clk.advance(deadline + time.Second)
	fresh, freshAt := receive()

	enc, err := newEncoder(b.cfg, b.caps, mq.MetadataAdaptive)
	if err != nil {
		t.Fatalf("newEncoder: %v", err)
	}
	src, err := b.newSource(mq.SourceConfig{Topic: "t", Subscription: "s", Prefetch: 4}, enc, url)
	if err != nil {
		t.Fatalf("newSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	// B admits first; A, parked since its receive, admits second.
	for _, rx := range []struct {
		m  *types.Message
		at time.Time
	}{{fresh, freshAt}, {stale, staleAt}} {
		if ok, err := src.admit(t.Context(), rx.m, rx.at, nil); !ok || err != nil {
			t.Fatalf("admitting %s: %v, %v", aws.ToString(rx.m.ReceiptHandle), ok, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	dst := make([]mq.DriverDelivery, 4)
	n, err := src.Fetch(ctx, dst)
	if n != 1 {
		t.Fatalf("Fetch handed out %d deliveries (%v), want the fresh one: its receipt has a whole window left, "+
			"and the copy admitted after it, whose window had closed, marked it lost (%d dropped)", n, err, src.Stale())
	}
	if got, want := dst[0].Ref.(*delivery).handle, aws.ToString(fresh.ReceiptHandle); got != want {
		t.Fatalf("Fetch handed out receipt %s, want the fresh %s", got, want)
	}
	if err := src.set.Ack(t.Context(), &dst[0]); err != nil {
		t.Fatalf("the fresh delivery's Ack returned %v", err)
	}
	if got := server.Depth(QueueNameOfURL(url)); got != 0 {
		t.Fatalf("the queue holds %d message(s) after the fresh delivery's Ack, want 0", got)
	}
}

// errDeleteRefused is a definite failure: a service answer that is not one of
// the mapped codes, so the settlement reports it as a plain refusal.
var errDeleteRefused = errors.New("the delete was refused")

// outpolled is the fake service as one host of several sees it: once lost is
// set, every ReceiveMessage this host makes comes back empty, because the
// other hosts' pollers got there first. diverted counts those calls. A long
// poll that was already parked in the service when lost was set still
// receives, so a test waits for a diverted call, which the source's one
// poller makes only once that poll has returned.
type outpolled struct {
	*sqsfake.Server
	lost     atomic.Bool
	diverted atomic.Int32
}

func (p *outpolled) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if !p.lost.Load() {
		return p.Server.ReceiveMessage(ctx, in, opts...)
	}
	p.diverted.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return &sqs.ReceiveMessageOutput{}, nil
	}
}

// serviceClock is the fake service's clock as an [mq.Clock], so that the
// driver's local lease check and the service agree on when a window has
// closed. Its timers are real ones.
type serviceClock struct{ s *sqsfake.Server }

func (c serviceClock) Now() time.Time { return c.s.Now() }

func (serviceClock) NewTimer(d time.Duration) mq.Timer { return wallTimer{t: time.NewTimer(d)} }

type wallTimer struct{ t *time.Timer }

func (w wallTimer) C() <-chan time.Time        { return w.t.C }
func (w wallTimer) Stop() bool                 { return w.t.Stop() }
func (w wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// TestADeliveryRunAbandonedLeavesTheInflightMapOnceItsWindowCloses is the
// sibling of TestAnAmbiguousSettlementLeavesTheInflightMap. Run offers a
// settlement the driver refused definitely two more times and then abandons
// the delivery without telling the driver, and a definite refusal leaves the
// delivery active in the in-flight map with its whole message. Only a
// redelivery of the message to this source, or Close, removed it. On a queue
// several hosts consume, the redelivery can go to another host, which deletes
// the message, and this host kept the entry for the life of the source: a
// DeleteMessageBatch throttled or unreachable for longer than Run's three
// attempts was enough, and every message it caught stayed in memory.
func TestADeliveryRunAbandonedLeavesTheInflightMapOnceItsWindowCloses(t *testing.T) {
	const n = 8
	server := sqsfake.New(sqsfake.Options{})
	api := &outpolled{Server: server}
	b, err := BackendWithAPI(t.Context(), api, server, driverkit.ClientBorrowed, Config{
		Region: "fake-region-1", AccessKeyID: "AKIAFAKE", SecretAccessKey: "not-a-real-secret",
		AutoCreate: true, WaitTime: time.Second, AckDeadline: 30 * time.Second, Clock: serviceClock{s: server},
	})
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	url, err := b.res.URL(t.Context(), "t")
	if err != nil {
		t.Fatalf("URL: %v", err)
	}
	body := strings.Repeat("x", 64<<10)
	for i := range n {
		if _, err := server.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(url), MessageBody: aws.String(strconv.Itoa(i) + body),
		}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}
	refuse := server.Fault(sqsfake.Fault{Op: "DeleteMessageBatch", Err: errDeleteRefused, Count: -1})

	c := mq.NewClient(b)
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "t", Subscription: "s", Prefetch: n})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	runCtx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- cons.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	}()
	t.Cleanup(func() {
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
		_ = cons.Close()
	})
	// Every Ack is refused three times, and Run then abandons the delivery.
	waitUntil(t, 20*time.Second, func() bool { return cons.Stats().SettleFailures >= 3*n }, func() string {
		return strconv.FormatInt(cons.Stats().SettleFailures, 10) + " settlement failure(s), want " +
			strconv.Itoa(3*n)
	})

	// The other hosts receive the redeliveries and delete them. The windows
	// close only once this host's long poll that was parked in the service
	// has returned: Advance wakes a parked poll, which would take the
	// redeliveries first.
	api.lost.Store(true)
	waitUntil(t, 5*time.Second, func() bool { return api.diverted.Load() > 0 }, func() string {
		return "this host made no ReceiveMessage after it lost the race for the redeliveries"
	})
	server.Remove(refuse)
	server.Advance(31 * time.Second)
	for got := 0; got < n; {
		out, err := server.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 10, VisibilityTimeout: 30,
		})
		if err != nil || len(out.Messages) == 0 {
			t.Fatalf("the other host received %d of %d redeliveries: %v", got, n, err)
		}
		for i := range out.Messages {
			if _, err := server.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
				QueueUrl: aws.String(url), ReceiptHandle: out.Messages[i].ReceiptHandle,
			}); err != nil {
				t.Fatalf("the other host's DeleteMessage: %v", err)
			}
			got++
		}
	}

	src := onlySource(t, b)
	waitUntil(t, 5*time.Second, func() bool { held, _ := inflightOf(src); return held == 0 }, func() string {
		held, bytes := inflightOf(src)
		return "the source still holds " + strconv.Itoa(held) + " abandoned deliver(ies) in its in-flight map, " +
			strconv.Itoa(bytes) + " bytes of message bodies, although their windows closed and another host " +
			"deleted their messages"
	})
}

// TestASweepLeavesWhatAWindowOrAnotherCallStillHolds holds the sweep to the
// deliveries no answer depends on: a window that has closed, and no settlement
// or extension in flight. A settlement's outcome decides what becomes of its
// delivery, and an extension in flight can move the window the sweep reads.
func TestASweepLeavesWhatAWindowOrAnotherCallStillHolds(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	src := &source{b: &Backend{}, inflight: map[string]*delivery{}}
	for _, tc := range []struct {
		id        string
		expires   time.Time
		state     uint32
		extending bool
	}{
		{"closed", now.Add(-time.Second), stateActive, false},
		{"closing as the sweep reads it", now, stateActive, false},
		{"open", now.Add(time.Second), stateActive, false},
		{"settling", now.Add(-time.Second), stateSettling, false},
		{"extending", now.Add(-time.Second), stateActive, true},
	} {
		ref := &delivery{id: tc.id, src: src}
		ref.extend(tc.expires)
		ref.state.Store(tc.state)
		if tc.extending {
			ref.extending.Store(&extension{answered: make(chan struct{})})
		}
		src.inflight[tc.id] = ref
	}
	src.sweep(now)
	for _, id := range []string{"closed", "closing as the sweep reads it"} {
		if _, ok := src.inflight[id]; ok {
			t.Errorf("the sweep kept the %s delivery, whose window has closed and which nothing holds", id)
		}
	}
	for _, id := range []string{"open", "settling", "extending"} {
		if _, ok := src.inflight[id]; !ok {
			t.Errorf("the sweep forgot the %s delivery", id)
		}
	}
}

// waitUntil polls cond until it holds, or fails with what describes the state
// it was waiting on.
func waitUntil(t *testing.T, budget time.Duration, cond func() bool, what func() string) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("after %v: %s", budget, what())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// onlySource is b's one open source.
func onlySource(t *testing.T, b *Backend) *source {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for src := range b.srcs {
		return src
	}
	t.Fatal("no open source")
	return nil
}

// inflightOf is how many deliveries src's in-flight map holds, and how many
// bytes of message bodies they keep.
func inflightOf(src *source) (int, int) {
	src.mu.Lock()
	defer src.mu.Unlock()
	bytes := 0
	for _, ref := range src.inflight {
		if ref.native != nil {
			bytes += len(aws.ToString(ref.native.Body))
		}
	}
	return len(src.inflight), bytes
}

// TestARequeueWithdrawsTheRestOfItsGroupStillBeingAdmitted is the moment the
// FIFO requeue tests cannot force: the consumer is handed m1 and requeues it
// while the poller is still admitting m2, which came in the same answer. The
// withdrawal scanned the in-flight map, where m2 was not yet, and m2 was then
// pushed and handed out ahead of m1. The withdrawal is recorded on the answer
// as well, so m2's admission gives it back instead. When the poller was held
// on a full ring until m2's window had closed, its handle may name another
// consumer's copy, so the admission withdraws it without releasing it, as
// Close and the withdrawal do.
func TestARequeueWithdrawsTheRestOfItsGroupStillBeingAdmitted(t *testing.T) {
	for _, tc := range []struct {
		name string
		// late is how far the clock moves before m2 is admitted.
		late      time.Duration
		invisible int
	}{
		{"window open", 0, 0},
		{"window closed", time.Hour + time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			clk := &stepClock{at: server.Now()}
			b, err := BackendWithAPI(t.Context(), server, server, driverkit.ClientBorrowed, Config{
				Region: "fake-region-1", AccessKeyID: "AKIAFAKE", SecretAccessKey: "not-a-real-secret",
				AutoCreate: true, FIFO: true, WaitTime: time.Second, AckDeadline: time.Hour, Clock: clk,
			})
			if err != nil {
				t.Fatalf("BackendWithAPI: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			url, err := b.res.URL(t.Context(), "t")
			if err != nil {
				t.Fatalf("URL: %v", err)
			}
			for _, body := range []string{"m1", "m2"} {
				if _, err := server.SendMessage(t.Context(), &sqs.SendMessageInput{
					QueueUrl: aws.String(url), MessageBody: aws.String(body),
					MessageGroupId: aws.String("G"), MessageDeduplicationId: aws.String(body),
				}); err != nil {
					t.Fatalf("SendMessage: %v", err)
				}
			}
			out, err := server.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
				QueueUrl: aws.String(url), MaxNumberOfMessages: 10, VisibilityTimeout: visibilitySeconds(time.Hour),
			})
			if err != nil || len(out.Messages) != 2 {
				t.Fatalf("ReceiveMessage: %d message(s), %v; want m1 and m2 in one answer", len(out.Messages), err)
			}
			enc, err := newEncoder(b.cfg, b.caps, mq.MetadataAdaptive)
			if err != nil {
				t.Fatalf("newEncoder: %v", err)
			}
			src, err := b.newSource(mq.SourceConfig{Topic: "t", Subscription: "s", Prefetch: 4}, enc, url)
			if err != nil {
				t.Fatalf("newSource: %v", err)
			}
			t.Cleanup(func() { _ = src.Close() })
			ans, answered := &answer{}, b.now()
			if ok, err := src.admit(t.Context(), &out.Messages[0], answered, ans); !ok || err != nil {
				t.Fatalf("admitting m1: %v, %v", ok, err)
			}
			dst := make([]mq.DriverDelivery, 4)
			if n, err := src.Fetch(t.Context(), dst); n != 1 || err != nil {
				t.Fatalf("Fetch: %d, %v; want m1", n, err)
			}
			if err := src.set.Nack(t.Context(), &dst[0], mq.NackOptions{Action: mq.NackActionRequeue}); err != nil {
				t.Fatalf("Nack: %v", err)
			}
			// The poller admits m2 only now. The service's clock stands still,
			// so m2 is still in flight there whatever the driver's clock says.
			clk.advance(tc.late)
			if ok, err := src.admit(t.Context(), &out.Messages[1], answered, ans); !ok || err != nil {
				t.Fatalf("admitting m2: %v, %v", ok, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if n, _ := src.Fetch(ctx, dst); n != 0 {
				t.Fatalf("Fetch handed out %q after m1 was requeued; it came in m1's answer and must wait behind it",
					dst[0].Message.Body)
			}
			if n := server.Invisible(QueueNameOfURL(url)); n != tc.invisible {
				t.Fatalf("%d message(s) are invisible, want %d: m2 is given back with m1 while its window is open, "+
					"and its handle is left alone once the window has closed", n, tc.invisible)
			}
		})
	}
}
