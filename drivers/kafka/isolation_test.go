package kafka_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestOneCallersCancellationFailsNoOtherPublication: franz-go fails a
// partition's whole buffer when the first record of its head batch has a
// context that is done (its gapless ordering), and with
// AllowIdempotentProduceCancellation one record's context is also the
// context of the produce request carrying it. The driver produced a
// synchronous publication under its caller's context, ended by its
// publisher's Close as well, so publication A, whose caller gave up, whose
// deadline passed or whose publisher closed, failed publication B, whose
// caller did nothing, with a cancellation B's caller never made; an admitted
// ConfirmNone or asynchronous B was failed after its call had returned.
//
// A and B wait on one partition: behind a produce the broker holds, or in one
// batch that lingers. Config.UnboundedDelivery, which turns the option off,
// does not help with the second.
func TestOneCallersCancellationFailsNoOtherPublication(t *testing.T) {
	type setup struct {
		name      string
		linger    bool
		unbounded bool
	}
	type how struct {
		name     string
		deadline bool // A's per-call deadline passes rather than its caller cancelling
		closeA   bool // A's publisher closes rather than its caller cancelling
		confirmB mq.ConfirmationMode
		asyncB   bool
	}
	for _, st := range []setup{
		{name: "behind a held produce"},
		{name: "in a lingering batch", linger: true},
		{name: "in a lingering batch, UnboundedDelivery", linger: true, unbounded: true},
	} {
		for _, hw := range []how{
			{name: "A cancelled"},
			{name: "A's deadline passes", deadline: true},
			{name: "A's publisher closes", closeA: true},
			{name: "A cancelled, B ConfirmNone", confirmB: mq.ConfirmNone},
			{name: "A cancelled, B asynchronous", asyncB: true},
		} {
			t.Run(st.name+"/"+hw.name, func(t *testing.T) {
				topic := newTopic("isolation")
				cl := cluster(t, 1, []string{topic})
				c, b := openPair(t, cl, func(cfg *kq.Config) {
					cfg.UnboundedDelivery = st.unbounded
					if st.linger {
						cfg.Linger = 500 * time.Millisecond
					}
				})
				pf, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
				if err != nil {
					t.Fatalf("Publisher: %v", err)
				}
				pa, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
				if err != nil {
					t.Fatalf("Publisher A: %v", err)
				}
				pb, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: hw.confirmB})
				if err != nil {
					t.Fatalf("Publisher B: %v", err)
				}
				publish(t, c, topic, &mq.Message{Body: []byte("warm")})
				want := int64(3) // warm, A and B
				buffered := int64(0)
				release := func() {}
				filler := make(chan error, 1)
				if !st.linger {
					var held <-chan struct{}
					held, release = holdFirstProduce(t, cl)
					go func() { filler <- pf.Publish(context.Background(), &mq.Message{Body: []byte("filler")}) }()
					<-held
					want, buffered = 4, 1
				}
				actx, cancelA := context.WithCancel(context.Background())
				if hw.deadline {
					actx, cancelA = context.WithTimeout(context.Background(), 50*time.Millisecond)
				}
				defer cancelA()
				aerr := make(chan error, 1)
				go func() { aerr <- pa.Publish(actx, &mq.Message{Body: []byte("A")}) }()
				waitFor(t, 5*time.Second, "A buffered", func() bool { return b.Client().BufferedProduceRecords() >= buffered+1 })
				berr := make(chan error, 1)
				switch {
				case hw.asyncB:
					if err := pb.PublishAsync(context.Background(), &mq.Message{Body: []byte("B")}, func(r mq.PublishResult) { berr <- r.Err }); err != nil {
						t.Fatalf("PublishAsync B: %v", err)
					}
				default:
					go func() { berr <- pb.Publish(context.Background(), &mq.Message{Body: []byte("B")}) }()
				}
				waitFor(t, 5*time.Second, "B buffered", func() bool { return b.Client().BufferedProduceRecords() >= buffered+2 })
				if hw.confirmB == mq.ConfirmNone {
					if err := <-berr; err != nil {
						t.Fatalf("B's ConfirmNone Publish = %v, want nil", err)
					}
				}
				amb := b.AmbiguousPublications()
				switch {
				case hw.closeA:
					if err := pa.Close(); err != nil {
						t.Fatalf("closing publisher A: %v", err)
					}
				case hw.deadline:
					<-actx.Done()
				default:
					cancelA()
				}
				ea := <-aerr
				if ea == nil || !mq.IsAmbiguous(ea) {
					t.Errorf("A, whose wait ended, = %v, want an unknown outcome", ea)
				}
				time.Sleep(100 * time.Millisecond)
				release()
				if !st.linger {
					if err := <-filler; err != nil {
						t.Fatalf("filler: %v", err)
					}
				}
				var eb error
				if hw.confirmB != mq.ConfirmNone {
					eb = <-berr
				}
				if eb != nil {
					t.Errorf("B, whose caller did nothing, = %v (code %v): another publication's cancellation failed it", eb, mq.CodeOf(eb))
				}
				if err := pb.Flush(t.Context()); err != nil {
					t.Errorf("B's Flush: %v", err)
				}
				waitFor(t, 5*time.Second, "A and B in the log", func() bool { return cl.PartitionInfo(topic, 0).HighWatermark >= want })
				// A's own unknown outcome is the one ambiguous answer.
				if n := b.AmbiguousPublications() - amb; n != 1 {
					t.Errorf("%d ambiguous publications counted, want 1 (A's): B was failed after its call returned", n)
				}
			})
		}
	}
}

// TestABatchCancelledWhileWaitingForRoomFailsNoOtherPublication: a batch's
// records are handed to the producer one after the other, and the last ones
// can wait for room in a full buffer. A batch that let its caller's context
// reach its records until the whole loop had run left the records already
// buffered under that context, so a caller who gave up while a later record
// waited for room failed every publication queued on the partition, as a
// single Publish once did. Each record is admitted on its own: the ones
// buffered are out of the caller's reach, and only the one still waiting is
// refused, which is definite because it was never placed.
func TestABatchCancelledWhileWaitingForRoomFailsNoOtherPublication(t *testing.T) {
	topic := newTopic("isolation-batch")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.MaxBufferedRecords = 4 })
	pf, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	pa, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher A: %v", err)
	}
	pb, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher B: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("warm")})
	held, release := holdFirstProduce(t, cl)
	filler := make(chan error, 1)
	go func() { filler <- pf.Publish(context.Background(), &mq.Message{Body: []byte("filler")}) }()
	<-held
	berr := make(chan error, 1)
	go func() { berr <- pb.Publish(context.Background(), &mq.Message{Body: []byte("B")}) }()
	waitFor(t, 5*time.Second, "B buffered", func() bool { return b.Client().BufferedProduceRecords() >= 2 })

	// A0 and A1 fill the buffer, A2 waits for room, and A3 is not handed
	// over yet.
	actx, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	aerr := make(chan error, 1)
	go func() {
		aerr <- pa.PublishBatch(actx, []*mq.Message{{Body: []byte("A0")}, {Body: []byte("A1")}, {Body: []byte("A2")}, {Body: []byte("A3")}})
	}()
	waitFor(t, 5*time.Second, "A2 waiting for room", func() bool { return b.Client().BufferedProduceRecords() >= 5 })
	amb := b.AmbiguousPublications()
	cancelA()
	var ea error
	select {
	case ea = <-aerr:
	case <-time.After(5 * time.Second):
		t.Errorf("A's batch had not returned 5s after its context ended: a record after the cancellation was handed over")
		release()
		ea = <-aerr
	}
	var be *mq.BatchError
	if !errors.As(ea, &be) || len(be.Errs) != 4 {
		t.Fatalf("A's batch = %v, want a *BatchError of 4", ea)
	}
	for i, e := range be.Errs[:2] {
		if !mq.IsAmbiguous(e) {
			t.Errorf("A%d, buffered when its wait ended, = %v, want an unknown outcome", i, e)
		}
	}
	if e := be.Errs[2]; e == nil || mq.IsAmbiguous(e) || !errors.Is(e, context.Canceled) {
		t.Errorf("A2, refused while it waited for room, = %v, want a definite cancellation", e)
	}
	if e := be.Errs[3]; e == nil || mq.IsAmbiguous(e) || !errors.Is(e, context.Canceled) {
		t.Errorf("A3, not handed over when the context ended, = %v, want a definite cancellation", e)
	}
	time.Sleep(100 * time.Millisecond)
	release()
	if err := <-filler; err != nil {
		t.Errorf("filler, whose caller did nothing, = %v (code %v): A's cancellation failed it", err, mq.CodeOf(err))
	}
	if err := <-berr; err != nil {
		t.Errorf("B, whose caller did nothing, = %v (code %v): A's cancellation failed it", err, mq.CodeOf(err))
	}
	// warm, filler, B, A0 and A1.
	waitFor(t, 5*time.Second, "filler, B, A0 and A1 in the log", func() bool { return cl.PartitionInfo(topic, 0).HighWatermark >= 5 })
	if n := b.AmbiguousPublications() - amb; n != 2 {
		t.Errorf("%d ambiguous publications counted, want 2 (A0's and A1's)", n)
	}
}

// TestClosingAPublisherEndsAPublishWaitingForRoom: the core ends a
// publication it is waiting for by closing the sink, and a synchronous
// publication whose record waits for room in a full buffer, under a context
// that never ends, was produced under a context only that caller could end.
// The publisher's Close then waited until the buffer had room: up to
// Config.DeliveryTimeout, or for ever under UnboundedDelivery. Close ends the
// wait for room, the record is refused, never placed, so the answer is
// definite, and a batch's records not handed over yet are refused too.
func TestClosingAPublisherEndsAPublishWaitingForRoom(t *testing.T) {
	for _, unbounded := range []bool{false, true} {
		for _, batch := range []bool{false, true} {
			t.Run("unbounded="+strconv.FormatBool(unbounded)+"/batch="+strconv.FormatBool(batch), func(t *testing.T) {
				topic := newTopic("isolation-close")
				cl := cluster(t, 1, []string{topic})
				c, b := openPair(t, cl, func(cfg *kq.Config) {
					cfg.MaxBufferedRecords = 1
					cfg.UnboundedDelivery = unbounded
					cfg.DeliveryTimeout = 60 * time.Second
				})
				pf, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
				if err != nil {
					t.Fatalf("Publisher: %v", err)
				}
				pa, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
				if err != nil {
					t.Fatalf("Publisher A: %v", err)
				}
				publish(t, c, topic, &mq.Message{Body: []byte("warm")})
				held, release := holdFirstProduce(t, cl)
				filler := make(chan error, 1)
				go func() { filler <- pf.Publish(context.Background(), &mq.Message{Body: []byte("filler")}) }()
				<-held
				aerr := make(chan error, 1)
				go func() {
					if batch {
						aerr <- pa.PublishBatch(context.Background(), []*mq.Message{{Body: []byte("A0")}, {Body: []byte("A1")}})
						return
					}
					aerr <- pa.Publish(context.Background(), &mq.Message{Body: []byte("A")})
				}()
				waitFor(t, 5*time.Second, "A waiting for room", func() bool { return b.Client().BufferedProduceRecords() >= 2 })
				amb := b.AmbiguousPublications()
				closed := make(chan error, 1)
				go func() { closed <- pa.Close() }()
				select {
				case err := <-closed:
					if err != nil {
						t.Errorf("closing publisher A: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Errorf("publisher A's Close had not returned after 3s while A waited for buffer room")
				}
				var ea error
				select {
				case ea = <-aerr:
				case <-time.After(time.Second):
					t.Errorf("A's publication had not returned 1s after its publisher's Close")
					release()
					ea = <-aerr
				}
				errs := []error{ea}
				var be *mq.BatchError
				if errors.As(ea, &be) {
					errs = be.Errs
				}
				for i, e := range errs {
					if e == nil || mq.IsAmbiguous(e) {
						t.Errorf("A's record %d, refused when its publisher closed, = %v, want a definite refusal", i, e)
					}
				}
				release()
				if err := <-filler; err != nil {
					t.Errorf("filler: %v", err)
				}
				if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != 2 {
					t.Errorf("the log holds %d records, want warm and filler alone", hw)
				}
				if n := b.AmbiguousPublications() - amb; n != 0 {
					t.Errorf("%d ambiguous publications counted for records never placed", n)
				}
			})
		}
	}
}

// TestClosingAPublisherEndsAnUnconfirmedPublishWaitingForRoom is the test
// above for the calls that do not wait for their record's promise: a
// ConfirmNone Publish, a PublishAsync and a PublishOwned. Their record waited
// for room in a full buffer under a context only their caller could end, so
// the publisher's Close waited until the buffer had room, as it did on every
// release before. Close ends the wait. The ConfirmNone Publish and the
// PublishOwned cannot wait for franz-go's refusal, so they answer
// ErrSettlementUnknown carrying the cancellation, or the refusal itself when
// it came first. PublishAsync returns nil, and its callback gets franz-go's
// refusal, or the core's own unknown outcome for a callback its Close found
// outstanding.
func TestClosingAPublisherEndsAnUnconfirmedPublishWaitingForRoom(t *testing.T) {
	for _, unbounded := range []bool{false, true} {
		for _, path := range []string{"ConfirmNone", "PublishAsync", "PublishOwned"} {
			t.Run("unbounded="+strconv.FormatBool(unbounded)+"/"+path, func(t *testing.T) {
				topic := newTopic("isolation-close-unconfirmed")
				cl := cluster(t, 1, []string{topic})
				c, b := openPair(t, cl, func(cfg *kq.Config) {
					cfg.MaxBufferedRecords = 1
					cfg.UnboundedDelivery = unbounded
					cfg.DeliveryTimeout = 60 * time.Second
				})
				pf, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
				if err != nil {
					t.Fatalf("Publisher: %v", err)
				}
				pcfg := mq.PublisherConfig{Topic: topic}
				if path == "ConfirmNone" {
					pcfg.Confirmation = mq.ConfirmNone
				}
				pa, err := c.Publisher(t.Context(), pcfg)
				if err != nil {
					t.Fatalf("Publisher A: %v", err)
				}
				publish(t, c, topic, &mq.Message{Body: []byte("warm")})
				held, release := holdFirstProduce(t, cl)
				filler := make(chan error, 1)
				go func() { filler <- pf.Publish(context.Background(), &mq.Message{Body: []byte("filler")}) }()
				<-held
				var released atomic.Int32
				aerr, async := make(chan error, 1), make(chan error, 1)
				go func() {
					switch path {
					case "PublishAsync":
						aerr <- pa.PublishAsync(context.Background(), &mq.Message{Body: []byte("A")}, func(r mq.PublishResult) { async <- r.Err })
					case "PublishOwned":
						aerr <- pa.PublishOwned(context.Background(), mq.OwnedMessage{
							Message: &mq.Message{Body: []byte("A")},
							Release: func(*mq.Message) { released.Add(1) },
						})
					default:
						aerr <- pa.Publish(context.Background(), &mq.Message{Body: []byte("A")})
					}
				}()
				waitFor(t, 5*time.Second, "A waiting for room", func() bool { return b.Client().BufferedProduceRecords() >= 2 })
				closed := make(chan error, 1)
				go func() { closed <- pa.Close() }()
				select {
				case err := <-closed:
					if err != nil {
						t.Errorf("closing publisher A: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Errorf("publisher A's Close had not returned after 3s while A waited for buffer room")
				}
				var ea error
				select {
				case ea = <-aerr:
				case <-time.After(time.Second):
					t.Errorf("A's call had not returned 1s after its publisher's Close")
					release()
					ea = <-aerr
				}
				switch path {
				case "PublishAsync":
					if ea != nil {
						t.Errorf("PublishAsync = %v, want nil and the outcome in its callback", ea)
					}
					// The core's Close answers a callback still outstanding
					// with its own unknown outcome, and that can come before
					// franz-go's refusal.
					select {
					case ea = <-async:
					case <-time.After(5 * time.Second):
						t.Fatal("A's callback had not run 5s after its publisher's Close")
					}
					if ea == nil {
						t.Error("A's callback = nil for a record its publisher's Close refused")
					}
				default:
					if ea == nil || !errors.Is(ea, context.Canceled) {
						t.Errorf("A = %v, want an answer carrying the cancellation", ea)
					}
				}
				if mq.IsAmbiguous(ea) != errors.Is(ea, mq.ErrSettlementUnknown) {
					t.Errorf("A = %v (code %v) is ambiguous %v and does not match ErrSettlementUnknown alike", ea, mq.CodeOf(ea), mq.IsAmbiguous(ea))
				}
				release()
				if err := <-filler; err != nil {
					t.Errorf("filler: %v", err)
				}
				if path == "PublishOwned" {
					waitFor(t, 5*time.Second, "the owned message to be released", func() bool { return released.Load() > 0 })
					if n := released.Load(); n != 1 {
						t.Errorf("the owned message was released %d times, want once", n)
					}
				}
				if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != 2 {
					t.Errorf("the log holds %d records, want warm and filler alone", hw)
				}
			})
		}
	}
}

// TestOneMovesCancellationFailsNoOtherPublication: a nack's move to a
// dead-letter topic is a produce as well, and its copy was produced under the
// nack's context, bounded by Config.RequestTimeout. A nack whose context
// ended, or whose bound ran out, failed every publication queued on the
// dead-letter partition with it. The move's wait ends instead, and the copy
// stays with franz-go.
func TestOneMovesCancellationFailsNoOtherPublication(t *testing.T) {
	topic, dlq := newTopic("isolation-move"), newTopic("isolation-move-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	publish(t, c, dlq, &mq.Message{Body: []byte("warm")})
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "g-isolation-move", Prefetch: 16, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	d := fetchN(t, t.Context(), src, 1)[0]

	held, release := holdFirstProduce(t, cl)
	filler := make(chan error, 1)
	go func() { filler <- c.Publish(context.Background(), dlq, &mq.Message{Body: []byte("filler")}) }()
	<-held
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nerr := make(chan error, 1)
	go func() { nerr <- src.Settler().Nack(ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter}) }()
	waitFor(t, 5*time.Second, "the move's copy buffered", func() bool { return b.Client().BufferedProduceRecords() >= 2 })
	berr := make(chan error, 1)
	go func() { berr <- c.Publish(context.Background(), dlq, &mq.Message{Body: []byte("B")}) }()
	waitFor(t, 5*time.Second, "B buffered", func() bool { return b.Client().BufferedProduceRecords() >= 3 })
	amb := b.AmbiguousPublications()
	cancel()
	if err := <-nerr; err == nil || !mq.IsAmbiguous(err) {
		t.Errorf("the nack whose wait ended = %v, want an unknown outcome", err)
	}
	time.Sleep(100 * time.Millisecond)
	release()
	if err := <-filler; err != nil {
		t.Errorf("filler, whose caller did nothing, = %v (code %v): the move's cancellation failed it", err, mq.CodeOf(err))
	}
	if err := <-berr; err != nil {
		t.Errorf("B, whose caller did nothing, = %v (code %v): the move's cancellation failed it", err, mq.CodeOf(err))
	}
	// warm, filler, the copy and B.
	waitFor(t, 5*time.Second, "filler, the copy and B in the log", func() bool { return cl.PartitionInfo(dlq, 0).HighWatermark >= 4 })
	if n := b.AmbiguousPublications() - amb; n != 1 {
		t.Errorf("%d ambiguous publications counted, want 1 (the move's)", n)
	}
}
