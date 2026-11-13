package gcppubsub_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// TestAPublicationWhoseWaitEndedIsAmbiguous: a context that ends while the
// driver waits on PublishResult.Get does not un-send the message. The
// library's bundler already holds it, and the service may store it. The
// driver mapped that context error with Map, so the caller was told a
// DEFINITE DeadlineExceeded or Canceled, and a caller who republishes the
// definite refusals -- which docs/concepts/confirmation.md advises --
// duplicated it (the review's concurrency-4). It is now mapped as a failure
// after the request may have left: ambiguous, with its code kept.
func TestAPublicationWhoseWaitEndedIsAmbiguous(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			c, srv := scripted(t)
			topic := newTopic("wait")
			provision(t, c, topic, "s")
			srv.PublishErr = func(string, *psapi.Outgoing) error {
				return fmt.Errorf("waiting for the publish result: %w", cause)
			}
			p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("opening the publisher: %v", err)
			}
			for name, publish := range map[string]func() error{
				"Publish": func() error {
					return c.Publish(context.Background(), topic, &mq.Message{Body: []byte("maybe")})
				},
				"PublishBatch": func() error {
					return p.PublishBatch(context.Background(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
				},
			} {
				err := publish()
				if err == nil {
					t.Fatalf("%s: the injected failure was not reported", name)
				}
				if !mq.IsAmbiguous(err) {
					t.Fatalf("%s = %v (code %v): IsAmbiguous = false; the bundler held the message when the wait ended, "+
						"so the service may store it", name, err, mq.CodeOf(err))
				}
				want := mq.CodeDeadlineExceeded
				if errors.Is(cause, context.Canceled) {
					want = mq.CodeCanceled
				}
				if mq.CodeOf(err) != want && mq.CodeOf(err) != mq.CodePartial {
					t.Fatalf("%s = %v: code %v, want %v", name, err, mq.CodeOf(err), want)
				}
			}
		})
	}
}

// TestAPublicationWhoseWaitEndedKeepsNoneOfTheCallersStorage: Sink.Publish
// "must not retain m, its Key, its Body or its header storage after it
// returns: a caller may reuse all of them". A publication whose context ends
// while the driver waits on the library's result is one the library still
// holds, and it holds the body by reference: the encoder hands the library the
// caller's own bytes. Publish and PublishBatch returned with the bytes still
// there, so a caller that reused its buffer, as the API lets it the moment the
// call returns, had the library send whatever the buffer held by then (hunt 8
// cs8-3). PublishOwned released the storage at the same point, handing it back
// to the owner's pool while the library still read it. All three now copy a
// body the library could still read after the call, and PublishOwned still
// releases on return.
//
// The fake holds a gated publication the way the library holds one, by the
// caller's bytes, and reads them only when it sends it.
func TestAPublicationWhoseWaitEndedKeepsNoneOfTheCallersStorage(t *testing.T) {
	const original, reused = "original-body", "REUSED-BUFFER"
	for _, path := range []string{"Publish", "PublishBatch", "PublishOwned"} {
		t.Run(path, func(t *testing.T) {
			c, srv := scripted(t)
			topic := newTopic("retain")
			provision(t, c, topic, "s")
			p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = p.Close() }()
			cons, err := c.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: "s"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = cons.Close() }()

			srv.GatePublishResults()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			bufs := [][]byte{[]byte(original), []byte(original)}
			var released atomic.Int32
			var wg sync.WaitGroup
			// The caller's context ends while the library holds the
			// publications.
			wg.Go(func() {
				waitUntil(5*time.Second, func() bool { return srv.HeldPublications() > 0 })
				cancel()
			})
			switch path {
			case "Publish":
				bufs = bufs[:1]
				err = p.Publish(ctx, &mq.Message{Body: bufs[0]})
			case "PublishBatch":
				err = p.PublishBatch(ctx, []*mq.Message{{Body: bufs[0]}, {Body: bufs[1]}})
			case "PublishOwned":
				bufs = bufs[:1]
				err = p.PublishOwned(ctx, mq.OwnedMessage{Message: &mq.Message{Body: bufs[0]}, Release: func(m *mq.Message) {
					// The owner's pool hands the storage to its next user.
					copy(m.Body, reused)
					released.Add(1)
				}})
			}
			wg.Wait()
			if !mq.IsAmbiguous(err) {
				t.Fatalf("%s = %v (code %v), want the ambiguous answer of a wait that ended", path, err, mq.CodeOf(err))
			}
			if path == "PublishOwned" && released.Load() != 1 {
				t.Fatalf("PublishOwned returned having released its message %d times, want once, before it returns",
					released.Load())
			}
			if path != "PublishOwned" {
				// The API lets the caller reuse its buffers once the call
				// has returned.
				for _, b := range bufs {
					copy(b, reused)
				}
			}
			srv.ReleasePublishResults()
			for range bufs {
				d := receiveOne(t, cons, 5*time.Second)
				if string(d.Body) != original {
					t.Fatalf("%s: the publication whose wait ended carried %q, want %q: the driver left the "+
						"caller's storage with the library after the call returned, and the library sent "+
						"what it held by then", path, d.Body, original)
				}
				_ = d.Ack()
			}
			if got := released.Load(); path == "PublishOwned" && got != 1 {
				t.Fatalf("PublishOwned released the message %d times, want exactly once", got)
			}
		})
	}
}

// TestAReleaseThatPublishesNeverWaitsOnTheCompletionPool: OwnedPublisher's
// Release "may publish again". The first round of the cs8-3 fix left the
// release of a PublishOwned whose context had ended to a completion worker,
// and a Release that published asynchronously while the completion queue was
// full parked that worker waiting for room only a worker could make: with one
// worker the pool stopped for good, and PublisherHandle.Close and Client.Close
// with it (review 9 B1). PublishOwned now copies the body when its context
// can end and releases on return, on the caller's goroutine.
func TestAReleaseThatPublishesNeverWaitsOnTheCompletionPool(t *testing.T) {
	const original, reused = "original-body", "REUSED-BUFFER"
	c, srv := scripted(t, func(cfg *gps.Config) {
		cfg.CompletionWorkers = 1
		cfg.CompletionQueue = 1
	})
	topic := newTopic("relpublish")
	provision(t, c, topic, "s")
	p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	srv.GatePublishResults()
	defer srv.ReleasePublishResults()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var released atomic.Int32
	republished := make(chan error, 1)
	var wg sync.WaitGroup
	var ownedErr error
	wg.Go(func() {
		ownedErr = p.PublishOwned(ctx, mq.OwnedMessage{Message: &mq.Message{Body: []byte(original)}, Release: func(m *mq.Message) {
			// The owner's pool hands the storage to its next user, and the
			// owner publishes again, as OwnedPublisher allows.
			copy(m.Body, reused)
			released.Add(1)
			republished <- p.PublishAsync(context.Background(), &mq.Message{Body: []byte("from-release")}, nil)
		}})
	})
	if !waitUntil(5*time.Second, func() bool { return srv.HeldPublications() == 1 }) {
		t.Fatal("the owned publication never reached the library")
	}
	cancel()
	wg.Wait()
	if !mq.IsAmbiguous(ownedErr) {
		t.Fatalf("PublishOwned = %v, want the ambiguous answer of a wait that ended", ownedErr)
	}
	if got := released.Load(); got != 1 {
		t.Fatalf("PublishOwned returned having released its message %d times, want once, before it returns: a "+
			"release left to a completion worker can wait on the pool for itself when it publishes", got)
	}
	if err := <-republished; err != nil {
		t.Fatalf("the release's PublishAsync: %v", err)
	}
	// The one worker waits on the release's publication, and one more
	// asynchronous publication takes the queue's one place.
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("filler")}, nil); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if !waitUntil(5*time.Second, func() bool { return srv.HeldPublications() == 3 }) {
		t.Fatalf("%d of the three publications reached the library", srv.HeldPublications())
	}
	srv.ReleasePublishResults()
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cons, err := c.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	for range 3 {
		d := receiveOne(t, cons, 5*time.Second)
		if string(d.Body) == reused {
			t.Fatalf("the owned publication carried %q: its storage went back to its owner before the library sent it", d.Body)
		}
		_ = d.Ack()
	}
}

// TestAnOrderedPublicationKeepsToItsContextWhenThePoolIsFull: an ordered
// Publish or PublishBatch whose wait ends leaves its key's count to the
// completion pool. The first round of the cs8-4 fix waited for the library's
// answer itself, past the caller's context, whenever the pool had no room:
// with a 50 ms context the call returned only once the library answered, up
// to the publish timeout for each bundle ahead on the key (review 9 B2). Such
// a publication now takes its place in the pool before the library sees it,
// bounded by its context, so a full pool refuses it, definitely and with
// nothing sent, when the context ends. A place taken and not needed is given
// back.
func TestAnOrderedPublicationKeepsToItsContextWhenThePoolIsFull(t *testing.T) {
	ordered := func(cfg *gps.Config) {
		cfg.Ordering = true
		cfg.CompletionWorkers = 1
		cfg.CompletionQueue = 1
	}
	for _, path := range []string{"Publish", "PublishBatch"} {
		t.Run(path, func(t *testing.T) {
			c, srv := scripted(t, ordered)
			topic := newTopic("fullpool")
			provision(t, c, topic, "s")
			p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = p.Close() }()
			srv.GatePublishResults()
			defer srv.ReleasePublishResults()
			// One asynchronous publication holds the worker and another the
			// pool's one place.
			for i := range 2 {
				if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("async")}, nil); err != nil {
					t.Fatalf("PublishAsync %d: %v", i, err)
				}
				if !waitUntil(5*time.Second, func() bool { return srv.HeldPublications() == i+1 }) {
					t.Fatalf("asynchronous publication %d never reached the library", i)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			var wg sync.WaitGroup
			returned := make(chan error, 1)
			wg.Go(func() {
				if path == "Publish" {
					returned <- p.Publish(ctx, &mq.Message{Key: []byte("k"), Body: []byte("ordered")})
					return
				}
				returned <- p.PublishBatch(ctx, []*mq.Message{
					{Key: []byte("k"), Body: []byte("o1")},
					{Key: []byte("k"), Body: []byte("o2")},
					{Body: []byte("keyless")},
				})
			})
			defer wg.Wait()
			select {
			case err = <-returned:
			case <-time.After(5 * time.Second):
				held := srv.HeldPublications()
				srv.ReleasePublishResults()
				t.Fatalf("%s with a 50 ms context was still blocked 5 s later (%d publications held): it waits "+
					"for the library past its context", path, held)
			}
			refusals, sent := []error{err}, 2
			if path == "PublishBatch" {
				// The ordered messages are refused as never attempted; the
				// keyless one needs no place, is sent, and its wait ends.
				var be *mq.BatchError
				if !errors.As(err, &be) || len(be.Errs) != 3 {
					t.Fatalf("PublishBatch = %v, want a *BatchError of three: two refusals and an ambiguous send", err)
				}
				if !mq.IsAmbiguous(be.Errs[2]) {
					t.Fatalf("the keyless message = %v, want the ambiguous answer of a sent message whose wait ended",
						be.Errs[2])
				}
				refusals, sent = be.Errs[:2], 3
				for i, e := range refusals {
					if !mq.IsRetryable(e) {
						t.Fatalf("ordered message %d = %v (code %v): not retryable, but it was never attempted",
							i, e, mq.CodeOf(e))
					}
				}
			}
			for _, e := range refusals {
				if e == nil || mq.IsAmbiguous(e) {
					t.Fatalf("%s = %v, want the definite refusal of a publication nothing was sent for", path, e)
				}
				if !errors.Is(e, context.DeadlineExceeded) && mq.CodeOf(e) != mq.CodeDeadlineExceeded {
					t.Fatalf("%s = %v (code %v), want its context's deadline", path, e, mq.CodeOf(e))
				}
			}
			if held := srv.HeldPublications(); held != sent {
				t.Fatalf("%d publications reached the library, want %d: a refused ordered publication must not "+
					"be sent", held, sent)
			}
		})
	}
	t.Run("places come back", func(t *testing.T) {
		c, _ := scripted(t, ordered)
		topic := newTopic("places")
		provision(t, c, topic, "s")
		p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		defer func() { _ = p.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// More ordered publications than the pool has places, each holding
		// one while it waits and answered in time.
		for i := range 10 {
			if err := p.Publish(ctx, &mq.Message{Key: []byte("k"), Body: []byte("ordered")}); err != nil {
				t.Fatalf("Publish %d: %v: the places taken by the ones before it were not given back", i, err)
			}
			if err := p.PublishBatch(ctx, []*mq.Message{{Key: []byte("k"), Body: []byte("o1")}, {Body: []byte("o2")}}); err != nil {
				t.Fatalf("PublishBatch %d: %v: the places taken by the ones before it were not given back", i, err)
			}
		}
		if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("async")}, nil); err != nil {
			t.Fatalf("PublishAsync after them: %v", err)
		}
	})
}

// TestAPublicationWhoseWaitEndedKeepsNoneOfTheCallersStorageOnTheLibrary is
// the same over the real client library, whose bundler holds the publication
// for its delay threshold after the caller's deadline has passed.
func TestAPublicationWhoseWaitEndedKeepsNoneOfTheCallersStorageOnTheLibrary(t *testing.T) {
	const original, reused = "original-body", "REUSED-BUFFER"
	for _, path := range []string{"Publish", "PublishOwned"} {
		t.Run(path, func(t *testing.T) {
			l := newLive(t)
			c := l.open(t, func(cfg *gps.Config) {
				cfg.PublishCountThreshold = 1000
				cfg.PublishDelayThreshold = 300 * time.Millisecond
			})
			topic := newTopic("retainlib")
			provision(t, c, topic, "s")
			p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = p.Close() }()
			cons, err := c.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: "s"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = cons.Close() }()
			buf := []byte(original)
			var released atomic.Int32
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if path == "Publish" {
				err = p.Publish(ctx, &mq.Message{Body: buf})
			} else {
				err = p.PublishOwned(ctx, mq.OwnedMessage{Message: &mq.Message{Body: buf}, Release: func(m *mq.Message) {
					copy(m.Body, reused)
					released.Add(1)
				}})
			}
			if !mq.IsAmbiguous(err) {
				t.Fatalf("%s = %v (code %v), want the ambiguous answer of a wait the bundler outlasted",
					path, err, mq.CodeOf(err))
			}
			if path == "Publish" {
				copy(buf, reused)
			}
			d := receiveOne(t, cons, 5*time.Second)
			if string(d.Body) != original {
				t.Fatalf("%s: the publication whose wait ended carried %q, want %q: the library sent the "+
					"caller's storage as it was after the call returned", path, d.Body, original)
			}
			_ = d.Ack()
			if path == "PublishOwned" && !waitUntil(5*time.Second, func() bool { return released.Load() == 1 }) {
				t.Fatalf("PublishOwned released the message %d times, want exactly once", released.Load())
			}
		})
	}
}
