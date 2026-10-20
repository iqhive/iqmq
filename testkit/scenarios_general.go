package testkit

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// generalScenarios is the list of scenarios that are not about one
// capability: construction, the round trip, the ownership contract, settlement,
// ordering, lifecycle, accounting and redaction.
//
//nolint:gocognit,maintidx // one function per scenario would scatter the list the specification states in one place
func generalScenarios() []Scenario {
	return append(metadataAndLeaseScenarios(), []Scenario{
		// ---- construction and topology -------------------------------
		{
			Name: "construction/invalid-configuration",
			Run: func(ctx context.Context, e *Env) error {
				if _, err := e.Client.Publisher(ctx, mq.PublisherConfig{}); !hasCode(err, mq.CodeInvalidConfig) &&
					!hasCode(err, mq.CodeInvalidArgument) {
					return failf("a publisher with no topic was accepted: %v", err)
				}
				if _, err := e.Client.Consumer(ctx, mq.ConsumerConfig{Subscription: e.Sub}); err == nil {
					return failf("a consumer with no topic was accepted")
				}
				if _, err := e.Client.Consumer(ctx, mq.ConsumerConfig{Topic: e.Topic}); err == nil {
					return failf("a consumer with no subscription was accepted")
				}
				if _, err := e.Client.Consumer(ctx, mq.ConsumerConfig{
					Topic: e.Topic, Subscription: e.Sub, Prefetch: -1,
				}); err == nil {
					return failf("a consumer with a negative prefetch was accepted")
				}
				if _, err := e.Client.Consumer(ctx, mq.ConsumerConfig{
					Topic: e.Topic, Subscription: e.Sub, Prefetch: mq.MaxPrefetch + 1,
				}); err == nil {
					return failf("a consumer with a prefetch past MaxPrefetch was accepted: the specification bounds every buffer")
				}
				// A requirement the driver cannot meet fails at
				// construction, not at the first message.
				missing := firstUnsupported(e.Caps)
				if missing < 0 {
					return Skip("the driver claims every feature, so no unmeetable requirement exists")
				}
				_, err := e.Client.Publisher(ctx, mq.PublisherConfig{
					Topic:        e.Topic,
					Requirements: []mq.Requirement{mq.RequireAtLeast(mq.Feature(missing), mq.Native)},
				})
				return wantUnsupported(err, "a publisher requiring "+mq.Feature(missing).String())
			},
		},
		{
			Name: "topology/no-hidden-creation",
			Run: func(ctx context.Context, e *Env) error {
				absent := e.Harness.topicName("never-provisioned")
				err := e.Client.Publish(ctx, absent, &mq.Message{Body: []byte("x")})
				switch {
				case err == nil:
					return Skip("the driver creates topology on first use; the rule is about Open, and this driver's automatic creation is configuration")
				case hasCode(err, mq.CodeNotFound), hasCode(err, mq.CodeUnauthorized), hasCode(err, mq.CodeForbidden):
					return nil
				default:
					return failf("publishing to a topic nobody created reported %v (code %v), want NotFound", err, mq.CodeOf(err))
				}
			},
		},

		// ---- the round trip ------------------------------------------
		{
			Name: "publish/roundtrip-bodies",
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				defer func() { _ = p.Close() }()

				bodies := [][]byte{nil, {}, []byte("plain"), {0x00, 0xff, 0x7f, 0x80}, bytes.Repeat([]byte{0xa5}, 64<<10)}
				if lim := e.Caps.Limit(mq.LimitMaxMessageBytes); lim.Known && lim.Value < int64(len(bodies[4])) {
					bodies[4] = bytes.Repeat([]byte{0xa5}, int(lim.Value))
				}
				// ONE ROUND TRIP AT A TIME, and the reason is that this
				// scenario is about the fidelity of one body and claims
				// nothing about order.
				//
				// It used to publish all five and correlate them by
				// ARRIVAL ORDER, which is an ordering assumption no
				// general scenario is allowed to make: a broker that
				// orders per key rather than globally fails it, and a
				// standard SQS queue fails it at random. That is not
				// hypothetical -- drivers/snssqs, whose fan-out is SNS
				// onto standard queues, reported "body 1: got 5 bytes,
				// want 0" on the run that found this, and the report
				// committed a day earlier had the same five bodies
				// passing. Correlating by content cannot fix it either,
				// because nil and empty are the two bodies the scenario
				// exists to tell apart and they have the same content.
				for i, b := range bodies {
					if err := p.Publish(ctx, &mq.Message{Body: b}); err != nil {
						return failf("publishing body %d (%d bytes): %v", i, len(b), err)
					}
					if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
						if !bodiesEqual(d.Body, b) {
							// A broker that cannot distinguish
							// nil from empty says so on its
							// page; it may not silently change
							// one into the other.
							if b == nil && len(d.Body) == 0 {
								return failf("a nil body came back as an empty one; the specification makes the distinction observable")
							}
							return failf("got %d bytes, want %d", len(d.Body), len(b))
						}
						return ack(d)
					}); err != nil {
						return failf("body %d: %v", i, err)
					}
				}
				return nil
			},
		},
		{
			Name: "publish/fidelity",
			Run: func(ctx context.Context, e *Env) error {
				n := e.Harness.fidelity()
				if testingShort() {
					n = min(n, 2000)
				}
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 512})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				defer func() { _ = p.Close() }()
				// The collector runs under a context the
				// PUBLISHER can cancel, so that a publication
				// the driver refuses ends the scenario at once
				// and under its own name.
				//
				// Without it the publisher's error sat in perr
				// while collectAck waited out the whole
				// scenario timeout for messages nobody was
				// going to send, and the failure arrived twenty
				// seconds late reading "publish/fidelity:
				// after 1228 of 2000 messages: context
				// deadline exceeded" -- so the first thing a
				// reader concluded was that byte fidelity had
				// broken, when what had happened was
				// ResourceExhausted on publication 1229.
				cctx, stopCollecting := context.WithCancel(ctx)
				defer stopCollecting()
				var wg sync.WaitGroup
				wg.Add(1)
				var perr error
				var published int
				go func() {
					defer wg.Done()
					for i := range n {
						m := &mq.Message{Body: fidelityBody(i)}
						if err := p.Publish(ctx, m); err != nil {
							perr = failf("the PUBLISHER refused message %d of %d after %d were accepted, so this scenario says nothing about byte fidelity: %v", i, n, i, err)
							stopCollecting()
							return
						}
						published = i + 1
					}
				}()
				seen := make([]bool, n)
				count := 0
				_, rerr := collectAck(cctx, c, n, e.Timeout(), func(d *mq.Delivery) error {
					i, ok := fidelityIndex(d.Body)
					if !ok {
						return failf("a delivery's body is not one this scenario published: %d bytes", len(d.Body))
					}
					if !bodiesEqual(d.Body, fidelityBody(i)) {
						return failf("message %d came back changed", i)
					}
					if seen[i] {
						return failf("message %d was delivered twice inside one at-least-once run with no failures", i)
					}
					seen[i] = true
					count++
					return nil
				})
				wg.Wait()
				if perr != nil {
					return perr
				}
				if rerr != nil {
					return failf("after %d of %d messages (%d published): %v", count, n, published, rerr)
				}
				e.Note("%d messages published and received byte for byte, none of them twice", n)
				return nil
			},
		},
		{
			Name: "publish/key-roundtrip",
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				keys := [][]byte{nil, {}, []byte("customer-7"), {0x00, 0xff}}
				// One round trip per key, for the reason
				// publish/roundtrip-bodies does it: correlating four
				// keys by ARRIVAL ORDER assumes an ordering this
				// scenario does not claim, and it fails a broker that
				// orders per key (SQS FIFO sees four keys as three
				// message groups) and fails a standard queue at random.
				for i, k := range keys {
					if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Key: k, Body: []byte("b")}); err != nil {
						return failf("publishing with key %q: %v", k, err)
					}
					if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
						switch {
						case len(k) == 0 && len(d.Key) == 0:
						case !bytes.Equal(d.Key, k):
							return failf("the key came back as %q, want %q", d.Key, k)
						}
						return ack(d)
					}); err != nil {
						return failf("key %d: %v", i, err)
					}
				}
				return nil
			},
		},

		// ---- publication outcomes ------------------------------------
		{
			Name: "publish/confirmation",
			Run: func(ctx context.Context, e *Env) error {
				p, err := e.Publisher(ctx, mq.PublisherConfig{Confirmation: mq.ConfirmAccepted})
				if err != nil {
					return failf("opening an accepted-confirmation publisher: %v", err)
				}
				defer func() { _ = p.Close() }()
				if err := p.Publish(ctx, &mq.Message{Body: []byte("confirmed")}); err != nil {
					return failf("a confirmed publication: %v", err)
				}
				if err := p.Flush(ctx); err != nil {
					return failf("Flush: %v", err)
				}
				// ConfirmDurable is refused at construction by a profile
				// with no durable boundary, which is the rule and the
				// negative half of the confirmation claim.
				dur, err := e.Client.Publisher(ctx, mq.PublisherConfig{Topic: e.Topic, Confirmation: mq.ConfirmDurable})
				switch {
				case err == nil:
					_ = dur.Close()
					if !e.Caps.Has(mq.FeatureDurableConfirm) {
						return failf("a durable-confirmation publisher was accepted by a driver that does not claim FeatureDurableConfirm")
					}
					return nil
				case errors.Is(err, mq.ErrUnsupported):
					if e.Caps.Has(mq.FeatureDurableConfirm) {
						return failf("a driver claiming FeatureDurableConfirm refused a durable-confirmation publisher: %v", err)
					}
					return nil
				default:
					return failf("opening a durable-confirmation publisher: %v (code %v)", err, mq.CodeOf(err))
				}
			},
		},
		{
			Name: "publish/invalid-topic",
			Run: func(ctx context.Context, e *Env) error {
				for _, bad := range []string{"", strings.Repeat("x", 1<<16), "a\x00b", "a\nb"} {
					if err := mq.ValidateTopic(bad); err == nil {
						continue // the portable grammar allows it; not this kit's business
					}
					err := e.Client.Publish(ctx, bad, &mq.Message{Body: []byte("x")})
					if err == nil {
						return failf("publishing to the invalid topic %q was accepted", bad)
					}
					if !hasCode(err, mq.CodeInvalidArgument) && !hasCode(err, mq.CodeNotFound) {
						return failf("publishing to %q reported %v (code %v), want InvalidArgument", bad, err, mq.CodeOf(err))
					}
				}
				return nil
			},
		},
		{
			Name:  "publish/oversized-payload",
			Shape: func(n *Need) { n.MaxMessageSize = 4 << 10 },
			Run: func(ctx context.Context, e *Env) error {
				lim := e.Caps.Limit(mq.LimitMaxMessageBytes)
				if !lim.Known {
					return Skip("the driver advertises no maximum message size, so there is no limit to exceed; the specification forbids reading an unadvertised limit as zero")
				}
				if lim.Value > 64<<20 {
					return Skip("the advertised maximum message size is too large to exceed in a conformance run")
				}
				body := make([]byte, lim.Value+1)
				err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: body})
				if err == nil {
					return failf("a message of %d bytes was accepted by a driver advertising a maximum of %d", len(body), lim.Value)
				}
				return wantCode(err, mq.CodeMessageTooLarge, "an oversized publication")
			},
		},
		{
			Name: "publish/concurrent-eight-goroutines",
			Run: func(ctx context.Context, e *Env) error {
				const goroutines, each = 8, 50
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 256})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				defer func() { _ = p.Close() }()
				var wg sync.WaitGroup
				errs := make([]error, goroutines)
				for g := range goroutines {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := range each {
							m := &mq.Message{Body: []byte(strconv.Itoa(g*each + i))}
							if err := p.Publish(ctx, m); err != nil {
								errs[g] = failf("goroutine %d: %v", g, err)
								return
							}
						}
					}()
				}
				wg.Wait()
				for _, err := range errs {
					if err != nil {
						return err
					}
				}
				seen := make(map[string]bool, goroutines*each)
				_, err = collectAck(ctx, c, goroutines*each, e.Timeout(), func(d *mq.Delivery) error {
					seen[string(d.Body)] = true
					return nil
				})
				if err != nil {
					return failf("after %d of %d messages: %v", len(seen), goroutines*each, err)
				}
				if len(seen) != goroutines*each {
					return failf("eight concurrent publishers produced %d distinct messages, want %d: a publication was lost", len(seen), goroutines*each)
				}
				return nil
			},
		},

		// ---- ownership and lifecycle ---------------------------------
		{
			Name: "ownership/safe-copy-alias",
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				defer func() { _ = p.Close() }()
				// The specification: a synchronous Publish retains nothing, so the
				// caller may reuse every buffer the instant it returns.
				body := []byte("original")
				key := []byte("key-original")
				m := &mq.Message{Key: key, Body: body}
				m.Headers.Set("h", []byte("value"))
				if err := p.Publish(ctx, m); err != nil {
					return failf("Publish: %v", err)
				}
				copy(body, "SCRIBBLED")
				copy(key, "SCRIBBLED!!")
				m.Reset()
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "original" {
						return failf("the driver retained the caller's body past Publish: it arrived as %q", d.Body)
					}
					if !bytes.HasPrefix(d.Key, []byte("key-original")) {
						return failf("the driver retained the caller's key past Publish: it arrived as %q", d.Key)
					}
					v, ok := d.Headers.Get("h")
					if !ok || string(v) != "value" {
						return failf("the driver retained the caller's header storage past Publish: it arrived as %q (present %v)", v, ok)
					}
					// A received body is borrowed until settlement, and
					// CloneMessage is the escape.
					clone := d.CloneMessage()
					if err := ack(d); err != nil {
						return failf("Ack: %v", err)
					}
					if string(clone.Body) != "original" {
						return failf("CloneMessage did not outlive the settlement: the clone is %q", clone.Body)
					}
					return nil
				})
			},
		},
		{
			Name: "lifecycle/context-cancellation",
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				cctx, cancel := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() {
					_, rerr := c.Receive(cctx, make([]mq.Delivery, 1))
					done <- rerr
				}()
				time.Sleep(20 * time.Millisecond)
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) && !hasCode(err, mq.CodeCanceled) {
						return failf("a cancelled Receive returned %v (code %v), want Canceled", err, mq.CodeOf(err))
					}
				case <-time.After(e.Timeout()):
					return failf("cancelling the context did not release a parked Receive: the specification makes every blocking call context-aware")
				}
				// A publish with an already-cancelled context does not
				// silently succeed.
				pctx, pcancel := context.WithCancel(ctx)
				pcancel()
				err = e.Client.Publish(pctx, e.Topic, &mq.Message{Body: []byte("x")})
				if err == nil {
					return failf("publishing with a cancelled context was accepted")
				}
				return nil
			},
		},
		{
			Name: "lifecycle/close-unblocks-receive",
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				done := make(chan error, 1)
				go func() {
					_, rerr := c.Receive(ctx, make([]mq.Delivery, 1))
					done <- rerr
				}()
				time.Sleep(20 * time.Millisecond)
				if err := c.Close(); err != nil {
					return failf("Close: %v", err)
				}
				select {
				case err := <-done:
					if !errors.Is(err, mq.ErrClosed) {
						return failf("a parked Receive woke with %v, want ErrClosed", err)
					}
				case <-time.After(e.Timeout()):
					return failf("Close did not unblock a parked Receive: the \"Close unblocks blocked operations\"")
				}
				if _, err := c.Receive(ctx, make([]mq.Delivery, 1)); !errors.Is(err, mq.ErrClosed) {
					return failf("Receive after Close: %v", err)
				}
				if err := c.Close(); err != nil {
					return failf("a second Close: %v", err)
				}
				return nil
			},
		},
		{
			Name: "lifecycle/publish-after-close",
			Run: func(ctx context.Context, e *Env) error {
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				if err := p.Close(); err != nil {
					return failf("Close: %v", err)
				}
				if err := p.Publish(ctx, &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrClosed) {
					return failf("Publish after Close: %v (code %v), want ErrClosed", err, mq.CodeOf(err))
				}
				if err := p.PublishBatch(ctx, []*mq.Message{{Body: []byte("x")}}); !errors.Is(err, mq.ErrClosed) {
					return failf("PublishBatch after Close: %v", err)
				}
				if err := p.Close(); err != nil {
					return failf("a second Close: %v", err)
				}
				// And the client's own close closes everything it opened.
				c2, err := e.Client.Publisher(ctx, mq.PublisherConfig{Topic: e.Topic})
				if err != nil {
					return failf("opening a second publisher: %v", err)
				}
				if err := e.Client.Close(); err != nil {
					return failf("closing the client: %v", err)
				}
				if err := c2.Publish(ctx, &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrClosed) {
					return failf("a publisher survived its client's Close: %v", err)
				}
				return nil
			},
		},
		{
			Name: "lifecycle/graceful-shutdown-drains",
			Run: func(ctx context.Context, e *Env) error {
				// The consumer is opened first, so that the subscription
				// exists before anything is published: a subscription
				// created afterwards would only see what came later on
				// every broker whose default start position is the end of
				// the topic, which is default.
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				if err := publishN(ctx, p, 16, "shutdown-"); err != nil {
					return err
				}
				if err := p.Shutdown(ctx); err != nil {
					return failf("Shutdown: %v", err)
				}
				if err := p.Shutdown(ctx); err != nil {
					return failf("a second Shutdown: %v", err)
				}
				n, err := collectAck(ctx, c, 16, e.Timeout(), nil)
				if err != nil {
					return failf("after a graceful publisher shutdown only %d of 16 messages arrived: %v", n, err)
				}
				if err := c.Shutdown(ctx); err != nil {
					return failf("a consumer Shutdown: %v", err)
				}
				if st := c.Stats(); st.InFlight != 0 {
					return failf("a shut-down consumer holds %d deliveries in flight, want 0", st.InFlight)
				}
				return nil
			},
		},

		// ---- settlement ----------------------------------------------
		{
			Name:    "settle/ack-prevents-redelivery",
			Feature: mq.FeatureExplicitAck, Has: true, Positive: true,
			Gate:  claims(mq.FeatureExplicitAck),
			Shape: func(n *Need) { n.AckDeadline = 2 * time.Second },
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("acked")}); err != nil {
					return failf("Publish: %v", err)
				}
				if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if err := d.Ack(); err != nil {
						return failf("Ack: %v", err)
					}
					if !d.Settled() {
						return failf("Settled reported false after a successful Ack")
					}
					return nil
				}); err != nil {
					return err
				}
				if err := e.Advance(ctx, 3*time.Second); err != nil {
					return failf("advancing past the lease: %v", err)
				}
				return expectNothing(ctx, c, 500*time.Millisecond, "an acknowledged message was redelivered after its lease would have expired")
			},
		},
		{
			Name:    "settle/ack-idempotency",
			Feature: mq.FeatureExplicitAck, Has: true, Positive: true,
			Gate: claims(mq.FeatureExplicitAck),
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("once")}); err != nil {
					return failf("Publish: %v", err)
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if err := d.Ack(); err != nil {
						return failf("Ack: %v", err)
					}
					if err := d.Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
						return failf("a second Ack reported %v (code %v), want ErrAlreadySettled: the specification says a program that settles twice has a bug", err, mq.CodeOf(err))
					}
					if err := d.Nack(); !errors.Is(err, mq.ErrAlreadySettled) {
						return failf("a Nack after an Ack reported %v, want ErrAlreadySettled", err)
					}
					return nil
				})
			},
		},
		{
			Name:    "settle/concurrent-serialisation",
			Feature: mq.FeatureExplicitAck, Has: true, Positive: true,
			Gate: claims(mq.FeatureExplicitAck),
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("raced")}); err != nil {
					return failf("Publish: %v", err)
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					const racers = 8
					var wg sync.WaitGroup
					errs := make([]error, racers)
					start := make(chan struct{})
					for i := range racers {
						wg.Add(1)
						go func() {
							defer wg.Done()
							<-start
							if i%2 == 0 {
								errs[i] = d.Ack()
								return
							}
							errs[i] = d.Nack()
						}()
					}
					close(start)
					wg.Wait()
					winners := 0
					for i, err := range errs {
						switch {
						case err == nil:
							winners++
						case settleFailed(err):
						default:
							return failf("racer %d got %v (code %v), which is not one of the settlement outcomes", i, err, mq.CodeOf(err))
						}
					}
					if winners != 1 {
						return failf("%d of %d concurrent settlements succeeded, want exactly one: the \"exactly one settlement wins\"", winners, racers)
					}
					return nil
				})
			},
		},
		{
			Name:    "settle/stale-rejection",
			Feature: mq.FeatureExplicitAck, Has: true, Positive: true,
			Gate:  claims(mq.FeatureExplicitAck),
			Shape: func(n *Need) { n.AckDeadline = 500 * time.Millisecond },
			Run: func(ctx context.Context, e *Env) error {
				if !e.Caps.Has(mq.FeatureAtLeastOnce) {
					return Skip("a lease that can expire needs at-least-once delivery")
				}
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("stale")}); err != nil {
					return failf("Publish: %v", err)
				}
				dst := make([]mq.Delivery, 1)
				bctx, cancel := context.WithTimeout(ctx, e.Timeout())
				defer cancel()
				if n, rerr := c.Receive(bctx, dst); n != 1 || rerr != nil {
					return failf("Receive: %d, %v", n, rerr)
				}
				if err := e.Advance(ctx, 2*time.Second); err != nil {
					return failf("advancing past the lease: %v", err)
				}
				// The message comes back, which is what makes the first
				// delivery stale.
				second := make([]mq.Delivery, 1)
				rctx, rcancel := context.WithTimeout(ctx, e.Timeout())
				defer rcancel()
				n, rerr := c.Receive(rctx, second)
				if n != 1 {
					return Skip("the message was not redelivered after its lease expired, so there is no stale settlement to reject: " + render(rerr))
				}
				err = dst[0].Ack()
				if err == nil {
					return failf("a settlement of the expired first delivery succeeded: it acknowledges a message another delivery now owns ")
				}
				if !errors.Is(err, mq.ErrLeaseLost) && !errors.Is(err, mq.ErrAlreadySettled) {
					return failf("a stale settlement reported %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
				}
				return second[0].Ack()
			},
		},

		// ---- batching ------------------------------------------------
		{
			Name:    "batch/partial-results-alignment",
			Feature: mq.FeatureBatchPublish, Has: true, Positive: true,
			Gate: claims(mq.FeatureBatchPublish),
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 64})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				defer func() { _ = p.Close() }()
				const n = 40
				ms := make([]*mq.Message, n)
				for i := range ms {
					ms[i] = &mq.Message{Body: []byte("batch-" + strconv.Itoa(i))}
				}
				if err := p.PublishBatch(ctx, ms); err != nil {
					var be *mq.BatchError
					if !errors.As(err, &be) {
						return failf("PublishBatch: %v", err)
					}
					if len(be.Errs) != len(ms) {
						return failf("a partial batch reported %d outcomes for %d messages: the index alignment is the whole contract", len(be.Errs), len(ms))
					}
					return failf("PublishBatch failed for %d of %d messages: %v", be.Failed(), len(ms), err)
				}
				seen := make(map[string]bool, n)
				if _, err := collectAck(ctx, c, n, e.Timeout(), func(d *mq.Delivery) error {
					seen[string(d.Body)] = true
					return nil
				}); err != nil {
					return failf("after %d of %d batched messages: %v", len(seen), n, err)
				}
				for i := range ms {
					if !seen["batch-"+strconv.Itoa(i)] {
						return failf("batched message %d was accepted and never delivered", i)
					}
				}
				// A batch in which a message fails validation publishes
				// nothing: the specification puts the check before any I/O.
				bad := []*mq.Message{{Body: []byte("ok")}, nil}
				if err := p.PublishBatch(ctx, bad); err == nil {
					return failf("a batch containing a nil message was accepted")
				}
				return nil
			},
		},

		// ---- fan-out and groups --------------------------------------
		{
			Name:    "fanout/isolation",
			Feature: mq.FeatureFanOut, Has: true, Positive: true,
			Gate:  claims(mq.FeatureFanOut),
			Shape: func(n *Need) { n.Subscriptions = []string{"fanout-a", "fanout-b"} },
			Run: func(ctx context.Context, e *Env) error {
				a, err := e.Client.Consumer(ctx, mq.ConsumerConfig{
					Topic: e.Topic, Subscription: e.Need.Sub(0),
				})
				if err != nil {
					return failf("opening the first subscription: %v", err)
				}
				defer func() { _ = a.Close() }()
				b, err := e.Client.Consumer(ctx, mq.ConsumerConfig{
					Topic: e.Topic, Subscription: e.Need.Sub(1),
				})
				if err != nil {
					return failf("opening the second subscription: %v", err)
				}
				defer func() { _ = b.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("fanned")}); err != nil {
					return failf("Publish: %v", err)
				}
				for i, c := range []*mq.ConsumerHandle{a, b} {
					if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
						if string(d.Body) != "fanned" {
							return failf("subscription %d got %q", i, d.Body)
						}
						return ack(d)
					}); err != nil {
						return failf("subscription %d: %v", i, err)
					}
				}
				// One subscription's settlement does not settle the
				// other's copy, which is what fan-out isolation means.
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("second")}); err != nil {
					return failf("Publish: %v", err)
				}
				if err := oneDelivery(ctx, a, e.Timeout(), ack); err != nil {
					return err
				}
				return oneDelivery(ctx, b, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "second" {
						return failf("the second subscription got %q after the first acked its own copy", d.Body)
					}
					return ack(d)
				})
			},
		},
		{
			Name:    "groups/competing-consumers",
			Feature: mq.FeatureConsumerGroups, Has: true, Positive: true,
			Gate: claims(mq.FeatureConsumerGroups),
			Run: func(ctx context.Context, e *Env) error {
				const n = 40
				a, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 8})
				if err != nil {
					return failf("opening the first consumer: %v", err)
				}
				defer func() { _ = a.Close() }()
				b, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 8})
				if err != nil {
					return failf("opening the second consumer: %v", err)
				}
				defer func() { _ = b.Close() }()
				for i := range n {
					if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte(strconv.Itoa(i))}); err != nil {
						return failf("publishing %d: %v", i, err)
					}
				}
				var mu sync.Mutex
				seen := map[string]int{}
				// One context for both consumers: whichever of them takes
				// the last message releases the other, instead of leaving
				// it parked until its own deadline.
				cctx, cancel := context.WithTimeout(ctx, e.Timeout()/2)
				defer cancel()
				var wg sync.WaitGroup
				for _, c := range []*mq.ConsumerHandle{a, b} {
					wg.Add(1)
					go func() {
						defer wg.Done()
						dst := make([]mq.Delivery, 8)
						for {
							m, rerr := c.Receive(cctx, dst)
							for i := range m {
								mu.Lock()
								seen[string(dst[i].Body)]++
								total := len(seen)
								mu.Unlock()
								_ = dst[i].Ack()
								if total >= n {
									cancel()
								}
							}
							if rerr != nil {
								return
							}
						}
					}()
				}
				wg.Wait()
				if len(seen) != n {
					return failf("two competing consumers saw %d distinct messages of %d: a message was lost", len(seen), n)
				}
				return nil
			},
		},

		// ---- backpressure, stats, escape hatches ---------------------
		{
			Name:  "backpressure/bounded",
			Shape: func(n *Need) { n.Capacity = 8 },
			Run: func(ctx context.Context, e *Env) error {
				// A consumer never hands out more than its prefetch
				// unsettled, which is the bound on the one buffer the
				// portable API lets a caller size.
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 4})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				defer func() { _ = p.Close() }()
				if err := publishN(ctx, p, 8, "bp-"); err != nil {
					return err
				}
				dst := make([]mq.Delivery, 64)
				bctx, cancel := context.WithTimeout(ctx, e.Timeout()/4)
				defer cancel()
				n, _ := c.Receive(bctx, dst)
				if n > 4 {
					return failf("a consumer with a prefetch of 4 handed out %d deliveries in one Receive: the prefetch is not a bound", n)
				}
				for i := range n {
					if err := ack(&dst[i]); err != nil {
						return failf("Ack: %v", err)
					}
				}
				return nil
			},
		},
		{
			Name: "stats/accounting",
			Run: func(ctx context.Context, e *Env) error {
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				defer func() { _ = p.Close() }()
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				const n = 12
				if err := publishN(ctx, p, n, "stat-"); err != nil {
					return err
				}
				if got := p.Stats().Published; got != n {
					return failf("the publisher counted %d publications, want %d", got, n)
				}
				if got := p.Stats().Pending; got != 0 {
					return failf("a quiescent publisher reports %d pending publications, want 0", got)
				}
				if _, err := collectAck(ctx, c, n, e.Timeout(), nil); err != nil {
					return err
				}
				st := c.Stats()
				if st.Received != n {
					return failf("the consumer counted %d deliveries, want %d", st.Received, n)
				}
				// A driver with no settlement at all (AckNone)
				// counts no acknowledgements, because none reach it:
				// the core answers every Ack with ErrNotSettleable.
				// Requiring a count there failed a driver that was
				// accounting correctly.
				if e.Caps.Has(mq.FeatureExplicitAck) && st.Acked != n {
					return failf("the consumer counted %d acknowledgements, want %d", st.Acked, n)
				}
				// The same gate, for the same reason: a delivery from a
				// source with no settler is complete the moment the
				// handler has it, and the core counts it in flight for
				// ever because nothing ever settles it. That is a defect
				// in the core's accounting rather than in the driver:
				// both counters are unusable on such a driver, and
				// anything that admitted work against them would stall.
				// It is recorded in the maintenance notes against the
				// increment that owns the consumer's accounting.
				if e.Caps.Has(mq.FeatureExplicitAck) && st.InFlight != 0 {
					return failf("a quiescent consumer holds %d deliveries in flight, want 0: the terminal release", st.InFlight)
				}
				return nil
			},
		},
		{
			// the hand-off, and the one scenario in the suite
			// whose subject is the capability table's numbers
			// rather than a behaviour.
			//
			// the specification's `message.max.bytes` lookup read a
			// DescribeConfigs resource that kfake answers with the
			// whole effective configuration and Apache Kafka
			// answers with the dynamically set cluster-wide
			// configuration only. The real broker said nothing,
			// the driver advertised no MaxMessageBytes for four
			// months, and nothing in the repository noticed --
			// because the specification makes "unknown" a legitimate answer and
			// publish/oversized-payload therefore SKIPS, so the
			// one scenario that would have caught it cannot fail
			// for this reason.
			//
			// The rule here is narrow on purpose. It does not
			// require any particular limit, and it does not turn
			// an unknown limit into a failure anywhere else --
			// the specification's own alternatives entry rejected doing that,
			// because it punishes every driver whose broker
			// genuinely bounds nothing. It asks one thing: that a
			// driver talking to a REAL broker learned at least one
			// of the seven numbers from it. Zero of seven
			// against a container or a managed service is a lookup
			// that failed, and a driver whose broker really states
			// none documents this scenario through Harness.Skip --
			// where the reason is published in its matrix entry
			// instead of being a silent skip.
			Name: "limits/advertised",
			Run: func(_ context.Context, e *Env) error {
				var known []string
				for k := range mq.LimitKeyCount {
					key := mq.LimitKey(k)
					if lim := e.Caps.Limit(key); lim.Known {
						known = append(known, key.String()+"="+strconv.FormatInt(lim.Value, 10))
					}
				}
				if len(known) > 0 {
					e.Note("the driver advertises %d of the %d limits: %s", len(known), mq.LimitKeyCount, strings.Join(known, " "))
					return nil
				}
				switch e.Harness.Evidence {
				case EvidenceContainer, EvidenceManaged:
					return failf("the driver advertises none of the %d limits against %v evidence: a limit a driver reads from a broker is a claim no in-process fake can falsify, and every scenario that skips on an unknown limit is blind to the lookup having failed", mq.LimitKeyCount, string(e.Harness.Evidence))
				case EvidenceEmulator, EvidenceInProcess:
					return Skip("the driver advertises none of the limits, and the evidence is " + string(e.Harness.Evidence) + " rather than a real broker, so there is nothing here that a failed lookup can be told apart from")
				default:
					return failf("the harness does not say what kind of evidence this run is")
				}
			},
		},
		{
			Name: "client/ping",
			Run: func(ctx context.Context, e *Env) error {
				if err := e.Client.Ping(ctx); err != nil {
					return failf("Ping: %v", err)
				}
				if err := e.Client.Close(); err != nil {
					return failf("Close: %v", err)
				}
				if err := e.Client.Ping(ctx); !errors.Is(err, mq.ErrClosed) {
					return failf("Ping after Close: %v (code %v), want ErrClosed", err, mq.CodeOf(err))
				}
				return nil
			},
		},
		{
			Name: "client/info-and-native",
			Run: func(ctx context.Context, e *Env) error {
				info := e.Client.Info()
				if info.Name == "" {
					return failf("DriverInfo.Name is empty: the specification makes an empty field \"the driver did not say\", and a driver always knows its own name")
				}
				if e.Harness.Driver != "" && info.Name != e.Harness.Driver {
					return failf("the client reports driver %q, the harness says %q", info.Name, e.Harness.Driver)
				}
				if e.Client.Native() == nil {
					return Skip("the driver surfaces no native escape hatch, which the specification allows")
				}
				// Info and Native answer after Close, from any goroutine.
				if err := e.Client.Close(); err != nil {
					return failf("Close: %v", err)
				}
				if e.Client.Info().Name != info.Name {
					return failf("a closed client stopped naming its driver")
				}
				if h := e.Client.Health(); h.State != mq.HealthClosed {
					return failf("a closed client reports health %v, want closed", h.State)
				}
				return nil
			},
		},
		{
			// a credential appears nowhere the kit
			// can see. The subject of this scenario is easy to
			// make RUN and was easy to make unfalsifiable, which
			// The specification showed by mutation.
			//
			// Its four original probes were the DriverInfo
			// strings, a publication to "\x00 invalid", a
			// Consumer with an empty ConsumerConfig, and
			// Health().Err. On a real driver the middle two are
			// answered by the CORE's validator before the driver
			// is reached -- an invalid topic name and a missing
			// subscription both fail validation -- so a driver
			// that rendered its whole configuration into every
			// error it built still passed. The specification watched it pass
			// under exactly that mutation and had to add
			// TestSecretsNeverRendered in its own module to catch
			// what the kit could not.
			//
			// So the probes below are chosen for one property: the
			// DRIVER builds the error. A publication and a
			// consumer on topology that does not exist reach the
			// driver's own error mapper, which is where a
			// configuration string would be interpolated; an
			// oversized publication reaches its limit check; Admin
			// reaches its administrative surface. Each is
			// VALID at the portable layer, which is what gets it
			// past the core.
			Name: "secrets/redaction",
			Run: func(ctx context.Context, e *Env) error {
				if len(e.Harness.Secrets) == 0 {
					return Skip("the harness declares no secrets, so there is nothing a redaction check could find; a check that cannot fail proves nothing")
				}
				var found []string
				var where []string
				reached := 0
				probe := func(what, s string) {
					for _, secret := range e.Harness.Secrets {
						if secret != "" && strings.Contains(s, secret) {
							found = append(found, s)
							where = append(where, what)
						}
					}
				}
				// A rendered error is only a probe when there
				// IS one, so the scenario counts the driver
				// errors it actually reached and says so.
				driverErr := func(what string, err error) {
					if err == nil {
						return
					}
					reached++
					probe(what, err.Error())
				}
				info := e.Client.Info()
				probe("DriverInfo", info.Name+" "+info.Profile+" "+info.ClientName+" "+info.ClientVersion+" "+info.BrokerVersion)
				if h := e.Client.Health(); h.Err != nil {
					probe("Health().Err", h.Err.Error())
				}
				// The core's validator answers these two, which
				// is why they are not enough on their own; they
				// stay because a driver that wrapped them would
				// still be caught.
				if err := e.Client.Publish(ctx, "\x00 invalid", &mq.Message{Body: []byte("x")}); err != nil {
					probe("Publish to an invalid topic name", err.Error())
				}
				if _, err := e.Client.Consumer(ctx, mq.ConsumerConfig{}); err != nil {
					probe("Consumer with no topic", err.Error())
				}
				// And these reach the driver. The names are
				// valid, so the core passes them through; the
				// topology does not exist, so the driver has to
				// answer.
				absent := e.Harness.topicName("secrets-absent")
				driverErr("Publish to a topic nobody created",
					e.Client.Publish(ctx, absent, &mq.Message{Body: []byte("x")}))
				_, cerr := e.Client.Consumer(ctx, mq.ConsumerConfig{Topic: absent, Subscription: e.Sub})
				driverErr("Consumer on a topic nobody created", cerr)
				if lim := e.Caps.Limit(mq.LimitMaxMessageBytes); lim.Known && lim.Value > 0 && lim.Value < 64<<20 {
					driverErr("an oversized publication",
						e.Client.Publish(ctx, e.Topic, &mq.Message{Body: make([]byte, lim.Value+1)}))
				}
				if admin, err := e.Client.Admin(); err == nil {
					_, serr := admin.Stats(ctx, absent, e.Sub)
					driverErr("Admin.Stats on a topic nobody created", serr)
					driverErr("Admin.EnsureTopic with no name", admin.EnsureTopic(ctx, mq.TopicSpec{}))
				} else {
					driverErr("Client.Admin", err)
				}
				if len(found) > 0 {
					return failf("a credential appeared in %d rendered string(s); the first was produced by %s and reads %q: forbid it", len(found), where[0], found[0])
				}
				if reached == 0 {
					return Skip("no probe reached an error the DRIVER built, so what passed is the core's validator rather than this driver's rendering; a check that cannot fail proves nothing ")
				}
				e.Note("%d driver-built error(s) rendered without the %d declared credential(s)", reached, len(e.Harness.Secrets))
				return nil
			},
		},
		{
			// The error a native client is most likely to render its
			// connection string into is the dial or authentication
			// failure, and `secrets/redaction` above cannot reach it:
			// that happens inside Open, before there is a client for a
			// scenario to hold, so every probe it has runs on a
			// connection that already succeeded.
			//
			// `dsn.Scrub` exists for exactly this -- "an error or a log
			// line built from a native client's message cannot carry a
			// credential the client embedded in it" -- and no shipped
			// driver calls it. They pass the other scenario because they
			// never interpolate their own configuration into an error,
			// which is the better answer structurally; what went
			// unprobed is the message the CLIENT LIBRARY wrote, and a
			// client library that echoes the URL it was handed is the
			// ordinary case rather than the exotic one.
			Name: "secrets/open-refusal",
			Run: func(ctx context.Context, e *Env) error {
				h := e.Harness
				if len(h.Secrets) == 0 {
					return Skip("the harness declares no secrets, so there is nothing a redaction check could find; a check that cannot fail proves nothing")
				}
				if h.OpenWrongCredential == nil {
					return Skip("the harness has no OpenWrongCredential, so the dial or authentication failure cannot be reached: it happens inside Open and every other probe here is handed a client that already connected")
				}
				err := h.OpenWrongCredential(ctx)
				if err == nil {
					// Not a pass. A probe whose setup did not take
					// is a probe that read nothing, and saying so is
					// the difference between this and a check that
					// cannot fail.
					return failf("opening with a deliberately wrong credential SUCCEEDED, so there is no refusal to read: either this deployment does not authenticate or OpenWrongCredential does not corrupt what it thinks it does")
				}
				// Neither is a deadline a pass, and this arm is the
				// whole reason the scenario is trustworthy.
				//
				// A driver that retries a refused dial swallows the
				// broker's answer into a backoff loop and returns
				// `context deadline exceeded` when the scenario's
				// budget runs out. That error contains no credential
				// because it contains nothing -- the first version of
				// this probe passed against a real RabbitMQ in exactly
				// 45.000 s, the harness timeout, having read the
				// deadline rather than the refusal. A harness whose
				// OpenWrongCredential does not bound its own dial
				// produces a check that cannot fail.
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return failf("opening with a wrong credential returned %v rather than the broker's refusal, so this probe read a clock and not an error: OpenWrongCredential has to bound its own dial -- one attempt, no reconnection budget -- or the driver's retry loop swallows the answer", err)
				}
				rendered := err.Error()
				for _, secret := range h.Secrets {
					if secret != "" && strings.Contains(rendered, secret) {
						return failf("the refusal from a wrong credential rendered one: %q. This is the error a client library is most likely to interpolate its connection string into, and dsn.Scrub is what a driver wraps it with", rendered)
					}
				}
				e.Note("the refusal from a wrong credential rendered none of the %d declared credential(s)", len(h.Secrets))
				return nil
			},
		},

		// ---- what the portable API cannot reach yet ------------------
		{
			Name: "publish/async-callback-exactly-once",
			Run: func(ctx context.Context, e *Env) error {
				// the contract, clause by clause: a nil return means
				// done runs exactly once, a non-nil return means it does
				// not run at all, and the message is borrowed until it
				// does. "Exactly once" is the clause that costs a caller
				// most when it is wrong: a callback that runs twice
				// releases a pooled message twice.
				//
				// The deliveries are consumed and settled while the
				// publications are in flight, because what the callback
				// means is the driver's to define — a broker-side ack for
				// Kafka, and for a borrowing in-process driver the moment
				// the last subscription has settled the record. A scenario
				// that published and then waited without consuming would
				// deadlock against the second kind and prove nothing about
				// the first.
				const n = 32
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 64})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				consumed := make(chan error, 1)
				go func() {
					_, cerr := collectAck(ctx, c, n, e.Timeout(), nil)
					consumed <- cerr
				}()
				var mu sync.Mutex
				calls := make(map[*mq.Message]int, n)
				mismatched := 0
				// fires counts every callback run, and fired only wakes the
				// wait below: a callback must never block the driver that
				// runs it, whatever it counts.
				var fires atomic.Int64
				fired := make(chan struct{}, 1)
				ms := make([]*mq.Message, n)
				for i := range ms {
					ms[i] = &mq.Message{Body: []byte("async-" + strconv.Itoa(i))}
					m := ms[i]
					if err := p.PublishAsync(ctx, m, func(res mq.PublishResult) {
						mu.Lock()
						calls[m]++
						fires.Add(1)
						if res.Message != m {
							mismatched++
						}
						mu.Unlock()
						wake(fired)
					}); err != nil {
						_ = p.Close()
						return failf("PublishAsync %d: %v", i, err)
					}
				}
				deadline := time.After(e.Timeout())
				for fires.Load() < n {
					select {
					case <-fired:
					case <-deadline:
						mu.Lock()
						got := len(calls)
						mu.Unlock()
						_ = p.Close()
						return failf("only %d of %d asynchronous callbacks ran: a nil return from PublishAsync promises the callback runs exactly once", got, n)
					}
				}
				if err := <-consumed; err != nil {
					_ = p.Close()
					return failf("consuming the asynchronous publications: %v", err)
				}
				// Closing the publisher must not produce a second run of
				// any callback: the specification says exactly once, close included.
				if err := p.Close(); err != nil {
					return failf("Close: %v", err)
				}
				time.Sleep(50 * time.Millisecond)
				mu.Lock()
				defer mu.Unlock()
				if mismatched != 0 {
					return failf("%d callbacks reported a message other than the one published", mismatched)
				}
				for i, m := range ms {
					if calls[m] != 1 {
						return failf("the callback for message %d ran %d times, want exactly once", i, calls[m])
					}
				}
				return nil
			},
		},
		{
			Name: "publish/owned-release-exactly-once",
			Run: func(ctx context.Context, e *Env) error {
				// The specification: PublishOwned takes ownership at invocation and
				// calls Release exactly once — including on rejection, on
				// close and on permanent failure. A release that never
				// happens leaks the caller's pool; one that happens twice
				// corrupts its free list. As above, the deliveries are
				// settled while the publications are in flight, because a
				// driver that owns the storage releases it when the last
				// subscription is done with it.
				const n = 16
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 64})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return failf("opening a publisher: %v", err)
				}
				consumed := make(chan error, 1)
				go func() {
					_, cerr := collectAck(ctx, c, n, e.Timeout(), nil)
					consumed <- cerr
				}()
				var mu sync.Mutex
				released := make(map[*mq.Message]int, n)
				mismatched := 0
				// frees and freed are fires and fired of publish/async-callback.
				var frees atomic.Int64
				freed := make(chan struct{}, 1)
				ms := make([]*mq.Message, n)
				for i := range ms {
					ms[i] = &mq.Message{Body: []byte("owned-" + strconv.Itoa(i))}
					m := ms[i]
					om := mq.OwnedMessage{Message: m, Release: func(got *mq.Message) {
						mu.Lock()
						released[m]++
						frees.Add(1)
						if got != m {
							mismatched++
						}
						mu.Unlock()
						wake(freed)
					}}
					if err := p.PublishOwned(ctx, om); err != nil {
						_ = p.Close()
						return failf("PublishOwned %d: %v", i, err)
					}
				}
				deadline := time.After(e.Timeout())
				for frees.Load() < n {
					select {
					case <-freed:
					case <-deadline:
						mu.Lock()
						got := len(released)
						mu.Unlock()
						_ = p.Close()
						return failf("only %d of %d owned publications released their storage: a caller's pool leaks one message per publication that never comes back", got, n)
					}
				}
				if err := <-consumed; err != nil {
					_ = p.Close()
					return failf("consuming the owned publications: %v", err)
				}
				if err := p.Shutdown(ctx); err != nil {
					return failf("Shutdown: %v", err)
				}
				time.Sleep(50 * time.Millisecond)
				mu.Lock()
				defer mu.Unlock()
				if mismatched != 0 {
					return failf("%d releases were handed a message other than the one transferred: the specification says Release is called with the message it was given", mismatched)
				}
				for i, m := range ms {
					if released[m] != 1 {
						return failf("the storage of owned message %d was released %d times, want exactly once: a second release corrupts the caller's free list", i, released[m])
					}
				}
				return nil
			},
		},
		{
			Name: "publish/ambiguous-outcome",
			Run: func(context.Context, *Env) error {
				return Skip("an ambiguous publication needs fault injection the portable API does not expose; conformance/harness's toxiproxy wrapper is where the network-driver form of this lives")
			},
		},
		{
			Name: "dlq/after-max-attempts",
			Run: func(context.Context, *Env) error {
				return Skip("dead-lettering after max attempts is enforced by the runner; the explicit-Reject half of the dead-letter claim is capability/DeadLetter")
			},
		},
	}...)
}

// metadataAndLeaseScenarios is the rest of the list: the metadata modes, the
// redelivery rules, the lease, the start positions and the dead-letter
// obligations that are not about one capability alone.
//
//nolint:gocognit,maintidx // see generalScenarios
func metadataAndLeaseScenarios() []Scenario {
	return []Scenario{
		{
			Name: "metadata/native-strict-rejection",
			Run: func(ctx context.Context, e *Env) error {
				unrepresentable, what := unrepresentableMessage(e.Caps)
				if unrepresentable == nil {
					return Skip("this driver can represent every kind of metadata natively, so strict mode has nothing to refuse")
				}
				p, err := e.Publisher(ctx, mq.PublisherConfig{Metadata: mq.MetadataNativeStrict})
				if err != nil {
					return failf("opening a strict publisher: %v", err)
				}
				defer func() { _ = p.Close() }()
				err = p.Publish(ctx, unrepresentable)
				if err == nil {
					return failf("a strict publisher accepted %s, which this driver says it cannot represent: the specification makes strict mode fail before any I/O", what)
				}
				return wantUnsupported(err, "a strict publisher given "+what)
			},
		},
		{
			Name: "metadata/envelope-roundtrip",
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Metadata: mq.MetadataPortableEnvelope})
				if err != nil {
					return SkipUnsupported(err, "opening an enveloping consumer")
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{Metadata: mq.MetadataPortableEnvelope})
				if err != nil {
					return SkipUnsupported(err, "opening an enveloping publisher")
				}
				defer func() { _ = p.Close() }()
				m := &mq.Message{Key: []byte("k"), Body: []byte("enveloped"), ID: "id-9"}
				m.Headers.Add("dup", []byte("one"))
				m.Headers.Add("dup", []byte{0x00, 0xff})
				m.Properties.ContentType = "application/json"
				if err := p.Publish(ctx, m); err != nil {
					return failf("an enveloped publication: %v", err)
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "enveloped" {
						return failf("the envelope round trip gave body %q", d.Body)
					}
					if string(d.Key) != "k" || d.ID != "id-9" {
						return failf("the envelope round trip gave key %q and id %q", d.Key, d.ID)
					}
					vals := d.Headers.Values("dup")
					if len(vals) != 2 || string(vals[0]) != "one" || !bytes.Equal(vals[1], []byte{0x00, 0xff}) {
						return failf("the envelope lost the duplicate or binary header: %d values", len(vals))
					}
					if d.Properties.ContentType != "application/json" {
						return failf("the envelope lost the content type: %q", d.Properties.ContentType)
					}
					return ack(d)
				})
			},
		},
		{
			Name:    "nack/immediate-redelivery",
			Feature: mq.FeatureNack, Has: true, Positive: true,
			Gate:  claims(mq.FeatureNack),
			Shape: func(n *Need) { n.AckDeadline = 30 * time.Second },
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("again")}); err != nil {
					return failf("Publish: %v", err)
				}
				first := uint32(0)
				if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					first = d.Info().Attempt
					return d.Nack(mq.NackRequeue())
				}); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "again" {
						return failf("the redelivery carries %q", d.Body)
					}
					if second := d.Info().Attempt; first != 0 && second <= first {
						return failf("the redelivery reports attempt %d after a first delivery at %d: the specification counts attempts from 1", second, first)
					}
					if first != 0 && !d.Info().Redelivered {
						return failf("a driver that counts attempts did not mark the redelivery as one")
					}
					return d.Ack()
				})
			},
		},
		{
			Name:    "lease/expiry-redelivery",
			Feature: mq.FeatureAtLeastOnce, Has: true, Positive: true,
			Gate:  claims(mq.FeatureAtLeastOnce),
			Shape: func(n *Need) { n.AckDeadline = 500 * time.Millisecond },
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("abandoned")}); err != nil {
					return failf("Publish: %v", err)
				}
				dst := make([]mq.Delivery, 1)
				bctx, cancel := context.WithTimeout(ctx, e.Timeout())
				defer cancel()
				if n, rerr := c.Receive(bctx, dst); n != 1 {
					return failf("Receive: %d, %v", n, rerr)
				}
				if lease := dst[0].Info().LeaseExpires; lease.IsZero() {
					return Skip("the driver grants no lease, so nothing expires")
				}
				if err := e.Advance(ctx, 3*time.Second); err != nil {
					return failf("advancing past the lease: %v", err)
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "abandoned" {
						return failf("the redelivery carries %q", d.Body)
					}
					return d.Ack()
				})
			},
		},
		{
			Name:    "lease/loss",
			Feature: mq.FeatureExtendLease, Has: true, Positive: true,
			Gate:  claims(mq.FeatureExtendLease),
			Shape: func(n *Need) { n.AckDeadline = 500 * time.Millisecond },
			Run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("lost")}); err != nil {
					return failf("Publish: %v", err)
				}
				dst := make([]mq.Delivery, 1)
				bctx, cancel := context.WithTimeout(ctx, e.Timeout())
				defer cancel()
				if n, rerr := c.Receive(bctx, dst); n != 1 {
					return failf("Receive: %d, %v", n, rerr)
				}
				if err := e.Advance(ctx, 3*time.Second); err != nil {
					return failf("advancing past the lease: %v", err)
				}
				// Reclaim the message, so the first delivery's lease is
				// definitely gone rather than merely old.
				second := make([]mq.Delivery, 1)
				rctx, rcancel := context.WithTimeout(ctx, e.Timeout())
				defer rcancel()
				n, _ := c.Receive(rctx, second)
				if n != 1 {
					return Skip("the message was not reclaimed after its lease expired, so no lease was lost")
				}
				defer func() { _ = second[0].Ack() }()
				err = dst[0].Extend(30 * time.Second)
				if err == nil {
					return failf("extending a lease the broker has taken back succeeded: the handler would keep working on a delivery someone else owns")
				}
				if !errors.Is(err, mq.ErrLeaseLost) && !errors.Is(err, mq.ErrAlreadySettled) && !errors.Is(err, mq.ErrUnsupported) {
					return failf("extending a lost lease reported %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
				}
				return nil
			},
		},
		{
			Name:  "start/positions",
			Shape: func(n *Need) { n.Retain = true },
			Run: func(ctx context.Context, e *Env) error {
				// StartLatest must not replay a topic's history, which is default and the reason the zero value
				// resolves to it.
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("before")}); err != nil {
					return failf("Publish: %v", err)
				}
				latest, err := e.Client.Consumer(ctx, mq.ConsumerConfig{
					Topic: e.Topic, Subscription: e.Sub + "-latest", Start: mq.StartLatest(),
				})
				if err != nil {
					return failf("opening a latest consumer: %v", err)
				}
				defer func() { _ = latest.Close() }()
				if err := expectNothing(ctx, latest, 500*time.Millisecond,
					"a StartLatest subscription replayed a message published before it existed"); err != nil {
					return err
				}
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("after")}); err != nil {
					return failf("Publish: %v", err)
				}
				if err := oneDelivery(ctx, latest, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "after" {
						return failf("a StartLatest subscription received %q", d.Body)
					}
					return ack(d)
				}); err != nil {
					return err
				}
				// The zero StartPosition is StartLatest, not "replay
				// everything": a consumer that omitted it must behave like
				// the one that named it.
				var zero mq.StartPosition
				if zero.Resolve().Kind() != mq.StartKindLatest {
					return failf("the zero StartPosition resolves to %v, want latest", render(zero.Resolve().Kind()))
				}
				return nil
			},
		},
		{
			Name:    "dlq/failed-publish-never-acks",
			Feature: mq.FeatureDeadLetter, Has: true, Positive: true,
			// The scenario's own topology names no dead-letter
			// destination — its Shape leaves DeadLetterTopic empty — so a
			// driver that claims the feature has nowhere to route to, and
			// Reject has to say so rather than destroy the message.
			Gate: claims(mq.FeatureDeadLetter),
			Run: func(ctx context.Context, e *Env) error {
				// The specification: a failed or ambiguous dead-letter publish never
				// acknowledges the original. The portable way to reach it
				// is a topic with no dead-letter destination: Reject must
				// refuse and leave the delivery settleable, rather than
				// destroying the message.
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return failf("opening a consumer: %v", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("nowhere")}); err != nil {
					return failf("Publish: %v", err)
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					err := d.Reject()
					if err == nil {
						return failf("Reject succeeded on a topic with no dead-letter destination: the message was destroyed where the specification requires ErrUnsupported")
					}
					if !errors.Is(err, mq.ErrUnsupported) {
						return failf("Reject with no dead-letter destination reported %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
					}
					if d.Settled() {
						return failf("a refused Reject settled the delivery: the specification leaves a refused settlement settleable")
					}
					// The delivery is still settleable, which is the
					// half that keeps the message from being lost.
					return d.Ack()
				})
			},
		},
		{
			Name:    "wildcards/filters",
			Feature: mq.FeatureWildcardSubscribe, Has: true, Positive: true,
			Gate:  claims(mq.FeatureWildcardSubscribe),
			Shape: func(n *Need) { n.Wildcards = true },
			Run: func(ctx context.Context, e *Env) error {
				// The filtering half of the claim, which the capability
				// probe does not check: a pattern subscription receives
				// what matches it and NOT what does not. A driver whose
				// pattern matched everything would pass the probe and fail
				// here, which is how "wildcards work" turns into "this
				// subscriber sees the whole broker".
				if e.Harness.Wildcard == nil {
					return Skip("the harness does not say how to write a subscription pattern for this broker ")
				}
				pattern := e.Harness.Wildcard(e.Topic)
				if pattern == "" || pattern == e.Topic {
					return Skip("the harness produced no pattern for this topic")
				}
				c, err := e.Client.Consumer(ctx, mq.ConsumerConfig{Topic: pattern, Subscription: e.Sub})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				other := e.Harness.topicName("wildcards-unmatched")
				if err := e.Client.Publish(ctx, other, &mq.Message{Body: []byte("unmatched")}); err != nil {
					return failf("publishing to an unmatched topic: %v", err)
				}
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("matched")}); err != nil {
					return err
				}
				if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "matched" {
						return failf("the pattern subscription received %q, want the matching publication", d.Body)
					}
					return ack(d)
				}); err != nil {
					return err
				}
				return expectNothing(ctx, c, time.Second, "a pattern subscription received a publication to a topic the pattern does not match")
			},
		},
	}
}

// unrepresentableMessage returns a message carrying metadata the driver says it
// cannot represent natively, and a description of it. It returns nil when the
// driver claims all three header capabilities, in which case strict mode has
// nothing to refuse.
func unrepresentableMessage(caps mq.Capabilities) (*mq.Message, string) {
	m := &mq.Message{Body: []byte("strict")}
	switch {
	case !caps.Has(mq.FeatureNativeHeaders):
		m.Headers.Set("any", []byte("header"))
		return m, "a header on a broker with no header support"
	case !caps.Has(mq.FeatureBinaryHeaders):
		m.Headers.Set("bin", []byte{0x00, 0xff})
		return m, "a binary header value"
	case !caps.Has(mq.FeatureDuplicateHeaders):
		m.Headers.Add("dup", []byte("one"))
		m.Headers.Add("dup", []byte("two"))
		return m, "a duplicate header key"
	}
	return nil, ""
}

// testingShort reports whether the run is a short one, without depending on the
// flag having been parsed: [Run] may be called from something that is not a test
// binary, and testing.Short panics there.
func testingShort() (short bool) {
	if !testing.Testing() {
		return false
	}
	defer func() {
		if recover() != nil {
			short = false
		}
	}()
	return testing.Short()
}

// oneDelivery receives exactly one delivery and hands it to fn where it lies.
func oneDelivery(ctx context.Context, c *mq.ConsumerHandle, budget time.Duration, fn func(*mq.Delivery) error) error {
	dst := make([]mq.Delivery, 1)
	bctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	n, err := c.Receive(bctx, dst)
	if n == 0 {
		return &noDeliveryError{inner: err}
	}
	return fn(&dst[0])
}

// noDeliveryError is "the receive produced nothing", which several capability
// probes have to tell apart from a receive that broke.
//
// Nothing arriving is the OBSERVATION a negative capability probe lives on: a
// queue broker's second fan-out reader, a deleted message's second replay
// reader and a non-batching driver's eighth slot all show up as an empty
// receive, and each of those is the driver doing something DIFFERENT rather
// than refusing. A receive that failed for another reason is a probe that
// could not run. Both used to render as one failf and the outcome could not
// tell them apart.
type noDeliveryError struct{ inner error }

// Error implements error, in the wording every committed report already
// carries for this case.
func (n *noDeliveryError) Error() string {
	if n.inner == nil {
		return "no delivery arrived"
	}
	return "no delivery arrived: " + n.inner.Error()
}

// Unwrap keeps the receive's own error, and its portable code, reachable.
func (n *noDeliveryError) Unwrap() error { return n.inner }

// isNoDelivery reports whether err is an empty receive rather than a broken
// one.
func isNoDelivery(err error) bool {
	var n *noDeliveryError
	return errors.As(err, &n)
}

// firstUnsupported returns a feature the driver does not claim, or -1.
func firstUnsupported(caps mq.Capabilities) int {
	for f := range mq.FeatureCount {
		if !caps.Has(mq.Feature(f)) {
			return f
		}
	}
	return -1
}

// fidelityBody is the byte pattern the fidelity scenario publishes: an index and
// a payload derived from it, so that a message can be checked against its own
// identity rather than against a table the kit holds.
func fidelityBody(i int) []byte {
	b := make([]byte, 0, 32)
	b = append(b, "fidelity:"...)
	b = strconv.AppendInt(b, int64(i), 10)
	b = append(b, ':')
	for j := range 8 {
		b = append(b, byte(i*31+j))
	}
	return b
}

// fidelityIndex reads the index back out of a fidelity body.
func fidelityIndex(b []byte) (int, bool) {
	if !bytes.HasPrefix(b, []byte("fidelity:")) {
		return 0, false
	}
	rest := b[len("fidelity:"):]
	i := bytes.IndexByte(rest, ':')
	if i < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(string(rest[:i]))
	if err != nil {
		return 0, false
	}
	return n, true
}

// wake leaves a wake-up token on ch unless one is already waiting.
func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
