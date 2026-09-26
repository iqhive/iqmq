package iqmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestAutoIDWritesIntoTheCallersMessage pins AutoID and
// Timestamp mutate the message the caller still owns, and only the field the
// caller left empty.
//
// The alternative — publishing a copy — costs the allocation per message
// The specification rule 1 forbids, so the mutation is the shape, and what makes it not a
// surprise is that it happens only when the caller configured it and only
// where the caller wrote nothing. Both halves are pinned here, because a
// publisher that overwrote a caller's own identifier would break
// de-duplication and a publisher that stamped nothing would break ordering.
func TestAutoIDWritesIntoTheCallersMessage(t *testing.T) {
	t.Parallel()
	t.Run("off-by-default", func(t *testing.T) {
		t.Parallel()
		b, p := advClient(t, openPlain, mq.PublisherConfig{})
		m := &mq.Message{Body: []byte("x")}
		if err := p.Publish(context.Background(), m); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if m.ID != "" || !m.Properties.CreatedAt.IsZero() {
			t.Fatalf("an unconfigured publisher stamped (%q, %v), want nothing: the specification generates neither unless configured",
				m.ID, m.Properties.CreatedAt)
		}
		if got := b.records()[0]; got.id != "" || !got.made.IsZero() {
			t.Fatalf("the driver saw (%q, %v), want nothing", got.id, got.made)
		}
	})

	t.Run("stamped-in-place", func(t *testing.T) {
		t.Parallel()
		clk := newFixedClock()
		b := newAdvBroker(openPlain)
		gen := &countingID{}
		p := advPublisher(t, b, mq.PublisherConfig{AutoID: gen, Timestamp: true}, mq.WithClock(clk))
		m := &mq.Message{Body: []byte("x")}
		if err := p.Publish(context.Background(), m); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if m.ID != "gen-a" {
			t.Fatalf("the caller's message carries ID %q, want gen-a: the stamp is in place ", m.ID)
		}
		if !m.Properties.CreatedAt.Equal(clk.Now()) {
			t.Fatalf("the caller's message carries CreatedAt %v, want %v", m.Properties.CreatedAt, clk.Now())
		}
		if got := b.records()[0]; got.id != "gen-a" || !got.made.Equal(clk.Now()) {
			t.Fatalf("the driver saw (%q, %v), want (gen-a, %v)", got.id, got.made, clk.Now())
		}
	})

	t.Run("never-overwrites", func(t *testing.T) {
		t.Parallel()
		clk := newFixedClock()
		b := newAdvBroker(openPlain)
		made := clk.Now().Add(-time.Hour)
		p := advPublisher(t, b, mq.PublisherConfig{AutoID: &countingID{}, Timestamp: true}, mq.WithClock(clk))
		m := &mq.Message{Body: []byte("x"), ID: "caller-chose-this"}
		m.Properties.CreatedAt = made
		if err := p.Publish(context.Background(), m); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if m.ID != "caller-chose-this" {
			t.Fatalf("the publisher rewrote the caller's ID to %q: a rewritten identifier breaks de-duplication", m.ID)
		}
		if !m.Properties.CreatedAt.Equal(made) {
			t.Fatalf("the publisher rewrote CreatedAt to %v, want %v", m.Properties.CreatedAt, made)
		}
	})

	t.Run("every-path", func(t *testing.T) {
		t.Parallel()
		// A stamp applied on one publication path and not another is the
		// failure nobody notices until a consumer sees half its messages
		// without identifiers.
		paths := []struct {
			name string
			call func(t *testing.T, p *mq.PublisherHandle, m *mq.Message)
		}{
			{"Publish", func(t *testing.T, p *mq.PublisherHandle, m *mq.Message) {
				if err := p.Publish(context.Background(), m); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}},
			{"PublishBatch", func(t *testing.T, p *mq.PublisherHandle, m *mq.Message) {
				if err := p.PublishBatch(context.Background(), []*mq.Message{m}); err != nil {
					t.Fatalf("PublishBatch: %v", err)
				}
			}},
			{"PublishItems", func(t *testing.T, p *mq.PublisherHandle, m *mq.Message) {
				if err := p.PublishItems(context.Background(), []mq.PublishItem{{Message: m}}); err != nil {
					t.Fatalf("PublishItems: %v", err)
				}
			}},
			{"PublishCompiled", func(t *testing.T, p *mq.PublisherHandle, m *mq.Message) {
				set, err := p.CompileOptions(p.Defaults())
				if err != nil {
					t.Fatalf("CompileOptions: %v", err)
				}
				if err := p.PublishCompiled(context.Background(), m, set); err != nil {
					t.Fatalf("PublishCompiled: %v", err)
				}
			}},
			{"PublishAsync", func(t *testing.T, p *mq.PublisherHandle, m *mq.Message) {
				if err := p.PublishAsync(context.Background(), m, nil); err != nil {
					t.Fatalf("PublishAsync: %v", err)
				}
			}},
			{"PublishOwned", func(t *testing.T, p *mq.PublisherHandle, m *mq.Message) {
				if err := p.PublishOwned(context.Background(), mq.OwnedMessage{Message: m}); err != nil {
					t.Fatalf("PublishOwned: %v", err)
				}
			}},
			{"PublishBatchOwned", func(t *testing.T, p *mq.PublisherHandle, m *mq.Message) {
				if err := p.PublishBatchOwned(context.Background(), []mq.OwnedMessage{{Message: m}}); err != nil {
					t.Fatalf("PublishBatchOwned: %v", err)
				}
			}},
		}
		for _, path := range paths {
			t.Run(path.name, func(t *testing.T) {
				t.Parallel()
				clk := newFixedClock()
				b := newAdvBroker(openPlain)
				p := advPublisher(t, b, mq.PublisherConfig{AutoID: staticID{"stamped"}, Timestamp: true}, mq.WithClock(clk))
				m := &mq.Message{Body: []byte("x")}
				path.call(t, p, m)
				if m.ID != "stamped" {
					t.Fatalf("%s left the message's ID %q, want stamped", path.name, m.ID)
				}
				if m.Properties.CreatedAt.IsZero() {
					t.Fatalf("%s left CreatedAt at the zero time", path.name)
				}
			})
		}
	})
}

// TestReceiptsFilledInPlace pins the receipt protocol: the storage is the
// caller's, the publisher zeroes it before the driver is told anything, and
// the driver fills it on success.
func TestReceiptsFilledInPlace(t *testing.T) {
	t.Parallel()
	t.Run("needs-the-feature", func(t *testing.T) {
		t.Parallel()
		b := newAdvBroker(openPlain, mq.FeatureReceipts)
		cl := mq.NewClient(b)
		t.Cleanup(func() { _ = cl.Close() })
		_, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders", Receipts: true})
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("opening a receipt publisher over a driver without FeatureReceipts = %v, want ErrUnsupported", err)
		}
	})

	t.Run("told-the-driver", func(t *testing.T) {
		t.Parallel()
		b, _ := advClient(t, openPlain, mq.PublisherConfig{Receipts: true})
		b.mu.Lock()
		cfg := b.sinkCfg
		b.mu.Unlock()
		if !cfg.Receipts {
			t.Fatal("SinkConfig.Receipts is false: a driver has to be told at open, not per message")
		}
	})

	t.Run("filled", func(t *testing.T) {
		t.Parallel()
		_, p := advClient(t, openPlain, mq.PublisherConfig{Receipts: true})
		var r mq.PublishReceipt
		m := &mq.Message{Body: []byte("x"), Receipt: &r}
		if err := p.Publish(context.Background(), m); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if r.MessageID != "srv-x" || r.Partition != 7 || r.Offset != 1 {
			t.Fatalf("the receipt is %+v, want the broker's answer filled in place", r)
		}
		if m.Receipt != &r {
			t.Fatal("the publisher replaced the caller's receipt slot")
		}
	})

	t.Run("zeroed-before-each-publication", func(t *testing.T) {
		t.Parallel()
		b, p := advClient(t, openPlain, mq.PublisherConfig{Receipts: true})
		var r mq.PublishReceipt
		m := &mq.Message{Body: []byte("x"), Receipt: &r}
		if err := p.Publish(context.Background(), m); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		// The second publication fails, so the slot must not still hold the
		// first one's answer: a stale receipt read as this publication's is
		// worse than none.
		refused := errors.New("refused")
		b.setFail(func(int, *mq.Message) error { return refused })
		if err := p.Publish(context.Background(), m); !errors.Is(err, refused) {
			t.Fatalf("Publish = %v, want the driver's refusal", err)
		}
		if r != (mq.PublishReceipt{}) {
			t.Fatalf("the receipt of a failed publication is %+v, want the zero receipt", r)
		}
	})

	t.Run("no-slot", func(t *testing.T) {
		t.Parallel()
		b, p := advClient(t, openPlain, mq.PublisherConfig{Receipts: true})
		if err := p.Publish(context.Background(), &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if got := len(b.records()); got != 1 {
			t.Fatalf("the driver saw %d publications, want 1: a message with no receipt slot publishes normally", got)
		}
	})

	t.Run("ignored-when-off", func(t *testing.T) {
		t.Parallel()
		_, p := advClient(t, openPlain, mq.PublisherConfig{})
		var r mq.PublishReceipt
		if err := p.Publish(context.Background(), &mq.Message{Body: []byte("x"), Receipt: &r}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if r != (mq.PublishReceipt{}) {
			t.Fatalf("a publisher without Receipts filled %+v, want nothing", r)
		}
	})

	t.Run("async", func(t *testing.T) {
		t.Parallel()
		b, p := advClient(t, openAsync, mq.PublisherConfig{Receipts: true})
		sink := advSinkOf[*advAsyncSink](t, b)
		var r mq.PublishReceipt
		got := make(chan mq.PublishResult, 1)
		m := &mq.Message{Body: []byte("x"), Receipt: &r}
		if err := p.PublishAsync(context.Background(), m, func(res mq.PublishResult) { got <- res }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		sink.complete(1, nil)
		select {
		case res := <-got:
			if res.Err != nil {
				t.Fatalf("the callback reported %v", res.Err)
			}
			if res.Receipt.MessageID != "srv-x" {
				t.Fatalf("the result's receipt is %+v, want the broker's answer", res.Receipt)
			}
			if res.Message != m {
				t.Fatal("the result names a different message than the one published")
			}
		default:
			t.Fatal("the callback did not run")
		}
	})

	t.Run("clone-copies-the-slot", func(t *testing.T) {
		t.Parallel()
		m := &mq.Message{Body: []byte("x"), Receipt: &mq.PublishReceipt{MessageID: "a"}}
		c := m.Clone()
		if c.Receipt == m.Receipt {
			t.Fatal("Clone shared the receipt slot: neither message may be observed through the other")
		}
		c.Receipt.MessageID = "b"
		if m.Receipt.MessageID != "a" {
			t.Fatalf("writing the clone's receipt changed the original to %q", m.Receipt.MessageID)
		}
		m.Reset()
		if m.Receipt != nil {
			t.Fatal("Reset left the receipt slot in place: a pooled message would leak the previous one's receipt")
		}
	})
}
