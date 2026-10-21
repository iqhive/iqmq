package driverkit_test

import (
	"context"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/wire"
)

// batchSource hands out one fixed batch and settles through s.
type batchSource struct {
	batch []mq.Message
	s     *extendingDriverSettler
}

func (b *batchSource) Fetch(_ context.Context, dst []mq.DriverDelivery) (int, error) {
	n := min(len(dst), len(b.batch))
	for i := range n {
		dst[i] = mq.DriverDelivery{Message: &b.batch[i], Token: uint64(i)}
	}
	return n, nil
}

func (b *batchSource) Settler() mq.Settler { return b.s }

func (b *batchSource) Close() error { return nil }

// TestEnvelopeFetchKeepsTheBatchPastARefusal holds that one envelope this
// consumer refuses costs that delivery and no other: the deliveries after it
// were taken from the source too, so they are still handed over, and the
// refused one is dead-lettered rather than held by nobody.
func TestEnvelopeFetchKeepsTheBatchPastARefusal(t *testing.T) {
	writer, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	reader, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:   mq.MetadataPortableEnvelope,
		Limits: &wire.Limits{MaxHeaderKey: 8, MaxID: 64, MaxProp: 64},
	})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	batch := make([]mq.Message, 0, 3)
	for _, key := range []string{"a", strings.Repeat("k", 9), "b"} {
		w, werr := writer.Wrap(headerMessage(key))
		if werr != nil {
			t.Fatalf("Wrap: %v", werr)
		}
		batch = append(batch, *w)
	}
	settler := &extendingDriverSettler{}
	src := reader.WrapSource(&batchSource{batch: batch, s: settler})
	dst := make([]mq.DriverDelivery, 3)
	n, err := src.Fetch(t.Context(), dst)
	if err == nil {
		t.Fatal("a batch holding a refused envelope reported no error")
	}
	if n != 2 || dst[0].Token != 0 || dst[1].Token != 2 {
		t.Fatalf("Fetch handed over %d deliveries (%v), want the two that decoded, 0 and 2", n, dst[:n])
	}
	if _, ok := dst[1].Message.Headers.Get("b"); !ok {
		t.Fatal("the delivery after the refusal was handed over undecoded")
	}
	settler.mu.Lock()
	defer settler.mu.Unlock()
	if len(settler.nacked) != 1 || settler.nacked[0] != 1 {
		t.Fatalf("the settler nacked %v, want only the refused delivery, 1", settler.nacked)
	}
}
