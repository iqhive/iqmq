package typed_test

import (
	"context"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/typed"
)

// idCodec is a codec whose Encode and Decode allocate nothing, so that
// TestAllocs measures what the typed layer itself costs rather than what a
// JSON decoder costs.
type idCodec struct{}

func (idCodec) ContentType() string { return "application/x-id" }

func (idCodec) Encode(v []byte) ([]byte, error) { return v, nil }

func (idCodec) Decode(b []byte) ([]byte, error) { return b, nil }

var _ codec.Typed[[]byte] = idCodec{}

// TestAllocs pins what typed dispatch and typed publication cost, exactly
// (rule 1).
//
// The numbers are not zero and are not meant to be: a typed dispatch builds
// one Delivery[T] per message so that a handler can never be handed one that
// another goroutine is still using (the safe default, ADR-009), and a
// typed Publish builds the message it publishes. What the test pins is that
// the count does not grow.
func TestAllocs(t *testing.T) {
	ctx := context.Background()

	t.Run("Dispatch", func(t *testing.T) {
		h := typed.NewHandler[[]byte](idCodec{}, typed.HandlerFunc[[]byte](
			func(_ context.Context, d *typed.Delivery[[]byte]) error {
				if len(d.Value) == 0 {
					return mq.ErrUnsupported
				}
				return nil
			},
		), typed.HandlerConfig{})
		d := &mq.Delivery{Message: mq.Message{Topic: "orders", Body: []byte("payload")}}
		// One allocation: the Delivery[T] handed to the typed handler.
		if got := testing.AllocsPerRun(1000, func() {
			if err := h.Handle(ctx, d); err != nil {
				t.Fatal(err)
			}
		}); got != 1 {
			t.Fatalf("a typed dispatch allocated %v times, want exactly 1 (the Delivery[T])", got)
		}
	})

	t.Run("Dispatch/content-type", func(t *testing.T) {
		h := typed.NewHandler[[]byte](idCodec{}, typed.HandlerFunc[[]byte](
			func(context.Context, *typed.Delivery[[]byte]) error { return nil },
		),
			typed.HandlerConfig{ContentType: typed.ContentTypeRequire})
		d := &mq.Delivery{Message: mq.Message{Topic: "orders", Body: []byte("payload")}}
		d.Properties.ContentType = "application/x-id"
		// Checking the content type must not cost an allocation: a policy
		// that parses the media type per delivery would show up here.
		if got := testing.AllocsPerRun(1000, func() {
			if err := h.Handle(ctx, d); err != nil {
				t.Fatal(err)
			}
		}); got != 1 {
			t.Fatalf("a typed dispatch with ContentTypeRequire allocated %v times, want exactly 1", got)
		}
	})

	t.Run("Publish", func(t *testing.T) {
		p := typed.NewPublisher[[]byte](discard{}, idCodec{})
		body := []byte("payload")
		// Two: the mq.Message Publish builds, which escapes into the
		// publisher, and the variadic option array.
		if got := testing.AllocsPerRun(1000, func() {
			if err := p.Publish(ctx, body); err != nil {
				t.Fatal(err)
			}
		}); got != 1 {
			t.Fatalf("a typed Publish allocated %v times, want exactly 1 (the message)", got)
		}
	})
}

// discard is an mq.Publisher that keeps nothing, so that the allocation of a
// typed publication is the typed layer's alone.
type discard struct{}

func (discard) Publish(context.Context, *mq.Message, ...mq.PublishOption) error { return nil }

func (discard) PublishBatch(context.Context, []*mq.Message, ...mq.PublishOption) error { return nil }

func (discard) Flush(context.Context) error { return nil }

func (discard) Close() error { return nil }
