package typed_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/typed"
)

// Order is the message type of the specification's own example.
type Order struct {
	ID   int    `json:"id"`
	Cust string `json:"cust"`
}

// recorder is an mq.Publisher that keeps what it was asked to publish.
type recorder struct {
	mu     sync.Mutex
	sent   []*mq.Message
	batch  [][]*mq.Message
	refuse error
}

func (r *recorder) Publish(_ context.Context, m *mq.Message, _ ...mq.PublishOption) error {
	if r.refuse != nil {
		return r.refuse
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, m.Clone())
	return nil
}

func (r *recorder) PublishBatch(_ context.Context, ms []*mq.Message, _ ...mq.PublishOption) error {
	if r.refuse != nil {
		return r.refuse
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*mq.Message, len(ms))
	for i, m := range ms {
		out[i] = m.Clone()
	}
	r.batch = append(r.batch, out)
	return nil
}

func (r *recorder) Flush(context.Context) error { return nil }
func (r *recorder) Close() error                { return nil }

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent) + len(r.batch)
}

var _ mq.Publisher = (*recorder)(nil)

// schemed is a codec that also names a schema, so that the schema-id stamping
// has something to stamp.
type schemed struct{ codec.Typed[Order] }

func (schemed) SchemaID() string { return "orders-v3" }

// unencodable is a value type whose codec always refuses to encode it.
type failingCodec struct{ codec.Typed[Order] }

var errCannotEncode = errors.New("this value does not encode")

func (failingCodec) Encode(Order) ([]byte, error) { return nil, errCannotEncode }

func TestPublishStampsBodyAndContentType(t *testing.T) {
	r := &recorder{}
	p := mustNewPublisher(t, r, codec.JSON[Order]())
	if err := p.Publish(context.Background(), Order{ID: 1, Cust: "cust-42"}); err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	if len(r.sent) != 1 {
		t.Fatalf("%d messages published, want 1", len(r.sent))
	}
	m := r.sent[0]
	if got := string(m.Body); got != `{"id":1,"cust":"cust-42"}` {
		t.Fatalf("body = %s, want the codec's encoding", got)
	}
	if m.Properties.ContentType != codec.ContentTypeJSON {
		t.Fatalf("content type = %q, want %q (the specification sets it automatically)", m.Properties.ContentType, codec.ContentTypeJSON)
	}
	if _, ok := m.Headers.Get(codec.HeaderSchemaID); ok {
		t.Fatal("a schema-id header was stamped by a codec that names no schema")
	}
	if p.ContentType() != codec.ContentTypeJSON {
		t.Fatalf("ContentType() = %q, want the codec's", p.ContentType())
	}
}

func TestPublishStampsTheSchemaID(t *testing.T) {
	r := &recorder{}
	p := mustNewPublisher[Order](t, r, schemed{codec.JSON[Order]()})
	if err := p.Publish(context.Background(), Order{ID: 2}); err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	v, ok := r.sent[0].Headers.Get(codec.HeaderSchemaID)
	if !ok {
		t.Fatal("no schema-id header for a codec that names a schema (the specification sets it automatically)")
	}
	if string(v) != "orders-v3" {
		t.Fatalf("schema-id = %q, want %q", v, "orders-v3")
	}
}

// schemaJSON is a byte-level codec bound to a schema, the shape avro's codec
// has, so that what reaches the publisher is whatever codec.Adapt makes of it.
type schemaJSON struct{ codec.Codec }

func (schemaJSON) SchemaID() string { return "orders-v4" }

// TestAnAdaptedSchemaCodecStampsTheSchemaID is the reported defect: codec.Adapt
// hid the codec's Schemer, so a publisher over avro.Typed -- which is
// codec.Adapt of a schema codec -- never stamped schema-id.
func TestAnAdaptedSchemaCodecStampsTheSchemaID(t *testing.T) {
	c, err := codec.New(codec.Auto)
	if err != nil {
		t.Fatal(err)
	}
	r := &recorder{}
	p := mustNewPublisher(t, r, codec.Adapt[Order](schemaJSON{c}))
	if err := p.Publish(context.Background(), Order{ID: 4}); err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	if v, ok := r.sent[0].Headers.Get(codec.HeaderSchemaID); !ok || string(v) != "orders-v4" {
		t.Fatalf("schema-id = %q (present %v), want the adapted codec's orders-v4", v, ok)
	}
}

// TestPublishMessageRemovesAStaleSchemaID is the reported defect: a message
// that already named a schema kept that header under a codec with none, so a
// JSON body went out labelled with another codec's schema.
func TestPublishMessageRemovesAStaleSchemaID(t *testing.T) {
	r := &recorder{}
	p := mustNewPublisher(t, r, codec.JSON[Order]())
	m := &mq.Message{Topic: "orders"}
	m.Headers.Set(codec.HeaderSchemaID, []byte("avro-fingerprint"))
	m.Headers.Add("x-tenant", []byte("acme"))
	if err := p.PublishMessage(context.Background(), m, Order{ID: 5}); err != nil {
		t.Fatalf("PublishMessage = %v, want nil", err)
	}
	got := r.sent[0]
	if v, ok := got.Headers.Get(codec.HeaderSchemaID); ok {
		t.Fatalf("schema-id = %q survived a codec that names no schema", v)
	}
	if v, ok := got.Headers.Get("x-tenant"); !ok || string(v) != "acme" {
		t.Fatalf("headers = %v, want the caller's own preserved", got.Headers)
	}
}

func TestPublishMessageKeepsTheCallersOwnFields(t *testing.T) {
	r := &recorder{}
	p := mustNewPublisher(t, r, codec.JSON[Order]())
	m := &mq.Message{
		Topic: "orders",
		ID:    "app-7",
		Key:   []byte("cust-42"),
	}
	m.Headers.Add("x-tenant", []byte("acme"))
	if err := p.PublishMessage(context.Background(), m, Order{ID: 3}); err != nil {
		t.Fatalf("PublishMessage = %v, want nil", err)
	}
	got := r.sent[0]
	if got.ID != "app-7" || string(got.Key) != "cust-42" || got.Topic != "orders" {
		t.Fatalf("message = %+v, want the caller's own topic, id and key preserved", got)
	}
	if v, ok := got.Headers.Get("x-tenant"); !ok || string(v) != "acme" {
		t.Fatalf("headers = %v, want the caller's own preserved", got.Headers)
	}
	if !strings.Contains(string(got.Body), `"id":3`) {
		t.Fatalf("body = %s, want the encoded value", got.Body)
	}
}

func TestPublishReportsAnEncodeFailureAndPublishesNothing(t *testing.T) {
	r := &recorder{}
	p := mustNewPublisher[Order](t, r, failingCodec{codec.JSON[Order]()})
	if err := p.Publish(context.Background(), Order{ID: 4}); !errors.Is(err, errCannotEncode) {
		t.Fatalf("Publish = %v, want the codec's own error", err)
	}
	if r.count() != 0 {
		t.Fatalf("%d publications for a value that does not encode, want 0", r.count())
	}
}

func TestPublishBatchRefusesTheWholeBatchOnAnEncodeFailure(t *testing.T) {
	r := &recorder{}
	p := mustNewPublisher[Order](t, r, failingCodec{codec.JSON[Order]()})
	err := p.PublishBatch(context.Background(), []Order{{ID: 1}, {ID: 2}})
	if !errors.Is(err, errCannotEncode) {
		t.Fatalf("PublishBatch = %v, want the codec's own error", err)
	}
	if r.count() != 0 {
		t.Fatalf("%d publications for a batch that did not encode, want 0: a partial batch is not what the caller asked for", r.count())
	}
	var be *mq.BatchError
	if errors.As(err, &be) {
		t.Fatalf("PublishBatch = %v, want the encode failure and not a *mq.BatchError: nothing reached the broker", err)
	}
}

func TestPublishBatchPublishesEveryValueAsOneBatch(t *testing.T) {
	r := &recorder{}
	p := mustNewPublisher(t, r, codec.JSON[Order]())
	if err := p.PublishBatch(context.Background(), []Order{{ID: 1}, {ID: 2}, {ID: 3}}); err != nil {
		t.Fatalf("PublishBatch = %v, want nil", err)
	}
	if len(r.batch) != 1 || len(r.batch[0]) != 3 {
		t.Fatalf("batches = %v, want one batch of three", r.batch)
	}
	for i, m := range r.batch[0] {
		if m.Properties.ContentType != codec.ContentTypeJSON {
			t.Fatalf("batch[%d] content type = %q, want the codec's", i, m.Properties.ContentType)
		}
	}
	if err := p.PublishBatch(context.Background(), nil); err != nil {
		t.Fatalf("PublishBatch of nothing = %v, want nil", err)
	}
	if len(r.batch) != 1 {
		t.Fatalf("an empty batch reached the publisher: %v", r.batch)
	}
}

// deliveryOf builds a delivery a caller could have built: no settlement
// right, which is all these handler tests need.
func deliveryOf(body, ctype string) *mq.Delivery {
	d := &mq.Delivery{Message: mq.Message{Topic: "orders", Body: []byte(body)}}
	d.Properties.ContentType = ctype
	return d
}

// seen records what a typed handler was given.
type seen struct {
	n      atomic.Int64
	values []Order
	mu     sync.Mutex
}

func (s *seen) handle(_ context.Context, d *typed.Delivery[Order]) error {
	s.mu.Lock()
	s.values = append(s.values, d.Value)
	s.mu.Unlock()
	s.n.Add(1)
	return nil
}

func TestHandlerDecodesAndCallsTheTypedHandler(t *testing.T) {
	s := &seen{}
	h := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](s.handle), typed.HandlerConfig{})
	d := deliveryOf(`{"id":9,"cust":"c"}`, codec.ContentTypeJSON)
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil", err)
	}
	if s.n.Load() != 1 {
		t.Fatalf("the handler ran %d times, want 1", s.n.Load())
	}
	if s.values[0] != (Order{ID: 9, Cust: "c"}) {
		t.Fatalf("value = %+v, want the decoded order", s.values[0])
	}
}

func TestTheEmbeddedDeliveryIsTheUntypedOne(t *testing.T) {
	var got *mq.Delivery
	h := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](
		func(_ context.Context, d *typed.Delivery[Order]) error {
			got = d.Delivery
			if d.Topic != "orders" {
				t.Errorf("d.Topic = %q, want the untyped delivery's own", d.Topic)
			}
			if err := d.Ack(); !errors.Is(err, mq.ErrNotSettleable) {
				t.Errorf("d.Ack() = %v, want ErrNotSettleable for a delivery with no settlement right", err)
			}
			return nil
		},
	), typed.HandlerConfig{})
	d := deliveryOf(`{"id":1}`, "")
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil", err)
	}
	if got != d {
		t.Fatalf("the typed delivery embedded %p, want the delivery the runner handed over (%p)", got, d)
	}
}

func TestAnUndecodableBodyIsDeadLetteredAndTheHandlerNeverSeesIt(t *testing.T) {
	s := &seen{}
	h := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](s.handle), typed.HandlerConfig{})
	err := h.Handle(context.Background(), deliveryOf(`{"id":`, codec.ContentTypeJSON))
	if err == nil {
		t.Fatal("Handle = nil for a body that does not decode; the delivery would be acked and the message lost")
	}
	if s.n.Load() != 0 {
		t.Fatalf("the handler ran %d times for a body that did not decode, want 0", s.n.Load())
	}
	if d, _ := mq.DirectiveOf(err); d != mq.DirectiveDeadLetter {
		t.Fatalf("directive = %v, want DirectiveDeadLetter: a decode failure is not transient", d)
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("Handle = %v, want the codec's own *codec.DecodeError reachable through it", err)
	}
	if mq.CodeOf(err) != mq.CodeCorruptMessage {
		t.Fatalf("CodeOf = %v, want CodeCorruptMessage", mq.CodeOf(err))
	}
}

func TestTheDecodeFailurePolicyIsTheCallers(t *testing.T) {
	bad := `{"id":`
	cases := []struct {
		name string
		fn   typed.DecodeErrorFunc
		want mq.Directive
	}{
		{"DeadLetter", typed.DeadLetter, mq.DirectiveDeadLetter},
		{"Discard", typed.Discard, mq.DirectiveDiscard},
		{"Fail", typed.Fail, mq.DirectiveNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &seen{}
			h := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](s.handle),
				typed.HandlerConfig{OnDecodeError: c.fn})
			err := h.Handle(context.Background(), deliveryOf(bad, ""))
			if err == nil {
				t.Fatal("Handle = nil for an undecodable body")
			}
			if d, _ := mq.DirectiveOf(err); d != c.want {
				t.Fatalf("directive = %v, want %v", d, c.want)
			}
			if s.n.Load() != 0 {
				t.Fatal("the handler ran for a body that did not decode")
			}
		})
	}
}

func TestADecodeErrorFuncMaySettleTheDeliveryItself(t *testing.T) {
	var settled atomic.Bool
	h := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](
		func(context.Context, *typed.Delivery[Order]) error { return nil },
	),
		typed.HandlerConfig{OnDecodeError: func(_ context.Context, d *mq.Delivery, _ error) error {
			settled.Store(d != nil)
			return nil
		}})
	if err := h.Handle(context.Background(), deliveryOf(`nonsense`, "")); err != nil {
		t.Fatalf("Handle = %v, want the policy's own nil", err)
	}
	if !settled.Load() {
		t.Fatal("the decode-failure policy was not given the raw delivery")
	}
}

func TestContentTypePolicies(t *testing.T) {
	body := `{"id":1}`
	cases := []struct {
		name    string
		policy  typed.ContentTypePolicy
		ctype   string
		handled bool
	}{
		{"ignore/absent", typed.ContentTypeIgnore, "", true},
		{"ignore/wrong", typed.ContentTypeIgnore, "application/xml", true},
		{"check/absent", typed.ContentTypeCheck, "", true},
		{"check/right", typed.ContentTypeCheck, codec.ContentTypeJSON, true},
		{"check/parameterised", typed.ContentTypeCheck, "application/json; charset=utf-8", true},
		{"check/suffix", typed.ContentTypeCheck, "application/vnd.acme+json", true},
		{"check/wrong", typed.ContentTypeCheck, "application/xml", false},
		{"require/absent", typed.ContentTypeRequire, "", false},
		{"require/right", typed.ContentTypeRequire, codec.ContentTypeJSON, true},
		{"require/wrong", typed.ContentTypeRequire, "application/xml", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &seen{}
			h := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](s.handle),
				typed.HandlerConfig{ContentType: c.policy})
			err := h.Handle(context.Background(), deliveryOf(body, c.ctype))
			if c.handled {
				if err != nil {
					t.Fatalf("Handle = %v, want nil: %s accepts %q", err, c.policy, c.ctype)
				}
				if s.n.Load() != 1 {
					t.Fatalf("the handler ran %d times, want 1", s.n.Load())
				}
				return
			}
			if err == nil {
				t.Fatalf("Handle = nil: %s must refuse %q", c.policy, c.ctype)
			}
			if s.n.Load() != 0 {
				t.Fatal("the handler ran for a content type the policy refuses")
			}
			var cte *codec.ContentTypeError
			if !errors.As(err, &cte) {
				t.Fatalf("Handle = %v, want a *codec.ContentTypeError ", err)
			}
		})
	}
}

func TestTheContentTypeHeaderIsReadWhenThePropertyIsEmpty(t *testing.T) {
	// The specification maps Properties.ContentType to a native field on the backends that
	// have one and to the content-type header everywhere else, so a handler
	// that reads only the property refuses messages from half the drivers.
	s := &seen{}
	h := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](s.handle),
		typed.HandlerConfig{ContentType: typed.ContentTypeRequire})
	d := deliveryOf(`{"id":1}`, "")
	d.Headers.Set(codec.HeaderContentType, []byte(codec.ContentTypeJSON))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil: the content type arrived as a header", err)
	}
	if s.n.Load() != 1 {
		t.Fatal("the handler did not run for a delivery whose content type is a header")
	}
}

func TestNewHandlerAndNewPublisherRefuseNilArguments(t *testing.T) {
	cases := []struct {
		name string
		call func() error
	}{
		{"publisher/nil codec", func() error { _, err := typed.NewPublisher[Order](&recorder{}, nil); return err }},
		{"publisher/nil publisher", func() error { _, err := typed.NewPublisher(nil, codec.JSON[Order]()); return err }},
		{"handler/nil codec", func() error {
			_, err := typed.NewHandler[Order](nil, typed.HandlerFunc[Order](nil), typed.HandlerConfig{})
			return err
		}},
		{"handler/nil handler", func() error {
			_, err := typed.NewHandler[Order](codec.JSON[Order](), nil, typed.HandlerConfig{})
			return err
		}},
		{"handler/bad policy", func() error {
			_, err := typed.NewHandler(codec.JSON[Order](), typed.HandlerFunc[Order](
				func(context.Context, *typed.Delivery[Order]) error { return nil },
			),
				typed.HandlerConfig{ContentType: typed.ContentTypePolicy(9)})
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.call() == nil {
				t.Fatal("no error: a construction error must not become a per-delivery failure")
			}
		})
	}
}

func TestContentTypePolicyString(t *testing.T) {
	for _, c := range []struct {
		p    typed.ContentTypePolicy
		want string
	}{
		{typed.ContentTypeIgnore, "ignore"},
		{typed.ContentTypeCheck, "check"},
		{typed.ContentTypeRequire, "require"},
		{typed.ContentTypePolicy(7), "ContentTypePolicy(?)"},
	} {
		if got := c.p.String(); got != c.want {
			t.Fatalf("String() = %q, want %q", got, c.want)
		}
		if c.p.Valid() != (c.want != "ContentTypePolicy(?)") {
			t.Fatalf("Valid() = %v for %q", c.p.Valid(), c.want)
		}
	}
}

// waitFor polls cond for up to five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRunAgainstTheMemoryDriver(t *testing.T) {
	cl, _ := memory.NewForTest(t, memory.TestTopics("orders"))
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	defer func() { _ = c.Close() }()
	pub, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening the publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	s := &seen{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- typed.Run(ctx, c, codec.JSON[Order](), s.handle)
	}()

	orders := mustNewPublisher(t, pub, codec.JSON[Order]())
	for i := range 8 {
		if err := orders.Publish(context.Background(), Order{ID: i, Cust: "c"}); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	waitFor(t, "every order to be handled", func() bool { return s.n.Load() == 8 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := map[int]bool{}
	for _, v := range s.values {
		if v.Cust != "c" {
			t.Fatalf("value = %+v, want the published order", v)
		}
		ids[v.ID] = true
	}
	if len(ids) != 8 {
		t.Fatalf("saw %d distinct orders, want 8", len(ids))
	}
}

func TestAnUndecodableMessageHaltsARunWithNoDeadLetterRoute(t *testing.T) {
	// ADR-011: dead-letter when there is an explicit route, otherwise halt,
	// never skip. A body that will not decode therefore stops the consumer
	// rather than disappearing.
	cl, _ := memory.NewForTest(t, memory.TestTopics("orders"))
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	defer func() { _ = c.Close() }()

	s := &seen{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- typed.Run(ctx, c, codec.JSON[Order](), s.handle) }()

	waitFor(t, "the subscription to exist", func() bool { return true })
	if err := cl.Publish(context.Background(), "orders", &mq.Message{Body: []byte("not json at all")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	err = <-done
	if !errors.Is(err, mq.ErrPoisonMessage) {
		t.Fatalf("Run = %v, want ErrPoisonMessage: an undecodable message with no dead-letter route halts (ADR-011)", err)
	}
	if s.n.Load() != 0 {
		t.Fatal("the typed handler saw a message that did not decode")
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("Run = %v, want the codec's own error reachable through the halt", err)
	}
}

// TestANilHandlerFuncIsRefusedAtConstruction is the nil handler that is not
// a nil interface. A nil HandlerFunc converted to Handler, and the nil fn
// typed.Run converts, passed NewHandler's h == nil check, so the first
// delivery panicked inside the runner and the consumer halted on a message
// nothing was wrong with: the outcome NewHandler's doc names as the one it
// refuses at construction.
func TestANilHandlerFuncIsRefusedAtConstruction(t *testing.T) {
	t.Run("NewHandler", func(t *testing.T) {
		h, err := typed.NewHandler[Order](codec.JSON[Order](), typed.HandlerFunc[Order](nil), typed.HandlerConfig{})
		if !errors.Is(err, typed.ErrNilHandler) {
			t.Fatalf("NewHandler(HandlerFunc(nil)) = (%T, %v), want ErrNilHandler", h, err)
		}
	})
	t.Run("Run", func(t *testing.T) {
		cl, _ := memory.NewForTest(t, memory.TestTopics("orders"), memory.TestRealClock())
		c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
		if err != nil {
			t.Fatalf("opening the consumer: %v", err)
		}
		defer func() { _ = c.Close() }()
		if err := cl.Publish(context.Background(), "orders", &mq.Message{Body: []byte(`{"id":1}`)}); err != nil {
			t.Fatalf("publishing: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := typed.Run[Order](ctx, c, codec.JSON[Order](), nil, mq.WithMaxAttempts(1)); !errors.Is(err, typed.ErrNilHandler) {
			t.Fatalf("typed.Run with a nil fn = %v, want the construction error ErrNilHandler", err)
		}
	})
}

func mustNewPublisher[T any](tb testing.TB, p mq.Publisher, c codec.Typed[T]) *typed.Publisher[T] {
	tb.Helper()
	pub, err := typed.NewPublisher(p, c)
	if err != nil {
		tb.Fatalf("NewPublisher: %v", err)
	}
	return pub
}

func mustNewHandler[T any](tb testing.TB, c codec.Typed[T], h typed.Handler[T], cfg typed.HandlerConfig) mq.Handler {
	tb.Helper()
	th, err := typed.NewHandler(c, h, cfg)
	if err != nil {
		tb.Fatalf("NewHandler: %v", err)
	}
	return th
}
