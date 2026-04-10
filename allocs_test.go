package iqmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// The values the option call sites use. They are package-level so that a call
// site is a call with arguments and not a call with constants the compiler
// could fold away.
var (
	benchDelay = 10 * time.Minute
	benchTTL   = time.Hour
)

// The sinks the accessor measurements assign to. They are package-level so
// that the result of an accessor cannot be dropped by the compiler, and so
// that a value which would have to escape does escape.
var (
	infoSink   mq.DeliveryInfo
	nativeSink any
)

// benchCapabilities is a filled capability set: every feature native, every
// limit advertised. It is what the allocation and benchmark call sites ask.
func benchCapabilities() mq.Capabilities {
	var caps mq.Capabilities
	for f := mq.Feature(0); int(f) < mq.FeatureCount; f++ {
		if err := caps.Set(f, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic}); err != nil {
			panic(err)
		}
	}
	caps.SetLimits(mq.Limits{
		MaxMessageBytes:  mq.Limit[int64]{Value: 1 << 20, Known: true},
		MaxBatchMessages: mq.Limit[int]{Value: 1000, Known: true},
		MaxHeaderCount:   mq.Limit[int]{Value: 64, Known: true},
		MaxHeaderBytes:   mq.Limit[int64]{Value: 8 << 10, Known: true},
		MaxDelay:         mq.Limit[time.Duration]{Value: time.Hour, Known: true},
	})
	return caps
}

// publishStub is the call shape Publish will have: a variadic
// PublishOption tail folded into a local PublishOptions. It stands in for
// Publish so that the claim — "0 allocs at call sites" — is measured at a
// real variadic call site in a different package, which is where the variadic
// array is either stack-allocated or not.
//
// The noinline directive is deliberate: inlined into the measuring closure,
// the call would prove nothing about a caller in another module.
//
//go:noinline
func publishStub(m *mq.Message, opts ...mq.PublishOption) error {
	var o mq.PublishOptions
	if err := o.Apply(opts...); err != nil {
		return err
	}
	if o.Delay < 0 || m == nil {
		return errors.New("unreachable, but the result has to be read")
	}
	return nil
}

// TestAllocs pins the allocation counts state, exactly.
//
// The specification rule 1: "every core hot path has a testing.AllocsPerRun test
// asserting the exact count". Not a bound — the count, so that a path that
// starts allocating one object fails here rather than at a release
// benchmark.
func TestAllocs(t *testing.T) {
	t.Run("PublishOption", func(t *testing.T) {
		m := &mq.Message{Topic: "orders", Body: []byte("x")}
		cases := []struct {
			name string
			call func()
		}{
			{"none", func() {
				if err := publishStub(m); err != nil {
					t.Fatal(err)
				}
			}},
			{"Delay", func() {
				if err := publishStub(m, mq.Delay(benchDelay)); err != nil {
					t.Fatal(err)
				}
			}},
			{"Delay+TTL", func() {
				if err := publishStub(m, mq.Delay(benchDelay), mq.TTL(benchTTL)); err != nil {
					t.Fatal(err)
				}
			}},
			{"five", func() {
				if err := publishStub(m, mq.Delay(benchDelay), mq.TTL(benchTTL),
					mq.Priority(7), mq.Partition(3), mq.DedupKey("order-42")); err != nil {
					t.Fatal(err)
				}
			}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if n := testing.AllocsPerRun(1000, c.call); n != 0 {
					t.Fatalf("the publish call site with %s allocated %v times, want exactly 0 ", c.name, n)
				}
			})
		}
	})

	t.Run("Headers", func(t *testing.T) {
		// Eight headers is the count the specification states the budget at.
		var h mq.Headers
		for i := range 8 {
			key, val := benchHeaderPair(i)
			h.Add(key, val)
		}
		last, _ := benchHeaderPair(7)
		val := []byte("benchmark-value")
		cases := []struct {
			name string
			call func()
		}{
			{"Get/present", func() { h.Get(last) }},
			{"Get/absent", func() { h.Get("absent") }},
			// Set of a key already present replaces in place and cannot
			// grow, so its budget is zero and not "may grow once".
			{"Set/present", func() { h.Set(last, val) }},
			{"Len", func() { _ = h.Len() }},
			{"All", func() {
				for range h.All() {
					_ = 0
				}
			}},
			{"Validate", func() {
				if err := h.Validate(mq.Limits{
					MaxHeaderCount: mq.Limit[int]{Value: 64, Known: true},
					MaxHeaderBytes: mq.Limit[int64]{Value: 8 << 10, Known: true},
				}, true); err != nil {
					t.Fatal(err)
				}
			}},
			{"Add+Delete", func() {
				// Add appends into the spare capacity a steady-state header
				// set has, and Delete takes the pair straight back out, so
				// the pair the loop adds is never charged for growth.
				h.Add("alloc-probe", val)
				h.Delete("alloc-probe")
			}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if n := testing.AllocsPerRun(1000, c.call); n != 0 {
					t.Fatalf("Headers.%s over 8 headers allocated %v times, want exactly 0 ", c.name, n)
				}
			})
		}
	})

	t.Run("Capabilities", func(t *testing.T) {
		caps := benchCapabilities()
		req := mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native)
		any := mq.RequireAny(mq.FeatureScheduledPublish, mq.Native, mq.EmulatedDurable)
		cases := []struct {
			name string
			call func()
		}{
			{"Has", func() { caps.Has(mq.FeatureExplicitAck) }},
			{"Support", func() { caps.Support(mq.FeatureExplicitAck) }},
			{"Limit", func() { caps.Limit(mq.LimitMaxHeaderCount) }},
			{"Require", func() {
				if err := caps.Require(req, any); err != nil {
					t.Fatal(err)
				}
			}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if n := testing.AllocsPerRun(1000, c.call); n != 0 {
					t.Fatalf("Capabilities.%s allocated %v times, want exactly 0 ", c.name, n)
				}
			})
		}
	})

	t.Run("Errors", func(t *testing.T) {
		// The specification: no fmt on a hot path and hot-path *OpError values
		// preallocated per handle. A preallocated error is only worth
		// preallocating if comparing it costs nothing either.
		opErr := &mq.OpError{
			Driver: "memory", Operation: mq.OpPublish, Topic: "orders",
			Code: mq.CodeClosed,
		}
		wrapped := errors.Join(errors.New("outer"), opErr)
		// A batch whose ambiguous entry is its last: IsAmbiguous reads every
		// entry of a multi-error, and reading them must cost nothing either.
		batch := &mq.BatchError{Errs: []error{opErr, nil, &mq.OpError{Code: mq.CodeUnavailable, Ambiguous: true}}}
		// IsRetryable short-circuits on a Retryable flag that is false, so
		// opErr above never reaches the IsAmbiguous walk the predicate gained
		// -- a pin that cannot see the new work is not
		// a pin. These two do reach it: the cheap case (an *OpError that
		// answers at once) and the expensive one (every entry of a batch).
		retryable := errors.Join(errors.New("outer"),
			&mq.OpError{Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true})
		retryableBatch := &mq.BatchError{Errs: []error{
			&mq.OpError{Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true},
			nil,
			&mq.OpError{Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true},
		}}
		cases := []struct {
			name string
			call func()
		}{
			{"Is/sentinel", func() {
				if !errors.Is(opErr, mq.ErrClosed) {
					t.Fatal("no match")
				}
			}},
			{"Is/wrapped", func() {
				if !errors.Is(wrapped, mq.ErrClosed) {
					t.Fatal("no match")
				}
			}},
			{"CodeOf", func() {
				if mq.CodeOf(wrapped) != mq.CodeClosed {
					t.Fatal("wrong code")
				}
			}},
			{"IsRetryable", func() { mq.IsRetryable(wrapped) }},
			{"IsRetryable/reachesTheAmbiguityWalk", func() {
				if !mq.IsRetryable(retryable) {
					t.Fatal("a definitely refused, retryable error is not retryable")
				}
			}},
			{"IsRetryable/batch", func() {
				if mq.IsRetryable(retryableBatch) {
					t.Fatal("a batch with an ambiguous entry reported itself safe to repeat")
				}
			}},
			{"IsAmbiguous", func() { mq.IsAmbiguous(wrapped) }},
			{"IsAmbiguous/batch", func() {
				if !mq.IsAmbiguous(batch) {
					t.Fatal("the ambiguous entry was not found")
				}
			}},
			{"DirectiveOf", func() { mq.DirectiveOf(wrapped) }},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if n := testing.AllocsPerRun(1000, c.call); n != 0 {
					t.Fatalf("%s allocated %v times, want exactly 0 ", c.name, n)
				}
			})
		}
	})
	t.Run("Publish", func(t *testing.T) {
		// The specification: "core-attributable allocations per message (network
		// drivers) ≤ 2 (0 with reuse)", and rule 1: the exact count,
		// not a bound. The driver under these calls allocates nothing per
		// message, so what is counted is the core's own cost.
		cl, p := allocsPublisher(t)
		ctx := context.Background()
		m := &mq.Message{Topic: "orders", Key: []byte("cust-42"), Body: []byte("body")}
		cases := []struct {
			name string
			want float64
			call func()
		}{
			{"bound/nooptions", 0, func() {
				if err := p.Publish(ctx, m); err != nil {
					t.Fatal(err)
				}
			}},
			{"bound/options", 0, func() {
				if err := p.Publish(ctx, m, mq.Delay(benchDelay), mq.TTL(benchTTL)); err != nil {
					t.Fatal(err)
				}
			}},
			{"client/nooptions", 0, func() {
				if err := cl.Publish(ctx, "orders", m); err != nil {
					t.Fatal(err)
				}
			}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if n := testing.AllocsPerRun(1000, c.call); n != c.want {
					t.Fatalf("Publish (%s) allocated %v times per message, want exactly %v ", c.name, n, c.want)
				}
			})
		}
	})

	t.Run("Delivery", func(t *testing.T) {
		// the accessors read metadata the consumer already holds, so
		// none of them may cost an allocation: an accessor that allocates
		// is the wrong shape, and a handler that reads Info per message
		// would pay for it per message. CloneMessage is deliberately not
		// here — a deep copy is what it was asked for.
		c := allocsConsumer(t, true)
		dst := make([]mq.Delivery, 1)
		if n, err := c.Receive(context.Background(), dst); n != 1 || err != nil {
			t.Fatalf("Receive = (%d, %v)", n, err)
		}
		d := &dst[0]
		cases := []struct {
			name string
			call func()
		}{
			{"Info", func() { infoSink = d.Info() }},
			{"Native", func() { nativeSink = d.Native() }},
			{"NativeAs", func() {
				m, ok := mq.NativeAs[*mq.Message](d)
				if !ok {
					t.Fatal("NativeAs reported false")
				}
				nativeSink = m
			}},
			{"Extend", func() {
				if err := d.Extend(time.Minute); err != nil {
					t.Fatal(err)
				}
			}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if n := testing.AllocsPerRun(1000, c.call); n != 0 {
					t.Fatalf("Delivery.%s allocated %v times, want exactly 0 ", c.name, n)
				}
			})
		}
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Receive", func(t *testing.T) {
		// The receive path's shape is the winner (ADR-006, Value): the
		// caller owns the slice, so a batch costs no container and a
		// delivery costs no message.
		//
		// What is left is the settlement record, and that is ADR-009's
		// decision showing up as a number: in the default safe mode a
		// delivery's settlement storage is allocated once and never
		// recycled — one allocation per message, inside the budget of
		// two — and under BufferReuse it comes back from the consumer's own
		// free list, which is the budget's "0 with reuse".
		for _, tc := range []struct {
			name  string
			reuse bool
			batch int
			want  float64
		}{
			{"safe/batch=1", false, 1, 1},
			{"safe/batch=32", false, 32, 32},
			{"reuse/batch=1", true, 1, 0},
			{"reuse/batch=32", true, 32, 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c := allocsConsumer(t, tc.reuse)
				ctx := context.Background()
				dst := make([]mq.Delivery, tc.batch)
				call := func() {
					n, err := c.Receive(ctx, dst)
					if err != nil || n != len(dst) {
						t.Fatalf("Receive = (%d, %v)", n, err)
					}
					for i := range n {
						if err := dst[i].Ack(); err != nil {
							t.Fatalf("Ack: %v", err)
						}
					}
				}
				if n := testing.AllocsPerRun(1000, call); n != tc.want {
					t.Fatalf("Receive+Ack of %d messages allocated %v times, want exactly %v ",
						tc.batch, n, tc.want)
				}
			})
		}
	})
}

// The allocation harness: a driver that allocates nothing per message, so
// that TestAllocs measures the core and not the fake broker.
//
// It is deliberately not the fake backend of spi_test.go, which clones every
// message it retains and keeps a queue — a real driver's behaviour, and
// noise in a measurement of the core-attributable count.

// allocsBackend is the driver.
type allocsBackend struct {
	caps mq.Capabilities
	msg  mq.Message
}

// newAllocsBackend builds a backend that advertises everything and limits
// nothing, so no publication is refused before it reaches the sink.
func newAllocsBackend() *allocsBackend {
	b := &allocsBackend{msg: mq.Message{Topic: "orders", Key: []byte("cust-42"), Body: []byte("body")}}
	b.caps = benchCapabilities()
	return b
}

func (b *allocsBackend) Info() mq.DriverInfo           { return mq.DriverInfo{Name: "allocs"} }
func (b *allocsBackend) Capabilities() mq.Capabilities { return b.caps }

func (b *allocsBackend) OpenSink(context.Context, mq.SinkConfig) (mq.Sink, error) {
	return allocsSink{}, nil
}

func (b *allocsBackend) OpenSource(context.Context, mq.SourceConfig) (mq.Source, error) {
	return &allocsSource{b: b}, nil
}

func (b *allocsBackend) Ping(context.Context) error { return nil }
func (b *allocsBackend) Native() any                { return nil }
func (b *allocsBackend) Close() error               { return nil }

// allocsSink discards. It implements Sink and nothing else, so the
// measurement runs through the mandatory surface.
type allocsSink struct{}

func (allocsSink) Publish(context.Context, *mq.Message, *mq.PublishOptions) error { return nil }
func (allocsSink) Close() error                                                   { return nil }

// allocsSource hands out deliveries from storage it owns, under a new
// generation each time, and settles them without bookkeeping.
type allocsSource struct {
	b   *allocsBackend
	gen uint32
}

func (s *allocsSource) Fetch(_ context.Context, dst []mq.DriverDelivery) (int, error) {
	for i := range dst {
		s.gen++
		dst[i] = mq.DriverDelivery{
			Message: &s.b.msg,
			Info:    mq.DeliveryInfo{Topic: "orders", Subscription: "billing", Attempt: 1},
			Token:   uint64(i),
			Gen:     s.gen,
			// A pointer in an interface field costs no allocation, which
			// is why the escape hatch can be measured at zero.
			Native: &s.b.msg,
		}
	}
	return len(dst), nil
}

func (s *allocsSource) Settler() mq.Settler { return s }
func (s *allocsSource) Close() error        { return nil }

func (s *allocsSource) Ack(context.Context, *mq.DriverDelivery) error { return nil }

func (s *allocsSource) Nack(context.Context, *mq.DriverDelivery, mq.NackOptions) error { return nil }

// Extend is the optional DeadlineExtender, so that Delivery.Extend is
// measured through a driver rather than at the core's ErrUnsupported.
func (s *allocsSource) Extend(context.Context, *mq.DriverDelivery, time.Duration) error {
	return nil
}

// allocsPublisher opens a client and a bound publisher over the harness.
func allocsPublisher(tb testing.TB) (*mq.Client, *mq.PublisherHandle) {
	tb.Helper()
	cl := mq.NewClient(newAllocsBackend())
	tb.Cleanup(func() {
		if err := cl.Close(); err != nil {
			tb.Errorf("closing the client: %v", err)
		}
	})
	p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		tb.Fatal(err)
	}
	return cl, p
}

// allocsConsumer opens a client and a consumer over the harness.
func allocsConsumer(tb testing.TB, reuse bool) *mq.ConsumerHandle {
	tb.Helper()
	cl := mq.NewClient(newAllocsBackend())
	tb.Cleanup(func() {
		if err := cl.Close(); err != nil {
			tb.Errorf("closing the client: %v", err)
		}
	})
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{
		Topic:        "orders",
		Subscription: "billing",
		BufferReuse:  reuse,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return c
}
