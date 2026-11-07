package driverkit_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

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
// were taken from the source too, so they are still handed over, the refused
// one is dead-lettered rather than held by nobody, and the fetch does not
// fail for it. A runner ends its Run on a fetch error, so a refusal reported
// that way stopped the consumer on the record every time it came back.
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
	if err != nil {
		t.Fatalf("a batch holding a refused envelope failed the fetch: %v", err)
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

// queueSource hands out one batch per Fetch, in order, and once they are all
// gone blocks until ctx is done. A nil s is a source with no settler.
type queueSource struct {
	batches [][]mq.Message
	s       mq.Settler
	fetches int
}

func (q *queueSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if q.fetches == len(q.batches) {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	batch := q.batches[q.fetches]
	q.fetches++
	n := min(len(dst), len(batch))
	for i := range n {
		id := uint64(10*q.fetches + i)
		dst[i] = mq.DriverDelivery{
			Message: &batch[i],
			Info:    mq.DeliveryInfo{Topic: "t", Sequence: id, HasSequence: true},
			Token:   id,
		}
	}
	return n, nil
}

func (q *queueSource) Settler() mq.Settler { return q.s }

func (q *queueSource) Close() error { return nil }

// TestEnvelopeFetchWaitsPastABatchItRefusedWhole holds that a fetch whose
// every delivery was refused fetches again rather than return nothing: a
// Source's Fetch blocks until a delivery is available, its context is done or
// it is closed, and an empty answer with no error is none of the three.
func TestEnvelopeFetchWaitsPastABatchItRefusedWhole(t *testing.T) {
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	good, err := env.Wrap(&mq.Message{Body: []byte("good")})
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	settler := &extendingDriverSettler{}
	src := env.WrapSource(&queueSource{
		batches: [][]mq.Message{{{Body: []byte("not an envelope")}}, {*good}},
		s:       settler,
	})
	dst := make([]mq.DriverDelivery, 1)
	n, err := src.Fetch(t.Context(), dst)
	if err != nil || n != 1 {
		t.Fatalf("Fetch = %d, %v; want the enveloped delivery behind the refused one", n, err)
	}
	if dst[0].Token != 20 || string(dst[0].Message.Body) != "good" {
		t.Fatalf("Fetch handed over token %d with body %q, want 20 and the decoded body", dst[0].Token, dst[0].Message.Body)
	}
	settler.mu.Lock()
	defer settler.mu.Unlock()
	if len(settler.nacked) != 1 || settler.nacked[0] != 10 {
		t.Fatalf("the settler nacked %v, want only the refused delivery, 10", settler.nacked)
	}
}

// TestEnvelopeFetchStopsWaitingWhenItsContextEnds holds that a fetch whose
// batch was refused whole answers to the caller's context before it fetches
// again. The source here, like the toy driver's, hands over a record it has
// ready without looking at the context, so a fetch that went on would take
// the next undecodable record from the subscription after its caller had
// gone.
func TestEnvelopeFetchStopsWaitingWhenItsContextEnds(t *testing.T) {
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	settler := &extendingDriverSettler{}
	q := &queueSource{
		batches: [][]mq.Message{{{Body: []byte("not an envelope")}}, {{Body: []byte("nor this")}}},
		s:       settler,
	}
	src := env.WrapSource(q)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1))
	if n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Fetch = %d, %v; want 0 and the context's error", n, err)
	}
	settler.mu.Lock()
	defer settler.mu.Unlock()
	if q.fetches != 1 || len(settler.nacked) != 1 {
		t.Fatalf("the cancelled fetch took %d batch(es) and nacked %v, want the first batch and its one delivery", q.fetches, settler.nacked)
	}
}

// refusingSettler is a source with no dead-letter route: it refuses every
// nack and, as the Settler contract requires of an action it cannot honour,
// settles nothing.
type refusingSettler struct{}

func (refusingSettler) Ack(context.Context, *mq.DriverDelivery) error { return nil }

func (refusingSettler) Nack(context.Context, *mq.DriverDelivery, mq.NackOptions) error {
	return mq.ErrUnsupported
}

// refusal is one call to WrapSourceReporting's onRefused.
type refusal struct {
	info         mq.DeliveryInfo
	cause        error
	deadLettered bool
}

// TestEnvelopeWrapSourceReportingReportsEachRefusal holds that onRefused
// hears about every delivery the wrapper refuses, with the delivery's own
// metadata, Unwrap's error and whether the source's settler took the
// dead-letter nack, refused it, or does not exist; and that a nil onRefused
// refuses the same way and reports nothing.
func TestEnvelopeWrapSourceReportingReportsEachRefusal(t *testing.T) {
	for _, tc := range []struct {
		name         string
		settler      mq.Settler
		report       bool
		deadLettered bool
	}{
		{"dead-lettered", &extendingDriverSettler{}, true, true},
		{"no dead-letter route", refusingSettler{}, true, false},
		{"no settler", nil, true, false},
		{"nil onRefused, no dead-letter route", refusingSettler{}, false, false},
		{"nil onRefused, no settler", nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
			if err != nil {
				t.Fatalf("NewEnvelope: %v", err)
			}
			var got []refusal
			var onRefused func(mq.DeliveryInfo, error, bool)
			if tc.report {
				onRefused = func(info mq.DeliveryInfo, cause error, deadLettered bool) {
					got = append(got, refusal{info: info, cause: cause, deadLettered: deadLettered})
				}
			}
			good, err := env.Wrap(&mq.Message{Body: []byte("good")})
			if err != nil {
				t.Fatalf("Wrap: %v", err)
			}
			src := env.WrapSourceReporting(&queueSource{
				batches: [][]mq.Message{{*good, {Body: []byte("not an envelope")}, *good}},
				s:       tc.settler,
			}, onRefused)
			n, err := src.Fetch(t.Context(), make([]mq.DriverDelivery, 3))
			if err != nil || n != 2 {
				t.Fatalf("Fetch = %d, %v; want the two enveloped deliveries and no error", n, err)
			}
			if !tc.report {
				return
			}
			if len(got) != 1 {
				t.Fatalf("onRefused was called %d time(s) for one refused delivery among two that decoded", len(got))
			}
			// The refused delivery is the second of the first fetch.
			if got[0].info.Topic != "t" || !got[0].info.HasSequence || got[0].info.Sequence != 11 {
				t.Errorf("onRefused was given %+v, want the refused delivery's own metadata (sequence 11)", got[0].info)
			}
			if !errors.Is(got[0].cause, wire.ErrMagic) {
				t.Errorf("onRefused was given %v, want Unwrap's error, wire.ErrMagic", got[0].cause)
			}
			if got[0].deadLettered != tc.deadLettered {
				t.Errorf("onRefused was told deadLettered=%v, want %v", got[0].deadLettered, tc.deadLettered)
			}
		})
	}
}

// TestEnvelopeWrapSourceReportingIsQuietForADecodableRecord holds that the
// report covers refusals and nothing else: an enveloped record, and under
// MetadataAdaptive a native body, decode and are handed over unreported.
func TestEnvelopeWrapSourceReportingIsQuietForADecodableRecord(t *testing.T) {
	for _, mode := range []mq.MetadataMode{mq.MetadataPortableEnvelope, mq.MetadataAdaptive} {
		t.Run(mode.String(), func(t *testing.T) {
			calls := 0
			env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode})
			if err != nil {
				t.Fatalf("NewEnvelope: %v", err)
			}
			writer, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
			if err != nil {
				t.Fatalf("NewEnvelope: %v", err)
			}
			wrapped, err := writer.Wrap(headerMessage("a"))
			if err != nil {
				t.Fatalf("Wrap: %v", err)
			}
			batch := []mq.Message{*wrapped}
			if mode == mq.MetadataAdaptive {
				batch = append(batch, mq.Message{Body: []byte("a native body")})
			}
			src := env.WrapSourceReporting(&queueSource{batches: [][]mq.Message{batch}, s: &extendingDriverSettler{}},
				func(mq.DeliveryInfo, error, bool) { calls++ })
			n, err := src.Fetch(t.Context(), make([]mq.DriverDelivery, len(batch)))
			if err != nil || n != len(batch) {
				t.Fatalf("Fetch = %d, %v; want all %d deliveries", n, err, len(batch))
			}
			if calls != 0 {
				t.Fatalf("onRefused was called %d time(s) for records that decoded", calls)
			}
		})
	}
}

// slotSource is queueSource with the message storage a driver may reuse:
// every Fetch writes its batch into the same per-slot messages, which the SPI
// allows once the fetch has returned.
type slotSource struct {
	batches [][]mq.Message
	slots   []mq.Message
	s       mq.Settler
	fetches int
}

func (q *slotSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if q.fetches == len(q.batches) {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	batch := q.batches[q.fetches]
	q.fetches++
	n := min(len(dst), len(batch), len(q.slots))
	for i := range n {
		q.slots[i] = batch[i]
		dst[i] = mq.DriverDelivery{Message: &q.slots[i], Token: uint64(10*q.fetches + i)}
	}
	return n, nil
}

func (q *slotSource) Settler() mq.Settler { return q.s }

func (q *slotSource) Close() error { return nil }

// deadLetter is one delivery a driver was asked to dead-letter, as it read
// the delivery when the nack reached it.
type deadLetter struct {
	token uint64
	body  string
}

// heldAckSettler is a driver settler with no batch method whose Ack waits
// for gate, so a flush carrying an Ack and then a Nack sends the Nack once
// gate opens. Its Nack records the delivery it was handed, read as a driver
// reads it to build its request.
type heldAckSettler struct {
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
	mu      sync.Mutex
	nacked  []deadLetter
}

func (s *heldAckSettler) Ack(context.Context, *mq.DriverDelivery) error {
	s.once.Do(func() { close(s.entered) })
	<-s.gate
	return nil
}

func (s *heldAckSettler) Nack(_ context.Context, d *mq.DriverDelivery, _ mq.NackOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nacked = append(s.nacked, deadLetter{token: d.Token, body: string(d.Message.Body)})
	return nil
}

// TestEnvelopeFetchDeadLettersTheRefusedDeliveryAndNotItsSlot holds that the
// dead-letter nack of a refused delivery carries that delivery whenever the
// settler sends it.
//
// The wrapper handed the settler a pointer into the caller's fetch buffer. A
// driverkit.BatchSettler keeps a settlement its flush has taken after the
// caller gives up on it, and sends it later from that pointer. So when the
// Fetch context ended while the flush was busy with an earlier round trip,
// the caller's next Fetch put a good delivery in the refused one's slot, and
// the driver was asked to dead-letter that delivery, which the caller was
// already handling, while the refused record was never nacked. The source
// here also reuses its message storage, as the SPI allows, so the nack has to
// carry its own copy of the message too.
func TestEnvelopeFetchDeadLettersTheRefusedDeliveryAndNotItsSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		good, err := env.Wrap(&mq.Message{Body: []byte("good")})
		if err != nil {
			t.Fatalf("Wrap: %v", err)
		}
		driver := &heldAckSettler{gate: make(chan struct{}), entered: make(chan struct{})}
		var mu sync.Mutex
		var reported []uint64
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
			Batching: driverkit.SettleCount,
			MaxItems: 2,
			OnResult: func(d *mq.DriverDelivery, _ error) {
				mu.Lock()
				reported = append(reported, d.Token)
				mu.Unlock()
			},
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		defer func() { _ = bs.Close() }()
		st := bs.Settler()

		// Another delivery's Ack is queued first, so the flush that takes
		// the refusal's nack sends it after this Ack's round trip.
		ackDone := make(chan error, 1)
		go func() { ackDone <- st.Ack(t.Context(), &mq.DriverDelivery{Token: 99}) }()
		synctest.Wait()

		src := env.WrapSource(&slotSource{
			batches: [][]mq.Message{{{Body: []byte("not an envelope")}}, {*good}},
			slots:   make([]mq.Message, 1),
			s:       st,
		})
		dst := make([]mq.DriverDelivery, 1)
		ctx, cancel := context.WithCancel(t.Context())
		type answer struct {
			n   int
			err error
		}
		first := make(chan answer, 1)
		go func() {
			n, ferr := src.Fetch(ctx, dst)
			first <- answer{n, ferr}
		}()
		<-driver.entered // the flush holds the Ack and the refusal's nack
		cancel()
		if a := <-first; a.n != 0 || !errors.Is(a.err, context.Canceled) {
			t.Fatalf("first Fetch = %d, %v; want 0 and the context's error", a.n, a.err)
		}

		// The caller fetches again into the same buffer.
		n, err := src.Fetch(t.Context(), dst)
		if err != nil || n != 1 || dst[0].Token != 20 || string(dst[0].Message.Body) != "good" {
			t.Fatalf("second Fetch = %d, %v (token %d); want the good delivery, token 20", n, err, dst[0].Token)
		}

		close(driver.gate) // the Ack's round trip ends and the flush sends the nack
		if err := <-ackDone; err != nil {
			t.Fatalf("Ack: %v", err)
		}
		synctest.Wait()

		driver.mu.Lock()
		nacked := slices.Clone(driver.nacked)
		driver.mu.Unlock()
		want := []deadLetter{{token: 10, body: "not an envelope"}}
		if !slices.Equal(nacked, want) {
			t.Errorf("the driver was asked to dead-letter %+v, want only the refused delivery %+v", nacked, want)
		}
		mu.Lock()
		defer mu.Unlock()
		if slices.Contains(reported, 20) || !slices.Contains(reported, 10) {
			t.Errorf("OnResult reported tokens %v, want 99 and the refused 10, never the good 20", reported)
		}
		if dst[0].Token != 20 || string(dst[0].Message.Body) != "good" {
			t.Errorf("the delivery the caller holds became token %d, body %q", dst[0].Token, dst[0].Message.Body)
		}
	})
}
