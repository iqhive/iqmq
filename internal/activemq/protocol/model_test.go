package protocol

import (
	"bytes"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// TestModel runs both arms through the properties a protocol
// mapping has to have, on both brokers.
//
// The two arms are not two implementations of one answer — they are two
// protocols against two products, and the whole point of the family is that
// the four combinations' feature coverage DIFFERS — so the model holds each of
// them to the rules put on any mapping, which is the half
// that must not differ:
//
//   - a message that is carried and restored comes back: the key, the headers,
//     the content type, the correlation identifier, the TTL, the priority and
//     the group.
//   - metadata neither protocol can represent is REFUSED under the modes that
//     do not envelope and DROPPED with a count under the one that does, which
//     is the contract.
//   - a refusal happens before any I/O: an oversized header, a reserved
//     header, a header that collides with the protocol's own.
//   - these drivers' own reserved entries and the broker's own headers never
//     reach the application.
//   - the attempt of a first delivery is one, whichever protocol said so and
//     however little it said.
//   - a publication with no delay writes NO delay, which matters because a
//     scheduled delay of zero is accepted by both brokers and turns an
//     ordinary publication into a scheduled one.
//   - the capability table is honest: every conditional support states its
//     condition, which is what `driverkit.CapabilityBuilder` refuses a table
//     without.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("roundtrip", func(t *testing.T) { modelRoundTrip(t, k) })
			t.Run("delay-slot-per-broker", func(t *testing.T) { modelDelay(t, k) })
			t.Run("zero-delay-writes-nothing", func(t *testing.T) { modelZeroDelay(t, k) })
			t.Run("first-delivery-is-attempt-one", func(t *testing.T) { modelAttempt(t, k) })
			t.Run("reserved-header-refused", func(t *testing.T) { modelReserved(t, k) })
			t.Run("oversized-header-refused", func(t *testing.T) { modelOversized(t, k) })
			t.Run("duplicate-header-refused", func(t *testing.T) { modelDuplicate(t, k) })
			t.Run("lenient-drops-and-counts", func(t *testing.T) { modelLenient(t, k) })
			t.Run("text-only", func(t *testing.T) { modelTextOnly(t, k) })
			t.Run("property-clash-refused", func(t *testing.T) { modelPropertyClash(t, k) })
			t.Run("capability-table-is-honest", func(t *testing.T) { modelTable(t, k) })
			t.Run("destination", func(t *testing.T) { modelDestination(t, k) })
			t.Run("min-broker", func(t *testing.T) { modelMinBroker(t, k) })
			t.Run("delivery-headers-are-hidden", func(t *testing.T) { modelDeliveryHeaders(t, k) })
			t.Run("dedup-is-artemis-only", func(t *testing.T) { modelDedup(t, k) })
		})
	}
}

// newProtocol builds one arm or fails the test.
func newProtocol(t *testing.T, k Kind) Protocol {
	t.Helper()
	p, err := New(k)
	if err != nil {
		t.Fatalf("New(%v) = %v", k, err)
	}
	return p
}

// names is the topology both arms derive a destination from, on one broker and
// with the flags that arm needs.
func names(t *testing.T, k Kind, b activemq.Broker) activemq.Names {
	t.Helper()
	cfg := activemq.Config{Broker: b, Topic: "orders", Subscription: "workers"}
	switch k.Resolve() {
	case AMQP10:
		cfg.Scheme = true
	case STOMP:
		cfg.STOMP = true
	case Auto, kindCount:
		t.Fatalf("Resolve answered %v", k.Resolve())
	}
	n, err := activemq.Derive(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// modelExpiry is the absolute expiry a driver would have resolved for the
// model's 90-second TTL. It is a constant rather than a clock reading, which is
// the point: neither the mapping nor its test reads the clock.
var modelExpiry = time.Unix(1_700_000_090, 0).UTC()

// sample is the message every round trip starts from: every property, a
// key, and two text headers.
//
// The headers are text because the round trip runs on both arms and a STOMP
// frame header cannot carry a binary value at all. The binary case is its own
// subtest (modelTextOnly), where the two arms are supposed to disagree.
func sample() *mq.Message {
	m := &mq.Message{
		Key:  []byte("order-42"),
		Body: []byte("body"),
		ID:   "id-1",
		Properties: mq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			CorrelationID:   "corr-1",
			CausationID:     "cause-1",
			ReplyTo:         "replies",
			CreatedAt:       time.Unix(1_700_000_000, 0).UTC(),
		},
	}
	m.Headers.Add("trace", []byte("abc"))
	m.Headers.Add("tenant", []byte("t-9"))
	return m
}

// modelOptions is the widest publication either arm maps: a key that is also
// the group, a TTL with the expiry a driver resolved for it, a priority, and a
// receipt.
func modelOptions(b activemq.Broker) Options {
	o := Options{
		Broker:     b,
		Persistent: true,
		KeyHeader:  KeyHeader,
		KeyAsGroup: true,
		Receipt:    "r-1",
		Expiry:     modelExpiry,
	}
	o.Publish.TTL = 90 * time.Second
	o.Publish.Priority, o.Publish.PrioritySet = 7, true
	return o
}

// modelRoundTrip is the fidelity requirement at the mapping level: what is
// carried is what comes back.
func modelRoundTrip(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	for _, b := range activemq.Brokers() {
		t.Run(b.String(), func(t *testing.T) {
			src := sample()
			o := modelOptions(b)
			var f Fields
			if err := p.Carry(&f, src, o); err != nil {
				t.Fatalf("Carry: %v", err)
			}

			// The named slots landed where they belong.
			if f.ContentType != src.Properties.ContentType {
				t.Errorf("content type is %q, want %q", f.ContentType, src.Properties.ContentType)
			}
			if f.CorrelationID != src.Properties.CorrelationID || f.ReplyTo != src.Properties.ReplyTo {
				t.Errorf("correlation/reply-to are (%q, %q), want (%q, %q)",
					f.CorrelationID, f.ReplyTo, src.Properties.CorrelationID, src.Properties.ReplyTo)
			}
			if !f.Timestamp.Equal(src.Properties.CreatedAt) {
				t.Errorf("timestamp is %v, want %v", f.Timestamp, src.Properties.CreatedAt)
			}
			if !f.Durable {
				t.Error("a persistent publication was not marked durable")
			}
			if f.Priority != 7 || !f.HasPriority {
				t.Errorf("priority is (%d, %v), want (7, true): the specification names priority as one of the four features its coverage score is read on", f.Priority, f.HasPriority)
			}
			// The GROUP is what makes a key ordered on ActiveMQ, and it
			// borrows the key's own storage rather than converting it.
			if !bytes.Equal(f.GroupID, src.Key) {
				t.Errorf("the group is %q, want the key %q: the group is how ActiveMQ orders a key", f.GroupID, src.Key)
			}
			// The key itself travels as a reserved entry, because NEITHER
			// protocol has a slot for one.
			if got := entryValue(&f, KeyHeader); !bytes.Equal(got, src.Key) {
				t.Errorf("the key entry %q is %q, want %q", KeyHeader, got, src.Key)
			}

			// The TTL, in the one spelling this protocol has. A driver
			// reading the field its protocol has must find it filled, and
			// the other arm must not have filled a field it would throw
			// away.
			switch k.Resolve() {
			case AMQP10:
				if f.TTL != 90*time.Second {
					t.Errorf("the AMQP 1.0 ttl is %v, want %v: the header section's ttl is a relative duration", f.TTL, 90*time.Second)
				}
				if f.ContentEncoding != src.Properties.ContentEncoding {
					t.Errorf("the AMQP 1.0 content-encoding property is %q, want %q", f.ContentEncoding, src.Properties.ContentEncoding)
				}
				if f.Receipt != "" {
					t.Errorf("the AMQP 1.0 arm filled a STOMP receipt (%q); a transfer is resolved by its own disposition", f.Receipt)
				}
			case STOMP:
				if f.TTL != 0 {
					t.Errorf("the STOMP arm filled a relative ttl (%v), which the protocol has no header for", f.TTL)
				}
				if !f.Expiry.Equal(modelExpiry) {
					t.Errorf("the STOMP expiry is %v, want the expiry the driver resolved, %v", f.Expiry, modelExpiry)
				}
				if f.ContentEncoding != "" {
					t.Errorf("the STOMP arm filled a content-encoding slot (%q); STOMP has no such frame header", f.ContentEncoding)
				}
				if got := string(entryValue(&f, EncodingHeader)); got != src.Properties.ContentEncoding {
					t.Errorf("the STOMP content encoding travelled as %q under %q, want %q", got, EncodingHeader, src.Properties.ContentEncoding)
				}
				if f.Receipt != "r-1" {
					t.Errorf("the STOMP receipt is %q, want %q", f.Receipt, "r-1")
				}
				// STOMP's `message-id` is the BROKER's on a delivery, so sending
				// the publisher's under that name would have it overwritten in
				// transit and lost.
				if f.MessageID != "" {
					t.Errorf("the STOMP arm filled `message-id` with %q; on a delivery that header is the broker's, so the publisher's identifier would be overwritten", f.MessageID)
				}
				if got := string(entryValue(&f, IDHeader)); got != src.ID {
					t.Errorf("the publisher's identifier travelled as %q under %q, want %q", got, IDHeader, src.ID)
				}
			case Auto, kindCount:
				// Unreachable: Resolve never answers Auto and the sentinel
				// is not a kind. Naming both is what makes the switch total,
				// so a new kind does not land in a silent default.
				t.Fatalf("Resolve answered %v", k.Resolve())
			}

			// And the message comes back.
			var back mq.Message
			if err := p.Restore(&back, &f, o); err != nil {
				t.Fatalf("Restore: %v", err)
			}
			if back.Properties != src.Properties {
				t.Errorf("the restored properties are %+v, want %+v", back.Properties, src.Properties)
			}
			// The publisher's own identifier comes back on BOTH arms, which is
			// what makes the specification's "protocol choice explicit" honest: a caller who
			// switches protocol finds the same message, even though one arm has
			// a `message-id` property and the other has a `message-id` that
			// belongs to the broker.
			if back.ID != src.ID {
				t.Errorf("the restored identifier is %q, want %q", back.ID, src.ID)
			}
			if !bytes.Equal(back.Key, src.Key) {
				t.Errorf("the restored key is %q, want %q", back.Key, src.Key)
			}
			if back.Headers.Len() != src.Headers.Len() {
				t.Fatalf("the restored message has %d headers, want %d: %v", back.Headers.Len(), src.Headers.Len(), headerList(&back))
			}
			for key, want := range src.Headers.All() {
				got, ok := back.Headers.Get(key)
				if !ok || !bytes.Equal(got, want) {
					t.Errorf("header %q restored as %q (present=%v), want %q", key, got, ok, want)
				}
			}
			for key := range back.Headers.All() {
				if ReservedHeader(key) {
					t.Errorf("the restored message carries one of these drivers' own headers %q: a round trip must not add headers the publisher never set", key)
				}
			}
		})
	}
}

// modelDelay is the one thing all four combinations do differently, and the
// reason [DelaySlot] is a value rather than a constant in each driver: the
// wrong slot is not an error, it is a publication delivered at once.
func modelDelay(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	for _, b := range activemq.Brokers() {
		t.Run(b.String(), func(t *testing.T) {
			o := modelOptions(b)
			o.Publish.Delay = 2500 * time.Millisecond
			o.Delay = 2500 * time.Millisecond
			var f Fields
			if err := p.Carry(&f, sample(), o); err != nil {
				t.Fatalf("Carry: %v", err)
			}
			want := DelayHeader
			wantKey := PropertyScheduledDelay
			if k.Resolve() == AMQP10 {
				if b == activemq.Artemis {
					want, wantKey = DelayAnnotation, AnnotationDeliveryDelay
				} else {
					want, wantKey = DelayProperty, PropertyScheduledDelay
				}
			}
			if f.Slot != want {
				t.Errorf("the delay slot is %v, want %v: measured, each broker IGNORES the other's slot and delivers the message immediately", f.Slot, want)
			}
			if got, _ := f.Slot.Keys(); got != wantKey {
				t.Errorf("the delay key is %q, want %q", got, wantKey)
			}
			if f.Delay != 2500*time.Millisecond {
				t.Errorf("the delay is %v, want 2.5s", f.Delay)
			}
			// Whole milliseconds, rounded UP, because that is the unit both
			// brokers' keys are in and because a publication must never be
			// released earlier than the caller asked.
			o.Publish.Delay = 1500 * time.Microsecond
			o.Delay = 1500 * time.Microsecond
			if err := p.Carry(&f, sample(), o); err != nil {
				t.Fatalf("Carry: %v", err)
			}
			if f.Delay != 2*time.Millisecond {
				t.Errorf("a delay of 1.5ms became %v, want 2ms: the brokers' keys are in whole milliseconds and rounding down would release the publication early", f.Delay)
			}
			// An absolute delivery time uses the absolute key where the slot
			// has one, and needs no clock at all. Where the slot has none --
			// STOMP, on both brokers -- the driver must have resolved the
			// instant into a duration, and a publication that brought
			// neither is REFUSED rather than sent with no delay at all: a
			// publication that was supposed to be held back for an hour and
			// went out at once is not a smaller failure than an error.
			at := time.Unix(1_700_000_500, 0).UTC()
			o.Publish.Delay, o.Delay = 0, 0
			o.Publish.DeliverAt = at
			err := p.Carry(&f, sample(), o)
			switch k.Resolve() {
			case AMQP10:
				if err != nil {
					t.Fatalf("Carry refused an absolute delivery time: %v", err)
				}
				if !f.DeliverAt.Equal(at) {
					t.Errorf("the absolute delivery time is %v, want %v", f.DeliverAt, at)
				}
				if _, absKey := f.Slot.Keys(); absKey == "" {
					t.Errorf("the %v slot names no absolute key, so an absolute delivery time has nowhere to go", f.Slot)
				}
				if f.Delay != 0 {
					t.Errorf("an absolute delivery time also produced the relative delay %v; writing both would let the two disagree", f.Delay)
				}
			case STOMP:
				if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
					t.Fatalf("the STOMP arm accepted an absolute delivery time with code %v, want %v: STOMP has no absolute scheduled-delivery header, so the driver has to resolve the instant against the injected clock", code, mq.CodeInvalidArgument)
				}
				// And with the duration resolved it goes in the relative key.
				o.Delay = 500 * time.Millisecond
				if err := p.Carry(&f, sample(), o); err != nil {
					t.Fatalf("Carry refused a resolved delay: %v", err)
				}
				if f.Delay != 500*time.Millisecond || f.Slot != DelayHeader {
					t.Errorf("a resolved delay of 500ms produced (%v, %v), want (500ms, %v)", f.Delay, f.Slot, DelayHeader)
				}
			case Auto, kindCount:
				t.Fatalf("Resolve answered %v", k.Resolve())
			}
		})
	}
}

// modelZeroDelay is the case that looks too obvious to test and is not.
//
// An `AMQ_SCHEDULED_DELAY` of 0 is ACCEPTED by both brokers, which turns an
// ordinary publication into a scheduled one — it goes through the scheduler,
// and on Artemis it is echoed back with a `_AMQ_SCHED_DELIVERY` time. So a
// mapping that wrote the key unconditionally would put every publication in the
// repository through the broker's scheduler, and nothing would report it.
func modelZeroDelay(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	for _, b := range activemq.Brokers() {
		var f Fields
		if err := p.Carry(&f, sample(), modelOptions(b)); err != nil {
			t.Fatalf("%v: Carry: %v", b, err)
		}
		if f.Slot != NoDelay {
			t.Errorf("%v: a publication with no delay has the slot %v, want %v: a scheduled delay of zero is accepted by both brokers and is not the same message", b, f.Slot, NoDelay)
		}
		if f.Delay != 0 || !f.DeliverAt.IsZero() {
			t.Errorf("%v: a publication with no delay carries delay=%v at=%v", b, f.Delay, f.DeliverAt)
		}
		if d, at := f.Slot.Keys(); d != "" || at != "" {
			t.Errorf("%v: the NoDelay slot names the keys (%q, %q); there is nothing to write", b, d, at)
		}
	}
}

// modelAttempt is the attempt on a first delivery, which is ONE, and the family's second-sharpest measured asymmetry.
//
// AMQP 1.0 carries a real `delivery-count`; STOMP carries a boolean and
// nothing else, measured on both brokers. So both arms answer 1 for a first
// delivery and only one of them can answer 3 for a third.
func modelAttempt(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	var first Fields
	if err := p.Carry(&first, sample(), modelOptions(activemq.Artemis)); err != nil {
		t.Fatal(err)
	}
	if got, exact := Attempt(k, &first); got != 1 || !exact {
		t.Errorf("a first delivery is attempt (%d, exact=%v), want (1, true): the specification counts from one", got, exact)
	}
	switch k.Resolve() {
	case AMQP10:
		// `delivery-count` is the number of PREVIOUS attempts, so the third
		// delivery carries 2 and is attempt 3. Measured as 0, 1, 2 across
		// three rounds on both brokers.
		third := Fields{DeliveryCount: 2}
		if got, exact := Attempt(k, &third); got != 3 || !exact {
			t.Errorf("delivery-count 2 is attempt (%d, exact=%v), want (3, true)", got, exact)
		}
	case STOMP:
		// `redelivered: true` means at least a second attempt and the
		// protocol cannot say which, so exact is false rather than 2 being
		// reported as a fact.
		again := Fields{Redelivered: true}
		got, exact := Attempt(k, &again)
		if got != 2 {
			t.Errorf("a redelivered STOMP message is attempt %d, want 2", got)
		}
		if exact {
			t.Error("a redelivered STOMP message reports an EXACT attempt: measured on both brokers, a STOMP delivery carries no delivery-count, no JMSXDeliveryCount and no _AMQ_DELIVERY_COUNT, so the count is unknown beyond \"not the first\"")
		}
		// A `delivery-count` on a STOMP delivery is nobody's: reading one
		// would invent a count the broker never sent.
		if got, _ := Attempt(k, &Fields{DeliveryCount: 7}); got != 1 {
			t.Errorf("the STOMP arm read a delivery-count of 7 as attempt %d; no STOMP delivery carries one", got)
		}
	case Auto, kindCount:
		t.Fatalf("Resolve answered %v", k.Resolve())
	}
}

// modelReserved is the header a caller must not be able to set, because the
// driver means something by it.
func modelReserved(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Add(KeyHeader, []byte("mine"))
	var f Fields
	o := Options{Broker: activemq.Artemis}
	err := p.Carry(&f, m, o)
	if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
		t.Fatalf("a header under %q was refused with code %v, want %v: shadowing it would give the message a key the application never chose",
			ReservedHeaderPrefix, code, mq.CodeInvalidArgument)
	}
	// A header that merely starts with "x-" is not reserved: both brokers'
	// own extensions use that prefix and a caller may well set one.
	m.Headers.Delete(KeyHeader)
	m.Headers.Add("x-something", []byte("theirs"))
	if err := p.Carry(&f, m, o); err != nil {
		t.Fatalf("a header under \"x-\" was refused: only %q is reserved: %v", ReservedHeaderPrefix, err)
	}
}

// modelOversized is the advertised limit enforced BEFORE any I/O, which is
// the half that matters on ActiveMQ: an over-long name detaches an Artemis link
// with amqp:not-found and takes its session with it, and on Classic it is
// accepted and becomes a destination nothing consumes.
func modelOversized(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	o := Options{Broker: activemq.Artemis}
	o.Limits.MaxHeaderBytes = mq.Limit[int64]{Value: 16, Known: true}
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Add("k", bytes.Repeat([]byte("v"), 32))
	var f Fields
	if code := mq.CodeOf(p.Carry(&f, m, o)); code != mq.CodeMessageTooLarge {
		t.Fatalf("an oversized header value was refused with code %v, want %v", code, mq.CodeMessageTooLarge)
	}
	// A key longer than a name can be is refused whatever the caller
	// advertises, because it is the BROKER's ceiling and not the driver's.
	m.Headers.Delete("k")
	m.Headers.Add(strings.Repeat("k", activemq.MaxNameLen+1), []byte("v"))
	if code := mq.CodeOf(p.Carry(&f, m, Options{Broker: activemq.Artemis})); code != mq.CodeMessageTooLarge {
		t.Fatalf("a %d-byte header key was refused with code %v, want %v", activemq.MaxNameLen+1, code, mq.CodeMessageTooLarge)
	}
	// An advertised count limit truncates under the lenient mode and refuses
	// under the others.
	m.Headers.Reset()
	for _, key := range []string{"a", "b", "c"} {
		m.Headers.Add(key, []byte("1"))
	}
	o = Options{Broker: activemq.Artemis}
	o.Limits.MaxHeaderCount = mq.Limit[int]{Value: 2, Known: true}
	if code := mq.CodeOf(p.Carry(&f, m, o)); code != mq.CodeMessageTooLarge {
		t.Fatalf("three headers under a limit of two were refused with code %v, want %v", code, mq.CodeMessageTooLarge)
	}
	o.Metadata = mq.MetadataLenientDrop
	if err := p.Carry(&f, m, o); err != nil {
		t.Fatalf("the lenient mode refused an over-count publication: %v", err)
	}
	if len(f.Props) != 2 || f.Dropped != 1 {
		t.Errorf("the lenient mode carried %d entries and dropped %d, want 2 and 1", len(f.Props), f.Dropped)
	}
	// But a limit that does not leave room for the MAPPING'S OWN entries is
	// a refusal in EVERY mode, the lenient one included. Those entries are
	// not metadata the caller asked for -- they are how the typed
	// properties reach the wire at all on a protocol with no slot for them --
	// so truncating them would return a message with no key, no identifier
	// and no content encoding, and the specification's "drop and count" is not licence to
	// lose the message's identity. A broker configured this tightly cannot
	// carry this driver's messages, which is a configuration error. The fuzz
	// target found this by truncating the STOMP arm's content-encoding entry
	// away and getting the property back empty.
	rich := &mq.Message{Key: []byte("k"), Body: []byte("b"), ID: "id-1"}
	rich.Properties.ContentEncoding = "gzip"
	rich.Properties.CausationID = "cause-1"
	rich.Headers.Add("keep", []byte("1"))
	tight := Options{Broker: activemq.Artemis, Metadata: mq.MetadataLenientDrop, KeyHeader: KeyHeader}
	tight.Limits.MaxHeaderCount = mq.Limit[int]{Value: 1, Known: true}
	if code := mq.CodeOf(p.Carry(&f, rich, tight)); code != mq.CodeMessageTooLarge {
		t.Errorf("a header limit of 1 that cannot hold the mapping's own entries was accepted with code %v, want %v: truncating them returns a message with no key and no identifier, and the lenient mode's contract is to drop the CALLER's metadata rather than the message's identity",
			code, mq.CodeMessageTooLarge)
	}
	// With room for them it drops only the caller's header, and every typed
	// property still round-trips.
	tight.Limits.MaxHeaderCount = mq.Limit[int]{Value: 4, Known: true}
	if err := p.Carry(&f, rich, tight); err != nil {
		t.Fatalf("a header limit with room for the mapping's own entries refused the publication: %v", err)
	}
	var back mq.Message
	if err := p.Restore(&back, &f, tight); err != nil {
		t.Fatal(err)
	}
	if back.ID != rich.ID || back.Properties.ContentEncoding != "gzip" || back.Properties.CausationID != "cause-1" {
		t.Errorf("a truncated publication lost a typed property: ID=%q encoding=%q causation=%q", back.ID, back.Properties.ContentEncoding, back.Properties.CausationID)
	}
}

// modelDuplicate is the promise meeting two protocols that cannot keep it,
// for two different reasons — a map on one arm, a frame that carries the
// duplicate and then throws it away on the other.
//
// The refusal has to be Unsupported and not InvalidArgument, because the
// caller's message is fine and this broker is what cannot carry it.
func modelDuplicate(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Add("dup", []byte("one"))
	m.Headers.Add("dup", []byte("two"))
	var f Fields
	for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataPortableEnvelope, mq.MetadataAdaptive} {
		err := p.Carry(&f, m, Options{Broker: activemq.Artemis, Metadata: mode})
		if err == nil {
			t.Fatalf("mode %v accepted a repeated header key", mode)
		}
		if code := mq.CodeOf(err); code != mq.CodeUnsupported {
			t.Fatalf("mode %v refused a repeated header with code %v, want %v: the caller's message is well formed and this broker cannot carry it", mode, code, mq.CodeUnsupported)
		}
	}
	// A single header of the same name is of course fine, so the refusal is
	// about the duplication and not about the name.
	m.Headers.Delete("dup")
	m.Headers.Add("dup", []byte("one"))
	if err := p.Carry(&f, m, Options{Broker: activemq.Artemis}); err != nil {
		t.Fatalf("a single header was refused: %v", err)
	}
}

// modelLenient is MetadataLenientDrop: drop what cannot be carried, and
// COUNT it, because the specification requires a hook and a metric rather than silence.
func modelLenient(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Add("keep", []byte("yes"))
	m.Headers.Add("dup", []byte("one"))
	m.Headers.Add("dup", []byte("two"))
	m.Headers.Add(KeyHeader, []byte("mine"))
	var f Fields
	o := Options{Broker: activemq.Artemis, Metadata: mq.MetadataLenientDrop}
	if err := p.Carry(&f, m, o); err != nil {
		t.Fatalf("the lenient mode refused a publication: %v", err)
	}
	if f.Dropped != 2 {
		t.Errorf("the lenient mode dropped %d entries, want 2 (the repeated key and the reserved one)", f.Dropped)
	}
	got := 0
	for i := range f.Props {
		if f.Props[i].Key == "keep" {
			got++
		}
	}
	if got != 1 {
		t.Errorf("the representable header was carried %d times, want once", got)
	}
	// And a de-duplication key on Classic, which has no de-duplication at
	// all, is dropped rather than refused under this mode.
	o.Broker = activemq.Classic
	o.Publish.DedupKey = "d-1"
	if err := p.Carry(&f, &mq.Message{Body: []byte("b")}, o); err != nil {
		t.Fatalf("the lenient mode refused a de-duplication key on Classic: %v", err)
	}
	if f.Dropped != 1 {
		t.Errorf("a de-duplication key on Classic dropped %d, want 1", f.Dropped)
	}
}

// modelTextOnly is the row the two arms are supposed to disagree on, and the
// one place a "portable" mapping would be tempted to mangle a value.
//
// A STOMP frame header is a text name and a text value, so a header value that
// is not valid UTF-8 cannot be carried at all — nor can a binary message key,
// because the key travels as a header. The AMQP 1.0 arm's application
// properties are typed and keep the bytes.
func modelTextOnly(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	binary := []byte{0x00, 0xff, 0xfe}
	o := Options{Broker: activemq.Artemis, KeyHeader: KeyHeader}

	m := &mq.Message{Body: []byte("b")}
	m.Headers.Add("blob", binary)
	var f Fields
	err := p.Carry(&f, m, o)
	switch k.Resolve() {
	case AMQP10:
		if err != nil {
			t.Fatalf("the AMQP 1.0 arm refused a binary header value: application properties are typed: %v", err)
		}
		if got := entryValue(&f, "blob"); !bytes.Equal(got, binary) {
			t.Errorf("the binary header value is %q, want %q byte for byte", got, binary)
		}
		if lvl := p.Support(activemq.Artemis, mq.FeatureBinaryHeaders).Level; lvl != mq.Native {
			t.Errorf("the AMQP 1.0 arm advertises FeatureBinaryHeaders as %v, want native", lvl)
		}
	case STOMP:
		if code := mq.CodeOf(err); code != mq.CodeUnsupported {
			t.Fatalf("the STOMP arm refused a binary header value with code %v, want %v: mangling it would hand the consumer a value the publisher never sent", code, mq.CodeUnsupported)
		}
		if lvl := p.Support(activemq.Artemis, mq.FeatureBinaryHeaders).Level; lvl != mq.Unsupported {
			t.Errorf("the STOMP arm advertises FeatureBinaryHeaders as %v, want unsupported: a frame header is text", lvl)
		}
		// A binary KEY is the same refusal, because the key is a header.
		bin := &mq.Message{Body: []byte("b"), Key: binary}
		if code := mq.CodeOf(p.Carry(&f, bin, o)); code != mq.CodeUnsupported {
			t.Fatalf("the STOMP arm carried a binary key with code %v, want %v", code, mq.CodeUnsupported)
		}
		// And a header whose NAME is one of STOMP's own frame headers is
		// refused rather than shadowed. A Classic delivery really does carry
		// these, so accepting one would shadow it on the way out AND hand
		// the broker's back as the application's on the way in.
		for _, name := range []string{"destination", "expires", "message-id", "subscription", "ack", "content-length", "selector"} {
			clash := &mq.Message{Body: []byte("b")}
			clash.Headers.Add(name, []byte("x"))
			if code := mq.CodeOf(p.Carry(&f, clash, o)); code != mq.CodeInvalidArgument {
				t.Errorf("a header called %q was refused with code %v, want %v: it would be shadowed on the way out and handed back as the application's on the way in", name, code, mq.CodeInvalidArgument)
			}
		}
		// A relative TTL with no resolved expiry is refused rather than
		// published without one: `expires` is an instant and this mapping
		// does not read the clock.
		ttl := Options{Broker: activemq.Artemis}
		ttl.Publish.TTL = time.Minute
		if code := mq.CodeOf(p.Carry(&f, &mq.Message{Body: []byte("b")}, ttl)); code != mq.CodeInvalidArgument {
			t.Errorf("a STOMP TTL with no resolved expiry was accepted with code %v, want %v", code, mq.CodeInvalidArgument)
		}
	case Auto, kindCount:
		t.Fatalf("Resolve answered %v", k.Resolve())
	}
}

// modelPropertyClash is the refusal BOTH arms owe, and the one the first draft
// of this package got backwards in both directions.
//
// A broker's own MESSAGE PROPERTY means something whichever protocol carries
// it: an application property called `JMSXGroupID` IS the message group over
// AMQP 1.0, and one called `_AMQ_DUPL_ID` IS a de-duplication key on Artemis.
// So a caller's header of that name is refused on both arms. A STOMP FRAME
// header is a different thing — `destination` and `subscription` are not
// message properties at all — so it is refused on the STOMP arm only, and the
// AMQP 1.0 arm carries it like any other header. The fuzz target found the
// second half by round-tripping a `destination` header through the reference
// arm and getting nothing back.
func modelPropertyClash(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	o := Options{Broker: activemq.Artemis, KeyHeader: KeyHeader}
	var f Fields
	for _, name := range []string{
		PropertyGroupID, PropertyScheduledDelay, PropertyScheduledTime,
		PropertyDuplicateID, PropertyArtemisGroupID, PropertyArtemisScheduled,
		PropertyClassicScheduledJob, "_AMQ_A_FUTURE_RELEASE_ADDS_THIS",
	} {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Add(name, []byte("x"))
		if code := mq.CodeOf(p.Carry(&f, m, o)); code != mq.CodeInvalidArgument {
			t.Errorf("a header called %q was accepted with code %v, want %v: the broker reads that property on both protocols, so carrying it would give the message a group, a delay or a de-duplication key the application never asked for",
				name, code, mq.CodeInvalidArgument)
		}
		if !ReservedProperty(name) {
			t.Errorf("ReservedProperty(%q) = false", name)
		}
	}
	// And a header whose name is only a STOMP FRAME header is carried by the
	// AMQP 1.0 arm and refused by the STOMP one, which is the asymmetry.
	for _, name := range []string{"destination", "subscription", "ack", "content-length", "receipt", "selector"} {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Add(name, []byte("x"))
		err := p.Carry(&f, m, o)
		switch k.Resolve() {
		case AMQP10:
			if err != nil {
				t.Errorf("the AMQP 1.0 arm refused a header called %q: it is a STOMP frame header and nobody's message property, so this arm carries it: %v", name, err)
			}
			if got := string(entryValue(&f, name)); got != "x" {
				t.Errorf("the AMQP 1.0 arm carried %q as %q, want %q", name, got, "x")
			}
		case STOMP:
			if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
				t.Errorf("the STOMP arm accepted a header called %q with code %v, want %v", name, code, mq.CodeInvalidArgument)
			}
		case Auto, kindCount:
			t.Fatalf("Resolve answered %v", k.Resolve())
		}
	}
	// A header with NO NAME is refused on both arms. The fuzz target found
	// this one too: an empty AMQP 1.0 property key is a property nothing will
	// show you, and an empty STOMP header name is a malformed frame the
	// broker answers by closing the connection.
	empty := &mq.Message{Body: []byte("b")}
	empty.Headers.Add("", []byte("x"))
	if code := mq.CodeOf(p.Carry(&f, empty, o)); code != mq.CodeInvalidArgument {
		t.Errorf("a header with an empty name was accepted with code %v, want %v", code, mq.CodeInvalidArgument)
	}
}

// modelTable is what `driverkit.CapabilityBuilder` would refuse: a conditional
// support with no condition, or an emulation with no explanation.
//
// The specification requires both, and a table that omitted one would reach a caller as
// "this might work, under conditions nobody wrote down".
func modelTable(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	for _, b := range activemq.Brokers() {
		for f := range mq.FeatureCount {
			feat := mq.Feature(f)
			s := p.Support(b, feat)
			switch s.Level {
			case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
				if s.Note == "" {
					t.Errorf("%v/%v is %v with no note: the message contract requires the condition or the emulation to be stated", b, feat, s.Level)
				}
			case mq.Native:
				if s.Scope == mq.ScopeNone {
					t.Errorf("%v/%v is native with no scope: a feature that holds over nothing is not a feature", b, feat)
				}
			case mq.Unsupported:
				if s.Scope != mq.ScopeNone {
					t.Errorf("%v/%v is unsupported and carries the scope %v", b, feat, s.Scope)
				}
			}
		}
		if n := p.Features(b); n < 10 || n > mq.FeatureCount {
			t.Errorf("%v/%v supports %d features natively, which is not a plausible count of the %d in the inventory", k, b, n, mq.FeatureCount)
		}
	}
	// criterion is read "on Artemis and Classic", so the two
	// brokers must not answer the same table: if they did, the row's two
	// numbers would be one number and the criterion would have no broker
	// half at all.
	if p.Features(activemq.Artemis) == p.Features(activemq.Classic) {
		t.Errorf("%v scores %d on both brokers: criterion is read on Artemis AND Classic, and a table that does not distinguish them cannot answer it",
			k, p.Features(activemq.Artemis))
	}
}

// modelDestination is the defect this package exists to make impossible: a
// name set derived for the OTHER protocol.
//
// On Classic a destination's own shape is what tells the broker whether a name
// is a queue or a topic, so a swapped flag does not fail — it publishes to a
// QUEUE called `VirtualTopic.orders`, which every consumer queue is subscribed
// to and none of them receives anything from.
func modelDestination(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	for _, b := range activemq.Brokers() {
		n := names(t, k, b)
		pub, err := p.Destination(n, Publish)
		if err != nil {
			t.Fatalf("%v: Destination(Publish): %v", b, err)
		}
		if pub != n.Publish {
			t.Errorf("%v: the publish destination is %q, want %q", b, pub, n.Publish)
		}
		con, err := p.Destination(n, Consume)
		if err != nil {
			t.Fatalf("%v: Destination(Consume): %v", b, err)
		}
		if con != n.Consume {
			t.Errorf("%v: the consume destination is %q, want %q", b, con, n.Consume)
		}
		// The other protocol's Classic form is refused. On Artemis both
		// protocols address bare names, so there is nothing to tell apart
		// and nothing to refuse — which is itself worth pinning, because a
		// check that refused an Artemis name would refuse every Artemis
		// connection.
		other := AMQP10
		if k.Resolve() == AMQP10 {
			other = STOMP
		}
		wrong := names(t, other, b)
		_, err = p.Destination(wrong, Publish)
		if b == activemq.Classic {
			if code := mq.CodeOf(err); code != mq.CodeInvalidConfig {
				t.Errorf("%v: a name set derived for %v produced a destination (code %v, want %v); on Classic a swapped flag addresses a queue called VirtualTopic.<topic> and nothing reports it",
					b, other, code, mq.CodeInvalidConfig)
			}
		} else if err != nil {
			t.Errorf("%v: a name set derived for %v was refused: on Artemis both protocols address bare names: %v", b, other, err)
		}
	}
	// A name set with nothing to address is refused rather than producing a
	// destination that reaches the broker's default by accident.
	if _, err := p.Destination(activemq.Names{}, Publish); err == nil {
		t.Error("an empty name set produced a publish destination")
	}
	if _, err := p.Destination(activemq.Names{Publish: "x"}, Consume); err == nil {
		t.Error("a name set with no consume destination produced one")
	}
}

// modelMinBroker is the supported-version compatibility term, which for
// this family is two versions per protocol rather than one.
func modelMinBroker(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	for _, b := range activemq.Brokers() {
		min := p.MinBroker(b)
		if min.IsZero() {
			t.Fatalf("%v: the protocol claims no minimum version, so nothing would ever be refused", b)
		}
		if err := activemq.CheckAtLeast(min, min, "the floor itself"); err != nil {
			t.Errorf("%v: the floor does not satisfy itself: %v", b, err)
		}
	}
	// Artemis's floor is 2.7 for BOTH protocols, and the reason is the
	// driver's own addressing rather than the protocol: a shared durable
	// subscription is addressed by fully qualified queue name, which arrived
	// in 2.7.0.
	if got := p.MinBroker(activemq.Artemis); got != activemq.MinArtemisFQQN {
		t.Errorf("the Artemis floor is %v, want %v: the subscription is an FQQN on both arms", got, activemq.MinArtemisFQQN)
	}
	// A broker one release below the floor is refused, naming both versions.
	old := activemq.Version{Major: 2, Minor: 6, Patch: 9}
	if err := activemq.CheckAtLeast(old, p.MinBroker(activemq.Artemis), "the subscription's FQQN"); err == nil {
		t.Errorf("Artemis %v satisfies the floor %v", old, p.MinBroker(activemq.Artemis))
	}
	// And Classic's floor differs BETWEEN the two protocols, which is what
	// makes the row's "supported-version compatibility" four numbers.
	wantClassic := activemq.MinClassicAMQP
	if k.Resolve() == STOMP {
		wantClassic = activemq.MinClassicSTOMP
	}
	if got := p.MinBroker(activemq.Classic); got != wantClassic {
		t.Errorf("the Classic floor is %v, want %v", got, wantClassic)
	}
}

// modelDeliveryHeaders is the case a round trip through Carry cannot reach, and
// the one the two filters in Restore really exist for.
//
// What reaches a real Restore is a DELIVERY, whose headers come from the
// broker. A Classic STOMP delivery was measured carrying `destination`,
// `message-id`, `subscription`, `ack`, `timestamp`, `expires`, `priority`,
// `persistent`, `redelivered`, `content-length` and `scheduledJobId`; Artemis
// echoes `_AMQ_GROUP_ID` and `_AMQ_SCHED_DELIVERY`. Every one of them would
// otherwise reach the application as a header nobody published, which is the fidelity scenario, and an application that echoed its headers would
// publish somebody else's `destination`.
func modelDeliveryHeaders(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	o := Options{Broker: activemq.Artemis, KeyHeader: KeyHeader}
	f := Fields{Props: []Entry{
		{Key: "trace", Bin: []byte("abc"), Kind: Binary},
		{Key: "destination", Text: "/queue/Consumer.workers.VirtualTopic.orders", Kind: Text},
		{Key: "subscription", Text: "sub-0", Kind: Text},
		{Key: "scheduledJobId", Text: "ID:job-1", Kind: Text},
		{Key: PropertyArtemisGroupID, Text: "order-42", Kind: Text},
		{Key: PropertyArtemisScheduled, Int: 1_700_000_500_000, Kind: Int},
		{Key: ReservedHeaderPrefix + "future", Bin: []byte{0x01}, Kind: Binary},
		{Key: CausationHeader, Text: "cause-1", Kind: Text},
		{Key: EncodingHeader, Text: "gzip", Kind: Text},
		{Key: IDHeader, Text: "id-1", Kind: Text},
		{Key: KeyHeader, Bin: []byte("order-42"), Kind: Binary},
	}}
	var back mq.Message
	if err := p.Restore(&back, &f, o); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// A broker's own MESSAGE PROPERTY is filtered on both arms, because both
	// arms receive it and the broker means something by it. A STOMP FRAME
	// header is filtered on the STOMP arm and on NO other: `destination` and
	// `subscription` are not message properties at all, so on the AMQP 1.0
	// arm a caller's application property of that name is the caller's and
	// comes back. Refusing them there would refuse a header that arm carries
	// perfectly well, which is the mistake the fuzz target found in the first
	// draft of this package.
	wantHeaders := 1
	if k.Resolve() == AMQP10 {
		wantHeaders = 3
	}
	for key := range back.Headers.All() {
		if ReservedHeader(key) || ReservedProperty(key) {
			t.Errorf("a delivery's own header %q reached the application", key)
		}
		if k.Resolve() == STOMP && ReservedFrameHeader(key) {
			t.Errorf("a STOMP frame's own header %q reached the application", key)
		}
	}
	if back.Headers.Len() != wantHeaders {
		t.Errorf("the restored message has %d headers, want %d: %v", back.Headers.Len(), wantHeaders, headerList(&back))
	}
	if got, ok := back.Headers.Get("trace"); !ok || string(got) != "abc" {
		t.Errorf("the application's own header did not survive: %q (present=%v)", got, ok)
	}
	if k.Resolve() == AMQP10 {
		if got, ok := back.Headers.Get("destination"); !ok || string(got) != "/queue/Consumer.workers.VirtualTopic.orders" {
			t.Errorf("the AMQP 1.0 arm dropped an application property called %q (present=%v): it is a STOMP frame header and nobody's property", "destination", ok)
		}
	}
	// The reserved entries Restore DOES read are read, so the filter is not
	// simply dropping everything it recognises.
	if back.Properties.CausationID != "cause-1" {
		t.Errorf("the causation identifier restored as %q", back.Properties.CausationID)
	}
	if back.Properties.ContentEncoding != "gzip" {
		t.Errorf("the content encoding restored as %q", back.Properties.ContentEncoding)
	}
	if back.ID != "id-1" {
		t.Errorf("the publisher's identifier restored as %q", back.ID)
	}
	if string(back.Key) != "order-42" {
		t.Errorf("the key restored as %q", back.Key)
	}
	// And the group is readable under either spelling the brokers echo,
	// which is what a consumer has to do because neither is promised.
	if group, ok := EchoedGroup(&f); !ok || string(group) != "order-42" {
		t.Errorf("EchoedGroup read %q (present=%v), want \"order-42\": Artemis echoes _AMQ_GROUP_ID and Classic echoes nothing", group, ok)
	}
	if _, ok := EchoedGroup(&Fields{}); ok {
		t.Error("EchoedGroup found a group in a delivery that carried none: Classic echoes nothing at all and a consumer must be able to tell")
	}
}

// modelDedup is the broker half of the family: Artemis de-duplicates a
// publication and Classic does not, over both protocols.
//
// A de-duplication key aimed at Classic is REFUSED rather than sent and
// ignored, because a caller who asked for de-duplication and silently did not
// get it is the worst of the three outcomes.
func modelDedup(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	m := &mq.Message{Body: []byte("b")}
	var f Fields
	o := Options{Broker: activemq.Artemis}
	o.Publish.DedupKey = "d-1"
	if err := p.Carry(&f, m, o); err != nil {
		t.Fatalf("Artemis refused a de-duplication key: %v", err)
	}
	if got := string(entryValue(&f, PropertyDuplicateID)); got != "d-1" {
		t.Errorf("the de-duplication key travelled as %q under %q, want %q", got, PropertyDuplicateID, "d-1")
	}
	if lvl := p.Support(activemq.Artemis, mq.FeatureDeduplication).Level; lvl != mq.Native {
		t.Errorf("Artemis advertises FeatureDeduplication as %v, want native", lvl)
	}
	o.Broker = activemq.Classic
	if code := mq.CodeOf(p.Carry(&f, m, o)); code != mq.CodeUnsupported {
		t.Fatalf("a de-duplication key on Classic was refused with code %v, want %v: Classic reads no _AMQ_DUPL_ID and would accept the property and ignore it", code, mq.CodeUnsupported)
	}
	if lvl := p.Support(activemq.Classic, mq.FeatureDeduplication).Level; lvl != mq.Unsupported {
		t.Errorf("Classic advertises FeatureDeduplication as %v, want unsupported", lvl)
	}
	// And a name set with no broker at all is refused, because nothing on
	// the wire says which product a caller meant.
	if code := mq.CodeOf(p.Carry(&f, m, Options{Broker: activemq.Broker(9)})); code != mq.CodeInvalidConfig {
		t.Errorf("a mapping with no broker was accepted with code %v, want %v", code, mq.CodeInvalidConfig)
	}
}

// entryValue is one entry's value by key, or nil.
func entryValue(f *Fields, key string) []byte {
	for i := range f.Props {
		if f.Props[i].Key == key {
			return f.Props[i].value()
		}
	}
	return nil
}

// headerList renders a message's headers for a failure message.
func headerList(m *mq.Message) []string {
	var out []string
	for k := range m.Headers.All() {
		out = append(out, k)
	}
	return out
}
