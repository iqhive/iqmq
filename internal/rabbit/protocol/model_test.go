package protocol

import (
	"bytes"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/rabbit"
)

// TestModel runs both arms through the properties a protocol
// mapping has to have.
//
// The two arms are not two implementations of one answer — they are two
// protocols, and the whole point of the family is that their feature subsets
// DIFFER — so the model holds each of them to the rules
// put on any mapping, which is the half that must not differ:
//
//   - a message that is carried and restored comes back byte for byte, so a
//     round trip through either protocol changes nothing.
//   - metadata neither protocol can represent is REFUSED under the modes that
//     do not envelope and DROPPED with a count under the one that does, which
//     is the contract.
//   - this driver's own reserved entries never reach the application, so a
//     round trip does not add headers the publisher never set.
//   - the capability table is honest: every conditional support states its
//     condition, which is what `driverkit.CapabilityBuilder` refuses a table
//     without.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("roundtrip", func(t *testing.T) { modelRoundTrip(t, k) })
			t.Run("duplicate-header-refused", func(t *testing.T) { modelDuplicate(t, k) })
			t.Run("reserved-header-refused", func(t *testing.T) { modelReserved(t, k) })
			t.Run("lenient-drops-and-counts", func(t *testing.T) { modelLenient(t, k) })
			t.Run("capability-table-is-honest", func(t *testing.T) { modelTable(t, k) })
			t.Run("address", func(t *testing.T) { modelAddress(t, k) })
			t.Run("min-broker", func(t *testing.T) { modelMinBroker(t, k) })
			t.Run("received-reserved-entry-is-hidden", func(t *testing.T) { modelReceivedReserved(t, k) })
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

// names is the topology both arms derive an address from.
func names(t *testing.T) rabbit.Names {
	t.Helper()
	n, err := rabbit.Derive(rabbit.Config{Prefix: "iqmq.", Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// sample is the message every round trip starts from: every property, a
// binary header, a key, and a body.
func sample() *mq.Message {
	m := &mq.Message{
		Key:  []byte{0x00, 0xff, 'k'},
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
	m.Headers.Add("binary", []byte{0x00, 0x01, 0xfe})
	return m
}

// modelRoundTrip is the fidelity requirement at the mapping level: what is
// carried is what comes back.
func modelRoundTrip(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	src := sample()
	var f Fields
	o := Options{KeyHeader: ReservedHeaderPrefix + "key", Persistent: true}
	o.Publish.TTL = 90 * time.Second
	o.Publish.Priority, o.Publish.PrioritySet = 7, true
	if err := p.Carry(&f, src, o); err != nil {
		t.Fatalf("Carry: %v", err)
	}

	// The fixed properties landed where they belong.
	if f.ContentType != src.Properties.ContentType || f.ContentEncoding != src.Properties.ContentEncoding {
		t.Errorf("content type/encoding are (%q, %q), want (%q, %q)", f.ContentType, f.ContentEncoding, src.Properties.ContentType, src.Properties.ContentEncoding)
	}
	if f.MessageID != src.ID || f.CorrelationID != src.Properties.CorrelationID || f.ReplyTo != src.Properties.ReplyTo {
		t.Errorf("identifier/correlation/reply-to are (%q, %q, %q), want (%q, %q, %q)",
			f.MessageID, f.CorrelationID, f.ReplyTo, src.ID, src.Properties.CorrelationID, src.Properties.ReplyTo)
	}
	if !f.Timestamp.Equal(src.Properties.CreatedAt) {
		t.Errorf("timestamp is %v, want %v", f.Timestamp, src.Properties.CreatedAt)
	}
	if !f.Durable {
		t.Error("a persistent publication was not marked durable")
	}
	if f.Priority != 7 || !f.HasPriority {
		t.Errorf("priority is (%d, %v), want (7, true)", f.Priority, f.HasPriority)
	}
	// Exactly one TTL spelling is filled, and it is the one this protocol
	// uses. A driver reading the field its protocol has must find it filled,
	// and the other arm must not have paid to render a string it would throw
	// away -- which is one of the few places the two arms do
	// different work per message.
	switch k.Resolve() {
	case AMQP091:
		if f.Expiration != "90000" {
			t.Errorf("the AMQP 0-9-1 expiration is %q, want %q", f.Expiration, "90000")
		}
		if f.TTL != 0 {
			t.Errorf("the AMQP 0-9-1 arm also filled the AMQP 1.0 ttl (%v), which it would throw away", f.TTL)
		}
	case AMQP10:
		if f.TTL != 90*time.Second {
			t.Errorf("the AMQP 1.0 ttl is %v, want %v", f.TTL, 90*time.Second)
		}
		if f.Expiration != "" {
			t.Errorf("the AMQP 1.0 arm rendered the AMQP 0-9-1 expiration string (%q), which is an allocation per publication it would throw away", f.Expiration)
		}
	case Auto, kindCount:
		// Unreachable: Resolve never answers Auto and the sentinel is not a
		// kind. Naming both is what makes the switch total, so a new kind
		// does not land in a silent default.
		t.Fatalf("Resolve answered %v", k.Resolve())
	}

	// And the message comes back.
	var back mq.Message
	if err := p.Restore(&back, &f, o); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if back.ID != src.ID || back.Properties != src.Properties {
		t.Errorf("the restored properties are %+v, want %+v", back.Properties, src.Properties)
	}
	if !bytes.Equal(back.Key, src.Key) {
		t.Errorf("the restored key is %x, want %x", back.Key, src.Key)
	}
	if back.Headers.Len() != src.Headers.Len() {
		t.Fatalf("the restored message has %d headers, want %d: %v", back.Headers.Len(), src.Headers.Len(), headerList(&back))
	}
	for key, want := range src.Headers.All() {
		got, ok := back.Headers.Get(key)
		if !ok || !bytes.Equal(got, want) {
			t.Errorf("header %q restored as %x (present=%v), want %x", key, got, ok, want)
		}
	}
	// This driver's own entries must not reach the application.
	for key := range back.Headers.All() {
		if strings.HasPrefix(key, ReservedHeaderPrefix) {
			t.Errorf("the restored message carries this driver's own header %q: a round trip must not add headers the publisher never set", key)
		}
	}
}

// modelDuplicate is the promise meeting a protocol that cannot keep it.
//
// Neither protocol's property table can carry a repeated key, so the honest
// answer under a mode that does not envelope is a refusal — and the refusal
// has to be Unsupported and not InvalidArgument, because the caller's message
// is fine and this broker is what cannot carry it.
func modelDuplicate(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Add("dup", []byte("one"))
	m.Headers.Add("dup", []byte("two"))
	var f Fields
	for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataPortableEnvelope, mq.MetadataAdaptive} {
		err := p.Carry(&f, m, Options{Metadata: mode})
		if err == nil {
			t.Fatalf("mode %v accepted a repeated header key: the table would carry one value where two were published", mode)
		}
		if code := mq.CodeOf(err); code != mq.CodeUnsupported {
			t.Fatalf("mode %v refused a repeated header with code %v, want %v: the caller's message is well formed and this broker cannot carry it", mode, code, mq.CodeUnsupported)
		}
	}
	// A single header of the same name is of course fine, so the refusal is
	// about the duplication and not about the name.
	m.Headers.Delete("dup")
	m.Headers.Add("dup", []byte("one"))
	if err := p.Carry(&f, m, Options{}); err != nil {
		t.Fatalf("a single header was refused: %v", err)
	}
}

// modelReserved is the header a caller must not be able to set, because the
// driver means something by it.
func modelReserved(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Add(ReservedHeaderPrefix+"key", []byte("mine"))
	var f Fields
	err := p.Carry(&f, m, Options{})
	if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
		t.Fatalf("a header under %q was refused with code %v, want %v: shadowing it would give the message a key the application never chose",
			ReservedHeaderPrefix, code, mq.CodeInvalidArgument)
	}
	// A header that merely starts with "x-" is not reserved: RabbitMQ's own
	// extensions use that prefix and a caller may well set one.
	m.Headers.Delete(ReservedHeaderPrefix + "key")
	m.Headers.Add("x-something", []byte("theirs"))
	if err := p.Carry(&f, m, Options{}); err != nil {
		t.Fatalf("a header under \"x-\" was refused: only %q is reserved: %v", ReservedHeaderPrefix, err)
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
	m.Headers.Add(ReservedHeaderPrefix+"key", []byte("mine"))
	var f Fields
	if err := p.Carry(&f, m, Options{Metadata: mq.MetadataLenientDrop}); err != nil {
		t.Fatalf("the lenient mode refused a publication: %v", err)
	}
	if f.Dropped != 2 {
		t.Errorf("the lenient mode dropped %d entries, want 2 (the repeated key and the reserved one)", f.Dropped)
	}
	got := 0
	for i := range f.Entries {
		if f.Entries[i].Key == "keep" {
			got++
		}
	}
	if got != 1 {
		t.Errorf("the representable header was carried %d times, want once", got)
	}
}

// modelTable is what `driverkit.CapabilityBuilder` would refuse: a conditional
// support with no condition, or an emulation with no explanation.
//
// The specification requires both, and a table that omitted one would reach a caller as
// "this might work, under conditions nobody wrote down".
func modelTable(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	for f := range mq.FeatureCount {
		feat := mq.Feature(f)
		s := p.Support(feat)
		switch s.Level {
		case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
			if s.Note == "" {
				t.Errorf("%v is %v with no note: the message contract requires the condition or the emulation to be stated", feat, s.Level)
			}
		case mq.Native:
			if s.Scope == mq.ScopeNone {
				t.Errorf("%v is native with no scope: a feature that holds over nothing is not a feature", feat)
			}
		case mq.Unsupported:
			if s.Note != "" || s.Scope != mq.ScopeNone {
				t.Errorf("%v is unsupported and carries a scope or a note: %+v", feat, s)
			}
		}
	}
	if n := p.Features(); n < 10 || n > mq.FeatureCount {
		t.Errorf("%v supports %d features natively, which is not a plausible count of the %d in the inventory", k, n, mq.FeatureCount)
	}
}

// modelAddress checks that each protocol's address is its OWN, which is the
// half of the family a shared derivation could quietly equalise.
func modelAddress(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	n := names(t)
	pub, err := p.Address(n, Publish)
	if err != nil {
		t.Fatal(err)
	}
	con, err := p.Address(n, Consume)
	if err != nil {
		t.Fatal(err)
	}
	// The 0-9-1 fields are filled by both, so that a driver reporting a
	// delivery's origin has the same names whichever protocol it speaks.
	if pub.Exchange != n.Exchange || pub.RoutingKey != n.RoutingKey {
		t.Errorf("the publish address is (%q, %q), want (%q, %q)", pub.Exchange, pub.RoutingKey, n.Exchange, n.RoutingKey)
	}
	if con.Queue != n.Queue {
		t.Errorf("the consume address names the queue %q, want %q", con.Queue, n.Queue)
	}
	switch k.Resolve() {
	case AMQP091:
		if pub.Node != "" || con.Node != "" {
			t.Errorf("the AMQP 0-9-1 arm produced the link addresses %q and %q: it has no link address", pub.Node, con.Node)
		}
	case AMQP10:
		if want := ExchangePrefix + n.Exchange + "/" + n.RoutingKey; pub.Node != want {
			t.Errorf("the publish link address is %q, want %q", pub.Node, want)
		}
		if want := QueuePrefix + n.Queue; con.Node != want {
			t.Errorf("the consume link address is %q, want %q", con.Node, want)
		}
	case Auto, kindCount:
		// Unreachable: Resolve never answers Auto and the sentinel is not a
		// kind. Naming both is what makes the switch total, so a new kind
		// does not land in a silent default.
		t.Fatalf("Resolve answered %v", k.Resolve())
	}
	// A name set with nothing to address is refused rather than producing an
	// address that reaches the broker's default exchange by accident.
	if _, err := p.Address(rabbit.Names{}, Publish); err == nil {
		t.Error("an empty name set produced a publish address")
	}
	if _, err := p.Address(rabbit.Names{Exchange: "x"}, Consume); err == nil {
		t.Error("a name set with no queue produced a consume address")
	}
}

// modelMinBroker is the specification's "supported-version compatibility" term, which is
// what settles this family's default before any measurement.
func modelMinBroker(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	min := p.MinBroker()
	if min.IsZero() {
		t.Fatal("the protocol claims no minimum broker version, so nothing would ever be refused")
	}
	// The version matrix's oldest image is 3.13, and whether this protocol
	// accepts it is the family's whole decision.
	oldest := rabbit.Version{Major: 3, Minor: 13}
	accepts := oldest.AtLeast(min)
	switch k.Resolve() {
	case AMQP091:
		if !accepts {
			t.Errorf("the AMQP 0-9-1 arm refuses RabbitMQ %v, which is the oldest image the matrix runs: it is the default precisely because it does not", oldest)
		}
	case AMQP10:
		if accepts {
			t.Errorf("the AMQP 1.0 arm accepts RabbitMQ %v: the specification forbids substituting it for AMQP 0-9-1 silently, and on 3.13 it is a plugin with different behaviour", oldest)
		}
		if !(rabbit.Version{Major: 4}).AtLeast(min) {
			t.Errorf("the AMQP 1.0 arm refuses RabbitMQ 4.0, which is the first release that speaks it natively")
		}
	case Auto, kindCount:
		// Unreachable: Resolve never answers Auto and the sentinel is not a
		// kind. Naming both is what makes the switch total, so a new kind
		// does not land in a silent default.
		t.Fatalf("Resolve answered %v", k.Resolve())
	}
}

// modelReceivedReserved is the case a round trip through Carry cannot reach,
// and the one the reserved-prefix filter in Restore really exists for.
//
// Carry refuses a caller's header under the reserved prefix and produces only
// two reserved entries of its own -- the causation identifier and the message
// key -- both of which Restore recognises by name. So a test that only ever
// restores what Carry produced never exercises the prefix filter at all, and a
// mutation that deleted it passed every other check in this file. What reaches
// a real Restore is a DELIVERY, whose entries come from the broker: a message
// published by this driver under a windowed publisher-confirm strategy carries
// the confirm correlation token (`internal/rabbit/confirm`'s, under the same
// prefix), and any entry a future version of this driver adds arrives the same
// way. Handing one of those back as an application header would make a round
// trip add a header the publisher never set, which is the fidelity
// scenario, and would let an application that echoes its headers publish a
// confirm token of somebody else's.
func modelReceivedReserved(t *testing.T, k Kind) {
	p := newProtocol(t, k)
	o := Options{KeyHeader: ReservedHeaderPrefix + "key"}
	f := Fields{Entries: []Entry{
		{Key: "trace", Text: "abc", Kind: Text},
		{Key: ReservedHeaderPrefix + "pub", Text: "p41", Kind: Text},
		{Key: ReservedHeaderPrefix + "future", Bin: []byte{0x01}, Kind: Binary},
		{Key: CausationHeader, Text: "cause-1", Kind: Text},
		{Key: o.KeyHeader, Bin: []byte("k"), Kind: Binary},
	}}
	var back mq.Message
	if err := p.Restore(&back, &f, o); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for key := range back.Headers.All() {
		if strings.HasPrefix(key, ReservedHeaderPrefix) {
			t.Errorf("a delivery's reserved entry %q reached the application as a header", key)
		}
	}
	if back.Headers.Len() != 1 {
		t.Errorf("the restored message has %d headers, want 1 (only the application's own): %v", back.Headers.Len(), headerList(&back))
	}
	if got, ok := back.Headers.Get("trace"); !ok || string(got) != "abc" {
		t.Errorf("the application's own header did not survive: %q (present=%v)", got, ok)
	}
	// The two reserved entries Restore DOES read are read, so the filter is
	// not simply dropping everything under the prefix.
	if back.Properties.CausationID != "cause-1" {
		t.Errorf("the causation identifier restored as %q", back.Properties.CausationID)
	}
	if string(back.Key) != "k" {
		t.Errorf("the key restored as %q", back.Key)
	}
}

// headerList renders a message's headers for a failure message.
func headerList(m *mq.Message) []string {
	var out []string
	for k := range m.Headers.All() {
		out = append(out, k)
	}
	return out
}
