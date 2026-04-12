package protocol

import (
	"bytes"
	"testing"
	"time"
	"unicode/utf8"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// FuzzActiveMQProtocol drives both arms through an operation
// script and holds them to three properties, over arbitrary messages, both
// brokers and all four metadata modes.
//
// It is a DIFFERENTIAL fuzz and the reference is the family's own reference
// kind, `amqp10`. That is the right oracle here
// because the property the family rests on is not "each arm is self-consistent"
// but "the two arms carry the SAME MESSAGE": the specification makes the protocol choice
// explicit, which is only honest if a caller who changes it finds the message
// they published. A round-trip test per arm would pass on two mappings that
// quietly disagreed about where the key went.
//
// The three properties, each of which is a real failure mode:
//
//  1. **FIDELITY.** A message that is CARRIED and then RESTORED is the message
//     that was published — the identifier, every property, the key and every
//     header — or the carry refused it. There is no third outcome: a mapping
//     that accepted a message and gave back a different one is worse than one
//     that refused it, because nothing in a deployment reports it.
//  2. **EQUIVALENCE.** When both arms accept a message, the two restored
//     messages are IDENTICAL. The two protocols put the same value in very
//     different places — the identifier is a properties-section field on one
//     arm and a reserved entry on the other, because STOMP's `message-id`
//     belongs to the broker; the content encoding likewise — so this is the
//     property those decisions exist to keep, and it is the one a driver
//     author would not think to check.
//  3. **PERMISSIVENESS.** Whatever the STOMP arm accepts, the reference arm
//     accepts too. STOMP refuses strictly more — a binary header value, a
//     binary key, a name that collides with a frame header, a relative TTL
//     with no resolved expiry — and the refusal sets are supposed to nest. If
//     they ever crossed, a caller moving to the DEFAULT protocol could be
//     refused a publication the non-default arm took, which is the one
//     direction the specification's "never silently substituted" cannot help with.
//
// Every refusal is also held it must carry a code a caller acts on,
// because "the broker cannot do this" and "your message is malformed" lead to
// different code and a refusal that answered neither would be untestable.
func FuzzActiveMQProtocol(f *testing.F) {
	seed := func(steps ...string) {
		w := fuzzutil.NewWriter()
		for _, s := range steps {
			script.WriteStep(w, s)
		}
		f.Add(w.Data())
	}
	// The seeds are step SELECTIONS, so each step's own argument reads fall
	// through to the next selection's bytes; they are seeds for the fuzzer to
	// grow rather than exact scripts, which is the shape
	// internal/rabbit/confirm's corpus has.
	seed("header", "key")
	seed("key", "group")
	seed("header", "header", "header")
	seed("binaryheader")
	seed("binarykey")
	seed("clashheader")
	seed("reservedheader")
	seed("ttl")
	seed("ttl", "expiry")
	seed("delay")
	seed("delay", "resolvedelay")
	seed("deliverat", "resolvedelay")
	seed("dedup", "broker")
	seed("broker", "dedup")
	seed("mode", "binaryheader")
	seed("mode", "mode", "clashheader", "header")
	seed("limitbytes", "header")
	seed("limitcount", "header", "header", "header")
	seed("property", "property", "key", "header")
	seed("priority", "ttl", "delay", "resolvedelay", "key", "group", "header")
	seed("reset", "header")
	seed("attempt")
	seed("attempt", "redeliver")
	seed("echo")
	seed("echo", "echo", "header")
	seed("echo", "key", "group")

	f.Fuzz(func(t *testing.T, data []byte) {
		sut := newProtocolSUT(t)
		script.Run(t, data, sut)
		// One last check with the script's final state, so that an input
		// whose last step is a mutation is checked too.
		sut.differential(t)
	})
}

// protocolSUT is the message and options being built, plus the two arms.
//
// There is no hand-written model of the mapping: the model is the SOURCE
// MESSAGE, which is what fidelity compares against, and the reference arm,
// which is what equivalence compares against. A third hand-written mapping
// would be a third thing to get wrong.
type protocolSUT struct {
	amqp Protocol
	st   Protocol
	// src is the message under construction.
	src *mq.Message
	// opts is the option set under construction.
	opts Options
	// headers counts the header keys handed out, so that a script can build
	// several distinct headers without colliding by accident.
	headers int
	// echo is what the BROKER would have added to a delivery, which nothing
	// [Protocol.Carry] produces can stand in for.
	//
	// Without it the two filters in [Protocol.Restore] are unreachable from
	// this target: `Carry` REFUSES every broker-property name, so a script
	// that only restores what it carried never feeds one in, and a mutation
	// that deleted the filter passed every check here while failing
	// TestModel. That is the defect class internal/rabbit/protocol records in
	// modelReceivedReserved, arrived at from the fuzz side.
	//
	// Only names filtered on BOTH arms go in, because the arms differ on the
	// rest: a `destination` application property is the caller's on the AMQP
	// 1.0 arm and is handed back, so injecting one would break fidelity
	// rather than test the filter.
	echo []Entry
	// fAmqp and fSt are the two arms' reusable buffers, reused across steps
	// exactly as a sink reuses one -- which is also what keeps the fuzz
	// target from hiding a Reset that does not reset.
	fAmqp, fSt Fields
	backAmqp   mq.Message
	backSt     mq.Message
}

// newProtocolSUT starts from a bare message on Artemis under the strict mode.
func newProtocolSUT(t testing.TB) *protocolSUT {
	t.Helper()
	amqp, err := New(AMQP10)
	if err != nil {
		t.Fatal(err)
	}
	st, err := New(STOMP)
	if err != nil {
		t.Fatal(err)
	}
	return &protocolSUT{
		amqp: amqp,
		st:   st,
		src:  &mq.Message{Body: []byte("body")},
		opts: Options{Broker: activemq.Artemis, KeyHeader: KeyHeader},
	}
}

// script is the operation script both arms are driven by.
var script = fuzzutil.Script[*protocolSUT]{
	MaxSteps: 64,
	Steps: []fuzzutil.Step[*protocolSUT]{
		{Name: "header", Weight: 6, Run: opHeader},
		{Name: "key", Weight: 4, Run: opKey},
		{Name: "property", Weight: 3, Run: opProperty},
		{Name: "group", Weight: 2, Run: opGroup},
		{Name: "ttl", Weight: 2, Run: opTTL},
		{Name: "expiry", Weight: 2, Run: opExpiry},
		{Name: "delay", Weight: 2, Run: opDelay},
		{Name: "deliverat", Weight: 1, Run: opDeliverAt},
		{Name: "resolvedelay", Weight: 2, Run: opResolveDelay},
		{Name: "priority", Weight: 1, Run: opPriority},
		{Name: "dedup", Weight: 1, Run: opDedup},
		{Name: "broker", Weight: 2, Run: opBroker},
		{Name: "mode", Weight: 2, Run: opMode},
		{Name: "limitbytes", Weight: 1, Run: opLimitBytes},
		{Name: "limitcount", Weight: 1, Run: opLimitCount},
		{Name: "binaryheader", Weight: 2, Run: opBinaryHeader},
		{Name: "binarykey", Weight: 1, Run: opBinaryKey},
		{Name: "clashheader", Weight: 2, Run: opClashHeader},
		{Name: "reservedheader", Weight: 1, Run: opReservedHeader},
		{Name: "echo", Weight: 3, Run: opEcho},
		{Name: "attempt", Weight: 1, Run: opAttempt},
		{Name: "redeliver", Weight: 1, Run: opRedeliver},
		{Name: "reset", Weight: 1, Run: opReset},
	},
	Check: func(t testing.TB, s *protocolSUT) { s.differential(t) },
}

// opHeader adds a header whose key and value come from the input.
//
// The key is derived from the counter as well as from the input so that the
// fuzzer can produce both a fresh header and a DUPLICATE of an existing one,
// which is the case and both protocols disagree about.
func opHeader(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	key := r.String(12)
	if r.Bool() {
		key = "h" + decimal(int64(s.headers))
	}
	s.headers++
	s.src.Headers.Add(key, r.Bytes(24))
}

// opBinaryHeader adds a header whose value is certainly not valid UTF-8, which
// is the row the two arms are supposed to disagree on.
func opBinaryHeader(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.headers++
	s.src.Headers.Add("blob"+decimal(int64(s.headers)), []byte{0x00, 0xff, 0xfe, r.Byte()})
}

// opClashHeader adds a header whose NAME is a STOMP frame header or one of the
// brokers' own, which the STOMP arm must refuse rather than shadow.
func opClashHeader(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	names := [...]string{
		"destination", "expires", "priority", "persistent", "timestamp",
		"message-id", "subscription", "ack", "redelivered", "content-length",
		"receipt", "reply-to", "correlation-id", "content-type",
		PropertyGroupID, PropertyScheduledDelay, PropertyDuplicateID,
		PropertyClassicScheduledJob, "_AMQ_SOMETHING",
	}
	s.src.Headers.Add(names[r.Choice(len(names))], r.Bytes(8))
}

// opReservedHeader adds a header under these drivers' own prefix, which BOTH
// arms must refuse: it is the one header a caller must not be able to set.
func opReservedHeader(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	names := [...]string{KeyHeader, CausationHeader, EncodingHeader, IDHeader, ReservedHeaderPrefix + "future"}
	s.src.Headers.Add(names[r.Choice(len(names))], r.Bytes(8))
}

// opKey sets a text key, which both arms can carry.
func opKey(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	k := r.Bytes(16)
	if !utf8.Valid(k) {
		// Keep this step's key carriable by both arms; opBinaryKey is the
		// step that deliberately is not.
		k = []byte("k" + decimal(int64(len(k))))
	}
	s.src.Key = k
}

// opBinaryKey sets a key that no frame header can carry.
func opBinaryKey(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.src.Key = []byte{0xff, 0xfe, r.Byte()}
}

// opGroup turns the key into the message group, which is how ActiveMQ orders
// one key.
func opGroup(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.opts.KeyAsGroup = r.Bool()
	if r.Bool() {
		// A key that is carried as a group and not as an entry, and the
		// other way round: the two are separate decisions and a mapping that
		// conflated them would lose one of them.
		s.opts.KeyHeader = ""
	} else {
		s.opts.KeyHeader = KeyHeader
	}
}

// opProperty sets one of the typed properties.
func opProperty(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	v := r.String(16)
	switch r.Choice(7) {
	case 0:
		s.src.Properties.ContentType = v
	case 1:
		s.src.Properties.ContentEncoding = v
	case 2:
		s.src.Properties.CorrelationID = v
	case 3:
		s.src.Properties.CausationID = v
	case 4:
		s.src.Properties.ReplyTo = v
	case 5:
		s.src.ID = v
	default:
		s.src.Properties.CreatedAt = time.Unix(int64(r.Int(0, 1<<31-1)), 0).UTC()
	}
}

// opTTL asks for a per-message TTL, which is relative on one arm and an
// instant on the other.
func opTTL(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.opts.Publish.TTL = r.Duration(time.Hour)
}

// opExpiry gives the mapping the absolute expiry a driver would have resolved,
// which is what the STOMP arm needs and what neither arm may read a clock for.
func opExpiry(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	if r.Bool() {
		s.opts.Expiry = time.Time{}
		return
	}
	s.opts.Expiry = time.Unix(int64(r.Int(1, 1<<31-1)), 0).UTC()
}

// opDelay asks for a relative scheduled delay.
func opDelay(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.opts.Publish.Delay = r.Duration(time.Hour)
	s.opts.Publish.DeliverAt = time.Time{}
}

// opDeliverAt asks for an absolute delivery time, which only the AMQP 1.0 arm
// has a slot for.
func opDeliverAt(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.opts.Publish.DeliverAt = time.Unix(int64(r.Int(1, 1<<31-1)), 0).UTC()
	s.opts.Publish.Delay = 0
}

// opResolveDelay gives the mapping the duration a driver resolved, or takes it
// away again.
func opResolveDelay(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	if r.Bool() {
		s.opts.Delay = 0
		return
	}
	s.opts.Delay = r.Duration(time.Hour)
}

// opPriority names a priority, including zero, which is a real priority.
func opPriority(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.opts.Publish.PrioritySet = r.Bool()
	s.opts.Publish.Priority = byte(r.Int(0, 9))
}

// opDedup asks for broker-side de-duplication, which Artemis has and Classic
// does not.
func opDedup(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.opts.Publish.DedupKey = r.String(12)
}

// opBroker switches products, which changes the delay slot and whether a
// de-duplication key can be carried at all.
func opBroker(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	brokers := activemq.Brokers()
	s.opts.Broker = brokers[r.Choice(len(brokers))]
}

// opMode switches the metadata mode, which decides whether what cannot be
// carried is refused or dropped and counted.
func opMode(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	modes := [...]mq.MetadataMode{
		mq.MetadataNativeStrict, mq.MetadataPortableEnvelope,
		mq.MetadataAdaptive, mq.MetadataLenientDrop,
	}
	s.opts.Metadata = modes[r.Choice(len(modes))]
}

// opLimitBytes advertises a header-byte ceiling, which must be enforced before
// any I/O.
func opLimitBytes(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.opts.Limits.MaxHeaderBytes = mq.Limit[int64]{Value: int64(r.Int(0, 64)), Known: r.Bool()}
}

// opLimitCount advertises a header-count ceiling.
func opLimitCount(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.opts.Limits.MaxHeaderCount = mq.Limit[int]{Value: r.Int(0, 8), Known: r.Bool()}
}

// opEcho adds a header the BROKER would have put on a delivery.
//
// Measured on the two pinned images: Artemis echoes a group identifier as
// `_AMQ_GROUP_ID` and a scheduled delivery as `_AMQ_SCHED_DELIVERY` (an
// absolute epoch time, not the delay that was sent), and Classic echoes a
// scheduled delivery as `scheduledJobId`. None of them is the publisher's
// header, and handing one back to an application would make a round trip add a
// header nobody published — the fidelity scenario — and let an application
// that echoed its headers publish somebody else's group.
func opEcho(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	names := [...]string{
		PropertyArtemisGroupID, PropertyArtemisScheduled, PropertyDuplicateID,
		PropertyGroupID, PropertyScheduledDelay, PropertyScheduledTime,
		PropertyClassicScheduledJob, "_AMQ_A_FUTURE_RELEASE_ADDS_THIS",
		ReservedHeaderPrefix + "future", ReservedHeaderPrefix + "pub",
	}
	e := Entry{Key: names[r.Choice(len(names))]}
	switch r.Choice(3) {
	case 0:
		e.Text, e.Kind = r.String(8), Text
	case 1:
		e.Bin, e.Kind = r.Bytes(8), Binary
	default:
		e.Int, e.Kind = int64(r.Int(-8, 1<<31-1)), Int
	}
	s.echo = append(s.echo, e)
}

// opAttempt sets what a DELIVERY would have said about its attempt count, so
// that the asymmetry between the two arms is exercised rather than described.
func opAttempt(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	n := r.Int(-2, 8)
	s.fAmqp.DeliveryCount, s.fSt.DeliveryCount = n, n
}

// opRedeliver sets STOMP's boolean, which is all a STOMP delivery says.
func opRedeliver(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	b := r.Bool()
	s.fAmqp.Redelivered, s.fSt.Redelivered = b, b
}

// opReset starts from a bare message again, keeping the options: a sink maps
// many messages with one option set, and a Reset that does not reset is how
// one message's key reaches the next one's delivery.
func opReset(t testing.TB, r *fuzzutil.Reader, s *protocolSUT) {
	s.src = &mq.Message{Body: []byte("body")}
	s.headers = 0
	s.echo = s.echo[:0]
	if r.Bool() {
		s.src.Body = nil
	}
}

// differential is the three properties, run after every step.
func (s *protocolSUT) differential(t testing.TB) {
	t.Helper()
	amqpErr := s.amqp.Carry(&s.fAmqp, s.src, s.opts)
	stErr := s.st.Carry(&s.fSt, s.src, s.opts)
	checkRefusal(t, "amqp10", amqpErr)
	checkRefusal(t, "stomp", stErr)

	// (3) PERMISSIVENESS: the refusal sets nest, with the reference arm as
	// the more permissive one.
	//
	// It holds under the three modes that refuse and NOT under
	// MetadataLenientDrop, and the exception is the property working rather
	// than failing: under that mode the two arms deliberately carry
	// different amounts -- the STOMP arm drops a binary key it cannot make
	// text of and the AMQP 1.0 arm keeps it -- so an advertised header COUNT
	// can be met by the arm that dropped something and breached by the arm
	// that did not. The fuzz target found exactly that, and the answer is to
	// state the property where it is a property: the more capable arm being
	// refused for carrying MORE is not the crossing this check exists to
	// catch.
	if o := s.opts.Metadata; o != mq.MetadataLenientDrop && amqpErr != nil && stErr == nil {
		t.Fatalf("the reference arm refused a publication the STOMP arm accepted under %v (%v): the refusal sets are supposed to nest, and a caller moving to the DEFAULT protocol must not be refused a message the non-default arm took", o, amqpErr)
	}

	// What the broker would have added to the delivery. It goes in AFTER
	// checkFields, because these entries are the broker's and not the
	// mapping's, and before Restore, because Restore is what has to filter
	// them out.
	if amqpErr == nil {
		checkFields(t, "amqp10", &s.fAmqp)
		s.fAmqp.Props = append(s.fAmqp.Props, s.echo...)
		s.backAmqp.Reset()
		if err := s.amqp.Restore(&s.backAmqp, &s.fAmqp, s.opts); err != nil {
			t.Fatalf("amqp10: Restore refused a Fields that Carry produced: %v", err)
		}
		s.checkFidelity(t, "amqp10", &s.backAmqp, s.fAmqp.Dropped)
	}
	if stErr == nil {
		checkFields(t, "stomp", &s.fSt)
		s.fSt.Props = append(s.fSt.Props, s.echo...)
		s.backSt.Reset()
		if err := s.st.Restore(&s.backSt, &s.fSt, s.opts); err != nil {
			t.Fatalf("stomp: Restore refused a Fields that Carry produced: %v", err)
		}
		s.checkFidelity(t, "stomp", &s.backSt, s.fSt.Dropped)
	}

	// (2) EQUIVALENCE, when both arms took the message.
	if amqpErr == nil && stErr == nil {
		s.checkEquivalence(t)
	}

	// The attempt is the one thing the two arms deliberately do NOT agree
	// about, so it is checked against each protocol's own promise rather
	// than against the other arm.
	s.checkAttempt(t)

	// And a group the broker echoed is readable under either spelling, which
	// is what a consumer has to do because Artemis echoes one and Classic
	// echoes nothing.
	if amqpErr == nil {
		group, known := EchoedGroup(&s.fAmqp)
		wantKnown := false
		for _, e := range s.echo {
			if e.Key == PropertyGroupID || e.Key == PropertyArtemisGroupID {
				wantKnown = true
				break
			}
		}
		if known != wantKnown {
			t.Fatalf("EchoedGroup reported present=%v (group %x) over entries %v, want present=%v", known, group, s.echo, wantKnown)
		}
	}
}

// checkRefusal holds a refusal a code a caller acts on.
func checkRefusal(t testing.TB, arm string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	switch code := mq.CodeOf(err); code {
	case mq.CodeUnsupported, mq.CodeInvalidArgument, mq.CodeInvalidConfig, mq.CodeMessageTooLarge:
	default:
		t.Fatalf("%s: Carry refused with code %v, which a caller cannot act on: %v", arm, code, err)
	}
}

// checkFields holds the produced entries to what the protocol can encode.
func checkFields(t testing.TB, arm string, f *Fields) {
	t.Helper()
	for i := range f.Props {
		e := &f.Props[i]
		if e.Key == "" {
			t.Fatalf("%s: entry %d has an empty key", arm, i)
		}
		if len(e.Key) > activemq.MaxNameLen {
			t.Fatalf("%s: entry %d has a %d-byte key, which neither broker's destination namespace accepts", arm, i, len(e.Key))
		}
		if !e.Kind.Valid() {
			t.Fatalf("%s: entry %d has the value kind %d", arm, i, e.Kind)
		}
		if arm == "stomp" && !utf8.ValidString(e.Key) {
			t.Fatalf("%s: entry %d has a key that is not valid UTF-8 (%q); a frame header name is text", arm, i, e.Key)
		}
		if arm == "stomp" && e.Kind == Binary && !utf8.Valid(e.Bin) {
			t.Fatalf("%s: entry %q carries a value that is not valid UTF-8 (%x); a frame header value is text and this one would be mangled on the wire", arm, e.Key, e.Bin)
		}
	}
	// Exactly one TTL spelling per arm, and it is that protocol's. Filling
	// both would let a driver read a value it should have ignored.
	if arm == "stomp" && f.TTL != 0 {
		t.Fatalf("stomp: a relative ttl of %v was produced for a protocol with no relative expiry header", f.TTL)
	}
	// A delay is in whole milliseconds, because that is the unit both
	// brokers' keys are in.
	if f.Delay%time.Millisecond != 0 {
		t.Fatalf("%s: the delay %v is not a whole number of milliseconds", arm, f.Delay)
	}
	// And nothing is written when nothing was asked for.
	if f.Slot == NoDelay && (f.Delay != 0 || !f.DeliverAt.IsZero()) {
		t.Fatalf("%s: the NoDelay slot carries delay=%v at=%v; a scheduled delay of zero is accepted by both brokers and is not the same message", arm, f.Delay, f.DeliverAt)
	}
	if f.Slot != NoDelay && f.Delay == 0 && f.DeliverAt.IsZero() {
		t.Fatalf("%s: the %v slot carries no value at all", arm, f.Slot)
	}
}

// checkFidelity is property (1): what was carried is what comes back.
func (s *protocolSUT) checkFidelity(t testing.TB, arm string, back *mq.Message, dropped int) {
	t.Helper()
	if back.ID != s.src.ID {
		t.Fatalf("%s: the identifier round-tripped as %q, want %q", arm, back.ID, s.src.ID)
	}
	if back.Properties != s.src.Properties {
		t.Fatalf("%s: the properties round-tripped as %+v, want %+v", arm, back.Properties, s.src.Properties)
	}
	// None of these drivers' own entries, and none of the brokers' own,
	// reached the application. This half holds under every mode, because
	// dropping metadata is not licence to invent it.
	for key := range back.Headers.All() {
		if ReservedHeader(key) {
			t.Fatalf("%s: the restored message carries one of these drivers' own headers %q", arm, key)
		}
		if ReservedProperty(key) {
			t.Fatalf("%s: the restored message carries a broker's own property %q", arm, key)
		}
		if arm == "stomp" && ReservedFrameHeader(key) {
			t.Fatalf("%s: the restored message carries the protocol's own frame header %q", arm, key)
		}
	}
	if dropped != 0 {
		// Under MetadataLenientDrop the headers and the key are deliberately
		// NOT all there -- a binary key cannot be carried over STOMP at all,
		// and that mode's contract is to drop it and count it rather than to
		// refuse the publication. The specification requires the COUNT, which is what
		// `dropped` is, and TestModel is what checks the count against a
		// known input; here the fidelity property simply does not apply.
		return
	}
	if s.opts.KeyHeader != "" && !bytes.Equal(back.Key, s.src.Key) && (len(back.Key) != 0 || len(s.src.Key) != 0) {
		t.Fatalf("%s: the key round-tripped as %x, want %x", arm, back.Key, s.src.Key)
	}
	if back.Headers.Len() != s.src.Headers.Len() {
		t.Fatalf("%s: %d headers round-tripped, want %d", arm, back.Headers.Len(), s.src.Headers.Len())
	}
	for key, want := range s.src.Headers.All() {
		got, ok := back.Headers.Get(key)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("%s: header %q round-tripped as %x (present=%v), want %x", arm, key, got, ok, want)
		}
	}
}

// checkEquivalence is property (2): the two arms carry the same message.
func (s *protocolSUT) checkEquivalence(t testing.TB) {
	t.Helper()
	a, b := &s.backAmqp, &s.backSt
	if a.ID != b.ID {
		t.Fatalf("the identifier is %q through AMQP 1.0 and %q through STOMP: one arm has a message-id property and the other has a message-id that belongs to the broker, and %q is where that difference escaped", a.ID, b.ID, IDHeader)
	}
	if a.Properties != b.Properties {
		t.Fatalf("the properties are %+v through AMQP 1.0 and %+v through STOMP: a caller who changes protocol must find the message they published", a.Properties, b.Properties)
	}
	if s.fAmqp.Dropped != 0 || s.fSt.Dropped != 0 {
		// Once EITHER arm has dropped something, the two are carrying
		// different subsets by design and comparing them says nothing. Under
		// MetadataLenientDrop the STOMP arm drops what it cannot make text of
		// and the AMQP 1.0 arm's typed properties keep it, and an advertised
		// header count then truncates each arm's own longer list -- so the
		// two DROP COUNTS can even coincide while the surviving headers
		// differ, which is how the fuzz target refuted the first version of
		// this gate. The identifier and the typed properties above hold
		// under every mode regardless, because neither arm ever drops one of
		// those (see carry's MaxHeaderCount refusal).
		return
	}
	if !bytes.Equal(a.Key, b.Key) && (len(a.Key) != 0 || len(b.Key) != 0) {
		t.Fatalf("the key is %x through AMQP 1.0 and %x through STOMP", a.Key, b.Key)
	}
	if a.Headers.Len() != b.Headers.Len() {
		t.Fatalf("%d headers through AMQP 1.0 and %d through STOMP", a.Headers.Len(), b.Headers.Len())
	}
	for key, want := range a.Headers.All() {
		got, ok := b.Headers.Get(key)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("header %q is %x through AMQP 1.0 and %x through STOMP (present=%v)", key, want, got, ok)
		}
	}
	// The group is the same value on both arms, which is what makes
	// per-key ordering mean the same thing whichever protocol is chosen.
	if !bytes.Equal(s.fAmqp.GroupID, s.fSt.GroupID) {
		t.Fatalf("the group is %x through AMQP 1.0 and %x through STOMP: it is how ActiveMQ orders a key and the two arms must name the same one", s.fAmqp.GroupID, s.fSt.GroupID)
	}
}

// checkAttempt holds each arm to its own promise about the attempt, which
// is the one thing the two deliberately do not agree about.
func (s *protocolSUT) checkAttempt(t testing.TB) {
	t.Helper()
	amqpN, amqpExact := Attempt(AMQP10, &s.fAmqp)
	stN, stExact := Attempt(STOMP, &s.fSt)
	if amqpN < 1 || stN < 1 {
		t.Fatalf("an attempt was reported as (%d, %d); the specification counts from one and a delivery is always at least the first", amqpN, stN)
	}
	if !amqpExact {
		t.Fatalf("the AMQP 1.0 arm reported an inexact attempt: `delivery-count` is a real count and delivery-count + 1 is exactly the attempt")
	}
	if want := max(s.fAmqp.DeliveryCount, 0) + 1; amqpN != want {
		t.Fatalf("delivery-count %d became attempt %d, want %d", s.fAmqp.DeliveryCount, amqpN, want)
	}
	// STOMP knows only "first" or "not first", measured on both brokers, and
	// a delivery-count it never receives must not become one.
	switch {
	case s.fSt.Redelivered:
		if stN != 2 || stExact {
			t.Fatalf("a redelivered STOMP message reported (%d, exact=%v), want (2, false): no STOMP delivery carries a count on either broker", stN, stExact)
		}
	default:
		if stN != 1 || !stExact {
			t.Fatalf("a first STOMP delivery reported (%d, exact=%v), want (1, true)", stN, stExact)
		}
	}
}
