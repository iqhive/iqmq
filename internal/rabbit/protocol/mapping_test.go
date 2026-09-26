package protocol

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/rabbit"
)

// carry and restore are the header mapping both RabbitMQ protocols share, and
// three of the things they decide are silent when they are wrong.
//
//   - The RESERVED PREFIX. protocol.go says it: "a message published with
//     `x-iqmq-key` set by the application would arrive with a key the
//     application never chose". A guard that let one through hands the
//     application's bytes to the driver's own slot, and nothing raises.
//   - The LENIENT mode. The specification says metadata dropped under MetadataLenientDrop is
//     reported through a hook and a metric "rather than silently", and
//     Fields.Dropped is that count. A lenient drop that forgot to increment it
//     is exactly the silence the mode exists to avoid.
//   - The TTL spelling. Exactly ONE of Expiration and TTL is filled, and which
//     one is the protocol's choice. Filling both, or the wrong one, gives a
//     message two deadlines or none.

// msg builds a message with the given headers, added in order so that a
// duplicate key really is a duplicate.
func msg(headers ...string) *mq.Message {
	m := &mq.Message{}
	for i := 0; i+1 < len(headers); i += 2 {
		m.Headers.Add(headers[i], []byte(headers[i+1]))
	}
	return m
}

func keys(f *Fields) []string {
	out := make([]string, 0, len(f.Entries))
	for i := range f.Entries {
		out = append(out, f.Entries[i].Key)
	}
	return out
}

func TestCarryRefusesAnInvalidMetadataModeBeforeTouchingTheMessage(t *testing.T) {
	t.Parallel()
	// The mode decides whether unrepresentable metadata is refused or dropped,
	// so a mode this package does not understand cannot be guessed at: guessing
	// strict would fail publications the caller allowed, and guessing lenient
	// would drop metadata the caller wanted refused.
	bad := Options{Metadata: mq.MetadataMode(99)}
	if bad.Metadata.Valid() {
		t.Fatal("MetadataMode(99) reports Valid; pick a larger value for this test")
	}
	var f Fields
	f.Entries = append(f.Entries, Entry{Key: "left-over"})
	if err := carry(&f, msg("a", "1"), bad, false); !errors.Is(err, ErrBadMetadataMode) {
		t.Errorf("carry = %v, want ErrBadMetadataMode", err)
	}
	// It must refuse BEFORE Reset, so a caller that ignored the error does not
	// send a half-built field set as if it were the message's.
	if len(f.Entries) != 1 || f.Entries[0].Key != "left-over" {
		t.Errorf("carry touched the destination before refusing: %v", keys(&f))
	}
	var m mq.Message
	if err := restore(&m, &Fields{}, bad); !errors.Is(err, ErrBadMetadataMode) {
		t.Errorf("restore = %v, want ErrBadMetadataMode", err)
	}
}

func TestCarryOfANilMessageIsAnEmptyFieldSetAndNotAnError(t *testing.T) {
	t.Parallel()
	// A sink may carry a nil message when it is building a control frame. The
	// destination must come back RESET rather than still holding the previous
	// message's headers, which is the reuse bug this signature invites: Fields
	// is pooled per sink.
	f := Fields{
		ContentType: "text/plain",
		MessageID:   "previous",
		Durable:     true,
		Dropped:     3,
	}
	f.Entries = append(f.Entries, Entry{Key: "from-the-last-message"})
	if err := carry(&f, nil, Options{}, false); err != nil {
		t.Fatalf("carry(nil): %v", err)
	}
	if f.ContentType != "" || f.MessageID != "" || f.Durable || f.Dropped != 0 || len(f.Entries) != 0 {
		t.Errorf("carry(nil) left the previous message behind: %+v", f)
	}
	// restore with a nil destination or nil fields is a no-op, not a panic: both
	// reach it from a driver's receive path where either may be absent.
	if err := restore(nil, &Fields{}, Options{}); err != nil {
		t.Errorf("restore(nil dst) = %v, want nil", err)
	}
	if err := restore(&mq.Message{}, nil, Options{}); err != nil {
		t.Errorf("restore(nil fields) = %v, want nil", err)
	}
}

func TestCarryRefusesAReservedHeaderRatherThanShadowingTheDriversOwn(t *testing.T) {
	t.Parallel()
	// Strict mode: the application's `x-iqmq-` header is a refusal. If it were
	// carried, it would land in the same table slot the driver uses for the key
	// and the causation identifier, and the consumer would read the
	// application's bytes as the driver's metadata.
	var f Fields
	err := carry(&f, msg(ReservedHeaderPrefix+"key", "forged"), Options{}, false)
	if !errors.Is(err, ErrReservedHeader) {
		t.Fatalf("carry = %v, want ErrReservedHeader", err)
	}
	// And the forged header must not be in the field set the caller might still
	// look at.
	for _, k := range keys(&f) {
		if strings.HasPrefix(k, ReservedHeaderPrefix) {
			t.Errorf("the refused reserved header %q is in the field set anyway", k)
		}
	}
	// The prefix check is a prefix check and not an equality check: any header
	// under the namespace is reserved, not only the ones in use today.
	for _, k := range []string{
		ReservedHeaderPrefix,
		ReservedHeaderPrefix + "causation",
		ReservedHeaderPrefix + "something-invented-next-year",
	} {
		var f Fields
		if err := carry(&f, msg(k, "v"), Options{}, false); !errors.Is(err, ErrReservedHeader) {
			t.Errorf("carry(%q) = %v, want ErrReservedHeader", k, err)
		}
	}
	// A header that merely starts with part of the prefix is the application's.
	var ok Fields
	if err := carry(&ok, msg("x-iqm", "v"), Options{}, false); err != nil {
		t.Errorf("carry(x-iqm) = %v; a header shorter than the prefix is not reserved", err)
	}
	if got := keys(&ok); len(got) != 1 || got[0] != "x-iqm" {
		t.Errorf("entries = %v, want [x-iqm]", got)
	}
}

func TestCarryRefusesADuplicateHeaderBecauseTheTableCannotHoldTwo(t *testing.T) {
	t.Parallel()
	var f Fields
	m := msg("dup", "first", "dup", "second")
	if m.Headers.Len() != 2 {
		t.Fatalf("the message has %d headers, want 2; Add is not keeping duplicates", m.Headers.Len())
	}
	if err := carry(&f, m, Options{}, false); !errors.Is(err, ErrDuplicateHeader) {
		t.Errorf("carry = %v, want ErrDuplicateHeader", err)
	}
	// Under lenient mode the second one is DROPPED and COUNTED, and the first
	// survives: dropping both, or dropping neither, are the two ways this can
	// go wrong quietly.
	f = Fields{}
	if err := carry(&f, m, Options{Metadata: mq.MetadataLenientDrop}, false); err != nil {
		t.Fatalf("lenient carry: %v", err)
	}
	if got := keys(&f); len(got) != 1 || got[0] != "dup" {
		t.Errorf("entries = %v, want exactly one 'dup'", got)
	}
	if f.Entries[0].Kind != Binary || string(f.Entries[0].Bin) != "first" {
		t.Errorf("the surviving entry is %+v, want the FIRST value", f.Entries[0])
	}
	if f.Dropped != 1 {
		t.Errorf("Dropped = %d, want 1: the specification requires the drop to be reported, not silent", f.Dropped)
	}
}

func TestCarryLenientModeCountsEveryKindOfDropItMakes(t *testing.T) {
	t.Parallel()
	lenient := Options{Metadata: mq.MetadataLenientDrop}
	for _, tc := range []struct {
		name        string
		m           *mq.Message
		o           Options
		wantDropped int
		wantKeys    []string
	}{
		{
			name:        "a reserved header",
			m:           msg("keep", "1", ReservedHeaderPrefix+"forged", "x"),
			o:           lenient,
			wantDropped: 1,
			wantKeys:    []string{"keep"},
		},
		{
			name:        "a key longer than the protocol's shortstr",
			m:           msg("keep", "1", strings.Repeat("k", rabbit.MaxNameLen+1), "x"),
			o:           lenient,
			wantDropped: 1,
			wantKeys:    []string{"keep"},
		},
		{
			name: "a header over the advertised byte limit",
			m:    msg("keep", "1", "big", strings.Repeat("v", 100)),
			o: Options{
				Metadata: mq.MetadataLenientDrop,
				Limits:   mq.Limits{MaxHeaderBytes: mq.Limit[int64]{Value: 50, Known: true}},
			},
			wantDropped: 1,
			wantKeys:    []string{"keep"},
		},
		{
			// The limit counts the KEY as well as the value, and this case is
			// the one that says so: the value alone is under the limit and only
			// key+value crosses it. Without it, a check that measured the value
			// alone would pass -- and the broker counts both, so the publication
			// would be refused on the wire with a channel exception instead.
			name: "a header whose KEY is what crosses the byte limit",
			m:    msg("keep", "1", strings.Repeat("k", 40), strings.Repeat("v", 20)),
			o: Options{
				Metadata: mq.MetadataLenientDrop,
				Limits:   mq.Limits{MaxHeaderBytes: mq.Limit[int64]{Value: 50, Known: true}},
			},
			wantDropped: 1,
			wantKeys:    []string{"keep"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var f Fields
			if err := carry(&f, tc.m, tc.o, false); err != nil {
				t.Fatalf("lenient carry: %v", err)
			}
			if got := keys(&f); len(got) != len(tc.wantKeys) || (len(got) > 0 && got[0] != tc.wantKeys[0]) {
				t.Errorf("entries = %v, want %v", got, tc.wantKeys)
			}
			if f.Dropped != tc.wantDropped {
				t.Errorf("Dropped = %d, want %d: a drop nobody counted is the silence the specification forbids", f.Dropped, tc.wantDropped)
			}
		})
	}
	// And the same inputs are REFUSALS under the default strict mode, or the
	// lenient cases above would prove nothing about leniency.
	for _, tc := range []struct {
		name string
		m    *mq.Message
		o    Options
		want error
	}{
		{"a reserved header", msg(ReservedHeaderPrefix+"f", "x"), Options{}, ErrReservedHeader},
		{"an over-long key", msg(strings.Repeat("k", rabbit.MaxNameLen+1), "x"), Options{}, ErrHeaderTooLarge},
		{
			"a header over the byte limit",
			msg("big", strings.Repeat("v", 100)),
			Options{Limits: mq.Limits{MaxHeaderBytes: mq.Limit[int64]{Value: 50, Known: true}}},
			ErrHeaderTooLarge,
		},
	} {
		t.Run("strict: "+tc.name, func(t *testing.T) {
			t.Parallel()
			var f Fields
			if err := carry(&f, tc.m, tc.o, false); !errors.Is(err, tc.want) {
				t.Errorf("carry = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestCarryHoldsTheHeaderCountLimitAndTruncatesOnlyWhenLenient(t *testing.T) {
	t.Parallel()
	m := msg("a", "1", "b", "2", "c", "3", "d", "4")
	lim := mq.Limits{MaxHeaderCount: mq.Limit[int]{Value: 2, Known: true}}
	var f Fields
	if err := carry(&f, m, Options{Limits: lim}, false); !errors.Is(err, ErrHeaderTooLarge) {
		t.Errorf("strict carry over the count limit = %v, want ErrHeaderTooLarge", err)
	}
	f = Fields{}
	if err := carry(&f, m, Options{Metadata: mq.MetadataLenientDrop, Limits: lim}, false); err != nil {
		t.Fatalf("lenient carry: %v", err)
	}
	if len(f.Entries) != 2 {
		t.Fatalf("kept %d entries, want 2 (the advertised limit)", len(f.Entries))
	}
	if f.Dropped != 2 {
		t.Errorf("Dropped = %d, want 2", f.Dropped)
	}
	// An UNKNOWN limit bounds nothing: capabilities.go is explicit that
	// "advertised as zero" and "not advertised" must never be read as each
	// other, and a driver that advertises no count limit must not have its
	// headers truncated to zero.
	f = Fields{}
	if err := carry(&f, m, Options{Limits: mq.Limits{MaxHeaderCount: mq.Limit[int]{Value: 0}}}, false); err != nil {
		t.Fatalf("carry with an unadvertised limit: %v", err)
	}
	if len(f.Entries) != 4 {
		t.Errorf("kept %d entries, want all 4: an unadvertised limit was enforced as zero", len(f.Entries))
	}
	// A limit that IS advertised as zero does bound, and to zero.
	f = Fields{}
	err := carry(&f, m, Options{Limits: mq.Limits{MaxHeaderCount: mq.Limit[int]{Value: 0, Known: true}}}, false)
	if !errors.Is(err, ErrHeaderTooLarge) {
		t.Errorf("carry with a limit advertised as zero = %v, want ErrHeaderTooLarge", err)
	}
}

func TestCarryPutsTheTTLInExactlyOneSpellingPerProtocol(t *testing.T) {
	t.Parallel()
	// Fields documents it: exactly one of Expiration and TTL is filled, and
	// filling both would give a message two deadlines that can disagree.
	o := Options{Publish: mq.PublishOptions{TTL: 1500 * time.Microsecond}}
	var asString Fields
	if err := carry(&asString, msg(), o, true); err != nil {
		t.Fatalf("carry: %v", err)
	}
	// Rounded UP: millisString's comment says a TTL must never be SHORTER than
	// the caller asked for, and 1.5ms truncated to 1ms expires a message half a
	// millisecond early on every hop.
	if asString.Expiration != "2" {
		t.Errorf("Expiration = %q, want \"2\" (rounded up from 1.5ms)", asString.Expiration)
	}
	if asString.TTL != 0 {
		t.Errorf("TTL = %v as well as Expiration %q; a message with two deadlines", asString.TTL, asString.Expiration)
	}
	var asDuration Fields
	if err := carry(&asDuration, msg(), o, false); err != nil {
		t.Fatalf("carry: %v", err)
	}
	if asDuration.TTL != 1500*time.Microsecond {
		t.Errorf("TTL = %v, want the caller's duration unchanged", asDuration.TTL)
	}
	if asDuration.Expiration != "" {
		t.Errorf("Expiration = %q as well as TTL %v", asDuration.Expiration, asDuration.TTL)
	}
	// A zero or negative TTL sets neither.
	for _, ttl := range []time.Duration{0, -time.Second} {
		for _, asStr := range []bool{true, false} {
			var f Fields
			if err := carry(&f, msg(), Options{Publish: mq.PublishOptions{TTL: ttl}}, asStr); err != nil {
				t.Fatalf("carry: %v", err)
			}
			if f.Expiration != "" || f.TTL != 0 {
				t.Errorf("TTL %v gave Expiration %q / TTL %v, want neither", ttl, f.Expiration, f.TTL)
			}
		}
	}
}

func TestMillisStringRendersTheSameDigitsStrconvWould(t *testing.T) {
	t.Parallel()
	// millisStringOf is a hand-rolled formatter written to keep strconv off the
	// hot path, so it is checked against strconv rather than against a handful
	// of literals: a hand-rolled digit loop is where an off-by-one lives, and
	// the value it renders is a message's TTL.
	for _, n := range []int64{
		0, 1, 9, 10, 99, 100, 999, 1000, 1234, 65535,
		1 << 20, 1 << 40, 999999999999999999,
		-1, -9, -10, -1234, -999999999999999999,
	} {
		if got, want := millisStringOf(n), strconv.FormatInt(n, 10); got != want {
			t.Errorf("millisStringOf(%d) = %q, want %q", n, got, want)
		}
	}
	// millisString rounds a duration UP to whole milliseconds, with a
	// non-positive duration rendering as the empty string (which is AMQP
	// 0-9-1's "no expiration") rather than as "0" (which is "expire at once").
	// Those two are opposite instructions to the broker.
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{0, ""},
		{-time.Second, ""},
		{time.Nanosecond, "1"},
		{time.Millisecond - time.Nanosecond, "1"},
		{time.Millisecond, "1"},
		{time.Millisecond + time.Nanosecond, "2"},
		{1500 * time.Microsecond, "2"},
		{2 * time.Millisecond, "2"},
		{time.Second, "1000"},
		{90 * time.Minute, "5400000"},
	} {
		if got := millisString(tc.d); got != tc.want {
			t.Errorf("millisString(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestRestoreKeepsTheDriversOwnEntriesOutOfTheApplicationsHeaders(t *testing.T) {
	t.Parallel()
	// the fidelity scenario: a round trip through this driver must not add
	// headers the publisher never set. The driver's key entry and its causation
	// entry are its own, so they come back as the message's Key and
	// CausationID and NOT as headers.
	f := &Fields{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		CorrelationID:   "corr",
		ReplyTo:         "reply-to",
		MessageID:       "id-1",
		Timestamp:       time.Unix(1700000000, 0).UTC(),
	}
	f.Entries = []Entry{
		{Key: "app", Bin: []byte("v"), Kind: Binary},
		{Key: CausationHeader, Text: "cause-1"},
		{Key: "x-routing-key", Bin: []byte("the-key"), Kind: Binary},
		{Key: ReservedHeaderPrefix + "invented", Text: "internal"},
		{Key: "as-text", Text: "text-value"},
		{Key: "as-int", Int: 4200, Kind: Int},
	}
	var m mq.Message
	if err := restore(&m, f, Options{KeyHeader: "x-routing-key"}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if m.Properties.ContentType != "application/json" || m.Properties.ContentEncoding != "gzip" {
		t.Errorf("content properties = %+v", m.Properties)
	}
	if m.Properties.CorrelationID != "corr" || m.Properties.ReplyTo != "reply-to" {
		t.Errorf("request/reply properties = %+v", m.Properties)
	}
	if m.ID != "id-1" || !m.Properties.CreatedAt.Equal(f.Timestamp) {
		t.Errorf("id = %q, createdAt = %v", m.ID, m.Properties.CreatedAt)
	}
	if m.Properties.CausationID != "cause-1" {
		t.Errorf("CausationID = %q, want cause-1 from the reserved entry", m.Properties.CausationID)
	}
	if string(m.Key) != "the-key" {
		t.Errorf("Key = %q, want the-key from the configured key header", m.Key)
	}
	for _, forbidden := range []string{CausationHeader, "x-routing-key", ReservedHeaderPrefix + "invented"} {
		if _, has := m.Headers.Get(forbidden); has {
			t.Errorf("%q came back as an application header; a round trip added a header the publisher never set", forbidden)
		}
	}
	// An entry's value reaches the headers whichever kind it is: a Text entry
	// as its text, an Int entry as its digits. Dropping the non-binary kinds
	// would lose a header the other protocol wrote.
	for _, tc := range []struct{ key, want string }{
		{"app", "v"},
		{"as-text", "text-value"},
		{"as-int", "4200"},
	} {
		got, has := m.Headers.Get(tc.key)
		if !has {
			t.Errorf("header %q is missing", tc.key)
			continue
		}
		if string(got) != tc.want {
			t.Errorf("header %q = %q, want %q", tc.key, got, tc.want)
		}
	}
	// An empty KeyHeader means the key travels as the routing key instead, so
	// an entry whose name happens to be empty must NOT be taken for it.
	var m2 mq.Message
	f2 := &Fields{Entries: []Entry{{Key: "", Bin: []byte("not-the-key"), Kind: Binary}}}
	if err := restore(&m2, f2, Options{KeyHeader: ""}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if m2.Key != nil {
		t.Errorf("Key = %q; an empty KeyHeader matched an empty entry name", m2.Key)
	}
}

func TestValueTextReadsEitherKindOfEntry(t *testing.T) {
	t.Parallel()
	// The causation identifier is written as Text by this package and may come
	// back as Binary from the other protocol's client library, so valueText has
	// to read both or a round trip loses the causation chain.
	if got := valueText(&Entry{Text: "t"}); got != "t" {
		t.Errorf("valueText(text) = %q, want t", got)
	}
	if got := valueText(&Entry{Bin: []byte("b"), Kind: Binary}); got != "b" {
		t.Errorf("valueText(binary) = %q, want b", got)
	}
	// And a causation entry that arrived as Binary still restores.
	var m mq.Message
	f := &Fields{Entries: []Entry{{Key: CausationHeader, Bin: []byte("cause-b"), Kind: Binary}}}
	if err := restore(&m, f, Options{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if m.Properties.CausationID != "cause-b" {
		t.Errorf("CausationID = %q, want cause-b from a binary entry", m.Properties.CausationID)
	}
}

func TestNewRefusesAKindThatNamesNoProtocol(t *testing.T) {
	t.Parallel()
	for _, k := range Kinds() {
		p, err := New(k)
		if err != nil {
			t.Fatalf("New(%v): %v", k, err)
		}
		if p.Kind() != k {
			t.Errorf("New(%v) returned the %v protocol", k, p.Kind())
		}
	}
	// Auto resolves to a concrete kind rather than refusing: it is the default a
	// DSN with no protocol parameter takes.
	p, err := New(Auto)
	if err != nil {
		t.Fatalf("New(Auto): %v", err)
	}
	if p.Kind() != AutoKind {
		t.Errorf("New(Auto) gave %v, want the AutoKind %v", p.Kind(), AutoKind)
	}
	// A kind outside the enumeration is a refusal, not the default: silently
	// substituting a protocol is what the specification forbids, because the two have
	// different feature subsets and different minimum broker versions.
	for _, k := range []Kind{Kind(99), Kind(200)} {
		p, err := New(k)
		if !errors.Is(err, ErrInvalidKind) {
			t.Errorf("New(%d) = (%v, %v), want ErrInvalidKind", k, p, err)
		}
		if p != nil {
			t.Errorf("a refused New also returned %v; a caller that ignored the error would open with it", p)
		}
		if k.Valid() {
			t.Errorf("Kind(%d).Valid() is true", k)
		}
		if got := k.String(); !strings.Contains(got, "?") {
			t.Errorf("Kind(%d).String() = %q, want something that reads as unknown", k, got)
		}
		if got := k.Ident(); got != "" {
			t.Errorf("Kind(%d).Ident() = %q, want empty", k, got)
		}
	}
	if got := ValueKind(99).String(); !strings.Contains(got, "?") {
		t.Errorf("ValueKind(99).String() = %q, want something that reads as unknown", got)
	}
	// ParseKind round-trips every slug and refuses anything else.
	for _, k := range Kinds() {
		got, ok := ParseKind(k.String())
		if !ok || got != k {
			t.Errorf("ParseKind(%q) = (%v, %v), want (%v, true)", k.String(), got, ok, k)
		}
	}
	for _, slug := range []string{"", "amqp", "AMQP091", "nope"} {
		if got, ok := ParseKind(slug); ok {
			t.Errorf("ParseKind(%q) = (%v, true), want false", slug, got)
		}
	}
	if got := Slugs(); len(got) != len(Kinds()) {
		t.Errorf("Slugs() has %d entries and Kinds() has %d", len(got), len(Kinds()))
	}
}
