package streams_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// Limits.resolve, the bookkeeping-field accessors and the field-name helpers.
//
// resolve is the one with teeth. fields.go says why it exists: "a non-positive
// bound means 'use the default': zero is what a caller who set nothing leaves
// behind, and reading it as 'nothing is allowed' would refuse every message."
// A driver constructs Limits as a struct literal and sets the fields it cares
// about, so EVERY unset field goes through resolve on every publication. One
// missing arm means one bound of zero, and a bound of zero refuses every
// message that has that field at all -- which for MaxBody is every message.

func TestLimitsResolveDefaultsEveryFieldTheCallerLeftUnset(t *testing.T) {
	t.Parallel()
	d := streams.DefaultLimits
	// For every field, a Limits with ONLY that field set must still accept a
	// message that fits the defaults everywhere else. That is what says resolve
	// filled the OTHER seven: if one arm were deleted, its bound would stay at
	// the zero the caller left and this ordinary message would be refused for
	// that field.
	fits := func() *mq.Message {
		m := &mq.Message{
			Key:   []byte("k"),
			Body:  []byte("body"),
			ID:    "id",
			Topic: "topic",
		}
		m.Headers.Add("hk", []byte("hv"))
		m.Properties.ContentType = "text/plain"
		m.Properties.CorrelationID = "corr"
		m.Properties.CausationID = "cause"
		m.Properties.ReplyTo = "reply"
		m.Properties.ContentEncoding = "identity"
		return m
	}
	setters := map[string]func(*streams.Limits){
		"MaxKey":         func(l *streams.Limits) { l.MaxKey = d.MaxKey },
		"MaxBody":        func(l *streams.Limits) { l.MaxBody = d.MaxBody },
		"MaxID":          func(l *streams.Limits) { l.MaxID = d.MaxID },
		"MaxTopic":       func(l *streams.Limits) { l.MaxTopic = d.MaxTopic },
		"MaxHeaders":     func(l *streams.Limits) { l.MaxHeaders = d.MaxHeaders },
		"MaxHeaderKey":   func(l *streams.Limits) { l.MaxHeaderKey = d.MaxHeaderKey },
		"MaxHeaderValue": func(l *streams.Limits) { l.MaxHeaderValue = d.MaxHeaderValue },
		"MaxProp":        func(l *streams.Limits) { l.MaxProp = d.MaxProp },
	}
	for name, set := range setters {
		var l streams.Limits
		set(&l)
		if err := l.Check(fits()); err != nil {
			t.Errorf("a Limits with only %s set refused an ordinary message: %v -- resolve left another bound at zero", name, err)
		}
	}
	// And the zero Limits accepts it too, which is the whole point: a driver
	// that named no limits gets the defaults, not a refusal.
	var zero streams.Limits
	if err := zero.Check(fits()); err != nil {
		t.Errorf("the zero Limits refused an ordinary message: %v", err)
	}
	// A NEGATIVE bound is "use the default" as well, not "refuse everything".
	neg := streams.Limits{MaxKey: -1, MaxBody: -1, MaxID: -1, MaxTopic: -1, MaxHeaders: -1, MaxHeaderKey: -1, MaxHeaderValue: -1, MaxProp: -1}
	if err := neg.Check(fits()); err != nil {
		t.Errorf("a Limits of negative bounds refused an ordinary message: %v", err)
	}
	// A bound the caller DID set is honoured, so resolve is not simply
	// overwriting everything with the defaults.
	tight := streams.Limits{MaxBody: 2}
	if err := tight.Check(fits()); err == nil {
		t.Error("a MaxBody of 2 accepted a four-byte body; resolve overwrote the caller's bound")
	}
}

func TestLimitsCheckRefusesWhatTheLayoutCannotCarryBack(t *testing.T) {
	t.Parallel()
	// Check runs before any I/O and its refusal is what keeps an entry that the
	// layout would write and Decode could not read from ever reaching the
	// broker. The two refusals are different codes for a reason: too large is MessageTooLarge and a non-UTF-8 field is InvalidArgument, and a
	// driver maps them to different portable errors.
	if err := streams.DefaultLimits.Check(nil); err != streams.ErrNilMessage { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell these sentinels apart
		t.Errorf("Check(nil) = %v, want ErrNilMessage", err)
	}
	d := streams.DefaultLimits
	const bad = "\xff\xfe"
	for _, tc := range []struct {
		name string
		m    func() *mq.Message
		want error
	}{
		{"an over-long key", func() *mq.Message {
			return &mq.Message{Key: []byte(strings.Repeat("k", d.MaxKey+1))}
		}, streams.ErrUnrepresentable},
		{"an over-long body", func() *mq.Message {
			return &mq.Message{Body: []byte(strings.Repeat("b", d.MaxBody+1))}
		}, streams.ErrUnrepresentable},
		{"an over-long id", func() *mq.Message {
			return &mq.Message{ID: strings.Repeat("i", d.MaxID+1)}
		}, streams.ErrUnrepresentable},
		{"an over-long topic", func() *mq.Message {
			return &mq.Message{Topic: strings.Repeat("t", d.MaxTopic+1)}
		}, streams.ErrUnrepresentable},
		{"too many headers", func() *mq.Message {
			m := &mq.Message{}
			for i := range d.MaxHeaders + 1 {
				m.Headers.Add("h"+strconv.Itoa(i), []byte("v"))
			}
			return m
		}, streams.ErrUnrepresentable},
		{"an over-long header key", func() *mq.Message {
			m := &mq.Message{}
			m.Headers.Add(strings.Repeat("k", d.MaxHeaderKey+1), []byte("v"))
			return m
		}, streams.ErrUnrepresentable},
		{"an over-long header value", func() *mq.Message {
			m := &mq.Message{}
			m.Headers.Add("k", []byte(strings.Repeat("v", d.MaxHeaderValue+1)))
			return m
		}, streams.ErrUnrepresentable},
		{"a non-UTF-8 id", func() *mq.Message {
			return &mq.Message{ID: bad}
		}, streams.ErrNonUTF8Field},
		{"a non-UTF-8 header key", func() *mq.Message {
			m := &mq.Message{}
			m.Headers.Add(bad, []byte("v"))
			return m
		}, streams.ErrNonUTF8Field},
		// Each property in turn, because they are checked in a loop over a
		// fixed array and a property left out of that array is a property
		// nothing bounds.
		{"an over-long content type", func() *mq.Message {
			m := &mq.Message{}
			m.Properties.ContentType = strings.Repeat("c", d.MaxProp+1)
			return m
		}, streams.ErrUnrepresentable},
		{"an over-long content encoding", func() *mq.Message {
			m := &mq.Message{}
			m.Properties.ContentEncoding = strings.Repeat("c", d.MaxProp+1)
			return m
		}, streams.ErrUnrepresentable},
		{"an over-long correlation id", func() *mq.Message {
			m := &mq.Message{}
			m.Properties.CorrelationID = strings.Repeat("c", d.MaxProp+1)
			return m
		}, streams.ErrUnrepresentable},
		{"an over-long causation id", func() *mq.Message {
			m := &mq.Message{}
			m.Properties.CausationID = strings.Repeat("c", d.MaxProp+1)
			return m
		}, streams.ErrUnrepresentable},
		{"an over-long reply-to", func() *mq.Message {
			m := &mq.Message{}
			m.Properties.ReplyTo = strings.Repeat("c", d.MaxProp+1)
			return m
		}, streams.ErrUnrepresentable},
		{"a non-UTF-8 content type", func() *mq.Message {
			m := &mq.Message{}
			m.Properties.ContentType = bad
			return m
		}, streams.ErrNonUTF8Field},
		{"a non-UTF-8 causation id", func() *mq.Message {
			m := &mq.Message{}
			m.Properties.CausationID = bad
			return m
		}, streams.ErrNonUTF8Field},
		{"a non-UTF-8 reply-to", func() *mq.Message {
			m := &mq.Message{}
			m.Properties.ReplyTo = bad
			return m
		}, streams.ErrNonUTF8Field},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wantErr(t, d.Check(tc.m()), tc.want, "Check")
		})
	}
	// A message exactly AT each bound is accepted: an off-by-one in the
	// comparison would refuse the largest message the layout can carry, which
	// no test of an over-long one can see.
	atBound := &mq.Message{
		Key:   []byte(strings.Repeat("k", d.MaxKey)),
		Body:  []byte(strings.Repeat("b", d.MaxBody)),
		ID:    strings.Repeat("i", d.MaxID),
		Topic: strings.Repeat("t", d.MaxTopic),
	}
	atBound.Properties.ContentType = strings.Repeat("c", d.MaxProp)
	atBound.Headers.Add(strings.Repeat("k", d.MaxHeaderKey), []byte(strings.Repeat("v", d.MaxHeaderValue)))
	if err := d.Check(atBound); err != nil {
		t.Errorf("a message exactly at every bound was refused: %v", err)
	}
}

func TestTheBookkeepingAccessorsAnswerZeroForAnEntryThatCarriesNothing(t *testing.T) {
	t.Parallel()
	// Attempt's zero is the API's "the broker does not count
	// attempts", and it is the honest answer for an entry that was published
	// rather than re-added. So an ABSENT field and an UNREADABLE one must both
	// be zero rather than an error the caller has to handle -- and neither may
	// come back as some other number, because the attempt count is what
	// MaxAttempts dead-letters on.
	for _, tc := range []struct {
		name   string
		fields []string
		want   uint32
	}{
		{"no fields at all", nil, 0},
		{"no attempt field", []string{"b", "body", "k", "key"}, 0},
		{"an attempt of three", []string{streams.FieldAttempt, "3"}, 3},
		{"an attempt of zero", []string{streams.FieldAttempt, "0"}, 0},
		{"the largest attempt", []string{streams.FieldAttempt, "4294967295"}, 4294967295},
		{"an attempt that overflows uint32", []string{streams.FieldAttempt, "4294967296"}, 0},
		// 2^32+1 rather than 2^32: a parser that read the value as a uint64 and
		// then narrowed it would TRUNCATE this to 1 and report attempt one for a
		// message that has been redelivered four billion times, restarting the
		// MaxAttempts accounting. 2^32 alone truncates to 0, which is the same
		// answer the refusal gives, so it cannot see the difference.
		{"an attempt one past the uint32 ceiling", []string{streams.FieldAttempt, "4294967297"}, 0},
		{"an attempt far past the uint32 ceiling", []string{streams.FieldAttempt, "8589934593"}, 0},
		{"an attempt that is not a number", []string{streams.FieldAttempt, "many"}, 0},
		{"a negative attempt", []string{streams.FieldAttempt, "-1"}, 0},
		{"an empty attempt", []string{streams.FieldAttempt, ""}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := streams.Attempt(tc.fields); got != tc.want {
				t.Errorf("Attempt(%q) = %d, want %d", tc.fields, got, tc.want)
			}
		})
	}
	// The packed spelling is "<seconds>,<nanoseconds>" since the epoch, written
	// out here rather than produced by the package's own formatter: a test that
	// round-trips a value through the same two functions cannot see them agree
	// on the WRONG encoding, and this one is what is already in every parked
	// entry in a live sorted set.
	when := time.Date(2026, 9, 28, 12, 0, 0, 500, time.UTC)
	packed := []string{
		streams.FieldDeliverAt,
		strconv.FormatInt(when.Unix(), 10) + "," + strconv.Itoa(when.Nanosecond()),
	}
	if got := streams.DeliverAt(packed); !got.Equal(when) {
		t.Errorf("DeliverAt = %v, want %v", got, when)
	}
	for _, tc := range []struct {
		name   string
		fields []string
	}{
		{"no fields at all", nil},
		{"no deliver-at field", []string{"b", "body"}},
		{"a deliver-at with no comma", []string{streams.FieldDeliverAt, "1700000000"}},
		{"a deliver-at whose seconds are not a number", []string{streams.FieldDeliverAt, "x,0"}},
		{"a deliver-at whose nanoseconds are not a number", []string{streams.FieldDeliverAt, "1,x"}},
		{"a deliver-at with negative nanoseconds", []string{streams.FieldDeliverAt, "1,-1"}},
		{"a deliver-at with a whole second of nanoseconds", []string{streams.FieldDeliverAt, "1,1000000000"}},
		{"an empty deliver-at", []string{streams.FieldDeliverAt, ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// The zero time is the honest "this entry carries no release time";
			// a garbage time would park the entry until the heat death of the
			// universe or release it immediately, and neither is what the
			// writer meant.
			//
			// DeliverAt's own `if err != nil { return time.Time{} }` is
			// REDUNDANT rather than load-bearing: parseTime already answers the
			// zero time alongside its error, so deleting the guard leaves every
			// case here passing. These cases pin the ANSWER, which is what a
			// caller sees; they do not pin that guard.
			if got := streams.DeliverAt(tc.fields); !got.IsZero() {
				t.Errorf("DeliverAt(%q) = %v, want the zero time", tc.fields, got)
			}
		})
	}
}

func TestInternalOnlyReadsTheBookkeepingNamespace(t *testing.T) {
	t.Parallel()
	// The bookkeeping fields are not message content and Decode does not
	// deliver them. Internal is the only way back to one, and it refuses a name
	// outside the namespace: reading "b" (the body) through it would hand a
	// strategy the application's payload as if it were the strategy's own
	// bookkeeping.
	fields := []string{
		"b", "the-body",
		streams.FieldAttempt, "2",
		streams.FieldPrefixInternal + "custom", "mine",
	}
	for _, tc := range []struct {
		name    string
		key     string
		want    string
		wantHit bool
	}{
		{"a bookkeeping field that is present", streams.FieldAttempt, "2", true},
		{"another bookkeeping field", streams.FieldPrefixInternal + "custom", "mine", true},
		{"a bookkeeping field that is absent", streams.FieldPrefixInternal + "absent", "", false},
		{"the body, which is not bookkeeping", "b", "", false},
		{"a header field", streams.FieldPrefixHeader + "0", "", false},
		{"a property field", streams.FieldPrefixProp + "0", "", false},
		{"an empty name", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, hit := streams.Internal(fields, tc.key)
			if hit != tc.wantHit || got != tc.want {
				t.Errorf("Internal(%q) = (%q, %v), want (%q, %v)", tc.key, got, hit, tc.want, tc.wantHit)
			}
		})
	}
	// An odd-length field list must not run off the end: a truncated reply is
	// the shape this reads, and a name in the last position has no value.
	if got, hit := streams.Internal([]string{streams.FieldAttempt}, streams.FieldAttempt); hit {
		t.Errorf("Internal on a truncated list = (%q, true), want a miss", got)
	}
}

func TestDecodeRefusesAnEntryItCannotRead(t *testing.T) {
	t.Parallel()
	if err := streams.Decode(nil, []string{"b", "x"}); err != streams.ErrNilMessage { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell these sentinels apart
		t.Errorf("Decode(nil) = %v, want ErrNilMessage", err)
	}
	// An odd field count is a truncated reply: the last name has no value, and
	// guessing one would deliver a message with a field the writer never set.
	var m mq.Message
	if err := streams.Decode(&m, []string{"b", "body", "k"}); err != streams.ErrCorruptEntry { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell these sentinels apart
		t.Errorf("Decode(odd list) = %v, want ErrCorruptEntry", err)
	}
	// A header or property index that is not a number is corruption, and one
	// this driver does not KNOW is a newer driver's field, which is ignored.
	// Those two must not be conflated: refusing the second would make every
	// message a newer publisher wrote undeliverable.
	for _, tc := range []struct {
		name   string
		fields []string
		want   error
	}{
		{"a header index that is not a number", []string{streams.FieldPrefixHeader + "x", "v"}, streams.ErrCorruptEntry},
		{"a property index that is not a number", []string{streams.FieldPrefixProp + "x", "v"}, streams.ErrCorruptEntry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var m mq.Message
			wantErr(t, streams.Decode(&m, tc.fields), tc.want, "Decode")
		})
	}
	// A property index past the ones this driver knows is skipped, and the rest
	// of the entry still decodes.
	var newer mq.Message
	if err := streams.Decode(&newer, []string{
		"b", "body",
		streams.FieldPrefixProp + "99", "from-a-newer-driver",
		"unknown-name", "also-ignored",
	}); err != nil {
		t.Fatalf("Decode of a newer driver's entry: %v", err)
	}
	if string(newer.Body) != "body" {
		t.Errorf("body = %q, want body: an unknown field stopped the decode", newer.Body)
	}
}

func TestManyHeadersRoundTripPastThePrecomputedNameTable(t *testing.T) {
	t.Parallel()
	// The field names are precomputed for the first 64 headers and BUILT by
	// concatenation beyond that. The two spellings have to be identical or a
	// message with 65 headers is written under one set of names and read back
	// looking for another -- and every header past the table would be lost with
	// no error anywhere, because an unknown field name is deliberately ignored.
	//
	// So the assertion is a round trip over a count that straddles the boundary.
	const n = 70
	m := &mq.Message{Body: []byte("body"), Topic: "t"}
	for i := range n {
		m.Headers.Add("h"+strconv.Itoa(i), []byte("v"+strconv.Itoa(i)))
	}
	if err := streams.DefaultLimits.Check(m); err != nil {
		t.Fatalf("a %d-header message is over the default limits: %v", n, err)
	}
	fields := streams.Encode(nil, m)
	var back mq.Message
	if err := streams.Decode(&back, fields); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if back.Headers.Len() != n {
		t.Fatalf("decoded %d headers, want %d: the names past the precomputed table did not match", back.Headers.Len(), n)
	}
	for i := range n {
		key := "h" + strconv.Itoa(i)
		got, ok := back.Headers.Get(key)
		if !ok {
			t.Errorf("header %q is missing after the round trip", key)
			continue
		}
		if string(got) != "v"+strconv.Itoa(i) {
			t.Errorf("header %q = %q, want %q", key, got, "v"+strconv.Itoa(i))
		}
	}
}

func TestPropNamesAnswerBeyondTheEnumeration(t *testing.T) {
	t.Parallel()
	// A property beyond the enumeration renders as its index rather than
	// panicking or as an empty name: an empty field name would collide with
	// every other empty name in the entry.
	for _, p := range []streams.Prop{streams.Prop(200), streams.Prop(255)} {
		if p.Valid() {
			t.Errorf("Prop(%d).Valid() is true", p)
		}
		want := streams.FieldPrefixProp + strconv.Itoa(int(p))
		if got := p.String(); got != want {
			t.Errorf("Prop(%d).String() = %q, want %q", p, got, want)
		}
	}
	// And every valid property's name is the prefix and its own index, which is
	// what makes setProp's switch and the name table agree.
	for i := range 8 {
		p := streams.Prop(i)
		if !p.Valid() {
			break
		}
		want := streams.FieldPrefixProp + strconv.Itoa(i)
		if got := p.String(); got != want {
			t.Errorf("Prop(%d).String() = %q, want %q", i, got, want)
		}
	}
}
