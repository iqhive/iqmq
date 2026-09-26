package wire_test

import (
	"bytes"
	"runtime"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/wire"
)

// corpus is the set of messages the round trip is checked over. It is chosen
// for the distinctions the envelope has to preserve rather than for variety:
// nil against empty, binary against text, duplicate keys, and the typed
// properties the specification maps to native fields where a backend has them.
func corpus() []*mq.Message {
	withHeaders := func(m *mq.Message, pairs ...any) *mq.Message {
		for i := 0; i+1 < len(pairs); i += 2 {
			k, _ := pairs[i].(string)
			v, _ := pairs[i+1].([]byte)
			m.Headers.Add(k, v)
		}
		return m
	}
	return []*mq.Message{
		{},
		{Body: []byte{}},
		{Key: []byte{}},
		{Key: []byte("cust-42"), Body: []byte("payload")},
		{Body: bytes.Repeat([]byte("x"), 4096)},
		{ID: "msg-1"},
		withHeaders(&mq.Message{}, "trace", []byte{0x00, 0xff, 0x7f, 0x80}),
		withHeaders(&mq.Message{}, "tag", []byte("a"), "tag", []byte("b")),
		withHeaders(&mq.Message{}, "empty", []byte{}),
		withHeaders(&mq.Message{}, "nil", nil),
		{Properties: mq.Properties{ContentType: "application/json"}},
		{Properties: mq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			CorrelationID:   "corr-1",
			CausationID:     "cause-1",
			ReplyTo:         "replies",
			CreatedAt:       time.Unix(1767225600, 123456789).UTC(),
		}},
		withHeaders(&mq.Message{
			Key:        []byte("k"),
			Body:       []byte("b"),
			ID:         "id",
			Properties: mq.Properties{ContentType: "text/plain"},
		}, "a", []byte("1"), "b", []byte{0x00}, "a", []byte("2")),
	}
}

// equal reports whether two messages carry the same content, keeping the
// nil-versus-empty distinction the ownership contract makes observable.
func equal(a, b *mq.Message) bool {
	if (a.Key == nil) != (b.Key == nil) || !bytes.Equal(a.Key, b.Key) {
		return false
	}
	if (a.Body == nil) != (b.Body == nil) || !bytes.Equal(a.Body, b.Body) {
		return false
	}
	if a.ID != b.ID || a.Properties != b.Properties {
		return false
	}
	if a.Headers.Len() != b.Headers.Len() {
		return false
	}
	ak, av := pairs(a)
	bk, bv := pairs(b)
	for i := range ak {
		if ak[i] != bk[i] {
			return false
		}
		if (av[i] == nil) != (bv[i] == nil) || !bytes.Equal(av[i], bv[i]) {
			return false
		}
	}
	return true
}

// pairs returns a message's header keys and values in order.
func pairs(m *mq.Message) ([]string, [][]byte) {
	var ks []string
	var vs [][]byte
	for k, v := range m.Headers.All() {
		ks = append(ks, k)
		vs = append(vs, v)
	}
	return ks, vs
}

// TestRoundTrip is the envelope's whole contract: Decode of Encode reproduces
// the message, in both layouts, for every message in the corpus — including
// the nil-versus-empty distinctions.
func TestRoundTrip(t *testing.T) {
	for _, k := range wire.Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			l := wire.New(k)
			if l.Kind() != k {
				t.Fatalf("New(%v).Kind() is %v", k, l.Kind())
			}
			for i, m := range corpus() {
				size := l.Size(m)
				buf, err := l.Encode(nil, m)
				if err != nil {
					t.Fatalf("message %d: Encode: %v", i, err)
				}
				if len(buf) != size {
					t.Fatalf("message %d: Size said %d and Encode wrote %d", i, size, len(buf))
				}
				got, ok := wire.Detect(buf)
				if !ok || got != k {
					t.Fatalf("message %d: Detect returned (%v, %v), want (%v, true)", i, got, ok, k)
				}
				var back mq.Message
				if err := l.Decode(buf, &back, nil); err != nil {
					t.Fatalf("message %d: Decode: %v", i, err)
				}
				if !equal(m, &back) {
					t.Fatalf("message %d did not survive the round trip:\n got %+v\nwant %+v", i, &back, m)
				}
			}
		})
	}
}

// TestEncodeAppendsToACallerBuffer pins the specification's "0 allocs with caller buffer":
// Encode appends, and a buffer with room is not reallocated.
func TestEncodeAppendsToACallerBuffer(t *testing.T) {
	l := wire.New(wire.Auto)
	m := &mq.Message{Key: []byte("k"), Body: []byte("body")}
	prefix := []byte("keep-me")
	buf := make([]byte, 0, len(prefix)+l.Size(m)+16)
	buf = append(buf, prefix...)
	before := cap(buf)
	out, err := l.Encode(buf, m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if cap(out) != before {
		t.Fatalf("Encode grew a buffer that had room: cap %d, was %d", cap(out), before)
	}
	if !bytes.HasPrefix(out, prefix) {
		t.Fatalf("Encode overwrote the caller's prefix: %q", out)
	}
	var back mq.Message
	if err := l.Decode(out[len(prefix):], &back, nil); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !equal(m, &back) {
		t.Fatalf("round trip through an appended buffer: %+v", &back)
	}
}

// TestAllocs pins the allocation counts the specification states for this package.
func TestAllocs(t *testing.T) {
	l := wire.New(wire.Auto)
	m := &mq.Message{Key: []byte("cust-42"), Body: bytes.Repeat([]byte("x"), 1024)}
	m.Headers.Add("trace", []byte("abc"))
	m.Headers.Add("tenant", []byte("acme"))
	buf := make([]byte, 0, l.Size(m)+64)
	t.Run("Size", func(t *testing.T) {
		call := func() { _ = l.Size(m) }
		call()
		if n := testing.AllocsPerRun(1000, call); n != 0 {
			t.Fatalf("Size allocated %v times, want exactly 0", n)
		}
	})
	t.Run("Encode", func(t *testing.T) {
		// The specification: "wire encode/decode 1 KiB ≤ 150 / 200 ns, 0 allocs with
		// caller buffer". The scratch the message is converted through is
		// pooled and grows once, so a steady stream of encodings into a
		// caller's buffer allocates nothing at all.
		call := func() {
			out, err := l.Encode(buf[:0], m)
			if err != nil || len(out) == 0 {
				t.Fatalf("Encode = (%d bytes, %v)", len(out), err)
			}
		}
		call()
		if n := testing.AllocsPerRun(1000, call); n != 0 {
			t.Fatalf("Encode into a caller buffer allocated %v times, want exactly 0 ", n)
		}
	})
}

// TestDecodeRejectsCorruption pins the canonical-encoding half of the
// contract: one message has exactly one valid encoding per layout, and
// Decode refuses everything else with a code a caller can branch on.
func TestDecodeRejectsCorruption(t *testing.T) {
	l := wire.New(wire.Auto)
	m := &mq.Message{Key: []byte("k"), Body: []byte("body"), ID: "id"}
	m.Headers.Add("h", []byte("v"))
	good, err := l.Encode(nil, m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	cases := map[string][]byte{
		"empty":         {},
		"bad magic":     append([]byte("XXXX"), good[4:]...),
		"truncated":     good[:len(good)-1],
		"one byte":      good[:1],
		"trailing byte": append(bytes.Clone(good), 0),
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			var back mq.Message
			err := l.Decode(src, &back, nil)
			if err == nil {
				t.Fatalf("Decode accepted %d corrupt bytes", len(src))
			}
			switch mq.CodeOf(err) {
			case mq.CodeCorruptMessage, mq.CodeMessageTooLarge:
			default:
				t.Fatalf("Decode of corruption answered %v (code %v)", err, mq.CodeOf(err))
			}
		})
	}
	// A limit is what bounds what Decode will build, and it is explicit:
	// The specification asks every parser to have one rather than to imply one.
	var back mq.Message
	err = l.Decode(good, &back, &wire.Limits{MaxBody: 1})
	if mq.CodeOf(err) != mq.CodeMessageTooLarge {
		t.Fatalf("Decode with a 1-byte body limit answered %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestZeroCopyDecodeAliasesTheSource pins the borrowed decode mode: the
// message's bytes are the caller's buffer, which is cheap and only valid
// while the buffer is.
func TestZeroCopyDecodeAliasesTheSource(t *testing.T) {
	l := wire.New(wire.Auto)
	m := &mq.Message{Key: []byte("k"), Body: []byte("body")}
	buf, err := l.Encode(nil, m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var borrowed mq.Message
	if err := l.Decode(buf, &borrowed, &wire.Limits{ZeroCopy: true}); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !equal(m, &borrowed) {
		t.Fatalf("a zero-copy decode produced %+v", &borrowed)
	}
	if len(borrowed.Body) == 0 || !aliases(buf, borrowed.Body) {
		t.Fatal("a zero-copy decode copied the body instead of aliasing the source")
	}
	// A copying decode does not alias, which is the distinction the mode is
	// for.
	var copied mq.Message
	if err := l.Decode(buf, &copied, nil); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if aliases(buf, copied.Body) {
		t.Fatal("a copying decode aliased the source")
	}
}

// aliases reports whether b points into a.
func aliases(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return &b[0] == &a[0] || bytes.Contains(a, b) && overlaps(a, b)
}

// overlaps reports whether b's first element is inside a.
func overlaps(a, b []byte) bool {
	for i := range a {
		if &a[i] == &b[0] {
			return true
		}
	}
	return false
}

// TestKinds pins the enumeration: Auto resolves, every kind has a slug, and
// the slug round-trips.
func TestKinds(t *testing.T) {
	if !wire.Auto.Valid() {
		t.Fatal("Auto is not a valid kind")
	}
	if wire.Auto.Resolve() == wire.Auto {
		t.Fatal("Auto did not resolve to a concrete layout")
	}
	for _, k := range wire.Kinds() {
		if !k.Valid() {
			t.Fatalf("%v is not valid", k)
		}
		got, ok := wire.ParseKind(k.String())
		if !ok || got != k {
			t.Fatalf("ParseKind(%q) returned (%v, %v)", k.String(), got, ok)
		}
		if strings.Contains(k.String(), "?") {
			t.Fatalf("kind %d has no slug: %q", k, k.String())
		}
	}
	if _, ok := wire.ParseKind("nonsense"); ok {
		t.Fatal("ParseKind accepted a slug no layout has")
	}
	if _, ok := wire.Detect([]byte("not an envelope")); ok {
		t.Fatal("Detect claimed a layout for bytes that are not an envelope")
	}
	// Only TLV skips an unknown optional field, which is what makes it the
	// forward-compatible layout.
	if wire.Fixed.ForwardCompatible() {
		t.Fatal("the fixed layout claims forward compatibility")
	}
	if !wire.TLV.ForwardCompatible() {
		t.Fatal("the TLV layout does not claim forward compatibility")
	}
	// A kind that names no layout is a programming error, and New says so
	// rather than returning something that fails later.
	defer func() {
		if recover() == nil {
			t.Fatal("New accepted a kind that names no layout")
		}
	}()
	_ = wire.New(wire.Kind(99))
}

// TestNilMessageIsRefused pins the boundary: a nil destination or source is
// an argument failure rather than a panic.
func TestNilMessageIsRefused(t *testing.T) {
	l := wire.New(wire.Auto)
	if n := l.Size(nil); n != 0 {
		t.Fatalf("Size(nil) is %d, want 0", n)
	}
	if _, err := l.Encode(nil, nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Encode(nil): %v (code %v)", err, mq.CodeOf(err))
	}
	if err := l.Decode([]byte("x"), nil, nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Decode into nil: %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestDecodeReplacesEveryField pins the rule Decode states: every field of
// the destination is replaced, so a reused message holds nothing of the one
// before it.
func TestDecodeReplacesEveryField(t *testing.T) {
	l := wire.New(wire.Auto)
	first := &mq.Message{Key: []byte("first"), Body: []byte("one"), ID: "id-1"}
	first.Headers.Add("a", []byte("1"))
	second := &mq.Message{Body: []byte("two")}
	a, err := l.Encode(nil, first)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b, err := l.Encode(nil, second)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst mq.Message
	if err := l.Decode(a, &dst, nil); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := l.Decode(b, &dst, nil); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !equal(second, &dst) {
		t.Fatalf("a reused destination kept something of the first message: %+v", &dst)
	}
}

// TestEncodeIsAFunctionOfTheMessage pins what a pooled scratch nearly took
// away: Encode's output depends on the message and on nothing else.
//
// The scratch a message is converted through holds the header table, and the
// layouts distinguish a NIL header slice from an empty one — they distinguish
// nil from empty for every byte field, which is what the ownership contract
// makes observable. A pooled table that had been used for a message WITH
// headers is non-nil and empty when it is reused for one without, so the same
// message encoded to different bytes depending on what the pool had last held.
//
// The order of the two encodings is the whole test and it is why the runtime
// collection is here rather than being tidied away: a sync.Pool is emptied by
// a collection, so this is the only way to reach the state a fresh process
// reaches once and never again. Without it both encodings come out of a pool
// some earlier test already dirtied, they agree with each other, and the test
// passes for a broken Encode — which is exactly what the first version of it
// did.
func TestEncodeIsAFunctionOfTheMessage(t *testing.T) {
	withHeaders := &mq.Message{Body: []byte("b")}
	withHeaders.Headers.Add("a", []byte("1"))
	withHeaders.Headers.Add("b", []byte("2"))
	for _, k := range wire.Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			l := wire.New(k)
			for i, m := range corpus() {
				// Two collections empty the pool's live and victim caches,
				// so the next conversion starts from a scratch nothing has
				// used.
				runtime.GC()
				runtime.GC()
				clean, err := l.Encode(nil, m)
				if err != nil {
					t.Fatalf("message %d: Encode: %v", i, err)
				}
				cleanSize := l.Size(m)
				// Dirty the pooled scratch with a message that has headers,
				// so that the table the next conversion reuses has capacity.
				if _, err := l.Encode(nil, withHeaders); err != nil {
					t.Fatalf("Encode: %v", err)
				}
				dirty, err := l.Encode(nil, m)
				if err != nil {
					t.Fatalf("message %d: Encode: %v", i, err)
				}
				if !bytes.Equal(clean, dirty) {
					t.Fatalf("message %d encoded to %x out of a fresh scratch and to %x out of a pooled one: Encode depends on the pool rather than on the message",
						i, clean, dirty)
				}
				if got := l.Size(m); got != cleanSize || got != len(clean) {
					t.Fatalf("message %d: Size said %d out of a fresh scratch, %d out of a pooled one, and Encode wrote %d",
						i, cleanSize, got, len(clean))
				}
			}
		})
	}
}

// TestDetectFullIsTheWholeEnvelopeQuestion pins the public discriminator a
// driver on a header-less broker has to use.
//
// Detect reads six bytes and DetectFull reads the envelope, and the difference
// is the recorded MetadataAdaptive defect: a native publication whose body
// begins with the magic was claimed by the sniff and decoded as metadata.
func TestDetectFullIsTheWholeEnvelopeQuestion(t *testing.T) {
	t.Parallel()
	for _, k := range wire.Kinds() {
		env, err := wire.New(k).Encode(nil, &mq.Message{Key: []byte("k"), Body: []byte("body"), ID: "id-1"})
		if err != nil {
			t.Fatalf("%v: Encode: %v", k, err)
		}
		if got, ok := wire.DetectFull(env); !ok || got != k {
			t.Errorf("%v: DetectFull(%v, %v) refused a whole envelope", k, got, ok)
		}
		forgery := []byte("IQMQ\x11\x00 a native body that only begins like an envelope")
		if _, ok := wire.Detect(forgery); !ok {
			t.Fatal("the forgery does not pass the six-byte sniff, so it proves nothing")
		}
		if got, ok := wire.DetectFull(forgery); ok {
			t.Errorf("DetectFull claimed %v for a native body", got)
		}
	}
}
