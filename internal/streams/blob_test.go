package streams_test

import (
	"bytes"
	"runtime"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
)

// blobMessages are the messages the two encoders are held to. Every one of
// them is a case where the body is the interesting part: absent, present and
// empty, small, and large enough that a per-byte copy is the dominant cost.
var blobMessages = []struct {
	name string
	m    *mq.Message
}{
	{"no-body", &mq.Message{Topic: "acme.orders", Key: []byte("k")}},
	{"empty-body", &mq.Message{Topic: "acme.orders", Body: []byte{}}},
	{"small-body", &mq.Message{Topic: "acme.orders", Body: []byte("payload")}},
	{"binary-body", &mq.Message{Topic: "acme.orders", Body: []byte{0, 1, 0xff, '\r', '\n', 0}}},
	{"nothing-at-all", &mq.Message{}},
}

// bodyOf returns a message with an n-byte body and a couple of other fields,
// so that the body is not the only token in the entry.
func bodyOf(n byte, size int) *mq.Message {
	m := &mq.Message{
		Topic:      "acme.orders",
		Key:        []byte("order-4711"),
		Body:       bytes.Repeat([]byte{n}, size),
		Properties: mq.Properties{ContentType: "application/json"},
	}
	m.Headers.Set("content-kind", []byte("order"))
	return m
}

// TestEncodeCopiesTheBodyAndEncodeAliasedDoesNot is the whole as a check.
//
// The two halves are one test because each is the other's control: a change
// that made [streams.Encode] alias would break for every client, and a
// change that made [streams.EncodeAliased] copy would silently restore the
// cost this increment exists to remove with nothing else to notice. Both are
// asked behaviourally -- mutate the caller's body and look at the token --
// which is the property that matters rather than the mechanism.
func TestEncodeCopiesTheBodyAndEncodeAliasedDoesNot(t *testing.T) {
	m := bodyOf('a', 64)

	fields := streams.Encode(nil, m)
	if fields[0] != streams.FieldBody {
		t.Fatalf("Encode put %q first, want the body field %q", fields[0], streams.FieldBody)
	}
	before := fields[1]
	m.Body[0] = 'z'
	if fields[1] != before {
		t.Errorf("Encode's body token changed when the caller mutated its slice: the specification lets the " +
			"caller reuse the buffer the instant the command is built, so the token must be a copy")
	}
	if fields[1][0] != 'a' {
		t.Errorf("Encode's body token starts %q, want 'a': it must hold the bytes as they were "+
			"when the command was built", fields[1][0])
	}

	m = bodyOf('a', 64)
	fields, blobs := streams.EncodeAliased(nil, nil, m)
	if len(blobs) != 1 {
		t.Fatalf("EncodeAliased produced %d blobs for a message with a body, want exactly 1", len(blobs))
	}
	if blobs[0].At != 1 || fields[0] != streams.FieldBody {
		t.Fatalf("EncodeAliased put the blob at token %d behind %q, want token 1 behind the body field",
			blobs[0].At, fields[0])
	}
	if fields[blobs[0].At] != "" {
		t.Errorf("EncodeAliased left %q in the blob's token slot, want the empty string: a reader "+
			"that ignores the blob set must send a visibly absent token, not a plausible wrong one",
			fields[blobs[0].At])
	}
	// The proof that nothing was copied is that the blob and the message
	// share one array. It is asked with a pointer comparison rather than with
	// `unsafe`, which the specification keeps out of this repository: two slices whose
	// first elements have the same address are the same storage.
	if &blobs[0].Bytes[0] != &m.Body[0] {
		t.Errorf("EncodeAliased's blob does not point at the caller's array, so the body was " +
			"copied after all: that is the copy this path exists to remove")
	}
	m.Body[0] = 'z'
	if blobs[0].Bytes[0] != 'z' {
		t.Errorf("EncodeAliased's blob did not see the caller's mutation, so it holds a copy")
	}
}

// TestEncodeAliasedMatchesEncode is what makes the fast path an optimisation
// rather than a second entry layout.
//
// Materialize is the seam every reader that wants strings goes through, so
// "the same command" means: the same tokens, byte for byte, after it. A
// divergence here is an entry that decodes differently depending on which
// client library published it, which is the one thing `internal/streams`
// exists to prevent.
func TestEncodeAliasedMatchesEncode(t *testing.T) {
	cases := append([]struct {
		name string
		m    *mq.Message
	}{}, blobMessages...)
	cases = append(cases, struct {
		name string
		m    *mq.Message
	}{"full", bodyOf('p', 1024)})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plain := streams.XAdd(streams.XAddArgs{
				Key: "{acme.orders}:0", ID: streams.IDAuto, MaxLen: 1000,
				Fields: streams.Encode(nil, tc.m),
			})
			f, b := streams.EncodeAliased(nil, nil, tc.m)
			aliased := streams.XAdd(streams.XAddArgs{
				Key: "{acme.orders}:0", ID: streams.IDAuto, MaxLen: 1000,
				Fields: f, FieldBlobs: b,
			})
			if !aliased.Valid() {
				t.Fatalf("the aliased command is not Valid: tokens %q blobs %v", aliased.Tokens, aliased.Blobs)
			}
			got := aliased.Materialize()
			if len(got.Blobs) != 0 {
				t.Errorf("Materialize left %d blobs behind", len(got.Blobs))
			}
			if len(got.Tokens) != len(plain.Tokens) {
				t.Fatalf("materialised %d tokens, Encode produced %d:\n got %q\nwant %q",
					len(got.Tokens), len(plain.Tokens), got.Tokens, plain.Tokens)
			}
			for i := range got.Tokens {
				if got.Tokens[i] != plain.Tokens[i] {
					t.Errorf("token %d is %q materialised and %q from Encode", i, got.Tokens[i], plain.Tokens[i])
				}
			}
			// And the same answer through the per-token accessor, which is
			// what the rueidis adapter reads: a Text that fell back to the
			// empty placeholder would publish an empty body.
			for i := range aliased.Tokens {
				if aliased.Text(i) != plain.Tokens[i] {
					t.Errorf("Text(%d) = %q, want %q", i, aliased.Text(i), plain.Tokens[i])
				}
			}
		})
	}
}

// TestCmdArgSeparatesTextFromBytes pins the accessor a blob-capable commander
// reads.
func TestCmdArgSeparatesTextFromBytes(t *testing.T) {
	body := []byte("payload")
	c := streams.Cmd{
		Tokens:   []string{"XADD", "{t}:0", "*", "b", "", "k", "key"},
		KeyStart: 1, KeyCount: 1,
		Blobs: []streams.Blob{{At: 4, Bytes: body}},
	}
	if !c.Valid() {
		t.Fatalf("the command is not Valid: %q %v", c.Tokens, c.Blobs)
	}
	if text, blob := c.Arg(4); text != "" || !bytes.Equal(blob, body) {
		t.Errorf("Arg(4) = (%q, %q), want (\"\", %q)", text, blob, body)
	}
	if &mustBlob(t, c, 4)[0] != &body[0] {
		t.Errorf("Arg(4) returned a copy of the bytes, not the caller's array")
	}
	if text, blob := c.Arg(6); text != "key" || blob != nil {
		t.Errorf("Arg(6) = (%q, %q), want (\"key\", nil): a non-blob token has no bytes", text, blob)
	}
	// A blob with no bytes needs no sentinel: the token's slot is the empty
	// string, so a commander that sees a nil blob and writes the text writes
	// the same empty bulk string. Both halves are asserted, because the
	// equivalence is what lets Arg return two values instead of three and is
	// what keeps the escaping empty-slice literal out of the publish path.
	empty := streams.Cmd{
		Tokens: []string{"XADD", "{t}:0", "*", "b", ""}, KeyStart: 1, KeyCount: 1,
		Blobs: []streams.Blob{{At: 4}},
	}
	if text, blob := empty.Arg(4); text != "" || blob != nil {
		t.Errorf("Arg on an empty blob = (%q, %v), want (\"\", nil)", text, blob)
	}
	if got := empty.Text(4); got != "" {
		t.Errorf("Text on an empty blob = %q, want the empty string", got)
	}
	if got := empty.Materialize().Tokens[4]; got != "" {
		t.Errorf("Materialize of an empty blob = %q, want the empty string", got)
	}
	// A zero-length body that the caller passed as a non-nil slice keeps its
	// blob, and the wire bytes are the same either way.
	zero, zb := streams.EncodeAliased(nil, nil, &mq.Message{Body: []byte{}})
	if len(zb) != 1 || zb[0].Bytes == nil || len(zb[0].Bytes) != 0 {
		t.Errorf("EncodeAliased of an empty non-nil body produced blobs %v, want one empty non-nil", zb)
	}
	if zero[0] != streams.FieldBody || zero[1] != "" {
		t.Errorf("EncodeAliased of an empty body produced %q, want the body field and an empty slot", zero)
	}
}

func mustBlob(t *testing.T, c streams.Cmd, i int) []byte {
	t.Helper()
	_, b := c.Arg(i)
	if b == nil {
		t.Fatalf("token %d is not a blob", i)
	}
	return b
}

// TestCmdValidRejectsAMalformedBlobSet is the guard that keeps a blob from
// becoming a silently empty token.
//
// Every row is a way the pairing between Tokens and Blobs can come apart, and
// every one of them publishes an entry with a wrong body rather than failing
// if it is let through. Valid is where the two client adapters and the
// in-memory server all check, so it is where the refusal belongs.
func TestCmdValidRejectsAMalformedBlobSet(t *testing.T) {
	base := func() streams.Cmd {
		return streams.Cmd{
			Tokens:   []string{"XADD", "{t}:0", "*", "b", "", "h:0", ""},
			KeyStart: 1, KeyCount: 1,
			Blobs: []streams.Blob{{At: 4, Bytes: []byte("x")}, {At: 6, Bytes: []byte("y")}},
		}
	}
	if !base().Valid() {
		t.Fatalf("the base command is not Valid, so every row below proves nothing")
	}
	for _, tc := range []struct {
		name   string
		mangle func(*streams.Cmd)
	}{
		// The verb keeps its text here deliberately: an empty Tokens[0] is
		// already refused by Valid's first line, so a row that emptied it
		// would pass whatever blobsValid did.
		{"verb-is-a-blob", func(c *streams.Cmd) { c.Blobs = []streams.Blob{{At: 0, Bytes: []byte("XADD")}} }},
		{"index-past-the-end", func(c *streams.Cmd) { c.Blobs[1].At = len(c.Tokens) }},
		{"negative-index", func(c *streams.Cmd) { c.Blobs[0].At = -1 }},
		{"out-of-order", func(c *streams.Cmd) { c.Blobs[0].At, c.Blobs[1].At = 6, 4 }},
		{"duplicate-index", func(c *streams.Cmd) { c.Blobs[1].At = 4 }},
		{"token-slot-not-empty", func(c *streams.Cmd) { c.Tokens[4] = "payload" }},
		{"blob-on-a-key", func(c *streams.Cmd) { c.Tokens[1] = ""; c.Blobs[0].At = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mangle(&c)
			if c.Valid() {
				t.Errorf("Valid accepted a command whose blob set is %s: tokens %q blobs %v",
					tc.name, c.Tokens, c.Blobs)
			}
		})
	}
}

// TestAllocsEncoding is the load-insensitive half of the evidence.
//
// The difference is exactly one allocation -- the `string(m.Body)` the aliased
// path does not make -- and that one allocation is len(Body) bytes wide, which
// is why the breach it causes grows with the payload. The
// assertion is on the DIFFERENCE rather than on either count, because the rest
// of the entry layout allocates per field and that is not what this measures.
//
// It is named for `^TestAllocs`, which is the pattern `make allocs` runs, and
// it SKIPS under the race detector. Both halves matter: an allocation
// assertion that does not match that pattern is asserted only by
// `make test-race`, where the detector's own shadow allocations move every
// figure it reads -- which is the shape with the sign
// reversed, and eight modules' budgets were once asserted by a test that could
// only run where its numbers were wrong.
func TestAllocsEncoding(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector perturbs allocation counts; `make allocs` runs without it")
	}
	m := bodyOf('a', 64<<10)
	var (
		fields []string
		blobs  []streams.Blob
	)
	fields = streams.Encode(fields[:0], m)
	fields, blobs = streams.EncodeAliased(fields[:0], blobs[:0], m)
	_ = blobs

	copied := int(testing.AllocsPerRun(200, func() {
		fields = streams.Encode(fields[:0], m)
	}))
	aliased := int(testing.AllocsPerRun(200, func() {
		fields, blobs = streams.EncodeAliased(fields[:0], blobs[:0], m)
	}))
	if copied-aliased != 1 {
		t.Errorf("Encode allocates %d times and EncodeAliased %d, a difference of %d: want exactly 1, "+
			"the %d-byte string conversion of the body", copied, aliased, copied-aliased, len(m.Body))
	}
}

// bytesPerRun is the allocated BYTES per call of f, which is the figure the
// allocation COUNT cannot show.
//
// `testing.AllocsPerRun` counts allocations, and the body copy is one of them
// whether the body is eight bytes or eight megabytes — so a count is blind to
// the only thing that makes this defect grow with payload. TotalAlloc is
// cumulative and monotonic, so the difference over n calls divided by n is the
// per-call figure, and it is unaffected by when the collector runs.
//
// It is load-insensitive: it measures what this goroutine allocated, not how
// long anything took, so it is a number this host can produce while a
// benchmark campaign is running next to it.
func bytesPerRun(n int, f func()) uint64 {
	var before, after runtime.MemStats
	f()
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range n {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(n)
}

func TestAllocsEncodingBytes(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector perturbs allocated bytes; `make allocs` runs without it")
	}
	const size = 64 << 10
	m := bodyOf('a', size)
	var (
		fields []string
		blobs  []streams.Blob
	)
	copied := bytesPerRun(2000, func() {
		fields = streams.Encode(fields[:0], m)
	})
	aliased := bytesPerRun(2000, func() {
		fields, blobs = streams.EncodeAliased(fields[:0], blobs[:0], m)
	})
	if copied < size {
		t.Errorf("Encode allocates %d bytes per message for a %d-byte body, want at least the body: "+
			"this measurement has stopped seeing the copy", copied, size)
	}
	if aliased > size/16 {
		t.Errorf("EncodeAliased allocates %d bytes per message for a %d-byte body, want well under "+
			"a sixteenth of it: the body is still being copied", aliased, size)
	}
	t.Logf("a %d-byte body costs %d bytes per message copied and %d aliased", size, copied, aliased)
}
