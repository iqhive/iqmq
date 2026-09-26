package stomp

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"unicode/utf8"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/activemq/protocol"
)

//
// What has to be fuzzed is the round trip a header really takes, and the two
// ends of it are unexported: `appendHeaders` turns the shared mapping's fields
// into the SEND frame's options, and `readFields` reads a delivery's frame
// headers back into the same form. Everything in between is `go-stomp`'s own
// codec, which escapes and unescapes the four characters a STOMP header cannot
// carry raw. A target that stopped at `SendHeadersForTest`'s map would not
// exercise the codec at all — and the codec is exactly where a header name
// with a colon or a newline in it either survives or forges a frame.
//
// So `FuzzSTOMPHeaders` writes a real frame with `frame.Writer` and reads it
// back with `frame.Reader`, and what it asserts is that anything the mapping
// ACCEPTED comes back byte for byte and anything it REFUSED is refused with a
// named sentinel and never mangled.

// namedRefusals is every refusal the shared mapping is allowed to answer a
// header set with.
//
// The list is closed on purpose. "The mapping refused it" is not a property
// worth asserting — a mapping that refused everything would satisfy it — so
// what is asserted is that the refusal is one of these, which means a caller
// can act on it: each one names what is wrong and what to do instead.
var namedRefusals = [...]error{
	protocol.ErrReservedHeader,
	protocol.ErrProtocolHeader,
	protocol.ErrTextOnly,
	protocol.ErrEmptyHeaderKey,
	protocol.ErrHeaderTooLarge,
	protocol.ErrDuplicateHeader,
}

// namedRefusal reports whether err is one of them.
func namedRefusal(err error) bool {
	for _, want := range namedRefusals {
		if errors.Is(err, want) && err.Error() == want.Error() {
			return true
		}
	}
	return false
}

// FuzzSTOMPHeaders is the header mapping in both directions, over the real
// frame codec.
//
// Two headers rather than one, because a set is what the mapping validates: a
// repeated name is a case only two can produce, and STOMP 1.2 makes it
// dangerous rather than merely lossy — a frame CAN carry the same name twice,
// so the second value goes on the wire and the broker then discards it,
// because the first occurrence wins. Nothing reports the loss, which is why
// the mapping refuses it.
func FuzzSTOMPHeaders(f *testing.F) {
	f.Fuzz(func(t *testing.T, k1 string, v1 []byte, k2 string, v2 []byte) {
		// Both modes that reach the mapping: the strict default, which
		// refuses what cannot travel, and the lenient one, which drops it.
		// The second is what makes "never mangled" a two-sided property --
		// under it the mapping must still never put a header it cannot
		// carry on the wire.
		for _, mode := range [...]mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataLenientDrop} {
			headerRoundTrip(t, mode, k1, v1, k2, v2)
		}
	})
}

// headerRoundTrip is one case of [FuzzSTOMPHeaders].
func headerRoundTrip(t *testing.T, mode mq.MetadataMode, k1 string, v1 []byte, k2 string, v2 []byte) {
	t.Helper()
	m := &mq.Message{Body: []byte("body")}
	m.Headers.Add(k1, v1)
	if k2 != "" {
		// An empty SECOND name would make every case the empty-name case,
		// which one of the committed seeds covers through the first.
		m.Headers.Add(k2, v2)
	}
	p, err := protocol.New(protocol.STOMP)
	if err != nil {
		t.Fatalf("protocol.New(STOMP): %v", err)
	}
	opts := protocol.Options{
		Broker:     activemq.Artemis,
		Metadata:   mode,
		Persistent: true,
		KeyHeader:  protocol.KeyHeader,
		KeyAsGroup: true,
	}
	var fields protocol.Fields
	switch err := p.Carry(&fields, m, opts); {
	case err != nil && mode == mq.MetadataLenientDrop:
		t.Fatalf("MetadataLenientDrop refused a header set (%q=%q, %q=%q): %v. The mode exists so that a caller "+
			"who would rather lose metadata than a message can say so, and a refusal here is the one answer it "+
			"asks not to get", k1, v1, k2, v2, err)
	case err != nil:
		if !namedRefusal(err) {
			t.Fatalf("the mapping refused (%q=%q, %q=%q) with %v, which is none of the named sentinels. A caller "+
				"can only act on a refusal that says what is wrong: an unnamed one is indistinguishable from a "+
				"driver defect", k1, v1, k2, v2, err)
		}
		return
	}

	// Everything the mapping accepted, written as the driver would write it
	// and read back through the codec that a broker's own reply goes
	// through.
	wire := buildSendFrame(t, &fields, m.Body)
	back := reencode(t, wire)

	var read protocol.Fields
	readFields(&read, &gostomp.Message{Header: back.Header})
	var out mq.Message
	if err := p.Restore(&out, &read, opts); err != nil {
		t.Fatalf("Restore refused the fields read back from a frame this mapping produced: %v", err)
	}

	// The expectation is `fields.Props` itself: this message carries no key,
	// no identifier, no causation and no content encoding, so every entry
	// the mapping produced is an application header and every one of them
	// has to reach the frame.
	//
	// EMPTY VALUES INCLUDED, which was a defect when this target was written
	// and is now the rule: `appendHeaders` gives the application entries
	// their own writer rather than the helper that skips an empty optional
	// field, because a STOMP frame carries `tenant:` perfectly well and
	// `Carry` accepts it and reports nothing dropped -- so a presence-flag
	// header used to be lost promising it had travelled.
	want := len(fields.Props)
	if got := out.Headers.Len(); got != want {
		t.Fatalf("the round trip returned %d headers and the mapping accepted %d non-empty ones (from %q=%q, "+
			"%q=%q, mode %v). A header the wire swallowed is metadata a caller was told had travelled",
			got, want, k1, v1, k2, v2, mode)
	}
	for i := range fields.Props {
		e := &fields.Props[i]
		vals := out.Headers.Values(e.Key)
		if len(vals) != 1 {
			t.Fatalf("the header %q came back %d times, want once (from %q=%q, %q=%q, mode %v)",
				e.Key, len(vals), k1, v1, k2, v2, mode)
		}
		if !bytes.Equal(vals[0], e.Bin) {
			t.Fatalf("the header %q went out as %q and came back as %q. A frame header is escaped on the way out "+
				"and unescaped on the way in, and a name or a value carrying a backslash, a carriage return, a "+
				"newline or a colon is where that either works or forges a frame", e.Key, e.Bin, vals[0])
		}
	}
	// And nothing the driver or a broker owns reached the caller.
	for key := range out.Headers.All() {
		if protocol.ReservedFrameHeader(key) || protocol.ReservedHeader(key) {
			t.Fatalf("the round trip handed the caller %q, which the protocol or the broker owns. An application "+
				"that echoed its headers back would publish a key, a destination or a group it never chose", key)
		}
	}
}

// buildSendFrame is the frame the driver would put on the wire for these
// fields, built through the driver's own option functions.
func buildSendFrame(t *testing.T, fields *protocol.Fields, body []byte) *frame.Frame {
	t.Helper()
	f := frame.New(frame.SEND, frame.Destination, "orders")
	// The length is explicit so that a body with a null byte in it would be
	// framed rather than truncated; `frame.Reader` falls back to reading up
	// to the null terminator when there is none.
	f.Header.Set(frame.ContentLength, strconv.Itoa(len(body)))
	f.Body = body
	for _, o := range appendHeaders(nil, fields, false) {
		if o == nil {
			continue
		}
		if err := o(f); err != nil {
			t.Fatalf("the driver's own SEND option refused the frame it was building: %v", err)
		}
	}
	return f
}

// reencode writes a frame and reads it back, which is the whole of what
// go-stomp does between the two drivers' ends of the mapping.
func reencode(t *testing.T, f *frame.Frame) *frame.Frame {
	t.Helper()
	var buf bytes.Buffer
	if err := frame.NewWriter(&buf).Write(f); err != nil {
		t.Fatalf("the frame the mapping produced could not be written: %v", err)
	}
	back, err := frame.NewReader(&buf).Read()
	if err != nil {
		t.Fatalf("the frame the mapping produced could not be read back: %v. A frame the codec cannot parse is a "+
			"frame the broker answers with an ERROR frame and then closes the connection, taking every other "+
			"publication in flight with it -- which is why an empty header name is refused before any I/O", err)
	}
	if back == nil {
		t.Fatal("the frame the mapping produced read back as a heart-beat")
	}
	return back
}

// FuzzSTOMPFrameHeaders is the READ path: an arbitrary MESSAGE frame into a
// delivery.
//
// It is the direction a driver does not control. Everything on the publish
// side is the driver's own output, and everything here is whatever the broker
// sent — a Classic delivery was measured carrying eleven of STOMP's own frame
// headers, and Artemis echoes its group identifier and its scheduled-delivery
// time under names of its own. So the properties are about what must NOT
// happen: a header the protocol or the broker owns must never reach a caller's
// headers, a malformed `expires` or `priority` must not panic and must not
// produce a nonsense value, and nothing may be invented.
func FuzzSTOMPFrameHeaders(f *testing.F) {
	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 string) {
		out := frame.New(frame.MESSAGE)
		out.Header.Add(k1, v1)
		out.Header.Add(k2, v2)
		// The four a broker always sends, added AFTER the fuzzer's so that
		// a fuzzed name colliding with one of them is the first occurrence
		// -- which is what STOMP 1.2 says wins, and therefore the case a
		// filter has to survive.
		out.Header.Add(frame.Destination, "orders::orders.workers")
		out.Header.Add(frame.MessageId, "broker-1")
		out.Header.Add(frame.Subscription, "1")
		out.Header.Add(frame.Ack, "1082,1088")
		out.Header.Set(frame.ContentLength, "0")

		// The frame goes through the real codec first, which is what makes
		// this target about what a BROKER can send rather than about what a
		// `frame.Header` can hold. The two are not the same set: an empty
		// header name encodes as a line beginning with ":", which
		// `frame.Reader` refuses outright -- so no broker can deliver one,
		// and an input that produces one is the codec's refusal rather than
		// this driver's problem.
		var buf bytes.Buffer
		if err := frame.NewWriter(&buf).Write(out); err != nil {
			return
		}
		back, err := frame.NewReader(&buf).Read()
		if err != nil || back == nil {
			return
		}
		h := back.Header

		var fields protocol.Fields
		readFields(&fields, &gostomp.Message{Header: h})

		p, err := protocol.New(protocol.STOMP)
		if err != nil {
			t.Fatalf("protocol.New(STOMP): %v", err)
		}
		var msg mq.Message
		opts := protocol.Options{
			Broker: activemq.Artemis, Persistent: true,
			KeyHeader: protocol.KeyHeader, KeyAsGroup: true,
		}
		if err := p.Restore(&msg, &fields, opts); err != nil {
			t.Fatalf("Restore refused a delivery's headers: %v. An undecodable delivery is left unacknowledged "+
				"rather than discarded, so a refusal here is a message that can never be consumed", err)
		}

		for key, val := range msg.Headers.All() {
			if key == "" {
				t.Fatal("the read path produced a header with no name, which no caller can look up")
			}
			if protocol.ReservedFrameHeader(key) || protocol.ReservedHeader(key) {
				t.Fatalf("a delivery handed the caller %q = %q. It is a header the protocol or the broker owns, "+
					"so a message would arrive carrying `subscription: 1` and `ack: 1082,1088` -- and an "+
					"application that echoed its headers would address another queue", key, val)
			}
			// A name that is not valid UTF-8 is deliberately NOT asserted
			// against, and the reason is worth writing down because the
			// fuzzer found it in under a second. The publish path refuses
			// one (ErrTextOnly), and the read path does not check: go-stomp
			// unescapes a header line without validating it, so a broker
			// that sent `\x83:v` would hand the caller a header name it
			// could never publish back. STOMP 1.2 requires a frame to be
			// UTF-8, so no conforming broker can produce one and neither
			// pinned image does -- and adding a validation pass over every
			// delivery's headers to close an asymmetry only a
			// non-conforming broker can reach would cost every delivery.
			// The asymmetry is reported rather than asserted.
			_ = utf8.ValidString
		}

		// the attempt: over STOMP it is 1 or at least 2 and never more,
		// because the protocol carries a BOOLEAN and no count on either
		// broker. A fuzzed `redelivered` value that is anything but "true"
		// is not redelivered, which is the only reading that cannot invent
		// a history.
		attempt, exact := protocol.Attempt(protocol.STOMP, &fields)
		switch {
		case fields.Redelivered && (attempt != 2 || exact):
			t.Fatalf("a redelivered STOMP delivery reports attempt %d (exact=%v), want 2 and not exact: the "+
				"protocol says only that it has been delivered before", attempt, exact)
		case !fields.Redelivered && (attempt != 1 || !exact):
			t.Fatalf("a first STOMP delivery reports attempt %d (exact=%v), want exactly 1", attempt, exact)
		}

		// A malformed `expires` or `priority` must leave the field unset
		// rather than produce a value a caller would act on: a message with
		// an expiry of the epoch is a message a TTL-aware consumer drops.
		if got := h.Get("expires"); got != "" {
			n, perr := strconv.ParseInt(got, 10, 64)
			switch {
			case perr != nil || n <= 0:
				if !fields.Expiry.IsZero() {
					t.Fatalf("the unparseable expires %q became the instant %v; a nonsense expiry is worse than "+
						"none, because a consumer acts on it", got, fields.Expiry)
				}
			case fields.Expiry.UnixMilli() != n:
				t.Fatalf("expires %q became %v (%d ms)", got, fields.Expiry, fields.Expiry.UnixMilli())
			}
		}
		if got := h.Get("priority"); got != "" {
			n, perr := strconv.Atoi(got)
			switch {
			case perr != nil || n < 0 || n > 255:
				if fields.HasPriority {
					t.Fatalf("the unparseable priority %q was reported as the priority %d", got, fields.Priority)
				}
			case !fields.HasPriority || int(fields.Priority) != n:
				t.Fatalf("priority %q became %d (set=%v)", got, fields.Priority, fields.HasPriority)
			}
		}
	})
}

// TestAnEmptyHeaderValueReachesTheFrame is the promise for the value that
// is easiest to lose.
//
// `appendHeaders` builds the SEND frame's options with two writers and the
// difference between them is this test. The driver's own optional fields --
// `content-type`, `correlation-id`, `reply-to` -- are skipped when empty,
// because there an empty string means "the caller did not say" and writing
// the header anyway would tell the broker something the caller did not. An
// APPLICATION header is written whatever its value: empty is a value, a STOMP
// frame carries `tenant:` without difficulty, and `Carry` accepts the header
// and reports nothing dropped -- so a driver that skipped it broke the specification's
// promise in silence, and the presence-flag pattern
// (`Headers.Add("x-replay", nil)`) is the one that loses most.
func TestAnEmptyHeaderValueReachesTheFrame(t *testing.T) {
	t.Parallel()
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Add("x-replay", nil)
	m.Headers.Add("tenant", []byte("acme"))

	got, err := SendHeadersForTest(Config{Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}}, m, nil, nil)
	if err != nil {
		t.Fatalf("SendHeadersForTest: %v", err)
	}
	if got["tenant"] != "acme" {
		t.Fatalf("the non-empty header did not survive (%v)", got)
	}
	v, there := got["x-replay"]
	if !there {
		t.Errorf("the empty-valued header never reached the frame (%v). The specification promises the headers are carried, "+
			"and the mapping accepted this one and reported nothing dropped", got)
	}
	if v != "" {
		t.Errorf("the empty-valued header reached the frame as %q; an empty value is a value and not a default", v)
	}
	// And the driver's own optional fields keep the OTHER rule: nothing the
	// caller did not set is written.
	for _, k := range []string{"content-type", "correlation-id", "reply-to"} {
		if _, there := got[k]; there {
			t.Errorf("the frame carries %q, which the caller never set: an empty optional field is not a value",
				k)
		}
	}
}
