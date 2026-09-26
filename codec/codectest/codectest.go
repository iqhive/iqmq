// Package codectest holds every codec in this repository to one contract.
//
// It exists because the failure contract for the append path is pinned in
// codec's own model test, over the two JSON kinds, in an internal test file
// that no other module can reach. The four codec modules outside the root
// (proto, msgpack, avro, cloudevents) are the first implementations of
// codec.Codec written against a format the root package does not know, and a
// contract each of them restates in its own words is four contracts.
//
// What Suite.Run checks, and why each check is here:
//
//   - ContentType is non-empty, which Codec's documentation promises.
//   - Round trip: Marshal then Unmarshal into a fresh target reproduces the
//     value.
//   - Determinism: two Marshal calls on one value produce identical bytes.
//     Dedup keys and content hashes depend on it, and it is what codec's own
//     JSON kinds are configured for.
//   - The append path produces exactly what Marshal produces, after the
//     caller's own prefix.
//   - **the pinned append failure contract**: a value the codec cannot
//     encode is an error that is NOT a *codec.DecodeError, the caller's buffer
//     comes back byte-for-byte untouched, and the next value appends at the
//     caller's own length. A publisher encodes into a pooled buffer it keeps
//     using, so a codec that returned a half-written slice would corrupt the
//     NEXT message rather than the failed one. Two of the four modules failed
//     this on their first implementation and one of them failed it because the
//     third-party library does: google.golang.org/protobuf's MarshalAppend
//     returns the partially encoded buffer alongside its error.
//   - A decode target that is not a non-nil pointer is codec.ErrNilTarget
//     (a programmer error, InvalidArgument) and not a decode failure.
//   - A body the codec cannot decode is a *codec.DecodeError, which carries
//     errcode.CorruptMessage, so a poison body is settled as the message's
//     fault rather than the caller's.
//   - Concurrency: Codec's documentation promises every implementation is safe
//     for concurrent use, and nothing else in the repository checks it.
//
// The suite is deliberately not a fuzz target and not a benchmark: each module
// owns those, because the interesting inputs are format-specific.
package codectest

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/iqhive/iqmq/codec"
)

// Case is one value the codec under test can encode and decode.
type Case struct {
	// Name is the subtest name.
	Name string
	// Value is the value to encode. It is whatever the codec's own
	// documentation says Marshal accepts — usually a pointer.
	Value any
	// NewTarget returns a fresh, non-nil decode target for Value.
	NewTarget func() any
	// Check reports whether the target NewTarget produced holds Value again
	// after a round trip. It returns nil when it does.
	Check func(target any) error
	// Wire, when non-empty, is the exact encoding Marshal must produce. It
	// pins a format rather than only a round trip, which is what catches a
	// library upgrade that changes the bytes.
	Wire []byte
}

// Suite is the contract one codec is held to.
type Suite struct {
	// Codec is the codec under test. It must implement codec.Appender: every
	// codec in this repository does, because the publisher's pooled-buffer
	// path needs it, and the append failure contract is the half of this
	// suite that has caught real defects.
	Codec codec.Codec
	// ContentType is the media type Codec must report.
	ContentType string
	// Cases are the values that round-trip. At least one is required, and the
	// first is the one the append and failure checks use as their "next
	// value".
	Cases []Case
	// Unencodable are values Marshal and AppendMarshal must reject. At least
	// one is required: it is what the append failure contract is checked
	// with.
	Unencodable []any
	// Corrupt are bodies Unmarshal must reject as a *codec.DecodeError. At
	// least one is required.
	Corrupt [][]byte
	// NotDeterministic waives the determinism check, with the reason. No
	// codec in this repository sets it; it exists so that a codec which
	// cannot be deterministic has to say so in one word rather than by
	// omitting a test.
	NotDeterministic string
}

// Run runs the contract. It is the whole of Suite's public surface.
func (s Suite) Run(t *testing.T) {
	t.Helper()
	if s.Codec == nil {
		t.Fatal("codectest: Suite.Codec is nil")
	}
	if len(s.Cases) == 0 {
		t.Fatal("codectest: Suite.Cases is empty, so the suite would check nothing")
	}
	if len(s.Unencodable) == 0 {
		t.Fatal("codectest: Suite.Unencodable is empty, so A20's append failure contract would not be checked")
	}
	if len(s.Corrupt) == 0 {
		t.Fatal("codectest: Suite.Corrupt is empty, so the *codec.DecodeError contract would not be checked")
	}
	t.Run("ContentType", s.contentType)
	t.Run("RoundTrip", s.roundTrip)
	t.Run("Determinism", s.determinism)
	t.Run("AppendPath", s.appendPath)
	t.Run("AppendFailure", s.appendFailure)
	t.Run("MarshalFailure", s.marshalFailure)
	t.Run("BadTarget", s.badTarget)
	t.Run("CorruptBody", s.corruptBody)
	t.Run("Concurrent", s.concurrent)
}

func (s Suite) contentType(t *testing.T) {
	got := s.Codec.ContentType()
	if got == "" {
		t.Fatal("ContentType() is empty; Codec's documentation says it never is")
	}
	if s.ContentType != "" && got != s.ContentType {
		t.Fatalf("ContentType() = %q, want %q", got, s.ContentType)
	}
}

func (s Suite) roundTrip(t *testing.T) {
	for _, c := range s.Cases {
		t.Run(c.Name, func(t *testing.T) {
			b, err := s.Codec.Marshal(c.Value)
			if err != nil {
				t.Fatalf("Marshal(%s): %v", c.Name, err)
			}
			if len(c.Wire) > 0 && !bytes.Equal(b, c.Wire) {
				t.Fatalf("Marshal(%s) = %x, want %x", c.Name, b, c.Wire)
			}
			target := c.NewTarget()
			if err := s.Codec.Unmarshal(b, target); err != nil {
				t.Fatalf("Unmarshal(%s): %v", c.Name, err)
			}
			if err := c.Check(target); err != nil {
				t.Fatalf("round trip of %s: %v", c.Name, err)
			}
		})
	}
}

func (s Suite) determinism(t *testing.T) {
	if s.NotDeterministic != "" {
		t.Skipf("waived: %s", s.NotDeterministic)
	}
	for _, c := range s.Cases {
		t.Run(c.Name, func(t *testing.T) {
			first, err := s.Codec.Marshal(c.Value)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			for i := range 8 {
				again, err := s.Codec.Marshal(c.Value)
				if err != nil {
					t.Fatalf("Marshal (call %d): %v", i+2, err)
				}
				if !bytes.Equal(first, again) {
					t.Fatalf("Marshal is not deterministic: %x then %x", first, again)
				}
			}
		})
	}
}

// appendPath checks that AppendMarshal produces exactly what Marshal does,
// after whatever the caller already had in the buffer.
func (s Suite) appendPath(t *testing.T) {
	a, ok := s.Codec.(codec.Appender)
	if !ok {
		t.Fatal("the codec does not implement codec.Appender; every codec in this repository does, because the publisher's pooled-buffer path needs it")
	}
	const prefix = "keep-me"
	for _, c := range s.Cases {
		t.Run(c.Name, func(t *testing.T) {
			want, err := s.Codec.Marshal(c.Value)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			dst := append(make([]byte, 0, 8+len(want)), prefix...)
			got, err := a.AppendMarshal(dst, c.Value)
			if err != nil {
				t.Fatalf("AppendMarshal: %v", err)
			}
			if !bytes.HasPrefix(got, []byte(prefix)) {
				t.Fatalf("AppendMarshal dropped the caller's prefix: %x", got)
			}
			if !bytes.Equal(got[len(prefix):], want) {
				t.Fatalf("AppendMarshal appended %x, want %x", got[len(prefix):], want)
			}
			// Appending into a nil buffer is the pooled buffer's first use.
			if got, err := a.AppendMarshal(nil, c.Value); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("AppendMarshal(nil) = %x, %v; want %x", got, err, want)
			}
		})
	}
}

// appendFailure is the pinned failure contract for the append path, restated
// over a codec the root package does not know. See the package comment.
func (s Suite) appendFailure(t *testing.T) {
	a := s.Codec.(codec.Appender)
	const prefix = "keep-me"
	for i, bad := range s.Unencodable {
		t.Run(name(i, bad), func(t *testing.T) {
			dst := append(make([]byte, 0, 256), prefix...)
			got, err := a.AppendMarshal(dst, bad)
			if err == nil {
				t.Fatalf("AppendMarshal(%T) succeeded, so it is not unencodable", bad)
			}
			var de *codec.DecodeError
			if errors.As(err, &de) {
				t.Fatalf("AppendMarshal(%T) = %v; an ENCODE failure is not a *codec.DecodeError", bad, err)
			}
			if string(got) != prefix {
				t.Fatalf("AppendMarshal(%T) = %q, want the caller's buffer %q back untouched: a publisher reuses this buffer, so a partial value corrupts the NEXT message",
					bad, got, prefix)
			}

			// The generic shape must not corrupt the buffer either. Adapt
			// hands the codec a *any, which is the indirection
			// codec.Indirect exists for.
			ta, ok := codec.Adapt[any](s.Codec).(codec.AppendingTyped[any])
			if !ok {
				t.Fatal("codec.Adapt did not carry the append path")
			}
			if got, err = ta.AppendEncode(dst, bad); err == nil || string(got) != prefix {
				t.Fatalf("AppendEncode(%T) = %q, %v; want an error and the caller's buffer", bad, got, err)
			}

			// The next value appends at the caller's own length, so the
			// failure left nothing behind.
			c := s.Cases[0]
			want, err := s.Codec.Marshal(c.Value)
			if err != nil {
				t.Fatalf("Marshal(%s): %v", c.Name, err)
			}
			got, err = a.AppendMarshal(dst, c.Value)
			if err != nil {
				t.Fatalf("AppendMarshal(%s) after a failure: %v", c.Name, err)
			}
			if string(got) != prefix+string(want) {
				t.Fatalf("AppendMarshal after a failure = %x, want %x", got, prefix+string(want))
			}
		})
	}
}

// marshalFailure checks that Marshal rejects the same values AppendMarshal
// does, and with an error that is not a decode error either.
func (s Suite) marshalFailure(t *testing.T) {
	for i, bad := range s.Unencodable {
		t.Run(name(i, bad), func(t *testing.T) {
			b, err := s.Codec.Marshal(bad)
			if err == nil {
				t.Fatalf("Marshal(%T) = %x, want an error", bad, b)
			}
			var de *codec.DecodeError
			if errors.As(err, &de) {
				t.Fatalf("Marshal(%T) = %v; an ENCODE failure is not a *codec.DecodeError", bad, err)
			}
		})
	}
}

// badTarget checks the programmer-error half of the error model.
//
// What it can assert is narrower than it looks, and the limit is stated here
// rather than discovered later: errcode.Error.Is matches any target carrying
// the same portable CODE, so errors.Is(err, codec.ErrNilTarget) is satisfied
// by any InvalidArgument error a codec chooses to return. That is the
// portable contract a caller depends on, and it is what this checks; pinning
// one specific sentinel needs an identity comparison, which each codec's own
// tests do.
func (s Suite) badTarget(t *testing.T) {
	body, err := s.Codec.Marshal(s.Cases[0].Value)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var nilPtr *int
	for _, target := range []any{nil, 42, "not a pointer", nilPtr} {
		err := s.Codec.Unmarshal(body, target)
		if !errors.Is(err, codec.ErrNilTarget) {
			t.Fatalf("Unmarshal into %#v = %v, want an InvalidArgument error such as codec.ErrNilTarget: "+
				"a bad target is the caller's bug, not a corrupt message", target, err)
		}
		var de *codec.DecodeError
		if errors.As(err, &de) {
			t.Fatalf("Unmarshal into %#v = %v, a *codec.DecodeError; a bad target is not a corrupt body", target, err)
		}
	}
}

func (s Suite) corruptBody(t *testing.T) {
	for i, body := range s.Corrupt {
		t.Run(name(i, body), func(t *testing.T) {
			target := s.Cases[0].NewTarget()
			err := s.Codec.Unmarshal(body, target)
			if err == nil {
				t.Fatalf("Unmarshal(%x) succeeded, so it is not corrupt", body)
			}
			var de *codec.DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("Unmarshal(%x) = %v (%T), want a *codec.DecodeError so the body is settled as the message's fault", body, err, err)
			}
			if de.ContentType != s.Codec.ContentType() {
				t.Fatalf("DecodeError.ContentType = %q, want %q", de.ContentType, s.Codec.ContentType())
			}
			if de.Err == nil {
				t.Fatal("DecodeError.Err is nil, so the decoder's own error was dropped and errors.As cannot reach it")
			}
		})
	}
}

// concurrent exercises Codec's documented promise that every implementation in
// this repository is safe for concurrent use. Run it under -race.
func (s Suite) concurrent(t *testing.T) {
	c := s.Cases[0]
	want, err := s.Codec.Marshal(c.Value)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	a := s.Codec.(codec.Appender)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 32 {
				b, err := s.Codec.Marshal(c.Value)
				if err != nil || !bytes.Equal(b, want) {
					t.Errorf("concurrent Marshal = %x, %v", b, err)
					return
				}
				if ab, err := a.AppendMarshal(make([]byte, 0, len(want)), c.Value); err != nil || !bytes.Equal(ab, want) {
					t.Errorf("concurrent AppendMarshal = %x, %v", ab, err)
					return
				}
				target := c.NewTarget()
				if err := s.Codec.Unmarshal(want, target); err != nil {
					t.Errorf("concurrent Unmarshal: %v", err)
					return
				}
				if err := c.Check(target); err != nil {
					t.Errorf("concurrent round trip: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// name builds a stable subtest name for an unnamed input.
func name(i int, v any) string {
	if b, ok := v.([]byte); ok {
		if len(b) == 0 {
			return "empty"
		}
		const hex = "0123456789abcdef"
		out := make([]byte, 0, 2*min(len(b), 12))
		for _, c := range b[:min(len(b), 12)] {
			out = append(out, hex[c>>4], hex[c&0xf])
		}
		return string(out)
	}
	return itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[n:])
}
