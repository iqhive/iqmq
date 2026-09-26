package compress_test

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/iqhive/iqmq/x/compress"
)

// fuzzLimit is the limit every fuzz target's decoder is built with. It is
// small so that a target which DID allocate on a declared size would run the
// worker out of memory quickly and visibly rather than slowly.
const fuzzLimit = 1 << 16

// FuzzParse: the frame header parser must never panic and must never return a
// payload that is not a suffix of its input.
//
// The second half is the property that matters and it is not obvious: Parse
// returns an aliasing slice, so a bug in the offset arithmetic would hand the
// caller bytes from inside the header -- a decoder would then decompress the
// declared size as if it were payload. Checking the identity of the slice
// rather than only its length is what catches an off-by-one there.
func FuzzParse(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("IQZ1"))
	f.Add([]byte("IQZ1\x00\x00\x00\x00\x00\x00"))
	f.Add(frame(compress.CodecGzip, 0, nil))
	f.Add(frame(compress.CodecStored, 5, []byte("hello")))
	f.Add(frame(compress.CodecStored, 0xFFFFFFFF, []byte("x")))
	f.Add(func() []byte { b := frame(compress.CodecStored, 1, []byte("x")); b[5] = 0x80; return b }())

	f.Fuzz(func(t *testing.T, body []byte) {
		h, payload, err := compress.Parse(body)
		if err != nil {
			if payload != nil {
				t.Fatalf("Parse failed with %v but returned a %d-byte payload", err, len(payload))
			}
			if !errors.Is(err, compress.ErrNotFramed) && !errors.Is(err, compress.ErrBadFlags) {
				t.Fatalf("Parse returned %v, which is neither ErrNotFramed nor ErrBadFlags", err)
			}
			return
		}
		if h.Flags != 0 {
			t.Fatalf("Parse accepted flags %#x", h.Flags)
		}
		if len(body) < compress.HeaderLen {
			t.Fatalf("Parse accepted a %d-byte body", len(body))
		}
		if want := body[compress.HeaderLen:]; !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q, want the %d bytes after the header", payload, len(want))
		}
		// The header round-trips: what Parse read is what AppendHeader
		// writes, so the two cannot drift.
		again := compress.AppendHeader(nil, h.Codec, h.Size)
		if !bytes.Equal(again, body[:compress.HeaderLen]) {
			t.Fatalf("AppendHeader(%+v) = %x, want the input's header %x", h, again, body[:compress.HeaderLen])
		}
	})
}

// FuzzDecode: no input decodes to more than the limit, and no input panics.
//
// This is the decompression-bomb property stated as a fuzz invariant rather
// than as one hand-built bomb, so that a coding whose reader has its own
// amplification -- a zlib dictionary, a flate stream with no header to be
// wrong -- is covered by the same rule.
func FuzzDecode(f *testing.F) {
	d, err := compress.NewDecoder(compress.Config{MaxDecompressed: fuzzLimit})
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte(nil))
	f.Add(frame(compress.CodecStored, 5, []byte("hello")))
	f.Add(frame(compress.CodecStored, 5, []byte("wrong length")))
	f.Add(frame(compress.CodecGzip, 100, []byte("not gzip at all")))
	f.Add(frame(compress.CodecFlate, 100, []byte{0x00, 0x01, 0x02}))
	f.Add(frame(compress.CodecZlib, 100, []byte{0x78, 0x9c, 0x03, 0x00}))
	f.Add(frame(200, 10, []byte("unknown codec")))
	for _, c := range allCodecs {
		e, err := compress.NewEncoder(compress.Config{Codec: c.id, MinSize: 1})
		if err != nil {
			f.Fatal(err)
		}
		framed, err := e.Encode(bytes.Repeat([]byte("iqmq "), 64))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(framed)
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		out, err := d.Decode(body)
		if err != nil {
			if out != nil {
				t.Fatalf("Decode failed with %v but returned %d bytes", err, len(out))
			}
			return
		}
		if int64(len(out)) > fuzzLimit {
			t.Fatalf("Decode produced %d bytes against a %d-byte limit", len(out), fuzzLimit)
		}
		// A successful decode agrees with the frame it came from: this is
		// the ErrSizeMismatch contract, checked from the other side.
		h, _, perr := compress.Parse(body)
		if perr != nil {
			t.Fatalf("Decode succeeded on a body Parse rejects with %v", perr)
		}
		if uint32(len(out)) != h.Size {
			t.Fatalf("Decode produced %d bytes from a frame declaring %d", len(out), h.Size)
		}
	})
}

// FuzzEncodeDecodeRoundTrip: every body survives framing and unframing, for
// every registered coding, and the framed form is always parseable by the
// decoder that will meet it.
func FuzzEncodeDecodeRoundTrip(f *testing.F) {
	f.Add([]byte(nil), uint8(1))
	f.Add([]byte("hello"), uint8(0))
	f.Add(bytes.Repeat([]byte("a"), 4096), uint8(1))
	f.Add(incompressible(1024), uint8(2))
	f.Add([]byte(compress.Magic), uint8(3))

	f.Fuzz(func(t *testing.T, body []byte, codec uint8) {
		id := allCodecs[int(codec)%len(allCodecs)].id
		e, err := compress.NewEncoder(compress.Config{Codec: id, MinSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		d, err := compress.NewDecoder(compress.Config{MaxDecompressed: 1 << 24})
		if err != nil {
			t.Fatal(err)
		}
		framed, err := e.Encode(body)
		if err != nil {
			t.Fatalf("Encode(%d bytes) = %v", len(body), err)
		}
		if compress.Framed(body) {
			// The documented pass-through. It is asserted here too so that
			// the fuzzer cannot present it as a round-trip failure.
			if !bytes.Equal(framed, body) {
				t.Fatal("a body carrying the magic was re-framed")
			}
			return
		}
		got, err := d.Decode(framed)
		if err != nil {
			t.Fatalf("Decode after Encode of %d bytes with codec %d: %v", len(body), id, err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("round trip changed a %d-byte body into %d bytes", len(body), len(got))
		}
	})
}

// FuzzDeclaredSizeIsNeverAllocated re-frames an arbitrary payload with an
// arbitrary declared size, which is the attacker's exact degree of freedom.
//
// It asserts the property the hand-built bombs assert, over every
// (payload, claim) pair the fuzzer can reach: the decode either refuses or
// produces exactly what it declared, and it never produces more than the
// limit. A decoder that believed the claim would fail this on the first input
// whose declared size exceeds its payload's real length.
func FuzzDeclaredSizeIsNeverAllocated(f *testing.F) {
	d, err := compress.NewDecoder(compress.Config{MaxDecompressed: fuzzLimit})
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte("hello"), uint8(0), uint32(5))
	f.Add([]byte("hello"), uint8(0), uint32(0xFFFFFFFF))
	f.Add(gzipOf(bytes.Repeat([]byte("z"), 1<<20)), uint8(1), uint32(16))
	f.Add(gzipOf(bytes.Repeat([]byte("z"), 1<<20)), uint8(1), uint32(0xFFFFFFFF))

	f.Fuzz(func(t *testing.T, payload []byte, codec uint8, declared uint32) {
		body := append([]byte(compress.Magic), codec, 0)
		body = binary.BigEndian.AppendUint32(body, declared)
		body = append(body, payload...)

		out, err := d.Decode(body)
		if err != nil {
			return
		}
		if int64(len(out)) > fuzzLimit {
			t.Fatalf("Decode produced %d bytes against a %d-byte limit", len(out), fuzzLimit)
		}
		if uint32(len(out)) != declared {
			t.Fatalf("Decode produced %d bytes from a frame declaring %d: the size check did not hold", len(out), declared)
		}
	})
}

// gzipOf is gzipZeros without the testing.TB, for a fuzz seed built at
// package scope.
func gzipOf(b []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(b)
	_ = w.Close()
	return buf.Bytes()
}
