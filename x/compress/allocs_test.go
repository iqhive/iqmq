package compress_test

import (
	"testing"

	"github.com/iqhive/iqmq/x/compress"
)

// TestAllocsParse: the header parse is on every framed delivery's path and
// allocates nothing.
//
// Everything the measured closure needs is built BEFORE it, which is the
// mistake the specification made in the other direction: it constructed a codec and parsed
// an Avro schema inside the closure AllocsPerRun measures and reported 840
// allocs/op for a path that allocates none. So the frame, and the sink the
// results are assigned to, exist outside.
func TestAllocsParse(t *testing.T) {
	body := frame(compress.CodecStored, 5, []byte("hello"))
	var h compress.Header
	var payload []byte
	var err error
	if got := testing.AllocsPerRun(200, func() {
		h, payload, err = compress.Parse(body)
	}); got != 0 {
		t.Errorf("Parse allocs/op = %v, want 0", got)
	}
	// Read the results, so that nothing above is dead code the compiler may
	// delete along with the allocation the test is about.
	if err != nil || h.Size != 5 || len(payload) != 5 {
		t.Fatalf("Parse = (%+v, %q, %v)", h, payload, err)
	}
}

// TestAllocsFramed: the sniff is on every delivery's path, framed or not.
func TestAllocsFramed(t *testing.T) {
	framed := frame(compress.CodecStored, 5, []byte("hello"))
	plain := []byte("hello, world, nothing to see here")
	var a, b bool
	if got := testing.AllocsPerRun(200, func() {
		a = compress.Framed(framed)
		b = compress.Framed(plain)
	}); got != 0 {
		t.Errorf("Framed allocs/op = %v, want 0", got)
	}
	if !a || b {
		t.Fatalf("Framed(framed) = %v, Framed(plain) = %v", a, b)
	}
}

// TestAllocsAppendHeader: the publish path's header write reuses the caller's
// buffer when it has room, so framing costs no allocation of its own beyond
// the body buffer the encoder already needs.
func TestAllocsAppendHeader(t *testing.T) {
	buf := make([]byte, 0, 64)
	var out []byte
	if got := testing.AllocsPerRun(200, func() {
		out = compress.AppendHeader(buf[:0], compress.CodecGzip, 1234)
	}); got != 0 {
		t.Errorf("AppendHeader allocs/op = %v, want 0", got)
	}
	h, _, err := compress.Parse(out)
	if err != nil || h.Codec != compress.CodecGzip || h.Size != 1234 {
		t.Fatalf("AppendHeader wrote %+v, %v", h, err)
	}
}

// TestAllocsStoredDecodeIsOneBuffer: the stored path is the one a caller on a
// hot topic will actually take (short bodies, and bodies compression does not
// shrink), so its cost is pinned: one buffer for the output and nothing else.
func TestAllocsStoredDecodeIsOneBuffer(t *testing.T) {
	d, err := compress.NewDecoder(compress.Config{})
	if err != nil {
		t.Fatal(err)
	}
	body := frame(compress.CodecStored, 5, []byte("hello"))
	var out []byte
	var derr error
	got := testing.AllocsPerRun(200, func() { out, derr = d.Decode(body) })
	if derr != nil || len(out) != 5 {
		t.Fatalf("Decode = (%q, %v)", out, derr)
	}
	// One allocation, for the returned slice. Not zero: Decode returns a
	// body the handler may retain, so it cannot hand back the delivery's own
	// storage (the safe-mode default, ADR-009).
	if got > 1 {
		t.Errorf("stored Decode allocs/op = %v, want at most 1 (the returned body)", got)
	}
	t.Logf("stored Decode allocs/op = %v", got)
}
