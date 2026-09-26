package compress_test

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"runtime"
	"testing"

	"github.com/iqhive/iqmq/x/compress"
)

// bombSize is what the bomb decompresses to: 64 MiB of zeros in about 64 KiB
// of gzip. It is deliberately the same order as the "five-byte input asked for
// 64 GiB" defect the specification found in two codecs, scaled down to something this test
// can afford to let the negative control actually allocate.
const bombSize = 64 << 20

// bombLimit is the limit the decoder under test is configured with.
const bombLimit = 1 << 20

// gzipZeros returns a gzip stream of n zero bytes.
func gzipZeros(tb testing.TB, n int) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	zeros := make([]byte, 64<<10)
	for left := n; left > 0; {
		chunk := len(zeros)
		if chunk > left {
			chunk = left
		}
		if _, err := w.Write(zeros[:chunk]); err != nil {
			tb.Fatal(err)
		}
		left -= chunk
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

// frame builds a frame by hand, so that a test can state a size the encoder
// would never write.
func frame(codec compress.CodecID, declared uint32, payload []byte) []byte {
	b := append([]byte(compress.Magic), byte(codec), 0)
	b = binary.BigEndian.AppendUint32(b, declared)
	return append(b, payload...)
}

// bytesAllocated reports how many bytes fn allocated in total.
//
// TotalAlloc and not HeapAlloc: the question is "did this allocate 64 MiB",
// and a heap measurement answers "is 64 MiB still live", which a decoder that
// allocated the bomb and then dropped it would pass. The measurement is
// itself under test -- see TestBytesAllocatedCanSeeAnAllocation below, which
// is the negative control for the instrument rather than for the decoder.
func bytesAllocated(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestBytesAllocatedCanSeeAnAllocation is the instrument's own check.
//
// An allocation budget nobody has watched exceed is assumed not to work, and
// four increments in a row have now found the defect in their apparatus
// rather than in the thing measured. So before any budget below is believed,
// this asserts that the meter reports at least the 64 MiB of a deliberate
// allocation. Without it, a `bytesAllocated` that always returned 0 would
// make every budget in this file pass.
func TestBytesAllocatedCanSeeAnAllocation(t *testing.T) {
	var sink []byte
	got := bytesAllocated(func() { sink = make([]byte, bombSize) })
	if len(sink) != bombSize {
		t.Fatal("the allocation was optimised away; the budgets below prove nothing")
	}
	if got < bombSize {
		t.Fatalf("bytesAllocated = %d for a %d-byte allocation; the meter cannot see an allocation, so no budget in this file means anything", got, bombSize)
	}
	t.Logf("meter sees %d bytes for a %d-byte allocation", got, bombSize)
}

// TestDecompressionBombIsRefusedBeforeAnythingIsAllocated covers the frame
// that declares its true, enormous size: the specification's "compression middleware caps
// decompressed output", taken at the cheapest point there is.
func TestDecompressionBombIsRefusedBeforeAnythingIsAllocated(t *testing.T) {
	d, err := compress.NewDecoder(compress.Config{MaxDecompressed: bombLimit})
	if err != nil {
		t.Fatal(err)
	}
	bomb := frame(compress.CodecGzip, bombSize, gzipZeros(t, bombSize))
	t.Logf("bomb: %d compressed bytes declaring %d", len(bomb), bombSize)

	var out []byte
	var derr error
	got := bytesAllocated(func() { out, derr = d.Decode(bomb) })
	if !errors.Is(derr, compress.ErrTooLarge) {
		t.Fatalf("Decode = (%d bytes, %v), want ErrTooLarge", len(out), derr)
	}
	// The declared size is over the limit, so the refusal happens before the
	// codec is looked up: this budget is tiny on purpose.
	const budget = 64 << 10
	if got > budget {
		t.Errorf("refusing the bomb allocated %d bytes, want at most %d: the declared size is being acted on before it is checked", got, budget)
	}
	t.Logf("refused after allocating %d bytes", got)
}

// TestABombThatLiesAboutItsSizeIsStillBounded covers the frame that declares
// a size UNDER the limit and then decompresses to far more.
//
// This is the case the declared-size check cannot catch, and it is why the
// decode is bounded a second time by the reader rather than only by the
// header. A decoder that trusted the header here would allocate the bomb.
func TestABombThatLiesAboutItsSizeIsStillBounded(t *testing.T) {
	d, err := compress.NewDecoder(compress.Config{MaxDecompressed: bombLimit})
	if err != nil {
		t.Fatal(err)
	}
	liar := frame(compress.CodecGzip, 1024, gzipZeros(t, bombSize))

	var out []byte
	var derr error
	got := bytesAllocated(func() { out, derr = d.Decode(liar) })
	if !errors.Is(derr, compress.ErrTooLarge) {
		t.Fatalf("Decode = (%d bytes, %v), want ErrTooLarge", len(out), derr)
	}
	// The budget is derived from the LIMIT and not written down as a round
	// number, because the first version of this test used a flat 8 MiB and
	// the race build came in at 8,426,840 bytes -- 0.5 % over, a failure
	// about the budget rather than about the decoder.
	//
	// The arithmetic: the output buffer grows by doubling from a 64 KiB
	// floor to just past the 1 MiB limit (the reader is given limit+1), so
	// the cumulative allocation is a few multiples of the limit, plus gzip's
	// own 32 KiB window. Measured: ~4.2 MB plain, ~8.4 MB under -race, which
	// roughly doubles it. Sixteen times the limit clears both with room and
	// is still a quarter of what the payload would have produced -- and the
	// assertion below, that the whole payload was NOT allocated, is the one
	// that carries the property.
	const budget = 16 * bombLimit
	if got > budget {
		t.Errorf("a lying bomb allocated %d bytes against a %d-byte limit, want at most %d (%d x the limit)", got, bombLimit, budget, budget/bombLimit)
	}
	if got >= bombSize {
		t.Errorf("a lying bomb allocated %d bytes, i.e. the whole payload: the decode is bounded only by the header", got)
	}
	t.Logf("lying bomb bounded after allocating %d bytes against a %d-byte limit", got, bombLimit)
}

// TestTheNaiveDecoderIsWhatTheBudgetsWouldCatch is the negative control for
// the two budgets above.
//
// It runs the same bomb through the implementation the two tests exist to
// forbid -- allocate the declared size, copy without a limit -- and asserts
// that the meter reports the whole 64 MiB. If this ever stops failing the
// budget, the budgets above have stopped meaning anything and this test says
// so rather than leaving them decorative (the playbook's "an instrument
// nobody has watched fail is assumed not to work").
func TestTheNaiveDecoderIsWhatTheBudgetsWouldCatch(t *testing.T) {
	bomb := frame(compress.CodecGzip, bombSize, gzipZeros(t, bombSize))
	var out []byte
	got := bytesAllocated(func() { out = naiveDecode(t, bomb) })
	if len(out) != bombSize {
		t.Fatalf("the naive decoder produced %d bytes, want the bomb's %d", len(out), bombSize)
	}
	if got < bombSize {
		t.Fatalf("the naive decoder allocated only %d bytes for a %d-byte bomb; the budgets above cannot be distinguishing anything", got, bombSize)
	}
	t.Logf("the forbidden implementation allocates %d bytes, against the %d-byte budget the real decoder is held to", got, 64<<10)
}

// naiveDecode is the defect, written out: size the buffer from the frame's
// own claim and copy until EOF. It exists only as the control above.
func naiveDecode(tb testing.TB, b []byte) []byte {
	tb.Helper()
	h, payload, err := compress.Parse(b)
	if err != nil {
		tb.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = zr.Close() }()
	buf := bytes.NewBuffer(make([]byte, 0, h.Size)) // <- the defect
	if _, err := buf.ReadFrom(zr); err != nil {     // <- and the other half
		tb.Fatal(err)
	}
	return buf.Bytes()
}
