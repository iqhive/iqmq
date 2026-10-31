package wire

import (
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/iqhive/iqmq/internal/payload"
)

// TestCheckRefusesAHeaderListThatEndsPast2GiB: the decoders record a header's
// position as an int32 and refuse a header list that ends past math.MaxInt32,
// which only unbounded limits let an envelope reach. Check had no such rule,
// so under unbounded limits it passed a payload whose envelope Decode then
// refused. FuzzWireCheck cannot build a payload that large.
//
// The 2 GiB of key or header value is an anonymous mapping that is never
// written, so the kernel backs none of it however often the test runs: Check
// and Size read only lengths. A heap slice of that size was fresh memory the
// first time and, run again in the same process, a reused span the runtime
// zeroed, 4 GB at -count=5. With no body and no properties the header list
// ends where the envelope does, at Size. That the decoders draw the line at
// the same byte was checked once by encoding and decoding these payloads,
// which takes 4 GiB of memory and is not a test a shared runner should run.
func TestCheckRefusesAHeaderListThatEndsPast2GiB(t *testing.T) {
	if math.MaxInt == math.MaxInt32 {
		t.Skip("an envelope past 2 GiB cannot be held on a 32-bit platform")
	}
	var over int64 = math.MaxInt32 + 16
	buf := unbacked(t, int(over))
	unbounded := &Limits{MaxKey: -1, MaxBody: -1, MaxID: -1, MaxHeaders: -1, MaxHeaderKey: -1, MaxHeaderValue: -1, MaxProp: -1}
	// Limits that bound every field and still let one field, or many headers
	// together, carry the list past 2 GiB: Check must not take them for
	// limits that rule the walk out.
	bounded := func(key, headers, value int) *Limits {
		return &Limits{MaxKey: key, MaxBody: 1, MaxID: 1, MaxHeaders: headers, MaxHeaderKey: 1, MaxHeaderValue: value, MaxProp: 1}
	}
	for _, k := range Kinds() {
		l := mustNew(t, k)
		for _, tc := range []struct {
			headers int
			inValue bool // the bulk is the first header's value, not the key
			spread  int  // each header's value is this long, and the key makes up the rest
			end     int  // where the envelope, and so the header list, ends
			refused bool
			lim     *Limits // nil is unbounded
		}{
			{headers: 1, end: math.MaxInt32, refused: false},
			{headers: 1, end: math.MaxInt32 + 1, refused: true},
			{headers: 3, end: math.MaxInt32 + 1, refused: true},
			{headers: 1000, end: math.MaxInt32, refused: false},
			{headers: 1000, end: math.MaxInt32 + 1, refused: true},
			{headers: 1, inValue: true, end: math.MaxInt32, refused: false},
			{headers: 1, inValue: true, end: math.MaxInt32 + 1, refused: true},
			{headers: 0, end: math.MaxInt32 + 1, refused: false},
			{headers: 1, end: math.MaxInt32 + 1, refused: true, lim: bounded(math.MaxInt32, 1, 1)},
			{headers: 1, inValue: true, end: math.MaxInt32 + 1, refused: true, lim: bounded(1, 1, math.MaxInt32)},
			{headers: 1000, spread: 1 << 21, end: math.MaxInt32, refused: false, lim: bounded(1<<26, 1000, 1<<21)},
			{headers: 1000, spread: 1 << 21, end: math.MaxInt32 + 1, refused: true, lim: bounded(1<<26, 1000, 1<<21)},
		} {
			p := &payload.Payload{Headers: make([]payload.KV, tc.headers)}
			for i := range p.Headers {
				p.Headers[i].Value = buf[:tc.spread]
			}
			bulk := &p.Key
			if tc.inValue {
				bulk = &p.Headers[0].Value
			}
			n := tc.end
			for range 4 {
				*bulk = buf[:n]
				n -= l.Size(p) - tc.end
			}
			*bulk = buf[:n]
			if got := l.Size(p); got != tc.end {
				t.Fatalf("%v: the envelope is %d bytes, want %d", k, got, tc.end)
			}
			lim := tc.lim
			if lim == nil {
				lim = unbounded
			}
			err := l.Check(p, lim)
			switch {
			case tc.refused && !errors.Is(err, ErrLimit):
				t.Errorf("%v: %d header(s) ending at MaxInt32%+d (bulk in a header value: %v, limits %+v): Check = %v, want ErrLimit, which Decode answers",
					k, tc.headers, tc.end-math.MaxInt32, tc.inValue || tc.spread > 0, *lim, err)
			case !tc.refused && err != nil:
				t.Errorf("%v: %d header(s) ending at MaxInt32%+d (bulk in a header value: %v, limits %+v): Check = %v, want nil, which Decode answers",
					k, tc.headers, tc.end-math.MaxInt32, tc.inValue || tc.spread > 0, *lim, err)
			}
		}
	}
}

// unbacked returns n bytes of address space that nothing will write to: a
// private anonymous mapping with no swap reserved, unmapped when t ends.
func unbacked(t *testing.T, n int) []byte {
	t.Helper()
	b, err := syscall.Mmap(-1, 0, n, syscall.PROT_READ, syscall.MAP_PRIVATE|syscall.MAP_ANON|syscall.MAP_NORESERVE)
	if err != nil {
		t.Skipf("cannot map %d bytes of address space: %v", n, err)
	}
	t.Cleanup(func() {
		if err := syscall.Munmap(b); err != nil {
			t.Errorf("munmap: %v", err)
		}
	})
	return b
}
