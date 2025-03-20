// Package fuzzutil turns fuzzer-supplied bytes into structured operations.
//
// Every stateful component in iqmq exposes an op-script interpreter: fuzz
// bytes are decoded by a Reader into a sequence of operations that are
// applied to both the component and a reference model, with invariants
// checked after every step. Writer is the exact mirror of Reader so that
// seed corpora can be authored in code and remain readable.
//
// The Reader never panics: once the input is exhausted every method returns
// its zero value and Exhausted reports true.
package fuzzutil

import (
	"encoding/binary"
	"time"
)

// Reader decodes values from fuzz input.
type Reader struct {
	data []byte
	pos  int
	over bool
}

// NewReader returns a Reader over data.
func NewReader(data []byte) *Reader { return &Reader{data: data} }

// Exhausted reports whether a read has run past the end of the input.
func (r *Reader) Exhausted() bool { return r.over || r.pos >= len(r.data) }

// Remaining returns the number of unread bytes.
func (r *Reader) Remaining() int {
	if r.pos >= len(r.data) {
		return 0
	}
	return len(r.data) - r.pos
}

// take returns the next n bytes, zero-padded when fewer remain.
func (r *Reader) take(n int, scratch []byte) []byte {
	avail := r.Remaining()
	if avail >= n {
		b := r.data[r.pos : r.pos+n]
		r.pos += n
		return b
	}
	r.over = true
	copy(scratch, r.data[r.pos:])
	for i := avail; i < n; i++ {
		scratch[i] = 0
	}
	r.pos = len(r.data)
	return scratch[:n]
}

// Byte reads one byte.
func (r *Reader) Byte() byte {
	var s [1]byte
	return r.take(1, s[:])[0]
}

// Bool reads one byte and returns its low bit.
func (r *Reader) Bool() bool { return r.Byte()&1 == 1 }

// Uint32 reads a little-endian uint32.
func (r *Reader) Uint32() uint32 {
	var s [4]byte
	return binary.LittleEndian.Uint32(r.take(4, s[:]))
}

// Uint64 reads a little-endian uint64.
func (r *Reader) Uint64() uint64 {
	var s [8]byte
	return binary.LittleEndian.Uint64(r.take(8, s[:]))
}

// intWidth returns the number of bytes used to encode a value in [lo, hi].
func intWidth(lo, hi int) int {
	span := uint64(hi-lo) + 1
	switch {
	case span <= 1<<8:
		return 1
	case span <= 1<<16:
		return 2
	case span <= 1<<32:
		return 4
	default:
		return 8
	}
}

// Int reads an integer in [lo, hi]. When hi < lo it returns lo without
// consuming input. The encoding width depends only on the range, so Writer
// can mirror it exactly.
func (r *Reader) Int(lo, hi int) int {
	if hi < lo {
		return lo
	}
	span := uint64(hi-lo) + 1
	var s [8]byte
	b := r.take(intWidth(lo, hi), s[:])
	var v uint64
	for i := len(b) - 1; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	if span == 0 { // full 64-bit span
		return lo + int(v)
	}
	return lo + int(v%span)
}

// Choice reads an index in [0, n). It returns 0 for n <= 1.
func (r *Reader) Choice(n int) int { return r.Int(0, n-1) }

// Duration reads a duration in [0, max]. It returns 0 for max <= 0.
func (r *Reader) Duration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(r.Uint64() % (uint64(max) + 1))
}

// Bytes reads a length in [0, max] followed by that many bytes. The result
// aliases the input; callers that retain it must copy. A truncated input
// yields the bytes that remain.
func (r *Reader) Bytes(max int) []byte {
	if max < 0 {
		max = 0
	}
	n := r.Int(0, max)
	if n == 0 {
		return []byte{}
	}
	if r.Remaining() < n {
		r.over = true
		b := r.data[r.pos:]
		r.pos = len(r.data)
		return b
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b
}

// String reads a string the way Bytes reads bytes.
func (r *Reader) String(max int) string { return string(r.Bytes(max)) }

// Writer encodes values in the format Reader decodes. It is used to author
// seed corpora.
type Writer struct{ buf []byte }

// NewWriter returns an empty Writer.
func NewWriter() *Writer { return &Writer{} }

// Data returns the encoded bytes.
func (w *Writer) Data() []byte { return w.buf }

// Byte writes one byte.
func (w *Writer) Byte(b byte) { w.buf = append(w.buf, b) }

// Bool writes a byte whose low bit is b.
func (w *Writer) Bool(b bool) {
	if b {
		w.Byte(1)
	} else {
		w.Byte(0)
	}
}

// Uint32 writes a little-endian uint32.
func (w *Writer) Uint32(v uint32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }

// Uint64 writes a little-endian uint64.
func (w *Writer) Uint64(v uint64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }

// Int writes v so that Reader.Int(lo, hi) returns v. Values outside [lo, hi]
// are clamped.
func (w *Writer) Int(lo, hi, v int) {
	if hi < lo {
		return
	}
	if v < lo {
		v = lo
	}
	if v > hi {
		v = hi
	}
	u := uint64(v - lo)
	width := intWidth(lo, hi)
	for range width {
		w.buf = append(w.buf, byte(u))
		u >>= 8
	}
}

// Choice writes v so that Reader.Choice(n) returns v.
func (w *Writer) Choice(n, v int) { w.Int(0, n-1, v) }

// Duration writes d so that Reader.Duration(max) returns d (clamped to max).
func (w *Writer) Duration(max, d time.Duration) {
	if max <= 0 {
		return
	}
	if d < 0 {
		d = 0
	}
	if d > max {
		d = max
	}
	w.Uint64(uint64(d))
}

// Bytes writes b so that Reader.Bytes(max) returns it (truncated to max).
func (w *Writer) Bytes(max int, b []byte) {
	if max < 0 {
		max = 0
	}
	if len(b) > max {
		b = b[:max]
	}
	w.Int(0, max, len(b))
	w.buf = append(w.buf, b...)
}

// String writes s the way Bytes writes bytes.
func (w *Writer) String(max int, s string) { w.Bytes(max, []byte(s)) }
