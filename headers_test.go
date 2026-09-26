package iqmq_test

import (
	"bytes"
	"slices"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestHeadersContract states the promises directly, one test per sentence,
// so that a reader can check the specification against the test rather than
// against the model.
func TestHeadersContract(t *testing.T) {
	t.Run("AddKeepsDuplicatesInOrder", func(t *testing.T) {
		var h mq.Headers
		h.Add("trace", []byte("one"))
		h.Add("trace", []byte("two"))
		h.Add("trace", []byte("three"))
		if got := h.Len(); got != 3 {
			t.Fatalf("Len after three Adds of one key = %d, want 3: the specification makes Add duplicate-preserving", got)
		}
		want := []string{"one", "two", "three"}
		vals := h.Values("trace")
		got := make([]string, 0, len(vals))
		for _, v := range vals {
			got = append(got, string(v))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("Values = %q, want %q in insertion order", got, want)
		}
	})

	t.Run("GetReturnsTheFirstValue", func(t *testing.T) {
		var h mq.Headers
		h.Add("trace", []byte("one"))
		h.Add("trace", []byte("two"))
		v, ok := h.Get("trace")
		if !ok || string(v) != "one" {
			t.Fatalf("Get = (%q, %v), want (\"one\", true): Get is the FIRST value", v, ok)
		}
	})

	t.Run("GetDistinguishesAbsentFromEmpty", func(t *testing.T) {
		var h mq.Headers
		h.Add("empty", nil)
		if v, ok := h.Get("empty"); !ok || v != nil {
			t.Fatalf("Get of a present key with a nil value = (%v, %v), want (nil, true)", v, ok)
		}
		if v, ok := h.Get("absent"); ok || v != nil {
			t.Fatalf("Get of an absent key = (%v, %v), want (nil, false)", v, ok)
		}
	})

	t.Run("SetReplacesEveryValueInPlace", func(t *testing.T) {
		var h mq.Headers
		h.Add("a", []byte("1"))
		h.Add("trace", []byte("one"))
		h.Add("b", []byte("2"))
		h.Add("trace", []byte("two"))
		h.Add("c", []byte("3"))
		h.Set("trace", []byte("only"))
		if got := h.Len(); got != 4 {
			t.Fatalf("Len after Set over two duplicates = %d, want 4: Set replaces every value", got)
		}
		var keys []string
		for k := range h.All() {
			keys = append(keys, k)
		}
		if want := []string{"a", "trace", "b", "c"}; !slices.Equal(keys, want) {
			t.Fatalf("order after Set = %q, want %q: Set replaces in place and does not move the pair", keys, want)
		}
		if v, _ := h.Get("trace"); string(v) != "only" {
			t.Fatalf("Get after Set = %q, want \"only\"", v)
		}
	})

	t.Run("SetAppendsAnAbsentKey", func(t *testing.T) {
		var h mq.Headers
		h.Add("a", []byte("1"))
		h.Set("b", []byte("2"))
		var keys []string
		for k := range h.All() {
			keys = append(keys, k)
		}
		if want := []string{"a", "b"}; !slices.Equal(keys, want) {
			t.Fatalf("order = %q, want %q", keys, want)
		}
	})

	t.Run("DeleteRemovesEveryValue", func(t *testing.T) {
		var h mq.Headers
		h.Add("a", []byte("1"))
		h.Add("trace", []byte("one"))
		h.Add("b", []byte("2"))
		h.Add("trace", []byte("two"))
		h.Delete("trace")
		if got := h.Len(); got != 2 {
			t.Fatalf("Len after Delete of a duplicated key = %d, want 2", got)
		}
		if v := h.Values("trace"); v != nil {
			t.Fatalf("Values after Delete = %q, want nil", v)
		}
		var keys []string
		for k := range h.All() {
			keys = append(keys, k)
		}
		if want := []string{"a", "b"}; !slices.Equal(keys, want) {
			t.Fatalf("order after Delete = %q, want %q", keys, want)
		}
	})

	t.Run("CaseSensitive", func(t *testing.T) {
		var h mq.Headers
		h.Add("Content-Type", []byte("upper"))
		h.Add("content-type", []byte("lower"))
		if got := h.Len(); got != 2 {
			t.Fatalf("Len = %d, want 2: the specification makes keys case-sensitive", got)
		}
		if v, _ := h.Get("content-type"); string(v) != "lower" {
			t.Fatalf("Get(\"content-type\") = %q, want \"lower\"", v)
		}
	})

	t.Run("BinaryValues", func(t *testing.T) {
		var h mq.Headers
		val := []byte{0x00, 0xff, 0x80, 0x7f}
		h.Add("bin", val)
		if v, _ := h.Get("bin"); !bytes.Equal(v, val) {
			t.Fatalf("Get = %x, want %x: the specification makes values binary-capable", v, val)
		}
	})

	t.Run("CloneIsIndependent", func(t *testing.T) {
		var h mq.Headers
		val := []byte("original")
		h.Add("k", val)
		c := h.Clone()
		// Three ways to observe a shallow copy.
		val[0] = 'X'
		if v, _ := c.Get("k"); string(v) != "original" {
			t.Fatalf("the clone saw a mutation of the original's value bytes: %q", v)
		}
		h.Set("k", []byte("replaced"))
		if v, _ := c.Get("k"); string(v) != "original" {
			t.Fatalf("the clone saw a Set on the original: %q", v)
		}
		c.Add("only-in-clone", nil)
		if _, ok := h.Get("only-in-clone"); ok {
			t.Fatal("the original saw an Add on the clone")
		}
	})

	t.Run("CloneOfEmptyIsUsable", func(t *testing.T) {
		var h mq.Headers
		c := h.Clone()
		c.Add("k", []byte("v"))
		if c.Len() != 1 || h.Len() != 0 {
			t.Fatalf("clone of an empty set: clone Len=%d, original Len=%d, want 1 and 0", c.Len(), h.Len())
		}
	})

	t.Run("ResetKeepsTheSetUsable", func(t *testing.T) {
		var h mq.Headers
		h.Add("a", []byte("1"))
		h.Add("b", []byte("2"))
		h.Reset()
		if h.Len() != 0 {
			t.Fatalf("Len after Reset = %d, want 0", h.Len())
		}
		if _, ok := h.Get("a"); ok {
			t.Fatal("Get found a key after Reset")
		}
		h.Add("c", []byte("3"))
		var keys []string
		for k := range h.All() {
			keys = append(keys, k)
		}
		if want := []string{"c"}; !slices.Equal(keys, want) {
			t.Fatalf("order after Reset and Add = %q, want %q", keys, want)
		}
	})

	t.Run("AllStopsWhenTheCallerStops", func(t *testing.T) {
		var h mq.Headers
		for _, k := range []string{"a", "b", "c"} {
			h.Add(k, nil)
		}
		n := 0
		for range h.All() {
			n++
			if n == 2 {
				break
			}
		}
		if n != 2 {
			t.Fatalf("All yielded %d pairs after a break at 2", n)
		}
	})

	t.Run("ZeroValueIsUsable", func(t *testing.T) {
		var h mq.Headers
		if h.Len() != 0 {
			t.Fatalf("the zero Headers has Len %d", h.Len())
		}
		if _, ok := h.Get("a"); ok {
			t.Fatal("the zero Headers found a key")
		}
		if v := h.Values("a"); v != nil {
			t.Fatal("the zero Headers returned values")
		}
		h.Delete("a")
		h.Reset()
		if err := h.Validate(mq.Limits{}, true); err != nil {
			t.Fatalf("the zero Headers failed validation: %v", err)
		}
		h.Set("a", []byte("1"))
		if v, ok := h.Get("a"); !ok || string(v) != "1" {
			t.Fatalf("Set on the zero Headers: Get = (%q, %v)", v, ok)
		}
	})
}

// TestHeadersValidate holds the limit rules, and in particular the rule
// that an unknown limit is never an overloaded zero. That rule is the half no benchmark measured, so it is stated here as its own table.
func TestHeadersValidate(t *testing.T) {
	fill := func(n int) mq.Headers {
		var h mq.Headers
		for i := range n {
			h.Add("k"+string(rune('a'+i)), []byte("v"))
			_ = i
		}
		return h
	}
	t.Run("UnknownIsNotZero", func(t *testing.T) {
		h := fill(3)
		if err := h.Validate(mq.Limits{}, false); err != nil {
			t.Fatalf("three headers against unadvertised limits = %v, want nil: "+
				"a Limit whose Known is false enforces nothing ", err)
		}
		zeroKnown := mq.Limits{MaxHeaderCount: mq.Limit[int]{Value: 0, Known: true}}
		err := h.Validate(zeroKnown, false)
		if err == nil {
			t.Fatal("three headers against an advertised limit of zero = nil, want a failure: " +
				"Known true with Value zero means no headers are permitted ")
		}
		if got := mq.CodeOf(err); got != mq.CodeMessageTooLarge {
			t.Fatalf("code = %v, want %v", got, mq.CodeMessageTooLarge)
		}
		// The two answers must differ. If they did not, Limit's Known field
		// would be decoration.
		if got := h.Validate(mq.Limits{}, false); got != nil {
			t.Fatalf("unadvertised = %v, advertised-zero = %v: they must not be the same answer", got, err)
		}
		// The same asymmetry for the byte limit.
		if err := h.Validate(mq.Limits{MaxHeaderBytes: mq.Limit[int64]{Known: false}}, false); err != nil {
			t.Fatalf("unadvertised MaxHeaderBytes = %v, want nil", err)
		}
		if err := h.Validate(mq.Limits{MaxHeaderBytes: mq.Limit[int64]{Value: 0, Known: true}}, false); err == nil {
			t.Fatal("advertised MaxHeaderBytes of zero = nil, want a failure")
		}
	})

	t.Run("OnlyTheHeaderLimitsAreRead", func(t *testing.T) {
		// A driver that advertises a tiny message or batch limit must not
		// have it applied to the header set.
		h := fill(3)
		lim := mq.Limits{
			MaxMessageBytes:  mq.Limit[int64]{Value: 1, Known: true},
			MaxBatchMessages: mq.Limit[int]{Value: 1, Known: true},
			MaxBatchBytes:    mq.Limit[int64]{Value: 1, Known: true},
		}
		if err := h.Validate(lim, false); err != nil {
			t.Fatalf("Validate read a limit that is not a header limit: %v", err)
		}
	})

	t.Run("Count", func(t *testing.T) {
		h := fill(3)
		lim := mq.Limits{MaxHeaderCount: mq.Limit[int]{Value: 3, Known: true}}
		if err := h.Validate(lim, false); err != nil {
			t.Fatalf("exactly at the limit = %v, want nil", err)
		}
		lim.MaxHeaderCount.Value = 2
		if err := h.Validate(lim, false); !isCode(err, mq.CodeMessageTooLarge) {
			t.Fatalf("one over the limit = %v, want CodeMessageTooLarge", err)
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		var h mq.Headers
		h.Add("ab", []byte("cd")) // four bytes
		lim := mq.Limits{MaxHeaderBytes: mq.Limit[int64]{Value: 4, Known: true}}
		if err := h.Validate(lim, false); err != nil {
			t.Fatalf("exactly at the limit = %v, want nil", err)
		}
		lim.MaxHeaderBytes.Value = 3
		if err := h.Validate(lim, false); !isCode(err, mq.CodeMessageTooLarge) {
			t.Fatalf("one over the limit = %v, want CodeMessageTooLarge", err)
		}
	})

	t.Run("ReservedPrefix", func(t *testing.T) {
		var h mq.Headers
		h.Add(mq.ReservedHeaderPrefix+"id", []byte("x"))
		err := h.Validate(mq.Limits{}, false)
		if !isCode(err, mq.CodeInvalidArgument) {
			t.Fatalf("a reserved key = %v, want CodeInvalidArgument: the specification rejects the conflict", err)
		}
	})

	t.Run("PortableKeys", func(t *testing.T) {
		var h mq.Headers
		h.Add("bad\xffkey", nil)
		if err := h.Validate(mq.Limits{}, false); err != nil {
			t.Fatalf("a non-UTF-8 key outside portable mode = %v, want nil", err)
		}
		if err := h.Validate(mq.Limits{}, true); !isCode(err, mq.CodeInvalidArgument) {
			t.Fatalf("a non-UTF-8 key in portable mode = %v, want CodeInvalidArgument", err)
		}
	})

	t.Run("CountBeforeBytesBeforeKeys", func(t *testing.T) {
		// A set that violates all three rules reports the count, so one
		// header set always produces one verdict.
		var h mq.Headers
		h.Add(mq.ReservedHeaderPrefix+"id", []byte("xxxxxxxx"))
		h.Add("bad\xffkey", []byte("xxxxxxxx"))
		lim := mq.Limits{
			MaxHeaderCount: mq.Limit[int]{Value: 1, Known: true},
			MaxHeaderBytes: mq.Limit[int64]{Value: 1, Known: true},
		}
		if err := h.Validate(lim, true); !isCode(err, mq.CodeMessageTooLarge) {
			t.Fatalf("all three rules violated = %v, want the count failure first", err)
		}
	})
}

// isCode reports whether err carries the code.
func isCode(err error, code mq.ErrorCode) bool { return err != nil && mq.CodeOf(err) == code }
