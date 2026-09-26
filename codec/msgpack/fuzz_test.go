package msgpack_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/codec"
	iqmsgpack "github.com/iqhive/iqmq/codec/msgpack"
	"github.com/iqhive/iqmq/internal/errcode"
)

// FuzzMsgpack drives the validator and the decoder with arbitrary bytes and
// holds them to five properties (the security checks):
//
//  1. Validate is total: it terminates and does not panic on any input, at
//     any limit setting, including the settings that remove the limits.
//  2. **Anti-amplification.** A body Validate accepts cannot contain more
//     values than it has bytes. That is the invariant the whole validator
//     exists for — the library allocates from a declared count, and a
//     five-byte body claiming four billion elements allocated 64 GiB before
//     this check existed — and it is checked here by counting the values in
//     what actually decoded.
//  3. Every rejection is classified: ErrTooLarge, ErrTooManyElements or
//     ErrTooDeep for a limit, and a *codec.DecodeError carrying
//     CorruptMessage for a malformed body. Nothing else.
//  4. A body that decodes re-encodes, and the second round trip is identical
//     to the first.
//  5. The append path produces exactly what Marshal produces.
func FuzzMsgpack(f *testing.F) {
	seeds := [][]byte{
		{},
		{0xc0},
		{0xc1},
		{0xc2},
		{0xc3},
		{0x00},
		{0x7f},
		{0xe0},
		{0xff},
		{0xa0},
		{0xa1, 'x'},
		{0xa5, 'h', 'i'},
		{0xd9, 0x02, 'a', 'b'},
		{0xd9, 0xff, 'a'},
		{0xc4, 0x01, 0x00},
		{0xc6, 0xff, 0xff, 0xff, 0xff},
		{0xdb, 0x7f, 0xff, 0xff, 0xff},
		{0x90},
		{0x91, 0xc0},
		{0x91},
		{0x80},
		{0x81, 0xa1, 'k', 0x01},
		{0x81, 0xa1, 'k'},
		{0xdc, 0x00, 0x02, 0xc0, 0xc0},
		{0xdc, 0xff, 0xff},
		{0xdd, 0xff, 0xff, 0xff, 0xff},
		{0xdf, 0xff, 0xff, 0xff, 0xff},
		{0xde, 0xff, 0xff},
		{0xd4, 0xff, 0x00},
		{0xd8, 0x01},
		{0xc7, 0x02, 0x00, 0x01, 0x02},
		{0xca, 0, 0, 0, 0},
		{0xcb, 0, 0, 0, 0, 0, 0, 0, 0},
		{0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		{0xc0, 0xc0}, // a second body after the first
		bytes.Repeat([]byte{0x91}, 128),
		bytes.Repeat([]byte{0x81, 0xa1, 'k'}, 64),
		[]byte(strings.Repeat("\x91", 4096)),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	def := iqmsgpack.New()
	app := def.(codec.Appender)
	f.Fuzz(func(t *testing.T, data []byte) {
		// 1. Validate is total at every limit setting, including unlimited.
		for _, cfg := range []iqmsgpack.Config{
			{},
			{MaxBytes: -1, MaxElements: -1, MaxDepth: -1},
			{MaxBytes: 16, MaxElements: 4, MaxDepth: 2},
		} {
			if err := iqmsgpack.Validate(data, cfg); err != nil {
				classifyValidate(t, data, err)
			}
		}

		var v any
		err := def.Unmarshal(data, &v)
		if err != nil {
			classifyUnmarshal(t, data, err)
			return
		}

		// 2. Anti-amplification: a body Validate accepted cannot hold more
		// values than it has bytes.
		if n := values(v); n > len(data) {
			t.Fatalf("a %d-byte body decoded to %d values: the validator let a declared count through", len(data), n)
		}
		if n := values(v); n > iqmsgpack.DefaultMaxElements {
			t.Fatalf("a body decoded to %d values, over the %d-element limit", n, iqmsgpack.DefaultMaxElements)
		}

		// 4. Stable re-encode.
		first, err := def.Marshal(v)
		if err != nil {
			t.Fatalf("a decoded body does not re-encode: %v (body %x)", err, data)
		}
		var again any
		if err := def.Unmarshal(first, &again); err != nil {
			t.Fatalf("the codec cannot decode its own output %x: %v", first, err)
		}
		second, err := def.Marshal(again)
		if err != nil {
			t.Fatalf("re-encoding the second decode failed: %v", err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("the second round trip differs: %x vs %x", second, first)
		}

		// 5. The append path agrees with Marshal.
		ap, err := app.AppendMarshal([]byte("P"), v)
		if err != nil || !bytes.Equal(ap, append([]byte("P"), first...)) {
			t.Fatalf("AppendMarshal = %x, want %x (%v)", ap, append([]byte("P"), first...), err)
		}
	})
}

// classifyValidate holds Validate's own errors to the four sentinels it
// documents. Validate is not a decode, so it does not wrap anything in a
// *codec.DecodeError: that is Unmarshal's job and classifyUnmarshal's check.
func classifyValidate(t *testing.T, data []byte, err error) {
	t.Helper()
	switch {
	case sameErr(err, iqmsgpack.ErrMalformed):
		if code := errcode.CodeOf(err); code != errcode.CorruptMessage {
			t.Fatalf("%v carries %v, want CorruptMessage", err, code)
		}
	case sameErr(err, iqmsgpack.ErrTooLarge, iqmsgpack.ErrTooManyElements, iqmsgpack.ErrTooDeep):
		if code := errcode.CodeOf(err); code != errcode.MessageTooLarge {
			t.Fatalf("%v carries %v, want MessageTooLarge", err, code)
		}
	default:
		t.Fatalf("Validate(%x) = %v (%T), which is none of its four documented errors", data, err, err)
	}
}

// classifyUnmarshal holds a decode rejection to the error model.
func classifyUnmarshal(t *testing.T, data []byte, err error) {
	t.Helper()
	if sameErr(err, iqmsgpack.ErrTooLarge, iqmsgpack.ErrTooManyElements, iqmsgpack.ErrTooDeep) {
		if code := errcode.CodeOf(err); code != errcode.MessageTooLarge {
			t.Fatalf("%v carries %v, want MessageTooLarge", err, code)
		}
		return
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("%v (%T) is neither a limit nor a *codec.DecodeError (body %x)", err, err, data)
	}
	if de.Err == nil || de.ContentType != iqmsgpack.ContentType {
		t.Fatalf("incomplete *codec.DecodeError %+v", de)
	}
	if code := errcode.CodeOf(err); code != errcode.CorruptMessage {
		t.Fatalf("%v carries %v, want CorruptMessage", err, code)
	}
}

// values counts the values in a decoded body: one per scalar, one per
// container, and one per map key.
func values(v any) int {
	switch t := v.(type) {
	case []any:
		n := 1
		for _, e := range t {
			n += values(e)
		}
		return n
	case map[string]any:
		n := 1
		for k, e := range t {
			_ = k
			n += 1 + values(e)
		}
		return n
	case map[any]any:
		n := 1
		for k, e := range t {
			n += values(k) + values(e)
		}
		return n
	default:
		return 1
	}
}

// sameErr reports whether err IS one of targets, by identity.
//
// errors.Is cannot be used for this: errcode.Error.Is matches any target
// carrying the same portable code, so errors.Is(ErrTooDeep, ErrTooLarge) is
// true -- all three limit sentinels are MessageTooLarge -- and
// *codec.DecodeError.Is matches any CorruptMessage target at all. An
// assertion written with errors.Is would pass whatever the validator returned.
func sameErr(err error, targets ...error) bool {
	for _, t := range targets {
		if err == t { //nolint:errorlint // identity is the question
			return true
		}
	}
	return false
}
