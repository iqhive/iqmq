package codec_test

import (
	"encoding"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/iqhive/iqmq/codec"
)

// The raw codec is the zero-copy path: it is what carries a []byte or a string
// body through without encoding it, and its Marshal/AppendMarshal pair is the
// single-publish path and the BATCH path of the same publication. So the
// property that matters is not that each arm returns something -- it is that
// the two functions AGREE. An AppendMarshal arm that disagreed with its Marshal
// arm would put a different body on the wire depending on whether the caller
// batched, and nothing in the type system says otherwise.
//
// The second property is ownership, which raw.go documents and which a caller
// relies on: Marshal of a []byte returns THAT slice (the memory engine's Owned
// mode is built on it), and Unmarshal into a *[]byte copies, so a decoded body
// never aliases the driver's receive buffer.

// bothHooks implements AppendMarshaler AND Marshaler, and the two produce
// DIFFERENT bytes, so which one the codec called is observable. raw.go's doc
// comment says AppendMarshaler is preferred over Marshaler when a value
// implements both.
type bothHooks struct{}

func (bothHooks) AppendMessage(dst []byte) ([]byte, error) {
	return append(dst, "from-append"...), nil
}
func (bothHooks) MarshalMessage() ([]byte, error) { return []byte("from-marshal"), nil }

// marshalerOnly implements Marshaler and nothing else.
type marshalerOnly struct{ body string }

func (m marshalerOnly) MarshalMessage() ([]byte, error) { return []byte(m.body), nil }

// failingMarshaler is a hook that returns an error, which the codec must pass
// through rather than swallow.
type failingMarshaler struct{}

var errHook = errors.New("the message type refused to encode itself")

func (failingMarshaler) MarshalMessage() ([]byte, error) { return nil, errHook }

// binAppender implements encoding.BinaryAppender AND encoding.BinaryMarshaler
// with different bytes, so the arm that ran is observable.
type binAppender struct{}

func (binAppender) AppendBinary(dst []byte) ([]byte, error) {
	return append(dst, "bin-append"...), nil
}
func (binAppender) MarshalBinary() ([]byte, error) { return []byte("bin-marshal"), nil }

// binMarshaler implements encoding.BinaryMarshaler only.
type binMarshaler struct{ body string }

func (b binMarshaler) MarshalBinary() ([]byte, error) { return []byte(b.body), nil }

// binUnmarshaler implements encoding.BinaryUnmarshaler.
type binUnmarshaler struct{ got string }

func (b *binUnmarshaler) UnmarshalBinary(data []byte) error {
	b.got = "bin:" + string(data)
	return nil
}

// msgUnmarshaler implements codec.Unmarshaler, which is preferred over
// BinaryUnmarshaler.
type msgUnmarshaler struct{ got string }

func (m *msgUnmarshaler) UnmarshalMessage(data []byte) error {
	m.got = "msg:" + string(data)
	return nil
}

func (m *msgUnmarshaler) UnmarshalBinary(data []byte) error {
	m.got = "bin:" + string(data)
	return nil
}

// notRaw is a type the raw codec has no arm for.
type notRaw struct{ N int }

var (
	_ codec.AppendMarshaler      = bothHooks{}
	_ codec.Marshaler            = bothHooks{}
	_ encoding.BinaryAppender    = binAppender{}
	_ encoding.BinaryMarshaler   = binAppender{}
	_ encoding.BinaryUnmarshaler = (*binUnmarshaler)(nil)
	_ codec.Unmarshaler          = (*msgUnmarshaler)(nil)
)

func TestRawMarshalAndAppendMarshalAgreeOnEveryShapeTheyAccept(t *testing.T) {
	t.Parallel()
	c := codec.Raw()
	app, ok := c.(codec.Appender)
	if !ok {
		t.Fatal("the raw codec does not implement Appender; the batch publish path would fall back to Marshal plus a copy")
	}
	someBytes := []byte("bytes")
	someString := "string"
	for _, tc := range []struct {
		name string
		v    any
		want string
	}{
		{"nil", nil, ""},
		{"a value implementing both hooks takes the append one", bothHooks{}, "from-append"},
		{"a Marshaler-only value", marshalerOnly{"hooked"}, "hooked"},
		{"a byte slice", someBytes, "bytes"},
		{"an empty byte slice", []byte{}, ""},
		{"a pointer to a byte slice", &someBytes, "bytes"},
		{"a string", someString, "string"},
		{"an empty string", "", ""},
		{"a pointer to a string", &someString, "string"},
		{"a BinaryAppender takes the append arm", binAppender{}, "bin-append"},
		{"a BinaryMarshaler-only value", binMarshaler{"binary"}, "binary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := c.Marshal(tc.v)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("Marshal = %q, want %q", got, tc.want)
			}
			// AppendMarshal onto a non-empty destination: it must APPEND, so the
			// prefix survives and the suffix equals what Marshal produced. A
			// replacement rather than an append would silently drop every
			// message already in a batch buffer.
			const prefix = "PRE"
			out, err := app.AppendMarshal([]byte(prefix), tc.v)
			if err != nil {
				t.Fatalf("AppendMarshal: %v", err)
			}
			if string(out) != prefix+tc.want {
				t.Errorf("AppendMarshal = %q, want %q: the batch path disagrees with the single-publish path", out, prefix+tc.want)
			}
		})
	}
}

func TestRawRefusesANilTypedPointerRatherThanDereferencingIt(t *testing.T) {
	t.Parallel()
	// A nil *[]byte or *string is not the untyped nil: it reaches the typed arm
	// and a dereference there is a panic inside the publish path. ErrNilTarget
	// is InvalidArgument, so the caller sees a rejected publication instead.
	c := codec.Raw()
	app := c.(codec.Appender) //nolint:forcetypeassert // asserted in the test above
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"a nil pointer to a byte slice", (*[]byte)(nil)},
		{"a nil pointer to a string", (*string)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := c.Marshal(tc.v)
			if !errors.Is(err, codec.ErrNilTarget) {
				t.Errorf("Marshal gave (%q, %v), want ErrNilTarget", got, err)
			}
			if got != nil {
				t.Errorf("a refused Marshal also returned %q", got)
			}
			// AppendMarshal must refuse it too, and must hand the destination
			// back UNCHANGED: a batch builder that kept a partially written
			// buffer would send a truncated message.
			out, err := app.AppendMarshal([]byte("PRE"), tc.v)
			if !errors.Is(err, codec.ErrNilTarget) {
				t.Errorf("AppendMarshal gave (%q, %v), want ErrNilTarget", out, err)
			}
			if string(out) != "PRE" {
				t.Errorf("AppendMarshal returned %q, want the destination unchanged", out)
			}
		})
	}
}

// TestRawUnmarshalRefusesANilUnmarshaler is the reported defect: a nil
// *msgUnmarshaler is an Unmarshaler, and its pointer-receiver method
// dereferenced nil, so the decode panicked where a nil *[]byte has always been
// ErrNilTarget.
func TestRawUnmarshalRefusesANilUnmarshaler(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"a nil codec.Unmarshaler", (*msgUnmarshaler)(nil)},
		{"a nil encoding.BinaryUnmarshaler", (*binUnmarshaler)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := codec.Raw().Unmarshal([]byte("x"), tc.v); !errors.Is(err, codec.ErrNilTarget) {
				t.Fatalf("Unmarshal into %T(nil) = %v, want ErrNilTarget", tc.v, err)
			}
		})
	}
}

func TestRawMarshalRefusesANilMarshaler(t *testing.T) {
	t.Parallel()
	app := codec.Raw().(codec.Appender) //nolint:forcetypeassert // Raw is an Appender
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"a nil codec.AppendMarshaler", (*bothHooks)(nil)},
		{"a nil codec.Marshaler", (*marshalerOnly)(nil)},
		{"a nil encoding.BinaryAppender", (*binAppender)(nil)},
		{"a nil encoding.BinaryMarshaler", (*binMarshaler)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, err := codec.Raw().Marshal(tc.v); !errors.Is(err, codec.ErrNilTarget) {
				t.Fatalf("Marshal(%T(nil)) = (%q, %v), want ErrNilTarget", tc.v, got, err)
			}
			out, err := app.AppendMarshal([]byte("PRE"), tc.v)
			if !errors.Is(err, codec.ErrNilTarget) || string(out) != "PRE" {
				t.Fatalf("AppendMarshal(%T(nil)) = (%q, %v), want ErrNilTarget and the destination unchanged", tc.v, out, err)
			}
		})
	}
}

func TestRawRefusesAValueItHasNoArmFor(t *testing.T) {
	t.Parallel()
	c := codec.Raw()
	app := c.(codec.Appender) //nolint:forcetypeassert // asserted above
	for _, v := range []any{notRaw{1}, &notRaw{1}, 42, 1.5, map[string]int{"a": 1}, []string{"a"}} {
		got, err := c.Marshal(v)
		if !errors.Is(err, codec.ErrNotRawValue) {
			t.Errorf("Marshal(%T) = (%q, %v), want ErrNotRawValue", v, got, err)
		}
		// The fall-through in AppendMarshal routes to Marshal, so the same value
		// must be refused there with the same error and an untouched
		// destination.
		out, err := app.AppendMarshal([]byte("PRE"), v)
		if !errors.Is(err, codec.ErrNotRawValue) {
			t.Errorf("AppendMarshal(%T) = (%q, %v), want ErrNotRawValue", v, out, err)
		}
		if string(out) != "PRE" {
			t.Errorf("AppendMarshal(%T) returned %q, want the destination unchanged", v, out)
		}
	}
}

// Adapt hands a codec the address of the caller's value, so through Adapt Raw
// sees a **[]byte for T = *[]byte and a *any for T = any. It refused both with
// ErrNotRawValue, although each holds a value it takes everywhere else.
func TestRawThroughAdaptTakesPointerAndInterfaceTypes(t *testing.T) {
	t.Parallel()
	encode := func(t *testing.T, name string, enc func() ([]byte, error), appendEnc func([]byte) ([]byte, error), want string) {
		t.Helper()
		if got, err := enc(); err != nil || string(got) != want {
			t.Errorf("%s: Encode = (%q, %v), want %q", name, got, err, want)
		}
		if got, err := appendEnc([]byte("PRE")); err != nil || string(got) != "PRE"+want {
			t.Errorf("%s: AppendEncode = (%q, %v), want %q", name, got, err, "PRE"+want)
		}
	}
	t.Run("*[]byte", func(t *testing.T) {
		t.Parallel()
		tc := codec.Adapt[*[]byte](codec.Raw())
		app := tc.(codec.AppendingTyped[*[]byte]) //nolint:forcetypeassert // Raw is an Appender
		body := []byte("body")
		encode(t, "&body", func() ([]byte, error) { return tc.Encode(&body) },
			func(dst []byte) ([]byte, error) { return app.AppendEncode(dst, &body) }, "body")
		got, err := tc.Decode([]byte("reply"))
		if err != nil || got == nil || string(*got) != "reply" {
			t.Fatalf("Decode = (%v, %v), want a *[]byte holding reply", got, err)
		}
	})
	t.Run("*string", func(t *testing.T) {
		t.Parallel()
		tc := codec.Adapt[*string](codec.Raw())
		app := tc.(codec.AppendingTyped[*string]) //nolint:forcetypeassert // Raw is an Appender
		s := "text"
		encode(t, "&s", func() ([]byte, error) { return tc.Encode(&s) },
			func(dst []byte) ([]byte, error) { return app.AppendEncode(dst, &s) }, "text")
		got, err := tc.Decode([]byte("reply"))
		if err != nil || got == nil || *got != "reply" {
			t.Fatalf("Decode = (%v, %v), want a *string holding reply", got, err)
		}
		if _, err := tc.Encode(nil); !errors.Is(err, codec.ErrNilTarget) {
			t.Errorf("Encode(nil) = %v, want ErrNilTarget, as Raw answers a nil *string", err)
		}
	})
	t.Run("a pointer to a message type", func(t *testing.T) {
		t.Parallel()
		enc := codec.Adapt[*marshalerOnly](codec.Raw())
		if got, err := enc.Encode(&marshalerOnly{body: "hooked"}); err != nil || string(got) != "hooked" {
			t.Errorf("Encode = (%q, %v), want hooked", got, err)
		}
		dec := codec.Adapt[*binUnmarshaler](codec.Raw())
		got, err := dec.Decode([]byte("y"))
		if err != nil || got == nil || got.got != "bin:y" {
			t.Fatalf("Decode = (%+v, %v), want a fresh *binUnmarshaler that decoded bin:y", got, err)
		}
	})
	t.Run("any", func(t *testing.T) {
		t.Parallel()
		tc := codec.Adapt[any](codec.Raw())
		app := tc.(codec.AppendingTyped[any]) //nolint:forcetypeassert // Raw is an Appender
		for _, v := range []any{[]byte("bytes"), "bytes", marshalerOnly{body: "bytes"}} {
			encode(t, fmt.Sprintf("%T", v), func() ([]byte, error) { return tc.Encode(v) },
				func(dst []byte) ([]byte, error) { return app.AppendEncode(dst, v) }, "bytes")
		}
		encode(t, "nil", func() ([]byte, error) { return tc.Encode(nil) },
			func(dst []byte) ([]byte, error) { return app.AppendEncode(dst, nil) }, "")
		if _, err := tc.Encode(notRaw{1}); !errors.Is(err, codec.ErrNotRawValue) {
			t.Errorf("Encode(notRaw) = %v, want ErrNotRawValue", err)
		}
		recv := []byte("received")
		got, err := tc.Decode(recv)
		b, ok := got.([]byte)
		if err != nil || !ok || string(b) != "received" {
			t.Fatalf("Decode = (%#v, %v), want the body as a []byte", got, err)
		}
		recv[0] = 'X'
		if string(b) != "received" {
			t.Fatalf("Decode returned %q after the receive buffer changed: it aliases the buffer, where a *[]byte target is copied", b)
		}
	})
}

// cyclic is a pointer type defined in terms of itself, so a value of it can
// point at itself.
type cyclic *cyclic

// Following the indirection Adapt adds must stop somewhere. A *any holding its
// own address overflowed the stack, and a cyclic pointer to itself looped for
// ever; each is a value Raw has no arm for.
func TestRawRefusesAValueThatPointsAtItself(t *testing.T) {
	t.Parallel()
	c := codec.Raw()
	app := c.(codec.Appender) //nolint:forcetypeassert // Raw is an Appender
	var self any
	self = &self
	var p cyclic
	p = &p
	var held any
	pp := &held
	held = &pp
	for name, v := range map[string]any{"a *any holding itself": &self, "a cyclic pointer": p, "a *any through a **any": &held} {
		if _, err := c.Marshal(v); !errors.Is(err, codec.ErrNotRawValue) {
			t.Errorf("Marshal(%s) = %v, want ErrNotRawValue", name, err)
		}
		if out, err := app.AppendMarshal([]byte("PRE"), v); !errors.Is(err, codec.ErrNotRawValue) || string(out) != "PRE" {
			t.Errorf("AppendMarshal(%s) = (%q, %v), want ErrNotRawValue and the destination unchanged", name, out, err)
		}
		if err := c.Unmarshal([]byte("x"), v); !errors.Is(err, codec.ErrNotRawValue) {
			t.Errorf("Unmarshal(%s) = %v, want ErrNotRawValue", name, err)
		}
	}
	// Indirect and Target are bounded the same way, so a codec that uses them
	// gets a pointer back to refuse rather than a call that never returns.
	if got := codec.Indirect(p); got == nil {
		t.Error("Indirect(a cyclic pointer) = nil, want the pointer it stopped at")
	}
	if _, err := codec.Target(p); err != nil {
		t.Errorf("Target(a cyclic pointer) = %v, want the pointer it stopped at", err)
	}
	if _, err := codec.Target(&self); err != nil {
		t.Errorf("Target(a *any holding itself) = %v, want the pointer it stopped at", err)
	}
}

// A target Raw refuses is left as the caller passed it: Unmarshal into a
// **int allocated the *int and then refused it, so the caller's nil pointer
// was not nil after the error.
func TestRawRefusesATargetBeforeAllocatingThroughIt(t *testing.T) {
	t.Parallel()
	var p *int
	if err := codec.Raw().Unmarshal([]byte("x"), &p); !errors.Is(err, codec.ErrNotRawValue) {
		t.Fatalf("Unmarshal(**int) = %v, want ErrNotRawValue", err)
	}
	if p != nil {
		t.Fatalf("the refused *int is %p after the error, want it still nil", p)
	}
	var pp **notRaw
	if err := codec.Raw().Unmarshal([]byte("x"), &pp); !errors.Is(err, codec.ErrNotRawValue) || pp != nil {
		t.Fatalf("Unmarshal(***notRaw) = %v leaving %p, want ErrNotRawValue and nil", err, pp)
	}
}

func TestRawPassesAMessageHooksOwnFailureThrough(t *testing.T) {
	t.Parallel()
	// A message type that refuses to encode itself must not become
	// ErrNotRawValue: the caller's own diagnosis is the useful one, and
	// replacing it would tell an operator the value was the wrong shape when
	// the type said something else entirely.
	c := codec.Raw()
	app := c.(codec.Appender) //nolint:forcetypeassert // asserted above
	if _, err := c.Marshal(failingMarshaler{}); !errors.Is(err, errHook) {
		t.Errorf("Marshal swallowed the hook's error: %v", err)
	}
	out, err := app.AppendMarshal([]byte("PRE"), failingMarshaler{})
	if !errors.Is(err, errHook) {
		t.Errorf("AppendMarshal swallowed the hook's error: %v", err)
	}
	if string(out) != "PRE" {
		t.Errorf("AppendMarshal returned %q after a failed hook, want the destination unchanged", out)
	}
}

func TestRawMarshalOfAByteSliceIsTheZeroCopyPath(t *testing.T) {
	t.Parallel()
	// raw.go documents this: Marshal of a []byte returns THAT slice, which is
	// what the memory engine's Owned mode is built on. A copy here would be
	// correct but would silently cost an allocation per publication on the one
	// path that exists to avoid it, so the identity of the backing array is the
	// assertion.
	c := codec.Raw()
	body := []byte("the body")
	got, err := c.Marshal(body)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(got) == 0 || unsafe.SliceData(got) != unsafe.SliceData(body) {
		t.Error("Marshal of a []byte copied it; the zero-copy publish path allocates per message")
	}
}

func TestRawUnmarshalCopiesSoADecodedBodyNeverAliasesTheReceiveBuffer(t *testing.T) {
	t.Parallel()
	// This is the other half of the ownership contract, and it is the dangerous
	// half: a driver reuses its receive buffer, so a decoded body that aliased
	// it would change under the application after the handler returned.
	c := codec.Raw()
	recv := []byte("payload")
	var dst []byte
	if err := c.Unmarshal(recv, &dst); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(dst) != "payload" {
		t.Fatalf("dst = %q, want payload", dst)
	}
	if unsafe.SliceData(dst) == unsafe.SliceData(recv) {
		t.Fatal("Unmarshal into a *[]byte aliased the caller's buffer; the driver reusing it would change the decoded body")
	}
	// Prove it by mutating the "receive buffer" the way a driver would.
	copy(recv, "XXXXXXX")
	if string(dst) != "payload" {
		t.Errorf("dst became %q after the receive buffer was reused", dst)
	}
	// And the target's capacity is reused rather than reallocated, which is
	// what makes the decode path allocation-free for a caller that keeps one
	// buffer per handler.
	reused := make([]byte, 0, 64)
	if err := c.Unmarshal([]byte("short"), &reused); err != nil {
		t.Fatalf("Unmarshal into a reusable target: %v", err)
	}
	if cap(reused) != 64 {
		t.Errorf("capacity = %d, want the caller's 64 kept", cap(reused))
	}
	if string(reused) != "short" {
		t.Errorf("reused = %q, want short", reused)
	}
	// A previous, longer value must not survive at the tail.
	long := append(make([]byte, 0, 32), "aaaaaaaaaaaa"...)
	if err := c.Unmarshal([]byte("bb"), &long); err != nil {
		t.Fatalf("Unmarshal over a longer value: %v", err)
	}
	if string(long) != "bb" {
		t.Errorf("long = %q, want bb: the previous value was not truncated", long)
	}
}

func TestRawUnmarshalTargetsAndTheirPreferenceOrder(t *testing.T) {
	t.Parallel()
	c := codec.Raw()
	// codec.Unmarshaler wins over encoding.BinaryUnmarshaler when a type
	// implements both: the message hook is the one the type wrote for iqmq.
	both := &msgUnmarshaler{}
	if err := c.Unmarshal([]byte("x"), both); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if both.got != "msg:x" {
		t.Errorf("got %q, want msg:x -- BinaryUnmarshaler was preferred over Unmarshaler", both.got)
	}
	bin := &binUnmarshaler{}
	if err := c.Unmarshal([]byte("y"), bin); err != nil {
		t.Fatalf("Unmarshal into a BinaryUnmarshaler: %v", err)
	}
	if bin.got != "bin:y" {
		t.Errorf("got %q, want bin:y", bin.got)
	}
	var s string
	if err := c.Unmarshal([]byte("str"), &s); err != nil {
		t.Fatalf("Unmarshal into a *string: %v", err)
	}
	if s != "str" {
		t.Errorf("s = %q, want str", s)
	}
	for _, tc := range []struct {
		name string
		v    any
		want error
	}{
		{"an untyped nil target", nil, codec.ErrNilTarget},
		{"a nil pointer to a byte slice", (*[]byte)(nil), codec.ErrNilTarget},
		{"a nil pointer to a string", (*string)(nil), codec.ErrNilTarget},
		{"a non-pointer target", notRaw{}, codec.ErrNotRawValue},
		{"a pointer to an unsupported type", &notRaw{}, codec.ErrNotRawValue},
		{"a value target that cannot be written", 42, codec.ErrNotRawValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := c.Unmarshal([]byte("data"), tc.v); !errors.Is(err, tc.want) {
				t.Errorf("Unmarshal(%T) = %v, want %v", tc.v, err, tc.want)
			}
		})
	}
	if c.ContentType() != "application/octet-stream" {
		t.Errorf("ContentType = %q, want application/octet-stream: a consumer picks its decoder by it", c.ContentType())
	}
}
