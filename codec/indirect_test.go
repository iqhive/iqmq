package codec

import (
	"errors"
	"reflect"
	"testing"
)

type ind struct{ N int }

// TestIndirect pins the rule Indirect implements: the indirection Adapt adds
// is followed, and the pointer the caller meant is not.
func TestIndirect(t *testing.T) {
	p := &ind{N: 1}
	pp := &p
	ppp := &pp
	var iface any = p
	var chanIface any = make(chan int)
	var nilIface any
	body := []byte("x")
	var nilP *ind

	tests := []struct {
		name string
		in   any
		want any
	}{
		{"nil", nil, nil},
		{"pointer-to-struct stays", p, p},
		{"pointer-to-pointer is followed", pp, p},
		{"three levels are followed", ppp, p},
		{"pointer-to-interface is followed", &iface, p},
		{"pointer-to-interface holding a chan", &chanIface, chanIface},
		{"pointer-to-nil-interface stops", &nilIface, &nilIface},
		{"pointer-to-slice stays", &body, &body},
		{"value stays", ind{N: 2}, ind{N: 2}},
		{"nil typed pointer stays", nilP, nilP},
		{"pointer to nil typed pointer is followed to the nil", &nilP, nilP},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Indirect(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Indirect(%T) = %#v (%T), want %#v (%T)", tc.in, got, got, tc.want, tc.want)
			}
		})
	}
}

// TestIndirectIsWhatAdaptNeeds is the reason Indirect exists, stated as the
// call the adapter actually makes: Typed[T].Encode takes the address of its
// argument, so a codec bound to a pointer T is handed a pointer to a pointer.
func TestIndirectIsWhatAdaptNeeds(t *testing.T) {
	var seen any
	spy := spyCodec{onMarshal: func(v any) { seen = Indirect(v) }}
	want := &ind{N: 7}
	if _, err := Adapt[*ind](spy).Encode(want); err != nil {
		t.Fatal(err)
	}
	if seen != any(want) {
		t.Fatalf("Adapt[*ind].Encode handed the codec %#v; Indirect resolved it to %#v, want %#v", seen, seen, want)
	}
	// Without Indirect the codec sees a **ind, which is what a type assertion
	// on a concrete message interface rejects.
	raw := spyCodec{onMarshal: func(v any) { seen = v }}
	if _, err := Adapt[*ind](raw).Encode(want); err != nil {
		t.Fatal(err)
	}
	if _, isPtrToStruct := seen.(*ind); isPtrToStruct {
		t.Fatal("Adapt[*ind].Encode handed the codec a *ind, so Indirect would have nothing to do and this test proves nothing")
	}
	if _, isPtrToPtr := seen.(**ind); !isPtrToPtr {
		t.Fatalf("Adapt[*ind].Encode handed the codec a %T, want a **ind", seen)
	}
}

// TestTarget pins the decode side: Target allocates through the nil pointer
// Adapt's Decode leaves behind and rejects a target that is not a pointer.
func TestTarget(t *testing.T) {
	var nilP *ind
	for _, bad := range []any{nil, 42, "s", nilP} {
		if _, err := Target(bad); !errors.Is(err, ErrNilTarget) {
			t.Fatalf("Target(%#v) = %v, want ErrNilTarget", bad, err)
		}
	}

	v := ind{}
	got, err := Target(&v)
	if err != nil || got != any(&v) {
		t.Fatalf("Target(*ind) = %#v, %v", got, err)
	}

	// The Adapt[*ind].Decode shape: a **ind whose element is nil.
	var p *ind
	got, err = Target(&p)
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("Target did not allocate through the nil *ind, so a decoder would write into nothing")
	}
	if got != any(p) {
		t.Fatalf("Target returned %#v, want the allocated %#v", got, p)
	}

	// A *any with nothing in it has no type to allocate, so it comes back as
	// it went in and the codec reports what it can decode into.
	var a any
	got, err = Target(&a)
	if err != nil || got != any(&a) {
		t.Fatalf("Target(*any) = %#v, %v; want the *any itself", got, err)
	}

	// A *any holding a pointer is followed to that pointer.
	held := &ind{N: 3}
	a = held
	got, err = Target(&a)
	if err != nil || got != any(held) {
		t.Fatalf("Target(*any holding *ind) = %#v, %v; want %#v", got, err, held)
	}
}

// spyCodec records what a codec is handed. It encodes nothing.
type spyCodec struct{ onMarshal func(v any) }

func (s spyCodec) ContentType() string { return "application/x-spy" }

func (s spyCodec) Marshal(v any) ([]byte, error) {
	s.onMarshal(v)
	return nil, nil
}

func (spyCodec) Unmarshal([]byte, any) error { return nil }
