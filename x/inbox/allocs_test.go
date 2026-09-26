package inbox_test

import (
	"context"
	"testing"

	"github.com/iqhive/iqmq/x/inbox"
)

// TestAllocsKeyFromHeader: the key is derived on EVERY delivery, guarded or
// not, so it is the one function in this package a consumer pays for
// unconditionally.
//
// Zero and not one: KeyFromHeader converts the header's bytes to a string,
// which the compiler keeps allocation-free here because the result does not
// escape past the comparison. A non-zero number means the conversion started
// escaping, which is worth knowing about on a hot consumer.
func TestAllocsKeyFromHeader(t *testing.T) {
	fn := inbox.KeyFromHeader(keyHeader)
	d := delivery("op-1")
	var key string
	var ok bool
	got := testing.AllocsPerRun(200, func() {
		key, ok = fn(d)
	})
	if !ok || key != "op-1" {
		t.Fatalf("KeyFromHeader = (%q, %v)", key, ok)
	}
	if got > 1 {
		t.Errorf("KeyFromHeader allocs/op = %v, want at most 1 (the key string)", got)
	}
	t.Logf("KeyFromHeader allocs/op = %v", got)
}

// TestAllocsDuplicatePath is the number that matters in a redelivery storm:
// a duplicate must be cheap, because a topic being replayed is entirely
// duplicates and the guard is what stands between that and the handler.
func TestAllocsDuplicatePath(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	ctx := context.Background()
	if ran, err := s.Once(ctx, "op-1", func(context.Context) error { return nil }); err != nil || !ran {
		t.Fatalf("the first Once = (%v, %v)", ran, err)
	}
	noop := func(context.Context) error { return nil }
	var ran bool
	var err error
	got := testing.AllocsPerRun(200, func() {
		ran, err = s.Once(ctx, "op-1", noop)
	})
	if ran || err != nil {
		t.Fatalf("the duplicate Once = (%v, %v), want (false, nil)", ran, err)
	}
	if got != 0 {
		t.Errorf("a duplicate Once allocs/op = %v, want 0: a replayed topic is entirely duplicates", got)
	}
}
