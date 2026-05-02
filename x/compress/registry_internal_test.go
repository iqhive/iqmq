package compress

import (
	"sync"
	"testing"
	"time"
)

// TestLookupTakesNoLock is what makes "Lookup is lock-free" a checked claim
// rather than a description.
//
// It holds the registry's WRITER lock -- the one Register takes -- and asks
// for a lookup. A Lookup that took any lock the writer side holds, which is
// what the RWMutex version did with RLock, cannot return until the lock is
// released, so it is still parked when the deadline passes. A Lookup that
// reads an immutable snapshot through an atomic pointer returns at once.
//
// The lock is taken through the field rather than through Register because a
// Register returns in nanoseconds: holding the writer side for two seconds is
// the only way to make a blocked reader distinguishable from a slow one.
func TestLookupTakesNoLock(t *testing.T) {
	r := NewRegistry()
	r.mu.Lock()
	defer r.mu.Unlock()

	done := make(chan Codec, 1)
	go func() {
		c, _ := r.Lookup(CodecGzip)
		done <- c
	}()
	select {
	case c := <-done:
		if c == nil || c.ID() != CodecGzip {
			t.Fatalf("Lookup(CodecGzip) = %v while a Register was in progress", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Lookup did not return while the registry's writer lock was held: " +
			"every decoded message would queue behind a Register")
	}
}

// TestRegisterIsCopyOnWrite: a snapshot a reader already holds is never
// written to. Register publishes a NEW table, so a reader that loaded the old
// one keeps a consistent view of it -- which is the property the lock-free
// read depends on, and which a Register that wrote into the published table in
// place would break without any lock-based test noticing.
func TestRegisterIsCopyOnWrite(t *testing.T) {
	r := NewRegistry()
	before := r.table.Load()
	if err := r.Register(testCodec{id: FirstUserCodec}); err != nil {
		t.Fatal(err)
	}
	after := r.table.Load()
	if before == after {
		t.Fatal("Register did not publish a new table; it wrote into the one readers hold")
	}
	if before[FirstUserCodec] != nil {
		t.Fatal("Register wrote into the snapshot a reader already held")
	}
	if after[FirstUserCodec] == nil {
		t.Fatal("the new table does not hold the registered codec")
	}
}

// TestZeroRegistryIsEmptyNotAPanic: the zero Registry is documented as empty.
// Its lookups find nothing and a Register on it works, rather than
// dereferencing a table nobody published.
func TestZeroRegistryIsEmptyNotAPanic(t *testing.T) {
	var r Registry
	if c, ok := r.Lookup(CodecGzip); ok || c != nil {
		t.Fatalf("the zero Registry's Lookup = (%v, %v), want (nil, false)", c, ok)
	}
	if n := len(r.Names()); n != 0 {
		t.Fatalf("the zero Registry names %d codecs", n)
	}
	if err := r.Register(testCodec{id: FirstUserCodec}); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Lookup(FirstUserCodec); !ok {
		t.Fatal("a codec registered on the zero Registry is not found")
	}
}

// testCodec is the smallest user codec, for the internal tests above.
type testCodec struct{ id CodecID }

func (c testCodec) ID() CodecID                          { return c.id }
func (testCodec) Name() string                           { return "test" }
func (testCodec) Encode(dst, src []byte) ([]byte, error) { return append(dst, src...), nil }
func (testCodec) Decode(dst, src []byte, _ int64) ([]byte, error) {
	return append(dst, src...), nil
}

// TestLookupRacesRegister is the race detector's view of the copy-on-write:
// sixteen readers look up every id while a writer registers the whole user
// range. A reader must only ever see a nil slot or a codec whose ID is the
// slot's -- a torn or recycled table would show up as the wrong codec, and a
// data race as a -race report.
func TestLookupRacesRegister(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for id := range 256 {
					c, ok := r.Lookup(CodecID(id))
					if ok && c.ID() != CodecID(id) {
						t.Errorf("Lookup(%d) returned the codec for %d", id, c.ID())
						return
					}
					if ok != (c != nil) {
						t.Errorf("Lookup(%d) = (%v, %v): ok and the codec disagree", id, c, ok)
						return
					}
				}
			}
		}()
	}
	for id := int(FirstUserCodec); id < 256; id++ {
		if err := r.Register(testCodec{id: CodecID(id)}); err != nil {
			t.Errorf("Register(%d) = %v", id, err)
		}
	}
	close(stop)
	wg.Wait()
	if got := len(r.Names()); got != 4+256-int(FirstUserCodec) {
		t.Fatalf("the registry names %d codecs after the storm, want %d", got, 4+256-int(FirstUserCodec))
	}
}

// TestAllocsLookup: the lookup is on every decoded message's path and
// allocates nothing. The registry is built outside the measured closure (the
// mistake the 840 allocs/op came from was building it inside).
func TestAllocsLookup(t *testing.T) {
	r := NewRegistry()
	var c Codec
	var ok bool
	if got := testing.AllocsPerRun(1000, func() {
		c, ok = r.Lookup(CodecGzip)
	}); got != 0 {
		t.Errorf("Lookup allocs/op = %v, want 0", got)
	}
	if !ok || c.ID() != CodecGzip {
		t.Fatalf("Lookup(CodecGzip) = (%v, %v)", c, ok)
	}
}
