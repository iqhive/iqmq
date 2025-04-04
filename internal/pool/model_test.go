package pool

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
)

// item is the object type the model tests and fuzz targets pool.
type item struct{ a, b, c uint64 }

// stamp fills v with a pattern derived from id that the holder can verify.
func stamp(v *item, id uint64) { *v = item{a: id, b: ^id, c: id * 3} }

// intact reports whether v still carries stamp(id).
func intact(v *item, id uint64) bool { return v.a == id && v.b == ^id && v.c == id*3 }

// TestModel checks the pooling contract of every kind: the reset invariant,
// exclusive ownership (no aliasing between live items), buffer geometry,
// retention, bounds, GC interplay, concurrent use and the debug canaries.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("reset", func(t *testing.T) { modelReset(t, k) })
			t.Run("alias", func(t *testing.T) { modelAlias(t, k) })
			t.Run("buffers", func(t *testing.T) { modelBuffers(t, k) })
			t.Run("retain", func(t *testing.T) { modelRetain(t, k) })
			t.Run("bounds", func(t *testing.T) { modelBounds(t, k) })
			t.Run("gc", func(t *testing.T) { modelGC(t, k) })
			t.Run("concurrent", func(t *testing.T) { modelConcurrent(t, k) })
			t.Run("debug", func(t *testing.T) { modelDebug(t, k) })
		})
	}
}

func modelReset(t *testing.T, k Kind) {
	check := func(p ObjectPool[item], want item) {
		t.Helper()
		held := make([]*item, 32)
		for i := range held {
			held[i] = p.Get()
			if *held[i] != want {
				t.Fatalf("fresh Get = %+v, want %+v", *held[i], want)
			}
			stamp(held[i], uint64(i+1))
		}
		for _, v := range held {
			p.Put(v)
		}
		for i := range 64 {
			v := p.Get()
			if *v != want {
				t.Fatalf("Get %d after dirty Put = %+v, want %+v", i, *v, want)
			}
			stamp(v, 99)
			p.Put(v)
		}
		p.Put(nil) // ignored
	}
	check(New(k, Config[item]{MaxIdle: 8, Chunk: 4}), item{})
	check(New(k, Config[item]{Reset: func(v *item) { *v = item{a: 7} }}), item{a: 7})
}

// modelAlias interleaves Get and Put and checks that no live object is ever
// handed out twice and that the pool never touches a live object.
func modelAlias(t *testing.T, k Kind) {
	p := New(k, Config[item]{MaxIdle: 6, Chunk: 3})
	live := map[*item]uint64{}
	var order []*item
	rng := rand.New(rand.NewPCG(uint64(k), 1))
	var next uint64
	for step := range 5000 {
		if len(order) == 0 || rng.IntN(3) > 0 && len(order) < 40 {
			v := p.Get()
			if v == nil {
				t.Fatalf("step %d: nil Get", step)
			}
			if *v != (item{}) {
				t.Fatalf("step %d: Get returned %+v", step, *v)
			}
			if _, dup := live[v]; dup {
				t.Fatalf("step %d: Get returned a live object", step)
			}
			next++
			stamp(v, next)
			live[v] = next
			order = append(order, v)
		} else {
			i := rng.IntN(len(order))
			v := order[i]
			order[i] = order[len(order)-1]
			order = order[:len(order)-1]
			if !intact(v, live[v]) {
				t.Fatalf("step %d: live object modified by the pool: %+v", step, *v)
			}
			delete(live, v)
			p.Put(v)
		}
		for v, id := range live {
			if !intact(v, id) {
				t.Fatalf("step %d: live object %d corrupted", step, id)
			}
		}
	}
}

func modelBuffers(t *testing.T, k Kind) {
	const maxSize = 1024
	p := NewBuffers(k, BufferConfig{MaxIdle: 8, MaxSize: maxSize, Chunk: 256})
	sizes := []int{0, 1, 15, 16, 17, 100, 255, 256, 257, 1023, maxSize, maxSize + 1, 5000}
	for round := range 3 {
		for _, n := range sizes {
			b := p.Get(n)
			if len(b) != n || cap(b) < n {
				t.Fatalf("Get(%d) len=%d cap=%d", n, len(b), cap(b))
			}
			switch {
			case n > maxSize && cap(b) != n:
				t.Fatalf("Get(%d) beyond MaxSize should be exact, cap=%d", n, cap(b))
			case n > 0 && n <= maxSize && cap(b) > maxSize:
				t.Fatalf("Get(%d) cap=%d exceeds MaxSize", n, cap(b))
			case k.Classed() && n > 0 && n <= maxSize && cap(b) != classOf(n):
				t.Fatalf("round %d: classed Get(%d) cap=%d, want %d", round, n, cap(b), classOf(n))
			}
			for i := range b {
				b[i] = byte(n)
			}
			p.Put(b)
		}
	}
	// Foreign and degenerate puts are tolerated.
	p.Put(nil)
	p.Put([]byte{})
	p.Put(make([]byte, 5))
	p.Put(make([]byte, 100))
	p.Put(make([]byte, 3000))
	for _, n := range sizes {
		b := p.Get(n)
		if len(b) != n || cap(b) < n || n <= maxSize && n > 0 && cap(b) > maxSize {
			t.Fatalf("after foreign puts: Get(%d) len=%d cap=%d", n, len(b), cap(b))
		}
	}
	// Two live buffers never overlap: writing one never changes another.
	live := make([][]byte, 0, 64)
	for i := range 64 {
		n := 1 + (i*37)%300
		b := p.Get(n)
		for j := range b {
			b[j] = byte(i)
		}
		live = append(live, b)
		if i%5 == 4 {
			p.Put(live[0])
			live = live[1:]
		}
		for j, lb := range live {
			id := byte(i - len(live) + 1 + j)
			for _, c := range lb {
				if c != id {
					t.Fatalf("live buffer %d changed to %#x (id %#x)", j, c, id)
				}
			}
		}
	}
	for _, b := range live {
		p.Put(b)
	}
}

func modelRetain(t *testing.T, k Kind) {
	objs := New(k, Config[item]{})
	bufs := NewBuffers(k, BufferConfig{})
	v := objs.Get()
	b := bufs.Get(64)
	first := bufKey(b)
	objs.Put(v)
	bufs.Put(b)
	w := objs.Get()
	c := bufs.Get(64)
	switch {
	case !k.Retains():
		if w == v || bufKey(c) == first {
			t.Fatal("None returned a Put item")
		}
	case k.GCCleared():
		t.Logf("sync.Pool reuse: object=%v buffer=%v (not guaranteed)", w == v, bufKey(c) == first)
	default:
		if w != v {
			t.Fatal("Put then Get did not return the idle object")
		}
		if bufKey(c) != first {
			t.Fatal("Put then Get did not return the idle buffer")
		}
	}
	runtime.KeepAlive(v)
	runtime.KeepAlive(b)
}

// modelBounds checks that bounded kinds retain at most MaxIdle idle items
// (per class for buffers) and that the bound is actually reached.
func modelBounds(t *testing.T, k Kind) {
	if !k.Bounded() {
		t.Skip("unbounded kind")
	}
	const maxIdle = 8
	objs := New(k, Config[item]{MaxIdle: maxIdle})
	put := map[*item]bool{}
	held := make([]*item, 4*maxIdle)
	for i := range held {
		held[i] = objs.Get()
	}
	for _, v := range held {
		put[v] = true
		objs.Put(v)
	}
	reused := 0
	for range held {
		if put[objs.Get()] {
			reused++
		}
	}
	if reused != maxIdle {
		t.Fatalf("objects: %d of %d Put objects came back, want exactly MaxIdle=%d", reused, len(held), maxIdle)
	}
	bufs := NewBuffers(k, BufferConfig{MaxIdle: maxIdle, MaxSize: 256})
	putB := map[*byte]bool{}
	heldB := make([][]byte, 4*maxIdle)
	for i := range heldB {
		heldB[i] = bufs.Get(64)
	}
	for _, b := range heldB {
		putB[bufKey(b)] = true
		bufs.Put(b)
	}
	reused = 0
	for range heldB {
		if putB[bufKey(bufs.Get(64))] {
			reused++
		}
	}
	if reused != maxIdle {
		t.Fatalf("buffers: %d of %d Put buffers came back, want exactly MaxIdle=%d", reused, len(heldB), maxIdle)
	}
	// A second class is bounded independently.
	for range 2 * maxIdle {
		bufs.Put(bufs.Get(200))
	}
	if got := bufs.Get(200); len(got) != 200 {
		t.Fatal("class 256")
	}
}

// modelGC: the garbage collector clears SyncPool but no other retaining kind.
func modelGC(t *testing.T, k Kind) {
	if !k.Retains() {
		t.Skip("nothing retained")
	}
	objs := New(k, Config[item]{})
	bufs := NewBuffers(k, BufferConfig{})
	v := objs.Get()
	b := bufs.Get(128)
	key := bufKey(b)
	objs.Put(v)
	bufs.Put(b)
	runtime.GC()
	runtime.GC()
	w := objs.Get()
	c := bufs.Get(128)
	if k.GCCleared() {
		if w == v || bufKey(c) == key {
			t.Fatal("sync.Pool retained across two GC cycles")
		}
	} else if w != v || bufKey(c) != key {
		t.Fatal("idle items lost across GC")
	}
	runtime.KeepAlive(v)
	runtime.KeepAlive(b)
}

// modelConcurrent has several goroutines share one pool, each stamping the
// objects and buffers it holds and verifying them before Put. Aliasing
// shows up as a corrupted stamp or as a data race.
func modelConcurrent(t *testing.T, k Kind) {
	const (
		workers = 8
		iters   = 3000
		hold    = 4
	)
	objs := New(k, Config[item]{MaxIdle: 16, Chunk: 4})
	bufs := NewBuffers(k, BufferConfig{MaxIdle: 16, MaxSize: 512, Chunk: 1024})
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := uint64(w+1) << 32
			ho := make([]*item, hold)
			hb := make([][]byte, hold)
			for i := range iters {
				j := i % hold
				if ho[j] != nil {
					if !intact(ho[j], id|uint64(i-hold)) {
						errs <- "object stamp corrupted"
						return
					}
					objs.Put(ho[j])
				}
				v := objs.Get()
				if *v != (item{}) {
					errs <- "Get returned a non-reset object"
					return
				}
				stamp(v, id|uint64(i))
				ho[j] = v
				if hb[j] != nil {
					for _, c := range hb[j] {
						if c != byte(w+1) {
							errs <- "buffer contents corrupted"
							return
						}
					}
					bufs.Put(hb[j])
				}
				n := 1 + (i*13+w)%600
				b := bufs.Get(n)
				if len(b) != n || cap(b) < n {
					errs <- "buffer geometry"
					return
				}
				for x := range b {
					b[x] = byte(w + 1)
				}
				hb[j] = b
				if i%64 == 0 {
					runtime.Gosched()
				}
			}
			for j := range hold {
				objs.Put(ho[j])
				bufs.Put(hb[j])
			}
		}()
	}
	wg.Wait()
	select {
	case msg := <-errs:
		t.Fatal(msg)
	default:
	}
}

// modelDebug exercises the iqmq_debug canaries: a double Put panics with
// DoublePutPanic for objects and buffers, a Put after a Get does not, and
// recycled buffers are poisoned.
func modelDebug(t *testing.T, k Kind) {
	if !Debug {
		t.Skip("release build: double Put is undefined and not exercised")
	}
	panics := func(fn func()) (msg any) {
		defer func() { msg = recover() }()
		fn()
		return nil
	}
	objs := New(k, Config[item]{})
	v := objs.Get()
	objs.Put(v)
	if got := panics(func() { objs.Put(v) }); got != DoublePutPanic {
		t.Fatalf("double Put of an object: recovered %v", got)
	}
	// A fresh Get lifts the canary for whatever comes back.
	w := objs.Get()
	if got := panics(func() { objs.Put(w) }); got != nil {
		t.Fatalf("Put after Get panicked: %v", got)
	}
	bufs := NewBuffers(k, BufferConfig{MaxSize: 256})
	b := bufs.Get(100)
	for i := range b {
		b[i] = 1
	}
	bufs.Put(b)
	if k.Retains() {
		for i, c := range b[:cap(b)] {
			if c != PoisonByte {
				t.Fatalf("recycled buffer byte %d = %#x, not poisoned", i, c)
			}
		}
	}
	if got := panics(func() { bufs.Put(b) }); got != DoublePutPanic {
		t.Fatalf("double Put of a buffer: recovered %v", got)
	}
	if got := panics(func() { bufs.Put(b[:10]) }); got != DoublePutPanic {
		t.Fatalf("double Put of a resliced buffer: recovered %v", got)
	}
	big := bufs.Get(5000) // never pooled, still tracked
	bufs.Put(big)
	if got := panics(func() { bufs.Put(big) }); got != DoublePutPanic {
		t.Fatalf("double Put of an oversize buffer: recovered %v", got)
	}
	c := bufs.Get(100)
	if got := panics(func() { bufs.Put(c) }); got != nil {
		t.Fatalf("Put after Get panicked: %v", got)
	}
	// Address reuse after collection is not a double Put: the canary keys
	// on the object, not the address.
	none := New(None, Config[item]{})
	for range 4 * canaryPruneFloor {
		none.Put(none.Get())
	}
	runtime.GC()
	for range 4 * canaryPruneFloor {
		if got := panics(func() { none.Put(none.Get()) }); got != nil {
			t.Fatalf("false double Put after address reuse: %v", got)
		}
	}
}
