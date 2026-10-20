package pool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// fuzzState is the reference model: the set of live objects and buffers
// with the stamps their holders wrote, and the items Put since (idle) for
// the debug double-Put step.
type fuzzState struct {
	objs    ObjectPool[item]
	bufs    BufferPool
	want    item // reset state
	maxSize int
	live    map[*item]uint64
	order   []*item
	idle    []*item
	liveB   []fuzzBuf
	idleB   [][]byte
	next    uint64
}

type fuzzBuf struct {
	b  []byte
	id byte
}

// checkLive asserts the pool has not touched any live item.
func (s *fuzzState) checkLive(t testing.TB) {
	t.Helper()
	for v, id := range s.live {
		if !intact(v, id) {
			t.Fatalf("live object %d corrupted: %+v", id, *v)
		}
	}
	for i, lb := range s.liveB {
		for j, c := range lb.b {
			if c != lb.id {
				t.Fatalf("live buffer %d byte %d = %#x, want %#x", i, j, c, lb.id)
			}
		}
	}
}

func (s *fuzzState) forgetIdle(v *item) {
	for i, w := range s.idle {
		if w == v {
			s.idle[i] = s.idle[len(s.idle)-1]
			s.idle = s.idle[:len(s.idle)-1]
			return
		}
	}
}

func (s *fuzzState) forgetIdleBuf(key *byte) {
	for i, b := range s.idleB {
		if bufKey(b) == key {
			s.idleB[i] = s.idleB[len(s.idleB)-1]
			s.idleB = s.idleB[:len(s.idleB)-1]
			return
		}
	}
}

// poolScript is the op script shared by the seeds and the fuzz body.
var poolScript = fuzzutil.Script[*fuzzState]{
	Steps: []fuzzutil.Step[*fuzzState]{
		{Name: "get", Weight: 4, Run: func(t testing.TB, _ *fuzzutil.Reader, s *fuzzState) {
			if len(s.order) >= 48 {
				return
			}
			v := s.objs.Get()
			if v == nil {
				t.Fatal("Get returned nil")
			}
			if *v != s.want {
				t.Fatalf("Get = %+v, want reset state %+v", *v, s.want)
			}
			if _, dup := s.live[v]; dup {
				t.Fatal("Get returned a live object")
			}
			s.forgetIdle(v)
			s.next++
			stamp(v, s.next)
			s.live[v] = s.next
			s.order = append(s.order, v)
		}},
		{Name: "put", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			if len(s.order) == 0 {
				return
			}
			i := r.Choice(len(s.order))
			v := s.order[i]
			s.order[i] = s.order[len(s.order)-1]
			s.order = s.order[:len(s.order)-1]
			if !intact(v, s.live[v]) {
				t.Fatalf("object modified while live: %+v", *v)
			}
			delete(s.live, v)
			s.objs.Put(v)
			s.idle = append(s.idle, v)
		}},
		{Name: "getbuf", Weight: 4, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			if len(s.liveB) >= 32 {
				return
			}
			n := r.Int(0, s.maxSize+64)
			b := s.bufs.Get(n)
			if len(b) != n || cap(b) < n {
				t.Fatalf("Get(%d): len %d cap %d", n, len(b), cap(b))
			}
			if n > s.maxSize && cap(b) != n {
				t.Fatalf("Get(%d) beyond MaxSize %d: cap %d", n, s.maxSize, cap(b))
			}
			if n > 0 && n <= s.maxSize && cap(b) > s.maxSize {
				t.Fatalf("Get(%d): cap %d exceeds MaxSize %d", n, cap(b), s.maxSize)
			}
			if n == 0 {
				return
			}
			key := bufKey(b)
			for _, lb := range s.liveB {
				if bufKey(lb.b) == key {
					t.Fatal("Get returned a live buffer")
				}
			}
			s.forgetIdleBuf(key)
			id := byte(s.next + 1)
			s.next++
			for i := range b {
				b[i] = id
			}
			s.liveB = append(s.liveB, fuzzBuf{b: b, id: id})
		}},
		{Name: "putbuf", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			if len(s.liveB) == 0 {
				return
			}
			i := r.Choice(len(s.liveB))
			lb := s.liveB[i]
			s.liveB[i] = s.liveB[len(s.liveB)-1]
			s.liveB = s.liveB[:len(s.liveB)-1]
			for _, c := range lb.b {
				if c != lb.id {
					t.Fatal("buffer modified while live")
				}
			}
			if r.Bool() {
				lb.b = lb.b[:len(lb.b)/2] // Put accepts any length of the same storage
			}
			s.bufs.Put(lb.b)
			s.idleB = append(s.idleB, lb.b)
		}},
		{Name: "foreign", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			s.bufs.Put(make([]byte, r.Int(0, s.maxSize+64)))
			s.objs.Put(nil)
		}},
		{Name: "doubleput", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			// Only iqmq_debug builds define the outcome: a panic.
			useBuf := r.Bool()
			i := r.Int(0, 63)
			if !Debug {
				return
			}
			var fn func()
			switch {
			case useBuf && len(s.idleB) > 0:
				fn = func() { s.bufs.Put(s.idleB[i%len(s.idleB)]) }
			case !useBuf && len(s.idle) > 0:
				fn = func() { s.objs.Put(s.idle[i%len(s.idle)]) }
			default:
				return
			}
			got := func() (msg any) {
				defer func() { msg = recover() }()
				fn()
				return nil
			}()
			if got != DoublePutPanic {
				t.Fatalf("double Put recovered %v, want %q", got, DoublePutPanic)
			}
		}},
	},
	Check: func(t testing.TB, s *fuzzState) { s.checkLive(t) },
}

// decodePoolHeader reads the pool configuration that precedes the script.
func decodePoolHeader(r *fuzzutil.Reader) (k Kind, cfg Config[item], bcfg BufferConfig, want item) {
	k = Kinds()[r.Choice(len(Kinds()))]
	cfg = Config[item]{MaxIdle: r.Int(1, 16), Chunk: r.Int(1, 8)}
	if r.Bool() {
		cfg.Reset = func(v *item) { *v = item{a: 7} }
		want = item{a: 7}
	}
	bcfg = BufferConfig{MaxIdle: r.Int(1, 16), MaxSize: r.Int(16, 512), Chunk: r.Int(16, 1024)}
	return k, cfg, bcfg, want
}

func writePoolHeader(w *fuzzutil.Writer, ki int, custom bool) {
	w.Choice(len(Kinds()), ki)
	w.Int(1, 16, 4)
	w.Int(1, 8, 3)
	w.Bool(custom)
	w.Int(1, 16, 4)
	w.Int(16, 512, 200)
	w.Int(16, 1024, 512)
}

// FuzzPool drives one kind with a random operation script against the
// ownership model: every Get yields a reset object nobody else holds, the
// pool never modifies a live item, buffer geometry matches the request and
// MaxSize, and (iqmq_debug) a double Put panics.
func FuzzPool(f *testing.F) {
	for ki := range Kinds() {
		for _, custom := range []bool{false, true} {
			w := fuzzutil.NewWriter()
			writePoolHeader(w, ki, custom)
			for range 4 {
				poolScript.WriteStep(f, w, "get")
			}
			poolScript.WriteStep(f, w, "getbuf")
			w.Int(0, 264, 100)
			poolScript.WriteStep(f, w, "getbuf")
			w.Int(0, 264, 300)
			poolScript.WriteStep(f, w, "put")
			w.Choice(4, 1)
			poolScript.WriteStep(f, w, "putbuf")
			w.Choice(2, 0)
			w.Bool(false)
			poolScript.WriteStep(f, w, "get")
			poolScript.WriteStep(f, w, "getbuf")
			w.Int(0, 264, 100)
			poolScript.WriteStep(f, w, "foreign")
			w.Int(0, 264, 40)
			poolScript.WriteStep(f, w, "doubleput")
			w.Bool(false)
			w.Int(0, 63, 0)
			poolScript.WriteStep(f, w, "put")
			w.Choice(4, 0)
			f.Add(w.Data())
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k, cfg, bcfg, want := decodePoolHeader(r)
		s := &fuzzState{
			objs:    mustNew(t, k, cfg),
			bufs:    mustNewBuffers(t, k, bcfg),
			want:    want,
			maxSize: bcfg.maxSize(),
			live:    map[*item]uint64{},
		}
		poolScript.Run(t, data[len(data)-r.Remaining():], s)
	})
}

// FuzzPoolConcurrent shares one pool between fuzz-chosen numbers of actors
// that each Get, stamp, verify and Put with fuzz-driven holding patterns.
// Aliasing shows up as a corrupted stamp or a data race.
func FuzzPoolConcurrent(f *testing.F) {
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		writePoolHeader(w, ki, false)
		w.Int(1, 6, 3)
		w.Int(1, 200, 40)
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k, cfg, bcfg, want := decodePoolHeader(r)
		actors, per := r.Int(1, 6), r.Int(1, 200)
		objs := mustNew(t, k, cfg)
		bufs := mustNewBuffers(t, k, bcfg)
		maxSize := bcfg.maxSize()
		var failed atomic.Bool
		var wg sync.WaitGroup
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], actors, func(id int, ar *fuzzutil.Reader) {
			wg.Add(1)
			defer wg.Done()
			const hold = 6
			base := uint64(id+1) << 32
			ho := make([]*item, hold)
			hb := make([][]byte, hold)
			for i := 0; i < per && !failed.Load(); i++ {
				j := ar.Int(0, hold-1)
				if ho[j] != nil {
					if !intact(ho[j], base|uint64(i-1)) && !intact(ho[j], ho[j].a) {
						failed.Store(true)
						return
					}
					objs.Put(ho[j])
					ho[j] = nil
				}
				if ar.Bool() {
					v := objs.Get()
					if *v != want {
						failed.Store(true)
						return
					}
					stamp(v, base|uint64(i))
					ho[j] = v
				}
				if hb[j] != nil {
					for _, c := range hb[j] {
						if c != byte(id+1) {
							failed.Store(true)
							return
						}
					}
					bufs.Put(hb[j])
					hb[j] = nil
				}
				if ar.Bool() {
					n := ar.Int(0, maxSize+16)
					b := bufs.Get(n)
					if len(b) != n || cap(b) < n {
						failed.Store(true)
						return
					}
					for x := range b {
						b[x] = byte(id + 1)
					}
					hb[j] = b
				}
				if ar.Bool() {
					runtime.Gosched()
				}
			}
			for j := range hold {
				objs.Put(ho[j])
				if hb[j] != nil {
					bufs.Put(hb[j])
				}
			}
		})
		wg.Wait()
		if failed.Load() {
			t.Fatalf("%v actors=%d per=%d: aliasing or corruption detected", k, actors, per)
		}
	})
}
