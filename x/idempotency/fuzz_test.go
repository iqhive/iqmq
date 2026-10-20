package idempotency_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/x/idempotency"
)

// The op script's alphabet. The key space is deliberately tiny so that the
// fuzzer spends its budget on INTERLEAVINGS of claim, commit, abort and the
// passage of time rather than on distinct strings: every interesting state of
// this store is two claims of one key with something between them.
const (
	fuzzKeys      = 6
	fuzzUnit      = time.Second
	fuzzMaxWindow = 8
	fuzzMaxJump   = 12 * fuzzUnit
)

// claimWorld is the store, the model, and the claims handed out so far.
type claimWorld struct {
	store  *idempotency.MemoryStore
	clk    *testClock
	model  map[string]*modelRow
	claims []idempotency.Claim
}

// modelRow is the reference implementation: what a correct store must answer.
type modelRow struct {
	token   string
	done    bool
	expires time.Time
	window  time.Duration
}

// live reports the row for key if its window has not lapsed, and drops it if
// it has — which is exactly what the store's own Claim does.
func (w *claimWorld) live(key string) (*modelRow, bool) {
	r, ok := w.model[key]
	if !ok {
		return nil, false
	}
	if !w.clk.Now().Before(r.expires) {
		delete(w.model, key)
		return nil, false
	}
	return r, true
}

// FuzzClaimStore is the differential fuzz target for the in-process store: the
// same op script drives the store and a map, and every verdict is compared.
//
// It is the check that the store's THREE states and its token comparison hold
// under interleavings a test would not think to write — a commit after the
// window, an abort of a key another claim has taken, a re-claim in the same
// instant the window lapses. The oracle is a map and a clock, which is small
// enough to read and therefore small enough to trust.
func FuzzClaimStore(f *testing.F) {
	for _, seed := range claimSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		clk := newTestClock()
		store, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{
			// Far more than the script can reach, so that an eviction never
			// makes the store legitimately disagree with the model. Eviction
			// is a stated loss and has its own test; mixing it in here would
			// make every divergence ambiguous.
			MaxEntries: 1024,
			Clock:      clk,
		})
		if err != nil {
			t.Fatalf("NewMemoryStore: %v", err)
		}
		defer func() { _ = store.Close() }()
		w := &claimWorld{store: store, clk: clk, model: map[string]*modelRow{}}
		claimScript().Run(t, data, w)
	})
}

// claimScript is the op script, shared by the target and the seed writer so
// that a seed cannot encode a step the target does not have.
func claimScript() *fuzzutil.Script[*claimWorld] {
	return &fuzzutil.Script[*claimWorld]{
		MaxSteps: 96,
		Steps: []fuzzutil.Step[*claimWorld]{
			{Name: "claim", Weight: 4, Run: stepClaim},
			{Name: "commit", Weight: 2, Run: stepCommit},
			{Name: "abort", Weight: 2, Run: stepAbort},
			{Name: "advance", Weight: 1, Run: stepAdvance},
		},
		Check: func(t testing.TB, w *claimWorld) {
			st := w.store.Stats()
			if st.First+st.Duplicates+st.InFlight != st.Claims {
				t.Fatalf("the in-process store answered %d claims as %d first + %d duplicate + %d in-flight; it cannot answer unknown",
					st.Claims, st.First, st.Duplicates, st.InFlight)
			}
			if st.Evicted != 0 {
				t.Fatalf("Stats().Evicted = %d: the script reached MaxEntries and the model cannot model that", st.Evicted)
			}
		},
	}
}

func stepClaim(t testing.TB, r *fuzzutil.Reader, w *claimWorld) {
	key := "k" + strconv.Itoa(r.Int(0, fuzzKeys-1))
	window := time.Duration(r.Int(1, fuzzMaxWindow)) * fuzzUnit
	want := idempotency.StatusFirst
	if row, ok := w.live(key); ok {
		want = idempotency.StatusInFlight
		if row.done {
			want = idempotency.StatusDuplicate
		}
	}
	got, err := w.store.Claim(t.Context(), key, window)
	if err != nil {
		t.Fatalf("Claim(%q, %v) reported %v", key, window, err)
	}
	if got.Status != want {
		t.Fatalf("Claim(%q) answered %v, the model says %v", key, got.Status, want)
	}
	switch want {
	case idempotency.StatusFirst:
		if got.Token == "" {
			t.Fatalf("Claim(%q) answered first with no token", key)
		}
		w.model[key] = &modelRow{token: got.Token, expires: w.clk.Now().Add(window), window: window}
		w.claims = append(w.claims, got)
	default:
		if got.Token != "" {
			t.Fatalf("Claim(%q) answered %v and still carried a token", key, got.Status)
		}
	}
}

func stepCommit(t testing.TB, r *fuzzutil.Reader, w *claimWorld) {
	c, ok := w.pick(r)
	if !ok {
		return
	}
	row, live := w.live(c.Key)
	wantLost := !live || row.token != c.Token
	err := w.store.Commit(t.Context(), c)
	if wantLost {
		if !errors.Is(err, idempotency.ErrClaimLost) {
			t.Fatalf("Commit of a stale claim on %q reported %v, want ErrClaimLost", c.Key, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("Commit of a live claim on %q reported %v", c.Key, err)
	}
	row.done = true
	row.expires = w.clk.Now().Add(row.window)
}

func stepAbort(t testing.TB, r *fuzzutil.Reader, w *claimWorld) {
	c, ok := w.pick(r)
	if !ok {
		return
	}
	row, live := w.live(c.Key)
	wantLost := !live || row.token != c.Token
	err := w.store.Abort(t.Context(), c)
	if wantLost {
		if !errors.Is(err, idempotency.ErrClaimLost) {
			t.Fatalf("Abort of a stale claim on %q reported %v, want ErrClaimLost", c.Key, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("Abort of a live claim on %q reported %v", c.Key, err)
	}
	delete(w.model, c.Key)
}

func stepAdvance(t testing.TB, r *fuzzutil.Reader, w *claimWorld) {
	_ = t
	w.clk.Advance(r.Duration(fuzzMaxJump))
}

// pick chooses one of the claims handed out so far, stale ones included: a
// stale claim is the interesting input, because it is the one the token
// comparison exists for.
func (w *claimWorld) pick(r *fuzzutil.Reader) (idempotency.Claim, bool) {
	if len(w.claims) == 0 {
		return idempotency.Claim{}, false
	}
	return w.claims[r.Int(0, len(w.claims)-1)], true
}

// claimSeeds authors the two interleavings that must be in the corpus: the
// lifecycle (claim, duplicate, window, claim) and the stale commit the token
// comparison refuses.
func claimSeeds(tb testing.TB) [][]byte {
	sc := claimScript()
	lifecycle := fuzzutil.NewWriter()
	sc.WriteStep(tb, lifecycle, "claim")
	lifecycle.Int(0, fuzzKeys-1, 0)
	lifecycle.Int(1, fuzzMaxWindow, 2)
	sc.WriteStep(tb, lifecycle, "commit")
	lifecycle.Int(0, 0, 0)
	sc.WriteStep(tb, lifecycle, "claim")
	lifecycle.Int(0, fuzzKeys-1, 0)
	lifecycle.Int(1, fuzzMaxWindow, 2)
	sc.WriteStep(tb, lifecycle, "advance")
	lifecycle.Duration(fuzzMaxJump, 5*fuzzUnit)
	sc.WriteStep(tb, lifecycle, "claim")
	lifecycle.Int(0, fuzzKeys-1, 0)
	lifecycle.Int(1, fuzzMaxWindow, 2)

	stale := fuzzutil.NewWriter()
	sc.WriteStep(tb, stale, "claim")
	stale.Int(0, fuzzKeys-1, 1)
	stale.Int(1, fuzzMaxWindow, 1)
	sc.WriteStep(tb, stale, "advance")
	stale.Duration(fuzzMaxJump, 4*fuzzUnit)
	sc.WriteStep(tb, stale, "claim")
	stale.Int(0, fuzzKeys-1, 1)
	stale.Int(1, fuzzMaxWindow, 4)
	sc.WriteStep(tb, stale, "commit")
	stale.Int(0, 1, 0)
	sc.WriteStep(tb, stale, "abort")
	stale.Int(0, 1, 0)

	return [][]byte{lifecycle.Data(), stale.Data()}
}
