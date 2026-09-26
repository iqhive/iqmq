package claimcheck_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/claimcheck"
)

// clock is a settable clock for the TTL: the real time plus an offset, so a
// file's real modification time can be aged without sleeping.
type clock struct{ offset atomic.Int64 }

func (c *clock) now() time.Time            { return time.Now().Add(time.Duration(c.offset.Load())) }
func (c *clock) advance(d time.Duration)   { c.offset.Add(int64(d)) }
func (c *clock) options() func() time.Time { return c.now }

func fileStore(tb testing.TB, opt claimcheck.FileOptions) *claimcheck.FileStore {
	tb.Helper()
	if opt.Name == "" {
		opt.Name = "files-1"
	}
	if opt.Dir == "" {
		opt.Dir = tb.TempDir()
	}
	if opt.SweepEvery == 0 {
		opt.SweepEvery = -1 // the tests sweep by hand
	}
	s, err := claimcheck.NewFileStore(opt)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

func TestFileStoreRoundTrip(t *testing.T) {
	s := fileStore(t, claimcheck.FileOptions{})
	pm, mw := pipeline(t, claimcheck.Config{Store: s, Threshold: 16, RequireDigest: true})
	body := bytes.Repeat([]byte("payload "), 4096)
	wire, err := publish(t, pm, body)
	if err != nil {
		t.Fatal(err)
	}
	seen, err := handle(t, mw, wire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, body) {
		t.Fatalf("the handler saw %d bytes, want %d", len(seen), len(body))
	}
	if got := s.Bytes(); got != int64(len(body)) {
		t.Errorf("Bytes = %d, want %d", got, len(body))
	}
}

// TestFileStoreWritesAtomically: after a Put the directory holds exactly one
// file, named by the key, with the whole payload -- no temporary file is left
// behind -- and a temporary file, which is what a Put that died mid-write
// leaves, is never readable as a payload and is swept once it is old enough
// to be nobody's write in progress.
func TestFileStoreWritesAtomically(t *testing.T) {
	dir := t.TempDir()
	var c clock
	s := fileStore(t, claimcheck.FileOptions{Dir: dir, Now: c.options()})
	payload := bytes.Repeat([]byte{7}, 1000)
	key, err := s.Put(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != key {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("the directory holds %v after one Put, want exactly [%s]", names, key)
	}
	onDisk, err := os.ReadFile(filepath.Join(dir, key))
	if err != nil || !bytes.Equal(onDisk, payload) {
		t.Fatalf("the file under the key holds %d bytes (%v), want the whole %d-byte payload", len(onDisk), err, len(payload))
	}

	// A crashed Put's leftover: the temporary name for a key.
	orphan := ".tmp-" + strings.Repeat("a", 32)
	if err := os.WriteFile(filepath.Join(dir, orphan), []byte("half a payl"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{orphan, strings.Repeat("a", 32)} {
		if _, err := s.Get(context.Background(), k, 1<<20); !errors.Is(err, claimcheck.ErrPayloadGone) {
			t.Errorf("Get(%q) = %v, want ErrPayloadGone: a half-written file must never read as a payload", k, err)
		}
	}
	if _, err := s.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, orphan)); err != nil {
		t.Fatalf("a fresh temporary file was swept, which would delete a Put in progress: %v", err)
	}
	c.advance(2 * time.Hour)
	if _, err := s.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, orphan)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("an abandoned temporary file survived the sweep: %v", err)
	}
	if _, err := s.Get(context.Background(), key, 1<<20); err != nil {
		t.Fatalf("the sweep of an orphan took a live payload with it: %v", err)
	}
}

// TestFileStoreKeysCannotEscape: a reference is remote input, so its key is.
// Every key the store could not have issued is ErrPayloadGone without
// touching the filesystem, and a symbolic link planted in the directory under
// a valid key's name reaches nothing outside it.
func TestFileStoreKeysCannotEscape(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("not yours"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := fileStore(t, claimcheck.FileOptions{Dir: dir})
	for _, key := range []string{
		"", "..", "../secret", "/etc/passwd", outside,
		strings.Repeat("a", 31), strings.Repeat("a", 33), strings.Repeat("A", 32),
		strings.Repeat("g", 32), strings.Repeat("a", 30) + "/.",
		"../" + strings.Repeat("a", 29), ".tmp-" + strings.Repeat("a", 27),
	} {
		if got, err := s.Get(context.Background(), key, 1<<20); !errors.Is(err, claimcheck.ErrPayloadGone) || got != nil {
			t.Errorf("Get(%q) = (%q, %v), want ErrPayloadGone", key, got, err)
		}
	}
	planted := strings.Repeat("b", 32)
	if err := os.Symlink(outside, filepath.Join(dir, planted)); err != nil {
		t.Skipf("cannot create a symbolic link here: %v", err)
	}
	got, err := s.Get(context.Background(), planted, 1<<20)
	if err == nil || bytes.Contains(got, []byte("not yours")) {
		t.Fatalf("a symbolic link under a key's name read %q from outside the directory", got)
	}
	if !errors.Is(err, claimcheck.ErrPayloadGone) {
		t.Errorf("Get of a planted link out of the directory = %v, want ErrPayloadGone: it is not a payload this store wrote, and retrying it would be refused again", err)
	}
}

// TestFileStoreBound: MaxBytes is refused, not evicted; a refused Put leaves
// the count where it was; and a reopened store reads the count back from the
// directory rather than starting from zero.
func TestFileStoreBound(t *testing.T) {
	dir := t.TempDir()
	s := fileStore(t, claimcheck.FileOptions{Dir: dir, MaxBytes: 1000})
	ctx := context.Background()
	first, err := s.Put(ctx, make([]byte, 600))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, make([]byte, 401)); !errors.Is(err, claimcheck.ErrTooLarge) {
		t.Fatalf("a Put past MaxBytes = %v, want ErrTooLarge", err)
	}
	if got := s.Bytes(); got != 600 {
		t.Fatalf("Bytes after a refused Put = %d, want 600: the refusal must not keep its reservation", got)
	}
	if _, err := s.Put(ctx, make([]byte, 400)); err != nil {
		t.Fatalf("a Put that fits exactly = %v", err)
	}
	if _, err := s.Get(ctx, first, 1<<20); err != nil {
		t.Fatalf("the first payload was evicted to make room: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	again := fileStore(t, claimcheck.FileOptions{Dir: dir, MaxBytes: 1000})
	if got := again.Bytes(); got != 1000 {
		t.Fatalf("a reopened store counts %d bytes, want the 1000 on disk", got)
	}
	if _, err := again.Put(ctx, []byte("x")); !errors.Is(err, claimcheck.ErrTooLarge) {
		t.Fatalf("a reopened full store accepted a Put: %v", err)
	}
}

// TestFileStoreTTL: a payload older than the TTL is gone whether or not the
// sweep has run, so what a consumer sees does not depend on the sweep's
// timing; the sweep then deletes it and the count follows.
func TestFileStoreTTL(t *testing.T) {
	var c clock
	s := fileStore(t, claimcheck.FileOptions{TTL: time.Hour, Now: c.options()})
	ctx := context.Background()
	key, err := s.Put(ctx, []byte("short-lived"))
	if err != nil {
		t.Fatal(err)
	}
	c.advance(59 * time.Minute)
	if _, err := s.Get(ctx, key, 1<<20); err != nil {
		t.Fatalf("a payload inside its TTL = %v", err)
	}
	if n, err := s.Sweep(ctx); err != nil || n != 0 {
		t.Fatalf("a sweep inside the TTL = (%d, %v), want (0, nil)", n, err)
	}
	c.advance(2 * time.Minute)
	if _, err := s.Get(ctx, key, 1<<20); !errors.Is(err, claimcheck.ErrPayloadGone) {
		t.Fatalf("an expired payload before any sweep = %v, want ErrPayloadGone", err)
	}
	if n, err := s.Sweep(ctx); err != nil || n != 1 {
		t.Fatalf("the sweep after the TTL = (%d, %v), want (1, nil)", n, err)
	}
	if got := s.Bytes(); got != 0 {
		t.Fatalf("Bytes after the sweep = %d, want 0", got)
	}
	if _, err := s.Get(ctx, key, 1<<20); !errors.Is(err, claimcheck.ErrPayloadGone) {
		t.Fatalf("a swept payload = %v, want ErrPayloadGone", err)
	}
}

// TestFileStoreSweepsOnItsOwn: the background goroutine is the lifecycle
// policy nobody has to remember to run.
func TestFileStoreSweepsOnItsOwn(t *testing.T) {
	var c clock
	dir := t.TempDir()
	s := fileStore(t, claimcheck.FileOptions{Dir: dir, TTL: time.Minute, SweepEvery: 10 * time.Millisecond, Now: c.options()})
	key, err := s.Put(context.Background(), []byte("expiring"))
	if err != nil {
		t.Fatal(err)
	}
	c.advance(2 * time.Minute)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, key)); errors.Is(err, fs.ErrNotExist) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the store's own sweep never deleted an expired payload")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestFileStoreGetRespectsMax: the file's size decides ErrTooLarge before a
// byte is read, and the boundary is inclusive.
func TestFileStoreGetRespectsMax(t *testing.T) {
	s := fileStore(t, claimcheck.FileOptions{})
	key, err := s.Put(context.Background(), make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), key, 99); !errors.Is(err, claimcheck.ErrTooLarge) {
		t.Errorf("Get with max 99 of a 100-byte payload = %v, want ErrTooLarge", err)
	}
	if got, err := s.Get(context.Background(), key, 100); err != nil || len(got) != 100 {
		t.Errorf("Get with max 100 = (%d bytes, %v)", len(got), err)
	}
}

// TestFileStoreUnavailableIsRetryable: a directory the store cannot read is
// ErrStoreUnavailable (CodeUnavailable, retried) and never ErrPayloadGone
// (poison) -- the mistake that dead-letters good messages because a disk was
// briefly unreadable -- and the cause is still reachable.
func TestFileStoreUnavailableIsRetryable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permissions do not bind root")
	}
	dir := t.TempDir()
	s := fileStore(t, claimcheck.FileOptions{Dir: dir})
	key, err := s.Put(context.Background(), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(dir, key), 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, key), 0o600) })
	_, err = s.Get(context.Background(), key, 1<<20)
	if !errors.Is(err, claimcheck.ErrStoreUnavailable) || errors.Is(err, claimcheck.ErrPayloadGone) {
		t.Fatalf("Get of an unreadable payload = %v, want ErrStoreUnavailable and not ErrPayloadGone", err)
	}
	if mq.CodeOf(err) != mq.CodeUnavailable {
		t.Errorf("code = %v, want CodeUnavailable", mq.CodeOf(err))
	}
	if !errors.Is(err, fs.ErrPermission) {
		t.Errorf("the cause is not reachable through the error: %v", err)
	}

	// A Put the directory refuses is unavailable too, and gives back the
	// bytes it reserved: a store that kept them would fill up with
	// publications that never happened.
	before := s.Bytes()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := s.Put(context.Background(), make([]byte, 100)); !errors.Is(err, claimcheck.ErrStoreUnavailable) {
		t.Fatalf("Put into a read-only directory = %v, want ErrStoreUnavailable", err)
	}
	if got := s.Bytes(); got != before {
		t.Fatalf("Bytes after a failed Put = %d, want %d: the failed write kept its reservation", got, before)
	}
}

// TestFileStoreCloseKeepsThePayloads: Close is idempotent, refuses later
// calls, and deletes nothing -- the payloads are the durable half of messages
// still in the broker, and a reopened store serves them.
func TestFileStoreCloseKeepsThePayloads(t *testing.T) {
	dir := t.TempDir()
	s, err := claimcheck.NewFileStore(claimcheck.FileOptions{Name: "files-1", Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	key, err := s.Put(context.Background(), []byte("durable"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := s.Close(); err != nil {
			t.Fatalf("Close #%d = %v", i+1, err)
		}
	}
	if _, err := s.Put(context.Background(), []byte("x")); !errors.Is(err, claimcheck.ErrClosed) {
		t.Errorf("Put after Close = %v, want ErrClosed", err)
	}
	if _, err := s.Get(context.Background(), key, 1<<20); !errors.Is(err, claimcheck.ErrClosed) {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
	again := fileStore(t, claimcheck.FileOptions{Dir: dir})
	if got, err := again.Get(context.Background(), key, 1<<20); err != nil || string(got) != "durable" {
		t.Fatalf("a reopened store's Get = (%q, %v): Close deleted a payload", got, err)
	}
}

func TestFileStoreConfigRefusals(t *testing.T) {
	dir := t.TempDir()
	for name, opt := range map[string]claimcheck.FileOptions{
		"no name":       {Dir: dir},
		"no dir":        {Name: "n"},
		"negative max":  {Name: "n", Dir: dir, MaxBytes: -1},
		"negative ttl":  {Name: "n", Dir: dir, TTL: -1},
		"name too long": {Name: strings.Repeat("n", claimcheck.MaxStoreLen+1), Dir: dir},
	} {
		if _, err := claimcheck.NewFileStore(opt); !errors.Is(err, claimcheck.ErrInvalidConfig) {
			t.Errorf("%s: NewFileStore = %v, want ErrInvalidConfig", name, err)
		}
	}
	file := filepath.Join(dir, "a-file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := claimcheck.NewFileStore(claimcheck.FileOptions{Name: "n", Dir: file}); !errors.Is(err, claimcheck.ErrStoreUnavailable) {
		t.Errorf("NewFileStore over a regular file = %v, want ErrStoreUnavailable", err)
	}
}

// TestFileStorePutRacesClose is the playbook's recurring defect class for this
// store: a payload placed while the store is closing. A Put either reports
// ErrClosed or reports success for a payload that is really on disk -- which
// a store reopened on the directory must be able to serve -- and Close returns
// only after every Put in flight has finished with the directory.
//
// Three thousand rounds, because the known instances of this class appeared
// at five to twenty failures per two or three thousand. It was mutation-checked
// by deleting Close's wait for operations in flight.
func TestFileStorePutRacesClose(t *testing.T) {
	const rounds = 3000
	ctx := context.Background()
	for i := range rounds {
		dir := t.TempDir()
		s, err := claimcheck.NewFileStore(claimcheck.FileOptions{Name: "files-1", Dir: dir, SweepEvery: -1})
		if err != nil {
			t.Fatal(err)
		}
		keys := make([]string, 2)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for j := range keys {
			wg.Add(1)
			go func() {
				defer wg.Done()
				keys[j], errs[j] = s.Put(ctx, []byte("payload"))
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
		wg.Wait()
		reopened, err := claimcheck.NewFileStore(claimcheck.FileOptions{Name: "files-1", Dir: dir, SweepEvery: -1})
		if err != nil {
			t.Fatal(err)
		}
		for j := range keys {
			switch {
			case errors.Is(errs[j], claimcheck.ErrClosed):
			case errs[j] != nil:
				t.Fatalf("round %d: Put = %v, want nil or ErrClosed", i, errs[j])
			default:
				if got, err := reopened.Get(ctx, keys[j], 1<<20); err != nil || string(got) != "payload" {
					t.Fatalf("round %d: Put reported success for %q and the directory serves (%q, %v)", i, keys[j], got, err)
				}
			}
		}
		_ = reopened.Close()
	}
}

// TestFileStoreConcurrency gives -race something to look at: Puts, Gets and
// Sweeps from eight goroutines.
func TestFileStoreConcurrency(t *testing.T) {
	s := fileStore(t, claimcheck.FileOptions{})
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				key, err := s.Put(ctx, []byte("payload"))
				if err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if _, err := s.Get(ctx, key, 1<<20); err != nil {
					t.Errorf("Get of a key Put just returned: %v", err)
					return
				}
				if _, err := s.Sweep(ctx); err != nil {
					t.Errorf("Sweep: %v", err)
					return
				}
				_ = s.Bytes()
			}
		}()
	}
	wg.Wait()
	if got := s.Bytes(); got != 8*20*int64(len("payload")) {
		t.Errorf("Bytes = %d after 160 seven-byte Puts, want %d", got, 8*20*len("payload"))
	}
}

// TestFileStoreNeverExposesAPartialPayload: while a large Put is writing, a
// reader polling the directory never finds a file under a key's name that is
// shorter than the payload. Write-then-rename is what guarantees it; a store
// that wrote under the key directly passes every other test here, because
// nothing else looks while the write is in progress.
func TestFileStoreNeverExposesAPartialPayload(t *testing.T) {
	dir := t.TempDir()
	s := fileStore(t, claimcheck.FileOptions{Dir: dir})
	payload := bytes.Repeat([]byte{0x5a}, 32<<20)
	stop := make(chan struct{})
	partial := make(chan string, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				name := e.Name()
				if len(name) != 32 || strings.HasPrefix(name, ".") {
					continue
				}
				if info, err := e.Info(); err == nil && info.Size() != int64(len(payload)) {
					select {
					case partial <- name:
					default:
					}
					return
				}
			}
		}
	}()
	for range 3 {
		if _, err := s.Put(context.Background(), payload); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	select {
	case name := <-partial:
		t.Fatalf("a reader found %s under a key's name before its payload was complete", name)
	default:
	}
}

// TestFileStoreCountIsExactUnderSweeps: the byte count a store enforces
// MaxBytes against is exact for its own writes even while sweeps run
// continuously beside them. A sweep replaces the count with what it read from
// the directory, and a Put between its reservation and its rename is not in
// the directory yet; the first version of this store dropped such a Put from
// the count, which TestFileStoreConcurrency caught as "Bytes = 1113 after 160
// seven-byte Puts, want 1120".
func TestFileStoreCountIsExactUnderSweeps(t *testing.T) {
	dir := t.TempDir()
	s := fileStore(t, claimcheck.FileOptions{Dir: dir})
	ctx := context.Background()
	stop := make(chan struct{})
	var sweeper sync.WaitGroup
	sweeper.Add(1)
	go func() {
		defer sweeper.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = s.Sweep(ctx)
			}
		}
	}()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if _, err := s.Put(ctx, []byte("payload")); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	sweeper.Wait()
	var onDisk int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if info, err := e.Info(); err == nil && len(e.Name()) == 32 {
			onDisk += info.Size()
		}
	}
	if got := s.Bytes(); got != onDisk || onDisk != 8*200*int64(len("payload")) {
		t.Fatalf("Bytes = %d, the directory holds %d, and 1600 seven-byte Puts are %d: the count drifted under sweeps", got, onDisk, 8*200*len("payload"))
	}
}
