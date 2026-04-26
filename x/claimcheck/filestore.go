package claimcheck

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"
)

// Defaults for a [FileStore].
const (
	// DefaultFileMaxBytes bounds a [FileStore] whose MaxBytes is zero: 1 GiB
	// of payloads.
	DefaultFileMaxBytes = 1 << 30
	// DefaultFileTTL is a [FileStore]'s lifecycle when its TTL is zero:
	// fourteen days, which is SQS's longest retention and twice Kafka's
	// default. It is a default and not an answer: set TTL to the broker's
	// retention plus its longest redelivery window, because a redelivery
	// after the TTL is [ErrPayloadGone].
	DefaultFileTTL = 14 * 24 * time.Hour
	// fileKeyLen is the length of every key a FileStore issues: 16 random
	// bytes, hex-encoded.
	fileKeyLen = 32
	// tmpPrefix marks a payload still being written. A name with it is
	// never a key, so a half-written file can never be read as a payload.
	tmpPrefix = ".tmp-"
	// tmpGrace is how old a temporary file must be before a sweep treats
	// it as the leftover of a Put that died, rather than one in progress.
	tmpGrace = time.Hour
)

// FileOptions configures a [FileStore].
type FileOptions struct {
	// Name is the store's identity, recorded in every reference. Required,
	// and it must be the same on every publisher and consumer that shares
	// the directory: see [Store.Name]. There is no default because a
	// default would be shared by two unrelated directories, and a consumer
	// of one would then accept references to the other and report their
	// payloads as gone.
	Name string
	// Dir is the directory the payloads live in. Required. It is created
	// (0700) when missing, and the store reads and writes nothing outside
	// it: every access goes through an [os.Root] opened on it, so neither a
	// key nor a symbolic link can reach another path.
	Dir string
	// MaxBytes bounds the bytes of payload the store holds. A Put that
	// would exceed it is refused with [ErrTooLarge] rather than evicting,
	// for the reason [MemoryStore] refuses: an evicted payload is a message
	// that will be dead-lettered as [ErrPayloadGone]. Zero takes
	// [DefaultFileMaxBytes]; negative is refused.
	//
	// The count is read from the directory when the store opens and again
	// at every sweep, and moved by this store's own Puts in between, so it
	// is exact for this process's writes. Several processes sharing one
	// directory each enforce their own view, which lags the others' writes
	// by at most one sweep interval.
	MaxBytes int64
	// TTL is the lifecycle policy. A payload older than TTL is GONE: Get
	// reports [ErrPayloadGone] for it whether or not a sweep has deleted
	// it yet, so what a consumer sees does not depend on when the sweep
	// ran. Zero takes [DefaultFileTTL]; negative is refused.
	TTL time.Duration
	// SweepEvery is how often the store's own goroutine deletes expired
	// payloads and abandoned temporary files. Zero takes TTL/16, clamped
	// to between a minute and an hour. Negative starts no goroutine: the
	// caller runs [FileStore.Sweep] itself, which is also the way to see a
	// sweep's error -- the goroutine's are retried at its next tick.
	SweepEvery time.Duration
	// Now is the clock the TTL is judged by. Nil takes time.Now. A
	// payload's age is its file's modification time against this clock.
	Now func() time.Time
}

// FileStore is a [Store] over a directory: one file per payload, written
// atomically, bounded in total size, and deleted by a TTL sweep that is the
// store's lifecycle policy.
//
// It is for a single host, or for hosts sharing a filesystem that gives
// rename its POSIX meaning. What it guarantees:
//
//   - **A payload is never read half-written.** Put writes a temporary file,
//     syncs it, renames it to its key and syncs the directory. A reader sees
//     either no file, which is [ErrPayloadGone], or the whole payload.
//   - **A key never escapes the directory.** Keys are 32 hex characters the
//     store generated; anything else is refused as [ErrPayloadGone] before
//     the filesystem is touched, and every access goes through an [os.Root],
//     so a reference forged to name "../../etc/passwd" or a symbolic link
//     planted in the directory reaches nothing outside it.
//   - **The three failures stay distinct.** No such file -- including an
//     expired one -- is [ErrPayloadGone] (permanent); any other filesystem
//     error is [ErrStoreUnavailable] (retryable), joined with the cause so
//     errors.Is still finds it; a payload over the caller's max is
//     [ErrTooLarge], decided from the file's size before it is read.
//
// The consumer still never deletes (the package documentation says why):
// deletion is the TTL, and only the sweep performs it.
//
// It is safe for concurrent use. Close waits for Puts and Gets in flight and
// stops the sweep; it deletes nothing, because the payloads are the durable
// half of messages still in the broker. The specification records the design
// and the mutation each of its tests was watched failing under.
type FileStore struct {
	name     string
	root     *os.Root
	maxBytes int64
	ttl      time.Duration
	now      func() time.Time

	mu sync.Mutex
	// onDisk is the bytes of the payloads in the directory: read by the
	// last scan and moved by every Put that has renamed its file since.
	onDisk int64
	// reserved is the bytes of Puts still writing. It is kept apart from
	// onDisk so that a sweep, which replaces onDisk with what it counted,
	// cannot drop a reservation it could not have seen.
	reserved int64
	closed   bool
	inFlight sync.WaitGroup
	// renames is held for reading by every Put's rename-and-count and for
	// writing by a sweep's scan, so a scan sees the directory with no
	// rename landing half-way through it and its count is exact for this
	// process's own writes. Only the rename waits: the slow part of a Put,
	// writing and syncing the temporary file, does not.
	renames sync.RWMutex

	stop chan struct{}
	done chan struct{}
}

// NewFileStore opens (creating when missing) a file store.
func NewFileStore(opt FileOptions) (*FileStore, error) {
	if opt.Name == "" || len(opt.Name) > MaxStoreLen || opt.Dir == "" || opt.MaxBytes < 0 || opt.TTL < 0 {
		return nil, ErrInvalidConfig
	}
	if opt.MaxBytes == 0 {
		opt.MaxBytes = DefaultFileMaxBytes
	}
	if opt.TTL == 0 {
		opt.TTL = DefaultFileTTL
	}
	if opt.SweepEvery == 0 {
		opt.SweepEvery = min(max(opt.TTL/16, time.Minute), time.Hour)
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if err := os.MkdirAll(opt.Dir, 0o700); err != nil {
		return nil, errors.Join(ErrStoreUnavailable, err)
	}
	root, err := os.OpenRoot(opt.Dir)
	if err != nil {
		return nil, errors.Join(ErrStoreUnavailable, err)
	}
	s := &FileStore{name: opt.Name, root: root, maxBytes: opt.MaxBytes, ttl: opt.TTL, now: opt.Now}
	onDisk, _, err := s.scan(context.Background(), false)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	s.onDisk = onDisk
	if opt.SweepEvery > 0 {
		s.stop, s.done = make(chan struct{}), make(chan struct{})
		go s.sweepLoop(opt.SweepEvery)
	}
	return s, nil
}

// Name implements [Store].
func (s *FileStore) Name() string { return s.name }

// begin admits one operation, or reports [ErrClosed].
func (s *FileStore) begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.inFlight.Add(1)
	return nil
}

// Put implements [Store]: write-then-rename, so the payload appears under its
// key whole or not at all.
func (s *FileStore) Put(ctx context.Context, payload []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.begin(); err != nil {
		return "", err
	}
	defer s.inFlight.Done()
	size := int64(len(payload))
	// Reserve the bytes before writing, so two concurrent Puts cannot both
	// fit into the same remaining space.
	s.mu.Lock()
	if s.onDisk+s.reserved+size > s.maxBytes {
		s.mu.Unlock()
		return "", ErrTooLarge
	}
	s.reserved += size
	s.mu.Unlock()

	key := newFileKey()
	if renamed, err := s.write(key, payload, size); err != nil {
		if !renamed {
			// Nothing reached the key's name, so the reservation is
			// given back. A failure AFTER the rename (the directory
			// sync) leaves a file nobody will reference: it is counted
			// as on disk, because it is, and the TTL deletes it.
			s.mu.Lock()
			s.reserved -= size
			s.mu.Unlock()
		}
		return "", errors.Join(ErrStoreUnavailable, err)
	}
	return key, nil
}

// write is Put's filesystem half: temporary file, sync, rename, sync the
// directory. The directory sync is what makes the rename itself survive a
// crash; without it a power cut can leave the reference published and the
// name unwritten.
func (s *FileStore) write(key string, payload []byte, size int64) (renamed bool, err error) {
	tmp := tmpPrefix + key
	if err := s.writeTemp(tmp, payload); err != nil {
		_ = s.root.Remove(tmp)
		return false, err
	}
	if err := s.rename(tmp, key, size); err != nil {
		_ = s.root.Remove(tmp)
		return false, err
	}
	dir, err := s.root.Open(".")
	if err != nil {
		return true, err
	}
	serr := dir.Sync()
	cerr := dir.Close()
	return true, errors.Join(serr, cerr)
}

// writeTemp writes payload to a new temporary file and syncs it.
func (s *FileStore) writeTemp(tmp string, payload []byte) error {
	f, err := s.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(payload)
	var serr error
	if werr == nil {
		serr = f.Sync()
	}
	cerr := f.Close()
	return errors.Join(werr, serr, cerr)
}

// Get implements [Store].
//
// The order is: refuse a key this store could not have issued (no I/O), open
// it (no such file is [ErrPayloadGone]), judge its age against the TTL (an
// expired payload is gone whether or not the sweep has run), compare its size
// with maxBytes ([ErrTooLarge] before reading), then read exactly that many
// bytes into one buffer. The size comes from the file and is bounded by
// maxBytes before anything is allocated; a file that changed after the stat
// is caught by the reference's own length and digest check, because this
// store's files are written once and never rewritten.
func (s *FileStore) Get(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	if !validFileKey(key) {
		return nil, ErrPayloadGone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.begin(); err != nil {
		return nil, err
	}
	defer s.inFlight.Done()
	f, err := s.root.Open(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrPayloadGone
		}
		// A symbolic link planted under a key's name that points out of
		// the directory is refused by the os.Root with an error of its
		// own. It is not a payload this store wrote, so it is gone rather
		// than unavailable -- retrying it would only be refused again.
		if st, lerr := s.root.Lstat(key); lerr == nil && st.Mode()&fs.ModeSymlink != 0 {
			return nil, ErrPayloadGone
		}
		return nil, errors.Join(ErrStoreUnavailable, err)
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, errors.Join(ErrStoreUnavailable, err)
	}
	if !st.Mode().IsRegular() {
		// Something other than a payload file has this name -- a
		// directory, a device. It is not a payload this store holds.
		return nil, ErrPayloadGone
	}
	if s.expired(st.ModTime()) {
		return nil, ErrPayloadGone
	}
	if st.Size() > maxBytes {
		return nil, ErrTooLarge
	}
	buf := make([]byte, st.Size())
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, errors.Join(ErrStoreUnavailable, err)
	}
	return buf, nil
}

func (s *FileStore) expired(mod time.Time) bool {
	return s.now().Sub(mod) > s.ttl
}

// Sweep deletes every payload older than the TTL and every temporary file
// abandoned by a Put that died, and re-reads the bytes the directory holds.
// It reports how many payloads it deleted.
//
// It is what the store's own goroutine runs, and it is exported for a caller
// that disabled the goroutine (a negative SweepEvery) to run it on its own
// schedule and see its error. It is the only thing that ever deletes a
// payload.
func (s *FileStore) Sweep(ctx context.Context) (int, error) {
	if err := s.begin(); err != nil {
		return 0, err
	}
	defer s.inFlight.Done()
	s.renames.Lock()
	defer s.renames.Unlock()
	onDisk, removed, err := s.scan(ctx, true)
	if err != nil {
		return removed, err
	}
	s.mu.Lock()
	s.onDisk = onDisk
	s.mu.Unlock()
	return removed, nil
}

// rename moves a written payload to its key and moves its bytes from reserved
// to onDisk, as one step no sweep's count can land in the middle of.
func (s *FileStore) rename(tmp, key string, size int64) error {
	s.renames.RLock()
	defer s.renames.RUnlock()
	if err := s.root.Rename(tmp, key); err != nil {
		return err
	}
	s.mu.Lock()
	s.reserved -= size
	s.onDisk += size
	s.mu.Unlock()
	return nil
}

// scan totals the bytes of the payloads in the directory, and when sweep is
// set deletes the expired ones and the abandoned temporary files first.
func (s *FileStore) scan(ctx context.Context, sweep bool) (used int64, removed int, err error) {
	dir, err := s.root.Open(".")
	if err != nil {
		return 0, 0, errors.Join(ErrStoreUnavailable, err)
	}
	defer func() { _ = dir.Close() }()
	for {
		if err := ctx.Err(); err != nil {
			return used, removed, err
		}
		entries, rerr := dir.ReadDir(256)
		for _, e := range entries {
			name := e.Name()
			info, ierr := e.Info()
			if ierr != nil || !info.Mode().IsRegular() {
				continue
			}
			switch {
			case validFileKey(name):
				if sweep && s.expired(info.ModTime()) {
					if err := s.root.Remove(name); err == nil || errors.Is(err, fs.ErrNotExist) {
						removed++
						continue
					}
				}
				used += info.Size()
			case sweep && strings.HasPrefix(name, tmpPrefix) && s.now().Sub(info.ModTime()) > tmpGrace:
				_ = s.root.Remove(name)
			}
		}
		if errors.Is(rerr, io.EOF) || (rerr == nil && len(entries) == 0) {
			return used, removed, nil
		}
		if rerr != nil {
			return used, removed, errors.Join(ErrStoreUnavailable, rerr)
		}
	}
}

func (s *FileStore) sweepLoop(every time.Duration) {
	defer close(s.done)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			_, _ = s.Sweep(context.Background())
		}
	}
}

// Bytes is how many bytes of payload the store believes the directory holds:
// the figure MaxBytes is enforced against.
func (s *FileStore) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.onDisk + s.reserved
}

// Close implements [Store]. It is idempotent. It stops the sweep, waits for
// the operations in flight, and closes the directory; it deletes nothing.
func (s *FileStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if s.stop != nil {
		close(s.stop)
		<-s.done
	}
	s.inFlight.Wait()
	return s.root.Close()
}

// newFileKey returns 16 bytes from crypto/rand, hex-encoded. Random rather
// than content-addressed: two publications of the same bytes get two files,
// so the TTL of one never shortens the life of the other.
func newFileKey() string {
	var b [fileKeyLen / 2]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error (Go 1.24+)
	return hex.EncodeToString(b[:])
}

// validFileKey reports whether key is one this store could have issued:
// exactly 32 lower-case hex characters. It is the first line of defence
// against a forged reference, and the [os.Root] is the second.
func validFileKey(key string) bool {
	if len(key) != fileKeyLen {
		return false
	}
	for i := range len(key) {
		c := key[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
