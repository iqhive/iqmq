package dsn

import (
	"context"
	"sort"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// OpenFunc opens a client from a DSN. It is the unit a driver registers:
// the driver parses the DSN itself, because only the driver knows which
// parameters it has.
type OpenFunc = func(ctx context.Context, dsn string) (*mq.Client, error)

// Registrar is the half of a [Registry] a driver needs, and the type a driver's
// Register takes (ADR-012).
//
// A driver depends on this interface and never on the concrete registry, which
// is what lets a program hold two registries — a test's and the process's —
// without the driver knowing either exists.
type Registrar interface {
	// Register associates a scheme with the function that opens a client
	// from a DSN. It fails when the scheme is already taken.
	Register(scheme string, open OpenFunc) error
}

// Driver is a registerable driver: its name, the schemes it answers to and the
// function that opens a client.
//
// The specification sketches this as Open(ctx, DriverConfig) (Backend, error). What a registry
// hands back is a client and not a backend, because a driver's own Open builds
// the client with the hooks and the clock its configuration names, and there is
// no DriverConfig in the frozen root API for it to take instead.
type Driver interface {
	// Name is the driver's registered name, such as "kafka" or "memory".
	Name() string
	// Schemes are the DSN schemes this driver answers to, such as "memory"
	// and "mem". There is at least one, and the first is the canonical one.
	Schemes() []string
	// Open opens a client from a DSN of one of those schemes.
	Open(ctx context.Context, dsn string) (*mq.Client, error)
}

// Registry maps a DSN scheme to the driver that opens it (ADR-012).
//
// It holds no global state: a Registry is a value a program owns, so two
// clients of the same driver, or two libraries with different driver sets, can
// coexist in one process. A program builds one in main, registers the drivers
// it links, and passes it to whatever opens clients.
//
// Every method is safe for concurrent use.
type Registry struct {
	mu    sync.RWMutex
	open  map[string]OpenFunc
	names map[string]string
}

// NewRegistry returns an empty registry. The zero Registry is also usable: it
// fills its maps on the first registration.
func NewRegistry() *Registry { return &Registry{} }

// Register associates a scheme with the function that opens a client from a DSN
// It fails when the scheme is empty, malformed, already taken, or the
// opener is nil.
//
// Registering the same scheme twice is a conflict and not a replacement: two
// drivers claiming one scheme is a programming error, and the alternative —
// last registration wins — decides it by import order, which nobody reading the
// program can see.
//
// A scheme is case-insensitive (RFC 3986), so it is registered, looked up and
// reported in its canonical lower-case spelling: "Kafka" registers "kafka",
// which is the scheme [Parse] reads out of "KAFKA://b1" and "kafka://b1" alike,
// and "Kafka" after "kafka" is the conflict it looks like.
func (r *Registry) Register(scheme string, open OpenFunc) error {
	return r.register(scheme, "", open)
}

// RegisterDriver registers every scheme a [Driver] answers to, atomically: a
// driver whose second scheme collides registers neither, so a failed
// registration never leaves half a driver reachable.
func (r *Registry) RegisterDriver(d Driver) error {
	if d == nil {
		return errNilDriver
	}
	schemes := d.Schemes()
	if len(schemes) == 0 {
		return errNoSchemes
	}
	canon := make([]string, len(schemes))
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, s := range schemes {
		c, err := r.checkLocked(s)
		if err != nil {
			return err
		}
		// The same scheme twice in one driver's list -- "kafka" and "KAFKA"
		// are one scheme -- is the conflict two drivers claiming it would
		// be. checkLocked cannot see it: nothing is registered until every
		// scheme has passed.
		for _, prev := range canon[:i] {
			if prev == c {
				return errSchemeTaken
			}
		}
		canon[i] = c
	}
	for _, s := range canon {
		r.setLocked(s, d.Name(), d.Open)
	}
	return nil
}

// register is the locked half of Register.
func (r *Registry) register(scheme, name string, open OpenFunc) error {
	if open == nil {
		return errNilOpener
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	canon, err := r.checkLocked(scheme)
	if err != nil {
		return err
	}
	r.setLocked(canon, name, open)
	return nil
}

// checkLocked reports whether scheme may still be taken, and returns the
// canonical spelling it is taken under.
//
// The whole string has to be a scheme. The check used to append "://" and hand
// the result to the DSN splitter, which stops at the FIRST "://" -- so
// "a://b" passed as the scheme "a" and was then stored under "a://b", a key no
// DSN can produce.
func (r *Registry) checkLocked(scheme string) (string, error) {
	canon, err := canonicalScheme(scheme)
	if err != nil {
		return "", errBadScheme
	}
	if _, taken := r.open[canon]; taken {
		return "", errSchemeTaken
	}
	return canon, nil
}

// setLocked records the scheme.
func (r *Registry) setLocked(scheme, name string, open OpenFunc) {
	if r.open == nil {
		r.open = map[string]OpenFunc{}
		r.names = map[string]string{}
	}
	r.open[scheme] = open
	if name == "" {
		name = scheme
	}
	r.names[scheme] = name
}

// Lookup returns the opener registered for a scheme, in any spelling of it.
func (r *Registry) Lookup(scheme string) (OpenFunc, bool) {
	canon, err := canonicalScheme(scheme)
	if err != nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	open, ok := r.open[canon]
	return open, ok
}

// Schemes returns every registered scheme, sorted.
func (r *Registry) Schemes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.open))
	for s := range r.open {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// DriverName returns the driver registered for a scheme, in any spelling of it.
func (r *Registry) DriverName(scheme string) (string, bool) {
	canon, err := canonicalScheme(scheme)
	if err != nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	name, ok := r.names[canon]
	return name, ok
}

// Open opens a client from a DSN: it reads the scheme, finds the driver
// that claimed it, and hands the whole DSN over.
//
// A scheme nothing claimed reports [github.com/iqhive/iqmq.ErrDriverNotRegistered],
// which is the code for it. A malformed DSN reports the parse failure, so
// that "kafka" and "kafak://b1" are distinguishable: the first is not a DSN and
// the second names a driver nobody registered.
func (r *Registry) Open(ctx context.Context, s string) (*mq.Client, error) {
	scheme, err := Scheme(s)
	if err != nil {
		return nil, err
	}
	open, ok := r.Lookup(scheme)
	if !ok {
		return nil, mq.ErrDriverNotRegistered
	}
	return open(ctx, s)
}

// The registration failures.
var (
	errNilOpener   = errcode.New(errcode.InvalidArgument, "iqmq/dsn: Register was given a nil opener")
	errNilDriver   = errcode.New(errcode.InvalidArgument, "iqmq/dsn: RegisterDriver was given a nil driver")
	errNoSchemes   = errcode.New(errcode.InvalidArgument, "iqmq/dsn: the driver answers to no scheme")
	errBadScheme   = errcode.New(errcode.InvalidArgument, "iqmq/dsn: the scheme is not a DSN scheme")
	errSchemeTaken = errcode.New(errcode.Conflict, "iqmq/dsn: the scheme is already registered")
)
