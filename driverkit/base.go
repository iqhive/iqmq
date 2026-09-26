package driverkit

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
)

// BaseConfig is what a [BaseBackend] is built with.
type BaseConfig struct {
	// Info describes the driver and the client library behind it.
	Info mq.DriverInfo
	// Capabilities is the driver's table.
	Capabilities mq.Capabilities
	// Native is the native client, surfaced through Native. It may be nil.
	Native any
	// Ownership says whether iqmq closes the native client.
	Ownership Ownership
	// CloseNative closes the native client. It is called at most once, and
	// only when Ownership is ClientOwned.
	CloseNative func() error
	// Health is the health reporter, or nil for a driver with no background
	// work and therefore nothing to report beyond ready and closed.
	Health *Health
}

// BaseBackend is the bookkeeping every backend has and nobody should write
// twice: the constant answers, the ownership of the native client, and a Close
// that runs once.
//
// A driver embeds it and implements the four methods that are actually the
// driver's — OpenSink, OpenSource, Ping and whatever optional surfaces it
// offers. Info, Capabilities, Native and Health are answered without I/O from
// any goroutine at any time, including after Close, which is what the specification requires of
// them.
//
// Close is idempotent and runs the driver's own teardown exactly once, then
// closes the native client when iqmq owns it. A second Close returns the first
// one's error, so a caller cannot be told a close failed and then that it
// succeeded. Neither the teardown nor the native close runs under this type's
// lock: see [BaseBackend.Close].
type BaseBackend struct {
	cfg    BaseConfig
	mu     sync.Mutex
	closed bool
	// done is created when the close starts and closed when it has
	// finished, so that a second Close can wait for the first one's answer
	// without holding a lock the first one's callbacks may need.
	done     chan struct{}
	closeErr error
	teardown func() error
}

// NewBase returns the base bookkeeping, with teardown as the driver's own close
// step. teardown runs before the native client is closed, and it runs exactly
// once. It may be nil.
func NewBase(cfg BaseConfig, teardown func() error) (*BaseBackend, error) {
	if err := CheckOwnership(cfg.Ownership); err != nil {
		return nil, err
	}
	return &BaseBackend{cfg: cfg, teardown: teardown}, nil
}

// Info describes the driver.
func (b *BaseBackend) Info() mq.DriverInfo { return b.cfg.Info }

// Capabilities is what this backend can and cannot do. It is constant for
// the life of the backend.
func (b *BaseBackend) Capabilities() mq.Capabilities { return b.cfg.Capabilities }

// Native returns the native client, or nil.
func (b *BaseBackend) Native() any { return b.cfg.Native }

// Health is the backend's current state. A backend built without a
// health reporter answers Ready until it is closed, which is the truth for a
// driver with no background work to fail in.
func (b *BaseBackend) Health() mq.HealthSnapshot {
	if b.cfg.Health != nil {
		return b.cfg.Health.Snapshot()
	}
	if b.Closed() {
		return mq.HealthSnapshot{State: mq.HealthClosed}
	}
	return mq.HealthSnapshot{State: mq.HealthReady}
}

// Closed reports whether Close has run.
func (b *BaseBackend) Closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// CheckOpen returns ErrClosed when the backend is closed and ctx's error when
// the context is done, which is the guard every Open method starts with.
func (b *BaseBackend) CheckOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.Closed() {
		return mq.ErrClosed
	}
	return nil
}

// Close runs the driver's teardown once and then closes the native client when
// iqmq owns it. It is idempotent and safe for concurrent use.
//
// It runs neither callback under this type's lock, and that is the whole shape
// of the function rather than a detail. b.mu is the lock [BaseBackend.Closed],
// [BaseBackend.CheckOpen] and [BaseBackend.Health] take, and every real client
// calls back into its owner from inside its own Close: franz-go fires
// OnBrokerDisconnect from there, nats.go fires its closed handler, rueidis
// resolves the outstanding commands. A handler that asks the backend anything
// at all then blocks on a mutex its own caller holds, and the promise that
// Info, Capabilities, Native and Health are "safe to call from any goroutine at
// any time, including after Close" becomes a deadlock — the one way of not
// answering that no error can report. The specification lost the Kafka driver's first test to
// it.
//
// What the lock still does is decide who closes: the flag and the completion
// channel are taken together, so exactly one caller runs the teardown. A second
// Close waits for that one and returns its error rather than reporting a
// success the first caller is about to be told is a failure; a "closing" flag
// with no wait was the alternative, and it breaks that half. The one thing a
// driver must therefore not do is call Close again FROM its own teardown or
// native close, which would be waiting for itself.
func (b *BaseBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		done := b.done
		b.mu.Unlock()
		<-done
		b.mu.Lock()
		err := b.closeErr
		b.mu.Unlock()
		return err
	}
	b.closed = true
	b.done = make(chan struct{})
	done, health, teardown := b.done, b.cfg.Health, b.teardown
	b.mu.Unlock()

	// From here to the store below nothing holds b.mu, so the driver's own
	// code may ask this backend anything the specification says it always answers.
	var err error
	if health != nil {
		health.Close()
	}
	if teardown != nil {
		err = teardown()
	}
	if cerr := b.cfg.Ownership.Close(b.cfg.CloseNative); cerr != nil && err == nil {
		err = cerr
	}

	b.mu.Lock()
	b.closeErr = err
	b.mu.Unlock()
	close(done)
	return err
}
