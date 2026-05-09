package driverkit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// TestBaseBackendCloseDoesNotHoldItsLockOverTheDriversCode is the finding
// , reproduced against the kit rather than against Kafka.
//
// BaseBackend.Close used to run the driver's teardown and the native client's
// close with b.mu held, and b.mu is the lock Closed, CheckOpen and Health take.
// Every real client calls back into its owner from inside its own Close —
// franz-go fires OnBrokerDisconnect from there, nats.go fires its closed
// handler, rueidis resolves outstanding commands — and a handler that asks the
// backend anything at all then blocks on a mutex its own caller holds. The specification says
// Info, Capabilities, Native and Health "are answered without I/O and are safe
// to call from any goroutine at any time, including after Close", and a
// deadlock is the one way of not answering that no error can report.
//
// The close runs in a goroutine and the test waits with a timeout, so an
// unfixed kit FAILS here rather than hanging the package's test binary for
// ever, which is what it did to the Kafka driver's first test.
func TestBaseBackendCloseDoesNotHoldItsLockOverTheDriversCode(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		// where says which of Close's two callbacks asks the backend a
		// question: both are the driver's own code, and both used to run
		// under b.mu.
		where string
	}{
		{name: "from the driver's teardown", where: "teardown"},
		{name: "from the native client's close", where: "native"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var base *driverkit.BaseBackend
			// asked is what the client's callback does: the four questions the specification
			// promises are always answerable, plus the guard every Open starts
			// with. Any one of them taking b.mu deadlocks under the old Close.
			var asked bool
			callback := func() error {
				_ = base.Info()
				_ = base.Capabilities()
				_ = base.Native()
				_ = base.Health()
				_ = base.Closed()
				_ = base.CheckOpen(ctx)
				asked = true
				return nil
			}
			cfg := driverkit.BaseConfig{
				Info:        mq.DriverInfo{Name: "fake"},
				Ownership:   driverkit.ClientOwned,
				CloseNative: func() error { return nil },
			}
			teardown := func() error { return nil }
			switch tc.where {
			case "teardown":
				teardown = callback
			case "native":
				cfg.CloseNative = callback
			}
			var err error
			base, err = driverkit.NewBase(cfg, teardown)
			if err != nil {
				t.Fatalf("NewBase: %v", err)
			}
			done := make(chan error, 1)
			go func() { done <- base.Close() }()
			select {
			case cerr := <-done:
				if cerr != nil {
					t.Fatalf("Close: %v", cerr)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("Close deadlocked: it held b.mu across the driver's %s, and the "+
					"callback that asked the backend a question blocked on that lock. "+
					"Every real client calls back from inside its own Close "+
					"", tc.where)
			}
			if !asked {
				t.Fatal("the callback never ran, so this test proves nothing")
			}
		})
	}
}

// TestBaseBackendCloseIsIdempotentUnderConcurrency holds the contract the lock
// was there for: a second Close reports the first one's error and the driver's
// teardown runs exactly once, however many goroutines close at once.
//
// It is the half a "closing" flag alone would have broken — the alternative
// The specification rejected — because a second Close that returned nil while the first was
// still tearing down would tell a caller the close succeeded and then the first
// caller that it failed.
func TestBaseBackendCloseIsIdempotentUnderConcurrency(t *testing.T) {
	boom := errors.New("teardown failed")
	var teardowns, closes int
	release := make(chan struct{})
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Ownership:   driverkit.ClientOwned,
		CloseNative: func() error { closes++; return nil },
	}, func() error {
		teardowns++
		<-release // hold the close open while the other goroutines arrive
		return boom
	})
	if err != nil {
		t.Fatalf("NewBase: %v", err)
	}
	const closers = 8
	var wg sync.WaitGroup
	errs := make([]error, closers)
	for i := range closers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = base.Close()
		}()
	}
	// Every closer is now either inside the teardown or waiting for it.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	for i, e := range errs {
		if !errors.Is(e, boom) {
			t.Errorf("closer %d was told %v, want the teardown's error: a caller cannot be "+
				"told a close failed and then that it succeeded", i, e)
		}
	}
	if teardowns != 1 || closes != 1 {
		t.Fatalf("teardown ran %d times and the native close %d, want one each", teardowns, closes)
	}
}
