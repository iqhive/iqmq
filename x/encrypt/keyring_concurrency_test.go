package encrypt_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/x/encrypt"
)

func TestStaticKeyringConcurrentRetireAndLookup(t *testing.T) {
	ctx := context.Background()
	suite := encrypt.SuiteAES128GCM
	r, err := encrypt.NewKeyring(suite, key(16, 1), "current")
	if err != nil {
		t.Fatal(err)
	}
	current, err := r.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}

	const keys = 128
	var added atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	// Competing writers must accept each id exactly once, without replacing
	// the current key or racing readers that are opening existing messages.
	for range 4 {
		wg.Go(func() {
			<-start
			for i := range keys {
				id := strconv.Itoa(i)
				err := r.Retire(suite, key(16, 2), id)
				if err == nil {
					added.Add(1)
				} else if !errors.Is(err, encrypt.ErrInvalidConfig) {
					t.Errorf("Retire(%q): %v", id, err)
				}
				if _, err := r.For(ctx, suite, id); err != nil {
					t.Errorf("lookup after Retire(%q): %v", id, err)
				}
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			<-start
			for range keys {
				aead, err := r.For(ctx, suite, "current")
				if err != nil || aead != current.AEAD {
					t.Errorf("current key lookup changed: %v", err)
				}
				got, err := r.Current(ctx)
				if err != nil || got != current {
					t.Errorf("current sealing key changed: %v", err)
				}
			}
		})
	}
	close(start)
	wg.Wait()
	if got := added.Load(); got != keys {
		t.Errorf("accepted %d retired keys, want %d", got, keys)
	}
}
