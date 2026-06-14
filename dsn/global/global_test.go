package global_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn/global"
	"bitbucket.org/iqhive/iqmq/memory"
)

// TestGlobalRegistry pins what this module is for: one registry per process,
// reached without threading one through a call stack, and the same refusals the
// registry itself has.
func TestGlobalRegistry(t *testing.T) {
	ctx := context.Background()
	if err := memory.Register(global.Registry()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	client, err := global.Open(ctx, "memory://?autocreate=true")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// The fuzz target registers a scheme of its own in this package, and
	// test order is shuffled, so the assertion is about the two this test
	// registered rather than about the registry's size.
	schemes := global.Schemes()
	for _, want := range []string{memory.Scheme, memory.SchemeAlias} {
		if !slices.Contains(schemes, want) {
			t.Errorf("Schemes is %q, want it to contain %q", schemes, want)
		}
	}
	if _, err := global.Open(ctx, "kafka://b1:9092"); !errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Errorf("an unregistered scheme: %v", err)
	}
	// A second registration of the same scheme is a conflict, and
	// MustRegister turns it into the panic a package initialiser needs.
	if err := memory.Register(global.Registry()); mq.CodeOf(err) != mq.CodeConflict {
		t.Errorf("a second Register: %v", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("MustRegister did not panic on a conflict")
			}
		}()
		global.MustRegister("memory", memory.OpenDSN)
	}()
}
