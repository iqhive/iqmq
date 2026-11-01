package testkit_test

import (
	"slices"
	"sync"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/testkit"
)

// closingBroker is the memory broker with the ClosingBackend step, recording
// the order of BeginClose and Close.
type closingBroker struct {
	*memory.Broker
	mu  *sync.Mutex
	log *[]string
}

func (b closingBroker) BeginClose() { b.event("begin-close") }

func (b closingBroker) Close() error {
	b.event("backend-close")
	return b.Broker.Close()
}

func (b closingBroker) event(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	*b.log = append(*b.log, name)
}

// TestWrapForwardsBeginClose: Wrap with zero Faults changes nothing, so a
// ClosingBackend behind it still hears BeginClose, once and before its Close.
// The wrapper forwarded Admin and Health and not BeginClose, so a driver
// tested through a harness that wraps it was back to a Client.Close that
// waits behind a write parked in its connection.
func TestWrapForwardsBeginClose(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		br, err := memory.NewBroker(memory.Config{})
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var log []string
		var b mq.Backend = closingBroker{Broker: br, mu: &mu, log: &log}
		if wrapped {
			b = testkit.Wrap(b, testkit.Faults{})
		}
		if err := mq.NewClient(b).Close(); err != nil {
			t.Fatalf("wrapped=%v: Client.Close = %v, want nil", wrapped, err)
		}
		mu.Lock()
		got := slices.Clone(log)
		mu.Unlock()
		if want := []string{"begin-close", "backend-close"}; !slices.Equal(got, want) {
			t.Fatalf("wrapped=%v: events = %v, want %v", wrapped, got, want)
		}
	}
}

// TestWrapOverABackendWithoutBeginClose: the forwarding step is a no-op for a
// backend that has none, so wrapping one changes nothing about its close.
func TestWrapOverABackendWithoutBeginClose(t *testing.T) {
	br, err := memory.NewBroker(memory.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mq.Backend(br).(mq.ClosingBackend); ok {
		t.Fatal("the memory broker has a BeginClose of its own; this test needs one without")
	}
	if err := mq.NewClient(testkit.Wrap(br, testkit.Faults{})).Close(); err != nil {
		t.Fatalf("Client.Close = %v, want nil", err)
	}
}
