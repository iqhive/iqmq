package rabbitfake

import (
	"testing"

	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
)

// TestConnRefusesAGoroutineOnceItHasDrained pins [fakeConn.goIfLive]'s half of
// the interlock between a consume that is still in flight and the connection
// close that overtakes it.
//
// It is an INTERNAL test, and deliberately so: the damage the interlock
// prevents is `sync.WaitGroup` misuse -- an `Add` whose counter was driven to
// zero by a `Wait` that has already returned -- and the only observable the
// fake leaves behind for it is a race report, which is probabilistic. The
// contract the registration has to keep is not, so that is what is held here:
// a connection that has drained accepts no further goroutines, and a live one
// accepts them.
//
// `fakeChan.ConsumeWithContext` releases the broker's mutex before it registers
// its context watcher, so the whole of a connection close can run in between;
// `FuzzRabbitTagSettlement`'s seed 6dad9459895ffff8 is a script that does
// exactly that, and the detector's report names `source.attachLocked` on the
// registering side and `fakeConn.drain` on the waiting side (issue 76).
func TestConnRefusesAGoroutineOnceItHasDrained(t *testing.T) {
	b := New(Config{})
	t.Cleanup(func() { _ = b.Close() })

	b.mu.Lock()
	c := b.newConnLocked(client.DialConfig{})
	b.mu.Unlock()
	c.start()

	ran := make(chan struct{})
	if !c.goIfLive(func() { close(ran) }) {
		t.Fatal("a live connection refused to register a goroutine, so the rest of this test would pass for the wrong reason")
	}
	<-ran

	c.close(nil, false)

	// The connection has drained: `drain` has waited its goroutine group down
	// to nothing and marked the connection dead. A registration here is the
	// misuse, and it is the one the race detector catches when it happens
	// concurrently instead of after.
	release := make(chan struct{})
	defer close(release)
	if c.goIfLive(func() { <-release }) {
		t.Fatal("a drained connection registered a goroutine on its WaitGroup: `drain`'s Wait has already driven the counter to zero, so this Add is `sync.WaitGroup`'s documented misuse and, when it happens concurrently with that Wait rather than after it, a data race (issue 76)")
	}
}
