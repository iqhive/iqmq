package kafka

import (
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/driverkit/offset"
)

// TestRevokeWaitAcceptsInflightSettlement pins step 2 of the revocation
// sequence. The wait is for the in-flight deliveries to settle, so a
// settlement that arrives during it must be accepted: refusing it as lease
// lost left the wait to run out the whole RevokeWait, and the boundary
// committed in step 3 then omitted a record the handler had finished.
func TestRevokeWaitAcceptsInflightSettlement(t *testing.T) {
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 10, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Track(tr.Gen(), 10); err != nil {
		t.Fatal(err)
	}
	key := tp{"t", 0}
	p := &partState{tp: key, tr: tr, inflight: 1, committed: 10, firstAt: map[int64]time.Time{}}
	b := &Backend{cfg: Config{RevokeWait: 5 * time.Second}, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	// A client with no group: the step-3 commit is answered at once with
	// "not a group", which is all this test needs from it.
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	s := &source{b: b, cl: cl, topic: "t", parts: map[tp]*partState{key: p}, order: []*partState{p}}
	s.cond = sync.NewCond(&s.mu)
	d := &mq.DriverDelivery{Token: 0, Gen: tr.Gen(), Info: mq.DeliveryInfo{Offset: 10}}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		s.handleRevocation(t.Context(), map[string][]int32{"t": {0}}, true)
	}()
	// Wait for step 1, so the settlement lands during the wait.
	for {
		s.mu.Lock()
		stopped := p.stale
		s.mu.Unlock()
		if stopped {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := (&settler{s: s}).Ack(t.Context(), d); err != nil {
		t.Errorf("Ack during the revoke wait = %v, want nil", err)
	}
	<-done
	if took := time.Since(start); took > time.Second {
		t.Errorf("the revocation took %v: the wait did not see the settlement", took)
	}
	if wm := tr.Watermark(); wm != 11 {
		t.Errorf("watermark after the revocation = %d, want 11", wm)
	}
	if err := (&settler{s: s}).Ack(t.Context(), d); err == nil {
		t.Error("a settlement after the revocation ended was accepted")
	}
}
