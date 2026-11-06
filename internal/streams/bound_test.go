package streams_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// readBlocks is the fake keeping the BLOCK argument of every XREADGROUP it is
// sent, "" for a read that polls, with the error the read ended with.
type readBlocks struct {
	*fakestreams.Server

	mu     sync.Mutex
	blocks []string
	errs   []error
}

func (s *readBlocks) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	v, err := s.Server.Do(ctx, c)
	if c.Name() == "XREADGROUP" {
		block := ""
		if i := slices.Index(c.Tokens, "BLOCK"); i >= 0 {
			block = c.Tokens[i+1]
		}
		s.mu.Lock()
		s.blocks = append(s.blocks, block)
		s.errs = append(s.errs, err)
		s.mu.Unlock()
	}
	return v, err
}

// TestReadEndsItsBlockBeforeItsContext is a read under a context whose
// deadline is nearer than Block. It used to send BLOCK Block whatever the
// deadline, so the client gave up on a read the server still held: both
// clients close the connection such a read waited on, and an entry the server
// handed it at the last moment was pending to this consumer and never
// delivered, until the sweep claimed it a deadline later as attempt 2.
func TestReadEndsItsBlockBeforeItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Block = 5 * time.Second })
		log := &readBlocks{Server: r.srv}
		for _, tc := range []struct {
			name  string
			left  time.Duration
			block string
		}{
			{"no deadline", 0, "5000"},
			{"a deadline past the block", time.Minute, "5000"},
			{"a deadline inside the block", time.Second, "850"},
			{"a short deadline", 200 * time.Millisecond, "50"},
			{"a deadline 2ms past the margin", 152 * time.Millisecond, "2"},
			{"a deadline within the margin", 100 * time.Millisecond, ""},
		} {
			ctx, cancel := r.ctx, context.CancelFunc(func() {})
			if tc.left > 0 {
				ctx, cancel = context.WithTimeout(r.ctx, tc.left)
			}
			_, _, err := r.cons.Read(ctx, log, nil, nil)
			cancel()
			log.mu.Lock()
			block, rerr := log.blocks[len(log.blocks)-1], log.errs[len(log.errs)-1]
			log.mu.Unlock()
			if block != tc.block {
				t.Errorf("%s: the read sent BLOCK %q, want %q", tc.name, block, tc.block)
			}
			if err != nil || rerr != nil {
				t.Errorf("%s: the read ended with %v (the server's answer: %v), want the server's empty answer", tc.name, err, rerr)
			}
		}
	})
}

// TestSweepAtMostClaimsNoMoreThanItIsAsked is the bound a caller with room
// for fewer entries than ClaimCount puts on a round: n per shard, and none
// at all for n of zero. A plain Sweep still claims up to ClaimCount.
func TestSweepAtMostClaimsNoMoreThanItIsAsked(t *testing.T) {
	crashed := newRig(t, "crashed", 1)
	for range 5 {
		crashed.publish(t, "x")
	}
	if got := crashed.read(t); len(got) != 5 {
		t.Fatalf("the consumer that crashes read %d entries, want 5", len(got))
	}
	cons, err := streams.NewConsumer(streams.ConsumerConfig{
		Keys: crashed.keys, Group: testGroup, Name: "live", Prefetch: 16, AckDeadline: testLease,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	crashed.srv.Advance(testLease + time.Second)
	owned := func() int {
		n := 0
		for _, p := range crashed.srv.Pending(crashed.keys.Stream(0), testGroup) {
			if p.Consumer == "live" {
				n++
			}
		}
		return n
	}
	for _, step := range []struct {
		n, want int
	}{{0, 0}, {2, 2}, {-1, 0}} {
		got, _, err := cons.SweepAtMost(crashed.ctx, crashed.srv, step.n, nil, nil)
		if err != nil {
			t.Fatalf("SweepAtMost(%d): %v", step.n, err)
		}
		if len(got) != step.want {
			t.Errorf("SweepAtMost(%d) claimed %d entries, want %d", step.n, len(got), step.want)
		}
	}
	if n := owned(); n != 2 {
		t.Errorf("the consumer owns %d entries after the bounded rounds, want 2", n)
	}
	got, _, err := cons.Sweep(crashed.ctx, crashed.srv, nil, nil)
	if err != nil || len(got) != 3 {
		t.Errorf("Sweep claimed %d entries, %v, want the other 3", len(got), err)
	}
}
