package idempotency_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/x/idempotency"
)

// TestCloseRacesClaimForThousandsOfRounds is this repository's recurring
// defect class, applied to a claim table.
//
// The class is "a record put into a lane, a store or a retained log while its
// subscription or the engine is closing": the placing goroutine checks the
// closed flag, Close drains, and the placement lands after the drain. It has
// been publish-vs-close and nack-vs-close in, nack-vs-close again in,
// and Seek-vs-close plus publish-vs-retained-log in.
//
// The hazard is NARROWER here and the reason is worth stating rather than
// assuming: a claim carries no payload, no Release callback and no
// engine-wide reservation, so a claim that landed after a Close would strand
// nothing — it would only be a row in a table nobody will read. What must not
// happen is a data race, a panic, or a closed store that still holds rows. So
// the check is on the flag and the table being under ONE mutex, which is the
// established fix for this class, rather than on a reservation that does not
// exist.
//
// Three thousand rounds under -race, because the known instances of this class
// appeared at five to twenty failures per two or three thousand and a hundred
// rounds proves nothing.
func TestCloseRacesClaimForThousandsOfRounds(t *testing.T) {
	const (
		rounds  = 3000
		workers = 4
	)
	for range rounds {
		s, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{MaxEntries: workers})
		if err != nil {
			t.Fatalf("NewMemoryStore: %v", err)
		}
		var (
			wg    sync.WaitGroup
			start = make(chan struct{})
			mu    sync.Mutex
			held  []idempotency.Claim
		)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				c, err := s.Claim(context.Background(), "k", time.Minute)
				switch {
				case errors.Is(err, idempotency.ErrClosed):
					// The claim lost the race, which is the answer a caller
					// has a policy for.
					return
				case err != nil:
					mu.Lock()
					t.Errorf("Claim reported %v, want nil or ErrClosed", err)
					mu.Unlock()
					return
				}
				mu.Lock()
				defer mu.Unlock()
				held = append(held, c)
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := s.Close(); err != nil {
				mu.Lock()
				t.Errorf("Close: %v", err)
				mu.Unlock()
			}
		}()
		close(start)
		wg.Wait()

		// A closed store holds nothing, whichever order the race resolved in.
		// This is the assertion: a claim that landed after the close would
		// leave the table non-empty.
		if got := s.Stats().Entries; got != 0 {
			t.Fatalf("a closed store holds %d entr(y/ies): a claim landed after Close dropped the table", got)
		}
		// And at most one worker was told it was first, however many raced
		// with the close.
		mu.Lock()
		firsts := 0
		for _, c := range held {
			if c.Status == idempotency.StatusFirst {
				firsts++
			}
		}
		mu.Unlock()
		if firsts > 1 {
			t.Fatalf("%d workers were told they were first for one key", firsts)
		}
		// Every surviving claim is now uncommittable, because the store is
		// closed. A caller that held one when the process shut down learns
		// so from Commit rather than from silence.
		for _, c := range held {
			if !c.Status.Held() {
				continue
			}
			if err := s.Commit(context.Background(), c); !errors.Is(err, idempotency.ErrClosed) {
				t.Fatalf("Commit of a claim held across Close reported %v, want ErrClosed", err)
			}
		}
	}
}
