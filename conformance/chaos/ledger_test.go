package chaos

import (
	"strconv"
	"sync"
	"testing"
)

func TestLedger(t *testing.T) {
	l := NewLedger()
	l.Published("a", Accepted)
	l.Published("b", Accepted)
	l.Published("c", Rejected)
	l.Published("d", Ambiguous)
	l.Published("e", Ambiguous)
	l.Published("e", Accepted) // retry succeeded: stronger outcome wins
	l.Published("e", Rejected) // weaker outcome ignored
	l.Delivered("a")
	l.Delivered("a")
	l.Delivered("c")
	l.Delivered("d")
	l.Delivered("e")
	l.Delivered("zzz")
	r := l.Check()
	if r.OK() {
		t.Fatal("report should not be ok")
	}
	if len(r.Missing) != 1 || r.Missing[0] != "b" {
		t.Errorf("missing = %v", r.Missing)
	}
	if len(r.RejectedDelivered) != 1 || r.RejectedDelivered[0] != "c" {
		t.Errorf("rejected delivered = %v", r.RejectedDelivered)
	}
	if r.Duplicates != 1 || r.AmbiguousDelivered != 1 || r.Delivered != 4 {
		t.Errorf("dups=%d ambiguous=%d delivered=%d", r.Duplicates, r.AmbiguousDelivered, r.Delivered)
	}
	if len(r.Unknown) != 1 || r.Unknown[0] != "zzz" {
		t.Errorf("unknown = %v", r.Unknown)
	}
	if r.Published[Accepted] != 3 || r.Published[Rejected] != 1 || r.Published[Ambiguous] != 1 {
		t.Errorf("published = %v", r.Published)
	}
	if err := r.Error(); err == nil {
		t.Error("Error() nil for defective report")
	}
	l.Reset()
	if ok := l.Check(); !ok.OK() || ok.Error() != nil {
		t.Error("reset ledger not ok")
	}
	if Outcome(9).String() != "unknown" || Accepted.String() != "accepted" {
		t.Error("String")
	}
}

func TestLedgerConcurrent(t *testing.T) {
	l := NewLedger()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				id := strconv.Itoa(w*1000 + i)
				l.Published(id, Accepted)
				l.Delivered(id)
			}
		})
	}
	wg.Wait()
	r := l.Check()
	if !r.OK() || r.Delivered != 8000 || r.Duplicates != 0 {
		t.Errorf("concurrent report = %+v", r)
	}
	if h := head([]string{"1", "2", "3", "4", "5", "6", "7"}); len(h) != 6 || h[5] != "…" {
		t.Errorf("head = %v", h)
	}
}
