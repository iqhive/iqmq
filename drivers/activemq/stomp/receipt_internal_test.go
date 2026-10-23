package stomp

import "testing"

// TestSinksOnOneBackendNameDistinctReceipts: every sink publishes over the
// backend's one connection, and go-stomp keys a pending receipt by its id, so
// two sinks that both ask for "iqmq-1" overwrite each other's wait -- one
// sink's publication is reported confirmed on the other's receipt. The ids
// must be unique on the connection, not merely within a sink.
func TestSinksOnOneBackendNameDistinctReceipts(t *testing.T) {
	b := &Backend{}
	a, c := &Sink{b: b}, &Sink{b: b}
	seen := map[string]bool{}
	for range 3 {
		for _, s := range []*Sink{a, c} {
			id := s.receiptID()
			if seen[id] {
				t.Fatalf("receipt id %q issued twice on one backend's connection", id)
			}
			seen[id] = true
		}
	}
}
