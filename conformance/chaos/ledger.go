// Package chaos holds the toxiproxy-driven fault scenarios and the ledger
// that turns "some messages were lost" into a precise accounting: every
// publish is recorded with its outcome and every delivery is recorded by id,
// so that after a drain the ledger can assert that no accepted message is
// missing and count duplicates, rejected-but-delivered and ambiguous
// messages separately.
package chaos

import (
	"fmt"
	"sort"
	"sync"
)

// Outcome is the publish outcome recorded for a message id.
type Outcome uint8

// Outcome values.
const (
	// Accepted means the publish returned nil: the message must be delivered.
	Accepted Outcome = iota + 1
	// Rejected means the broker definitely refused it: delivery is a defect.
	Rejected
	// Ambiguous means acceptance is unknown: delivery is neither required
	// nor a defect.
	Ambiguous
)

// String implements fmt.Stringer.
func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case Rejected:
		return "rejected"
	case Ambiguous:
		return "ambiguous"
	}
	return "unknown"
}

// Ledger records publishes and deliveries. It is safe for concurrent use.
type Ledger struct {
	mu        sync.Mutex
	published map[string]Outcome
	delivered map[string]int
}

// NewLedger returns an empty ledger.
func NewLedger() *Ledger {
	return &Ledger{published: map[string]Outcome{}, delivered: map[string]int{}}
}

// Published records the outcome of publishing id. Recording the same id
// twice keeps the stronger outcome (Accepted > Ambiguous > Rejected), which
// matches a retry that eventually succeeds.
func (l *Ledger) Published(id string, o Outcome) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur, ok := l.published[id]; ok && rank(cur) >= rank(o) {
		return
	}
	l.published[id] = o
}

func rank(o Outcome) int {
	switch o {
	case Accepted:
		return 3
	case Ambiguous:
		return 2
	case Rejected:
		return 1
	}
	return 0
}

// Delivered records one delivery of id.
func (l *Ledger) Delivered(id string) {
	l.mu.Lock()
	l.delivered[id]++
	l.mu.Unlock()
}

// Report is the result of a Check.
type Report struct {
	// Published counts per outcome.
	Published map[Outcome]int
	// Delivered is the number of distinct ids delivered.
	Delivered int
	// Missing lists accepted ids never delivered: a loss.
	Missing []string
	// Duplicates counts extra deliveries of ids (at-least-once duplicates).
	Duplicates int
	// RejectedDelivered lists ids reported rejected that were delivered.
	RejectedDelivered []string
	// AmbiguousDelivered counts ambiguous ids that turned out delivered.
	AmbiguousDelivered int
	// Unknown lists delivered ids that were never published by this ledger.
	Unknown []string
}

// OK reports whether the ledger shows no loss and no rejected-but-delivered
// message. Duplicates are allowed (at-least-once).
func (r *Report) OK() bool {
	return len(r.Missing) == 0 && len(r.RejectedDelivered) == 0 && len(r.Unknown) == 0
}

// Error returns a description of the defects, or nil.
func (r *Report) Error() error {
	if r.OK() {
		return nil
	}
	return fmt.Errorf("chaos ledger: %d accepted message(s) lost %v, %d rejected message(s) delivered %v, %d unknown id(s) %v",
		len(r.Missing), head(r.Missing), len(r.RejectedDelivered), head(r.RejectedDelivered), len(r.Unknown), head(r.Unknown))
}

func head(ids []string) []string {
	if len(ids) > 5 {
		return append(ids[:5:5], "…")
	}
	return ids
}

// Check computes the report. Call it after the consumer has drained.
func (l *Ledger) Check() *Report {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := &Report{Published: map[Outcome]int{}}
	for id, o := range l.published {
		r.Published[o]++
		n := l.delivered[id]
		switch {
		case n == 0 && o == Accepted:
			r.Missing = append(r.Missing, id)
		case n > 0 && o == Rejected:
			r.RejectedDelivered = append(r.RejectedDelivered, id)
		case n > 0 && o == Ambiguous:
			r.AmbiguousDelivered++
		}
		if n > 1 {
			r.Duplicates += n - 1
		}
	}
	for id := range l.delivered {
		if _, ok := l.published[id]; !ok {
			r.Unknown = append(r.Unknown, id)
			continue
		}
		r.Delivered++
	}
	sort.Strings(r.Missing)
	sort.Strings(r.RejectedDelivered)
	sort.Strings(r.Unknown)
	return r
}

// Reset clears the ledger.
func (l *Ledger) Reset() {
	l.mu.Lock()
	l.published = map[string]Outcome{}
	l.delivered = map[string]int{}
	l.mu.Unlock()
}
