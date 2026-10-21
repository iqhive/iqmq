package iqmq

import (
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
)

// TestLeaseNextInIsAlwaysPositiveAndAtMostHalfTheGrant is the specification's
// arithmetic. The scheduler re-schedules each extension at now plus this
// interval, so an interval at or below zero fires the same ticket immediately
// and for ever — which is what a lead at or above the lease used to produce.
//
// Two properties are asserted for every row rather than only the expected
// number, because the number is a consequence and the properties are the
// contract: the interval is positive, and it is strictly less than the amount
// granted, so the next extension lands before the deadline it is renewing.
func TestLeaseNextInIsAlwaysPositiveAndAtMostHalfTheGrant(t *testing.T) {
	for _, tc := range []struct {
		name string
		lead time.Duration
		by   time.Duration
		want time.Duration
	}{
		{"a lead well under half the lease is honoured exactly", time.Second, time.Minute, 59 * time.Second},
		{"a lead of exactly half the lease is the floor", 30 * time.Second, time.Minute, 30 * time.Second},
		{"a lead over half the lease falls back to the floor", 40 * time.Second, time.Minute, 30 * time.Second},
		{"a lead equal to the lease does not schedule at zero", time.Minute, time.Minute, 30 * time.Second},
		{"a lead four times the lease does not schedule in the past", 200 * time.Millisecond, 50 * time.Millisecond, 25 * time.Millisecond},
		{"a two-nanosecond lease still schedules forwards", time.Hour, 2 * time.Nanosecond, time.Nanosecond},
		// Half of one nanosecond is zero, so the floor needs its own floor:
		// an interval of zero is the same spin arrived at by rounding
		// instead of by subtraction.
		{"a one-nanosecond lease does not round the interval to zero", time.Hour, time.Nanosecond, time.Nanosecond},
		{"a one-nanosecond lease and a one-nanosecond lead", time.Nanosecond, time.Nanosecond, time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &leaseScheduler{lead: tc.lead}
			got := s.nextIn(tc.by)
			if got != tc.want {
				t.Fatalf("nextIn(%v) with lead %v = %v, want %v", tc.by, tc.lead, got, tc.want)
			}
			// Property one, which holds for every input: the interval is
			// positive, so the run goroutine parks instead of re-firing the
			// same ticket.
			if got <= 0 {
				t.Fatalf("nextIn(%v) with lead %v = %v: the scheduler fires the same ticket for ever", tc.by, tc.lead, got)
			}
			// Property two: the next extension lands before the deadline it
			// is renewing. It is unachievable for a one-nanosecond lease —
			// there is no duration strictly between zero and one — and a
			// lease that short is a broken driver rather than a broker. The
			// scheduler gives up the race and keeps the parking, which is the
			// right way round: a lost lease is reported at the settlement,
			// and a saturated core is reported by nobody.
			if tc.by > time.Nanosecond && got >= tc.by {
				t.Fatalf("nextIn(%v) with lead %v = %v, which is not before the deadline it renews", tc.by, tc.lead, got)
			}
		})
	}
}

// TestLeaseAddNamesTheTicketUnderTheLock pins where the scheduler entry is
// pointed at its ticket: in add, under the lock the run goroutine fires
// entries under, so that a ticket reused by the next delivery in its slot is
// never written while that goroutine may be reading it.
func TestLeaseAddNamesTheTicketUnderTheLock(t *testing.T) {
	s, err := newLeaseScheduler(clock.Real{}, time.Second, time.Minute)
	if err != nil {
		t.Fatalf("newLeaseScheduler: %v", err)
	}
	defer s.close()
	var tk leaseTicket
	s.add(&tk, time.Now().Add(time.Hour).UnixNano())
	s.mu.Lock()
	got := tk.entry.Value
	s.mu.Unlock()
	s.release(&tk)
	if got != &tk {
		t.Fatalf("the scheduled entry names %p, want its own ticket %p", got, &tk)
	}
}
