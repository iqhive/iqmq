package stomp

import (
	"context"
	"errors"
	"io"
	"testing"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// TestASettlementGoesOutOnTheDeliverysOwnConnection is [settler.conn]. It used
// to ask the source for its connection, which re-dialled when the delivery's
// had died after [settler.begin], and the ACK then went out on a socket that
// had no such subscription.
func TestASettlementGoesOutOnTheDeliverysOwnConnection(t *testing.T) {
	t.Parallel()
	dead, live := &gostomp.Conn{}, &gostomp.Conn{}
	s := &settler{src: &Source{b: &Backend{BaseBackend: &driverkit.BaseBackend{}}}}

	if _, err := s.conn(context.Background(), &held{conn: dead}); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("a delivery whose connection the source no longer holds got %v, want ErrLeaseLost", err)
	}
	s.src.conn = live
	if _, err := s.conn(context.Background(), &held{conn: dead}); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("a delivery from a connection the source has replaced got %v, want ErrLeaseLost", err)
	}
	got, err := s.conn(context.Background(), &held{conn: live})
	if err != nil || got != live {
		t.Errorf("a delivery on the live connection got %p, %v, want its own connection", got, err)
	}
}

// TestAFailedDeadLetterCopyLeavesTheDeliverySettleable is
// [settler.deadLetterFailed]. Mapped under OpNack, a dead backend connection
// read as LeaseLost, which made the core forget a delivery still unacknowledged
// on the source's own connection, and a receipt timeout as SettlementUnknown,
// which parked its slot for ever. Neither copy settled the original, so both
// must be definite, retryable refusals that leave the slot active.
func TestAFailedDeadLetterCopyLeavesTheDeliverySettleable(t *testing.T) {
	t.Parallel()
	for _, code := range []mq.ErrorCode{mq.CodeLeaseLost, mq.CodeSettlementUnknown} {
		table, err := driverkit.NewTokenTable(1)
		if err != nil {
			t.Fatalf("NewTokenTable: %v", err)
		}
		s := &settler{src: &Source{table: table}}
		token, ok := table.Claim(&held{})
		if !ok {
			t.Fatal("Claim found no free slot in an empty table")
		}
		if _, err := table.Settle(token); err != nil {
			t.Fatalf("Settle: %v", err)
		}
		err = s.deadLetterFailed(&mq.DriverDelivery{Token: token},
			&mq.OpError{Operation: mq.OpNack, Code: code, Ambiguous: true})
		var oe *mq.OpError
		if !errors.As(err, &oe) {
			t.Fatalf("%v: deadLetterFailed returned %v, want an *mq.OpError", code, err)
		}
		if oe.Code != mq.CodeUnavailable || !oe.Retryable || oe.Ambiguous {
			t.Errorf("%v became code %v retryable=%v ambiguous=%v, want Unavailable, retryable, not ambiguous",
				code, oe.Code, oe.Retryable, oe.Ambiguous)
		}
		if st, err := table.State(token); err != nil || st != driverkit.StateActive {
			t.Errorf("%v left the slot %v (%v), want it active so the delivery can be settled again", code, st, err)
		}
	}
}

// TestAnAmbiguousSettlementKeepsItsSlotWhateverItsCode is [settler.finish]'s
// first arm.
//
// MapNet keeps the code of the rule that matched and ORs in the transport's
// ambiguity, so a settlement whose frame may have left when its connection went
// -- an EOF with `sentAlready` -- is coded LeaseLost by `lost(gone)` and flagged
// ambiguous. finish switched on the code alone, took the lost-lease arm and
// RELEASED the slot: a delivery whose ACK the broker may have applied became
// one the next delivery could take the slot of. The token table reads the flag
// (`mq.IsAmbiguous`), and the driver has to read what the table reads.
func TestAnAmbiguousSettlementKeepsItsSlotWhateverItsCode(t *testing.T) {
	t.Parallel()
	table, err := driverkit.NewTokenTable(1)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	s := &settler{src: &Source{b: &Backend{errs: newErrorMapper()}, table: table}}
	token, ok := table.Claim(&held{})
	if !ok {
		t.Fatal("Claim found no free slot in an empty table")
	}
	if _, err := table.Settle(token); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	// A connection the source does not hold, so finish's own repair is a
	// no-op and the only thing under test is the arm it takes.
	got := s.finish(&mq.DriverDelivery{Token: token}, &gostomp.Conn{}, io.ErrUnexpectedEOF, mq.OpAck)
	if !mq.IsAmbiguous(got) {
		t.Fatalf("finish mapped an EOF after the frame was handed over to %v, which is not ambiguous; the fixture "+
			"no longer reaches the case under test", got)
	}
	if st, err := table.State(token); err != nil || st == driverkit.StateFree {
		t.Errorf("an ambiguous settlement left the slot %v (%v): it was released, so a delivery whose ACK the "+
			"broker may have applied can be replaced in its slot and the outcome is lost", st, err)
	}
	if n := s.src.stale.Load(); n != 0 {
		t.Errorf("an ambiguous settlement was counted as %d stale refusal(s); it was not refused, its outcome is "+
			"unknown", n)
	}
}
