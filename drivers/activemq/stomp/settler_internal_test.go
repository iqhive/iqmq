package stomp

import (
	"context"
	"errors"
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
