package activemq_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Azure/go-amqp"

	"github.com/iqhive/iqmq/driverkit"
	amq "github.com/iqhive/iqmq/drivers/activemq"
)

// TestAdminCallIsBoundedWhenTheBrokerStalls is an Artemis that opens the
// connection and the management session and then answers nothing more: no
// ATTACH for the management links and no END when the session is closed.
//
// The call's own deadline ends the attach. The session's close then ran on
// `context.WithoutCancel` of that context, which drops the deadline as well as
// the cancellation, so the Admin call waited for an END that never came, for
// ever. It must give up within the driver's OperationTimeout instead.
func TestAdminCallIsBoundedWhenTheBrokerStalls(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	go stalledArtemis(server)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := amqp.NewConn(ctx, client, nil)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	cfg := artemisConfig()
	cfg.OperationTimeout = 100 * time.Millisecond
	b, err := amq.BackendWithClient(ctx, conn, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c, err := amq.ClientForBackend(b, cfg)
	if err != nil {
		t.Fatalf("ClientForBackend: %v", err)
	}
	adm, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- adm.Purge(context.Background(), "orders", "billing") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Purge succeeded against a broker that answered nothing")
		}
	case <-time.After(3 * time.Second):
		// Closing the socket releases the parked close, so the goroutine
		// above ends with the test.
		_ = server.Close()
		t.Fatalf("Purge was still waiting 3s after a 100ms OperationTimeout, and returned %v only once the socket closed: "+
			"the session close is unbounded", <-done)
	}
}

// stalledArtemis answers the protocol header, OPEN and BEGIN, and nothing
// else, until its side of the pipe is closed.
func stalledArtemis(c net.Conn) {
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(c, hdr); err != nil {
		return
	}
	if _, err := c.Write(hdr); err != nil {
		return
	}
	for {
		var size [4]byte
		if _, err := io.ReadFull(c, size[:]); err != nil {
			return
		}
		rest := make([]byte, binary.BigEndian.Uint32(size[:])-4)
		if _, err := io.ReadFull(c, rest); err != nil {
			return
		}
		channel := binary.BigEndian.Uint16(rest[2:4])
		body := rest[int(rest[0])*4-4:]
		if len(body) < 3 || body[0] != 0x00 || body[1] != 0x53 {
			continue // an empty (heartbeat) frame
		}
		var reply []byte
		switch body[2] {
		case 0x10: // open: a container id and nothing else
			reply = []byte{0x00, 0x53, 0x10, 0xc0, 7, 1, 0xa1, 4, 'f', 'a', 'k', 'e'}
		case 0x11: // begin: remote-channel, next-outgoing-id and both windows
			reply = []byte{
				0x00, 0x53, 0x11, 0xc0, 15, 4,
				0x60, byte(channel >> 8), byte(channel),
				0x43,
				0x70, 0x00, 0x00, 0xff, 0xff,
				0x70, 0x00, 0x00, 0xff, 0xff,
			}
		default:
			continue
		}
		frame := make([]byte, 8, 8+len(reply))
		binary.BigEndian.PutUint32(frame[0:4], uint32(8+len(reply)))
		frame[4] = 2
		binary.BigEndian.PutUint16(frame[6:8], channel)
		if _, err := c.Write(append(frame, reply...)); err != nil {
			return
		}
	}
}
