package stomp_test

import (
	"context"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// A connection the LIBRARY closes. go-stomp ends a connection itself when a
// heart-beat it was owed does not arrive and when the broker sends an ERROR
// frame, and the socket watcher used to treat every close as the driver's own
// and stay silent -- so the one death that needs no broker and no network to
// happen was the one nothing heard.

// TestAConnectionTheLibraryClosesIsNoticed is the backend's connection timed
// out by go-stomp's own heart-beat check while nothing was publishing.
//
// The library's process loop closes the socket and the watcher disarmed on the
// close: health stayed Ready, the dial loop was never woken, and the client
// learned of the death only when a publication met `ErrAlreadyClosed`. The
// read timeout is the promised interval plus go-stomp's fixed five-second
// heart-beat allowance, which is why this test waits as long as it does.
func TestAConnectionTheLibraryClosesIsNoticed(t *testing.T) {
	t.Parallel()
	const interval = 100 * time.Millisecond
	srv := newServer(t, stompsrv.Options{
		ServerHeader:      activemq.ServerArtemis + "/2.44.0",
		PromiseHeartBeats: interval,
	})
	cfg := reconnecting(artemisConfig(srv))
	cfg.HeartbeatReceive = interval
	b := openBackend(t, cfg)

	deadline := time.Now().Add(4 * budget)
	for b.Reconnects() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if b.Reconnects() == 0 {
		t.Fatalf("go-stomp closed the backend's connection when the promised heart-beats did not arrive, and "+
			"the driver never re-dialled (%d connection(s) accepted, health %v): the death was never reported",
			srv.Accepted(), b.Health().State)
	}
}

// TestAPublicationOnAConnectionAlreadyClosedIsNotAmbiguous is the publish path
// meeting a connection that is already closed.
//
// `Conn.Send` answers `ErrAlreadyClosed` from its first check, before the frame
// is built, so the broker cannot have the message. It was mapped with every
// other closed-connection error as AMBIGUOUS: the caller was told a
// publication that provably never left might have been taken, and the window
// counter counted it.
func TestAPublicationOnAConnectionAlreadyClosedIsNotAmbiguous(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	conn, err := gostomp.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server directly: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	b, err := stomp.BackendWithClient(ctx, conn, driverkit.ClientBorrowed, artemisConfig(srv))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)

	// The caller closes its own connection, which is the plainest way to put
	// a closed connection under a sink.
	if err := conn.Disconnect(); err != nil {
		t.Fatalf("closing the borrowed connection: %v", err)
	}
	err = snk.Publish(ctx, &mq.Message{Body: []byte("x")}, nil)
	if err == nil {
		t.Fatal("a publication on a closed connection reported success")
	}
	if mq.IsAmbiguous(err) {
		t.Errorf("a publication refused before its frame was built reports an unknown outcome: %v", err)
	}
	if !mq.IsRetryable(err) || mq.CodeOf(err) != mq.CodeUnavailable {
		t.Errorf("the refusal is %v (code %v, retryable %v), want a retryable Unavailable", err, mq.CodeOf(err),
			mq.IsRetryable(err))
	}
	if got := b.Ambiguous(); got != 0 {
		t.Errorf("Backend.Ambiguous() is %d after a publication that never left", got)
	}
	if got := countOf(srv, frame.SEND); got != 0 {
		t.Errorf("the server received %d SEND frame(s); the fixture closed the connection first", got)
	}
}
