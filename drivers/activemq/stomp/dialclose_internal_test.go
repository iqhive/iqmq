package stomp

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// TestADialThatFinishesAfterCloseInstallsNothing is the reconnect loop's dial
// completing once the backend has closed.
//
// Close cancels the supervisor's dial, but only a dial still in its handshake
// notices: [Backend.dialOne] drops a connection whose context ended while it
// waited for CONNECTED. One whose CONNECTED had already arrived went on to
// install its connection after [Backend.closeNative] had taken the old one, and
// that connection, its socket and go-stomp's two goroutines outlived the
// client. The dial here starts after Close, which reaches the same install
// without a race to win.
func TestADialThatFinishesAfterCloseInstallsNothing(t *testing.T) {
	t.Parallel()
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := OpenBackend(ctx, Config{
		Broker:         activemq.Artemis,
		Addrs:          []string{srv.Addr()},
		ReceiptTimeout: time.Second,
		DialTimeout:    5 * time.Second,
		NoReconnect:    true,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("closing the backend: %v", err)
	}

	err = b.dial(ctx)
	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()
	if conn != nil {
		_ = conn.MustDisconnect()
		t.Fatal("a dial that finished after Close installed its connection, which nothing will close")
	}
	if !errors.Is(err, mq.ErrClosed) {
		t.Errorf("the dial reports %v, want ErrClosed", err)
	}
	if err := srv.WaitAccepted(2, 5*time.Second); err != nil {
		t.Fatalf("the dial never reached the server, so this test did not reach the install: %v", err)
	}
}
