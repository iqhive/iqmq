package jetstream_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	sjs "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// TestASynchronousPublicationTheConnectionCutIsAmbiguous is a Publish waiting
// for its PubAck when the connection is cut under it. A synchronous
// publication is a request, and nats.go fails a request still waiting for its
// reply with ErrDisconnected when it is told to reconnect and with
// ErrConnectionClosed when it closes. The driver read the first as a definite,
// retryable Unavailable and the second as a definite Closed, as it does for a
// write refused before anything left, so the core retried a message the stream
// already held.
//
// The stream is created with NoAck and holds the message before the cut, so
// nothing but the cut answers the publication, and the PubAck timeout is a
// minute. A reconnection is the drop the asynchronous paths already answer
// SettlementUnknown. A close stays Closed, because the connection is not
// coming back, and is ambiguous too.
func TestASynchronousPublicationTheConnectionCutIsAmbiguous(t *testing.T) {
	cuts := []struct {
		name string
		cut  func(*natsgo.Conn) error
		code mq.ErrorCode
	}{
		{"a forced reconnection", (*natsgo.Conn).ForceReconnect, mq.CodeSettlementUnknown},
		{"the connection closes", func(nc *natsgo.Conn) error { nc.Close(); return nil }, mq.CodeClosed},
	}
	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
			b := openBackend(t, func(cfg *nq.Config) {
				cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
				cfg.PublishTimeout = time.Minute
			})
			snk := sinkOn(t, b, newTopic("cut"), mq.ConfirmAccepted)
			res := make(chan error, 1)
			go func() { res <- snk.Publish(t.Context(), &mq.Message{Body: []byte("cut")}, nil) }()
			waitForStored(t, stream, 1, 10*time.Second)
			if err := cut.cut(b.Conn()); err != nil {
				t.Fatalf("cutting the connection: %v", err)
			}
			var err error
			select {
			case err = <-res:
			case <-time.After(10 * time.Second):
				t.Fatal("the publication did not return after the connection was cut")
			}
			if !mq.IsAmbiguous(err) || mq.CodeOf(err) != cut.code {
				t.Errorf("a publication the stream holds and %s cut reported %v (code %v, ambiguous %v, retryable %v), "+
					"want an ambiguous %v: a retry duplicates the message",
					cut.name, err, mq.CodeOf(err), mq.IsAmbiguous(err), mq.IsRetryable(err), cut.code)
			}
			// The flag is read off the *mq.OpError a close answers with,
			// because the ambiguity hides it from mq.IsRetryable. A closed
			// connection is not coming back, so it stays false.
			var oe *mq.OpError
			if errors.As(err, &oe) && oe.Retryable {
				t.Errorf("a publication %s cut reported %v with the retryable flag set; the connection is not coming back",
					cut.name, err)
			}
		})
	}
}

// TestASynchronousPublicationOnAClosedConnectionIsDefinite is the other side
// of the close. nats.go refuses a request on a connection that has already
// closed before it writes anything, so nothing reached the stream and the
// answer stays a definite Closed.
func TestASynchronousPublicationOnAClosedConnectionIsDefinite(t *testing.T) {
	b := openBackend(t)
	snk := sinkOn(t, b, newTopic("closed"), mq.ConfirmAccepted)
	b.Conn().Close()
	err := snk.Publish(t.Context(), &mq.Message{Body: []byte("refused")}, nil)
	var oe *mq.OpError
	if !errors.As(err, &oe) || oe.Code != mq.CodeClosed || mq.IsAmbiguous(err) {
		t.Fatalf("a publication on a closed connection reported %v (code %v, ambiguous %v), want a definite Closed: nothing was written",
			err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
}

// sinkOn opens an SPI sink on topic with confirm, closed on cleanup.
func sinkOn(tb testing.TB, b *nq.Backend, topic string, confirm mq.ConfirmationMode) mq.Sink {
	tb.Helper()
	snk, err := b.OpenSink(tb.Context(), mq.SinkConfig{Topic: topic, Confirmation: confirm})
	if err != nil {
		tb.Fatalf("OpenSink: %v", err)
	}
	tb.Cleanup(func() { _ = snk.Close() })
	return snk
}

// waitForStored waits until stream holds at least n messages. On a NoAck
// stream it is the only sign that a publication has reached the stream, since
// nothing answers it.
func waitForStored(tb testing.TB, stream string, n uint64, budget time.Duration) {
	tb.Helper()
	jsc, err := sjs.New(natsConn(tb))
	if err != nil {
		tb.Fatalf("jetstream context: %v", err)
	}
	for deadline := time.Now().Add(budget); ; time.Sleep(time.Millisecond) {
		st, err := jsc.Stream(tb.Context(), stream)
		if err != nil {
			tb.Fatalf("looking up stream %s: %v", stream, err)
		}
		info, err := st.Info(tb.Context())
		if err != nil {
			tb.Fatalf("reading stream %s: %v", stream, err)
		}
		if info.State.Msgs >= n {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("stream %s held %d messages after %v, want %d", stream, info.State.Msgs, budget, n)
		}
	}
}
