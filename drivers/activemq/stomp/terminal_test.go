package stomp_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// A client that will never reconnect. NoReconnect, a spent ReconnectAttempts
// budget, a borrowed connection and a connection the caller gave with no
// Config.Addrs each make the loss of the client's connection terminal, and the
// dial loop stops for good. Every publication and every fetch after that was
// refused with "the connection is not established", which is the reconnect
// window's answer and is retryable, so a caller that believed it retried a
// client that would never come back, for ever.

// TestAPublicationOnAClientThatWillNotReconnectIsNotRetryable is the sink's
// half, once for each way the loss becomes terminal. On a NoReconnect client
// one refused SEND is enough: the broker closes the connection after its ERROR
// frame, and nothing dials another.
func TestAPublicationOnAClientThatWillNotReconnectIsNotRetryable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		open func(t *testing.T, srv *stompsrv.Server) *stomp.Backend
		kill func(t *testing.T, srv *stompsrv.Server, snk mq.Sink)
		// reason is what the refusal has to name, because "Unavailable" on
		// its own leaves an operator guessing which setting stopped the client.
		reason string
	}{
		{
			name:   "no-reconnect",
			open:   func(t *testing.T, srv *stompsrv.Server) *stomp.Backend { return openBackend(t, artemisConfig(srv)) },
			kill:   refuseOneSend,
			reason: "NoReconnect",
		},
		{
			name: "reconnect-attempts-spent",
			open: func(t *testing.T, srv *stompsrv.Server) *stomp.Backend {
				cfg := reconnecting(artemisConfig(srv))
				cfg.ReconnectAttempts = 2
				return openBackend(t, cfg)
			},
			kill: func(t *testing.T, srv *stompsrv.Server, _ mq.Sink) {
				t.Helper()
				srv.RefuseConnects("stompsrv: refusing every new connection")
				dropConnection(t, srv)
			},
			reason: "ReconnectAttempts",
		},
		{
			name: "borrowed",
			open: func(t *testing.T, srv *stompsrv.Server) *stomp.Backend {
				return lentBackend(t, srv, driverkit.ClientBorrowed, artemisConfig(srv))
			},
			kill:   func(t *testing.T, srv *stompsrv.Server, _ mq.Sink) { t.Helper(); dropConnection(t, srv) },
			reason: "borrowed",
		},
		{
			name: "owned-without-addrs",
			open: func(t *testing.T, srv *stompsrv.Server) *stomp.Backend {
				// The dial loop is on, so that what stops it is the missing
				// address and nothing else.
				cfg := reconnecting(artemisConfig(srv))
				cfg.Addrs = nil
				return lentBackend(t, srv, driverkit.ClientOwned, cfg)
			},
			kill:   func(t *testing.T, srv *stompsrv.Server, _ mq.Sink) { t.Helper(); dropConnection(t, srv) },
			reason: "Config.Addrs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := artemisServer(t)
			b := tc.open(t, srv)
			snk := openSink(t, b, "orders", mq.ConfirmAccepted)
			publishOne(t, snk, "before the connection goes")
			tc.kill(t, srv, snk)

			last := settledFailure(func() error {
				ctx, cancel := context.WithTimeout(context.Background(), budget)
				defer cancel()
				return snk.Publish(ctx, &mq.Message{Body: []byte("after")}, nil)
			})
			if last == nil || mq.IsRetryable(last) || mq.IsAmbiguous(last) || mq.CodeOf(last) != mq.CodeUnavailable {
				t.Fatalf("a publication on a client whose connection is gone for good reports %v (code %v, "+
					"retryable %v, ambiguous %v), want a definite, non-retryable Unavailable: nothing will dial "+
					"another connection, so a caller told to retry retries for ever",
					last, mq.CodeOf(last), mq.IsRetryable(last), mq.IsAmbiguous(last))
			}
			if !strings.Contains(last.Error(), tc.reason) {
				t.Errorf("the refusal %v does not name %s, which is what stopped the client", last, tc.reason)
			}
		})
	}
}

// TestTheHealthOfAClientThatWillNotReconnectSaysWhy is the same stop as Health
// reports it, for the two settings the dial loop itself stops on. The client
// goes Degraded for good, and its error was errClosed, which is errors.Is
// mq.ErrClosed: a caller watching Health read a client nobody had closed as
// closed, and the reason every refusal names was nowhere in it.
func TestTheHealthOfAClientThatWillNotReconnectSaysWhy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		open   func(t *testing.T, srv *stompsrv.Server) *stomp.Backend
		kill   func(t *testing.T, srv *stompsrv.Server, snk mq.Sink)
		reason string
	}{
		{
			name:   "no-reconnect",
			open:   func(t *testing.T, srv *stompsrv.Server) *stomp.Backend { return openBackend(t, artemisConfig(srv)) },
			kill:   refuseOneSend,
			reason: "NoReconnect",
		},
		{
			name: "owned-without-addrs",
			open: func(t *testing.T, srv *stompsrv.Server) *stomp.Backend {
				cfg := reconnecting(artemisConfig(srv))
				cfg.Addrs = nil
				return lentBackend(t, srv, driverkit.ClientOwned, cfg)
			},
			kill:   func(t *testing.T, srv *stompsrv.Server, _ mq.Sink) { t.Helper(); dropConnection(t, srv) },
			reason: "Config.Addrs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := artemisServer(t)
			b := tc.open(t, srv)
			snk := openSink(t, b, "orders", mq.ConfirmAccepted)
			publishOne(t, snk, "before the connection goes")
			tc.kill(t, srv, snk)
			// A connection the caller lent has no watcher of its own, so it is
			// a publication that meets the death and reports it.
			_ = settledFailure(func() error {
				ctx, cancel := context.WithTimeout(context.Background(), budget)
				defer cancel()
				return snk.Publish(ctx, &mq.Message{Body: []byte("after")}, nil)
			})

			// Health first reports the death with its own cause, and the dial
			// loop's answer, the one that stays, follows it.
			var h mq.HealthSnapshot
			await(t, "the dial loop giving up", func() bool {
				h = b.Health()
				return h.State == mq.HealthDegraded && h.Err != nil &&
					(errors.Is(h.Err, mq.ErrClosed) || strings.Contains(h.Err.Error(), tc.reason))
			})
			if errors.Is(h.Err, mq.ErrClosed) {
				t.Fatalf("a client that will not reconnect reports health %v with error %v, which is ErrClosed: "+
					"nobody closed it, and the error does not name %s, which is what stopped it", h.State, h.Err, tc.reason)
			}
			if mq.CodeOf(h.Err) != mq.CodeUnavailable {
				t.Errorf("health's error %v has code %v, want Unavailable, the code every refusal reports",
					h.Err, mq.CodeOf(h.Err))
			}
		})
	}
}

// TestAFetchOnAClientThatWillNotReconnectIsNotRetryable is the source's half.
// The source keeps its own connection, but the death of the client's takes the
// source's with it, and a source asks the client for its connection before it
// subscribes again. That question answered "not established" for good, so the
// source's own refusal under NoReconnect was never reached.
func TestAFetchOnAClientThatWillNotReconnectIsNotRetryable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		cfg    func(srv *stompsrv.Server) stomp.Config
		before func(srv *stompsrv.Server)
		reason string
	}{
		{
			name:   "no-reconnect",
			cfg:    artemisConfig,
			before: func(*stompsrv.Server) {},
			// The source's own sentence: its connection went with the
			// client's, and NoReconnect binds a source's re-dial as well.
			reason: "this source's connection was lost",
		},
		{
			name: "reconnect-attempts-spent",
			cfg: func(srv *stompsrv.Server) stomp.Config {
				cfg := reconnecting(artemisConfig(srv))
				cfg.ReconnectAttempts = 2
				return cfg
			},
			before: func(srv *stompsrv.Server) { srv.RefuseConnects("stompsrv: refusing every new connection") },
			reason: "ReconnectAttempts",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := artemisServer(t)
			cfg := tc.cfg(srv)
			b := openBackend(t, cfg)
			src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)
			snk := openSink(t, b, "orders", mq.ConfirmAccepted)
			tc.before(srv)
			// The refused SEND kills the CLIENT's connection and leaves the
			// source's socket alone, which is the case under test: what the
			// source is refused for is the client's answer, not its own.
			refuseOneSend(t, srv, snk)

			last := settledFailure(func() error { return fetchError(t, src, 100*time.Millisecond) })
			if last == nil || mq.IsRetryable(last) || mq.CodeOf(last) != mq.CodeUnavailable {
				t.Fatalf("a fetch on a client whose connection is gone for good reports %v (code %v, retryable "+
					"%v), want a non-retryable Unavailable: a consumer loop told to retry polls a client that will "+
					"never come back", last, mq.CodeOf(last), mq.IsRetryable(last))
			}
			if !strings.Contains(last.Error(), tc.reason) {
				t.Errorf("the refusal %v does not say %q", last, tc.reason)
			}
		})
	}
}

// TestABrokersErrorOnASourceThatWillNotRedialReachesTheNextFetch is a
// broker's ERROR frame on the source's own connection under NoReconnect, over
// a socket.
//
// The socket's death notice and the pump that carries the broker's sentence
// reach the source on different goroutines, and a fetch that ran between them
// reported the source's refusal to re-dial in place of the sentence. Nothing
// holds that gap open here, so without the fetch's wait for the pump this
// failed in 28 runs of 100, and 79 under the race detector;
// TestTheFirstFetchOnASourceThatWillNotRedialReportsTheBrokersReason holds it
// open by hand.
func TestABrokersErrorOnASourceThatWillNotRedialReachesTheNextFetch(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv) // NoReconnect
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	refusal := "AMQ339001 Destination does not exist " + consume
	if err := srv.ErrorFrame(refusal); err != nil {
		t.Fatalf("ErrorFrame: %v", err)
	}

	err := fetchError(t, src, budget)
	if mq.CodeOf(err) != mq.CodeNotFound || !strings.Contains(err.Error(), "AMQ339001") {
		t.Fatalf("the first fetch after the broker's ERROR frame reports %v (code %v), want NotFound with the "+
			"broker's sentence: it is the only diagnosis STOMP offers", err, mq.CodeOf(err))
	}
	if err := fetchError(t, src, 100*time.Millisecond); err == nil || mq.IsRetryable(err) {
		t.Errorf("the fetch after it reports %v, want the source's own non-retryable refusal", err)
	}
}

// settledFailure calls op until it has failed without being retryable three
// times in a row, and returns its last answer. The first answers after a
// death may be the death itself, which is retryable and is reported once;
// what has to follow, and stay, is the refusal.
func settledFailure(op func() error) error {
	var last error
	for run, deadline := 0, time.Now().Add(budget); run < 3 && time.Now().Before(deadline); {
		last = op()
		run++
		if last == nil || mq.IsRetryable(last) {
			run = 0
			time.Sleep(time.Millisecond)
		}
	}
	return last
}

// refuseOneSend makes the broker refuse the next publication to "orders" with
// an ERROR frame naming its receipt, which both brokers follow by closing the
// connection, and publishes it on snk.
func refuseOneSend(t *testing.T, srv *stompsrv.Server, snk mq.Sink) {
	t.Helper()
	pub, _ := destinations(t, settings(activemq.Artemis), "orders", "workers")
	srv.FailSend(pub, "AMQ229032 User writer does not have permission='SEND' on address orders")
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("refused")}, nil); err == nil {
		t.Fatalf("a publication the broker refused reported success\nframes:%s", frameLog(srv))
	}
}

// dropConnection closes the server's newest connection, or fails the test.
func dropConnection(t *testing.T, srv *stompsrv.Server) {
	t.Helper()
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
}

// lentBackend opens a backend over a connection the test dialled itself, as a
// caller of [stomp.BackendWithClient] does, and registers its close.
func lentBackend(t *testing.T, srv *stompsrv.Server, own driverkit.Ownership, cfg stomp.Config) *stomp.Backend {
	t.Helper()
	conn, err := gostomp.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server directly: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	b, err := stomp.BackendWithClient(ctx, conn, own, cfg)
	if err != nil {
		_ = conn.MustDisconnect()
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() {
		_ = b.Close()
		if own == driverkit.ClientBorrowed {
			_ = conn.MustDisconnect()
		}
	})
	return b
}
