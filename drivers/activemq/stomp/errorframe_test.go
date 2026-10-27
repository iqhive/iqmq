package stomp_test

import (
	"context"
	"errors"
	"strings"
	"sync"
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

// The ERROR frame a broker refuses one frame with, and which publication it
// belongs to.
//
// go-stomp hands an ERROR frame to EVERY receipt it is waiting on, so before
// the driver read the frame's `receipt-id` a refusal of one frame was reported
// as the refusal of whichever receipted SEND was waiting behind it. Measured
// against both pinned images with the driver at c7a039a: a ConfirmNone
// publication to a destination the login may not write, and a ConfirmAccepted
// one to a destination it may, written right after it --
//
//	Classic: publish topic=allowed: Unknown: User reader is not authorized to
//	         write to: queue://denied            ambiguous=false retryable=false
//	Artemis: publish topic=allowed: Forbidden: AMQ229032 User reader does not
//	         have permission='CREATE_ADDRESS' on address denied   ambiguous=false
//
// -- a definite failure naming the OTHER publication's destination, while
// Classic had stored the message (a consumer drained it) and Artemis had not
// read it at all (it answered it "AMQ339010 Connection is not valid", with its
// receipt-id, after the first ERROR). A caller told "failed" republishes the
// first and duplicates it; a caller told "forbidden" drops the second and
// loses it. The same broker named the refused SEND's own receipt whenever
// that SEND asked for one, which is what the driver now reads.

// errorFrameConfig is a configuration for one broker pointed at a server,
// whose receipt timeout is the budget rather than [shortReceipt]: the ERROR
// frame has to reach a publication that is still waiting, and one whose wait
// had already timed out would be another test's subject.
func errorFrameConfig(srv *stompsrv.Server, broker activemq.Broker) stomp.Config {
	cfg := settings(broker)
	cfg.Addrs = []string{srv.Addr()}
	cfg.ReceiptTimeout = budget
	return cfg
}

// publishInFlight starts one publication whose RECEIPT the server holds, waits
// until its SEND frame -- the server's `nth` -- has arrived, and returns a
// function that waits for the publication's answer.
//
// The wait for the frame is what makes the ERROR frame a test writes next
// reach a publication that is waiting for its receipt: go-stomp registers the
// wait before it writes the frame, so a frame the server has is a wait the
// library holds.
func publishInFlight(t *testing.T, srv *stompsrv.Server, snk mq.Sink, dest string, nth int) func() error {
	t.Helper()
	srv.HoldReceiptsOn(dest, 1)
	answer := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		answer <- snk.Publish(ctx, &mq.Message{Body: []byte("in flight")}, nil)
	}()
	t.Cleanup(wg.Wait)
	if err := srv.WaitFrames(frame.SEND, nth, budget); err != nil {
		t.Fatalf("waiting for the publication's SEND frame: %v\nframes:%s", err, frameLog(srv))
	}
	return func() error {
		select {
		case err := <-answer:
			return err
		case <-time.After(2 * budget):
			t.Fatalf("the publication never returned after an ERROR frame closed its connection\nframes:%s", frameLog(srv))
			return nil
		}
	}
}

// publishOne publishes one message and fails the test on any error.
func publishOne(t *testing.T, snk mq.Sink, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := snk.Publish(ctx, &mq.Message{Body: []byte(what)}, nil); err != nil {
		t.Fatalf("publishing %s: %v", what, err)
	}
}

// requireStray asserts err is the publication's connection dying -- ambiguous,
// Unavailable -- and that it still carries the broker's sentence.
func requireStray(t *testing.T, srv *stompsrv.Server, err error, sentence string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the publication reported success; nothing answered it but an ERROR frame\nframes:%s", frameLog(srv))
	}
	if !mq.IsAmbiguous(err) || mq.CodeOf(err) != mq.CodeUnavailable {
		t.Errorf("the publication reports %v (code %v, ambiguous=%v), want an ambiguous Unavailable. The ERROR "+
			"frame names no receipt of this publication, so it refuses another frame: this one may be stored "+
			"(Classic processes it after the refusal) or never read (Artemis), and a definite answer is wrong "+
			"one way or the other", err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
	var ef *stomp.ErrorFrameError
	if !errors.As(err, &ef) || ef.Message != sentence {
		t.Errorf("the failure %v does not carry the broker's sentence %q as an *ErrorFrameError. It is the whole "+
			"of what STOMP says about the frame the broker did refuse", err, sentence)
	}
}

// TestARefusalNamingThePublicationIsDefinite is the half the fix must keep:
// both brokers answer a refused receipted SEND with an ERROR frame whose
// `receipt-id` is that SEND's `receipt`, and that is a definite refusal with
// the broker's own sentence.
func TestARefusalNamingThePublicationIsDefinite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		broker    activemq.Broker
		refusal   string
		forbidden bool
	}{
		{activemq.Artemis, "AMQ229032 User reader does not have permission='CREATE_ADDRESS' on address orders", true},
		{activemq.Classic, "User reader is not authorized to write to: topic://VirtualTopic.orders", false},
	} {
		t.Run(tc.broker.String(), func(t *testing.T) {
			t.Parallel()
			srv := brokerServer(t, tc.broker)
			cfg := errorFrameConfig(srv, tc.broker)
			b := openBackend(t, cfg)
			orders := openSink(t, b, "orders", mq.ConfirmAccepted)
			pub, _ := destinations(t, cfg, "orders", "workers")
			srv.FailSend(pub, tc.refusal)

			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			err := orders.Publish(ctx, &mq.Message{Body: []byte("refused")}, nil)
			if err == nil {
				t.Fatalf("a publication the broker refused reported success\nframes:%s", frameLog(srv))
			}
			if mq.IsAmbiguous(err) || mq.CodeOf(err) == mq.CodeUnavailable {
				t.Errorf("the refusal reports %v (code %v, ambiguous=%v), want a definite refusal: the ERROR frame "+
					"names this publication's own receipt", err, mq.CodeOf(err), mq.IsAmbiguous(err))
			}
			if tc.forbidden && mq.CodeOf(err) != mq.CodeForbidden {
				t.Errorf("the refusal's code is %v, want %v", mq.CodeOf(err), mq.CodeForbidden)
			}
			if !strings.Contains(err.Error(), tc.refusal) {
				t.Errorf("the refusal %v does not carry the broker's sentence %q", err, tc.refusal)
			}
		})
	}
}

// TestAnErrorFrameForAnotherFrameLeavesThePublicationAmbiguous is the
// measurement above as a test: a ConfirmNone publication the broker refuses,
// and a receipted one waiting for its RECEIPT when the ERROR frame -- which
// names no receipt, as both brokers' refusal of a receipt-less SEND does --
// arrives.
func TestAnErrorFrameForAnotherFrameLeavesThePublicationAmbiguous(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		broker  activemq.Broker
		refusal string
	}{
		{activemq.Artemis, "AMQ229032 User reader does not have permission='CREATE_ADDRESS' on address denied"},
		{activemq.Classic, "User reader is not authorized to write to: queue://denied"},
	} {
		t.Run(tc.broker.String(), func(t *testing.T) {
			t.Parallel()
			srv := brokerServer(t, tc.broker)
			cfg := errorFrameConfig(srv, tc.broker)
			b := openBackend(t, cfg)
			denied := openSink(t, b, "denied", mq.ConfirmNone)
			orders := openSink(t, b, "orders", mq.ConfirmAccepted)
			pub, _ := destinations(t, cfg, "orders", "workers")

			publishOne(t, denied, "the receipt-less publication the broker refuses")
			answer := publishInFlight(t, srv, orders, pub, 2)
			if err := srv.ErrorFrame(tc.refusal); err != nil {
				t.Fatalf("ErrorFrame: %v", err)
			}
			requireStray(t, srv, answer(), tc.refusal)
		})
	}
}

// TestACodecRefusalIsThePublicationsOnlyWhenNothingElseIsUnanswered is the one
// refusal of a RECEIPTED frame that names no receipt: Classic's codec
// refusing a frame it could not read, measured as "The maximum frame size was
// exceeded" for a receipted SEND over the connector's `wireFormat.maxFrameSize`.
// It is the publication's when every other frame on the connection was
// answered, and ambiguous otherwise -- and a connection with nothing else
// unanswered is no proof for any other receipt-less ERROR, because both
// brokers send one about the connection itself.
func TestACodecRefusalIsThePublicationsOnlyWhenNothingElseIsUnanswered(t *testing.T) {
	t.Parallel()
	const codec = "The maximum frame size was exceeded"
	for _, tc := range []struct {
		name     string
		refusal  string
		before   mq.ConfirmationMode
		definite bool
	}{
		// The publication before it was answered, so the count it left
		// behind is zero again and the codec was reading this frame.
		{"nothing-else-unanswered", codec, mq.ConfirmAccepted, true},
		// A receipt-less SEND is never answered, so the codec may have been
		// reading that one -- and this frame was then never read at all.
		{"after-a-receipt-less-send", codec, mq.ConfirmNone, false},
		// Not a codec refusal: a clean connection says nothing about whose
		// it is.
		{"not-a-codec-refusal", "User reader is not authorized to write to: queue://denied", mq.ConfirmAccepted, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := classicServer(t)
			cfg := errorFrameConfig(srv, activemq.Classic)
			b := openBackend(t, cfg)
			first := openSink(t, b, "first", tc.before)
			orders := openSink(t, b, "orders", mq.ConfirmAccepted)
			pub, _ := destinations(t, cfg, "orders", "workers")

			publishOne(t, first, "the publication before")
			answer := publishInFlight(t, srv, orders, pub, 2)
			if err := srv.ErrorFrame(tc.refusal); err != nil {
				t.Fatalf("ErrorFrame: %v", err)
			}
			err := answer()
			if !tc.definite {
				requireStray(t, srv, err, tc.refusal)
				return
			}
			if err == nil {
				t.Fatalf("a publication the codec refused reported success\nframes:%s", frameLog(srv))
			}
			if mq.IsAmbiguous(err) {
				t.Errorf("the codec refusal reports %v (ambiguous), want a definite refusal: every other frame on "+
					"the connection was answered, so the frame the codec could not read is this one", err)
			}
			if !strings.Contains(err.Error(), codec) {
				t.Errorf("the refusal %v does not carry the broker's sentence %q", err, codec)
			}
		})
	}
}

// TestACodecRefusalOnTheCallersConnectionIsAmbiguous is the codec refusal of
// the subtest "nothing-else-unanswered" above, on a connection the caller gave
// [stomp.BackendWithClient] instead of one the backend dialled. The caller may
// write frames of its own on that connection, which nothing in the backend can
// count, so the count that made the refusal this publication's proves nothing
// here: the frame the codec could not read may be one of the caller's, and
// this publication's may never have been read at all.
func TestACodecRefusalOnTheCallersConnectionIsAmbiguous(t *testing.T) {
	t.Parallel()
	const codec = "The maximum frame size was exceeded"
	srv := classicServer(t)
	conn, err := gostomp.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server directly: %v", err)
	}
	cfg := errorFrameConfig(srv, activemq.Classic)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	b, err := stomp.BackendWithClient(ctx, conn, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	first := openSink(t, b, "first", mq.ConfirmAccepted)
	orders := openSink(t, b, "orders", mq.ConfirmAccepted)
	pub, _ := destinations(t, cfg, "orders", "workers")

	publishOne(t, first, "the publication before")
	answer := publishInFlight(t, srv, orders, pub, 2)
	if err := srv.ErrorFrame(codec); err != nil {
		t.Fatalf("ErrorFrame: %v", err)
	}
	requireStray(t, srv, answer(), codec)
}
