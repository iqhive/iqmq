package stomp_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/activemq/protocol"
)

// TestAnEnvelopingModeDeliversEveryBodyItAccepted is the
// sink's envelope decision has to agree with the source's, for every body.
//
// The source decodes through `driverkit.Envelope.Unwrap` in the configured
// mode, and the kit's `Envelope.Wrap` is the publish side that agrees with it:
// under MetadataAdaptive it envelopes a body the read side would claim -- one
// that begins with the envelope's magic  -- and under
// MetadataPortableEnvelope it envelopes everything. The sink decided BEFORE
// calling Wrap, from `Needed` and its own key and nil-body question, and
// published natively whatever neither case caught. So an adaptive body
// beginning `IQMQ\x01` went out natively and its own source refused it, and
// under the portable mode EVERY message with no metadata did: each was counted
// undecodable and left unacknowledged, a record that can be neither delivered
// nor settled. The first case is FuzzAMQPMapping's seed
// accf8580a55f6015 on the other ActiveMQ protocol.
func TestAnEnvelopingModeDeliversEveryBodyItAccepted(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode mq.MetadataMode
		body []byte
	}{
		{"adaptive/magic-prefixed", mq.MetadataAdaptive, []byte("IQMQ\x01\x02\x00\x0200")},
		{"adaptive/plain", mq.MetadataAdaptive, []byte("plain")},
		{"portable/plain", mq.MetadataPortableEnvelope, []byte("plain")},
		{"portable/magic-prefixed", mq.MetadataPortableEnvelope, []byte("IQMQ\x01\x02\x00\x0200")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := artemisServer(t)
			cfg := artemisConfig(srv)
			cfg.Metadata = tc.mode
			b := openBackend(t, cfg)
			src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
			snk := openSink(t, b, "orders", mq.ConfirmAccepted)
			if err := snk.Publish(t.Context(), &mq.Message{Body: tc.body}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			publish, _ := destinations(t, cfg, "orders", "workers")
			if _, err := srv.Forward(publish, consume); err != nil {
				t.Fatal(err)
			}
			// Wait for the driver to have DECIDED about the message the
			// server just forwarded: it hands it over, or it refuses to
			// decode it and says so in `Undecodable`. Both are observable,
			// which is why this needs no clock of its own. The two-second
			// bound it replaced would report "was not delivered" on a host
			// too busy to have run the pump yet, and blame the mapping for
			// the runner -- the same two-second shape in
			// `TestCriticalRefusedSubscribeReportsNotFound` went red exactly
			// that way. The per-call deadline below is a
			// poll interval: no assertion can fail because it expires.
			dst := make([]mq.DriverDelivery, 1)
			var n int
			var err error
			for {
				fctx, fcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				n, err = src.Fetch(fctx, dst)
				fcancel()
				// Any answer that is not the poll's own deadline ends the
				// wait, including an error: a loop that only watched for
				// the two GOOD outcomes would spin for ever on a driver
				// that reports something else, which is how the same shape
				// hung for twenty-one minutes in critical_test.go.
				if n > 0 || src.Undecodable() > 0 || mq.CodeOf(err) != mq.CodeDeadlineExceeded {
					break
				}
			}
			if n != 1 {
				t.Fatalf("a body the %v sink ACCEPTED was not delivered (n=%d err=%v undecodable=%d): its own source "+
					"refuses what the sink published, so the record can be neither delivered nor settled",
					tc.mode, n, err, src.Undecodable())
			}
			if got := dst[0].Message.Body; !bytes.Equal(got, tc.body) {
				t.Fatalf("the body came back as %q, want %q", got, tc.body)
			}
		})
	}
}

// TestAnEnvelopedPublicationKeepsItsMessageGroup reads the SEND frame of a
// keyed publication in every mode that envelopes, and then the message back.
//
// FeatureOrderedByKey is claimed on the `JMSXGroupID` header, which is how
// both brokers hand one key to one consumer at a time, and the shared mapping
// writes it from the message's key. The sink mapped the WRAPPED message, which
// `driverkit.Envelope.Wrap` returns with no key, so under
// MetadataPortableEnvelope every keyed SEND went out in no group, and under
// MetadataAdaptive so did every one it enveloped: a keyed message with a nil
// body, for one. The key itself still came back, from the envelope, so nothing
// but the ordering said anything was wrong. A key that is not valid UTF-8 has
// no frame-header form, natively or not, and stays out of the group.
func TestAnEnvelopedPublicationKeepsItsMessageGroup(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mode  mq.MetadataMode
		msg   *mq.Message
		group string // "" for no JMSXGroupID header at all
	}{
		{"native-strict", mq.MetadataNativeStrict, &mq.Message{Key: []byte("customer-7"), Body: []byte("b")}, "customer-7"},
		{"portable", mq.MetadataPortableEnvelope, &mq.Message{Key: []byte("customer-7"), Body: []byte("b")}, "customer-7"},
		{"portable/binary-key", mq.MetadataPortableEnvelope, &mq.Message{Key: []byte("\x00\xff"), Body: []byte("b")}, ""},
		{"adaptive/nil-body", mq.MetadataAdaptive, &mq.Message{Key: []byte("customer-7")}, "customer-7"},
		{"adaptive/binary-key", mq.MetadataAdaptive, &mq.Message{Key: []byte("\x00\xff"), Body: []byte("b")}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := artemisServer(t)
			cfg := artemisConfig(srv)
			cfg.Metadata = tc.mode
			b := openBackend(t, cfg)
			src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
			snk := openSink(t, b, "orders", mq.ConfirmAccepted)
			if err := snk.Publish(t.Context(), tc.msg, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			sends := srv.FramesOf(frame.SEND)
			if len(sends) != 1 {
				t.Fatalf("the server received %d SEND frames, want 1.\nframes:%s", len(sends), frameLog(srv))
			}
			got, ok := sends[0].Get(protocol.PropertyGroupID), sends[0].Has(protocol.PropertyGroupID)
			switch {
			case tc.group == "" && ok:
				t.Errorf("the SEND frame carries %s %q for a key with no frame-header form", protocol.PropertyGroupID, got)
			case tc.group != "" && got != tc.group:
				t.Errorf("the SEND frame's %s is %q (present: %v), want %q: the broker orders a key by that header "+
					"alone and cannot read the envelope", protocol.PropertyGroupID, got, ok, tc.group)
			}

			// The header is for the broker only. Classic echoes it on
			// delivery, which relay reproduces, and the message that comes
			// back is the one published, its key read from the envelope.
			publish, _ := destinations(t, cfg, "orders", "workers")
			relay(t, srv, publish, consume)
			back := fetchN(t, src, 1)[0].Message
			if !bytes.Equal(back.Key, tc.msg.Key) {
				t.Errorf("the key came back as %q, want %q", back.Key, tc.msg.Key)
			}
			if !bytes.Equal(back.Body, tc.msg.Body) || (back.Body == nil) != (tc.msg.Body == nil) {
				t.Errorf("the body came back as %q (nil: %v), want %q (nil: %v)",
					back.Body, back.Body == nil, tc.msg.Body, tc.msg.Body == nil)
			}
			if v, ok := back.Headers.Get(protocol.PropertyGroupID); ok {
				t.Errorf("the delivery hands the broker's %s %q back as an application header", protocol.PropertyGroupID, v)
			}
		})
	}
}
