package stomp_test

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// brokersAt is the brokers the environment names, each with a prefix of the
// run's own: IQMQ_ACTIVEMQ_CLASSIC_ADDR, and IQMQ_ARTEMIS_ADDR when
// IQMQ_ARTEMIS_AUTOCREATE says its auto-creation is on, with the credentials
// TestConformance reads.
func brokersAt(t *testing.T) map[string]stomp.Config {
	t.Helper()
	prefix := "iqmq.headers." + strconv.FormatInt(time.Now().UnixNano(), 36) + "."
	out := map[string]stomp.Config{}
	if addr := os.Getenv("IQMQ_ACTIVEMQ_CLASSIC_ADDR"); addr != "" {
		out["classic"] = withPasscode(stomp.Config{
			Broker: activemq.Classic, Addrs: []string{addr}, Login: os.Getenv("IQMQ_ACTIVEMQ_CLASSIC_USER"),
			ReceiptTimeout: 5 * time.Second, Prefix: prefix,
		}, "classic")
	}
	if addr := os.Getenv("IQMQ_ARTEMIS_ADDR"); addr != "" && os.Getenv("IQMQ_ARTEMIS_AUTOCREATE") != "" {
		out["artemis"] = withPasscode(stomp.Config{
			Broker: activemq.Artemis, Addrs: []string{addr}, Login: envOr("IQMQ_ARTEMIS_USER", "artemis"),
			ReceiptTimeout: 5 * time.Second, Prefix: prefix,
		}, "artemis")
	}
	if len(out) == 0 {
		t.Skip("set IQMQ_ACTIVEMQ_CLASSIC_ADDR, or IQMQ_ARTEMIS_ADDR and IQMQ_ARTEMIS_AUTOCREATE=1 (and the " +
			"credentials TestConformance reads), to run this against a broker; what it reads is the question")
	}
	return out
}

// withPasscode is cfg with the passcode the environment names for broker, if
// it names one.
func withPasscode(cfg stomp.Config, broker string) stomp.Config {
	if pw := os.Getenv(passwordVar(broker)); pw != "" {
		cfg.Passcode = dsn.Secret(pw)
		// A passcode over a plaintext endpoint is refused unless the
		// deployment says so, and a test against a development broker is
		// saying so.
		cfg.InsecurePlainLogin = true
	}
	return cfg
}

// TestHeadersReadBackOnABroker publishes, through the driver, headers the
// driver used to refuse and a header at Classic's line bound, and reads each
// back from the broker.
//
// Up to v1.1.2 the driver refused any one header whose key and value passed
// 255 bytes, so the trace context and the 70-byte headers here were refused
// with MessageTooLarge. Classic reads a frame header line of 10241 octets and
// answers a longer one with an ERROR frame and the end of the connection, so a
// line one octet over is refused before any I/O and the connection is still
// the same one afterwards; and up to v1.1.2 a message key that long, which
// travels as `JMSXGroupID`, reached the broker and closed the connection.
// Artemis has no such bound and carries the longer line.
func TestHeadersReadBackOnABroker(t *testing.T) {
	for name, cfg := range brokersAt(t) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := stomp.Provision(ctx, cfg, testkit.Need{Topic: "orders", Subscriptions: []string{"workers"}}); err != nil {
				t.Fatalf("Provision: %v", err)
			}
			b := collisionBackend(ctx, t, cfg)
			snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmAccepted})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			t.Cleanup(func() { _ = snk.Close() })
			src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 8})
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			t.Cleanup(func() { _ = src.Close() })

			bound := 10239 // "k", the colon and the value: 10241 octets
			if cfg.Broker == activemq.Artemis {
				bound = 10240
			}
			sent := []*mq.Message{
				withHeader(withHeader(&mq.Message{Body: []byte("traced")},
					"traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
					"baggage", strings.Repeat("v", 300)),
				withHeader(withHeader(withHeader(withHeader(&mq.Message{Body: []byte("several")},
					"header-0", strings.Repeat("v", 62)), "header-1", strings.Repeat("v", 62)),
					"header-2", strings.Repeat("v", 62)), "header-3", strings.Repeat("v", 62)),
				withHeader(&mq.Message{Body: []byte("at the bound")}, "k", strings.Repeat("v", bound)),
			}
			var published []*mq.Message
			for _, m := range sent {
				if err := snk.Publish(ctx, m, nil); err != nil {
					t.Errorf("publishing %q with %d bytes of headers: %v", m.Body, headerBytes(m), err)
					continue
				}
				published = append(published, m)
			}
			if cfg.Broker == activemq.Classic {
				for what, m := range map[string]*mq.Message{
					"a header line of 10242 octets": withHeader(&mq.Message{Body: []byte("over")}, "k",
						strings.Repeat("v", 10240)),
					"a key whose JMSXGroupID line is 10242 octets": {
						Body: []byte("over"), Key: bytes.Repeat([]byte("k"), 10230),
					},
				} {
					err := snk.Publish(ctx, m, nil)
					if mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsAmbiguous(err) {
						t.Errorf("publishing %s = %v (code %v, ambiguous %v), want MessageTooLarge before any I/O",
							what, err, mq.CodeOf(err), mq.IsAmbiguous(err))
					}
				}
				if n := b.Reconnects(); n != 0 {
					t.Errorf("the backend reconnected %d times: a refused line must not cost the connection", n)
				}
				last := &mq.Message{Body: []byte("after")}
				if err := snk.Publish(ctx, last, nil); err != nil {
					t.Fatalf("publishing after the refusals: %v", err)
				}
				published = append(published, last)
			}
			got := fetchAll(ctx, t, src, len(published))
			for i, m := range published {
				if i >= len(got) {
					break
				}
				if !bytes.Equal(got[i].Message.Body, m.Body) {
					t.Errorf("delivery %d is %q, want %q", i, got[i].Message.Body, m.Body)
					continue
				}
				for k, v := range m.Headers.All() {
					if back, _ := got[i].Message.Headers.Get(k); !bytes.Equal(back, v) {
						t.Errorf("%q: header %q came back as %d bytes, want the %d published", m.Body, k, len(back),
							len(v))
					}
				}
			}
			for i := range got {
				if err := src.Settler().Ack(ctx, &got[i]); err != nil {
					t.Errorf("Ack: %v", err)
				}
			}
		})
	}
}

// TestHeaderCountsOnABroker publishes, through the driver, header sets at and
// past Classic's 1001 header names, and reads back the ones it sends.
//
// Classic refuses a frame with more names, "The maximum number of headers was
// exceeded", and closes the connection; up to round 1 of the hunt 9 fixes 998
// headers reached it and did that. Now a frame over the bound is refused before
// any I/O and the connection is the same one afterwards, and
// MetadataLenientDrop drops the headers past it. The frame's own lines here are
// `content-length`, `destination`, `persistent` and the receipt, and a content
// type is one name written twice. Artemis has no such bound and carries 1024
// headers, the most the envelope's admission bound lets through.
func TestHeaderCountsOnABroker(t *testing.T) {
	for name, cfg := range brokersAt(t) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := stomp.Provision(ctx, cfg, testkit.Need{Topic: "counts", Subscriptions: []string{"workers"}}); err != nil {
				t.Fatalf("Provision: %v", err)
			}
			b := collisionBackend(ctx, t, cfg)
			snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "counts", Confirmation: mq.ConfirmAccepted})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			t.Cleanup(func() { _ = snk.Close() })
			src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "counts", Subscription: "workers", Prefetch: 8})
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			t.Cleanup(func() { _ = src.Close() })
			counted := func(body string, n int, contentType string) *mq.Message {
				m := withHeaderCount(&mq.Message{Body: []byte(body)}, n)
				m.Properties.ContentType = contentType
				return m
			}
			// kept is how many of a published message's headers, h0 onwards,
			// the delivery must carry.
			type sent struct {
				m    *mq.Message
				kept int
			}
			var published []sent
			publish := func(s mq.Sink, m *mq.Message, kept int) {
				if err := s.Publish(ctx, m, nil); err != nil {
					t.Errorf("publishing %q with %d headers: %v", m.Body, m.Headers.Len(), err)
					return
				}
				published = append(published, sent{m, kept})
			}
			if cfg.Broker == activemq.Artemis {
				publish(snk, counted("1024 typed", 1024, "text/plain"), 1024)
			} else {
				publish(snk, counted("997", 997, ""), 997)
				publish(snk, counted("996 typed", 996, "text/plain"), 996)
				for _, m := range []*mq.Message{counted("998", 998, ""), counted("997 typed", 997, "text/plain")} {
					err := snk.Publish(ctx, m, nil)
					if mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsAmbiguous(err) {
						t.Errorf("publishing %q = %v (code %v, ambiguous %v), want MessageTooLarge before any I/O",
							m.Body, err, mq.CodeOf(err), mq.IsAmbiguous(err))
					}
				}
				if n := b.Reconnects(); n != 0 {
					t.Errorf("the backend reconnected %d times: a refused frame must not cost the connection", n)
				}
				lenient := cfg
				lenient.Metadata = mq.MetadataLenientDrop
				lsnk, err := collisionBackend(ctx, t, lenient).OpenSink(ctx,
					mq.SinkConfig{Topic: "counts", Confirmation: mq.ConfirmAccepted})
				if err != nil {
					t.Fatalf("OpenSink (lenient): %v", err)
				}
				t.Cleanup(func() { _ = lsnk.Close() })
				publish(lsnk, counted("1000 lenient", 1000, ""), 997)
				publish(snk, &mq.Message{Body: []byte("after")}, 0)
			}
			got := fetchAll(ctx, t, src, len(published))
			for i, s := range published {
				d := &got[i]
				if !bytes.Equal(d.Message.Body, s.m.Body) {
					t.Errorf("delivery %d is %q, want %q", i, d.Message.Body, s.m.Body)
					continue
				}
				for j := range s.m.Headers.Len() {
					_, there := d.Message.Headers.Get("h" + strconv.Itoa(j))
					if there != (j < s.kept) {
						t.Errorf("%q: header h%d delivered %v, want %v (the first %d of %d)", s.m.Body, j, there,
							j < s.kept, s.kept, s.m.Headers.Len())
						break
					}
				}
			}
			for i := range got {
				if err := src.Settler().Ack(ctx, &got[i]); err != nil {
					t.Errorf("Ack: %v", err)
				}
			}
		})
	}
}

// TestAdaptiveEnvelopesOnABroker publishes, under MetadataAdaptive, the two
// messages a native Classic frame cannot carry, a header line of 10242 octets
// and 998 headers, and reads both back with every header.
//
// Classic answers either natively with an ERROR frame and the end of the
// connection, so NativeStrict refuses them before any I/O; the envelope carries
// them in the body, which is what Adaptive is for. Before round 2 of the hunt 9
// fixes Adaptive refused both. Classic only: Artemis carries both natively.
func TestAdaptiveEnvelopesOnABroker(t *testing.T) {
	cfg, ok := brokersAt(t)["classic"]
	if !ok {
		t.Skip("set IQMQ_ACTIVEMQ_CLASSIC_ADDR to run this against ActiveMQ Classic, whose frame bounds are the question")
	}
	cfg.Metadata = mq.MetadataAdaptive
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := stomp.Provision(ctx, cfg, testkit.Need{Topic: "adaptive", Subscriptions: []string{"workers"}}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	b := collisionBackend(ctx, t, cfg)
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "adaptive", Confirmation: mq.ConfirmAccepted})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "adaptive", Subscription: "workers", Prefetch: 8})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sent := []*mq.Message{
		withHeader(withHeader(&mq.Message{Body: []byte("long")}, "tenant", "acme"), "k", strings.Repeat("v", 10240)),
		withHeaderCount(&mq.Message{Body: []byte("many")}, 998),
	}
	for _, m := range sent {
		if err := snk.Publish(ctx, m, nil); err != nil {
			t.Fatalf("publishing %q under MetadataAdaptive: %v", m.Body, err)
		}
	}
	if n := b.Reconnects(); n != 0 {
		t.Errorf("the backend reconnected %d times", n)
	}
	got := fetchAll(ctx, t, src, len(sent))
	for i, m := range sent {
		if !bytes.Equal(got[i].Message.Body, m.Body) {
			t.Errorf("delivery %d is %q, want %q", i, got[i].Message.Body, m.Body)
			continue
		}
		for k, v := range m.Headers.All() {
			if back, _ := got[i].Message.Headers.Get(k); !bytes.Equal(back, v) {
				t.Errorf("%q: header %q came back as %d bytes, want the %d published", m.Body, k, len(back), len(v))
				break
			}
		}
		if err := src.Settler().Ack(ctx, &got[i]); err != nil {
			t.Errorf("Ack: %v", err)
		}
	}
}

// fetchAll fetches until it has n deliveries or ctx ends.
func fetchAll(ctx context.Context, t *testing.T, src mq.Source, n int) []mq.DriverDelivery {
	t.Helper()
	var got []mq.DriverDelivery
	for len(got) < n {
		buf := make([]mq.DriverDelivery, n-len(got))
		k, err := src.Fetch(ctx, buf)
		got = append(got, buf[:k]...)
		if err != nil {
			t.Fatalf("Fetch after %d of %d deliveries: %v", len(got), n, err)
		}
	}
	return got
}
