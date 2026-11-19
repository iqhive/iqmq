package stomp_test

import (
	"bytes"
	"context"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/activemq/protocol"
)

// fixedClock is a clock that does not move.
//
// It is injected rather than tolerated because STOMP's `expires` header is an
// ABSOLUTE epoch-millisecond instant and TTL is a relative duration: the
// driver resolves one against the other, so a test that read the real clock
// could only assert that the answer was "about right" — and "about right" is
// the assertion that would still pass if the arithmetic were an hour out.
type fixedClock struct{ at time.Time }

// Now is the fixed instant.
func (c fixedClock) Now() time.Time { return c.at }

// NewTimer returns a timer that never fires, which is deliberate: nothing on
// the mapping path may reach for one, and a caller that did should hang
// visibly rather than pass quietly.
func (c fixedClock) NewTimer(time.Duration) mq.Timer { return deadTimer{} }

// deadTimer is a timer that never fires.
type deadTimer struct{}

// C is a channel nothing is ever sent on.
func (deadTimer) C() <-chan time.Time { return nil }

// Stop reports that nothing was stopped.
func (deadTimer) Stop() bool { return false }

// Reset reports that nothing was re-armed.
func (deadTimer) Reset(time.Duration) bool { return false }

// epoch is the instant every expiry in this file is measured from. It is an
// exact millisecond so that the assertion is an equality rather than a range.
var epoch = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

// mappingConfig is a configuration whose table is complete, for the two
// brokers.
func mappingConfig(b activemq.Broker) stomp.Config {
	return stomp.Config{Broker: b, Addrs: []string{"localhost:61613"}}
}

// sendHeaders is the SEND frame's headers for one message, or a fatal failure.
func sendHeaders(t *testing.T, cfg stomp.Config, m *mq.Message, o *mq.PublishOptions) map[string]string {
	t.Helper()
	got, err := stomp.SendHeadersForTest(cfg, m, o, fixedClock{at: epoch})
	if err != nil {
		t.Fatalf("SendHeadersForTest: %v", err)
	}
	return got
}

// TestSendHeaders is the whole of what a STOMP publication says about a
// message, because on this protocol the headers ARE the mapping: there is one
// flat set of text headers and no sections, no typed values and no key.
func TestSendHeaders(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		broker activemq.Broker
		msg    *mq.Message
		opts   *mq.PublishOptions
		want   map[string]string // headers that must be present with this value
		absent []string          // headers that must not be there at all
	}{
		"a bare message is persistent and nothing else": {
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			// `persistent: true` is not decoration: without it neither
			// broker writes the message to its store, so a publication that
			// a RECEIPT confirmed would still be lost to a restart -- and ConfirmDurable would be a claim about nothing.
			want: map[string]string{"persistent": "true"},
			// A publication with no delay must write no delay key at all.
			// An AMQ_SCHEDULED_DELAY of 0 is ACCEPTED by both brokers and
			// turns an ordinary publication into a scheduled one.
			absent: []string{
				protocol.PropertyScheduledDelay, "expires", "priority", "timestamp",
				protocol.PropertyGroupID, protocol.KeyHeader,
			},
		},
		"a delivery time that has already passed publishes at once": {
			// DeliverAt in the past is due NOW, and the case a
			// scheduler produces most often: a retry loop that recomputes
			// DeliverAt from a stored instant. `Sink.frameFor` therefore
			// clears the instant as well as the duration, because the
			// shared mapping reads "DeliverAt set, Delay zero" as "the
			// driver did not resolve it" and refuses the publication with
			// ErrNeedsResolvedDelay -- a sentence naming an internal
			// contract the caller cannot act on. What must NOT appear is a
			// delay key of any kind: `AMQ_SCHEDULED_DELAY: 0` is accepted
			// by both brokers and turns an ordinary publication into a
			// scheduled one.
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{DeliverAt: epoch.Add(-time.Hour)},
			want:   map[string]string{"persistent": "true"},
			absent: []string{protocol.PropertyScheduledDelay},
		},
		"a key becomes both the reserved entry and the JMS group": {
			// Neither protocol has a slot for a message key, so the key
			// always costs an entry -- and it is written TWICE on purpose.
			// `x-iqmq-key` is what a consumer of this driver reads back as
			// Message.Key; `JMSXGroupID` is what makes the ORDER work,
			// because the broker hands one group to one consumer at a time.
			// A driver that wrote only the first would carry the key and
			// lose the ordering, with nothing reporting it.
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b"), Key: []byte("customer-7")},
			want: map[string]string{
				protocol.KeyHeader:       "customer-7",
				protocol.PropertyGroupID: "customer-7",
				"persistent":             "true",
			},
		},
		"a TTL becomes an absolute expires against the injected clock": {
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{TTL: 90 * time.Second},
			want: map[string]string{
				"expires": itoa64(epoch.Add(90 * time.Second).UnixMilli()),
			},
		},
		"a delay becomes whole milliseconds": {
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{Delay: 2500 * time.Millisecond},
			want:   map[string]string{protocol.PropertyScheduledDelay: "2500"},
		},
		"a sub-millisecond delay rounds UP and never down": {
			// 1500 µs must not become 1 ms. Rounding down releases the
			// publication half a millisecond BEFORE the caller asked, and
			// the same rounding applied to a TTL would expire a message the
			// caller still considered live. Neither is visible in
			// production.
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{Delay: 1500 * time.Microsecond},
			want:   map[string]string{protocol.PropertyScheduledDelay: "2"},
		},
		"a delay under a millisecond still delays": {
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{Delay: time.Microsecond},
			want:   map[string]string{protocol.PropertyScheduledDelay: "1"},
		},
		"an absolute delivery time becomes a delay, because STOMP has no absolute slot": {
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{DeliverAt: epoch.Add(3 * time.Second)},
			want:   map[string]string{protocol.PropertyScheduledDelay: "3000"},
			absent: []string{protocol.PropertyScheduledTime, protocol.AnnotationDeliveryTime},
		},
		"a priority of zero is a real priority": {
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{Priority: 0, PrioritySet: true},
			want:   map[string]string{"priority": "0"},
		},
		"a priority is written as a decimal": {
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{Priority: 7, PrioritySet: true},
			want:   map[string]string{"priority": "7"},
		},
		"the typed properties go in their own slots": {
			broker: activemq.Artemis,
			msg: &mq.Message{
				Body: []byte("b"),
				ID:   "publisher-id",
				Properties: mq.Properties{
					ContentType:     "application/json",
					ContentEncoding: "gzip",
					CorrelationID:   "corr-1",
					ReplyTo:         "replies",
					CausationID:     "cause-1",
					CreatedAt:       epoch,
				},
			},
			want: map[string]string{
				"content-type":   "application/json",
				"correlation-id": "corr-1",
				"reply-to":       "replies",
				"timestamp":      itoa64(epoch.UnixMilli()),
				// STOMP has no `content-encoding` header at all and its
				// `message-id` belongs to the BROKER rather than to the
				// publisher, so both travel as reserved entries of these
				// drivers' own. A publisher identifier sent as `message-id`
				// would be overwritten in transit.
				protocol.EncodingHeader:  "gzip",
				protocol.IDHeader:        "publisher-id",
				protocol.CausationHeader: "cause-1",
			},
			absent: []string{"message-id", "content-encoding"},
		},
		"an application header is carried as itself": {
			broker: activemq.Artemis,
			msg:    withHeader(&mq.Message{Body: []byte("b")}, "tenant", "acme"),
			want:   map[string]string{"tenant": "acme"},
		},
		"a de-duplication key is Artemis's own property": {
			broker: activemq.Artemis,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{DedupKey: "order-42"},
			want:   map[string]string{protocol.PropertyDuplicateID: "order-42"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := sendHeaders(t, mappingConfig(tc.broker), tc.msg, tc.opts)
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("the SEND frame's %q header is %q, want %q\nwhole frame: %v", k, got[k], v, got)
				}
			}
			for _, k := range tc.absent {
				if _, there := got[k]; there {
					t.Errorf("the SEND frame carries %q = %q, which it must not\nwhole frame: %v", k, got[k], got)
				}
			}
		})
	}
}

// TestSendHeadersRefusals is anything the protocol cannot carry
// is refused BEFORE a frame is written.
//
// It matters more on STOMP than the rule suggests. A broker that refuses a
// frame answers with an ERROR frame and then CLOSES the connection, measured
// on both — so one bad publication costs every other publisher on that
// connection its link. A header with a colon or a newline in its name is
// worse still: go-stomp encodes it without complaint, and what reaches the
// broker is a frame with a header the caller forged.
func TestSendHeadersRefusals(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		broker activemq.Broker
		msg    *mq.Message
		opts   *mq.PublishOptions
		want   error
		code   mq.ErrorCode
	}{
		"a header under the reserved prefix is refused rather than shadowed": {
			// A message published with `x-iqmq-key` set by the application
			// would arrive with a key the application never chose.
			msg:  withHeader(&mq.Message{Body: []byte("b")}, protocol.KeyHeader, "not-yours"),
			want: protocol.ErrReservedHeader,
		},
		"any header under the reserved prefix, not just a known one": {
			msg:  withHeader(&mq.Message{Body: []byte("b")}, "x-iqmq-whatever", "v"),
			want: protocol.ErrReservedHeader,
		},
		"a STOMP frame header's name is refused": {
			// `destination` would address another queue, and nothing would
			// report that it had.
			msg:  withHeader(&mq.Message{Body: []byte("b")}, "destination", "/queue/elsewhere"),
			want: protocol.ErrProtocolHeader,
		},
		"a protocol header that would become the message's own expiry": {
			msg:  withHeader(&mq.Message{Body: []byte("b")}, "expires", "1"),
			want: protocol.ErrProtocolHeader,
		},
		"a broker's own message property is refused": {
			// `JMSXGroupID` really IS the message group, so a caller's
			// header of that name would silently reorder their traffic.
			msg:  withHeader(&mq.Message{Body: []byte("b")}, protocol.PropertyGroupID, "hijack"),
			want: protocol.ErrProtocolHeader,
		},
		"Artemis's own prefix is refused by prefix and not by list": {
			msg:  withHeader(&mq.Message{Body: []byte("b")}, "_AMQ_SOMETHING_NEW", "v"),
			want: protocol.ErrProtocolHeader,
		},
		"a header value that is not valid UTF-8 is refused rather than mangled": {
			// A frame header is TEXT. Carrying the bytes anyway would hand
			// the consumer a value the publisher never wrote, and the
			// consumer would have no way to tell.
			msg:  withHeaderBytes(&mq.Message{Body: []byte("b")}, "blob", []byte{0xff, 0xfe}),
			want: protocol.ErrTextOnly,
		},
		"a header NAME that is not valid UTF-8 is refused too": {
			msg:  withHeader(&mq.Message{Body: []byte("b")}, "\xff", "v"),
			want: protocol.ErrTextOnly,
		},
		"a header with no name is refused": {
			// A STOMP frame header with an empty name begins with its own
			// ":", which is a malformed frame the broker answers with an
			// ERROR that closes the connection -- taking every other
			// publication in flight with it.
			msg:  withHeader(&mq.Message{Body: []byte("b")}, "", "v"),
			want: protocol.ErrEmptyHeaderKey,
		},
		"a key that is not valid UTF-8 cannot be a frame header": {
			msg:  &mq.Message{Body: []byte("b"), Key: []byte{0x80, 0x00}},
			want: protocol.ErrTextOnly,
		},
		"a de-duplication key on Classic, which has no de-duplication": {
			broker: activemq.Classic,
			msg:    &mq.Message{Body: []byte("b")},
			opts:   &mq.PublishOptions{DedupKey: "order-42"},
			want:   protocol.ErrNoDeduplication,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			broker := tc.broker
			if !broker.Valid() {
				// A row that names no broker is one whose refusal is the
				// MAPPING's own, so neither product reaches the wire and
				// either stands in. It has to be named explicitly because
				// `activemq.BrokerUnset` is the zero value and is refused:
				// the field is required, which is what stops a Classic
				// deployment that omitted the line from being mapped the
				// Artemis way.
				broker = activemq.Artemis
			}
			got, err := stomp.SendHeadersForTest(mappingConfig(broker), tc.msg, tc.opts, fixedClock{at: epoch})
			if err == nil {
				t.Fatalf("the mapping accepted a message it must refuse, and produced %v. A STOMP broker answers a "+
					"frame it refuses with an ERROR frame and then closes the connection, so the cost of accepting "+
					"one is every other publisher on that connection", got)
			}
			if tc.want != nil && !errorMatches(err, tc.want) {
				t.Errorf("the refusal is %v, want %v", err, tc.want)
			}
			if tc.code != 0 && mq.CodeOf(err) != tc.code {
				t.Errorf("the refusal's code is %v, want %v", mq.CodeOf(err), tc.code)
			}
		})
	}
}

// TestSendHeadersLenientModeDropsRatherThanRefuses is the fourth mode, and
// it is the other direction of the table above: a driver whose only answer was
// a refusal would make the mode meaningless.
func TestSendHeadersLenientModeDropsRatherThanRefuses(t *testing.T) {
	t.Parallel()
	cfg := mappingConfig(activemq.Artemis)
	cfg.Metadata = mq.MetadataLenientDrop
	m := withHeaderBytes(withHeader(&mq.Message{Body: []byte("b")}, "tenant", "acme"),
		"blob", []byte{0xff, 0xfe})
	got := sendHeaders(t, cfg, m, nil)
	if _, there := got["blob"]; there {
		t.Errorf("the un-carryable header survived as %q; MetadataLenientDrop drops it, and carrying the bytes "+
			"would be the mangling the refusal exists to prevent", got["blob"])
	}
	if got["tenant"] != "acme" {
		t.Errorf("the carryable header was dropped as well (%v). LenientDrop drops what cannot travel, not "+
			"everything", got)
	}
}

// TestSendHeadersCarryAHeaderSetOver255Bytes maps headers whose keys and
// values total more than 255 bytes, in one header or in several.
//
// The driver advertised MaxHeaderBytes = 255, internal/activemq's name
// ceiling, and the shared mapping held each header's key and value together to
// it, so a W3C traceparent beside a 300-byte baggage header was refused with
// MessageTooLarge. Neither broker bounds a STOMP header near there: measured,
// Artemis 2.44.0 carried a 1 MiB value and Classic 6.2.0 a header line of
// 10241 octets, and both a key of 10000 bytes.
func TestSendHeadersCarryAHeaderSetOver255Bytes(t *testing.T) {
	t.Parallel()
	traced := withHeader(withHeader(&mq.Message{Body: []byte("b")},
		"traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
		"baggage", strings.Repeat("v", 300))
	several := &mq.Message{Body: []byte("b")}
	for i := range 4 {
		several.Headers.Add("header-"+itoa64(i), []byte(strings.Repeat("v", 62)))
	}
	long := withHeader(&mq.Message{Body: []byte("b")}, "k", strings.Repeat("v", 9000))
	for _, broker := range activemq.Brokers() {
		t.Run(broker.String(), func(t *testing.T) {
			t.Parallel()
			cfg := mappingConfig(broker)
			if lim := capsOf(t, cfg).Limit(mq.LimitMaxHeaderBytes); lim.Known {
				t.Errorf("MaxHeaderBytes is advertised as %d. It is the whole header set, and neither broker "+
					"bounds that", lim.Value)
			}
			for name, m := range map[string]*mq.Message{
				"a traceparent and a 300-byte baggage header": traced,
				"four 70-byte headers":                        several,
				"a 9001-byte header":                          long,
			} {
				got, err := stomp.SendHeadersForTest(cfg, m, nil, fixedClock{at: epoch})
				if err != nil {
					t.Errorf("%s: the mapping refused a message with %d bytes of headers: %v", name,
						headerBytes(m), err)
					continue
				}
				for k, v := range m.Headers.All() {
					if got[k] != string(v) {
						t.Errorf("%s: header %q went out as %d bytes, want the %d published", name, k,
							len(got[k]), len(v))
					}
				}
			}
		})
	}
}

// TestSendHeadersRefuseALineClassicCannotRead is the one bound a STOMP frame
// header has on these brokers, held exactly.
//
// ActiveMQ Classic reads a frame header line of at most 10241 octets, the name
// and the value as escaped on the wire and the colon between them: its STOMP
// codec stops a line once it holds more than 10240 octets, a constant and not
// a setting. A longer line is answered with an ERROR frame, "The maximum
// header length was exceeded", and the connection closes, taking every
// publication in flight on it. Measured on apache/activemq-classic:6.2.0:
// 10241 octets carried, 10242 refused, and a colon, escaped as `\c`, counts
// twice. The bound covers every line, so a message key, which travels as
// `JMSXGroupID` and `x-iqmq-key`, is held to it too. Artemis 2.44.0 carried a
// 1 MiB value, so it bounds nothing here.
func TestSendHeadersRefuseALineClassicCannotRead(t *testing.T) {
	t.Parallel()
	header := func(v string) *mq.Message { return withHeader(&mq.Message{Body: []byte("b")}, "k", v) }
	keyed := func(n int) *mq.Message { return &mq.Message{Body: []byte("b"), Key: bytes.Repeat([]byte("k"), n)} }
	for name, tc := range map[string]struct {
		broker  activemq.Broker
		msg     *mq.Message
		carried bool
	}{
		"Classic, a line of 10241 octets":                       {activemq.Classic, header(strings.Repeat("v", 10239)), true},
		"Classic, a line of 10242 octets":                       {activemq.Classic, header(strings.Repeat("v", 10240)), false},
		"Classic, 5119 colons, a line of 10240 octets":          {activemq.Classic, header(strings.Repeat(":", 5119)), true},
		"Classic, 5120 colons, a line of 10242 octets":          {activemq.Classic, header(strings.Repeat(":", 5120)), false},
		"Classic, a key whose JMSXGroupID line is 10241 octets": {activemq.Classic, keyed(10229), true},
		"Classic, a key whose JMSXGroupID line is 10242 octets": {activemq.Classic, keyed(10230), false},
		"Classic, a key of 5115 colons, escaped to a JMSXGroupID line of 10242": {
			activemq.Classic, &mq.Message{Body: []byte("b"), Key: bytes.Repeat([]byte(":"), 5115)}, false,
		},
		"Artemis, a line of 10242 octets":                       {activemq.Artemis, header(strings.Repeat("v", 10240)), true},
		"Artemis, a key whose JMSXGroupID line is 10242 octets": {activemq.Artemis, keyed(10230), true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := stomp.SendHeadersForTest(mappingConfig(tc.broker), tc.msg, nil, fixedClock{at: epoch})
			switch {
			case tc.carried && err != nil:
				t.Fatalf("the mapping refused a frame header line the broker reads: %v", err)
			case !tc.carried && err == nil:
				t.Fatalf("the mapping let through a frame header line ActiveMQ Classic answers by closing the "+
					"connection (%d headers)", len(got))
			case !tc.carried && (mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsRetryable(err)):
				t.Fatalf("the refusal is %v (code %v, retryable %v), want MessageTooLarge, not retryable",
					err, mq.CodeOf(err), mq.IsRetryable(err))
			}
		})
	}
}

// TestALineClassicCannotReadIsRefusedBeforeAnyIO is that bound through Publish.
// Classic's own answer to the line is an ERROR frame and the end of the
// connection, so the refusal has to come before the frame is written: then
// nothing reaches the broker and the connection goes on serving the next
// publication.
func TestALineClassicCannotReadIsRefusedBeforeAnyIO(t *testing.T) {
	t.Parallel()
	srv := classicServer(t)
	cfg := settings(activemq.Classic)
	cfg.Addrs = []string{srv.Addr()}
	b := openBackend(t, cfg)
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	for name, m := range map[string]*mq.Message{
		"a header line of 10242 octets": withHeader(&mq.Message{Body: []byte("b")}, "k", strings.Repeat("v", 10240)),
		"a key whose line is 10242":     {Body: []byte("b"), Key: bytes.Repeat([]byte("k"), 10230)},
	} {
		err := snk.Publish(ctx, m, nil)
		if mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsAmbiguous(err) || mq.IsRetryable(err) {
			t.Errorf("%s: Publish = %v (code %v, ambiguous %v), want MessageTooLarge before any I/O", name, err,
				mq.CodeOf(err), mq.IsAmbiguous(err))
		}
	}
	if n := srv.Count("SEND"); n != 0 {
		t.Fatalf("%d SEND frames reached the server, want none: Classic closes the connection on a line it "+
			"cannot read\nframes:%s", n, frameLog(srv))
	}
	value := strings.Repeat("v", 10239)
	if err := snk.Publish(ctx, withHeader(&mq.Message{Body: []byte("b")}, "k", value), nil); err != nil {
		t.Fatalf("publishing a header line of 10241 octets, which Classic reads: %v", err)
	}
	if sends := srv.FramesOf("SEND"); len(sends) != 1 || sends[0].Get("k") != value {
		t.Fatalf("the server received %d SEND frames, want one carrying the 10239-byte value", len(sends))
	}
}

// TestSendHeadersRefuseMoreHeaderNamesThanClassicReads is ActiveMQ Classic's
// other STOMP bound, held exactly.
//
// Classic reads at most 1001 header names in one frame: its STOMP codec stops
// at the next line once it holds more than MAX_HEADERS, 1000, a constant, and
// answers with an ERROR frame, "The maximum number of headers was exceeded",
// and the end of the connection. The frame's own lines count: `content-length`
// and `destination`, which go-stomp writes, and the driver's, here `persistent`
// and the receipt. A content type is written twice, once by go-stomp and once by
// the driver, and Classic keeps the first and counts one name. Measured on
// apache/activemq-classic:6.2.0 through this driver: 997 application headers
// carried and 998 refused, and beside a content type 996 and 997. Artemis
// 2.44.0 carried 5000 headers, so it bounds nothing here.
func TestSendHeadersRefuseMoreHeaderNamesThanClassicReads(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		broker      activemq.Broker
		headers     int
		contentType string
		carried     bool
	}{
		"Classic, 997 headers":                  {activemq.Classic, 997, "", true},
		"Classic, 998 headers":                  {activemq.Classic, 998, "", false},
		"Classic, 996 headers and content-type": {activemq.Classic, 996, "text/plain", true},
		"Classic, 997 headers and content-type": {activemq.Classic, 997, "text/plain", false},
		"Artemis, 1024 headers":                 {activemq.Artemis, 1024, "text/plain", true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := withHeaderCount(&mq.Message{Body: []byte("b")}, tc.headers)
			m.Properties.ContentType = tc.contentType
			got, err := stomp.SendHeadersForTest(mappingConfig(tc.broker), m, nil, fixedClock{at: epoch})
			switch {
			case tc.carried && err != nil:
				t.Fatalf("the mapping refused %d headers, which the broker reads: %v", tc.headers, err)
			case !tc.carried && err == nil:
				t.Fatalf("the mapping let through %d headers (%d in the frame), which ActiveMQ Classic answers by "+
					"closing the connection", tc.headers, len(got))
			case !tc.carried && (mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsRetryable(err)):
				t.Fatalf("the refusal is %v (code %v, retryable %v), want MessageTooLarge, not retryable",
					err, mq.CodeOf(err), mq.IsRetryable(err))
			}
		})
	}
}

// TestMoreHeaderNamesThanClassicReadsAreRefusedBeforeAnyIO is that bound
// through Publish: nothing reaches the broker, and a frame at the bound goes out
// with every line, the doubled content type included. Under
// MetadataLenientDrop the application headers that do not fit are dropped and
// the rest published, the message key's own entries kept.
func TestMoreHeaderNamesThanClassicReadsAreRefusedBeforeAnyIO(t *testing.T) {
	t.Parallel()
	t.Run("refused", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		srv := classicServer(t)
		cfg := settings(activemq.Classic)
		cfg.Addrs = []string{srv.Addr()}
		snk := openSink(t, openBackend(t, cfg), "orders", mq.ConfirmAccepted)
		typed := withHeaderCount(&mq.Message{Body: []byte("b")}, 997)
		typed.Properties.ContentType = "text/plain"
		for name, m := range map[string]*mq.Message{
			"998 headers":                  withHeaderCount(&mq.Message{Body: []byte("b")}, 998),
			"997 headers and content-type": typed,
		} {
			err := snk.Publish(ctx, m, nil)
			if mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsAmbiguous(err) || mq.IsRetryable(err) {
				t.Errorf("%s: Publish = %v (code %v, ambiguous %v), want MessageTooLarge before any I/O", name, err,
					mq.CodeOf(err), mq.IsAmbiguous(err))
			}
		}
		if n := srv.Count("SEND"); n != 0 {
			t.Fatalf("%d SEND frames reached the server, want none", n)
		}
		at := withHeaderCount(&mq.Message{Body: []byte("b")}, 996)
		at.Properties.ContentType = "text/plain"
		if err := snk.Publish(ctx, at, nil); err != nil {
			t.Fatalf("publishing 996 headers beside a content type, which Classic reads: %v", err)
		}
		if sends := srv.FramesOf("SEND"); len(sends) != 1 || sends[0].Len() != 1002 {
			t.Fatalf("the server received %d SEND frames, want one of 1002 lines (1001 names)", len(sends))
		}
	})
	t.Run("lenient", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		srv := classicServer(t)
		cfg := settings(activemq.Classic)
		cfg.Addrs = []string{srv.Addr()}
		cfg.Metadata = mq.MetadataLenientDrop
		snk := openSink(t, openBackend(t, cfg), "orders", mq.ConfirmAccepted)
		m := withHeaderCount(&mq.Message{Body: []byte("b"), Key: []byte("customer-7")}, 1000)
		if err := snk.Publish(ctx, m, nil); err != nil {
			t.Fatalf("MetadataLenientDrop refused 1000 headers on Classic: %v", err)
		}
		sends := srv.FramesOf("SEND")
		if len(sends) != 1 || sends[0].Len() != 1001 {
			t.Fatalf("the server received %d SEND frames, want one of 1001 lines", len(sends))
		}
		if sends[0].Get(protocol.KeyHeader) != "customer-7" || sends[0].Get(protocol.PropertyGroupID) != "customer-7" {
			t.Errorf("the key's own entries were dropped: %s=%q, %s=%q", protocol.KeyHeader,
				sends[0].Get(protocol.KeyHeader), protocol.PropertyGroupID, sends[0].Get(protocol.PropertyGroupID))
		}
		if !sends[0].Has("h0") || sends[0].Has("h999") {
			t.Errorf("LenientDrop kept h999 or dropped h0; it drops the headers past the bound")
		}
	})
}

// withHeaderCount adds n short headers, h0 to h<n-1>, to a message.
func withHeaderCount(m *mq.Message, n int) *mq.Message {
	for i := range n {
		m.Headers.Add("h"+strconv.Itoa(i), []byte("v"))
	}
	return m
}

// TestSendHeadersCarryAKeyOf256BytesAndLenientDropsALongLine is the half of the
// fix that is the shared mapping's, in the root module: it no longer refuses a
// header key over 255 bytes, internal/activemq's name ceiling, which neither
// broker bounds a header key by (both carried a 10000-byte key), and under
// MetadataLenientDrop it drops an application header Classic cannot read, as
// that mode drops every header the frame cannot carry, rather than refusing the
// message.
func TestSendHeadersCarryAKeyOf256BytesAndLenientDropsALongLine(t *testing.T) {
	t.Parallel()
	if v, old := rootModuleBefore(t, "v1.1.3"); old {
		t.Skipf("this build uses root module %s, whose mapping refuses a header key over 255 bytes and does "+
			"not know Classic's line", v)
	}
	key := strings.Repeat("k", 256)
	for _, broker := range activemq.Brokers() {
		got := sendHeaders(t, mappingConfig(broker), withHeader(&mq.Message{Body: []byte("b")}, key, "v"), nil)
		if got[key] != "v" {
			t.Errorf("%v: a 256-byte header key went out as %q, want it carried", broker, got[key])
		}
	}
	cfg := mappingConfig(activemq.Classic)
	cfg.Metadata = mq.MetadataLenientDrop
	m := withHeader(withHeader(&mq.Message{Body: []byte("b")}, "tenant", "acme"), "k", strings.Repeat("v", 10240))
	got := sendHeaders(t, cfg, m, nil)
	if _, there := got["k"]; there {
		t.Errorf("a header line of 10242 octets went out under MetadataLenientDrop; Classic would close the " +
			"connection on it, and the mode drops what the frame cannot carry")
	}
	if got["tenant"] != "acme" {
		t.Errorf("the carryable header was dropped as well (%v)", got)
	}
}

// rootModuleBefore reports the version of the root module this test binary was
// built with, and whether it is a release older than want.
//
// CI also builds this module with GOWORK=off against the root release its
// go.mod requires, so a test of behaviour the root module gained after that
// release skips there until the release train moves the requirement on. A
// workspace build uses the root module in the tree, which the build
// information reports as (devel), and so does a replacement by a directory;
// neither is older than anything. drivers/rabbitmq, drivers/kafka and
// drivers/redis have the same helper.
func rootModuleBefore(t testing.TB, want string) (string, bool) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("this test binary carries no build information")
	}
	for _, dep := range info.Deps {
		if dep.Path != "github.com/iqhive/iqmq" {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		have, okHave := releaseNumbers(dep.Version)
		limit, okLimit := releaseNumbers(want)
		return dep.Version, okHave && okLimit && slices.Compare(have, limit) < 0
	}
	t.Fatal("this test binary's build information does not list the root module")
	return "", false
}

// releaseNumbers is the major, minor and patch numbers of a module version
// such as v1.1.2 or v1.1.3-0.20261118010203-0123456789ab, and false for
// anything else, (devel) and the empty version of a directory among them.
func releaseNumbers(version string) ([]int, bool) {
	core, ok := strings.CutPrefix(version, "v")
	if !ok {
		return nil, false
	}
	if i := strings.IndexAny(core, "-+"); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return nil, false
	}
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

// headerBytes is a message's header set's size, keys and values counted, as
// the root counts MaxHeaderBytes.
func headerBytes(m *mq.Message) int {
	n := 0
	for k, v := range m.Headers.All() {
		n += len(k) + len(v)
	}
	return n
}

// TestSubscribeHeaders pins the three SUBSCRIBE headers that are the brokers'
// own vocabulary, each of which was measured.
//
// None of them is optional decoration. Without `subscription-type: MULTICAST`
// a SUBSCRIBE to a fully qualified queue name is read by Artemis as an ANYCAST
// consumer, which is a different routing type on the same address — so the
// subscription would attach and consume from somewhere else. And the two
// flow-control headers are in different UNITS: Artemis's window is BYTES and
// Classic's is a MESSAGE COUNT, which is the one place the portable prefetch
// cannot be expressed and the reason the driver bounds the count on its own
// side as well.
func TestSubscribeHeaders(t *testing.T) {
	t.Parallel()
	artemis := stomp.SubscribeHeadersForTest(mappingConfig(activemq.Artemis), 4)
	if got := artemis["subscription-type"]; got != "MULTICAST" {
		t.Errorf("the Artemis SUBSCRIBE carries subscription-type %q, want MULTICAST: without it the broker reads "+
			"a fully qualified queue name as an ANYCAST consumer, which is a different routing type on the same "+
			"address\nwhole frame: %v", got, artemis)
	}
	if got := artemis["consumer-window-size"]; got != itoa64(stomp.DefaultConsumerWindow) {
		t.Errorf("the Artemis SUBSCRIBE carries consumer-window-size %q, want the resolved default %d in BYTES",
			got, stomp.DefaultConsumerWindow)
	}
	if _, there := artemis["activemq.prefetchSize"]; there {
		t.Errorf("the Artemis SUBSCRIBE carries Classic's flow-control header, which Artemis does not read: %v",
			artemis)
	}

	classic := stomp.SubscribeHeadersForTest(mappingConfig(activemq.Classic), 4)
	if got := classic["activemq.prefetchSize"]; got != "4" {
		t.Errorf("the Classic SUBSCRIBE carries activemq.prefetchSize %q, want the source's own prefetch 4 -- it "+
			"is a MESSAGE COUNT there, which is why the count can be passed to Classic and not to "+
			"Artemis\nwhole frame: %v", got, classic)
	}
	for _, k := range [...]string{"subscription-type", "consumer-window-size"} {
		if _, there := classic[k]; there {
			t.Errorf("the Classic SUBSCRIBE carries %q, which is Artemis's vocabulary: %v", k, classic)
		}
	}
	// The source's prefetch overrides the client's, which is what
	// SourceConfig.Prefetch means.
	cfg := mappingConfig(activemq.Classic)
	cfg.Prefetch = 64
	if got := stomp.SubscribeHeadersForTest(cfg, 0)["activemq.prefetchSize"]; got != "64" {
		t.Errorf("a source with no prefetch of its own sent %q, want the client's 64", got)
	}
	if got := stomp.SubscribeHeadersForTest(cfg, 8)["activemq.prefetchSize"]; got != "8" {
		t.Errorf("a source with a prefetch of 8 sent %q; SourceConfig.Prefetch overrides the client's", got)
	}
}

// TestSubscribeSelectorAppearsOnlyWhenConfigured is the evidence rule on
// the wire: a claim that cannot be exercised is not made, and a header nobody
// asked for is not sent.
func TestSubscribeSelectorAppearsOnlyWhenConfigured(t *testing.T) {
	t.Parallel()
	if got, there := stomp.SubscribeHeadersForTest(mappingConfig(activemq.Classic), 4)["selector"]; there {
		t.Errorf("a client with no selector sent selector=%q. An empty selector is not a selector that matches "+
			"everything: on Classic it is an expression the broker parses, and a bad one arrives as an ERROR "+
			"frame that closes the connection", got)
	}
	cfg := mappingConfig(activemq.Classic)
	cfg.Selector = "colour = 'red'"
	got := stomp.SubscribeHeadersForTest(cfg, 4)
	if got["selector"] != "colour = 'red'" {
		t.Errorf("the SUBSCRIBE carries selector %q, want the configured expression verbatim -- a selector the "+
			"driver rewrote would filter something the caller did not ask for\nwhole frame: %v",
			got["selector"], got)
	}
}

// TestSubscribeAlwaysAsksForIndividualAcknowledgement is the specification's
// "`client-individual` ack", which is what makes a per-delivery ACK mean one
// delivery.
//
// The mode is not in [stomp.SubscribeHeadersForTest]'s output because
// go-stomp sets it from the `AckMode` argument rather than from an option, so
// what pins it is the frame the server actually received.
func TestSubscribeAlwaysAsksForIndividualAcknowledgement(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	_, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	subs := srv.FramesOf("SUBSCRIBE")
	if len(subs) != 1 {
		t.Fatalf("the server received %d SUBSCRIBE frames, want 1", len(subs))
	}
	if got := subs[0].Get("ack"); got != "client-individual" {
		t.Errorf("the SUBSCRIBE asks for ack %q, want client-individual. `client` acknowledges every PREVIOUS "+
			"delivery as well, so one ACK would settle messages the caller never saw", got)
	}
	if got := subs[0].Get("destination"); got != consume {
		t.Errorf("the SUBSCRIBE names %q, want the derived destination %q", got, consume)
	}
	if !strings.Contains(consume, activemq.FQQN) {
		t.Errorf("the Artemis consume destination %q is not a fully qualified queue name; a SUBSCRIBE to the bare "+
			"address would get whatever queue the broker chose", consume)
	}
}

// itoa64 renders a 64-bit integer, for a header whose value is a number.
func itoa64[T int | int64](n T) string {
	neg := n < 0
	if neg {
		n = -n
	}
	if n == 0 {
		return "0"
	}
	var buf [24]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// withHeader adds one text header to a message and returns it.
func withHeader(m *mq.Message, k, v string) *mq.Message {
	m.Headers.Add(k, []byte(v))
	return m
}

// withHeaderBytes adds one header whose value is raw bytes.
func withHeaderBytes(m *mq.Message, k string, v []byte) *mq.Message {
	m.Headers.Add(k, v)
	return m
}

// errorMatches reports whether err carries want, by identity or by code and
// message.
//
// The driver wraps a mapping refusal in its own operation error, and the
// sentinels the shared mapping uses are `errcode` values, so `errors.Is` finds
// them by code alone — which would make "refused for the right reason"
// indistinguishable from "refused for another reason with the same code". So
// the message has to match too.
func errorMatches(err, want error) bool {
	return mq.CodeOf(err) == mq.CodeOf(want) && strings.Contains(err.Error(), trimPrefix(want.Error()))
}

// trimPrefix drops the shared mapping's package prefix from a sentinel's text,
// because the driver's wrapper puts its own in front.
func trimPrefix(s string) string {
	if i := strings.Index(s, ": "); i >= 0 {
		return s[i+2:]
	}
	return s
}
