package jetstream_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	sjs "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// TestSubjectMapping holds the topic→subject mapping to the driver page's rules.
func TestSubjectMapping(t *testing.T) {
	cases := []struct {
		name, prefix, topic, want string
		wantErr                   bool
	}{
		{name: "plain", topic: "orders", want: "orders"},
		{name: "slash becomes dot", topic: "orders/created", want: "orders.created"},
		{name: "prefix is a token", prefix: "acme", topic: "orders", want: "acme.orders"},
		{name: "prefix and slashes", prefix: "acme", topic: "a/b/c", want: "acme.a.b.c"},
		{name: "dollar is allowed", topic: "$KV.bucket", want: "$KV.bucket"},
		{name: "empty topic", topic: "", wantErr: true},
		{name: "single wildcard refused", topic: "orders.*", wantErr: true},
		{name: "tail wildcard refused", topic: "orders.>", wantErr: true},
		{name: "empty token", topic: "orders..created", wantErr: true},
		{name: "space", topic: "orders created", wantErr: true},
		{name: "control byte", topic: "orders\x00", wantErr: true},
		{name: "non ascii", topic: "ordérs", wantErr: true},
		{name: "quote", topic: `orders"`, wantErr: true},
		{name: "too long", topic: strings.Repeat("a", nq.MaxSubjectLen+1), wantErr: true},
		{name: "too many tokens", topic: strings.TrimSuffix(strings.Repeat("a.", nq.MaxSubjectTokens+1), "."), wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := nq.Subject(c.prefix, c.topic)
			switch {
			case c.wantErr && err == nil:
				t.Fatalf("Subject(%q, %q) = %q, want an error", c.prefix, c.topic, got)
			case c.wantErr:
				if mq.CodeOf(err) != mq.CodeInvalidArgument {
					t.Errorf("code = %v, want InvalidArgument", mq.CodeOf(err))
				}
			case err != nil:
				t.Fatalf("Subject(%q, %q): %v", c.prefix, c.topic, err)
			case got != c.want:
				t.Errorf("Subject(%q, %q) = %q, want %q", c.prefix, c.topic, got, c.want)
			}
		})
	}
}

// TestPatternMapping is the subscription half: a wildcard is the point there,
// and a ">" that is not the last token is refused because it matches nothing
// while reading as though it matched everything.
func TestPatternMapping(t *testing.T) {
	for _, c := range []struct {
		topic   string
		want    string
		wantErr bool
	}{
		{topic: "orders.*", want: "orders.*"},
		{topic: "orders.>", want: "orders.>"},
		{topic: "*", want: "*"},
		{topic: ">", want: ">"},
		{topic: "orders.>.late", wantErr: true},
		{topic: "orders.*.created", want: "orders.*.created"},
	} {
		got, err := nq.Pattern("", c.topic)
		switch {
		case c.wantErr && err == nil:
			t.Errorf("Pattern(%q) = %q, want an error", c.topic, got)
		case !c.wantErr && err != nil:
			t.Errorf("Pattern(%q): %v", c.topic, err)
		case !c.wantErr && got != c.want:
			t.Errorf("Pattern(%q) = %q, want %q", c.topic, got, c.want)
		}
	}
}

// TestTopicOfStripsThePrefix holds the reverse mapping, including the fact that
// it is NOT injective: "a/b" and "a.b" are one subject and no reverse can tell
// which was published.
func TestTopicOfStripsThePrefix(t *testing.T) {
	if got := nq.TopicOf("acme", "acme.orders.created"); got != "orders.created" {
		t.Errorf("TopicOf = %q, want orders.created", got)
	}
	if got := nq.TopicOf("", "orders.created"); got != "orders.created" {
		t.Errorf("TopicOf with no prefix = %q", got)
	}
	if got := nq.TopicOf("acme", "other.orders"); got != "other.orders" {
		t.Errorf("TopicOf of a subject outside the prefix = %q, want it unchanged", got)
	}
	a, _ := nq.Subject("", "a/b")
	b, _ := nq.Subject("", "a.b")
	if a != b {
		t.Fatalf("the mapping became injective: %q != %q", a, b)
	}
}

// TestDurableNames holds the durable-consumer naming rule: one subscription name
// on two topics is two groups and therefore two durables, or the second would
// inherit the first's ack floor and skip messages.
func TestDurableNames(t *testing.T) {
	one, err := nq.Durable("iqmq-", "workers", "orders.created")
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	two, err := nq.Durable("iqmq-", "workers", "orders.shipped")
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if one == two {
		t.Fatalf("one subscription name on two filters produced one durable %q: the second would inherit the first's ack floor ", one)
	}
	again, err := nq.Durable("iqmq-", "workers", "orders.created")
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if again != one {
		t.Fatalf("the same subscription and filter produced two durables %q and %q: a durable subscription would lose its position on every reopen", one, again)
	}
	if !strings.HasPrefix(one, "iqmq-workers-") {
		t.Errorf("durable %q does not read as its subscription: a name nobody can find in `nats consumer ls` is a name nobody can operate", one)
	}
	if err := nq.CheckName(one); err != nil {
		t.Errorf("the durable this driver builds is not a name JetStream accepts: %v", err)
	}
	// Every byte JetStream refuses is mapped, and the hash keeps two names
	// that map alike apart.
	dirty, err := nq.Durable("iqmq-", "a.b c/d>e*f", "x")
	if err != nil {
		t.Fatalf("Durable of a name full of refused bytes: %v", err)
	}
	if err := nq.CheckName(dirty); err != nil {
		t.Errorf("CheckName(%q): %v", dirty, err)
	}
	if _, err := nq.Durable("iqmq-", "", "x"); err == nil {
		t.Error("a durable was built from an empty subscription")
	}
	if _, err := nq.Durable("iqmq-", strings.Repeat("a", nq.MaxNameLen+1), "x"); err == nil {
		t.Error("a durable was built from a subscription longer than a JetStream name")
	}
}

// TestDurableNamesDoNotCollide holds that sanitising and clamping, which are
// many-to-one, never fold two subscriptions onto one durable. Two groups that
// should each see every message would otherwise share one consumer and split the
// stream between them.
func TestDurableNamesDoNotCollide(t *testing.T) {
	seen := map[string]string{}
	long := strings.Repeat("x", nq.MaxNameLen-10)
	for _, sub := range []string{
		"billing.v1", "billing-v1", "billing/v1", "billing v1", "billing>v1",
		long + "A", long + "B", long + "AAAAAAAAA", long + "BBBBBBBBB",
	} {
		d, err := nq.Durable("", sub, "app.orders")
		if err != nil {
			t.Fatalf("Durable(%q): %v", sub, err)
		}
		if err := nq.CheckName(d); err != nil {
			t.Errorf("Durable(%q) = %q, which JetStream refuses: %v", sub, d, err)
		}
		if prev, ok := seen[d]; ok {
			t.Errorf("subscriptions %q and %q share the durable %q", prev, sub, d)
		}
		seen[d] = sub
	}
	// The prefix takes room from the subscription, so a name that fits alone
	// is clamped under a prefix and has to stay apart there too.
	a, err := nq.Durable("iqmq-", long+"A", "f")
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	b, err := nq.Durable("iqmq-", long+"B", "f")
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if a == b {
		t.Errorf("two long subscriptions clamped to one durable %q", a)
	}
}

// TestDurableNamesKeepTheirEarlierSpelling pins the durable a clean, unclamped
// subscription maps to. Those names are what earlier releases created on
// servers in use, and a change to them would orphan every such durable with its
// ack floor and start the group again from its start position.
func TestDurableNamesKeepTheirEarlierSpelling(t *testing.T) {
	for _, c := range []struct{ prefix, sub, filter, want string }{
		{"", "billing", "app.orders", "billing-mfee5gh9gf1z"},
		{"iqmq-", "orders_v2", "s.orders.>", "iqmq-orders_v2-mz54s23a0lrp"},
		{"", "Audit-Log", "app.*.events", "Audit-Log-wbmzm2kk8vmj"},
		{"pfx_", "sub", "", "pfx_sub-33niihzj4ux45"},
	} {
		got, err := nq.Durable(c.prefix, c.sub, c.filter)
		if err != nil {
			t.Fatalf("Durable(%q, %q, %q): %v", c.prefix, c.sub, c.filter, err)
		}
		if got != c.want {
			t.Errorf("Durable(%q, %q, %q) = %q, want %q: an upgrade would orphan the existing durable", c.prefix, c.sub, c.filter, got, c.want)
		}
	}
}

// TestCheckName holds the stream and consumer name alphabet.
func TestCheckName(t *testing.T) {
	for _, ok := range []string{"IQMQ", "orders-1", "a_b", "A1"} {
		if err := nq.CheckName(ok); err != nil {
			t.Errorf("CheckName(%q): %v", ok, err)
		}
	}
	for _, bad := range []string{"", "a.b", "a b", "a*b", "a>b", "a/b", "a\\b", "a\x00", "ä", strings.Repeat("a", nq.MaxNameLen+1)} {
		if err := nq.CheckName(bad); err == nil {
			t.Errorf("CheckName(%q) accepted a name JetStream refuses", bad)
		}
	}
}

// TestNATSHeaderNamespaceIsRefused is the rule the Core driver did not need. The
// server owns the Nats- namespace: Nats-Msg-Id is de-duplication, Nats-TTL is
// expiry, Nats-Schedule is a schedule. A caller header there would change the
// broker's behaviour rather than travel with the message.
func TestNATSHeaderNamespaceIsRefused(t *testing.T) {
	for _, k := range []string{
		sjs.MsgIDHeader, sjs.MsgTTLHeader, sjs.ScheduleHeader, sjs.ScheduleTargetHeader,
		"Nats-Anything", "nats-msg-id", "NATS-TTL",
	} {
		if nq.HeaderKeyOK(k) {
			t.Errorf("HeaderKeyOK(%q) accepted a header in the server's own namespace ", k)
		}
		if !nq.IsNATSHeader(k) {
			t.Errorf("IsNATSHeader(%q) = false", k)
		}
	}
	for _, k := range []string{"natsy", "nat", "x-nats-id", "content-kind"} {
		if nq.IsNATSHeader(k) {
			t.Errorf("IsNATSHeader(%q) = true: the rule is a prefix and not a substring", k)
		}
	}
	var m mq.Message
	m.Headers.Set("Nats-Msg-Id", []byte("k"))
	err := nq.Representable(&m)
	if err == nil {
		t.Fatal("Representable accepted a message carrying a Nats- header")
	}
	if mq.CodeOf(err) != mq.CodeUnsupported {
		t.Errorf("code = %v, want Unsupported", mq.CodeOf(err))
	}
}

// TestHeaderFramingRules holds what a NATS header block can carry, which is a
// question about framing rather than taste.
func TestHeaderFramingRules(t *testing.T) {
	for _, k := range []string{"content-kind", "X-Trace", "a_b-1"} {
		if !nq.HeaderKeyOK(k) {
			t.Errorf("HeaderKeyOK(%q) = false", k)
		}
	}
	for _, k := range []string{"", "a:b", "a b", "a\rb", "a\nb", " a", "ä", strings.Repeat("k", nq.MaxHeaderKeyLen+1)} {
		if nq.HeaderKeyOK(k) {
			t.Errorf("HeaderKeyOK(%q) accepted a name the framing breaks", k)
		}
	}
	for _, v := range [][]byte{nil, {}, []byte("plain"), []byte("a b"), []byte("ünicode")} {
		if !nq.HeaderValueOK(v) {
			t.Errorf("HeaderValueOK(%q) = false", v)
		}
	}
	for _, v := range [][]byte{
		{0x00},
		{0xff},
		[]byte("a\r\nb"), []byte(" leading"), []byte("\tleading"),
		bytes.Repeat([]byte("v"), nq.MaxHeaderValueLen+1),
	} {
		if nq.HeaderValueOK(v) {
			t.Errorf("HeaderValueOK(%q) accepted a value that does not survive the round trip", v)
		}
	}
}

// TestRepresentableRefusesWhatCannotRoundTrip is MetadataNativeStrict's own
// predicate: what it accepts comes back byte for byte, and what it refuses is
// refused before any I/O.
func TestRepresentableRefusesWhatCannotRoundTrip(t *testing.T) {
	good := &mq.Message{Body: []byte("x"), ID: "id-1"}
	good.Headers.Set("k", []byte("v"))
	good.Properties.ContentType = "application/json"
	if err := nq.Representable(good); err != nil {
		t.Fatalf("Representable of a plain message: %v", err)
	}
	binary := &mq.Message{}
	binary.Headers.Set("trace", []byte{0x00, 0xff})
	if err := nq.Representable(binary); err == nil {
		t.Error("Representable accepted a binary header value: FeatureBinaryHeaders is advertised as unsupported")
	}
	reserved := &mq.Message{}
	reserved.Headers.Set(mq.ReservedHeaderPrefix+"key", []byte("v"))
	if err := nq.Representable(reserved); err == nil {
		t.Error("Representable accepted a header in the iqmq- namespace")
	}
	if err := nq.Representable(nil); err == nil {
		t.Error("Representable(nil) returned no error")
	}
	badReply := &mq.Message{}
	badReply.Properties.ReplyTo = "not a subject"
	if err := nq.Representable(badReply); err == nil {
		t.Error("Representable accepted a ReplyTo that is not a subject")
	}
}

// TestEncodeDecodeRoundTrip holds the metadata over a protocol that has a
// subject, a reply subject and text headers, and nothing else.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Nanosecond)
	in := &mq.Message{
		Key:  []byte{0x00, 0x01, 0xff},
		Body: []byte("payload"),
		ID:   "id-7",
	}
	in.Headers.Add("dup", []byte("one"))
	in.Headers.Add("dup", []byte("two"))
	in.Properties.ContentType = "application/json"
	in.Properties.ContentEncoding = "gzip"
	in.Properties.CorrelationID = "corr"
	in.Properties.CausationID = "cause"
	in.Properties.CreatedAt = created
	in.Properties.ReplyTo = "replies.inbox"

	var msg natsgo.Msg
	nq.Encode(&msg, in, nil)
	if msg.Reply != "" {
		t.Errorf("Encode set the native reply subject %q: a JetStream publication replaces it with its PubAck inbox, and an asynchronous one is refused", msg.Reply)
	}
	if got := msg.Header.Get(nq.HeaderReplyTo); got != "replies.inbox" {
		t.Errorf("%s = %q, want the reply-to's subject", nq.HeaderReplyTo, got)
	}
	var out mq.Message
	nq.Decode(&out, "orders.created", msg.Reply, msg.Header, msg.Data, "")
	if !bytes.Equal(out.Key, in.Key) {
		t.Errorf("key = %x, want %x", out.Key, in.Key)
	}
	if string(out.Body) != "payload" {
		t.Errorf("body = %q", out.Body)
	}
	if out.ID != "id-7" {
		t.Errorf("id = %q", out.ID)
	}
	if vals := out.Headers.Values("dup"); len(vals) != 2 || string(vals[0]) != "one" || string(vals[1]) != "two" {
		t.Errorf("duplicate header came back as %d values: the specification keeps the duplicates and their order", len(vals))
	}
	if out.Properties.ContentType != "application/json" || out.Properties.ContentEncoding != "gzip" {
		t.Errorf("content properties = %+v", out.Properties)
	}
	if out.Properties.CorrelationID != "corr" || out.Properties.CausationID != "cause" {
		t.Errorf("correlation properties = %+v", out.Properties)
	}
	if !out.Properties.CreatedAt.Equal(created) {
		t.Errorf("created = %v, want %v", out.Properties.CreatedAt, created)
	}
	if out.Properties.ReplyTo != "replies.inbox" {
		t.Errorf("reply-to = %q", out.Properties.ReplyTo)
	}
	if out.Topic != "orders.created" {
		t.Errorf("topic = %q", out.Topic)
	}
}

// TestDecodeDropsTheServersOwnHeaders is the rule the scheduled-publication path
// made necessary: a message the scheduler generated carries Nats-Scheduler and
// Nats-Schedule-Next, a republished one carries Nats-Stream and Nats-Sequence,
// and handing those to the application as caller headers would be inventing
// metadata it never published.
func TestDecodeDropsTheServersOwnHeaders(t *testing.T) {
	hdr := natsgo.Header{}
	hdr.Set("Nats-Scheduler", "iqmqsched.S.1")
	hdr.Set("Nats-Schedule-Next", "purge")
	hdr.Set("Nats-Stream", "S")
	hdr.Set("Nats-Sequence", "4")
	hdr.Set("content-kind", "order")
	var out mq.Message
	nq.Decode(&out, "orders", "", hdr, []byte("x"), "")
	for k := range out.Headers.All() {
		if nq.IsNATSHeader(k) {
			t.Errorf("Decode handed the caller the server's own header %q", k)
		}
	}
	if v, ok := out.Headers.Get("content-kind"); !ok || string(v) != "order" {
		t.Errorf("Decode dropped the caller's own header too: %q (present %v)", v, ok)
	}
}

// TestDecodeReplyToWithoutTheHeader holds that a message with no
// [nq.HeaderReplyTo], which is what an earlier release or a non-iqmq publisher
// wrote, still decodes the reply subject it is given, and that the header wins
// when both are there.
func TestDecodeReplyToWithoutTheHeader(t *testing.T) {
	var out mq.Message
	nq.Decode(&out, "s.orders", "s.replies.a", nil, []byte("x"), "s")
	if out.Properties.ReplyTo != "replies.a" {
		t.Errorf("reply-to without the header = %q, want %q", out.Properties.ReplyTo, "replies.a")
	}
	hdr := natsgo.Header{}
	hdr.Set(nq.HeaderReplyTo, "s.replies.b")
	out = mq.Message{}
	nq.Decode(&out, "s.orders", "s.replies.a", hdr, []byte("x"), "s")
	if out.Properties.ReplyTo != "replies.b" {
		t.Errorf("reply-to with the header = %q, want %q", out.Properties.ReplyTo, "replies.b")
	}
	if _, ok := out.Headers.Get(nq.HeaderReplyTo); ok {
		t.Errorf("Decode handed the caller the reserved %s header", nq.HeaderReplyTo)
	}
}

// TestNilVersusEmptyBody holds the observable distinction over a protocol
// that has neither: nats.go builds every payload with make([]byte, n), so a
// zero-length payload always arrives as an empty non-nil slice.
func TestNilVersusEmptyBody(t *testing.T) {
	for _, c := range []struct {
		name    string
		body    []byte
		wantNil bool
	}{
		{name: "nil stays nil", body: nil, wantNil: true},
		{name: "empty stays empty", body: []byte{}, wantNil: false},
	} {
		t.Run(c.name, func(t *testing.T) {
			var msg natsgo.Msg
			nq.Encode(&msg, &mq.Message{Body: c.body}, nil)
			var out mq.Message
			// The wire always hands an empty non-nil payload over, which is
			// what makes the marker load-bearing.
			nq.Decode(&out, "t", "", msg.Header, []byte{}, "")
			if got := out.Body == nil; got != c.wantNil {
				t.Fatalf("body nil = %v, want %v", got, c.wantNil)
			}
		})
	}
}

// TestDecodeIsDeterministic holds the sorted-header answer: nats.Header is a Go
// map, so the publisher's order is gone and an unsorted iteration would give one
// message a different header order on every delivery.
func TestDecodeIsDeterministic(t *testing.T) {
	hdr := natsgo.Header{}
	for _, k := range []string{"zed", "alpha", "mid", "beta", "gamma"} {
		hdr.Set(k, k)
	}
	var want []string
	for i := range 20 {
		var out mq.Message
		nq.Decode(&out, "t", "", hdr, []byte("x"), "")
		var got []string
		for k := range out.Headers.All() {
			got = append(got, k)
		}
		if i == 0 {
			want = got
			continue
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("round %d decoded the header order %v, want %v", i, got, want)
		}
	}
}

// TestScheduleSubjectIsASibling holds the one structural rule the schedule route
// rests on: the control messages live outside the topic tree, so a wildcard
// subscription under the prefix cannot receive them.
func TestScheduleSubjectIsASibling(t *testing.T) {
	root := "iqmqsched.ORDERS"
	one := nq.ScheduleSubject(root, 1)
	two := nq.ScheduleSubject(root, 2)
	if one == two {
		t.Fatalf("two scheduled publications share the subject %q: the second would overwrite the first's schedule", one)
	}
	if err := nq.CheckSubject(one, false); err != nil {
		t.Fatalf("CheckSubject(%q): %v", one, err)
	}
	if strings.HasPrefix(one, "acme.") {
		t.Fatalf("the schedule subject %q is inside the topic tree", one)
	}
	for _, pattern := range []string{">", "iqmqsched.>", "iqmqsched.ORDERS.>", "iqmqsched.*.*", "*.ORDERS.>"} {
		if !nq.PatternReachesSchedules(pattern, root) {
			t.Errorf("PatternReachesSchedules(%q, %q) = false: a consumer on it would receive the driver's own schedule messages", pattern, root)
		}
	}
	for _, pattern := range []string{"acme.>", "acme.orders.*", "iqmqsched", "iqmqsched.OTHER.>", "orders"} {
		if nq.PatternReachesSchedules(pattern, root) {
			t.Errorf("PatternReachesSchedules(%q, %q) = true: a legitimate subscription is being refused", pattern, root)
		}
	}
	if nq.PatternReachesSchedules("", root) || nq.PatternReachesSchedules(">", "") {
		t.Error("PatternReachesSchedules answered true for an empty argument")
	}
}
