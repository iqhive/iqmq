package nats_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nats"
)

// TestSubjectMapping holds the topic-to-subject mapping to the rules the driver
// page states, in both directions.
func TestSubjectMapping(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, topic, want string
		wantErr                   bool
	}{
		{name: "plain", topic: "orders", want: "orders"},
		{name: "portable hierarchy becomes NATS hierarchy", topic: "orders/created", want: "orders.created"},
		{name: "dots are already the NATS separator", topic: "orders.created", want: "orders.created"},
		{name: "prefix is a token", prefix: "acme", topic: "orders/created", want: "acme.orders.created"},
		{name: "underscores and dashes survive", topic: "_INBOX.a-b_c", want: "_INBOX.a-b_c"},
		{name: "empty", topic: "", wantErr: true},
		{name: "empty token", topic: "a..b", wantErr: true},
		{name: "leading separator", topic: "/a", wantErr: true},
		{name: "trailing separator", topic: "a/", wantErr: true},
		{name: "space", topic: "a b", wantErr: true},
		{name: "newline", topic: "a\nb", wantErr: true},
		{name: "null", topic: "a\x00b", wantErr: true},
		{name: "star is a wildcard and never a publication", topic: "orders.*", wantErr: true},
		{name: "arrow is a wildcard and never a publication", topic: "orders.>", wantErr: true},
		{name: "star inside a token", topic: "or*ders", wantErr: true},
		{name: "too long", topic: strings.Repeat("a", 2000), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nq.Subject(tc.prefix, tc.topic)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Subject(%q, %q) = %q, want a refusal", tc.prefix, tc.topic, got)
				}
				if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
					t.Fatalf("Subject(%q, %q) code = %v, want InvalidArgument", tc.prefix, tc.topic, code)
				}
				return
			}
			if err != nil {
				t.Fatalf("Subject(%q, %q): %v", tc.prefix, tc.topic, err)
			}
			if got != tc.want {
				t.Fatalf("Subject(%q, %q) = %q, want %q", tc.prefix, tc.topic, got, tc.want)
			}
			if back := nq.TopicOf(tc.prefix, got); tc.prefix != "" && back == got {
				t.Fatalf("TopicOf(%q, %q) = %q: the prefix was not stripped", tc.prefix, got, back)
			}
		})
	}
}

// TestPatternAllowsOnlyWholeTokenWildcards is the other half of the mapping: a
// subscription may name a pattern, and only in the shapes NATS really has.
func TestPatternAllowsOnlyWholeTokenWildcards(t *testing.T) {
	for _, tc := range []struct {
		topic   string
		wantErr bool
	}{
		{topic: "orders.*"},
		{topic: "orders.*.created"},
		{topic: "orders.>"},
		{topic: "*"},
		{topic: ">"},
		{topic: "orders.>.tail", wantErr: true},
		{topic: "orders.a*", wantErr: true},
		{topic: "orders.>x", wantErr: true},
	} {
		got, err := nq.Pattern("", tc.topic)
		switch {
		case tc.wantErr && err == nil:
			t.Errorf("Pattern(%q) = %q, want a refusal", tc.topic, got)
		case !tc.wantErr && err != nil:
			t.Errorf("Pattern(%q): %v", tc.topic, err)
		}
	}
}

// TestQueueNames holds the subscription-to-queue-group mapping to the same
// alphabet, plus the dot a queue group may carry.
func TestQueueNames(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wantErr bool
	}{
		{name: "workers"},
		{name: "orders.workers"},
		{name: ""},
		{name: "a b", wantErr: true},
		{name: "a*", wantErr: true},
		{name: strings.Repeat("q", 300), wantErr: true},
	} {
		err := nq.CheckQueue(tc.name)
		wantErr := tc.wantErr || tc.name == ""
		if (err != nil) != wantErr {
			t.Errorf("CheckQueue(%q) = %v, wantErr %v", tc.name, err, wantErr)
		}
	}
}

// TestHeaderFramingRules pins the three rules a NATS header really breaks on.
//
// Each of them is a round trip that silently changes the value, not a taste
// judgement: a colon truncates the name, a CR or LF forges a second header, and
// a leading space is eaten by the reader.
func TestHeaderFramingRules(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{key: "content-kind", want: true},
		{key: "X-Trace-Id", want: true},
		{key: "", want: false},
		{key: "a:b", want: false},
		{key: "a b", want: false},
		{key: "a\r\nb", want: false},
		{key: "héader", want: false},
	} {
		if got := nq.HeaderKeyOK(tc.key); got != tc.want {
			t.Errorf("HeaderKeyOK(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
	for _, tc := range []struct {
		val  []byte
		want bool
	}{
		{val: []byte("plain"), want: true},
		{val: []byte(""), want: true},
		{val: []byte("héllo"), want: true},
		{val: []byte(" leading"), want: false},
		{val: []byte("\tleading"), want: false},
		{val: []byte("a\rb"), want: false},
		{val: []byte("a\nb"), want: false},
		{val: []byte{0x00}, want: false},
		{val: []byte{0xff, 0xfe}, want: false},
	} {
		if got := nq.HeaderValueOK(tc.val); got != tc.want {
			t.Errorf("HeaderValueOK(%q) = %v, want %v", tc.val, got, tc.want)
		}
	}
}

// TestEncodeDecodeRoundTrip is the mapping's own round trip, with no broker in
// the way: everything the specification names goes out and comes back.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	created := time.Date(2026, 9, 13, 10, 11, 12, 131415, time.UTC)
	src := &mq.Message{
		Topic: "orders.created",
		Key:   []byte{0x00, 0xff, 0x10},
		Body:  []byte("payload"),
		ID:    "msg-1",
		Properties: mq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			CorrelationID:   "corr-1",
			CausationID:     "cause-1",
			ReplyTo:         "replies.one",
			CreatedAt:       created,
		},
	}
	src.Headers.Add("tag", []byte("one"))
	src.Headers.Add("tag", []byte("two"))
	src.Headers.Set("content-kind", []byte("order"))

	var msg natsgo.Msg
	msg.Subject = "orders.created"
	nq.Encode(&msg, src, nil)

	var got mq.Message
	nq.Decode(&got, &msg, "")

	if !bytes.Equal(got.Key, src.Key) {
		t.Errorf("key = %q, want %q", got.Key, src.Key)
	}
	if !bytes.Equal(got.Body, src.Body) {
		t.Errorf("body = %q, want %q", got.Body, src.Body)
	}
	if got.ID != src.ID {
		t.Errorf("id = %q, want %q", got.ID, src.ID)
	}
	if got.Properties.ContentType != src.Properties.ContentType ||
		got.Properties.ContentEncoding != src.Properties.ContentEncoding ||
		got.Properties.CorrelationID != src.Properties.CorrelationID ||
		got.Properties.CausationID != src.Properties.CausationID ||
		got.Properties.ReplyTo != src.Properties.ReplyTo {
		t.Errorf("properties = %+v, want %+v", got.Properties, src.Properties)
	}
	if !got.Properties.CreatedAt.Equal(created) {
		t.Errorf("created at = %v, want %v", got.Properties.CreatedAt, created)
	}
	if vals := got.Headers.Values("tag"); len(vals) != 2 ||
		string(vals[0]) != "one" || string(vals[1]) != "two" {
		t.Errorf("duplicate header came back as %d values", len(vals))
	}
	if v, ok := got.Headers.Get("content-kind"); !ok || string(v) != "order" {
		t.Errorf("header content-kind = %q (present %v)", v, ok)
	}
	// The reserved headers are the driver's own and never reappear as
	// caller headers: a round trip that leaked them would let a republish
	// fail the reserved-prefix rule. The five dead-letter provenance
	// keys are the exception and are carried deliberately — this message
	// has none of them, so nothing under the prefix may appear here at all.
	for k := range got.Headers.All() {
		if strings.HasPrefix(k, mq.ReservedHeaderPrefix) {
			t.Errorf("the decoded message carries the reserved header %q", k)
		}
	}
}

// TestNilVersusEmptyBody is the distinction, which NATS cannot make on its
// own: every zero-length payload arrives as an empty non-nil slice, so the
// driver marks the empty half and leaves the absent half unmarked.
func TestNilVersusEmptyBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{name: "nil", body: nil},
		{name: "empty", body: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var msg natsgo.Msg
			msg.Subject = "b"
			nq.Encode(&msg, &mq.Message{Body: tc.body}, nil)
			var got mq.Message
			nq.Decode(&got, &msg, "")
			if (got.Body == nil) != (tc.body == nil) {
				t.Fatalf("a %s body came back with nil = %v", tc.name, got.Body == nil)
			}
		})
	}
}

// TestRepresentableRefusesWhatCannotBeCarried is strict mode as a pure
// predicate: every refusal is ErrUnsupported, because what it reports is a
// broker with no field for the metadata.
func TestRepresentableRefusesWhatCannotBeCarried(t *testing.T) {
	binary := &mq.Message{Body: []byte("b")}
	binary.Headers.Set("trace-state", []byte{0x00, 0xff})
	reserved := &mq.Message{Body: []byte("b")}
	reserved.Headers.Set(mq.ReservedHeaderPrefix+"key", []byte("stolen"))
	badKey := &mq.Message{Body: []byte("b")}
	badKey.Headers.Set("a:b", []byte("v"))

	for _, tc := range []struct {
		name string
		m    *mq.Message
		want mq.ErrorCode
	}{
		{name: "binary header value", m: binary, want: mq.CodeUnsupported},
		{name: "reserved prefix", m: reserved, want: mq.CodeInvalidArgument},
		{name: "unframeable key", m: badKey, want: mq.CodeUnsupported},
		{name: "reply-to that is not a subject", m: &mq.Message{Properties: mq.Properties{ReplyTo: "a b"}}, want: mq.CodeUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := nq.Representable(tc.m)
			if err == nil {
				t.Fatalf("Representable accepted %s", tc.name)
			}
			if got := mq.CodeOf(err); got != tc.want {
				t.Fatalf("Representable(%s) code = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
	if err := nq.Representable(&mq.Message{Body: []byte("b")}); err != nil {
		t.Fatalf("a plain message was refused: %v", err)
	}
}

// TestDecodeIsDeterministic is a nats.Header is a map, so the
// publisher header order is gone; sorting is what stops the same message
// decoding to a different header order on every delivery.
func TestDecodeIsDeterministic(t *testing.T) {
	var msg natsgo.Msg
	msg.Subject = "h"
	msg.Header = natsgo.Header{}
	for _, k := range []string{"zeta", "alpha", "mu", "beta", "omega", "gamma", "delta"} {
		msg.Header.Set(k, "v")
	}
	var want []string
	for i := range 25 {
		var m mq.Message
		nq.Decode(&m, &msg, "")
		var order []string
		for k := range m.Headers.All() {
			order = append(order, k)
		}
		if i == 0 {
			want = order
			continue
		}
		if strings.Join(order, ",") != strings.Join(want, ",") {
			t.Fatalf("decode %d gave the header order %v, the first gave %v", i, order, want)
		}
	}
	if strings.Join(want, ",") != "alpha,beta,delta,gamma,mu,omega,zeta" {
		t.Fatalf("the decoded header order is %v, want it sorted", want)
	}
}
