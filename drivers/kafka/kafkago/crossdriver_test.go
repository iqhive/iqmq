package kafkago_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	franzkgo "github.com/twmb/franz-go/pkg/kgo"

	mq "bitbucket.org/iqhive/iqmq"
	franz "bitbucket.org/iqhive/iqmq/drivers/kafka"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago"
)

// The cross-driver equivalence suite, and it is this increment's answer to
// "reuse everything in drivers/kafka".
//
// Nothing was moved out of drivers/kafka. The pure, broker-level pieces the
// two Kafka drivers must agree about — the reserved header names, the topic
// grammar, the murmur2 partition assignment, the portable error codes — are
// DUPLICATED, for one reason: a kafkago module that imported drivers/kafka to
// share them would link franz-go into every binary that chose kafka-go, which
// is the opposite of what a second client library is for. Duplication without
// a check is drift, so the check is here instead, and it is stronger than
// sharing would have been — it compares the two drivers' OBSERVABLE behaviour
// rather than asserting they call the same function.
//
// These tests are the only place in this module that imports drivers/kafka,
// and they are the reason franz-go appears in go.mod at all. `GOWORK=off go
// build ./...` links neither.

// TestCrossDriverReservedHeaders pins every reserved header name to the
// franz-go driver's.
//
// A name that drifted would not be an error anywhere: an unrecognised
// reserved header is surfaced as an ordinary header, so a message published by
// one driver and consumed by the other would lose its identifier, its content
// type or its attempt count SILENTLY. Two Kafka drivers in one deployment
// write the same topics, so this is a wire compatibility contract.
func TestCrossDriverReservedHeaders(t *testing.T) {
	want := []string{
		franz.HeaderID,
		franz.HeaderContentType,
		franz.HeaderContentEncoding,
		franz.HeaderCorrelationID,
		franz.HeaderCausationID,
		franz.HeaderReplyTo,
		franz.HeaderCreated,
		franz.HeaderAttempts,
		franz.HeaderOriginTopic,
		franz.HeaderOriginSubscription,
		franz.HeaderOriginPartition,
		franz.HeaderOriginOffset,
		franz.HeaderFailure,
		franz.HeaderFirstDeliveredAt,
		franz.HeaderEnvelope,
	}
	got := kq.ReservedHeaders()
	if len(got) != len(want) {
		t.Fatalf("kafkago names %d reserved headers and the franz-go driver names %d:\n kafkago: %q\n franz-go: %q", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("reserved header %d: kafkago says %q, the franz-go driver says %q", i, got[i], want[i])
		}
	}
	// And the five provenance names are the CORE, which is where they
	// are canonical: drivers/kafka re-declares them too, and a driver that
	// invented its own would break the portable dead-letter contract as well
	// as the other driver.
	for _, pair := range [][2]string{
		{kq.HeaderOriginTopic, mq.HeaderOriginTopic},
		{kq.HeaderOriginSubscription, mq.HeaderOriginSubscription},
		{kq.HeaderAttempts, mq.HeaderAttempts},
		{kq.HeaderFailure, mq.HeaderFailure},
		{kq.HeaderFirstDeliveredAt, mq.HeaderFirstDeliveredAt},
	} {
		if pair[0] != pair[1] {
			t.Errorf("provenance header %q is not the core's %q", pair[0], pair[1])
		}
	}
}

// topicCorpus is the names the two drivers' topic grammars are compared on:
// the legal shapes, the illegal bytes, the reserved names, the length
// boundary and the pattern metacharacters.
var topicCorpus = []string{
	"", "a", "orders", "orders.v2", "orders_v2", "orders-v2", "ORDERS", "0",
	".", "..", "...", "a.b.c", "a/b", "a b", "a\tb", "a\x00b", "a\xffb",
	"a:b", "a@b", "a+b", "a%b", "a#b", "a,b", "a;b",
	"^orders.*$", "orders.*", "orders+", "orders?", "orders(1)", "orders[1]",
	"orders{1}", "a|b", "a\\b", "orders$",
	string(bytes.Repeat([]byte("a"), 248)),
	string(bytes.Repeat([]byte("a"), 249)),
	string(bytes.Repeat([]byte("a"), 250)),
	"ünïcödé", "\u00e9",
}

// TestCrossDriverTopicGrammar holds the two drivers to one answer for every
// topic name.
//
// Two Kafka drivers that disagreed about which names are legal would accept in
// one deployment what they refuse in the other, and the code they refuse
// with is what a caller's error handling switches on — so both the verdict and
// the code have to match.
func TestCrossDriverTopicGrammar(t *testing.T) {
	for _, name := range topicCorpus {
		a := franz.CheckTopic(name)
		b := kq.CheckTopic(name)
		if (a == nil) != (b == nil) {
			t.Errorf("CheckTopic(%q): the franz-go driver says %v and kafkago says %v", name, a, b)
			continue
		}
		if a == nil {
			continue
		}
		if ca, cb := mq.CodeOf(a), mq.CodeOf(b); ca != cb {
			t.Errorf("CheckTopic(%q): the franz-go driver refuses with %v and kafkago with %v", name, ca, cb)
		}
	}
	// The same for the prefixed form, which is what a deployment with a
	// TopicPrefix actually sends.
	for _, name := range topicCorpus {
		an, a := franz.Topic("acme.", name)
		bn, b := kq.Topic("acme.", name)
		if (a == nil) != (b == nil) || an != bn {
			t.Errorf("Topic(%q, %q): franz-go -> (%q, %v), kafkago -> (%q, %v)", "acme.", name, an, a, bn, b)
		}
	}
	// And the group-name grammar, which is the other name a subscription
	// carries onto the wire.
	for _, name := range []string{"", "g", "g-1", "g 1", "g\x00", "g\xff", string(bytes.Repeat([]byte("g"), 255)), string(bytes.Repeat([]byte("g"), 256))} {
		a, b := franz.CheckGroup(name), kq.CheckGroup(name)
		if (a == nil) != (b == nil) {
			t.Errorf("CheckGroup(%q): franz-go %v, kafkago %v", name, a, b)
		}
	}
}

// FuzzCrossDriverTopic is TestCrossDriverTopicGrammar over inputs nobody
// chose.
//
// The corpus above is what a person thought of. A topic name reaches these
// functions from a caller's own configuration, so the interesting inputs are
// the ones a person would not write: a name that is legal up to its last byte,
// a metacharacter in the middle of a legal name, a name exactly at the length
// bound with a prefix that pushes it over.
func FuzzCrossDriverTopic(f *testing.F) {
	for _, name := range topicCorpus {
		f.Add(name, "")
		f.Add(name, "acme.")
	}
	f.Fuzz(func(t *testing.T, name, prefix string) {
		an, a := franz.Topic(prefix, name)
		bn, b := kq.Topic(prefix, name)
		if (a == nil) != (b == nil) {
			t.Fatalf("Topic(%q, %q): the franz-go driver says %v and kafkago says %v; two Kafka drivers must accept and refuse the same names", prefix, name, a, b)
		}
		if a != nil {
			if ca, cb := mq.CodeOf(a), mq.CodeOf(b); ca != cb {
				t.Fatalf("Topic(%q, %q): refused with %v by franz-go and %v by kafkago", prefix, name, ca, cb)
			}
			return
		}
		if an != bn {
			t.Fatalf("Topic(%q, %q): franz-go produced %q and kafkago %q", prefix, name, an, bn)
		}
		// A name both accept must also survive the round trip back through
		// TopicOf, which is what a delivery's resolved topic is.
		if franz.TopicOf(prefix, an) != kq.TopicOf(prefix, bn) {
			t.Fatalf("TopicOf(%q, %q): franz-go %q, kafkago %q", prefix, an, franz.TopicOf(prefix, an), kq.TopicOf(prefix, bn))
		}
	})
}

// keyCorpus is the keys the two partitioners are compared on.
var keyCorpus = [][]byte{
	nil,
	{},
	[]byte("a"), []byte("b"), []byte("key-1"), []byte("key-2"),
	[]byte("orders/42"), []byte("\x00"), []byte("\xff\xfe"),
	[]byte("a-very-much-longer-partition-key-than-anybody-would-type"),
	bytes.Repeat([]byte{0x5a}, 1024),
}

// TestCrossDriverPartitioning holds the two drivers to the same partition for
// the same key.
//
// This is the sharpest of the four contracts. A key that hashed differently
// under the two drivers would split one key's records across two partitions,
// and the per-key ordering would break for every consumer of the topic —
// silently, and only in a deployment that happens to run both drivers or a
// Java producer alongside either. Both drivers compute it with the specification's
// measured murmur2, and this is what says so from the outside.
func TestCrossDriverPartitioning(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 7, 8, 16, 31, 64, 1024} {
		for _, key := range keyCorpus {
			if key == nil {
				// A keyless record has no order to keep and both drivers
				// spread it deliberately: franz-go sticks to one partition per
				// batch, kafkago rotates every BatchMaxRecords records.
				// There is nothing to compare.
				continue
			}
			p := &franz.TopicPartitioner{}
			want := p.Partition(&franzkgo.Record{Key: key}, n)
			got := kq.PartitionFor(key, n)
			if got != want {
				t.Errorf("key %q over %d partitions: the franz-go driver says %d and kafkago says %d", key, n, want, got)
			}
		}
	}
}

// FuzzCrossDriverPartitioning is TestCrossDriverPartitioning over keys nobody
// chose, which is where a hash disagreement would actually live: murmur2's
// tail handling depends on the key's length modulo four, so the interesting
// keys are the ones whose length is not a multiple of it.
func FuzzCrossDriverPartitioning(f *testing.F) {
	for _, key := range keyCorpus {
		if key != nil {
			f.Add(key, 4)
			f.Add(key, 7)
		}
	}
	f.Fuzz(func(t *testing.T, key []byte, n int) {
		if key == nil || n <= 0 || n > 1<<16 {
			return
		}
		p := &franz.TopicPartitioner{}
		want := p.Partition(&franzkgo.Record{Key: key}, n)
		if got := kq.PartitionFor(key, n); got != want {
			t.Fatalf("key %q over %d partitions: franz-go %d, kafkago %d", key, n, want, got)
		}
	})
}

// TestCrossDriverRoundTrip publishes with one driver and consumes with the
// other, in both directions, against one cluster.
//
// It is the end-to-end form of the three contracts above and it catches what
// none of them can on its own: a field that one driver writes into a native
// Kafka field and the other reads out of a header, an ordering of the reserved
// headers one of them depends on, a timestamp one of them stamps and the other
// reads as a creation time.
func TestCrossDriverRoundTrip(t *testing.T) {
	topic := newTopic("xdriver")
	cl := cluster(t, 4, []string{topic})
	addrs := cl.ListenAddrs()

	franzClient, err := franz.Open(t.Context(), franz.Config{
		Brokers:        addrs,
		CommitInterval: 50 * time.Millisecond,
		FetchMaxWait:   50 * time.Millisecond,
		RequestTimeout: 3 * time.Second,
		Linger:         -1,
	})
	if err != nil {
		t.Fatalf("opening the franz-go driver: %v", err)
	}
	t.Cleanup(func() { _ = franzClient.Close() })
	goClient := openOn(t, cl)

	created := time.Now().UTC().Truncate(time.Millisecond)
	msg := func() *mq.Message {
		m := &mq.Message{
			ID:   "id-7",
			Key:  []byte("key-7"),
			Body: []byte("body-7"),
			Properties: mq.Properties{
				ContentType:     "application/json",
				ContentEncoding: "gzip",
				CorrelationID:   "corr-7",
				CausationID:     "cause-7",
				ReplyTo:         "reply-7",
				CreatedAt:       created,
			},
		}
		m.Headers.Add("trace", []byte("t-7"))
		m.Headers.Add("trace", []byte("t-8"))
		m.Headers.Add("tenant", []byte("acme"))
		return m
	}
	check := func(t *testing.T, d *mq.Delivery) {
		t.Helper()
		if d.ID != "id-7" {
			t.Errorf("ID %q", d.ID)
		}
		if string(d.Key) != "key-7" || string(d.Body) != "body-7" {
			t.Errorf("key %q body %q", d.Key, d.Body)
		}
		p := d.Properties
		if p.ContentType != "application/json" || p.ContentEncoding != "gzip" ||
			p.CorrelationID != "corr-7" || p.CausationID != "cause-7" || p.ReplyTo != "reply-7" {
			t.Errorf("properties %+v", p)
		}
		if !p.CreatedAt.Equal(created) {
			t.Errorf("CreatedAt %v, want %v", p.CreatedAt, created)
		}
		var traces [][]byte
		for k, v := range d.Headers.All() {
			if k == "trace" {
				traces = append(traces, v)
			}
		}
		if len(traces) != 2 || string(traces[0]) != "t-7" || string(traces[1]) != "t-8" {
			t.Errorf("duplicate header values %q; Kafka headers are ordered and duplicate-preserving", traces)
		}
		if v, ok := d.Headers.Get("tenant"); !ok || string(v) != "acme" {
			t.Errorf("tenant header %q (present %v)", v, ok)
		}
	}

	t.Run("franz-go-publishes", func(t *testing.T) {
		cons := consumerOn(t, goClient, topic, "xd-1")
		publish(t, franzClient, topic, msg())
		d := receiveOne(t, cons, 20*time.Second)
		check(t, d)
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
	t.Run("kafkago-publishes", func(t *testing.T) {
		h, err := franzClient.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "xd-2"})
		if err != nil {
			t.Fatalf("opening the franz-go consumer: %v", err)
		}
		t.Cleanup(func() { _ = h.Close() })
		publish(t, goClient, topic, msg())
		d := receiveOne(t, h, 20*time.Second)
		check(t, d)
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
	t.Run("same-partition-for-the-same-key", func(t *testing.T) {
		// The partitioner contract, end to end: the same key published by the
		// two drivers must land on the same partition of the same topic.
		franzRec := &mq.Message{Key: []byte("sticky"), Body: []byte("f"), Receipt: &mq.PublishReceipt{}}
		goRec := &mq.Message{Key: []byte("sticky"), Body: []byte("g"), Receipt: &mq.PublishReceipt{}}
		fp, err := franzClient.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Receipts: true})
		if err != nil {
			t.Fatalf("franz-go publisher: %v", err)
		}
		defer func() { _ = fp.Close() }()
		gp, err := goClient.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Receipts: true})
		if err != nil {
			t.Fatalf("kafkago publisher: %v", err)
		}
		defer func() { _ = gp.Close() }()
		if err := fp.Publish(t.Context(), franzRec); err != nil {
			t.Fatalf("franz-go publish: %v", err)
		}
		if err := gp.Publish(t.Context(), goRec); err != nil {
			t.Fatalf("kafkago publish: %v", err)
		}
		if franzRec.Receipt.Partition != goRec.Receipt.Partition {
			t.Fatalf("the key %q landed on partition %d under the franz-go driver and %d under kafkago: one key's order is then split across two partitions ",
				"sticky", franzRec.Receipt.Partition, goRec.Receipt.Partition)
		}
		if want := int32(kq.PartitionFor([]byte("sticky"), 4)); franzRec.Receipt.Partition != want {
			t.Fatalf("the key landed on partition %d and the pure formula says %d", franzRec.Receipt.Partition, want)
		}
	})
}

// publishCodes is the portable classification both Kafka drivers must give the
// same broker answer on the publish path.
//
// It is written out by protocol error code rather than by client constant,
// because that is the thing the two clients share: kafka-go spells code 10
// MessageSizeTooLarge and franz-go spells it MessageTooLarge, and a caller's
// retry policy switches on neither — it switches on the portable code and on
// IsAmbiguous, which is what this pins.
var publishCodes = []struct {
	code      int16
	name      string
	want      mq.ErrorCode
	retryable bool
	ambiguous bool
}{
	{2, "CORRUPT_MESSAGE", mq.CodeCorruptMessage, false, false},
	{3, "UNKNOWN_TOPIC_OR_PARTITION", mq.CodeNotFound, true, false},
	{5, "LEADER_NOT_AVAILABLE", mq.CodeUnavailable, true, false},
	{6, "NOT_LEADER_OR_FOLLOWER", mq.CodeUnavailable, true, false},
	{7, "REQUEST_TIMED_OUT", mq.CodeSettlementUnknown, true, true},
	{8, "BROKER_NOT_AVAILABLE", mq.CodeUnavailable, true, false},
	{9, "REPLICA_NOT_AVAILABLE", mq.CodeUnavailable, true, false},
	{10, "MESSAGE_TOO_LARGE", mq.CodeMessageTooLarge, false, false},
	{13, "NETWORK_EXCEPTION", mq.CodeUnavailable, true, false},
	{14, "COORDINATOR_LOAD_IN_PROGRESS", mq.CodeUnavailable, true, false},
	{15, "COORDINATOR_NOT_AVAILABLE", mq.CodeUnavailable, true, false},
	{16, "NOT_COORDINATOR", mq.CodeUnavailable, true, false},
	{17, "INVALID_TOPIC_EXCEPTION", mq.CodeInvalidArgument, false, false},
	{18, "RECORD_LIST_TOO_LARGE", mq.CodeMessageTooLarge, false, false},
	{19, "NOT_ENOUGH_REPLICAS", mq.CodeUnavailable, true, false},
	{20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND", mq.CodeSettlementUnknown, true, true},
	{21, "INVALID_REQUIRED_ACKS", mq.CodeInvalidArgument, false, false},
	{29, "TOPIC_AUTHORIZATION_FAILED", mq.CodeForbidden, false, false},
	{30, "GROUP_AUTHORIZATION_FAILED", mq.CodeForbidden, false, false},
	{31, "CLUSTER_AUTHORIZATION_FAILED", mq.CodeForbidden, false, false},
	{33, "UNSUPPORTED_SASL_MECHANISM", mq.CodeUnauthorized, false, false},
	{34, "ILLEGAL_SASL_STATE", mq.CodeUnauthorized, false, false},
	{35, "UNSUPPORTED_VERSION", mq.CodeUnsupported, false, false},
	{43, "UNSUPPORTED_FOR_MESSAGE_FORMAT", mq.CodeUnsupported, false, false},
	{44, "POLICY_VIOLATION", mq.CodeForbidden, false, false},
	{45, "OUT_OF_ORDER_SEQUENCE_NUMBER", mq.CodeSettlementUnknown, false, true},
	{46, "DUPLICATE_SEQUENCE_NUMBER", mq.CodeSettlementUnknown, false, true},
	{47, "INVALID_PRODUCER_EPOCH", mq.CodeSettlementUnknown, false, true},
	{53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", mq.CodeForbidden, false, false},
	{54, "SECURITY_DISABLED", mq.CodeForbidden, false, false},
	{56, "KAFKA_STORAGE_ERROR", mq.CodeUnavailable, true, false},
	{58, "SASL_AUTHENTICATION_FAILED", mq.CodeUnauthorized, false, false},
	{59, "UNKNOWN_PRODUCER_ID", mq.CodeSettlementUnknown, false, true},
	{65, "DELEGATION_TOKEN_AUTHORIZATION_FAILED", mq.CodeForbidden, false, false},
	{73, "TOPIC_DELETION_DISABLED", mq.CodeForbidden, false, false},
	{74, "FENCED_LEADER_EPOCH", mq.CodeUnavailable, true, false},
	{75, "UNKNOWN_LEADER_EPOCH", mq.CodeUnavailable, true, false},
	{76, "UNSUPPORTED_COMPRESSION_TYPE", mq.CodeUnsupported, false, false},
	{80, "PREFERRED_LEADER_NOT_AVAILABLE", mq.CodeUnavailable, true, false},
	{81, "GROUP_MAX_SIZE_REACHED", mq.CodeResourceExhausted, true, false},
	{87, "INVALID_RECORD", mq.CodeCorruptMessage, false, false},
	{89, "THROTTLING_QUOTA_EXCEEDED", mq.CodeResourceExhausted, true, false},
	{90, "PRODUCER_FENCED", mq.CodeSettlementUnknown, false, true},
	{100, "UNKNOWN_TOPIC_ID", mq.CodeNotFound, true, false},
}

// TestCrossDriverErrorCodes holds the two drivers to one portable
// classification of the same broker answer.
//
// [BatchOutcome] is the route in, on both drivers: it is exported, pure, and
// maps through the publish rules with sentAlready true, which is exactly the
// path a produce response takes. A caller that switched brokers by switching
// drivers and found REQUEST_TIMED_OUT ambiguous under one and a plain failure
// under the other would retry into a duplicate under one of them.
func TestCrossDriverErrorCodes(t *testing.T) {
	for _, row := range publishCodes {
		gotGo := kq.BatchOutcome([]error{kafkaGoError(row.code)})[0]
		gotFranz := franz.BatchOutcome([]error{franzError(row.code)})[0]
		if gotGo == nil || gotFranz == nil {
			t.Errorf("code %d (%s): one driver mapped it to nil (kafkago %v, franz-go %v)", row.code, row.name, gotGo, gotFranz)
			continue
		}
		for _, c := range []struct {
			who string
			err error
		}{{"kafkago", gotGo}, {"franz-go", gotFranz}} {
			if code := mq.CodeOf(c.err); code != row.want {
				t.Errorf("code %d (%s): %s maps it to %v, want %v", row.code, row.name, c.who, code, row.want)
			}
			if amb := mq.IsAmbiguous(c.err); amb != row.ambiguous {
				t.Errorf("code %d (%s): %s reports IsAmbiguous %v, want %v", row.code, row.name, c.who, amb, row.ambiguous)
			}
			// The table's retryable column is each DRIVER's own judgement
			// about the protocol code, which lives on the *mq.OpError; it
			// used to be read through mq.IsRetryable, which now answers
			// "safe to repeat" -- that judgement after the precedence,
			// where ambiguity wins. Both are held
			// across the two drivers, so the two rows the precedence
			// changes (REQUEST_TIMED_OUT and
			// NOT_ENOUGH_REPLICAS_AFTER_APPEND, both retryable AND
			// ambiguous) still pin the flag as well as the verdict. If they
			// disagreed on either, a caller who switched clients would get
			// a different retry decision for one broker answer, which is
			// what this suite exists to prevent.
			var e *mq.OpError
			if !errors.As(c.err, &e) {
				t.Errorf("code %d (%s): %s did not map it to an *mq.OpError: %v", row.code, row.name, c.who, c.err)
			} else if e.Retryable != row.retryable {
				t.Errorf("code %d (%s): %s marks it retryable %v, want %v", row.code, row.name, c.who, e.Retryable, row.retryable)
			}
			if r, want := mq.IsRetryable(c.err), row.retryable && !row.ambiguous; r != want {
				t.Errorf("code %d (%s): %s reports IsRetryable %v, want %v (flag %v, ambiguous %v)",
					row.code, row.name, c.who, r, want, row.retryable, row.ambiguous)
			}
		}
	}
}

// TestCrossDriverErrorCodeSurvey walks every protocol error code and holds the
// two drivers to one answer wherever BOTH of them have one.
//
// It is the half [TestCrossDriverErrorCodes] cannot cover: that table is what
// a person wrote down, and the protocol has a hundred and six codes. A code
// only one driver classifies is fine and is logged — one driver being more
// specific than the other is not a contract violation — but a code both
// classify and classify DIFFERENTLY is, because a caller's handling would then
// depend on which client it was built with.
func TestCrossDriverErrorCodeSurvey(t *testing.T) {
	const lastCode = 106
	agreed, onlyGo, onlyFranz := 0, 0, 0
	for code := int16(1); code <= lastCode; code++ {
		gotGo := kq.BatchOutcome([]error{kafkaGoError(code)})[0]
		gotFranz := franz.BatchOutcome([]error{franzError(code)})[0]
		cg, cf := mq.CodeOf(gotGo), mq.CodeOf(gotFranz)
		// Ambiguity has to agree for EVERY code, named or not, and it is the
		// one half of the classification that is not a matter of taste: an
		// ambiguous outcome tells a caller "the record may be in the log,
		// retry only if you can bear a duplicate", and a definite refusal
		// tells it "nothing was written". Reporting the second as the first
		// is how a caller stops retrying something it should retry; the other
		// way round is how it duplicates. kafka-go's Error type satisfies
		// net.Error, which made driverkit's transport fallback call fifty-five
		// definite broker refusals ambiguous.
		if mq.IsAmbiguous(gotGo) != mq.IsAmbiguous(gotFranz) {
			t.Errorf("code %3d: kafkago reports IsAmbiguous %v and the franz-go driver %v (kafkago %v, franz-go %v)",
				code, mq.IsAmbiguous(gotGo), mq.IsAmbiguous(gotFranz), cg, cf)
		}
		switch {
		case cg == mq.CodeUnknown && cf == mq.CodeUnknown:
		case cg == mq.CodeUnknown:
			onlyFranz++
			t.Logf("code %3d: only the franz-go driver classifies it (%v)", code, cf)
		case cf == mq.CodeUnknown:
			onlyGo++
			t.Logf("code %3d: only kafkago classifies it (%v)", code, cg)
		case cg != cf:
			t.Errorf("code %3d: kafkago says %v and the franz-go driver says %v; one portable code per broker answer ", code, cg, cf)
		default:
			agreed++
		}
	}
	t.Logf("error codes: %d classified alike, %d only by kafkago, %d only by the franz-go driver", agreed, onlyGo, onlyFranz)
	if agreed < len(publishCodes) {
		t.Fatalf("only %d codes are classified alike, and the pinned table has %d: the survey has stopped covering the table", agreed, len(publishCodes))
	}
}

// TestCrossDriverNullAndEmptyBodiesSurvive is the observable distinction,
// in both directions and across both drivers.
//
// Kafka's record value is a NULLABLE byte string: a null value and a
// zero-length value are different bytes on the wire (length -1 against length
// 0), and the specification makes a caller able to tell them apart. This test exists
// because the first draft of this driver could NOT: it read records with
// [kgo.Reader], whose legacy `Conn`/`Batch` path calls `readNewBytes`, which
// returns nil for any length at or below zero — so an empty body came back as
// a nil one and the `publish/roundtrip-bodies` scenario failed. The
// information was never lost on the wire, only on the way in: kafka-go's
// modern protocol path sets the value's page reference whenever the length is
// at or above zero, and `protocol.ReadAll` answers nil only for a nil reader.
// So the driver fetches with [kgo.Client.Fetch] and the distinction holds.
//
// All four crossings are checked, because the defect was on one side of one of
// them and a test that checked only its own driver would have passed
// throughout.
func TestCrossDriverNullAndEmptyBodiesSurvive(t *testing.T) {
	topic := newTopic("emptybody")
	cl := cluster(t, 1, []string{topic})
	goClient := openOn(t, cl)
	franzClient, err := franz.Open(t.Context(), franz.Config{
		Brokers:        cl.ListenAddrs(),
		CommitInterval: 50 * time.Millisecond,
		FetchMaxWait:   50 * time.Millisecond,
		RequestTimeout: 3 * time.Second,
		Linger:         -1,
	})
	if err != nil {
		t.Fatalf("opening the franz-go driver: %v", err)
	}
	t.Cleanup(func() { _ = franzClient.Close() })

	consumerFor := func(t *testing.T, c *mq.Client, sub string) *mq.ConsumerHandle {
		t.Helper()
		h, cerr := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: sub})
		if cerr != nil {
			t.Fatalf("Consumer: %v", cerr)
		}
		t.Cleanup(func() { _ = h.Close() })
		return h
	}
	for _, cross := range []struct {
		name string
		from *mq.Client
		to   *mq.Client
		sub  string
	}{
		{"kafkago-to-kafkago", goClient, goClient, "eb-1"},
		{"kafkago-to-franz-go", goClient, franzClient, "eb-2"},
		{"franz-go-to-kafkago", franzClient, goClient, "eb-3"},
		{"franz-go-to-franz-go", franzClient, franzClient, "eb-4"},
	} {
		t.Run(cross.name, func(t *testing.T) {
			cons := consumerFor(t, cross.to, cross.sub)
			publish(t, cross.from, topic, &mq.Message{Key: []byte("n"), Body: nil})
			publish(t, cross.from, topic, &mq.Message{Key: []byte("e"), Body: []byte{}})
			publish(t, cross.from, topic, &mq.Message{Key: []byte("p"), Body: []byte("plain")})
			got := map[string][]byte{}
			nils := map[string]bool{}
			receiveN(t, cons, 3, 25*time.Second, func(d *mq.Delivery) error {
				got[string(d.Key)] = d.Body
				nils[string(d.Key)] = d.Body == nil
				return d.Ack()
			})
			if !nils["n"] {
				t.Errorf("a nil body came back as %d non-nil bytes; the specification makes the distinction observable", len(got["n"]))
			}
			if nils["e"] {
				t.Error("an EMPTY body came back as a nil one: the null-versus-zero-length distinction was lost on the way in, which is what kgo.Reader does and kgo.Client.Fetch does not ")
			}
			if len(got["e"]) != 0 {
				t.Errorf("the empty body came back as %q", got["e"])
			}
			if string(got["p"]) != "plain" {
				t.Errorf("the plain body came back as %q", got["p"])
			}
		})
	}
}
