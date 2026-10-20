package nsq

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

// TestClientConfigNeverLetsGoNsqSettleAMessage holds the three settings in
// [clientConfig] that are NOT tuning, and whose go-nsq defaults each break a claim this driver makes.
//
// Every one of them is a default that does something reasonable for a program
// using go-nsq directly and wrong for a driver underneath iqmq's core:
//
//	MaxAttempts        go-nsq's default is 5, and what it does when a
//	                   message's Attempts passes it is call FINISH and log a
//	                   warning. That is an accepted message DISCARDED by the
//	                   client library while the core still believed it was
//	                   retrying -- a silent loss, which the specification makes a
//	                   settlement disqualifier. Giving up is the core's
//	                   decision and it dead-letters rather than dropping.
//	MaxBackoffDuration go-nsq's default backoff sets RDY to zero for up to two
//	                   minutes after a requeue, so the WHOLE subscription
//	                   stops receiving because one message failed. The specification gives
//	                   redelivery policy to one layer; a client library
//	                   pausing the consumer underneath the core makes the
//	                   core's retry schedule a fiction.
//	BackoffMultiplier  the other half of the same pause: zeroing the duration
//	                   alone leaves the multiplier scaling it back up.
//
// DefaultRequeueDelay is here for the same reason: a delay go-nsq chose is a
// second redelivery policy beside the core's.
//
// This is the whole of what the function is for, and none of it needs an nsqd:
// the assertions read the *gonsq.Config the driver would hand the library.
func TestClientConfigNeverLetsGoNsqSettleAMessage(t *testing.T) {
	t.Parallel()
	gc, err := clientConfig(Config{Addrs: []string{"127.0.0.1:4150"}}.resolve(), 8, time.Minute)
	if err != nil {
		t.Fatalf("clientConfig: %v", err)
	}
	if gc.MaxAttempts != 0 {
		t.Errorf("MaxAttempts is %d, want 0: go-nsq FINISHes a message whose Attempts passes this and logs a "+
			"warning, which loses an accepted message the core was still retrying ", gc.MaxAttempts)
	}
	if gc.MaxBackoffDuration != 0 {
		t.Errorf("MaxBackoffDuration is %v, want 0: go-nsq's backoff sets RDY to zero for up to two minutes, so "+
			"the whole subscription stops receiving because ONE message failed ", gc.MaxBackoffDuration)
	}
	if gc.BackoffMultiplier != 0 {
		t.Errorf("BackoffMultiplier is %v, want 0: it scales MaxBackoffDuration back up, so zeroing the duration "+
			"alone does not stop go-nsq pausing the consumer", gc.BackoffMultiplier)
	}
	if gc.DefaultRequeueDelay != 0 {
		t.Errorf("DefaultRequeueDelay is %v, want 0: a delay go-nsq chose is a second redelivery policy beside "+
			"the core's ", gc.DefaultRequeueDelay)
	}
	if gc.MaxInFlight != 8 {
		t.Errorf("MaxInFlight is %d, want the 8 the caller asked for", gc.MaxInFlight)
	}
}

// TestAProducersConfigCarriesNoMessageTimeout is, which cost
// a whole scenario before it was understood.
//
// msg_timeout is negotiated in the IDENTIFY handshake by EVERY client,
// receiving or not, and nsqd refuses one below its --min-msg-timeout. So a
// producer built with the consumer's ack deadline has its CONNECTION refused
// for a value it has no use for, which arrives as
//
//	publish failed: failed to IDENTIFY
//
// and is nothing like a message-timeout error. A zero deadline leaves nsqd's
// own --msg-timeout in place, which is what a producer wants; the consumer's
// configuration is built separately and does carry the deadline, because that
// is what a delivery's lease is.
func TestAProducersConfigCarriesNoMessageTimeout(t *testing.T) {
	t.Parallel()
	cfg := Config{Addrs: []string{"127.0.0.1:4150"}}.resolve()
	producer, err := clientConfig(cfg, 1, 0)
	if err != nil {
		t.Fatalf("clientConfig for a producer: %v", err)
	}
	if producer.MsgTimeout != 0 {
		t.Errorf("a producer's MsgTimeout is %v, want 0: nsqd refuses a msg_timeout below its --min-msg-timeout "+
			"in the IDENTIFY handshake, and the refusal arrives as \"failed to IDENTIFY\" on the first publish "+
			"", producer.MsgTimeout)
	}
	consumer, err := clientConfig(cfg, 4, 90*time.Second)
	if err != nil {
		t.Fatalf("clientConfig for a consumer: %v", err)
	}
	if consumer.MsgTimeout != 90*time.Second {
		t.Errorf("a consumer's MsgTimeout is %v, want the 90s deadline it was given: msg_timeout IS the "+
			"delivery's lease", consumer.MsgTimeout)
	}
}

// TestClientConfigCarriesTheConfiguredIdentityAndTransport holds the
// pass-through half of [clientConfig], which is the only place this driver
// decides how go-nsq behaves.
//
// The deadlines matter because HeartbeatInterval must stay below ReadTimeout
// for go-nsq to accept the pair at all, and the credential matters because it
// is the one field that has to be REVEALED on the way through: a dsn.Secret
// renders as a placeholder, and handing go-nsq the placeholder would make
// every AUTH handshake fail with a secret that looks present in every log.
func TestClientConfigCarriesTheConfiguredIdentityAndTransport(t *testing.T) {
	t.Parallel()
	tlsCfg := &tls.Config{ServerName: "nsqd.example", MinVersion: tls.VersionTLS12}
	cfg := Config{
		Addrs:               []string{"127.0.0.1:4150"},
		ClientID:            "iqmq-1",
		Hostname:            "host.example",
		UserAgent:           "iqmq/test",
		AuthSecret:          dsn.Secret("s3cr3t"),
		TLS:                 tlsCfg,
		Snappy:              true,
		MaxRequeueDelay:     30 * time.Minute,
		DialTimeout:         3 * time.Second,
		ReadTimeout:         50 * time.Second,
		WriteTimeout:        4 * time.Second,
		HeartbeatInterval:   20 * time.Second,
		LookupdPollInterval: 7 * time.Second,
		OutputBufferTimeout: 40 * time.Millisecond,
	}.resolve()
	gc, err := clientConfig(cfg, 1, 0)
	if err != nil {
		t.Fatalf("clientConfig: %v", err)
	}
	for _, tc := range []struct{ field, got, want string }{
		{"ClientID", gc.ClientID, "iqmq-1"},
		{"Hostname", gc.Hostname, "host.example"},
		{"UserAgent", gc.UserAgent, "iqmq/test"},
		{"AuthSecret", gc.AuthSecret, "s3cr3t"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s is %q, want %q", tc.field, tc.got, tc.want)
		}
	}
	if !gc.TlsV1 {
		t.Error("a Config with a TLS configuration produced TlsV1 false, so the connection would be plaintext " +
			"while the caller believes it asked for TLS")
	}
	if gc.TlsConfig != tlsCfg {
		t.Errorf("TlsConfig is %v, want the caller's own: a configuration of the driver's making would lose the "+
			"caller's roots and its minimum version", gc.TlsConfig)
	}
	if !gc.Snappy {
		t.Error("Snappy was not carried through, so nsqd would not compress")
	}
	for _, tc := range []struct {
		field     string
		got, want time.Duration
	}{
		{"MaxRequeueDelay", gc.MaxRequeueDelay, 30 * time.Minute},
		{"DialTimeout", gc.DialTimeout, 3 * time.Second},
		{"ReadTimeout", gc.ReadTimeout, 50 * time.Second},
		{"WriteTimeout", gc.WriteTimeout, 4 * time.Second},
		{"HeartbeatInterval", gc.HeartbeatInterval, 20 * time.Second},
		{"LookupdPollInterval", gc.LookupdPollInterval, 7 * time.Second},
		{"OutputBufferTimeout", gc.OutputBufferTimeout, 40 * time.Millisecond},
	} {
		if tc.got != tc.want {
			t.Errorf("%s is %v, want %v", tc.field, tc.got, tc.want)
		}
	}
}

// TestDeflateIsOnlyReadWhenItIsAskedFor holds the one conditional pair in
// [clientConfig]: DeflateLevel is resolved to 6 for every configuration, and
// asking nsqd for a deflate level it was not told to expect is a connection
// refusal rather than a slower connection.
func TestDeflateIsOnlyReadWhenItIsAskedFor(t *testing.T) {
	t.Parallel()
	off, err := clientConfig(Config{Addrs: []string{"127.0.0.1:4150"}}.resolve(), 1, 0)
	if err != nil {
		t.Fatalf("clientConfig: %v", err)
	}
	if off.Deflate {
		t.Error("a Config that did not ask for deflate produced Deflate true, which nsqd refuses unless it was " +
			"started with --deflate")
	}
	on, err := clientConfig(Config{Addrs: []string{"127.0.0.1:4150"}, Deflate: true, DeflateLevel: 3}.resolve(), 1, 0)
	if err != nil {
		t.Fatalf("clientConfig with deflate: %v", err)
	}
	if !on.Deflate {
		t.Fatal("a Config that asked for deflate produced Deflate false")
	}
	if on.DeflateLevel != 3 {
		t.Errorf("DeflateLevel is %d, want the 3 that was configured", on.DeflateLevel)
	}
}

// TestClientConfigReturnsGoNsqsOwnRefusal is the last line of [clientConfig]
// and the reason it is not swallowed.
//
// go-nsq's Validate names the FIELD it rejected. A driver that replaced the
// error with one of its own would send an operator looking at its Config for a
// setting go-nsq had already told them about by name, so the error travels
// unchanged.
func TestClientConfigReturnsGoNsqsOwnRefusal(t *testing.T) {
	t.Parallel()
	// A deflate level outside 1..9 is go-nsq's own range check, and it is
	// reachable here because Config.Validate does not police the level of a
	// Config that was never validated -- which is the case clientConfig is
	// the last guard for.
	_, err := clientConfig(Config{Addrs: []string{"127.0.0.1:4150"}, Deflate: true, DeflateLevel: 99}.resolve(), 1, 0)
	if err == nil {
		t.Fatal("clientConfig accepted a deflate level of 99; go-nsq refuses it and the connection would fail " +
			"instead, with the refusal arriving at the first publish")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "deflate") {
		t.Errorf("the refusal is %q and does not name the field; go-nsq's own message does, and rewriting it "+
			"loses the name", err)
	}
}

// TestPublicationNeedsAnNsqdAddress is the configuration that can consume and
// cannot publish, and the place it finds out.
//
// Publication goes to an nsqd directly. nsqlookupd is a discovery service for
// CONSUMERS and cannot be published through, so a Config carrying only lookupd
// addresses is legal — it consumes — and a producer built from it is refused
// here rather than dialling something that cannot take a PUB.
func TestPublicationNeedsAnNsqdAddress(t *testing.T) {
	t.Parallel()
	_, err := newProducer(Config{LookupdAddrs: []string{"127.0.0.1:4161"}})
	if !errors.Is(err, errNoPublishAddress) {
		t.Fatalf("newProducer with no nsqd address reported %v, want errNoPublishAddress", err)
	}
	if code := mq.CodeOf(err); code != mq.CodeInvalidConfig {
		t.Errorf("the refusal reports %v, want %v: this is a configuration that cannot be published through and "+
			"not a broker that is down, and an operator who retries it waits for ever", code, mq.CodeInvalidConfig)
	}
}

// TestAProducerHoldsOneEndpointPerAddressAndMovesBetweenThem holds the specification's
// division of labour on the publish path.
//
// There is no failover and no retry inside the driver: a driver that tried the
// next endpoint on a failure would turn one publication into two attempts with
// the core's own attempt count blind to the second. What it does instead is
// move the cursor, so the core's next attempt lands on the next endpoint
// anyway.
//
// Addr reports the endpoint the NEXT publication would use, which is the one an
// operator needs when a publication has just failed. Reporting the one that
// just failed instead would be the same string in the common case and wrong
// exactly when it matters.
func TestAProducerHoldsOneEndpointPerAddressAndMovesBetweenThem(t *testing.T) {
	t.Parallel()
	p, err := newProducer(Config{Addrs: []string{"127.0.0.1:4150", "127.0.0.1:4250"}}.resolve())
	if err != nil {
		t.Fatalf("newProducer: %v", err)
	}
	defer p.Close()
	if len(p.nsqds) != 2 {
		t.Fatalf("a Config naming two nsqd addresses produced %d producer(s); one per address is what makes the "+
			"cursor mean anything", len(p.nsqds))
	}
	if got := p.Addr(); got != "127.0.0.1:4150" {
		t.Errorf("before any publication Addr is %q, want the first address", got)
	}
	first, second := p.pick().String(), p.pick().String()
	if first != "127.0.0.1:4150" || second != "127.0.0.1:4250" {
		t.Errorf("two picks chose %q then %q, want the two addresses in turn: a cursor that did not move would "+
			"make the core's next attempt land on the endpoint that just failed", first, second)
	}
	if got := p.Addr(); got != "127.0.0.1:4150" {
		t.Errorf("after two picks over two endpoints Addr is %q, want the first again", got)
	}
	// Close is idempotent because the core may close a backend whose sink has
	// already been released, and go-nsq's Stop is not safe twice.
	p.Close()
	p.Close()
}

// TestASingleEndpointProducerNeedsNoCursor is the branch [producer.pick] takes
// for the common deployment, and it is separate because the arithmetic below it
// is what a one-element slice makes pointless rather than wrong.
func TestASingleEndpointProducerNeedsNoCursor(t *testing.T) {
	t.Parallel()
	p, err := newProducer(Config{Addrs: []string{"127.0.0.1:4150"}}.resolve())
	if err != nil {
		t.Fatalf("newProducer: %v", err)
	}
	defer p.Close()
	for i := range 3 {
		if got := p.pick().String(); got != "127.0.0.1:4150" {
			t.Fatalf("pick %d chose %q, want the only address", i, got)
		}
	}
	if got := p.Addr(); got != "127.0.0.1:4150" {
		t.Errorf("Addr is %q, want the only address", got)
	}
}

// TestAProducerRefusesAConfigurationGoNsqRefuses is [newProducer]'s first
// error path: the client configuration is built before any endpoint is, so a
// refusal costs no connection and names go-nsq's field.
func TestAProducerRefusesAConfigurationGoNsqRefuses(t *testing.T) {
	t.Parallel()
	if _, err := newProducer(Config{
		Addrs: []string{"127.0.0.1:4150"}, Deflate: true, DeflateLevel: 99,
	}.resolve()); err == nil {
		t.Fatal("newProducer accepted a configuration go-nsq's Validate refuses")
	}
}

// TestAConsumerIsBuiltWithTheDeadlineAsItsLease pins the other caller of
// [clientConfig] and the name checks go-nsq makes before any connection.
//
// A channel name nsqd would refuse is refused here, which is the difference
// between an error at Open and a subscription that exists and never receives.
func TestAConsumerIsBuiltWithTheDeadlineAsItsLease(t *testing.T) {
	t.Parallel()
	cfg := Config{Addrs: []string{"127.0.0.1:4150"}}.resolve()
	c, err := newConsumer(cfg, "orders", "workers", 4, 45*time.Second)
	if err != nil {
		t.Fatalf("newConsumer: %v", err)
	}
	if c.c == nil {
		t.Fatal("newConsumer returned a consumer with no go-nsq consumer behind it")
	}
	// The shutdown is the driver's own, in the order source.Close uses it: a
	// handler first, because go-nsq only reaches its exit when the last
	// handler goroutine returns, then Stop and a bounded WaitStopped.
	// The specification forbids a Close that waits indefinitely, and a consumer built
	// without a handler never finishes stopping at all -- which is why the
	// order is the contract and not tidiness.
	c.AddConcurrentHandlers(gonsq.HandlerFunc(func(*gonsq.Message) error { return nil }), 1)
	c.Stop()
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.WaitStopped(stopCtx); err != nil {
		t.Fatal("the consumer did not finish stopping; go-nsq reaches its exit only when the last handler " +
			"goroutine returns, so a Close that does not get there leaves the rdyLoop running for ever")
	}
	if _, err := newConsumer(cfg, "orders", "not a channel name", 4, 45*time.Second); err == nil {
		t.Error("newConsumer accepted a channel name nsqd refuses; the refusal would then arrive as a " +
			"subscription that connects and never receives")
	}
	if _, err := newConsumer(Config{
		Addrs: []string{"127.0.0.1:4150"}, Deflate: true, DeflateLevel: 99,
	}.resolve(), "orders", "workers", 4, 45*time.Second); err == nil {
		t.Error("newConsumer accepted a configuration go-nsq's Validate refuses")
	}
}

// TestNoPublishCommandIsWrittenUnderADoneContext is the requirement on the
// three publish verbs, checked at the one place all three share.
//
// The guard is before the send and not after it, which is what makes the answer
// a DEFINITE refusal: nothing was written, so the caller may retry without
// risking a duplicate. A cancellation noticed after the write is the ambiguous
// case and is [TestAPublishCancelledInFlightIsAmbiguous].
func TestNoPublishCommandIsWrittenUnderADoneContext(t *testing.T) {
	t.Parallel()
	p, err := newProducer(Config{Addrs: []string{"127.0.0.1:4150"}}.resolve())
	if err != nil {
		t.Fatalf("newProducer: %v", err)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Publish", func() error { return p.Publish(ctx, "orders", []byte("x")) }},
		{"MultiPublish", func() error { return p.MultiPublish(ctx, "orders", [][]byte{[]byte("x")}) }},
		{"DeferredPublish", func() error { return p.DeferredPublish(ctx, "orders", time.Second, []byte("x")) }},
		{"Ping", func() error { return p.Ping(ctx) }},
	} {
		err := tc.call()
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s under a cancelled context reported %v, want context.Canceled", tc.name, err)
		}
		if mq.IsAmbiguous(err) {
			t.Errorf("%s under a cancelled context is AMBIGUOUS: nothing was written, so reporting an unknown "+
				"outcome would stop a caller retrying a publication the broker never saw", tc.name)
		}
	}
}

// TestAPublishCancelledInFlightIsAmbiguous is the other half, and the one
// exists for.
//
// The command has been written and nsqd's answer has not been read. nsqd may
// well have taken it, so reporting "did not publish" would make a duplicate
// look like a loss — the disqualifier from the other direction. The send is
// stubbed rather than real because the distinction is about what the driver
// does with a context that fires AFTER the write, and an nsqd would only make
// the write slower.
func TestAPublishCancelledInFlightIsAmbiguous(t *testing.T) {
	t.Parallel()
	p, err := newProducer(Config{Addrs: []string{"127.0.0.1:4150"}}.resolve())
	if err != nil {
		t.Fatalf("newProducer: %v", err)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	got := p.await(ctx, func(*gonsq.Producer, chan *gonsq.ProducerTransaction) error {
		// The frame is on the wire; the answer has not arrived.
		cancel()
		return nil
	})
	// The raw answer is the driver's OWN error and is not yet portable: it
	// becomes ambiguous to mq.IsAmbiguous when the rule table maps it, and the
	// predicate the table consults is [ambiguous]. Asserting mq.IsAmbiguous
	// here would assert something about the mapper instead, which
	// [TestAnAmbiguousPublishIsRecognisedByTheErrorMapper] does.
	if !ambiguous(got) {
		t.Fatalf("a publish cancelled after the command was written reported %v, which this driver's own "+
			"ambiguity predicate does not recognise: the rule table would then map it as a definite failure, a "+
			"caller would retry it, and the broker would hold two ", got)
	}
	if !errors.Is(got, context.Canceled) {
		t.Errorf("the ambiguous answer %v does not unwrap to context.Canceled, which the specification requires to stay "+
			"discoverable through whatever a driver wraps it in", got)
	}
	if !strings.Contains(got.Error(), "answer was not read") {
		t.Errorf("the ambiguous answer reads %q and does not say what is unknown; an operator reading it has to "+
			"decide whether to republish", got)
	}
}

// TestASendThatWroteNothingIsADefiniteFailure is [producer.await]'s middle
// branch: go-nsq refused before the frame left, so the outcome is known.
func TestASendThatWroteNothingIsADefiniteFailure(t *testing.T) {
	t.Parallel()
	p, err := newProducer(Config{Addrs: []string{"127.0.0.1:4150"}}.resolve())
	if err != nil {
		t.Fatalf("newProducer: %v", err)
	}
	defer p.Close()
	stopped := errors.New("producer stopped")
	got := p.await(context.Background(), func(*gonsq.Producer, chan *gonsq.ProducerTransaction) error {
		return stopped
	})
	if !errors.Is(got, stopped) {
		t.Fatalf("await reported %v, want go-nsq's own refusal", got)
	}
	if mq.IsAmbiguous(got) {
		t.Error("a send that wrote nothing is reported as ambiguous, which stops a caller retrying a publication " +
			"the broker never saw")
	}
}

// TestAnAnsweredPublishReportsTheTransactionsOwnResult is the success branch,
// and it is worth a case because the transaction's Error is the only thing
// distinguishing an accepted publication from a refused one.
func TestAnAnsweredPublishReportsTheTransactionsOwnResult(t *testing.T) {
	t.Parallel()
	p, err := newProducer(Config{Addrs: []string{"127.0.0.1:4150"}}.resolve())
	if err != nil {
		t.Fatalf("newProducer: %v", err)
	}
	defer p.Close()
	refused := errors.New("E_BAD_TOPIC")
	for _, tc := range []struct {
		name   string
		answer error
	}{
		{"accepted", nil},
		{"refused", refused},
	} {
		got := p.await(context.Background(), func(_ *gonsq.Producer, done chan *gonsq.ProducerTransaction) error {
			done <- &gonsq.ProducerTransaction{Error: tc.answer}
			return nil
		})
		if !errors.Is(got, tc.answer) {
			t.Errorf("a %s publication reported %v, want %v", tc.name, got, tc.answer)
		}
	}
}

// TestPingNeedsAnEndpoint is [producer.Ping]'s second guard. A health probe
// with nowhere to probe is a configuration answer and not an unhealthy broker,
// and the health surface reports the two differently.
func TestPingNeedsAnEndpoint(t *testing.T) {
	t.Parallel()
	p := &producer{}
	if err := p.Ping(context.Background()); !errors.Is(err, errNoPublishAddress) {
		t.Errorf("Ping with no endpoint reported %v, want errNoPublishAddress: an operator told the broker is "+
			"unavailable looks at the broker, and this is the Config", err)
	}
	if got := p.Addr(); got != "" {
		t.Errorf("Addr with no endpoint is %q, want the empty string", got)
	}
}

// TestAnAmbiguousPublishIsRecognisedByTheErrorMapper closes the loop between
// [ambiguousError] and the rule table: the mapper sets its flag from
// [ambiguous] rather than from a type switch of its own, so an ambiguous
// outcome that the predicate does not recognise is one the table maps as
// definite.
func TestAnAmbiguousPublishIsRecognisedByTheErrorMapper(t *testing.T) {
	t.Parallel()
	own := &ambiguousError{cause: context.DeadlineExceeded}
	if !ambiguous(own) {
		t.Fatal("ambiguous does not recognise this driver's own ambiguous error, so the rule table would map it " +
			"as a definite failure and a caller would republish")
	}
	if ambiguous(context.DeadlineExceeded) {
		t.Error("ambiguous claims a bare context deadline, which on a publish that never left is definite")
	}
	mapped := newErrorMapper().Map(mq.OpPublish, "orders", "", own)
	if !mq.IsAmbiguous(mapped) {
		t.Errorf("the rule table mapped this driver's ambiguous publish to %v, which is not ambiguous", mapped)
	}
	if code := mq.CodeOf(mapped); code != mq.CodeAmbiguous {
		t.Errorf("the mapped code is %v, want %v", code, mq.CodeAmbiguous)
	}
	if !errors.Is(mapped, context.DeadlineExceeded) {
		t.Errorf("the mapping lost the cause; the specification requires context.DeadlineExceeded to stay discoverable")
	}
}
