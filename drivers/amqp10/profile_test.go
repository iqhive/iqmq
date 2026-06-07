package amqp10_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// The seam's own tests: what a profile must supply, what it may leave out, and
// what happens when a broker-specific module gets one of them wrong.

// TestProfileValidateRefusesAnUnusableProfile is the specification's "validate config before
// starting background work" applied to the seam itself.
//
// A profile with no SourceAddress is a programming error in the
// broker-specific module, and finding it after a ten-second dial and one attach
// is finding it in the wrong place.
func TestProfileValidateRefusesAnUnusableProfile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*aq.Profile)
	}{
		{"no driver name", func(p *aq.Profile) { p.Driver = "" }},
		{"no SinkAddress", func(p *aq.Profile) { p.SinkAddress = nil }},
		{"no SourceAddress", func(p *aq.Profile) { p.SourceAddress = nil }},
		{"a slot outside the enumeration", func(p *aq.Profile) { p.Delay.Slot = aq.Slot(99) }},
		{"an attempt source outside the enumeration", func(p *aq.Profile) {
			p.Attempt.Source = aq.AttemptSource(99)
		}},
		{"an annotation attempt source with no key", func(p *aq.Profile) {
			p.Attempt = aq.AttemptRule{Source: aq.AttemptFromAnnotation}
		}},
		{"a delay slot with no key", func(p *aq.Profile) { p.Delay.Key = "" }},
		{"a dedup slot with no key", func(p *aq.Profile) { p.Dedup.Key = "" }},
		{"a nack-delay slot with no key", func(p *aq.Profile) { p.NackDelay.Key = "" }},
		{"a durable claim with no boundary stated", func(p *aq.Profile) { p.Durable.Note = "" }},
		{"a negative delay ceiling", func(p *aq.Profile) { p.Delay.Max = -time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testProfile()
			tc.break_(&p)
			err := p.Validate()
			if err == nil {
				t.Fatalf("a profile with %s validated; the specification requires the refusal before any background work", tc.name)
			}
			assertCode(t, err, mq.CodeInvalidConfig, "a profile with "+tc.name)
			// And Open refuses it too, so a module that never calls Validate
			// still cannot dial with it.
			if _, oerr := aq.BackendWithDialer(context.Background(), p, aq.Config{}, newPeer()); oerr == nil {
				t.Fatalf("Open accepted a profile with %s", tc.name)
			}
		})
	}
}

// TestProfileValidateAcceptsTheMinimum is the other half: a profile that fills
// only the required fields works, and every optional rule's zero value is the
// honest "this broker has no way to express that" rather than a guess.
func TestProfileValidateAcceptsTheMinimum(t *testing.T) {
	p := bareProfile()
	if err := p.Validate(); err != nil {
		t.Fatalf("the minimal profile of the documentation was refused: %v", err)
	}
	f := open(t, p, aq.Config{})
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	// A plain publication works.
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil); err != nil {
		t.Fatalf("a plain publication on the minimal profile: %v", err)
	}
	// And every control the profile said nothing about is refused rather than
	// guessed at.
	for _, tc := range []struct {
		name string
		opts mq.PublishOptions
	}{
		{"a delay", mq.PublishOptions{Delay: time.Minute}},
		{"a delivery time", mq.PublishOptions{DeliverAt: time.Now().Add(time.Minute)}},
		{"a de-duplication key", mq.PublishOptions{DedupKey: "k"}},
	} {
		err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, &tc.opts)
		assertIs(t, err, mq.ErrUnsupported, tc.name+" on a profile that names no rule for it")
	}
	// ConfirmDurable fails when the SINK is opened, which is the rule.
	_, err := f.b.OpenSink(context.Background(), mq.SinkConfig{Topic: "topic", Confirmation: mq.ConfirmDurable})
	assertIs(t, err, mq.ErrUnsupported, "ConfirmDurable on a profile with no durable boundary")
}

// TestProfileAddressingDecidesTheLinkAddresses is the seam's most important
// property, and the one rests on: what a sink's target and a source's
// source really are is the profile's answer and nothing else's.
//
// The three shapes below are the three real brokers this core was written for.
// A core that had picked any one of them would be wrong on the other two.
func TestProfileAddressingDecidesTheLinkAddresses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sink       func(string) (string, error)
		source     func(string, string) (string, error)
		wantSink   string
		wantSource string
	}{
		{
			name:       "Artemis: a fully qualified queue name",
			sink:       func(topic string) (string, error) { return topic, nil },
			source:     func(topic, sub string) (string, error) { return topic + "::" + sub, nil },
			wantSink:   "orders",
			wantSource: "orders::workers",
		},
		{
			name:       "RabbitMQ 4: exchange and queue paths",
			sink:       func(topic string) (string, error) { return "/exchanges/amq.direct/" + topic, nil },
			source:     func(topic, sub string) (string, error) { return "/queues/" + topic + "." + sub, nil },
			wantSink:   "/exchanges/amq.direct/orders",
			wantSource: "/queues/orders.workers",
		},
		{
			name: "ActiveMQ Classic: a virtual topic",
			sink: func(topic string) (string, error) { return "topic://VirtualTopic." + topic, nil },
			source: func(topic, sub string) (string, error) {
				return "queue://Consumer." + sub + ".VirtualTopic." + topic, nil
			},
			wantSink:   "topic://VirtualTopic.orders",
			wantSource: "queue://Consumer.workers.VirtualTopic.orders",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testProfile()
			p.SinkAddress, p.SourceAddress = tc.sink, tc.source
			f := open(t, p, aq.Config{})
			snk := f.sink(mq.SinkConfig{Topic: "orders"})
			if got := snk.Address(); got != tc.wantSink {
				t.Fatalf("the sink attached to %q, want %q", got, tc.wantSink)
			}
			src := f.source(mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 2})
			if got := src.Address(); got != tc.wantSource {
				t.Fatalf("the source attached to %q, want %q", got, tc.wantSource)
			}
			// And the address really reaches the peer's ATTACH, which is what
			// a naming test has to prove: an address the driver computed and
			// did not use is an address nobody honours.
			if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil); err != nil {
				t.Fatalf("publishing: %v", err)
			}
			if got := f.sender(budget).Address(); got != tc.wantSink {
				t.Fatalf("the peer saw the sending link attach to %q, want %q", got, tc.wantSink)
			}
		})
	}
}

// TestProfileAddressRefusalReachesTheCaller holds the other half of the
// addressing seam: a topic the profile cannot map is the profile's refusal, and
// the core returns it unchanged so that errors.Is reaches whatever sentinel the
// profile wrapped.
func TestProfileAddressRefusalReachesTheCaller(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	_, err := f.b.OpenSink(context.Background(), mq.SinkConfig{Topic: ""})
	assertIs(t, err, mq.ErrInvalidTopic, "a topic the profile refuses")
	_, err = f.b.OpenSource(context.Background(), mq.SourceConfig{Topic: "t", Subscription: "", Prefetch: 1})
	assertIs(t, err, mq.ErrInvalidTopic, "a subscription the profile refuses")
}

// TestProfileBrokerGateRefusesAtOpen is the version gate, which is the
// profile's rule and not the core's: RabbitMQ's AMQP 1.0 endpoint exists from
// 4.0 and its 3.13 plugin is a different protocol, so "refuse below 4.0"
// belongs to the RabbitMQ profile.
func TestProfileBrokerGateRefusesAtOpen(t *testing.T) {
	tooOld := errors.New("this driver needs RabbitMQ 4.0 or later")
	p := testProfile()
	p.CheckBroker = func(f aq.Facts) error {
		if strings.HasPrefix(f.Version, "3.") {
			return tooOld
		}
		return nil
	}
	// A peer that says 3.13 is refused, and the refusal is the profile's own
	// error so that a caller sees the reason rather than a generic code.
	peer := newPeerWith(map[string]any{"product": "rabbitmq", "version": "3.13.7"})
	if _, err := aq.BackendWithDialer(context.Background(), p, aq.Config{}, peer); !errors.Is(err, tooOld) {
		t.Fatalf("opening against a 3.13 peer = %v, want the profile's own refusal", err)
	}
	// A peer that says 4.3 is accepted, and its facts are what the gate read.
	f := openWith(t, p, aq.Config{}, newPeerWith(map[string]any{"product": "rabbitmq", "version": "4.3.5"}))
	if got := f.b.Facts(); got.Product != "rabbitmq" || got.Version != "4.3.5" {
		t.Fatalf("the facts are %+v, want the peer's own connection properties", got)
	}
	if got := f.b.Info().BrokerVersion; got != "4.3.5" {
		t.Fatalf("DriverInfo.BrokerVersion = %q, want the peer's own version", got)
	}
}

// TestProfileFactsAreThePeersOwnWords holds the rule that an empty field is
// "the peer did not say" rather than a claim — including when the peer sends
// nothing at all, which a profile's gate then has to decide about.
func TestProfileFactsAreThePeersOwnWords(t *testing.T) {
	f := openWith(t, testProfile(), aq.Config{}, newPeerWith(nil))
	got := f.b.Facts()
	if got.Product != "" || got.Version != "" || got.Platform != "" {
		t.Fatalf("a silent peer produced %+v, want empty fields", got)
	}
	if v := f.b.Info().BrokerVersion; v != "" {
		t.Fatalf("DriverInfo.BrokerVersion = %q for a silent peer; the specification makes an empty string \"the driver did not "+
			"say\" rather than a guess", v)
	}
	// The rendering is printable whatever the peer sent, because a broker's
	// own properties arrive off the wire and a log line built from them has to
	// be valid UTF-8.
	odd := openWith(t, testProfile(), aq.Config{}, newPeerWith(map[string]any{"product": "\x00\x01", "version": "ok"}))
	if s := odd.b.Facts().String(); !isPrintable(s) {
		t.Fatalf("the facts rendered as %q, which is not printable", s)
	}
}

// TestProfileInfoNamesTheProfileAndNotTheCore holds the identity: the name a
// caller sees is the broker-specific module's, and the client library is
// go-amqp.
func TestProfileInfoNamesTheProfileAndNotTheCore(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	info := f.b.Info()
	if info.Name != "amqp10test" {
		t.Fatalf("DriverInfo.Name = %q, want the profile's own driver name", info.Name)
	}
	if info.Profile != "artemislike" {
		t.Fatalf("DriverInfo.Profile = %q, want the profile's variant name", info.Profile)
	}
	if info.DriverVersion != "v0.0.0-test" {
		t.Fatalf("DriverInfo.DriverVersion = %q, want the profile's own version", info.DriverVersion)
	}
	if info.ClientName != aq.ClientModule || info.ClientVersion != aq.ClientVersion {
		t.Fatalf("the client is reported as %s %s, want %s %s",
			info.ClientName, info.ClientVersion, aq.ClientModule, aq.ClientVersion)
	}
}

// TestProfileAnnotationPrefixDecidesWhereAHeaderGoes is the seam's smallest
// field and one of the most useful: it is what makes a broker-specific control
// reachable from the portable API without the core knowing what it means.
func TestProfileAnnotationPrefixDecidesWhereAHeaderGoes(t *testing.T) {
	for _, tc := range []struct {
		prefix     string
		key        string
		annotation bool
	}{
		{"x-opt-", "x-opt-thing", true},
		{"x-opt-", "x-thing", false},
		{"x-", "x-thing", true},
		{"", "x-opt-thing", false},
	} {
		p := testProfile()
		p.AnnotationPrefix = tc.prefix
		caps, err := aq.CapabilitiesForTest(p, aq.Facts{})
		if err != nil {
			t.Fatalf("capabilities: %v", err)
		}
		m, err := aq.MapperForTest(p, mq.MetadataNativeStrict, caps, true)
		if err != nil {
			t.Fatalf("mapper: %v", err)
		}
		msg := &mq.Message{Body: []byte("x")}
		msg.Headers.Add(tc.key, []byte("v"))
		out, err := m.Encode(aq.NewBuf(), msg, nil)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		_, isAnno := out.Annotations[tc.key]
		if isAnno != tc.annotation {
			t.Errorf("with prefix %q, header %q became an annotation=%v, want %v",
				tc.prefix, tc.key, isAnno, tc.annotation)
		}
	}
}

// TestProfileAttemptSourceDecidesTheAttemptCount holds Attempt against
// each of the three places a broker may keep it.
//
// The AMQP header's delivery-count counts REDELIVERIES and is zero on the first
// transfer, where Attempt is 1 — so a driver that reported the header raw
// would say "attempt 0" for every first delivery, which the specification makes "the broker
// does not count attempts".
func TestProfileAttemptSourceDecidesTheAttemptCount(t *testing.T) {
	t.Run("the AMQP header, which is the default", func(t *testing.T) {
		f := open(t, testProfile(), aq.Config{})
		src := f.source(mq.SourceConfig{Prefetch: 2})
		ready := make(chan struct{})
		go func() {
			rcv := f.receiver(budget)
			m := message("tag-1", "x")
			m.Header.DeliveryCount = 2
			rcv.Deliver(m)
			close(ready)
		}()
		d := fetchOne(t, src, budget)
		<-ready
		if got := d.Info.Attempt; got != 3 {
			t.Fatalf("a delivery-count of 2 became attempt %d, want 3: the header counts redeliveries and the specification's "+
				"Attempt is 1 on the first delivery", got)
		}
		if !d.Info.Redelivered {
			t.Fatal("a third attempt is not reported as a redelivery")
		}
	})
	t.Run("a broker annotation", func(t *testing.T) {
		p := testProfile()
		p.Attempt = aq.AttemptRule{
			Source: aq.AttemptFromAnnotation, Slot: aq.SlotMessageAnnotation,
			Key: "x-opt-delivery-count", FirstIsOne: true,
		}
		f := open(t, p, aq.Config{})
		src := f.source(mq.SourceConfig{Prefetch: 2})
		go func() {
			rcv := f.receiver(budget)
			m := message("tag-1", "x")
			m.Annotations = map[any]any{"x-opt-delivery-count": uint32(4)}
			rcv.Deliver(m)
		}()
		d := fetchOne(t, src, budget)
		if got := d.Info.Attempt; got != 4 {
			t.Fatalf("an annotation of 4 with FirstIsOne became attempt %d, want 4", got)
		}
	})
	t.Run("no count at all", func(t *testing.T) {
		p := testProfile()
		p.Attempt = aq.AttemptRule{Source: aq.AttemptNone}
		f := open(t, p, aq.Config{})
		src := f.source(mq.SourceConfig{Prefetch: 2})
		go func() { f.receiver(budget).Deliver(message("tag-1", "x")) }()
		d := fetchOne(t, src, budget)
		if got := d.Info.Attempt; got != 0 {
			t.Fatalf("a profile that reports no attempt count produced attempt %d; the specification makes zero \"the broker does "+
				"not count attempts\", which is honest and is not the same as \"this is the first delivery\"", got)
		}
	})
}

// TestProfileAdminIsASeamAndNotAStub is admin.go's rule: the interface is
// satisfied CONDITIONALLY, because the core asserts it once and a backend that
// satisfied it and then answered ErrUnsupported would make FeatureAdmin look
// available to a caller who asked the client rather than the table.
func TestProfileAdminIsASeamAndNotAStub(t *testing.T) {
	without := open(t, testProfile(), aq.Config{})
	if _, ok := aq.AdminBackend(without.b).(mq.AdminBackend); ok {
		t.Fatal("a backend whose profile supplies no Admin satisfies mq.AdminBackend; the core asserts it once, so " +
			"the client would offer a topology surface that answers ErrUnsupported for ever")
	}

	p := testProfile()
	admin := &fakeAdmin{}
	p.Admin = func(context.Context, *aq.Backend) (mq.Admin, error) { return admin, nil }
	p.Capabilities = func(aq.Facts) []aq.Claim {
		return []aq.Claim{aq.Conditional(mq.FeatureAdmin, mq.ScopeTopic, "a management endpoint")}
	}
	with := open(t, p, aq.Config{})
	ab, ok := aq.AdminBackend(with.b).(mq.AdminBackend)
	if !ok {
		t.Fatal("a backend whose profile supplies an Admin does not satisfy mq.AdminBackend")
	}
	got, err := ab.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if got != mq.Admin(admin) {
		t.Fatalf("Admin returned %T, want the profile's own surface", got)
	}
	if !with.b.Capabilities().Has(mq.FeatureAdmin) {
		t.Fatal("the table does not claim FeatureAdmin although the profile supplies one, so the surface and the " +
			"table disagree")
	}
}

// isPrintable reports whether every byte of s is printable ASCII or a valid
// multi-byte sequence, which is what a log pipeline can carry.
func isPrintable(s string) bool {
	for _, r := range s {
		if r == 0xfffd {
			return false
		}
		if r < 0x20 && r != '\t' && r != '\n' {
			return false
		}
	}
	return true
}
