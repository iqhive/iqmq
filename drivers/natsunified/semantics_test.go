package natsunified_test

import (
	"errors"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/natsunified"
)

// TestConfirmDurableIsRefusedAtConstructionOnCore is the second question this
// increment exists to answer: a facade must not turn the fail-fast into a
// per-message failure.
//
// The refusal happens at `Client.Publisher`, before there is a message, because
// [natsunified.Open] resolves the profile — so the sink a publisher opens is
// drivers/nats's own sink and the refusal is drivers/nats's own. What this test
// adds is the proof that the facade did not move it: the publisher must not
// exist, and nothing must have reached the broker.
//
// The negative half is the part that is easy to get wrong. A facade that
// deferred the profile choice would return a publisher here and fail at the
// first Publish, and the assertion that catches that is not "Publisher returned
// an error" but "no publisher came back at all".
func TestConfirmDurableIsRefusedAtConstructionOnCore(t *testing.T) {
	client := openFacade(t, coreConfig(t))
	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{
		Topic:        coreTopic(t),
		Confirmation: mq.ConfirmDurable,
	})
	if err == nil {
		_ = pub.Close()
		t.Fatal("Publisher with ConfirmDurable succeeded on the Core profile; NATS Core has no durable boundary and the specification requires the refusal at construction")
	}
	if pub != nil {
		t.Errorf("Publisher returned a handle beside its error (%v); a refused publisher must not exist", err)
	}
	// The refusal is a configuration error and not a transport one: the
	// profile cannot define the boundary, which is a fact about the broker
	// known before any I/O.
	if code := mq.CodeOf(err); code != mq.CodeInvalidConfig && code != mq.CodeUnsupported {
		t.Errorf("ConfirmDurable on Core carries %v, want CodeInvalidConfig or CodeUnsupported", code)
	}
	// And the weaker modes still work, so the refusal is about the durable
	// boundary and not about the publisher.
	for _, c := range []mq.ConfirmationMode{mq.ConfirmDefault, mq.ConfirmNone, mq.ConfirmAccepted} {
		h, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: coreTopic(t), Confirmation: c})
		if err != nil {
			t.Errorf("Publisher with %v on Core: %v", c, err)
			continue
		}
		_ = h.Close()
	}
}

// TestConfirmDurableWorksOnJetStream is the same control on the other profile,
// end to end: the publisher opens, the publication is confirmed, and the message
// is really in the stream.
//
// Both halves are needed. A publisher that opens and a publication that is
// accepted would also be true of a facade that silently downgraded the
// confirmation, so the message is consumed back to show the stream has it.
func TestConfirmDurableWorksOnJetStream(t *testing.T) {
	cfg := jsConfig(t)
	client := openFacade(t, cfg)
	topic := "t"
	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmDurable})
	if err != nil {
		t.Fatalf("Publisher with ConfirmDurable on the JetStream profile: %v", err)
	}
	defer func() { _ = pub.Close() }()

	con, err := client.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "durable-confirm", Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = con.Close() }()

	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("durable")}); err != nil {
		t.Fatalf("a ConfirmDurable publication: %v", err)
	}
	d := receiveOne(t, con, 10*time.Second)
	if string(d.Body) != "durable" {
		t.Errorf("received %q, want durable", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Errorf("Ack: %v", err)
	}
}

// TestSettlementDiffersByProfile is AckNone on one side and explicit
// settlement on the other, reached through the one entry point.
//
// It is what a caller who selects a profile at run time most needs to have seen
// once: the same portable code, the same call, and an error on one profile that
// is a no-op success on the other. The facade cannot smooth this over and does
// not try to.
func TestSettlementDiffersByProfile(t *testing.T) {
	t.Run("core", func(t *testing.T) {
		client := openFacade(t, coreConfig(t))
		topic := coreTopic(t)
		con, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "settle"})
		if err != nil {
			t.Fatalf("opening a consumer: %v", err)
		}
		defer func() { _ = con.Close() }()
		pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
		if err != nil {
			t.Fatalf("opening a publisher: %v", err)
		}
		defer func() { _ = pub.Close() }()
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("core")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		d := receiveOne(t, con, 10*time.Second)
		if err := d.Ack(); !errors.Is(err, mq.ErrNotSettleable) {
			t.Errorf("Ack on the Core profile returned %v, want ErrNotSettleable (AckNone)", err)
		}
		if err := d.Nack(); !errors.Is(err, mq.ErrNotSettleable) {
			t.Errorf("Nack on the Core profile returned %v, want ErrNotSettleable", err)
		}
	})
	t.Run("jetstream", func(t *testing.T) {
		client := openFacade(t, jsConfig(t))
		con, err := client.Consumer(t.Context(), mq.ConsumerConfig{
			Topic: "t", Subscription: "settle", Start: mq.StartEarliest(),
		})
		if err != nil {
			t.Fatalf("opening a consumer: %v", err)
		}
		defer func() { _ = con.Close() }()
		pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "t"})
		if err != nil {
			t.Fatalf("opening a publisher: %v", err)
		}
		defer func() { _ = pub.Close() }()
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("js")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		d := receiveOne(t, con, 10*time.Second)
		if err := d.Ack(); err != nil {
			t.Errorf("Ack on the JetStream profile returned %v, want nil", err)
		}
	})
}

// TestRequireRefusesAtOpenOnTheWrongProfile holds [natsunified.Config.Require],
// which is the only refusal this facade adds that neither driver has.
//
// It is the answer to the question a DSN cannot ask: a program that needs
// settlement, given a `nats://` DSN, would otherwise discover that at its first
// consumer — after the client is built, the connection is up and the program
// believes it is running. With a requirement it discovers it at the Open.
//
// The requirement is checked against the table the connected broker really
// produced, so the same requirement is met on the JetStream profile: both
// directions, because a check that only ever refuses proves nothing about what
// it lets through.
func TestRequireRefusesAtOpenOnTheWrongProfile(t *testing.T) {
	settles := []mq.Requirement{
		mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native),
		mq.RequireAtLeast(mq.FeatureAtLeastOnce, mq.Native),
	}

	core := coreConfig(t)
	core.Require = settles
	client, err := natsunified.Open(t.Context(), core)
	if err == nil {
		_ = client.Close()
		t.Fatal("Open on the Core profile met a requirement for native explicit acknowledgement; NATS Core has none")
	}
	if client != nil {
		t.Error("Open returned a client beside its requirement error; a refused client is a connection the caller cannot close")
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("the requirement failure is %v, want ErrUnsupported", err)
	}
	// The error has to name what was not met, or a DSN-configured operator
	// cannot tell which profile to use instead.
	if msg := err.Error(); !contains(msg, "ExplicitAck") {
		t.Errorf("the requirement failure %q does not name the feature that was not met", msg)
	}

	js := jsConfig(t)
	js.Require = settles
	client, err = natsunified.Open(t.Context(), js)
	if err != nil {
		t.Fatalf("Open on the JetStream profile did not meet a requirement it honours natively: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("closing: %v", err)
	}
}

// TestRequireClosesTheClientItRefuses is the other half of the requirement
// check, and it is the half a facade gets wrong.
//
// A requirement can only be evaluated after the dial — JetStream's table is read
// off the bound stream — so by the time the facade refuses, there is a live
// connection. Returning the error without closing it leaks one connection per
// refused Open, invisibly, in the code path an operator hits when a deployment
// is misconfigured and retries. The server's own connection count is the
// observation: it does not depend on which goroutines the client library parks.
func TestRequireClosesTheClientItRefuses(t *testing.T) {
	cfg := coreConfig(t)
	cfg.Require = []mq.Requirement{mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native)}
	for range 5 {
		if _, err := natsunified.Open(t.Context(), cfg); err == nil {
			t.Fatal("Open met an impossible requirement")
		}
	}
	waitForNoClients(t, coreServer)

	bcfg := coreConfig(t)
	bcfg.Require = cfg.Require
	for range 5 {
		if _, err := natsunified.OpenBackend(t.Context(), bcfg); err == nil {
			t.Fatal("OpenBackend met an impossible requirement")
		}
	}
	waitForNoClients(t, coreServer)
}

// TestRequireRefusesAnUnevaluableRequirement holds the delegation: the facade
// does not evaluate requirements itself, so a requirement that cannot be
// evaluated at all is the core's CodeInvalidArgument and not a facade error.
func TestRequireRefusesAnUnevaluableRequirement(t *testing.T) {
	cfg := coreConfig(t)
	cfg.Require = []mq.Requirement{{}}
	client, err := natsunified.Open(t.Context(), cfg)
	if err == nil {
		_ = client.Close()
		t.Fatal("Open accepted a zero Requirement; the specification makes it invalid rather than vacuously satisfied")
	}
	if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
		t.Errorf("a zero Requirement carries %v, want CodeInvalidArgument", code)
	}
	waitForNoClients(t, coreServer)
}

// contains is strings.Contains, spelled here so that the failure messages above
// read as one thought.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
