package jetstream_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	sjs "github.com/nats-io/nats.go/jetstream"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	nq "bitbucket.org/iqhive/iqmq/drivers/jetstream"
)

// TestCapabilitiesConsidersEveryFeature is what keeps the table honest as the inventory grows.
//
// A feature nobody mentioned renders as unsupported, which is the safe answer —
// and indistinguishable from one the driver forgot. The builder's Missing() is
// what tells them apart, and the driver refuses to open at all when the table is
// incomplete, so this test is the inventory's ratchet.
func TestCapabilitiesConsidersEveryFeature(t *testing.T) {
	caps := openClient(t).Capabilities()
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		s := caps.Support(f)
		if !s.Level.Valid() {
			t.Errorf("%v is advertised at a level outside the five", f)
		}
	}
	// The table is not empty and not everything: a driver that claimed
	// nothing, or everything, would pass a shape check and fail a caller.
	claimed, unclaimed := 0, 0
	for i := range mq.FeatureCount {
		if caps.Has(mq.Feature(i)) {
			claimed++
		} else {
			unclaimed++
		}
	}
	if claimed == 0 || unclaimed == 0 {
		t.Fatalf("%d features claimed and %d not: the specification makes the absent half as load-bearing as the present half", claimed, unclaimed)
	}
}

// TestConditionalClaimsStateTheirCondition holds the rule about claims
// themselves: "conditional" with no note tells a program that something might
// work under conditions nobody wrote down.
func TestConditionalClaimsStateTheirCondition(t *testing.T) {
	caps := openClient(t).Capabilities()
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		s := caps.Support(f)
		switch s.Level {
		case mq.Conditional:
			if s.Note == "" {
				t.Errorf("%v is conditional with no condition", f)
			}
		case mq.EmulatedVolatile, mq.EmulatedDurable:
			if s.Note == "" {
				t.Errorf("%v is %v with no note saying what the emulation is", f, s.Level)
			}
		case mq.Native, mq.Unsupported:
		}
		// The requirement machinery answers about the claim the way the specification
		// says it must.
		if err := caps.Require(mq.RequireAtLeast(f, s.Level)); err != nil {
			t.Errorf("a requirement for %v at its own level %v was refused: %v", f, s.Level, err)
		}
		if s.Level != mq.Native {
			if err := caps.Require(mq.RequireAtLeast(f, mq.Native)); err == nil {
				t.Errorf("%v is advertised at %v and accepted a requirement for native support", f, s.Level)
			} else if !errors.Is(err, mq.ErrUnsupported) {
				t.Errorf("an unmet requirement for %v reported %v", f, err)
			}
		}
	}
}

// TestCapabilityClaimsFollowTheStream is the version matrix, reachable from one
// server.
//
// Every conditional claim is read off the stream the client is really bound to
// rather than off a version string, and this is the proof: the same driver
// against a stream with AllowMsgTTL claims MessageTTL, and against a stream
// without it claims nothing and refuses the control. A server older than 2.11
// produces exactly the second stream, because it ignores the field.
func TestCapabilityClaimsFollowTheStream(t *testing.T) {
	for _, c := range []struct {
		name          string
		mutate        func(*sjs.StreamConfig)
		wantTTL       bool
		wantSchedules bool
		wantDurable   bool
	}{
		{
			name:        "a 2.10-shaped stream claims neither",
			mutate:      nil,
			wantDurable: true,
		},
		{
			name:        "a 2.11-shaped stream claims the TTL",
			mutate:      func(cfg *sjs.StreamConfig) { cfg.AllowMsgTTL = true },
			wantTTL:     true,
			wantDurable: true,
		},
		{
			name: "a 2.12-shaped stream claims both",
			mutate: func(cfg *sjs.StreamConfig) {
				cfg.AllowMsgTTL = true
				cfg.AllowMsgSchedules = true
			},
			wantTTL: true, wantSchedules: true, wantDurable: true,
		},
		{
			name:        "a memory stream withdraws durability",
			mutate:      func(cfg *sjs.StreamConfig) { cfg.Storage = sjs.MemoryStorage },
			wantDurable: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			stream := provisionStream(t, func(cfg *sjs.StreamConfig) {
				// The schedule tree has to be captured or the claim is
				// withdrawn for a second, equally real reason: a control
				// message the stream does not hold is a schedule that
				// never fires.
				cfg.Subjects = append(cfg.Subjects, nq.DefaultScheduleSubject+"."+cfg.Name+".>")
				if c.mutate != nil {
					c.mutate(cfg)
				}
			})
			caps := openOnStream(t, stream).Capabilities()
			if got := caps.Has(mq.FeatureMessageTTL); got != c.wantTTL {
				t.Errorf("MessageTTL claimed = %v, want %v", got, c.wantTTL)
			}
			if got := caps.Has(mq.FeatureScheduledPublish); got != c.wantSchedules {
				t.Errorf("ScheduledPublish claimed = %v, want %v", got, c.wantSchedules)
			}
			// Durability is the third fact the stream decides, and the one
			// a caller must never read off a driver name.
			if got := caps.Has(mq.FeatureDurableStorage); got != c.wantDurable {
				t.Errorf("DurableStorage claimed = %v, want %v", got, c.wantDurable)
			}
			if got := caps.Has(mq.FeatureDurableConfirm); got != c.wantDurable {
				t.Errorf("DurableConfirm claimed = %v, want %v", got, c.wantDurable)
			}
		})
	}
}

// TestMemoryStreamRefusesADurableRequirement is what the withdrawn claim buys a
// caller: a program that requires durable storage fails at construction against
// a memory-backed stream rather than losing messages on the next restart.
func TestMemoryStreamRefusesADurableRequirement(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.Storage = sjs.MemoryStorage })
	c := openOnStream(t, stream)
	_, err := c.Publisher(t.Context(), mq.PublisherConfig{
		Topic:        newTopic("durable"),
		Requirements: []mq.Requirement{mq.RequireAtLeast(mq.FeatureDurableStorage, mq.Native)},
	})
	if err == nil {
		t.Fatal("a publisher requiring durable storage was accepted against a memory-backed stream")
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("the refusal reported %v, want ErrUnsupported", err)
	}
	// A file-backed stream meets it, which is what makes the refusal above a
	// fact about the deployment rather than about the driver.
	if _, err := openClient(t).Publisher(t.Context(), mq.PublisherConfig{
		Topic:        newTopic("durable"),
		Requirements: []mq.Requirement{mq.RequireAtLeast(mq.FeatureDurableStorage, mq.Native)},
	}); err != nil {
		t.Fatalf("a file-backed stream refused a durable requirement: %v", err)
	}
}

// TestDeadLetterClaimNamesItsCondition holds for the one claim whose
// condition is a configuration field: the note says whether this client has a
// destination, because a caller reading "conditional" needs to know which side
// of the condition it is on.
func TestDeadLetterClaimNamesItsCondition(t *testing.T) {
	without := openClient(t).Capabilities().Support(mq.FeatureDeadLetter)
	if without.Level != mq.Conditional {
		t.Fatalf("DeadLetter is advertised at %v, want conditional", without.Level)
	}
	if !strings.Contains(without.Note, "No destination is configured") {
		t.Errorf("the note does not say this client has no destination: %q", without.Note)
	}
	with := openClient(t, func(cfg *nq.Config) { cfg.DeadLetterTopic = "dlq.holding" }).
		Capabilities().Support(mq.FeatureDeadLetter)
	if !strings.Contains(with.Note, "This client routes to") {
		t.Errorf("the note does not name the destination: %q", with.Note)
	}
	if !strings.Contains(with.Note, "and only then sends +TERM") {
		t.Errorf("the note does not state the order the route depends on: %q", with.Note)
	}
}

// TestExactlyOnceDeliveryIsNotClaimed is the line between what JetStream markets
// and what the inventory names.
//
// Publication de-duplication is real and advertised. Exactly-once DELIVERY is
// not: a delivery whose lease expires is redelivered, so a handler may run
// twice. The claim is therefore unsupported, and the reason it cannot be proved
// negatively by the conformance kit is that the kit exercises
// ExactlyOnceDelivery with the de-duplication probe — which this broker passes.
// That is recorded in the maintenance notes and skipped with a reason in the report.
func TestExactlyOnceDeliveryIsNotClaimed(t *testing.T) {
	caps := openClient(t).Capabilities()
	if caps.Has(mq.FeatureExactlyOnceDelivery) {
		t.Error("ExactlyOnceDelivery is claimed: a lease that expires redelivers, so a handler may run twice")
	}
	if !caps.Has(mq.FeatureDeduplication) {
		t.Error("Deduplication is not claimed although Nats-Msg-Id works")
	}
	dedup := caps.Support(mq.FeatureDeduplication)
	if !strings.Contains(dedup.Note, "de-duplicates PUBLICATIONS and never deliveries") {
		t.Errorf("the de-duplication note does not draw the line a caller acts on: %q", dedup.Note)
	}
}

// TestCapabilityLimitsAreOnlyWhatIsKnown holds the rule that an unadvertised
// limit is never an overloaded zero.
func TestCapabilityLimitsAreOnlyWhatIsKnown(t *testing.T) {
	caps := openClient(t).Capabilities()
	if lim := caps.Limit(mq.LimitMaxMessageBytes); !lim.Known || lim.Value <= 0 {
		t.Errorf("MaxMessageBytes = %v: the server's own max_payload is the one limit this driver knows", lim)
	}
	for _, key := range []mq.LimitKey{
		mq.LimitMaxBatchMessages, mq.LimitMaxBatchBytes,
		mq.LimitMaxHeaderCount, mq.LimitMaxHeaderBytes,
		mq.LimitMaxDelay, mq.LimitMaxLease,
	} {
		if lim := caps.Limit(key); lim.Known {
			t.Errorf("limit %v is advertised as %v: the driver does not know it, and the specification forbids guessing", key, lim.Value)
		}
	}
}

// TestFactsAreReadable holds the evidence surface: a conditional claim that
// cannot be checked against its own inputs is a claim on trust.
func TestFactsAreReadable(t *testing.T) {
	b := openBackend(t, func(cfg *nq.Config) { cfg.DoubleAck = true; cfg.DuplicateWindow = time.Minute })
	facts := b.Facts()
	for _, want := range []string{
		"storage=file", "allowmsgttl=", "allowmsgschedules=",
		"schedulesconfigured=", "duplicates=1m0s", "maxpayload=", "doubleack=true",
	} {
		if !strings.Contains(facts, want) {
			t.Errorf("Facts() does not carry %q: %s", want, facts)
		}
	}
}

// TestInfoNamesTheProfile holds the driver names itself, its profile, the
// library behind it and the broker version, and every field it does not know is
// empty rather than guessed.
func TestInfoNamesTheProfile(t *testing.T) {
	c := openClient(t)
	info := c.Info()
	if info.Name != nq.DriverName {
		t.Errorf("Name = %q, want %q", info.Name, nq.DriverName)
	}
	if info.Profile != "jetstream" {
		t.Errorf("Profile = %q, want jetstream: it is what keeps this driver's answers apart from the Core driver's", info.Profile)
	}
	if info.ClientName != "github.com/nats-io/nats.go" {
		t.Errorf("ClientName = %q", info.ClientName)
	}
	if info.ClientVersion == "" {
		t.Error("ClientVersion is empty")
	}
	if info.BrokerVersion == "" {
		t.Error("BrokerVersion is empty: the client reports one and the specification asks for it")
	}
	if info.DriverVersion != "" {
		t.Errorf("DriverVersion = %q, want empty until the module is tagged", info.DriverVersion)
	}
	// Info and Health answer after Close, from any goroutine.
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.Info().Name != nq.DriverName {
		t.Error("a closed client stopped naming its driver")
	}
	if h := c.Health(); h.State != mq.HealthClosed {
		t.Errorf("a closed client reports health %v, want closed", h.State)
	}
}

// TestAdminIsUnsupported holds the answer for a backend with no topology
// surface, and the capability that says so. JetStream HAS an administrative API;
// this driver does not wrap it, and the honest answer is a refusal rather than a
// half-implementation.
func TestAdminIsUnsupported(t *testing.T) {
	c := openClient(t)
	if c.Capabilities().Has(mq.FeatureAdmin) {
		t.Error("FeatureAdmin is claimed although this driver implements no AdminBackend")
	}
	if _, err := c.Admin(); !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("Admin() reported %v, want ErrUnsupported", err)
	}
}

// TestHealthBecomesReadyAndThenClosed holds the state machine at the two
// transitions a test can make without breaking a link.
func TestHealthBecomesReadyAndThenClosed(t *testing.T) {
	b := openBackend(t)
	if got := b.Health().State; got != mq.HealthReady {
		t.Fatalf("a freshly opened backend reports health %v, want ready", got)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := b.Health().State; got != mq.HealthClosed {
		t.Errorf("a closed backend reports health %v, want closed", got)
	}
	if got := b.DegradedTransitions(); got != 0 {
		t.Errorf("a backend that never lost its link counted %d degraded transitions", got)
	}
}

// TestOpenRefusesAMissingStream is at the other end: without
// ManageStream the driver creates nothing, so a stream nobody provisioned is a
// refusal at Open and not a client that fails later.
func TestOpenRefusesAMissingStream(t *testing.T) {
	cfg := nq.Config{Addrs: []string{sharedAddr}, Stream: "NOSUCHSTREAM_B02"}
	_, err := nq.Open(t.Context(), cfg)
	if err == nil {
		t.Fatal("Open succeeded against a stream nobody created")
	}
	if mq.CodeOf(err) != mq.CodeNotFound {
		t.Errorf("the refusal reported %v (code %v), want NotFound", err, mq.CodeOf(err))
	}
}

// TestBorrowedConnectionSurvivesClose holds explicit ownership, both ways: iqmq
// never closes a connection it was not asked to close, and always closes the one
// it dialled.
func TestBorrowedConnectionSurvivesClose(t *testing.T) {
	nc := natsConn(t)
	name := streamName(t)
	cfg := nq.Config{
		Stream:        name,
		SubjectPrefix: strings.ToLower(name),
		ManageStream:  true,
	}
	c, err := nq.OpenWithClient(t.Context(), nc, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if nc.IsClosed() {
		t.Fatal("closing a client with a BORROWED connection closed the caller's connection")
	}
	owned, err := nq.OpenWithClient(t.Context(), nc, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("OpenWithClient (owned): %v", err)
	}
	if err := owned.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !nc.IsClosed() {
		t.Fatal("closing a client with an OWNED connection left it open")
	}
	if _, err := nq.OpenWithClient(t.Context(), nil, driverkit.ClientOwned, cfg); err == nil {
		t.Error("OpenWithClient accepted a nil connection")
	}
}
