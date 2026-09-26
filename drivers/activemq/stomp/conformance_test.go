package stomp_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance runs the whole suite against a real broker.
//
// It is env-gated with IQMQ_ARTEMIS_ADDR or
// IQMQ_ACTIVEMQ_CLASSIC_ADDR set it runs against that endpoint, and with
// neither set it SKIPS. This module starts no container and needs none — every
// other test in the package runs against the in-process server of
// `internal/stompsrv` — and the shipped evidence for the generated capability
// matrix is `conformance/suites`' run against the pinned images.
//
// **The topology has to exist first, and on Artemis this driver cannot make
// it.** The specification forbids implicit creation, and over STOMP the only topology
// operation there is IS a subscription: on Classic a consumer queue comes into
// existence when one attaches, which is what this driver's own
// [stomp.Provision] does, and on Artemis with auto-creation disabled a
// SUBSCRIBE to a destination that does not exist is refused with `AMQ339001
// Destination does not exist`. So the Artemis run either goes through
// [stomp.ConformanceHarnessWithProvisioner] with a step that uses
// `drivers/activemq`'s Admin, or against a broker whose auto-creation is on.
// IQMQ_ARTEMIS_AUTOCREATE is the acknowledgement that somebody has arranged
// one of the two, so that a run against a broker with neither skips with a
// reason instead of failing forty scenarios for one cause.
func TestConformance(t *testing.T) {
	artemis, classic := os.Getenv("IQMQ_ARTEMIS_ADDR"), os.Getenv("IQMQ_ACTIVEMQ_CLASSIC_ADDR")
	if artemis == "" && classic == "" {
		t.Skip("set IQMQ_ARTEMIS_ADDR or IQMQ_ACTIVEMQ_CLASSIC_ADDR to run the suite against a real broker " +
			"over STOMP; this module's own tests need none, and the shipped evidence is conformance/suites' run " +
			"against the pinned images")
	}
	for name, tc := range map[string]struct {
		addr  string
		cfg   stomp.Config
		image string
	}{
		"artemis": {
			addr: artemis,
			cfg: stomp.Config{
				Broker: activemq.Artemis,
				Login:  envOr("IQMQ_ARTEMIS_USER", "artemis"),
			},
			image: "apache/activemq-artemis:2.44.0-alpine",
		},
		"classic": {
			addr: classic,
			cfg: stomp.Config{
				Broker: activemq.Classic,
				Login:  os.Getenv("IQMQ_ACTIVEMQ_CLASSIC_USER"),
				// Measured: Classic's pinned image allows an anonymous
				// CONNECT, and its `selector` header really filters -- the
				// one row this arm wins.
				Selector: os.Getenv("IQMQ_ACTIVEMQ_CLASSIC_SELECTOR"),
			},
			image: "apache/activemq-classic:6.2.0",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.addr == "" {
				t.Skipf("no endpoint for %s", name)
			}
			if name == "artemis" && os.Getenv("IQMQ_ARTEMIS_AUTOCREATE") == "" {
				t.Skip("this driver creates no topology and STOMP has no frame that could. Set " +
					"IQMQ_ARTEMIS_AUTOCREATE=1 once the broker's auto-creation is on, or run the suite through " +
					"ConformanceHarnessWithProvisioner with a step that uses drivers/activemq's Admin")
			}
			cfg := tc.cfg
			cfg.Addrs = strings.Split(tc.addr, ",")
			if pw := os.Getenv(passwordVar(name)); pw != "" {
				cfg.Passcode = dsn.Secret(pw)
				// A passcode over a plaintext endpoint is refused unless the
				// deployment says so, and a conformance run against a
				// development broker is saying so.
				cfg.InsecurePlainLogin = true
			}
			h := stomp.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: tc.image})
			rep := testkit.RunConformance(t, h)
			pass, fail, skip := rep.Counts()
			t.Logf("conformance: %d passed, %d failed, %d skipped, of %d scenarios",
				pass, fail, skip, len(rep.Results))
			proven, unproven := 0, 0
			for _, v := range rep.Verify() {
				if v.Failed {
					t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
				}
				if v.Proven() {
					proven++
					continue
				}
				unproven++
				t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s",
					v.Positive, v.Negative, v.Feature, v.Claimed)
			}
			t.Logf("claims: %d proved in both directions, %d unverified", proven, unproven)
			if dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS"); dir != "" {
				path, err := rep.Save(dir)
				if err != nil {
					t.Fatalf("saving the report: %v", err)
				}
				t.Logf("report written to %s", path)
			}
			if t.Failed() {
				if path, err := rep.Save(filepath.Join(t.TempDir(), "reports")); err == nil {
					t.Logf("the failing run's report is at %s", path)
				}
			}
		})
	}
}

// envOr is the variable's value or a default.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// passwordVar is the environment variable a broker's passcode comes from.
func passwordVar(broker string) string {
	if broker == "classic" {
		return "IQMQ_ACTIVEMQ_CLASSIC_PASSWORD"
	}
	return "IQMQ_ARTEMIS_PASSWORD"
}

// TestConformanceHarnessIsWellFormed is what makes the skip above safe.
//
// A harness nobody has built is a harness that fails on the day somebody sets
// the environment variable, and the failure then looks like the broker's. This
// builds both profiles' harnesses and checks every part the kit validates,
// against the in-process server rather than a broker — so the Open path, the
// provisioner and the Quiesce hook are all exercised with no container
// anywhere.
func TestConformanceHarnessIsWellFormed(t *testing.T) {
	t.Parallel()
	for name, broker := range map[string]activemq.Broker{
		"artemis": activemq.Artemis,
		"classic": activemq.Classic,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := brokerServer(t, broker)
			cfg := settings(broker)
			cfg.Addrs = []string{srv.Addr()}
			cfg.Passcode = dsn.Secret("hunter2")
			cfg.InsecurePlainLogin = true
			h := stomp.ConformanceHarness(cfg, testkit.EvidenceContainer,
				testkit.Broker{Image: "apache/activemq-" + name})

			if h.Driver != stomp.DriverName {
				t.Errorf("the harness names the driver %q, want %q: the generated matrix keys a row on the "+
					"driver-and-profile pair, so a driver that named itself something else would appear as another "+
					"broker", h.Driver, stomp.DriverName)
			}
			wantProfile := stomp.ProfileArtemis
			if broker == activemq.Classic {
				wantProfile = stomp.ProfileClassic
			}
			if h.Profile != wantProfile {
				t.Errorf("the harness names the profile %q, want %q", h.Profile, wantProfile)
			}
			if !strings.Contains(h.Profile, "stomp") {
				t.Errorf("the profile %q does not name the PROTOCOL. drivers/activemq uses the bare product names "+
					"over AMQP 1.0, and two conformance reports cannot share a key", h.Profile)
			}
			if h.Open == nil {
				t.Fatal("the harness has no Open, so the kit would refuse it")
			}
			if h.TopicName == nil {
				t.Fatal("the harness has no TopicName")
			}
			if h.Quiesce == nil {
				t.Error("the harness has no Quiesce. Several of the probes publish, quiesce and then receive " +
					"once, and a subscription that was not established yet would read nothing at all -- which is " +
					"how a probe comes to read one delivery of eight and nothing says why")
			}
			if h.Wildcard == nil {
				t.Error("the harness supplies no wildcard pattern. This driver does not CLAIM " +
					"FeatureWildcardSubscribe, and without a pattern the kit's negative probe skips -- which " +
					"leaves the claim unverified in BOTH directions rather than proved negative")
			}
			if !h.Evidence.Valid() {
				t.Error("the harness does not say what kind of evidence its run is; a report that cannot be read " +
					"honestly is worse than none")
			}
			if h.Fidelity <= 0 {
				t.Error("the harness lowers the fidelity count to nothing")
			}
			if h.Fidelity >= 100_000 {
				t.Logf("the harness does not lower the fidelity count (%d). Every publication here is a SEND frame "+
					"and a RECEIPT the broker commits before it answers, and both brokers are a JVM writing a "+
					"journal", h.Fidelity)
			}
			if len(h.Skip) == 0 {
				t.Error("the harness documents no skips. A SUBSCRIBE reads what a destination HOLDS, so STOMP has " +
					"no committed position and no retained history at all, and the start positions have to be " +
					"skipped with a reason the report publishes")
			}
			if _, there := h.Skip["start/positions"]; !there {
				t.Errorf("the harness does not skip the start positions (%v). StartLatest and StartEarliest are "+
					"the same place on a queue, so a scenario that ran would pass for the wrong reason", h.Skip)
			}
			if broker == activemq.Artemis {
				if _, there := h.Skip["capability/ServerFilter/works"]; !there {
					t.Error("the Artemis harness does not skip the server-filter scenario. Measured: a `selector` " +
						"header is ignored on a fully qualified queue name, so Config.Validate refuses one here " +
						"and there is nothing to exercise")
				}
			}
			for _, reason := range h.Skip {
				if strings.TrimSpace(reason) == "" {
					t.Error("a skip has no reason. A skip is never a pass: the claim it would have proved stays " +
						"unverified in the generated matrix, and the reason is the only thing that says why")
				}
			}
			if len(h.Secrets) == 0 {
				t.Error("the harness names no secrets, so the redaction scenario has nothing to look for and " +
					"would pass without checking anything")
			}
			for _, s := range h.Secrets {
				if s != "hunter2" {
					t.Errorf("the harness names %q as a secret, which is not the passcode it was given", s)
				}
			}

			// Every generated name has to be one both the driver and
			// `internal/activemq` accept, and they have to be DISTINCT per
			// purpose: on Artemis the derived queue is `<topic>.<sub>` and a
			// queue name is unique across the whole broker, so two scenarios
			// sharing a topic would share a queue and the second would
			// consume the first's messages.
			names := map[string]bool{}
			for _, purpose := range [...]string{"publish-fidelity", "settle/ack", "dead letter", "start/positions"} {
				name := h.TopicName(purpose)
				if _, _, err := stomp.DestinationsForTest(cfg, name, "workers"); err != nil {
					t.Errorf("the harness produced the topic name %q, which this profile cannot address: %v",
						name, err)
				}
				if strings.Contains(name, ".") {
					t.Errorf("the harness produced the topic name %q, which contains a \".\": the derived Artemis "+
						"queue is `<topic>.<subscription>` and activemq.ParseQueue takes it apart at the LAST "+
						"\".\", so the name would parse back as a different topic", name)
				}
				names[name] = true
			}
			if len(names) != 4 {
				t.Errorf("the harness produced %d distinct names for four purposes; two scenarios sharing a topic "+
					"share a queue, and the second would consume the first's messages", len(names))
			}

			// The Open path, end to end, against the in-process server: it
			// shapes the Need, provisions the destination by subscribing and
			// letting go, dials, and hands back a client the Quiesce hook can
			// find again.
			need := testkit.Need{Topic: h.TopicName("harness-selftest"), Subscriptions: []string{"workers"}}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			c, err := h.Open(ctx, need)
			if err != nil {
				t.Fatalf("the harness could not open a client for an ordinary Need: %v", err)
			}
			if err := h.Quiesce(ctx, c); err != nil {
				t.Errorf("Quiesce: %v", err)
			}
			if err := c.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
				t.Errorf("closing the harness's client: %v", err)
			}
			// A harness that had lost track of its backend would answer the
			// quiesce with a no-op, which is the defect the specification recorded: a type
			// assertion on Backend.Native fails silently.
			if err := h.Quiesce(ctx, nil); err == nil {
				t.Error("Quiesce accepted a client the harness never opened. A silent no-op there turns a probe " +
					"that read one delivery of eight into a mystery, which is why the failure has a sentinel")
			}
		})
	}
}

// TestHarnessRefusesTheNeedsThisProtocolCannotBuild holds the harness to the specification's
// narrow rule: refuse a Need only when accepting it would make a scenario
// prove something FALSE.
//
// A refusal becomes a skip with a reason in the report, and a skip is never a
// pass — the claim the scenario would have proved stays unverified, which is
// the only honest answer for something nobody checked. Accepting one of these
// would be worse: a partitioned scenario would find everything in order
// because a STOMP destination is the only lane there is, and a replay scenario
// would read an empty queue and call it a pass.
func TestHarnessRefusesTheNeedsThisProtocolCannotBuild(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	h := stomp.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: "artemis"})
	base := testkit.Need{Topic: h.TopicName("refusals"), Subscriptions: []string{"workers"}}

	for name, need := range map[string]testkit.Need{
		"several partitions": {Topic: base.Topic, Subscriptions: base.Subscriptions, Partitions: 4},
		"a retained topic":   {Topic: base.Topic, Subscriptions: base.Subscriptions, Retain: true},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			c, err := h.Open(ctx, need)
			if c != nil {
				_ = c.Close()
			}
			if err == nil {
				t.Fatalf("the harness accepted a Need for %s", name)
			}
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Errorf("the refusal of %s is %v, which the kit turns into a FAILURE rather than a skip with a "+
					"reason: only an error wrapping ErrUnsupported becomes a skip", name, err)
			}
			if !strings.Contains(err.Error(), "cannot build") {
				t.Errorf("the refusal does not say what could not be built, so the skip in the report would name "+
					"no reason: %v", err)
			}
		})
	}

	// The other direction, which is what makes the rule narrow rather than
	// convenient: a wildcard topology is ACCEPTED and provisioned as an
	// ordinary topic, because this driver does not CLAIM
	// FeatureWildcardSubscribe and the kit's negative probe is what proves
	// the claim. A harness that refused it would leave the negative
	// unverified too.
	t.Run("a wildcard topology is accepted, so the negative claim is provable", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		need := testkit.Need{Topic: h.TopicName("wildcards"), Subscriptions: []string{"workers"}, Wildcards: true}
		c, err := h.Open(ctx, need)
		if err != nil {
			t.Fatalf("the harness refused a wildcard topology: %v. Without a client the kit's negative probe "+
				"skips, and FeatureWildcardSubscribe then stays unverified in BOTH directions rather than proved "+
				"absent", err)
		}
		if err := c.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("closing the client: %v", err)
		}
	})
}
