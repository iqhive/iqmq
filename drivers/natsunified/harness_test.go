package natsunified_test

import (
	"errors"
	"sort"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/jetstream"
	"bitbucket.org/iqhive/iqmq/drivers/nats"
	"bitbucket.org/iqhive/iqmq/drivers/natsunified"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// TestConformanceHarnessIsKeyedPerProfile holds the shape of the evidence
// this facade publishes.
//
// Two runs, two report files, two columns in the generated matrix. The profile
// has to be spelled out even though the JetStream driver's own harness leaves it
// empty — there the driver name IS the variant, here the driver name is the
// facade's — and two reports that both wanted to be called `natsunified.json`
// would have overwritten each other, leaving one profile's evidence to be the
// other's.
func TestConformanceHarnessIsKeyedPerProfile(t *testing.T) {
	seen := map[string]bool{}
	for _, cfg := range []natsunified.Config{coreConfig(t), jsConfig(t)} {
		h := natsunified.ConformanceHarness(cfg, testkit.EvidenceInProcess, testkit.Broker{Image: "embedded"})
		if h.Driver != natsunified.DriverName {
			t.Errorf("%v: harness Driver = %q, want %q", cfg.Profile, h.Driver, natsunified.DriverName)
		}
		if h.Profile != cfg.Profile.String() {
			t.Errorf("%v: harness Profile = %q, want %q", cfg.Profile, h.Profile, cfg.Profile)
		}
		key := h.Driver + "/" + h.Profile
		if seen[key] {
			t.Errorf("both profiles produced the report key %q; one run's evidence would overwrite the other's", key)
		}
		seen[key] = true
		if h.Open == nil || h.TopicName == nil {
			t.Errorf("%v: the harness lost the profile driver's Open or TopicName", cfg.Profile)
		}
	}
	// A harness for a configuration with no profile has no Open, which is what
	// testkit.RunConformance refuses rather than running a suite against
	// nothing.
	h := natsunified.ConformanceHarness(natsunified.Config{}, testkit.EvidenceInProcess, testkit.Broker{Image: "embedded"})
	if h.Open != nil {
		t.Error("a harness built from a Config with no profile has an Open; there is nothing for it to open")
	}
}

// TestConformanceHarnessReusesTheProfileDriversHarness is the other half of the
// facade's harness decision: everything except Open, Driver and Profile is the
// profile driver's, because every one of those is a fact about the BROKER.
//
// The fidelity figure and the skip list are the two that matter most. The specification asks
// for 100,000 interleaved messages and both drivers lower it with a reason on
// their own pages; a facade that reset it would run a different scenario and
// report it under the same name. The skip list is the driver's statement of what
// is inapplicable to its broker, and a facade that dropped an entry would report
// a failure the broker cannot avoid, while one that added an entry would report
// a pass it had not earned.
func TestConformanceHarnessReusesTheProfileDriversHarness(t *testing.T) {
	ev, broker := testkit.EvidenceInProcess, testkit.Broker{Image: "embedded"}

	ccfg := coreConfig(t)
	cf := natsunified.ConformanceHarness(ccfg, ev, broker)
	cd := nats.ConformanceHarness(ccfg.Core, ev, broker)
	compareHarness(t, "core", cf, cd)

	jcfg := jsConfig(t)
	jf := natsunified.ConformanceHarness(jcfg, ev, broker)
	jd := jetstream.ConformanceHarness(jcfg.JetStream, ev, broker)
	compareHarness(t, "jetstream", jf, jd)
}

// compareHarness fails when the facade's harness differs from the driver's in
// anything but Driver, Profile and Open.
func compareHarness(tb testing.TB, profile string, facade, driver testkit.Harness) {
	tb.Helper()
	if facade.Fidelity != driver.Fidelity {
		tb.Errorf("%s: fidelity is %d through the facade and %d through the driver; the figure is the driver's to lower",
			profile, facade.Fidelity, driver.Fidelity)
	}
	if facade.Timeout != driver.Timeout {
		tb.Errorf("%s: per-scenario timeout is %v through the facade and %v through the driver", profile, facade.Timeout, driver.Timeout)
	}
	// The facade's skip list is the driver's plus exactly one: the scenario
	// the KIT cannot run against a facade (`client/info-and-native`). Every other entry is a statement about the broker and is the
	// driver's to make, so a facade that dropped one would report a failure the
	// broker cannot avoid and one that added one would report a pass it had not
	// earned.
	for name, reason := range driver.Skip {
		if facade.Skip[name] != reason {
			tb.Errorf("%s: the facade lost or changed the driver's skip %q: facade %q, driver %q", profile, name, facade.Skip[name], reason)
		}
	}
	extra := make([]string, 0, 1)
	for name := range facade.Skip {
		if _, ok := driver.Skip[name]; !ok {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	if strings.Join(extra, ",") != "client/info-and-native" {
		tb.Errorf("%s: the facade adds the skips %v; exactly one is expected (client/info-and-native, the scenario the kit cannot run against a facade)",
			profile, extra)
	}
	if (facade.Wildcard == nil) != (driver.Wildcard == nil) {
		tb.Errorf("%s: one harness can write a wildcard pattern and the other cannot", profile)
	}
	if (facade.Quiesce == nil) != (driver.Quiesce == nil) {
		tb.Errorf("%s: one harness can quiesce the broker and the other cannot", profile)
	}
	if facade.Client != driver.Client {
		tb.Errorf("%s: client library differs: facade %+v, driver %+v", profile, facade.Client, driver.Client)
	}
}

// TestConformanceHarnessOpensThroughTheFacade is what makes the facade's
// conformance run mean something.
//
// If the harness's Open went straight to the profile driver, the two runs would
// re-prove the drivers and say nothing about the facade — and they would still
// be green, which is why this is checked rather than assumed. The probe is a
// requirement only the facade evaluates: [natsunified.Config.Require] is the
// facade's own refusal, so a harness whose Open reaches the facade honours it and
// one that bypasses the facade cannot see it at all.
func TestConformanceHarnessOpensThroughTheFacade(t *testing.T) {
	for _, tc := range []struct {
		cfg natsunified.Config
		req mq.Requirement
	}{
		// A feature the profile does not have, so the facade's own check is
		// the only thing that can refuse the Open.
		{coreConfig(t), mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native)},
		{jsConfig(t), mq.RequireAtLeast(mq.FeatureRequestReply, mq.Native)},
	} {
		cfg := tc.cfg
		cfg.Require = []mq.Requirement{tc.req}
		h := natsunified.ConformanceHarness(cfg, testkit.EvidenceInProcess, testkit.Broker{Image: "embedded"})
		client, err := h.Open(t.Context(), testkit.Need{Topic: "t", Subscriptions: []string{"conformance"}})
		if err == nil {
			_ = client.Close()
			t.Errorf("%v: the harness opened a client that does not meet %v; its Open does not go through the facade, so the conformance run would re-prove the driver and say nothing about the facade",
				cfg.Profile, tc.req)
			continue
		}
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("%v: the harness's Open failed with %v, want ErrUnsupported from the facade's requirement check", cfg.Profile, err)
		}
	}
	// And with no requirement it really opens, so the check above is about the
	// requirement and not about a harness that cannot open at all.
	for _, cfg := range []natsunified.Config{coreConfig(t), jsConfig(t)} {
		h := natsunified.ConformanceHarness(cfg, testkit.EvidenceInProcess, testkit.Broker{Image: "embedded"})
		client, err := h.Open(t.Context(), testkit.Need{Topic: h.TopicName("t"), Subscriptions: []string{"conformance"}})
		if err != nil {
			t.Fatalf("%v: the harness could not open a client at all: %v", cfg.Profile, err)
		}
		if got := natsunified.ProfileOf(client.Info()); got != cfg.Profile {
			t.Errorf("the harness opened a %v client for the %v profile", got, cfg.Profile)
		}
		_ = client.Close()
	}
}
