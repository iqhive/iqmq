//go:build integration

package suites

import (
	"os"
	"testing"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_rabbitmq runs the whole suite against the pinned
// RabbitMQ container and writes the report `conformance/cmd/confgen` turns
// into capabilities.json and docs/drivers/matrix.md.
//
// The evidence is a CONTAINER and not an emulator: this is the real broker,
// the same image a deployment would run, so every claim in the report is a
// claim about RabbitMQ rather than about something that imitates it. The one
// thing it is not is a CLUSTER — a quorum queue on a single node has one
// replica, so the publisher confirm this report measures is a majority of one.
// docs/drivers/rabbitmq.md says so, which is what the evidence rule asks of
// a claim whose strength depends on the deployment.
func TestConformance_rabbitmq(t *testing.T) {
	runRabbitConformance(t, harness.RabbitMQ(t), false)
}

// TestConformance_rabbitmq_delayed runs the same suite against the image that
// carries the delayed-message exchange plugin.
//
// It is a separate report because the profile is part of the report's key and
// the two profiles differ: with the plugin, FeatureDelayedRedelivery changes
// mechanism (the two arms) and Limits.MaxDelay is the plugin's ceiling rather
// than the delay ladder's. FeatureScheduledPublish is Unsupported under both,
// because no topology the driver declares delivers a delayed publication to
// every subscription of a topic. A caller who read the
// stock image's matrix row and deployed the plugin image would be planning
// against a different broker — which is exactly what the specification makes a capability
// table per client for.
//
// It is env-gated because the image is BUILT rather than pulled:
// `make image-rabbitmq-delayed` builds it from
// hack/images/rabbitmq-delayed/Dockerfile, and the plugin only supports the
// RabbitMQ minor it was built for.
func TestConformance_rabbitmq_delayed(t *testing.T) {
	if os.Getenv("IQMQ_RABBITMQ_DELAYED") == "" {
		t.Skip("IQMQ_RABBITMQ_DELAYED is not set; the delayed-message image is built rather than pulled (make image-rabbitmq-delayed) and the stock image is the shipped evidence")
	}
	runRabbitConformance(t, harness.RabbitMQDelayed(t), true)
}

// runRabbitConformance is the one body both profiles share, so that they
// cannot drift apart.
func runRabbitConformance(t *testing.T, b *harness.Broker, plugin bool) {
	// goleak is on the DRIVER. This one starts several background goroutines
	// per client -- a supervision loop that watches the connection, a confirm
	// loop per sink and a delivery pump per source -- and every one of them is
	// bounded by the backend's own context. A Close that did not cancel it
	// would leak one per handle across a hundred and fifty scenarios and be
	// invisible in every other test.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	cfg := rq.Config{
		Addrs:    []string{b.Addr},
		Username: b.Auth.User,
		Password: dsn.Secret(b.Auth.Password),
		// The conformance kit's own provisioning is the operator, which is
		// why declaration is on here and off by default in production (the specification
		// is a rule about a deployment).
		Declare: true,
		// DeadLetter is on for every scenario, and the DESTINATION is set per
		// scenario by the harness's `shape` from `Need.DeadLetterTopic`.
		//
		// The split matters because of how the kit gates its three capability
		// scenarios. `capability/DeadLetter/works` gets the Need's shape and
		// `capability/DeadLetter/claim` does NOT, so a driver that only
		// claimed the feature under the shaped configuration would have its
		// claim scenario skipped and the feature would stay UNVERIFIED however
		// well it worked. Turning the feature on in the base configuration is
		// what makes both directions run.
		// The plugin image gets the detection, and the stock image gets it
		// too: the negative direction of the detection is a claim as much as
		// the positive one, and a run that never probed would leave the
		// detection, and the delayed-redelivery arm it chooses, unverified in
		// both directions.
		DeadLetter:        true,
		DetectDelayPlugin: true,
		ConnectionName:    "iqmq-conformance",
	}
	if plugin {
		// With the plugin present the driver's own detection upgrades family
		// The specification to it; naming the kind explicitly would test the
		// configuration rather than the detection.
		_ = plugin
	}
	h := rq.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{
		Image: b.Image, Version: b.Version,
	})
	if plugin {
		h.Profile = "amqp091-delayed"
	}
	h.Commit = os.Getenv("IQMQ_COMMIT")
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance %s: %d passed, %d failed, %d skipped, of %d scenarios",
		rep.Key(), pass, fail, skip, len(rep.Results))

	proven := 0
	for _, v := range rep.Verify() {
		if v.Failed {
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		}
		if v.Proven() {
			proven++
			continue
		}
		// An unproven claim is not a failure -- the evidence rule is that
		// it does not RENDER -- but it is worth naming in the log, because
		// the generated matrix silently omits it and the driver page has to
		// say why.
		t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s",
			v.Positive, v.Negative, v.Feature, v.Claimed)
	}
	t.Logf("capability claims proved in both directions: %d of %d", proven, len(rep.Verify()))

	dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS")
	if dir == "" {
		dir = os.Getenv("IQMQ_CONFORMANCE_REPORT_DIR")
	}
	if dir == "" {
		dir = t.TempDir()
	}
	path, err := rep.Save(dir)
	if err != nil {
		t.Fatalf("saving the report: %v", err)
	}
	t.Logf("report written to %s", path)
}
