package gcppubsub

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because two different modules run
// it: this module's own env-gated test against an endpoint named by
// IQMQ_PUBSUB_ADDR, and the conformance suite against the
// pinned emulator, which is the evidence the generated capability matrix is
// built from. A harness duplicated in two places would drift, and the half
// that drifted would be the one nobody ran.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  "v2",
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: clientModuleV2, Version: clientVersionV2},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every message here is
		// a gRPC publication to a service, and the fidelity scenario
		// publishes them one at a time, so the figure is lowered to keep one
		// scenario inside the suite's per-scenario timeout.
		// docs/drivers/gcppubsub.md says so, which is what the specification asks of a
		// driver that lowers it.
		Fidelity: 2_000,
		TopicName: func(purpose string) string {
			// A Pub/Sub identifier starts with a letter, takes letters,
			// digits and a little punctuation, and is at most 255
			// characters. The nonce and the counter come FIRST and the
			// purpose is what gets truncated, so two scenarios with the same
			// long prefix cannot share a topic.
			head := "iqmqconf-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-"
			return head + truncate(sanitise(purpose), MaxIDLen-len(head)-len("-dlq"))
		},
		Secrets: secretsOf(cfg),
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			return openConformance(ctx, shape(cfg, n), n)
		},
		// The wildcard is supplied even though this driver does not claim
		// FeatureWildcardSubscribe, which is the lesson: without it both
		// wildcard scenarios skip and the claim stays unverified in BOTH
		// directions. With it, the driver's own ErrUnsupported proves the
		// negative.
		//
		// The pattern carries a "*", and that is deliberate rather than
		// decorative. A pattern that was a LEGAL topic name — "<topic>-star"
		// was the first draft — is one this driver happily resolves to a
		// topic of that name, creates, and then receives nothing on: the
		// probe times out with "no delivery arrived" instead of reading a
		// refusal, which is the kit's known hole for a driver that does
		// something DIFFERENT rather than refusing. The
		// "*" is what makes the answer a refusal.
		Wildcard: func(topic string) string { return topic + "-*" },
		Quiesce:  quiesce,
		Skip:     conformanceSkips(ev),
	}
}

// clientModuleV2 and clientVersionV2 are the pinned v2 client, restated here
// rather than imported from internal/psv2 so that the exported harness does
// not leak an internal package's constants into its signature.
const (
	clientModuleV2  = "cloud.google.com/go/pubsub/v2"
	clientVersionV2 = "v2.6.0"
)

// openConformance opens a client and provisions what the scenario's Need
// describes.
//
// The provisioning is the harness's and not the driver's, and it has to happen
// HERE rather than lazily, because several scenarios publish before they
// open a consumer and a Pub/Sub subscription's backlog begins at its own
// creation: a subscription created after the publication would never see it,
// and every one of those scenarios would look like a driver that drops
// publications. `drivers/snssqs` recorded the same shape for the same reason,
// and the kit still has no provisioning hook.
func openConformance(ctx context.Context, cfg Config, n testkit.Need) (*mq.Client, error) {
	client, err := Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	admin, err := client.Admin()
	if err != nil {
		//nolint:contextcheck // Client.Close takes no context: the specification makes a prompt close something a caller does not cancel, and Shutdown is the bounded form
		_ = client.Close()
		return nil, err
	}
	topics := []string{n.Topic}
	if n.DeadLetterTopic != "" {
		topics = append(topics, n.DeadLetterTopic)
	}
	for _, topic := range topics {
		if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
			//nolint:contextcheck // as above: Client.Close takes no context
			_ = client.Close()
			return nil, err
		}
	}
	for _, topic := range topics {
		for _, sub := range n.Subscriptions {
			if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
				//nolint:contextcheck // as above: Client.Close takes no context
				_ = client.Close()
				return nil, err
			}
		}
	}
	return client, nil
}

// secretsOf is the credentials the kit must find in no rendered string.
//
// A Pub/Sub credential is a service-account key rather than a password in a
// DSN, so this is usually empty and `secrets/redaction` usually skips. It is
// wired anyway: a run configured with CredentialsJSON gets the check, and a
// check that cannot fail proves nothing.
func secretsOf(cfg Config) []string {
	if s := cfg.CredentialsJSON.Reveal(); s != "" {
		return []string{s}
	}
	return nil
}

// quiesce waits for the service to have settled.
//
// Pub/Sub's own documentation calls subscription state eventually consistent —
// a subscription created a moment ago may not yet receive a publication — and
// the emulator is immediate. It is a sleep because there is nothing to poll:
// no API reports "every message you sent is now deliverable".
func quiesce(ctx context.Context, _ *mq.Client) error {
	timer := time.NewTimer(quiesceDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// quiesceDelay is how long Quiesce waits.
const quiesceDelay = 250 * time.Millisecond

// shape applies the parts of a scenario's Need that are CONFIGURATION on this
// driver rather than topology.
//
// AutoCreate is turned ON here and is OFF by default in production, which is
// not a contradiction: the rule is about a deployment, and the conformance
// kit's own provisioning is the operator.
//
// Ordering is turned ON for every scenario, so that FeatureOrderedByKey's
// positive probe runs at all. It costs nothing elsewhere: a publication with
// no key is unordered whatever the flag says, and a binary key becomes a
// base64 ordering key rather than a refusal.
func shape(cfg Config, n testkit.Need) Config {
	out := cfg
	out.AutoCreate = true
	out.Ordering = true
	switch {
	case n.AckDeadline > 0:
		out.AckDeadline = n.AckDeadline
	case n.AckDeadline < 0:
		// The kit's "leases that never expire". Every Pub/Sub lease expires,
		// so the honest answer is the longest one the service grants: ten
		// minutes is "never" inside a forty-five-second scenario, and
		// refusing the Need would skip the scenario instead of running it.
		out.AckDeadline = MaxAckDeadline
		out.MaxExtension = MaxExtensionCeiling
	}
	// A Need that asks for a lease SHORTER than the service's own ten-second
	// minimum is asking for a lease that expires quickly, and the client
	// library is what would stop it: it extends a held message's deadline
	// automatically up to Config.MaxExtension, so with the default ten
	// minutes no lease ever expires while a handler holds it and every
	// scenario about an expired lease would wait for ever. Capping the
	// extension budget to the requested deadline is what makes those
	// scenarios mean what they say.
	//
	// It is bounded by MinAckDeadline rather than applied to every Need
	// because a Need asking for ten seconds is asking for a lease that
	// WORKS: capability/ExtendLease's probe asks for ten and then extends by
	// thirty, which needs a budget larger than the deadline.
	if n.AckDeadline > 0 && n.AckDeadline < MinAckDeadline {
		out.MaxExtension = n.AckDeadline
	}
	if n.Retain {
		// A seek backwards needs acknowledged messages kept, and the
		// service's own minimum retention is ten minutes.
		out.RetainAcked = true
		out.Retention = MinRetention
	}
	if n.DeadLetterTopic != "" {
		// the destination is the SUBSCRIPTION's and the SPI's
		// SourceConfig does not carry it, so on this driver it is
		// configuration and the harness is what turns a scenario's Need into
		// it. MaxDeliveryAttempts is at least five because that is the
		// service's own minimum; the explicit Reject route does not wait for
		// it, and the native policy is what the number governs.
		out.DeadLetterTopic = n.DeadLetterTopic
		out.MaxDeliveryAttempts = int(max(n.MaxAttempts, 5))
	}
	if n.Capacity > 0 {
		out.Prefetch = min(n.Capacity, mq.MaxPrefetch)
	}
	return out
}

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// Every entry is a claim the report cannot support, so the list is populated
// only after watching a scenario fail against the pinned emulator and finding
// that the failure is the SERVICE's shape rather than the driver's defect —
// never in anticipation.
//
// It is keyed on the EVIDENCE, and that is the point of the one entry in it. A
// claim the emulator cannot exercise is not a claim the SERVICE cannot honour,
// so a skip hard-coded for every run would hide a real failure against a real
// project. The specification asks for exactly this distinction for this driver: "emulator
// with unverified marks for DLQ/EOD/seek".
func conformanceSkips(ev testkit.Evidence) map[string]string {
	skips := map[string]string{}
	if ev != testkit.EvidenceEmulator {
		return skips
	}
	skips["capability/SeekByTime/works"] = "the Pub/Sub EMULATOR does not implement Seek: the request comes back Unimplemented, so this run cannot exercise the positive direction of a claim the managed service honours. The claim stays UNVERIFIED with this reason rather than being asserted from the client library's documentation, which is what the capability matrix exists to distinguish. Config.RetainAcked and the Seek at OpenSource are exercised against the scripted fake in the driver's own tests; a run against a real project (testkit.EvidenceManaged) drops this skip and proves the claim"
	return skips
}

// sanitise turns a scenario name into Pub/Sub identifier characters.
func sanitise(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_' || r == '.' || r == '~':
			return r
		default:
			return '-'
		}
	}, purpose)
}

// truncate cuts s to at most n bytes.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	return s[:n]
}
