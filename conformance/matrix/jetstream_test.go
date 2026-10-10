//go:build matrix

// It is not the conformance suite. The conformance suite answers "what does this
// driver do against the image the project ships against"; this answers the
// narrower and sharper question a capability table raises: WHICH CLAIMS DEPEND
// ON THE BROKER'S VERSION, and does the driver really change its answer when the
// broker does. A conditional claim whose condition nobody varied is a claim
// nobody checked.
package matrix

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/conformance/harness"
	jq "github.com/iqhive/iqmq/drivers/jetstream"

	mq "github.com/iqhive/iqmq"
)

// DefaultNATSImages is the matrix `make matrix DRIVER=jetstream` runs when
// IQMQ_NATS_IMAGES names nothing.
//
// The three are chosen because they straddle the two version gates this driver's
// capability table depends on: per-message TTL arrived in 2.11 and message
// schedules in 2.12, so 2.10 has neither, 2.11 has the first and 2.12 onwards
// has both. The pinned image the conformance suite uses is 2.14, which is the
// fourth row and the one the generated matrix is built from.
var DefaultNATSImages = []string{
	"nats:2.10-alpine",
	"nats:2.11-alpine",
	"nats:2.12-alpine",
	harness.Images["nats"],
}

// TestMatrix_jetstream holds the driver's version-gated claims to the servers
// that decide them.
//
// The expectation is computed from the SERVER'S OWN VERSION rather than from a
// table of image names, which is what makes the test useful when somebody adds
// an image: `IQMQ_NATS_IMAGES=nats:2.13-alpine make matrix DRIVER=jetstream`
// needs no code change, and a claim that stopped tracking its gate fails on the
// image that crosses it.
//
// Everything else the profile claims is checked to be the SAME on every
// version, because it is: at-least-once, explicit settlement, the lease and the
// dead-letter route are 2.9 features, and a driver whose settlement claims moved
// with the server would be a driver nobody could write against.
func TestMatrix_jetstream(t *testing.T) {
	for _, image := range natsImages() {
		t.Run(sanitise(image), func(t *testing.T) {
			// harness.Image honours IQMQ_NATS_IMAGE, so setting it is what
			// makes the Broker report the image it really started — which a
			// WithImage customiser would not, and a report naming the wrong
			// image is worse than no report.
			t.Setenv("IQMQ_NATS_IMAGE", image)
			b := harness.NATS(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			stream := "MATRIX_" + strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36))
			client, err := jq.Open(ctx, jq.Config{
				Addrs:         []string{"nats://" + b.Addr},
				Stream:        stream,
				SubjectPrefix: "matrix",
				ManageStream:  true,
			})
			if err != nil {
				t.Fatalf("opening a client against %s: %v", image, err)
			}
			defer func() { _ = client.Close() }()

			version := client.Info().BrokerVersion
			if version == "" {
				t.Fatal("the client reports no broker version, so nothing here can be gated on one")
			}
			major, minor := parseVersion(t, version)
			caps := client.Capabilities()
			t.Logf("image %s reports server %s (%d.%d)", image, version, major, minor)

			// The two version gates.
			wantTTL := atLeast(major, minor, 2, 11)
			if got := caps.Has(mq.FeatureMessageTTL); got != wantTTL {
				t.Errorf("server %s claims MessageTTL = %v, want %v: Nats-TTL arrived in 2.11 and the claim is read off the stream's AllowMsgTTL, which an older server ignores",
					version, got, wantTTL)
			}
			wantSchedules := atLeast(major, minor, 2, 12)
			if got := caps.Has(mq.FeatureScheduledPublish); got != wantSchedules {
				t.Errorf("server %s claims ScheduledPublish = %v, want %v: message schedules arrived in 2.12 and the claim is read off the stream's AllowMsgSchedules",
					version, got, wantSchedules)
			}

			// The refusal is the other half of the gate: a claim withdrawn
			// has to make the control fail rather than pass silently.
			topic := "orders." + strconv.FormatInt(time.Now().UnixNano(), 36)
			ttlErr := client.Publish(ctx, topic, &mq.Message{Body: []byte("x")}, mq.TTL(time.Minute))
			switch {
			case wantTTL && ttlErr != nil:
				t.Errorf("server %s claims MessageTTL and refused it: %v", version, ttlErr)
			case !wantTTL && ttlErr == nil:
				t.Errorf("server %s cannot honour a per-message TTL and accepted one: the message would never expire and the caller would not know", version)
			}
			schedErr := client.Publish(ctx, topic, &mq.Message{Body: []byte("x")}, mq.Delay(time.Minute))
			switch {
			case wantSchedules && schedErr != nil:
				t.Errorf("server %s claims ScheduledPublish and refused it: %v", version, schedErr)
			case !wantSchedules && schedErr == nil:
				t.Errorf("server %s cannot schedule a publication and accepted one: it would be delivered at once", version)
			}

			// What does NOT move with the version.
			for _, f := range []mq.Feature{
				mq.FeatureAtLeastOnce, mq.FeatureExplicitAck, mq.FeatureNack,
				mq.FeatureExtendLease, mq.FeatureDelayedRedelivery,
				mq.FeatureDurableSubscriptions, mq.FeatureConsumerGroups,
				mq.FeatureDurableStorage, mq.FeatureDurableConfirm,
				mq.FeatureReplay, mq.FeatureSeekByTime, mq.FeatureSeekBySequence,
				mq.FeatureWildcardSubscribe, mq.FeatureDeduplication,
			} {
				if !caps.Has(f) {
					t.Errorf("server %s does not claim %v, which every 2.9+ JetStream has", version, f)
				}
			}
			for _, f := range []mq.Feature{
				mq.FeatureAtMostOnce, mq.FeatureExactlyOnceDelivery,
				mq.FeaturePriority, mq.FeatureBinaryHeaders,
				mq.FeatureTransactions, mq.FeatureAdmin,
			} {
				if caps.Has(f) {
					t.Errorf("server %s claims %v, which no JetStream version has (in this driver)", version, f)
				}
			}

			// And the disqualifier, on every version: the reply subject of an
			// expired delivery still works at the server, so the driver has
			// to refuse the settlement.
			checkStaleAck(ctx, t, b.Addr, version)
		})
	}
}

// checkStaleAck holds the stale-ack guard against one server.
//
// It is here as well as in the driver's own tests because the behaviour it
// guards against is the SERVER's: nats-server's AckExplicit path deletes its
// pending entry by stream sequence without looking at which delivery the reply
// came from. A version that started refusing a stale ack itself would be good
// news and is worth knowing about; a version that still takes it is why the
// guard cannot be removed.
func checkStaleAck(ctx context.Context, t *testing.T, addr, version string) {
	t.Helper()
	stream := "STALE_" + strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36))
	client, err := jq.Open(ctx, jq.Config{
		Addrs:         []string{"nats://" + addr},
		Stream:        stream,
		SubjectPrefix: "stale",
		ManageStream:  true,
		AckWait:       600 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("opening a client for the stale-ack check: %v", err)
	}
	defer func() { _ = client.Close() }()

	topic := "records"
	cons, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	if err := client.Publish(ctx, topic, &mq.Message{Body: []byte("stale")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	dst := make([]mq.Delivery, 1)
	rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
	defer rcancel()
	if n, rerr := cons.Receive(rctx, dst); n != 1 {
		t.Fatalf("Receive: %d, %v", n, rerr)
	}
	first := dst[0]
	second := make([]mq.Delivery, 1)
	sctx, scancel := context.WithTimeout(ctx, 20*time.Second)
	defer scancel()
	if n, rerr := cons.Receive(sctx, second); n != 1 {
		t.Fatalf("the message was not redelivered on server %s, so there is no stale settlement to refuse: %v", version, rerr)
	}
	if err := first.Ack(); err == nil {
		t.Errorf("server %s: acking the expired delivery succeeded through iqmq: the guard is not holding", version)
	}
	if err := second[0].Ack(); err != nil {
		t.Errorf("server %s: acking the current delivery failed: %v", version, err)
	}
}

// natsImages is the matrix IQMQ_NATS_IMAGES names, or [DefaultNATSImages].
func natsImages() []string {
	raw := os.Getenv("IQMQ_NATS_IMAGES")
	if strings.TrimSpace(raw) == "" {
		return DefaultNATSImages
	}
	var out []string
	for part := range strings.SplitSeq(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return DefaultNATSImages
	}
	return out
}

// parseVersion reads the major and minor of a server version such as "2.11.17".
func parseVersion(t *testing.T, version string) (int, int) {
	t.Helper()
	v := strings.TrimPrefix(version, "v")
	parts := strings.Split(v, ".")
	if len(parts) < 2 {
		t.Fatalf("the server version %q has no major.minor", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		t.Fatalf("the server version %q has no numeric major", version)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("the server version %q has no numeric minor", version)
	}
	return major, minor
}

// atLeast reports whether major.minor is at least wantMajor.wantMinor.
func atLeast(major, minor, wantMajor, wantMinor int) bool {
	if major != wantMajor {
		return major > wantMajor
	}
	return minor >= wantMinor
}

// sanitise turns an image reference into a subtest name.
func sanitise(image string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-':
			return r
		default:
			return '_'
		}
	}, image)
}
