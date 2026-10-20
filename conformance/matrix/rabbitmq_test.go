//go:build matrix

package matrix

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/conformance/harness"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	rq10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"github.com/iqhive/iqmq/dsn"
)

// DefaultRabbitImages is the matrix `make matrix DRIVER=rabbitmq` runs when
// IQMQ_RABBITMQ_IMAGES names nothing.
//
// The three straddle the one version gate this driver PAIR has: AMQP 1.0
// arrived in RabbitMQ 4.0, so 3.13 has it only as a plugin with different
// behaviour and 4.1 and 4.3 have it natively. The pinned image the conformance
// suite uses is the fourth row and the one the generated matrix is built from.
var DefaultRabbitImages = []string{
	"rabbitmq:3.13-management-alpine",
	"rabbitmq:4.1-management-alpine",
	harness.Images["rabbitmq"],
}

// TestMatrix_rabbitmq holds both RabbitMQ profiles to the servers that decide
// their claims.
//
// The result is asymmetric and that asymmetry IS the finding. The AMQP 0-9-1
// driver's capability table must be the SAME on all three versions — quorum
// queues, `x-delivery-count`, per-message TTL and dead-letter routing are all
// 3.8-era features, and a driver whose settlement claims moved with the server
// would be a driver nobody could write against. The AMQP 1.0 profile's answer
// is binary: it works from 4.0 and refuses everything below, which is the specification's
// "never silently substituted" made a test rather than a sentence.
//
// The expectation is computed from the SERVER'S OWN VERSION rather than from a
// table of image names, so `IQMQ_RABBITMQ_IMAGES=rabbitmq:4.2 make matrix
// DRIVER=rabbitmq` needs no code change and a claim that stopped tracking its
// gate fails on the image that crosses it.
func TestMatrix_rabbitmq(t *testing.T) {
	var baseline map[mq.Feature]mq.Support
	var baselineVersion string
	for _, image := range rabbitImages() {
		t.Run(sanitise(image), func(t *testing.T) {
			// harness.Image honours IQMQ_RABBITMQ_IMAGE, so setting it is
			// what makes the Broker report the image it really started --
			// which a WithImage customiser would not, and a report naming
			// the wrong image is worse than no report.
			t.Setenv("IQMQ_RABBITMQ_IMAGE", image)
			b := harness.RabbitMQ(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			cfg := rq.Config{
				Addrs:      []string{b.Addr},
				Username:   b.Auth.User,
				Password:   dsn.Secret(b.Auth.Password),
				Prefix:     "matrix" + strconv.FormatInt(time.Now().UnixNano(), 36) + ".",
				Declare:    true,
				DeadLetter: true,
			}
			client, err := rq.Open(ctx, cfg)
			if err != nil {
				t.Fatalf("opening the AMQP 0-9-1 driver against %s: %v", image, err)
			}
			defer func() { _ = client.Close() }()

			version := client.Info().BrokerVersion
			if version == "" || version == "unknown" {
				t.Fatal("the client reports no broker version, so nothing here can be gated on one")
			}
			major := majorOf(t, version)
			t.Logf("image %s reports server %s (major %d)", image, version, major)

			// (1) The 0-9-1 table does not move. Every row is compared with
			// the first version's, which is a stronger statement than a list
			// of features: it catches a NOTE that changed as well as a level.
			caps := client.Capabilities()
			got := map[mq.Feature]mq.Support{}
			for i := range mq.FeatureCount {
				got[mq.Feature(i)] = caps.Support(mq.Feature(i))
			}
			switch baseline {
			case nil:
				baseline, baselineVersion = got, version
			default:
				for feature, want := range baseline {
					if have := got[feature]; have != want {
						t.Errorf("%v differs between server %s and server %s:\n  %s: %+v\n  %s: %+v\nThe AMQP 0-9-1 claims are 3.8-era features and must not move with the version",
							feature, baselineVersion, version, baselineVersion, want, version, have)
					}
				}
			}

			// (2) The AMQP 1.0 profile's gate, in both directions. It is the
			// only claim in this driver pair that a version decides, and the specification forbids substituting the other protocol for it.
			// No Declare: the AMQP 1.0 profile REFUSES it, because
			// RabbitMQ 4's AMQP 1.0 endpoint creates no topology and an
			// attach to an address nothing provisioned is `amqp:not-found`.
			// The 0-9-1 client above has already declared this prefix's
			// exchange, which is what makes the address resolvable at all.
			ten, tenErr := rq10.Open(ctx, rq10.Config{
				Addrs:    []string{b.Addr},
				Username: b.Auth.User,
				Password: dsn.Secret(b.Auth.Password),
				Prefix:   cfg.Prefix,
				// The pinned containers speak plain AMQP on a loopback port,
				// so SASL PLAIN is in the clear and the profile refuses it
				// unless a caller says so deliberately. Saying it here is what
				// keeps THIS test about the version gate: without it every
				// image was refused for the credential reason and the gate's
				// two directions were never reached.
				InsecurePlainSASL: true,
			})
			if ten != nil {
				defer func() { _ = ten.Close() }()
			}
			switch {
			case major >= 4 && tenErr != nil:
				t.Errorf("the AMQP 1.0 profile was refused by server %s, which speaks it natively: %v", version, tenErr)
			case major < 4 && tenErr == nil:
				t.Errorf("the AMQP 1.0 profile OPENED against server %s: on 3.13 it is a plugin whose feature set, address syntax and settlement behaviour differ, and the driver documentation forbids substituting one protocol for the other silently", version)
			case major < 4 && !errors.Is(tenErr, mq.ErrUnsupported):
				t.Errorf("the AMQP 1.0 profile was refused by server %s with %v, want ErrUnsupported: a caller distinguishes \"this broker is too old\" from \"the dial failed\"", version, tenErr)
			case major < 4:
				// The refusal has to name SOMETHING an operator can act on,
				// and which of the two it names depends on how far the
				// connection got. A broker that answers the OPEN frame is
				// refused by the profile's version gate and the message names
				// the version; a broker that refuses the PROTOCOL HEADER
				// never sends an OPEN frame at all, so no gate runs and the
				// only thing there is to say is which protocol could not be
				// negotiated. rabbitmq:3.13-management-alpine is the second
				// kind — its image does not load the AMQP 1.0 plugin — and
				// the message is `unexpected protocol version 0.9.1`, which
				// names the protocol the peer offered instead.
				msg := tenErr.Error()
				if !strings.Contains(msg, version) && !strings.Contains(msg, "protocol version") {
					t.Errorf("the refusal names neither the broker's version nor the protocol it could not "+
						"negotiate, so an operator cannot tell which server refused or why: %v", tenErr)
				}
			}

			// (3) The attempt counter, which the specification marks `(verify)`. It is a
			// quorum-queue field and quorum queues are 3.8-era, so it has to
			// work on every version in the matrix -- and the only way to
			// know is to nack a delivery and read the count.
			checkAttemptCount(t, ctx, client, version)
		})
	}
}

// checkAttemptCount publishes, nacks and re-receives, so that
// `x-delivery-count` is read from a real broker rather than asserted from the
// capability table.
//
// It is the specification's `(verify)` for this driver: "x-delivery-count → Attempt (quorum
// queues **(verify)**)". A table that claimed it and a broker that did not
// send it would leave MaxAttempts comparing a number that never rises,
// and nothing below a real nack can tell the two apart.
func checkAttemptCount(t *testing.T, ctx context.Context, client *mq.Client, version string) {
	t.Helper()
	topic := "attempts"
	cons, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "workers"})
	if err != nil {
		t.Fatalf("opening a consumer against server %s: %v", version, err)
	}
	defer func() { _ = cons.Close() }()
	if err := client.Publish(ctx, topic, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("publishing against server %s: %v", version, err)
	}
	dst := make([]mq.Delivery, 1)
	first, err := receiveWithin(ctx, cons, dst, 30*time.Second)
	if err != nil {
		t.Fatalf("receiving against server %s: %v", version, err)
	}
	if first.Info().Attempt != 1 {
		t.Errorf("server %s reports Attempt %d on a first delivery, want 1: the API makes zero \"the broker does not count attempts\", and a quorum queue does",
			version, first.Info().Attempt)
	}
	if err := first.Nack(); err != nil {
		t.Fatalf("nacking against server %s: %v", version, err)
	}
	second, err := receiveWithin(ctx, cons, dst, 30*time.Second)
	if err != nil {
		t.Fatalf("receiving the redelivery against server %s: %v", version, err)
	}
	if got := second.Info().Attempt; got != 2 {
		t.Errorf("server %s reports Attempt %d on a redelivery, want 2: x-delivery-count is what MaxAttempts is compared against, and a count that does not rise makes it silently ineffective",
			version, got)
	}
	// WHICH header the count arrived in, named rather than inferred. This is
	// the matrix's most consequential finding and the reason the driver reads
	// two names: 3.13.7 and 4.1.8 send `x-delivery-count` and 4.3.5 sends
	// `x-acquired-count`. The check above would catch the rename as a wrong
	// Attempt; this one says WHY, which is the difference between a test that
	// fails and a test that explains.
	switch native, ok := second.Native().(*amqp.Delivery); {
	case ok:
		_, byDelivery := native.Headers[rq.DeliveryCountHeader]
		_, byAcquired := native.Headers[rq.AcquiredCountHeader]
		switch {
		case byDelivery && byAcquired:
			t.Errorf("server %s sent BOTH %s and %s; the driver reads either and would be adding two counts if they ever differed",
				version, rq.DeliveryCountHeader, rq.AcquiredCountHeader)
		case !byDelivery && !byAcquired:
			t.Errorf("server %s sent neither %s nor %s on a redelivery from a quorum queue, so Attempt has no source at all: %v",
				version, rq.DeliveryCountHeader, rq.AcquiredCountHeader, native.Headers)
		case byDelivery:
			t.Logf("server %s counts redeliveries in %s", version, rq.DeliveryCountHeader)
		default:
			t.Logf("server %s counts redeliveries in %s", version, rq.AcquiredCountHeader)
		}
	default:
		t.Errorf("the delivery's Native is %T, so the header the count arrived in cannot be named", second.Native())
	}
	if !second.Info().Redelivered {
		t.Errorf("server %s does not mark the redelivery redelivered", version)
	}
	if err := second.Ack(); err != nil {
		t.Fatalf("acknowledging against server %s: %v", version, err)
	}
}

// receiveWithin receives one delivery within a bound.
func receiveWithin(ctx context.Context, cons *mq.ConsumerHandle, dst []mq.Delivery, within time.Duration) (mq.Delivery, error) {
	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	for {
		n, err := cons.Receive(ctx, dst)
		if err != nil {
			return mq.Delivery{}, err
		}
		if n > 0 {
			return dst[0], nil
		}
	}
}

// rabbitImages is IQMQ_RABBITMQ_IMAGES, or the default three.
func rabbitImages() []string {
	if v := os.Getenv("IQMQ_RABBITMQ_IMAGES"); v != "" {
		var out []string
		for part := range strings.SplitSeq(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return DefaultRabbitImages
}

// majorOf is the major version a server reported.
func majorOf(t *testing.T, version string) int {
	t.Helper()
	head, _, _ := strings.Cut(version, ".")
	n, err := strconv.Atoi(strings.TrimSpace(head))
	if err != nil {
		t.Fatalf("the server reported the version %q, whose major number cannot be read: %v", version, err)
	}
	return n
}
