//go:build integration

package suites

import (
	"context"
	"testing"

	"go.uber.org/goleak"

	amqstomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// The two STOMP suites of the second arm.
//

// TestConformance_activemq_artemis_stomp runs the suite against Apache
// ActiveMQ Artemis over STOMP 1.2.
//
// **Its topology is provisioned over AMQP 1.0**, by `drivers/activemq`'s
// Admin, and that is not a convenience: STOMP has no topology frame at all,
// and this broker is started with auto-creation OFF, so a SUBSCRIBE to a
// destination that does not exist is refused with `AMQ339001 Destination does
// not exist`. A suite that let the broker create the topology would be
// testing a deployment the specification tells this project not to build.
//
// The two drivers derive the same names from `internal/activemq`, which is
// what makes provisioning with one and consuming with the other a test of the
// addressing rather than a coincidence.
func TestConformance_activemq_artemis_stomp(t *testing.T) {
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := artemisBroker(t)
	cfg := amqstomp.Config{
		Broker:   activemq.Artemis,
		Addrs:    []string{b.Addrs["stomp"]},
		Login:    b.Auth.User,
		Passcode: dsn.Secret(b.Auth.Password),
		Prefix:   activemqPrefix,
		// The container speaks plain STOMP on 61613, so the passcode
		// travels in the CONNECT frame in clear and the driver refuses it
		// unless the caller says so deliberately.
		InsecurePlainLogin: true,
	}
	provision := func(ctx context.Context, _ amqstomp.Config, n testkit.Need) error {
		return artemisProvisioner(b, activemqPrefix)(ctx, n)
	}
	h := amqstomp.ConformanceHarnessWithProvisioner(cfg, testkit.EvidenceContainer,
		testkit.Broker{Image: b.Image, Version: b.Version}, provision)
	runActiveMQ(t, h)
}

// TestConformance_activemq_classic_stomp runs the suite against Apache
// ActiveMQ Classic over STOMP 1.2.
//
// This is the combination the specification keeps STOMP for. Classic's virtual topics are
// the fan-out, and a STOMP `selector` header is the one server-side filter
// either protocol offers here — measured: it filters on Classic and is ignored
// on Artemis when the subscription is a fully qualified queue name, which is
// the only form the Artemis profile's durable shared subscription takes.
//
// Its topology is provisioned by the driver ITSELF, with a subscribe and a
// release, because on Classic that is the whole of the topology API: a
// consumer queue comes into existence when a subscription attaches to it and
// the broker cannot be told not to. It has to exist before the first
// publication, which is why the harness does it deliberately rather than
// letting the consume path do it by accident.
//
// The broker's message scheduler is on, so the delayed-publication claim is
// exercised rather than withdrawn.
func TestConformance_activemq_classic_stomp(t *testing.T) {
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := classicBroker(t)
	cfg := amqstomp.Config{
		Broker:             activemq.Classic,
		Addrs:              []string{b.Addrs["stomp"]},
		Login:              b.Auth.User,
		Passcode:           dsn.Secret(b.Auth.Password),
		Prefix:             activemqPrefix,
		InsecurePlainLogin: true,
		Scheduler:          true,
		Prioritised:        true,
	}
	h := amqstomp.ConformanceHarness(cfg, testkit.EvidenceContainer,
		testkit.Broker{Image: b.Image, Version: b.Version})
	runActiveMQ(t, h)
}
