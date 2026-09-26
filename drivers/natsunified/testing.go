package natsunified

import (
	"context"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this facade hands the conformance kit, for
// one profile.
//
// It is the profile driver's OWN harness with two things changed and nothing
// else:
//
//  1. Every scenario's client is opened through [Open] — this facade's entry
//     point, with this facade's validation and this facade's requirement
//     check — instead of through the profile driver's Open. That is the whole
//     point of running the suite twice: a facade whose conformance evidence came
//     from the driver underneath it would prove nothing about the facade.
//  2. The report is keyed `natsunified/<profile>` rather than `nats/core` or
//     `jetstream`, so the two runs land in two files and the generated matrix
//     shows the facade's columns beside the drivers' own — which is what makes a
//     lost claim visible as a difference between two columns rather than
//     invisible in a column that is not there.
//
// Everything else — which topology Needs this broker can build, the subject
// naming, the run's stream, the wildcard syntax, the lowered fidelity, the
// scenarios the driver documents as inapplicable — comes from the profile
// driver, because every one of those is a fact about the BROKER and a facade
// that had its own opinion about them would be testing a different deployment.
// The seam that makes the reuse possible is `nats.ConformanceHarnessWith` and
// `jetstream.ConformanceHarnessWith`, which this increment added to both
// drivers.
//
// A [Config] with no profile is a programming error here rather than a returned
// one: a harness is built in a test's first line and the kit's own validation
// refuses it a few lines later with a message about the driver. The
// returned harness names no Open in that case, which is exactly what
// `RunConformance` fails on.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var h testkit.Harness
	switch cfg.Profile {
	case ProfileCore:
		h = nats.ConformanceHarnessWith(cfg.Core, ev, broker,
			func(ctx context.Context, c nats.Config) (*mq.Client, error) {
				return Open(ctx, Config{Profile: ProfileCore, Core: c, Require: cfg.Require})
			})
	case ProfileJetStream:
		h = jetstream.ConformanceHarnessWith(cfg.JetStream, ev, broker,
			func(ctx context.Context, c jetstream.Config) (*mq.Client, error) {
				return Open(ctx, Config{Profile: ProfileJetStream, JetStream: c, Require: cfg.Require})
			})
	case ProfileUnset:
		return testkit.Harness{Driver: DriverName, Evidence: ev, Broker: broker}
	default:
		return testkit.Harness{Driver: DriverName, Evidence: ev, Broker: broker}
	}
	h.Driver = DriverName
	// The profile is always spelled out, including for JetStream, whose own
	// harness deliberately leaves it empty because its driver name already is
	// the variant. Here the driver name is the facade's, so the profile is the
	// only thing that tells the two runs apart — and without it the JetStream
	// run would be keyed plain `natsunified`, which reads as THE facade rather
	// than as one of its two profiles.
	h.Profile = cfg.Profile.String()
	// One scenario the kit cannot run against a facade, added to whatever the
	// profile driver already documents as inapplicable.
	h.Skip = withFacadeSkips(h.Skip)
	return h
}

// withFacadeSkips adds the scenarios the kit cannot run against a FACADE to the
// ones the profile driver documents as inapplicable to its BROKER.
//
// There is exactly one, and it is a defect in the kit rather than a behaviour
// this facade lacks. `client/info-and-native` asserts
//
//	Client.Info().Name == Harness.Driver
//
// which is true of every driver and false of a facade by construction. The
// report has to be keyed `natsunified/<profile>` — anything else collides with
// the two reports the profile drivers publish themselves — and the client has to
// report `nats` or `jetstream`, because the specification forbids renaming the
// driver that is really serving the caller. The two cannot both be satisfied,
// and the only way to satisfy the scenario would be to WRAP and rename, which is
// the design this facade exists to avoid.
//
// A skip is never a pass, so everything else that scenario checks — a non-empty
// name, a native escape hatch, Info and Health answering after Close — is held
// by drivers/natsunified's own TestInfoAndNativeAfterClose on both profiles,
// with the one assertion that cannot hold inverted. The kit's own fix is one
// field: a harness that can name the driver its clients will report, separately
// from the name its report is keyed by. Recorded in the maintenance notes with the
// kit as its owner.
func withFacadeSkips(driverSkips map[string]string) map[string]string {
	out := make(map[string]string, len(driverSkips)+1)
	for name, reason := range driverSkips {
		out[name] = reason
	}
	out["client/info-and-native"] = "the kit asserts Client.Info().Name == Harness.Driver, which a facade cannot satisfy: the report is keyed natsunified/<profile> so that it does not collide with the profile drivers' own, and the client reports nats or jetstream because a facade must not rename the driver serving the caller. drivers/natsunified's TestInfoAndNativeAfterClose holds every other assertion of this scenario on both profiles (see the maintenance notes)"
	return out
}
