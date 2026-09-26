package goredis

import (
	"context"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceProfile is what this driver's conformance report is filed under.
//
// The report's name is `<driver>-<profile>.json`, and the driver is "redis"
// because that is what this is: the same driver, the same broker, the same
// semantics. The profile names the CLIENT LIBRARY, which is one step along
// from `drivers/natsunified`'s use of the field (a packaging) and is the
// honest place to put it: the whole value of this report is that it can be
// diffed against `redis-streams.json` scenario for scenario, and a report
// filed under a driver name of its own would be compared against nothing and
// would add a row to the generated matrix for a broker that has one.
const ConformanceProfile = "goredis"

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is `drivers/redis`'s harness with three things changed — the client
// identity, the profile, and the Open that makes the run go through go-redis —
// and it is DERIVED from that harness rather than written again. The harness
// encodes which of a scenario's topology Needs this broker can build, which
// scenarios have nothing to observe here, how a Need becomes configuration,
// and how many messages the fidelity scenario sends; every one of those is a
// fact about Redis Streams rather than about a client library, and a second
// copy of them would drift.
//
// It is in a non-test file because two modules run it: this module's own
// env-gated test against a server named by IQMQ_REDIS_ADDR, and
// The conformance suite against the pinned container, which is
// the evidence the generated capability matrix is built from.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	h := rd.ConformanceHarness(cfg, ev, broker)
	h.Profile = ConformanceProfile
	h.Client = testkit.ClientInfo{Module: ClientModule, Version: ClientVersion}
	h.Open = func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
		shaped, err := rd.ConformanceConfigFor(cfg, n)
		if err != nil {
			// A Need this broker cannot build is the shared harness's
			// refusal, and it wraps mq.ErrUnsupported so that the kit turns
			// it into a skip naming what was missing.
			return nil, err
		}
		return Open(ctx, shaped)
	}
	return h
}
