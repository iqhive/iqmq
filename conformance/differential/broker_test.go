//go:build integration

package differential

import (
	"context"
	"testing"
	"time"

	"github.com/iqhive/iqmq/conformance/harness"
	jq "github.com/iqhive/iqmq/drivers/jetstream"
	rb "github.com/iqhive/iqmq/drivers/rabbitmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/testkit"
)

// The broker arms the same operation script against the
// memory engine and against a real broker, compared by settled multiset and
// per-key order.
//
// One target per suite, named FuzzDifferential_<suite> so that
// `make differential DRIVER=<suite>` selects exactly one — which is what
// starting ONE broker at a time means in practice on a shared host. They are
// behind the integration tag and are therefore invisible to
// `go test -list '^Fuzz'`, so neither the fuzz ratchet nor `make fuzz-seeds`
// sees them; FuzzDifferentialMemory in memory_test.go is the untagged target
// those two run.
//
// Each target's seed corpus is small on purpose. A seed is a container run of
// the whole script plus its drain, so twenty seeds is a minute of broker time
// per driver; `make differential` runs the seeds, and a real campaign is
// `make fuzz-long PKG=./differential TARGET=FuzzDifferential_jetstream` with a
// broker already up.

// brokerOpts is the timing a container arm needs. Everything is larger than
// the memory arm's: a receive that would be instant in process is a round trip
// here, and a drain that gave up after five seconds would truncate the
// observation rather than compare it.
func brokerOpts() RunOptions {
	return RunOptions{
		ReceiveTimeout:     3 * time.Second,
		AckDeadline:        5 * time.Second,
		DrainBudget:        60 * time.Second,
		MaxDrainDeliveries: 8 * MaxOps,
	}
}

// fuzzBroker is the body of every broker target: probe once, then run each
// input through the pair.
//
// The probe is hoisted out of the fuzz body deliberately. Probe opens a client
// of its own, and a fuzz campaign that re-probed per input would spend most of
// its container time opening connections; caching it on the Arm means one
// probe per target.
func fuzzBroker(f *testing.F, arm Arm) {
	f.Helper()
	pair := &Pair{Oracle: OracleArm(), Subject: arm, Opts: brokerOpts()}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	caps, err := Probe(ctx, &pair.Subject)
	if err != nil {
		f.Fatalf("probing %s: %v", arm.Name, err)
	}
	perm := PermissionsFor(caps)
	f.Logf("%s: %s", arm.Name, perm.Explain())
	if perm.ReorderWithinKey && perm.Duplicates {
		// Honest, and worth a line in the log rather than a skip: for
		// such a driver the differential is a loss, settlement and
		// unknown-id test and not an ordering one, and Summary.OrderedKeys
		// is zero for every run. A reader who cannot see that from the
		// log would take "no divergence" for more than it is.
		f.Logf("%s claims no ordering, so the ordering half of the relation binds nothing here", arm.Name)
	}

	// Before any seed runs, one script is run through the pair and the
	// DEFECT TABLE is applied to the observation the real broker produced.
	//
	// It costs one extra container run of one script and it answers the
	// question the seeds cannot: the relation was proved to reject defects
	// on a hand-written fixture and on a memory run, and neither of those
	// has this broker's id shapes, key handling, redelivery flags or
	// settlement pattern in it. A relation that depended on something only
	// the fixture has would accept every real run whatever it did.
	proveRelationBites(f, pair)

	for _, seed := range seeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		res, err := pair.Run(ctx, DecodeScript(data))
		if err != nil {
			t.Fatalf("running %s: %v", arm.Name, err)
		}
		t.Log(res)
		if err := res.Result.Err(); err != nil {
			t.Fatalf("%s is not a legal reading of the memory engine\n  %s\n  %v\n  oracle:  %+v\n  subject: %+v",
				arm.Name, res, err, res.Oracle, res.Subject)
		}
	})
}

// proveRelationBites runs one script against the real broker and requires the
// relation to reject every defect the transform table can inject into what
// came back.
func proveRelationBites(f *testing.F, pair *Pair) {
	f.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	base, err := pair.Run(ctx, Script{Keys: 3, Ops: []Op{
		{Kind: OpPublish, Key: 0, Action: ActionNackThenAck},
		{Kind: OpPublish, Key: 1, Action: ActionAck},
		{Kind: OpPublish, Key: 0, Action: ActionDrop},
		{Kind: OpPublish, Key: 2, Action: ActionAck},
		{Kind: OpPublish, Key: 0, Action: ActionAck},
		{Kind: OpPublish, Key: 1, Action: ActionAck},
		{Kind: OpReceive, N: 6},
		{Kind: OpReceive, N: 4},
	}})
	if err != nil {
		f.Fatalf("the relation-bites run failed: %v", err)
	}
	if err := base.Result.Err(); err != nil {
		f.Fatalf("%s is not a legal reading of the memory engine on the base script\n  %s\n  %v",
			pair.Subject.Name, base, err)
	}
	f.Logf("relation-bites base run: %s", base)

	applied, accepted := 0, 0
	for _, tr := range Transforms() {
		if tr.Legal {
			continue
		}
		got, changed := tr.Apply(base.Oracle, base.Subject.Clone())
		if !changed {
			continue
		}
		applied++
		if res := Compare(base.Oracle, got, tr.Perm); res.OK() {
			accepted++
			f.Errorf("the relation ACCEPTED %q injected into %s's own observation [%s]",
				tr.Name, pair.Subject.Name, res.Summary)
		}
	}
	if applied < 4 {
		f.Errorf("only %d defect(s) found a target in %s's observation; the base script proved little",
			applied, pair.Subject.Name)
	}
	if accepted == 0 {
		f.Logf("the relation rejected all %d defect(s) injected into %s's own observation", applied, pair.Subject.Name)
	}
}

// seeds is the committed corpus every broker target starts from: a plain
// publish-and-ack, a nack with a redelivery, a drop, and a three-key
// interleave, which between them reach every branch of the relation a broker
// can reach.
func seeds() [][]byte {
	return [][]byte{
		WriteScript(Script{Keys: 1, Ops: []Op{
			{Kind: OpPublish, Action: ActionAck},
			{Kind: OpPublish, Action: ActionAck},
			{Kind: OpReceive, N: 2},
		}}),
		WriteScript(Script{Keys: 2, Ops: []Op{
			{Kind: OpPublish, Key: 0, Action: ActionNackThenAck},
			{Kind: OpPublish, Key: 1, Action: ActionAck},
			{Kind: OpPublish, Key: 0, Action: ActionAck},
			{Kind: OpReceive, N: 3},
			{Kind: OpReceive, N: 2},
		}}),
		WriteScript(Script{Keys: 2, Ops: []Op{
			{Kind: OpPublish, Key: 0, Action: ActionDrop},
			{Kind: OpPublish, Key: 1, Action: ActionAck},
			{Kind: OpReceive, N: 2},
		}}),
		WriteScript(Script{Keys: 3, Ops: []Op{
			{Kind: OpPublish, Key: 0, Action: ActionAck},
			{Kind: OpPublish, Key: 1, Action: ActionAck},
			{Kind: OpPublish, Key: 2, Action: ActionAck},
			{Kind: OpPublish, Key: 0, Action: ActionAck},
			{Kind: OpPublish, Key: 1, Action: ActionAck},
			{Kind: OpPublish, Key: 2, Action: ActionAck},
			{Kind: OpReceive, N: 6},
		}}),
	}
}

// FuzzDifferential_jetstream compares the JetStream driver against the memory
// engine.
func FuzzDifferential_jetstream(f *testing.F) {
	b := harness.NATS(f)
	fuzzBroker(f, Arm{
		Name: "jetstream",
		Harness: jq.ConformanceHarness(
			jq.Config{Addrs: []string{"nats://" + b.Addr}},
			testkit.EvidenceContainer,
			testkit.Broker{Image: b.Image, Version: b.Version},
		),
	})
}

// FuzzDifferential_redis compares the Redis Streams driver against the memory
// engine.
func FuzzDifferential_redis(f *testing.F) {
	b := harness.Redis(f)
	fuzzBroker(f, Arm{
		Name: "redis",
		Harness: rd.ConformanceHarness(
			rd.Config{Addrs: []string{b.Addr}},
			testkit.EvidenceContainer,
			testkit.Broker{Image: b.Image, Version: b.Version},
		),
	})
}

// FuzzDifferential_rabbitmq compares the AMQP 0-9-1 driver against the memory
// engine.
func FuzzDifferential_rabbitmq(f *testing.F) {
	b := harness.RabbitMQ(f)
	fuzzBroker(f, Arm{
		Name: "rabbitmq",
		Harness: rb.ConformanceHarness(
			rb.Config{
				Addrs:    []string{b.Addr},
				Username: b.Auth.User,
				Password: dsn.Secret(b.Auth.Password),
				// The differential's own provisioning is the operator,
				// which is why declaration is on here and off by
				// default in production (the specification is a rule about a
				// deployment).
				Declare: true,
			},
			testkit.EvidenceContainer,
			testkit.Broker{Image: b.Image, Version: b.Version},
		),
	})
}
