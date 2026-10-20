package differential

import (
	"context"
	"math/rand/v2"
	"strconv"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/testkit"
)

// MemoryConfig is how a memory arm differs from the oracle.
//
// The oracle is the memory engine with nothing injected, and it is the only
// arm whose behaviour the relation asserts directly ([KindOracle]). The same
// engine with a fault armed is a SUBJECT: a broker-shaped thing that
// duplicates, reorders or loses, which is what makes the whole pipeline —
// script decode, both runs, the relation — testable without a container. Four
// increments in a row have found a defect in their own apparatus rather than
// in the thing measured, and this is how this one is held to that.
type MemoryConfig struct {
	// Relaxed builds the topic with OrderingRelaxed, which is what the
	// reorder fault needs: memory refuses to reorder a lane whose topic
	// asked for strict order, on the grounds that "a fault that broke a
	// promise would test the test".
	Relaxed bool
	// Arm runs once after the client is open, and is where a fault is
	// armed. It is given the broker rather than the client because the
	// injector lives on the broker.
	Arm func(*memory.Broker) error
}

// MemoryArm builds an arm over the in-process memory engine.
//
// AutoCreate is ON here, unlike the conformance harness's memory arm, and the
// reason is worth stating: this arm exists to be compared against a network
// broker whose topology the network driver's own harness builds, so the
// differential's job is to publish and consume rather than to test topology
// creation. The specification's "no hidden topology creation" is a conformance scenario
// and is not re-tested here.
func MemoryArm(name string, cfg MemoryConfig) Arm {
	topo := memory.TopologyConfig{
		Partitions:  1,
		AckDeadline: 2 * time.Minute,
		Ordering:    memory.OrderingStrict,
	}
	if cfg.Relaxed {
		topo.Ordering = memory.OrderingRelaxed
	}
	return Arm{
		Name: name,
		Harness: testkit.Harness{
			Driver:   memory.DriverName,
			Evidence: testkit.EvidenceInProcess,
			Broker:   testkit.Broker{Image: "in-process", Version: memory.Version},
			Timeout:  time.Minute,
			TopicName: func(purpose string) string {
				// Forty random bits keep two arms of one process from
				// sharing a topic name without a counter shared by the
				// process.
				return "diff." + strconv.FormatUint(rand.Uint64()>>24, 36) + "." + purpose //nolint:gosec // uniqueness, not secrecy
			},
			Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
				t := topo
				if n.AckDeadline > 0 {
					t.AckDeadline = n.AckDeadline
				}
				if n.Partitions > 0 {
					t.Partitions = n.Partitions
				}
				b, err := memory.NewBroker(memory.Config{AutoCreate: true, DefaultTopology: t})
				if err != nil {
					return nil, err
				}
				client := mq.NewClient(b)
				if cfg.Arm != nil {
					if err := cfg.Arm(b); err != nil {
						_ = client.Close()
						return nil, err
					}
				}
				return client, nil
			},
		},
	}
}

// OracleArm is the memory engine with nothing injected: the reference every
// subject is compared against.
func OracleArm() Arm { return MemoryArm("memory", MemoryConfig{}) }
