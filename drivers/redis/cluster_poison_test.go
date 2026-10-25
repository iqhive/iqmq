package redis_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// TestAnUndecodableEntryIsDeadLettered is the entry the reclaim sweep claimed
// for ever.
//
// The source used to leave an entry it could not decode pending, on the
// reasoning that MaxAttempts would dead-letter it. MaxAttempts is the core's
// and counts the deliveries a handler sees, and this one never reaches a
// handler, so nothing ever settled it. With a dead-letter stream configured
// it is now moved there at once, as a Reject would move it.
func TestAnUndecodableEntryIsDeadLettered(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.DeadLetterSuffix = rd.DefaultDeadLetterSuffix })
	_, src := r.openPair(t)
	keys, _ := streams.NewKeys(testTopic, 1)
	if _, err := r.srv.Do(r.ctx, streams.Cmd{
		Tokens: []string{"XADD", keys.Stream(0), "*", streams.FieldBody, "x", streams.FieldBody, "y"}, KeyStart: 1, KeyCount: 1,
	}); err != nil {
		t.Fatalf("XADD: %v", err)
	}
	ctx, cancel := context.WithTimeout(r.ctx, 200*time.Millisecond)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, _ := src.Fetch(ctx, dst); n != 0 {
		t.Fatalf("Fetch handed out %d deliveries for an entry that does not decode", n)
	}
	if n := len(r.srv.Pending(keys.Stream(0), testSub)); n != 0 {
		t.Fatalf("the undecodable entry is still pending (%d): the sweep will claim it again for ever", n)
	}
	if n := len(r.srv.Entries(keys.Stream(0) + rd.DefaultDeadLetterSuffix)); n != 1 {
		t.Fatalf("the dead-letter stream holds %d entries, want 1", n)
	}
}

// TestScriptCacheRecoveryDoesNotDependOnScriptLoad is the cluster's empty
// script cache.
//
// The recovery from NOSCRIPT used to be SCRIPT LOAD and the same EVALSHA
// again. SCRIPT LOAD names no key, so a cluster client sends it to whichever
// node it likes, and the node that answered NOSCRIPT stays empty: every
// settlement on that node's slots then failed until something else loaded the
// script there. A SCRIPT LOAD that does not reach the empty cache is what the
// fault below stands for, and the settlement must still land.
func TestScriptCacheRecoveryDoesNotDependOnScriptLoad(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	r.srv.Fail("EVALSHA", fakestreams.ErrNoScript)
	r.srv.Fail("SCRIPT LOAD", errors.New("MOVED: loaded on another node"))
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Fatalf("Ack did not recover from an empty script cache that SCRIPT LOAD cannot reach: %v", err)
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	if n := len(r.srv.Pending(keys.Stream(0), testSub)); n != 0 {
		t.Fatalf("the entry is still pending after a recovered acknowledgement (%d)", n)
	}
}

// clusterCommander is a commander that says it fronts a cluster: it sends
// everything to its first node, as a cluster client sends a keyless command to
// whichever node it likes, and lists every node when asked.
type clusterCommander struct {
	*fakestreams.Server
	nodes []streams.Commander
}

func (c clusterCommander) Nodes(context.Context) ([]streams.Commander, error) { return c.nodes, nil }

// TestListTopicsScansEveryClusterNode is the SCAN a cluster client sent to one
// node.
//
// SCAN names no key, and a cursor means something only to the node that
// issued it, so one iteration through a cluster client listed one node's
// topics. The listing now runs a SCAN on each node. The first node is listed
// twice, the way a node list that includes a replica holds the same keys
// twice, and its topic must still be reported once.
func TestListTopicsScansEveryClusterNode(t *testing.T) {
	first := fakestreams.New(fakestreams.Options{AOF: true})
	second := fakestreams.New(fakestreams.Options{AOF: true})
	open := func(cmder streams.Commander) *rd.Backend {
		b, err := rd.BackendWithCommander(context.Background(), cmder, driverkit.ClientBorrowed, rd.Config{
			AutoCreate: true, Clock: fakeClock{first},
		})
		if err != nil {
			t.Fatalf("BackendWithCommander: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	for cmder, topic := range map[streams.Commander]string{first: "alpha", second: "beta"} {
		adm, err := open(cmder).Admin()
		if err != nil {
			t.Fatalf("Admin: %v", err)
		}
		if err := adm.EnsureTopic(context.Background(), mq.TopicSpec{Name: topic}); err != nil {
			t.Fatalf("EnsureTopic(%s): %v", topic, err)
		}
	}
	adm, err := open(clusterCommander{Server: first, nodes: []streams.Commander{first, second, first}}).Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	var names []string
	for topic, err := range adm.ListTopics(context.Background()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		names = append(names, topic)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"alpha", "beta"}) {
		t.Fatalf("ListTopics over a two-node cluster reported %v, want [alpha beta]", names)
	}
}
