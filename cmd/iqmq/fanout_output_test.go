package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
)

// A destination topic whose subscriptions take a publication independently
// (memory's FanoutIndependent) answers mq.ErrPartial when some of them took
// it and the others had no room. The publication is in the broker, on the
// subscriptions that took it, and publishing it again puts a second copy on
// each of them. These drive `bridge` and `pub` into that answer.

// partialDestination is a memory broker whose topic "mirror" has a full
// subscription, "full", and one with room for one message, "room".
func partialDestination(t *testing.T) *memory.Broker {
	t.Helper()
	dest, err := memory.NewBroker(memory.Config{AutoCreate: false, DefaultTopology: memory.TopologyConfig{
		CapacityMessages: 1, Overflow: memory.OverflowReject, Fanout: memory.FanoutIndependent,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dest.Close() })
	ensure(t, dest, "mirror", "full")
	ensure(t, dest, "mirror", "room")
	publish(t, dest, "mirror", &mq.Message{Body: []byte("filler")})
	if got := drain(t, dest, "mirror", "room", 1); len(got) != 1 {
		t.Fatalf("setup: room held %d message(s), want 1", len(got))
	}
	return dest
}

// TestBridgeSettlesAPartialFanout: the bridge used to read a partial fan-out
// as a clean refusal. It printed "the source delivery is being returned, so
// nothing is lost" without a word about the copy the destination already had,
// and requeued the source delivery, so the next run put the message on the
// subscription that had it a second time. A partial fan-out is a publication
// the destination took, so the source delivery is settled, the run says
// which subscriptions do not have the message, and it exits 1.
func TestBridgeSettlesAPartialFanout(t *testing.T) {
	source, err := memory.NewBroker(memory.Config{AutoCreate: false})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	dest := partialDestination(t)
	reg := dsn.NewRegistry()
	for scheme, broker := range map[string]*memory.Broker{srcScheme: source, dstScheme: dest} {
		b := broker
		if err := reg.Register(scheme, func(ctx context.Context, _ string) (*mq.Client, error) {
			return memory.OpenWithClient(ctx, b, memory.ClientBorrowed)
		}); err != nil {
			t.Fatal(err)
		}
	}
	ensure(t, source, "orders", "bridge")
	publish(t, source, "orders", &mq.Message{Body: []byte("one")})

	h := harnessWith(t, reg, nil)
	code := h.run("bridge", "--max", "1", "-o", "json", srcScheme+"://", "orders", "bridge", dstScheme+"://", "mirror")
	if code != exitFailure {
		t.Fatalf("bridge = %d, want %d: some of the destination's subscriptions do not have the message\nstderr:\n%s",
			code, exitFailure, h.stderr())
	}
	var rep struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h.stdout())
	}
	r := rep.Result
	if r["partial"] != 1.0 || r["published"] != 1.0 || r["moved"] != 1.0 || r["publish_failures"] != 0.0 {
		t.Errorf("result = %v, want partial 1, published 1, moved 1 and publish_failures 0", r)
	}
	if strings.Contains(h.stderr(), "nothing is lost") || !strings.Contains(h.stderr(), "only some of the destination's subscriptions") {
		t.Errorf("the partial fan-out was not reported as one:\n%s", h.stderr())
	}
	if got := drain(t, dest, "mirror", "room", 1); len(got) != 1 || string(got[0].Body) != "one" {
		t.Errorf("the subscription with room holds %d message(s), want the one bridged", len(got))
	}
	if n := depth(t, source, "orders", "bridge"); n != 0 {
		t.Errorf("the source still holds %d message(s): the delivery the destination took was returned, "+
			"so the next run publishes it to the subscription that has it again", n)
	}
}

// TestPubNamesAPartialFanout: pub named every failed publication that was not
// ambiguous "refused", which is the word that makes an operator publish it
// again; a partial fan-out is on the subscriptions that took it.
func TestPubNamesAPartialFanout(t *testing.T) {
	dest := partialDestination(t)
	reg := dsn.NewRegistry()
	if err := reg.Register(dstScheme, func(ctx context.Context, _ string) (*mq.Client, error) {
		return memory.OpenWithClient(ctx, dest, memory.ClientBorrowed)
	}); err != nil {
		t.Fatal(err)
	}
	h := harnessWith(t, reg, nil)
	if code := h.run("pub", dstScheme+"://", "mirror", "one"); code != exitFailure {
		t.Fatalf("pub = %d, want %d\nstderr:\n%s", code, exitFailure, h.stderr())
	}
	if got := drain(t, dest, "mirror", "room", 1); len(got) != 1 {
		t.Fatalf("the subscription with room holds %d message(s), want 1", len(got))
	}
	const want = "iqmq pub: message 0 PARTIALLY ACCEPTED"
	if strings.Contains(h.stderr(), "refused") || !strings.Contains(h.stderr(), want) {
		t.Errorf("pub printed\n%s\nwant a line containing %q and no refusal", h.stderr(), want)
	}
}
