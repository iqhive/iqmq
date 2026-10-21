package goredis_test

import (
	"context"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// TestDoPinnedRunsTheWaitWithItsWrites is the commander half of the durable
// boundary: the driver hands a durable publication's XADD and WAITAOF to
// DoPinned as one call, because WAITAOF counts the writes of the connection
// it runs on, and the call has to answer both, in order.
func TestDoPinnedRunsTheWaitWithItsWrites(t *testing.T) {
	_, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	p, ok := goredisCommander(t, srv.Addr()).(interface {
		DoPinned(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error)
	})
	if !ok {
		t.Fatal("the go-redis commander cannot pin a connection, so a durable publication's WAITAOF may run on one that wrote nothing")
	}
	wait, err := streams.WaitAOF(1, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	xadd := streams.XAdd(streams.XAddArgs{Key: "{pinned}", ID: streams.IDAuto, Fields: []string{"f", "v"}})
	res, err := p.DoPinned(context.Background(), []streams.Cmd{xadd, wait})
	if err != nil || len(res) != 2 {
		t.Fatalf("DoPinned = (%d results, %v), want 2 and nil", len(res), err)
	}
	if res[0].Err != nil || streams.CheckXAdd(res[0].Value) != nil {
		t.Fatalf("the XADD answered %v / %v", res[0].Err, streams.CheckXAdd(res[0].Value))
	}
	ack, err := streams.ParseWaitAOF(res[1].Value)
	if res[1].Err != nil || err != nil || !ack.Reached(1, 0) {
		t.Fatalf("the WAITAOF answered %+v / %v / %v, want the local append-only file reached", ack, res[1].Err, err)
	}
}
