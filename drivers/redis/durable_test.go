package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/rueidis"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// pinningRecorder is a recorder that also offers the one-connection pipeline
// both shipped commanders offer, and keeps each pinned call whole.
type pinningRecorder struct {
	*recorder
	pinned [][]string
}

func (p *pinningRecorder) DoPinned(ctx context.Context, cs []streams.Cmd) ([]streams.Result, error) {
	names := make([]string, len(cs))
	for i := range cs {
		names[i] = cs[i].Name()
	}
	p.mu.Lock()
	p.pinned = append(p.pinned, names)
	p.mu.Unlock()
	return p.DoMulti(ctx, cs)
}

// TestDurablePublishPinsTheWaitToItsWrites pins the durable boundary to the
// connection that wrote. WAITAOF counts the writes of the connection it runs
// on, and it used to go out as a call of its own after the XADDs, which a
// multiplexed or pooled client may run on another connection, one that wrote
// nothing, so the wait answered at once and the publication was confirmed
// durable when it was not.
func TestDurablePublishPinsTheWaitToItsWrites(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	rec := &pinningRecorder{recorder: &recorder{srv: srv}}
	r := newRecordingRig(t, rec, srv)
	sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic, Confirmation: mq.ConfirmDurable})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("one")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	batch := []*mq.Message{{Topic: testTopic, Body: []byte("two")}, {Topic: testTopic, Body: []byte("three")}}
	if err := sink.(mq.BatchSink).PublishBatch(r.ctx, batch, nil); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}

	want := [][]string{{"XADD", "WAITAOF"}, {"XADD", "XADD", "WAITAOF"}}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.pinned) != len(want) {
		t.Fatalf("the durable publications made %d pinned calls %v, want %v", len(rec.pinned), rec.pinned, want)
	}
	for i := range want {
		if len(rec.pinned[i]) != len(want[i]) {
			t.Fatalf("pinned call %d sent %v, want %v", i, rec.pinned[i], want[i])
		}
		for j := range want[i] {
			if rec.pinned[i][j] != want[i][j] {
				t.Fatalf("pinned call %d sent %v, want %v", i, rec.pinned[i], want[i])
			}
		}
	}
}

// TestADurableBoundaryThatFailsAfterTheWriteIsAmbiguous is a WAITAOF that
// fails outright after its XADD landed. The entry is in the stream, so the
// publication was not refused: whether it reached the boundary is what nobody
// knows, as for a WAITAOF that came back short. The reply used to be mapped as
// a definite failure, so a retryable one (LOADING) told the caller to publish
// the entry again, and in a batch it read as the same outcome as a refused
// XADD beside it: the batch collapsed to the refusal, and retrying it
// duplicated the message already in the stream.
func TestADurableBoundaryThatFailsAfterTheWriteIsAmbiguous(t *testing.T) {
	r := newRig(t)
	sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic, Confirmation: mq.ConfirmDurable})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	loading := errors.New("LOADING Redis is loading the dataset in memory")

	r.srv.Fail("WAITAOF", loading)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("stored")}, nil); !mq.IsAmbiguous(err) {
		t.Errorf("Publish with the WAITAOF failing after the XADD landed = %v (IsAmbiguous false, IsRetryable %v), "+
			"want an ambiguous outcome: the entry is in the stream", err, mq.IsRetryable(err))
	}

	r.srv.Fail("XADD", errors.New("READONLY You can't write against a read only replica."))
	r.srv.Fail("WAITAOF", loading)
	batch := []*mq.Message{{Topic: testTopic, Body: []byte("refused")}, {Topic: testTopic, Body: []byte("stored")}}
	err = sink.(mq.BatchSink).PublishBatch(r.ctx, batch, nil)
	var be *mq.BatchError
	if !errors.As(err, &be) || len(be.Errs) != 2 || mq.IsAmbiguous(be.Errs[0]) || !mq.IsAmbiguous(be.Errs[1]) {
		t.Errorf("PublishBatch(refused, stored; WAITAOF failing) = %v, want [a definite refusal, an ambiguous outcome]: "+
			"the second message is in the stream", err)
	}
}

// TestRueidisDoPinnedRunsTheWaitWithItsWrites is the rueidis commander's half:
// its DoPinned takes a dedicated connection, where Do and DoMulti ride the
// shared multiplexed pipelines, and answers the XADD and the WAITAOF in order.
func TestRueidisDoPinnedRunsTheWaitWithItsWrites(t *testing.T) {
	fake := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	srv, err := respsrv.Serve(fake, respsrv.Options{Version: "7.4.2"})
	if err != nil {
		t.Fatalf("respsrv.Serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{srv.Addr()}, DisableCache: true})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	cmder, err := rd.NewCommander(client)
	if err != nil {
		client.Close()
		t.Fatalf("NewCommander: %v", err)
	}
	t.Cleanup(func() { _ = cmder.Close() })
	p, ok := cmder.(interface {
		DoPinned(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error)
	})
	if !ok {
		t.Fatal("the rueidis commander cannot pin a connection")
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
