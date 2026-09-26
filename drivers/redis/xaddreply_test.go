package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// This file is the DRIVER's side.
//
// The publish paths used to read XADD's reply with [streams.ParseXAdd] and
// throw the identifier away, and they now read it with [streams.CheckXAdd].
// `internal/streams` holds those two functions to each other, which says the
// swap changed no ANSWER; what that cannot say is that the sink still ACTS on
// each answer, because the sink is what turns a reply into one message's
// outcome at one index. These are the three answers and the index, through the
// real sink:
//
//   - a null reply is NOMKSTREAM refusing to create the stream, and it has to
//     reach the caller as CodeNotFound — the one answer a FEATURE depends on,
//     and the one an "ignore the reply in the batch path" optimisation would
//     silently turn into the successful publication of nothing;
//   - a reply that is not a string, and a string that is not an identifier,
//     are corruption (CodeCorruptMessage): a server, a proxy or a script that
//     answers XADD with "OK" has not told us the entry was stored; and
//   - the result is index-aligned with the batch, so the message whose reply
//     was refused is the message reported as failed and no other.
//
// Everything here drives the real sink over the real command layer, and only
// XADD's replies are substituted. That is the narrowest intervention that
// reaches the branch: how a null came to be at the server is the server's
// business and no part of this property.

// xaddReplier is the fake server with XADD's replies under the test's control.
//
// subst is asked for each XADD in the order they are sent, counting from the
// moment [replyRig] arms it, and returns the reply to substitute and whether to
// substitute at all — so one batch can mix refused replies with real ones,
// which is the only way the index assertion can mean anything. Everything that
// is not an XADD, and every XADD subst declines, goes to the in-memory server
// and comes back untouched.
//
// The counter is armed AFTER the sink is opened, and that is belt and braces
// rather than a fix for anything: MEASURED, by arming at construction instead,
// both batch tests and the single-publish test still pass, because OpenSink on
// this driver sends no XADD of its own. It stays because "position 1 of the
// batch" is a claim about the batch, and a test whose positions are offset by
// whatever the opening happens to send would still fail loudly on the refused
// positions while silently asserting the wrong ones — the opening's command set
// is not this test's property to depend on.
type xaddReplier struct {
	*fakestreams.Server
	subst func(n int) (streams.Value, bool)
	n     int
}

func (x *xaddReplier) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	v, err := x.Server.Do(ctx, cmd)
	if err != nil || cmd.Name() != "XADD" || x.subst == nil {
		return v, err
	}
	n := x.n
	x.n++
	if sub, ok := x.subst(n); ok {
		return sub, nil
	}
	return v, nil
}

func (x *xaddReplier) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	res, err := x.Server.DoMulti(ctx, cmds)
	if err != nil || x.subst == nil {
		return res, err
	}
	for i := range res {
		if i >= len(cmds) || cmds[i].Name() != "XADD" || res[i].Err != nil {
			continue
		}
		n := x.n
		x.n++
		if sub, ok := x.subst(n); ok {
			res[i] = streams.Result{Value: sub}
		}
	}
	return res, nil
}

// substAt substitutes v for the XADD replies at the given positions and leaves
// every other reply alone.
func substAt(v streams.Value, at ...int) func(int) (streams.Value, bool) {
	return func(n int) (streams.Value, bool) {
		for _, i := range at {
			if i == n {
				return v, true
			}
		}
		return streams.Value{}, false
	}
}

// replyRig is a sink over a server whose XADD replies the test substitutes.
// The sink is opened BEFORE the substitution is armed, so the opening's own
// commands cannot consume a position.
func replyRig(t *testing.T, subst func(int) (streams.Value, bool)) mq.Sink {
	t.Helper()
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	x := &xaddReplier{Server: srv}
	ctx := context.Background()
	b, err := rd.BackendWithCommander(ctx, x, driverkit.ClientOwned, rd.Config{
		AckDeadline:   testLease,
		ClaimInterval: time.Hour,
		Block:         time.Millisecond,
		AutoCreate:    true,
		Clock:         fakeClock{srv},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	x.n, x.subst = 0, subst
	return sink
}

// batchErrs returns one error per message of a batch's answer: nil where the
// message was accepted. A single error that is not a *BatchError is the specification's
// collapse of a wholly failed batch, so it stands for every message.
func batchErrs(t *testing.T, err error, n int) []error {
	t.Helper()
	out := make([]error, n)
	if err == nil {
		return out
	}
	var be *mq.BatchError
	if !errors.As(err, &be) {
		for i := range out {
			out[i] = err
		}
		return out
	}
	if len(be.Errs) != n {
		t.Fatalf("BatchError carries %d errors for a batch of %d: the result is not index-aligned with the batch", len(be.Errs), n)
	}
	copy(out, be.Errs)
	return out
}

func TestBatchPublishActsOnEachXAddReplyAtItsOwnIndex(t *testing.T) {
	const n = 4
	refusedAt := []int{1, 3}
	for _, tc := range []struct {
		name  string
		reply streams.Value
		want  mq.ErrorCode
	}{
		{"NOMKSTREAM's null refusal is a missing topic", streams.Nil(), mq.CodeNotFound},
		{"an integer reply is corruption", streams.Int(7), mq.CodeCorruptMessage},
		{"an array reply is corruption", streams.Arr(streams.Str("1-1")), mq.CodeCorruptMessage},
		{"a string that is not an identifier is corruption", streams.Str("OK"), mq.CodeCorruptMessage},
		{"an unparseable sequence is corruption", streams.Str("1767225600123-nope"), mq.CodeCorruptMessage},
		{"an identifier that overflows uint64 is corruption", streams.Str("18446744073709551616-0"), mq.CodeCorruptMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := replyRig(t, substAt(tc.reply, refusedAt...))
			bs, ok := sink.(mq.BatchSink)
			if !ok {
				t.Fatal("the sink is not a BatchSink; FeatureBatchPublish is advertised as native")
			}
			ms := make([]*mq.Message, n)
			for i := range ms {
				ms[i] = &mq.Message{Topic: testTopic, Body: []byte{byte('a' + i)}}
			}
			errs := batchErrs(t, bs.PublishBatch(context.Background(), ms, nil), n)
			for i, err := range errs {
				refused := i == refusedAt[0] || i == refusedAt[1]
				switch {
				case refused && err == nil:
					t.Errorf("message %d: accepted, and its XADD reply was %v", i, tc.reply)
				case refused && mq.CodeOf(err) != tc.want:
					t.Errorf("message %d: code %v (%v), want %v", i, mq.CodeOf(err), err, tc.want)
				case !refused && err != nil:
					t.Errorf("message %d: %v, and its XADD was answered normally; the result is misaligned", i, err)
				}
			}
		})
	}
}

// TestBatchPublishCollapsesAWhollyRefusedBatch is the other half of the index
// property: when EVERY reply is refused with the same outcome, the collapse
// gives one error rather than a BatchError, and it still has to carry the
// reply's own diagnosis.
func TestBatchPublishCollapsesAWhollyRefusedBatch(t *testing.T) {
	sink := replyRig(t, substAt(streams.Nil(), 0, 1, 2))
	ms := []*mq.Message{
		{Topic: testTopic, Body: []byte("a")},
		{Topic: testTopic, Body: []byte("b")},
		{Topic: testTopic, Body: []byte("c")},
	}
	err := sink.(mq.BatchSink).PublishBatch(context.Background(), ms, nil)
	if err == nil {
		t.Fatal("a batch every one of whose XADDs was refused by NOMKSTREAM returned nil")
	}
	var be *mq.BatchError
	if errors.As(err, &be) {
		t.Fatalf("a wholly refused batch with one outcome gave a BatchError (%v); the specification collapses it", err)
	}
	if code := mq.CodeOf(err); code != mq.CodeNotFound {
		t.Errorf("code %v, want CodeNotFound: every XADD was answered with NOMKSTREAM's null", code)
	}
}

// TestSinglePublishActsOnTheXAddReply is the same three answers on the path
// that is not the batch. Both call the same check, so a change to one is a
// change to the other, and the single-message path is the one that could
// legitimately have wanted the identifier.
func TestSinglePublishActsOnTheXAddReply(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply streams.Value
		want  mq.ErrorCode
	}{
		{"NOMKSTREAM's null refusal is a missing topic", streams.Nil(), mq.CodeNotFound},
		{"an array reply is corruption", streams.Arr(streams.Str("1-1")), mq.CodeCorruptMessage},
		{"a string that is not an identifier is corruption", streams.Str("OK"), mq.CodeCorruptMessage},
		{"an identifier is accepted", streams.Str("1767225600123-4"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := replyRig(t, substAt(tc.reply, 0))
			err := sink.Publish(context.Background(), &mq.Message{Topic: testTopic, Body: []byte("a")}, nil)
			switch {
			case tc.want == 0 && err != nil:
				t.Errorf("Publish: %v, want the publication accepted", err)
			case tc.want != 0 && err == nil:
				t.Errorf("Publish accepted a publication whose XADD reply was %v", tc.reply)
			case tc.want != 0 && mq.CodeOf(err) != tc.want:
				t.Errorf("Publish: code %v (%v), want %v", mq.CodeOf(err), err, tc.want)
			}
		})
	}
}
