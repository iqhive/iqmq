package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	rd "bitbucket.org/iqhive/iqmq/drivers/redis"
	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// TestErrorMappingCoversEveryConditionRedisReports is the table.
//
// Redis has no typed errors: every condition is a reply whose first word
// names it. So the mapping is a prefix table, and this is what keeps it
// honest — each row is a reply text a real server sends, and the code and the
// retryability are what a caller acts on.
//
// The retryability column is the one worth getting right. A NOGROUP that was
// marked retryable would make a source spin against a consumer group somebody
// destroyed, and an OOM that was not would make a publisher give up on a
// broker that is merely full.
func TestErrorMappingCoversEveryConditionRedisReports(t *testing.T) {
	cases := []struct {
		reply     string
		op        string
		code      mq.ErrorCode
		retryable bool
	}{
		{"NOGROUP No such key or consumer group", mq.OpFetch, mq.CodeNotFound, false},
		{"BUSYGROUP Consumer Group name already exists", mq.OpAdmin, mq.CodeConflict, false},
		{"WRONGTYPE Operation against a key holding the wrong kind of value", mq.OpPublish, mq.CodeInvalidArgument, false},
		{"CROSSSLOT Keys in request don't hash to the same slot", mq.OpNack, mq.CodeUnsupported, false},
		{"MOVED 1234 10.0.0.2:6379", mq.OpPublish, mq.CodeUnavailable, true},
		{"CLUSTERDOWN Hash slot not served", mq.OpPublish, mq.CodeUnavailable, true},
		{"LOADING Redis is loading the dataset in memory", mq.OpFetch, mq.CodeUnavailable, true},
		{"READONLY You can't write against a read only replica.", mq.OpPublish, mq.CodeUnavailable, true},
		{"OOM command not allowed when used memory > 'maxmemory'.", mq.OpPublish, mq.CodeBackpressure, true},
		{"BUSY Redis is busy running a script", mq.OpAck, mq.CodeResourceExhausted, true},
		{"NOSCRIPT No matching script", mq.OpAck, mq.CodeUnavailable, true},
		{"NOAUTH Authentication required.", mq.OpOpen, mq.CodeUnauthorized, false},
		{"WRONGPASS invalid username-password pair", mq.OpOpen, mq.CodeUnauthorized, false},
		{"NOPERM this user has no permissions to run the 'xadd' command", mq.OpPublish, mq.CodeForbidden, false},
		{"ERR Invalid stream ID specified as stream command argument", mq.OpPublish, mq.CodeInvalidArgument, false},
		{"ERR unknown command 'WAITAOF'", mq.OpPublish, mq.CodeUnsupported, false},
		{"ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled", mq.OpPublish, mq.CodeUnsupported, false},
	}
	for _, tc := range cases {
		t.Run(tc.reply[:min(len(tc.reply), 24)], func(t *testing.T) {
			r := newRig(t)
			// The reply is injected as the next failure of the command the
			// operation uses, so the mapping is exercised through the
			// driver's own path rather than through a unit-tested helper.
			got := rd.MapForTest(r.b, tc.op, errors.New(tc.reply))
			if code := mq.CodeOf(got); code != tc.code {
				t.Fatalf("%q mapped to %v, want %v", tc.reply, code, tc.code)
			}
			if retryable := mq.IsRetryable(got); retryable != tc.retryable {
				t.Fatalf("%q mapped to retryable=%v, want %v: a condition whose retryability is "+
					"wrong makes a caller spin or give up for the wrong reason",
					tc.reply, retryable, tc.retryable)
			}
			// The specification requires the native error to remain reachable.
			if !errors.Is(got, got) || got.Error() == "" {
				t.Fatalf("%q mapped to an error with no text", tc.reply)
			}
		})
	}
}

// TestGenericErrorRepliesMapTheSameWithAndWithoutTheERRPrefix is the half of
// the table above that the table above cannot see.
//
// The cases there are the text a SERVER sends. What reaches the mapper is the
// text a CLIENT LIBRARY hands over, and the two libraries this driver runs
// behind disagree about exactly one thing: rueidis strips a leading "ERR "
// from every error reply it decodes and go-redis keeps it. Under rueidis --
// this module's own client -- every rule that named the prefix was therefore
// dead, and an invalid stream identifier came out as CodeUnknown. Feeding the
// mapper only the server's spelling is a check that could not fail on the
// defect it existed to prevent.
func TestGenericErrorRepliesMapTheSameWithAndWithoutTheERRPrefix(t *testing.T) {
	cases := []struct {
		reply string
		code  mq.ErrorCode
	}{
		{"Invalid stream ID specified as stream command argument", mq.CodeInvalidArgument},
		{"The ID specified in XADD is equal or smaller than the target stream top item", mq.CodeInvalidArgument},
		{"wrong number of arguments for 'xadd' command", mq.CodeInvalidArgument},
		{"syntax error", mq.CodeInvalidArgument},
		{"unknown command 'WAITAOF'", mq.CodeUnsupported},
		{"DB index is out of range", mq.CodeInvalidConfig},
		{"SELECT is not allowed in cluster mode", mq.CodeInvalidConfig},
		{"WAITAOF cannot be used when numlocal is set but appendonly is disabled", mq.CodeUnsupported},
	}
	r := newRig(t)
	for _, tc := range cases {
		t.Run(tc.reply[:min(len(tc.reply), 24)], func(t *testing.T) {
			for _, reply := range []string{tc.reply, "ERR " + tc.reply} {
				got := rd.MapForTest(r.b, mq.OpPublish, errors.New(reply))
				if code := mq.CodeOf(got); code != tc.code {
					t.Errorf("%q mapped to %v, want %v", reply, code, tc.code)
				}
			}
		})
	}
}

// TestTransportFailureAfterAWriteIsAmbiguous is the one distinction no reply
// text can make.
//
// A publication whose XADD was written and whose reply never arrived MAY have
// been stored, so its outcome is unknown; a server REPLY means the command
// reached the server and was refused, so it is definite. Getting it backwards
// is how a duplicate comes to look like a loss: a caller told "failed"
// republishes, and the first copy was stored after all.
func TestTransportFailureAfterAWriteIsAmbiguous(t *testing.T) {
	r := newRig(t)
	sink, _ := r.openPair(t)
	r.srv.Fail("XADD", &net.OpError{Op: "write", Err: io.EOF})
	err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil)
	if err == nil {
		t.Fatal("Publish reported success for a write that never got a reply")
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("a publication whose reply never arrived is %v; the specification makes it ambiguous, because "+
			"the server may have stored it and a caller told 'failed' will republish", err)
	}
	// And the other direction: a server reply is DEFINITE, even though the
	// command also reached the server. A refusal reported as ambiguous sends
	// a caller down the de-duplication path for a message that was never
	// stored.
	r.srv.Fail("XADD", errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
	err = sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil)
	if err == nil {
		t.Fatal("Publish reported success for a refused command")
	}
	if mq.IsAmbiguous(err) {
		t.Fatalf("a server refusal is reported as ambiguous: %v", err)
	}
	// And the case the rule table cannot help with: a reply this driver has
	// never seen. It is STILL definite — a reply means the server processed
	// the command and refused it — and without a rule to match it the
	// classification would otherwise take the caller's sentAlready and call
	// it ambiguous, sending a caller down the de-duplication path for a
	// message that was never stored.
	r.srv.Fail("XADD", errors.New("ERR a condition this driver has never heard of"))
	err = sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil)
	if err == nil {
		t.Fatal("Publish reported success for an unknown server refusal")
	}
	if mq.IsAmbiguous(err) {
		t.Fatalf("an unknown server refusal is reported as ambiguous: %v; a reply means the "+
			"server refused the command, whatever the reply says", err)
	}
}

// TestPoisonEntryIsNotDeliveredAsAnEmptyMessage is the poison-message
// route.
//
// An entry this driver cannot decode is in the stream and pending in this
// group, so it cannot be ignored — something has to settle it or it comes
// back for ever. What it must NOT be is delivered: a handler given a message
// that is not the one that was published is worse than one that never
// arrives.
func TestPoisonEntryIsNotDeliveredAsAnEmptyMessage(t *testing.T) {
	r := newRig(t)
	_, src := r.openPair(t)
	keys, _ := streams.NewKeys(testTopic, 1)
	// An entry with a header index that does not run from zero: a field list
	// no correct encoder writes, and one that would silently lose a header
	// if it were accepted.
	if _, err := r.srv.Do(r.ctx, streams.XAdd(streams.XAddArgs{
		Key:    keys.Stream(0),
		ID:     streams.IDAuto,
		Fields: []string{"b", "body", "h:0", "1:kv", "h:2", "1:kv"},
	})); err != nil {
		t.Fatalf("XADD: %v", err)
	}
	dst := make([]mq.DriverDelivery, 1)
	fctx, cancel := context.WithTimeout(r.ctx, 300*time.Millisecond)
	defer cancel()
	n, err := src.Fetch(fctx, dst)
	if n != 0 {
		t.Fatalf("Fetch delivered %d undecodable entries; a handler given a message that is not "+
			"the one that was published is worse than a message that never arrives", n)
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch: %v", err)
	}
	// The specification: it surfaces through health rather than only in a log, because a
	// stream with an undecodable entry in it is a subscription that has
	// stopped making progress and nothing else would say so.
	if r.b.Health().State != mq.HealthDegraded {
		t.Fatalf("health is %v after an undecodable entry; the specification requires a background failure to "+
			"surface somewhere other than a log", r.b.Health().State)
	}
}

// TestSettlementOfAForeignDeliveryIsRefused: ErrNotSettleable rather
// than a panic.
func TestSettlementOfAForeignDeliveryIsRefused(t *testing.T) {
	r := newRig(t)
	_, src := r.openPair(t)
	for _, tc := range []struct {
		name string
		d    *mq.DriverDelivery
	}{
		{"nil", nil},
		{"zero", &mq.DriverDelivery{}},
		{"foreign-ref", &mq.DriverDelivery{Ref: "not ours", Gen: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := src.Settler().Ack(r.ctx, tc.d); !errors.Is(err, mq.ErrNotSettleable) {
				t.Fatalf("Ack of a %s delivery returned %v, want ErrNotSettleable", tc.name, err)
			}
		})
	}
}
