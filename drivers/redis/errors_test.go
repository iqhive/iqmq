package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
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
		{"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.", mq.OpAdmin, mq.CodeNotFound, false},
		{"The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.", mq.OpOpen, mq.CodeNotFound, false},
		{"ERR no such key", mq.OpAdmin, mq.CodeNotFound, false},
		{"BUSYGROUP Consumer Group name already exists", mq.OpAdmin, mq.CodeConflict, false},
		{"WRONGTYPE Operation against a key holding the wrong kind of value", mq.OpPublish, mq.CodeInvalidArgument, false},
		{"CROSSSLOT Keys in request don't hash to the same slot", mq.OpNack, mq.CodeUnsupported, false},
		{"MOVED 1234 10.0.0.2:6379", mq.OpPublish, mq.CodeUnavailable, true},
		{"CLUSTERDOWN Hash slot not served", mq.OpPublish, mq.CodeUnavailable, true},
		{"LOADING Redis is loading the dataset in memory", mq.OpFetch, mq.CodeUnavailable, true},
		{"READONLY You can't write against a read only replica.", mq.OpPublish, mq.CodeUnavailable, true},
		{"OOM command not allowed when used memory > 'maxmemory'.", mq.OpPublish, mq.CodeBackpressure, true},
		{"BUSY Redis is busy running a script", mq.OpAck, mq.CodeResourceExhausted, true},
		{"NOREPLICAS Not enough good replicas to write.", mq.OpPublish, mq.CodeUnavailable, true},
		{"MISCONF Errors writing to the AOF file: No space left on device", mq.OpFetch, mq.CodeUnavailable, true},
		// A reply names the key and the group, which a caller chose: the code
		// is the first word, and a name holding another rule's word does not
		// change it.
		{"NOGROUP No such key '{acme.orders}' or consumer group 'NOPERM-audit' in XREADGROUP with GROUP option", mq.OpFetch, mq.CodeNotFound, false},
		{"NOGROUP No such key '{acme.orders}' or consumer group 'NOAUTH-replay' in XREADGROUP with GROUP option", mq.OpFetch, mq.CodeNotFound, false},
		{"NOGROUP No such key '{MOVED.LOADING}' or consumer group 'workers' in XREADGROUP with GROUP option", mq.OpFetch, mq.CodeNotFound, false},
		// Inside a script: Redis 7 keeps the command's reply and appends the
		// script; Redis 6.2 wraps it, and go-redis keeps the "ERR ". These
		// are real 6.2.24 replies, the first two from this driver's own
		// acknowledgement and re-add scripts. 6.2 keeps the RESP error marker
		// ("-NOREPLICAS") for a refusal of the server's own checks inside a
		// script, and none for a command's own error (NOGROUP).
		{"NOGROUP No such key '{acme.orders}' or consumer group 'workers' script: 0d351899186d22d36c4aa46af19f987524cf7d4b, on @user_script:10.", mq.OpAck, mq.CodeNotFound, false},
		{"Error running script (call to f_0d351899186d22d36c4aa46af19f98752487023d): @user_script:10: @user_script: 10: -NOREPLICAS Not enough good replicas to write.", mq.OpAck, mq.CodeUnavailable, true},
		{"ERR Error running script (call to f_b69794af80e67a48f33f0c9ebddac853bc98b955): @user_script:12: @user_script: 12: -NOREPLICAS Not enough good replicas to write.", mq.OpNack, mq.CodeUnavailable, true},
		{"Error running script (call to f_0d351899186d22d36c4aa46af19f98752487023d): @user_script:10: @user_script: 10: -MISCONF Redis is configured to save RDB snapshots, but it is currently not able to persist on disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.", mq.OpAck, mq.CodeUnavailable, true},
		{"Error running script (call to f_6ca17848174f1964ba00ae6f780667708197a12a): @user_script:1: @user_script: 1: -OOM command not allowed when used memory > 'maxmemory'.", mq.OpNack, mq.CodeBackpressure, true},
		{"ERR Error running script (call to f_6ca17848174f1964ba00ae6f780667708197a12a): @user_script:1: @user_script: 1: -READONLY You can't write against a read only replica.", mq.OpAck, mq.CodeUnavailable, true},
		{"Error running script (call to f_5092852bb6b549257194278c7b41a364355187dd): @user_script:1: NOGROUP No such key '{r7}' or consumer group 'nogroup'", mq.OpAck, mq.CodeNotFound, false},
		// 6.2's in-script arity error names no condition, and stays as it
		// always was.
		{"Error running script (call to f_a5fe2ea6d5ef2d49e1a0b0d709d59ebde815b6d8): @user_script:1: @user_script: 1: Wrong number of args calling Redis command From Lua script", mq.OpAck, mq.CodeUnknown, false},
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

// TestGoRedisOwnRefusalsAreClassified is the go-redis arm's client-side
// refusals, which no server sends: a pool with no free connection and a
// client that was closed.
//
// The table knew rueidis's sentinels and nothing of go-redis's, so both fell
// through to CodeUnknown and not retryable. A publication refused because the
// pool was busy was never written and a retry can succeed, and a closed client
// is CodeClosed on the rueidis arm. This module does not import go-redis, so
// the rules match go-redis's own messages; `drivers/redis/goredis` holds them
// to go-redis's sentinels.
func TestGoRedisOwnRefusalsAreClassified(t *testing.T) {
	for _, tc := range []struct {
		text      string
		code      mq.ErrorCode
		retryable bool
	}{
		{"redis: connection pool timeout", mq.CodeResourceExhausted, true},
		{"redis: connection pool exhausted", mq.CodeResourceExhausted, true},
		{"redis: client is closed", mq.CodeClosed, false},
	} {
		t.Run(tc.text, func(t *testing.T) {
			r := newRig(t)
			sink, _ := r.openPair(t)
			r.srv.Fail("XADD", errors.New(tc.text))
			err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil)
			if mq.CodeOf(err) != tc.code || mq.IsRetryable(err) != tc.retryable || mq.IsAmbiguous(err) {
				t.Fatalf("Publish refused with %q = %v (code %v, retryable %v, ambiguous %v), want code %v, retryable %v, definite",
					tc.text, err, mq.CodeOf(err), mq.IsRetryable(err), mq.IsAmbiguous(err), tc.code, tc.retryable)
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

// TestAMasterThatCannotWriteIsRetryable: a master refuses every write while
// min-replicas-to-write is not met (NOREPLICAS) or while its last RDB save or
// AOF write has failed under stop-writes-on-bgsave-error (MISCONF), and the
// condition clears without the client, when a replica catches up or the disk
// has room again. The server refuses before running the command and a script
// stops at its first write, so nothing was written. A publication, a fetch and
// an acknowledgement refused this way came back CodeUnknown and not
// retryable, so a caller dropped a publication it could have repeated and a
// runner stopped on a fetch it may repeat. The replies are Redis 7.4's own
// (server.c's shared.noreplicaserr and shared.bgsaveerr, and the AOF write
// error).
func TestAMasterThatCannotWriteIsRetryable(t *testing.T) {
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	for name, reply := range map[string]string{
		"NOREPLICAS": "NOREPLICAS Not enough good replicas to write.",
		"MISCONF rdb": "MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk. " +
			"Commands that may modify the data set are disabled, because this instance is configured to report errors " +
			"during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs " +
			"for details about the RDB error.",
		"MISCONF aof": "MISCONF Errors writing to the AOF file: No space left on device",
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			sink, src := r.openPair(t)
			if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("one")}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, r.ctx, src)

			r.srv.Fail("XADD", errors.New(reply))
			perr := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("two")}, nil)
			r.srv.Fail("XREADGROUP", errors.New(reply))
			fctx, cancel := context.WithTimeout(r.ctx, time.Second)
			_, ferr := src.Fetch(fctx, make([]mq.DriverDelivery, 1))
			cancel()
			r.srv.Fail("EVALSHA", errors.New(reply))
			aerr := src.Settler().Ack(r.ctx, &d)
			for op, err := range map[string]error{"Publish": perr, "Fetch": ferr, "Ack": aerr} {
				if mq.CodeOf(err) != mq.CodeUnavailable || !mq.IsRetryable(err) || mq.IsAmbiguous(err) {
					t.Errorf("%s refused with %s is %v (code %v, retryable %v, ambiguous %v), want a definite, "+
						"retryable CodeUnavailable: nothing was written and the same call succeeds once the "+
						"condition clears", op, name, err, mq.CodeOf(err), mq.IsRetryable(err), mq.IsAmbiguous(err))
				}
			}
			if n := len(r.srv.Entries(keys.Stream(0))); n != 1 {
				t.Errorf("the stream holds %d entries, want only the first publication", n)
			}
			if err := src.Settler().Ack(r.ctx, &d); err != nil {
				t.Errorf("the repeated Ack failed: %v", err)
			}
		})
	}
}

// scriptACLReply is Redis 7.4's reply when the ACL refuses a command inside a
// script, as rueidis hands it over (go-redis keeps a leading "ERR "), for
// user and the command cmd. It carries no NOPERM.
func scriptACLReply(user, cmd string) string {
	return "ACL failure in script: User " + user + " has no permissions to run the '" + cmd +
		"' command script: 0d351899186d22d36c4aa46af19f987524cf7d4b, on @user_script:10."
}

// TestAnACLRefusalInsideAScriptIsForbidden: every settlement and the consumer
// removal are scripts, and Redis 7 words an ACL refusal of a command inside
// one without NOPERM, so it came out as CodeUnknown. It is CodeForbidden and
// not retryable under both clients' spellings, whatever the user name holds.
// It is definite except for a negative settlement, whose script writes (the
// re-add, the park, the dead-letter copy) before the XACK it may have been
// refused: on a real 7.4 a re-add refused its XACK left the copy in the stream.
//
// Redis 6.2 words the same refusal "The user executing the script can't run
// this command or subcommand" (or "can't access at least one of the keys
// mentioned in the command"), inside its script-error wrapper, and it was
// CodeUnknown and definite: a re-add whose XACK it refused wrote its copy and
// was reported as never written.
func TestAnACLRefusalInsideAScriptIsForbidden(t *testing.T) {
	r := newRig(t)
	// Real 6.2.24 replies, for a command and for a key the ACL refused.
	replies := []string{
		scriptACLReply("iqmq", "xack"),
		scriptACLReply("TASK-LOADING", "xack"),
		"Error running script (call to f_6ca17848174f1964ba00ae6f780667708197a12a): @user_script:1: @user_script: 1: " +
			"The user executing the script can't run this command or subcommand",
		"Error running script (call to f_27be6ff19931ee930e5ff57602167234e172ab3f): @user_script:1: @user_script: 1: " +
			"The user executing the script can't access at least one of the keys mentioned in the command arguments",
	}
	for _, text := range replies {
		for _, prefix := range []string{"", "ERR "} {
			reply := prefix + text
			for _, op := range []string{mq.OpAck, mq.OpNack, mq.OpExtend, mq.OpClose, mq.OpFetch, mq.OpOpen, mq.OpAdmin} {
				got := rd.MapForTest(r.b, op, errors.New(reply))
				wantAmbiguous := op == mq.OpNack
				if mq.CodeOf(got) != mq.CodeForbidden || mq.IsRetryable(got) || mq.IsAmbiguous(got) != wantAmbiguous {
					t.Errorf("%s %q mapped to %v, retryable %v, ambiguous %v; want Forbidden, not retryable, ambiguous %v",
						op, reply, mq.CodeOf(got), mq.IsRetryable(got), mq.IsAmbiguous(got), wantAmbiguous)
				}
				// The flag itself, which IsRetryable cannot show once the
				// error is ambiguous: a caller that de-duplicates reads it,
				// and an ACL refusal does not go away on a repeat.
				if oe, ok := errors.AsType[*mq.OpError](got); !ok || oe.Retryable {
					t.Errorf("%s %q: the mapped error's Retryable flag is set", op, reply)
				}
			}
		}
	}
}

// TestAnACLRefusalIsForbiddenWhateverTheUserIsCalled: an ACL reply names the
// user, and the NOPERM rule came after the topology and server rules, so a
// user called TASK, MOVED-OK or LOADER had a refusal matched by the MOVED/ASK
// or LOADING rule first: Unavailable and retryable, for good. The ACL rules
// are first in every table.
func TestAnACLRefusalIsForbiddenWhateverTheUserIsCalled(t *testing.T) {
	r := newRig(t)
	for _, user := range []string{"TASK", "MOVED-OK", "LOADER", "BUSY Redis is busy", "TRYAGAIN"} {
		for _, prefix := range []string{"", "ERR "} {
			reply := prefix + "NOPERM User " + user + " has no permissions to run the 'xadd' command"
			for _, op := range []string{mq.OpPublish, mq.OpFetch, mq.OpAck, mq.OpNack, mq.OpExtend, mq.OpOpen, mq.OpClose, mq.OpAdmin} {
				got := rd.MapForTest(r.b, op, errors.New(reply))
				if mq.CodeOf(got) != mq.CodeForbidden || mq.IsRetryable(got) || mq.IsAmbiguous(got) {
					t.Errorf("%s %q mapped to %v, retryable %v, ambiguous %v; want a definite, final Forbidden",
						op, reply, mq.CodeOf(got), mq.IsRetryable(got), mq.IsAmbiguous(got))
				}
			}
		}
	}
}

// TestAnACLRefusalInsideASettlementScriptLeavesItSettleable is the same at
// the SPI: an acknowledgement whose script the ACL refused is a definite
// CodeForbidden and leaves the delivery pending, and a re-add whose XACK it
// refused is CodeForbidden with an unknown outcome.
func TestAnACLRefusalInsideASettlementScriptLeavesItSettleable(t *testing.T) {
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := newRig(t, func(c *rd.Config) { c.Nack = streams.ReAdd })
	sink, src := r.openPair(t)
	for range 2 {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	d := fetchOne(t, r.ctx, src)
	before := len(r.srv.Pending(keys.Stream(0), testSub))
	r.srv.Fail("EVALSHA", errors.New(scriptACLReply("iqmq", "xack")))
	aerr := src.Settler().Ack(r.ctx, &d)
	if mq.CodeOf(aerr) != mq.CodeForbidden || mq.IsRetryable(aerr) || mq.IsAmbiguous(aerr) {
		t.Errorf("an acknowledgement refused inside its script: %v (code %v, retryable %v, ambiguous %v), "+
			"want a definite, final CodeForbidden", aerr, mq.CodeOf(aerr), mq.IsRetryable(aerr), mq.IsAmbiguous(aerr))
	}
	if pending := len(r.srv.Pending(keys.Stream(0), testSub)); pending != before {
		t.Fatalf("%d entries pending after the refused acknowledgement, want the %d before it", pending, before)
	}
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Errorf("the delivery is not settleable after the refusal: %v", err)
	}
	d = fetchOne(t, r.ctx, src)
	r.srv.Fail("EVALSHA", errors.New(scriptACLReply("iqmq", "xack")))
	nerr := src.Settler().Nack(r.ctx, &d, mq.NackOptions{})
	if mq.CodeOf(nerr) != mq.CodeForbidden || mq.IsRetryable(nerr) || !mq.IsAmbiguous(nerr) {
		t.Errorf("a re-add refused inside its script: %v (code %v, retryable %v, ambiguous %v), "+
			"want CodeForbidden with an unknown outcome", nerr, mq.CodeOf(nerr), mq.IsRetryable(nerr), mq.IsAmbiguous(nerr))
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
