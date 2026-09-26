package reqreply_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/reqreply"
)

// FuzzReqReplyCorrelation is the API's "fuzzed for
// reply-at-most-once and timeout cleanup", driven through the PUBLIC API over
// the real memory driver rather than against the correlation store directly —
// internal/corr already has its own targets, and what is unproven is the
// client built on top of it.
//
// The script is a byte string, one operation per byte:
//
//	'r' or anything unlisted  a request the responder answers
//	's'                       a stray reply carrying a fuzz-supplied id
//	'd'                       a REPLAY of the previous reply, id and all
//	't'                       a request nothing answers, with a short deadline
//
// The invariants, all four checked after every operation:
//
//   - reply at most once: a replayed reply matches nothing, because the
//     entry was removed when the first copy arrived. A client that left the
//     entry in place would deliver one answer twice, and the second delivery
//     would satisfy a LATER request that happened to reuse the id.
//   - no cross-matching: a stray correlation id — another client's, a
//     responder's invention, arbitrary bytes — never wakes one of our
//     waiters.
//   - every reply is settled, matched or not: an unsettled delivery holds a
//     lease and is redelivered for ever.
//   - timeout cleanup: Pending returns to zero, always. That is the leak
//     this design can have.
func FuzzReqReplyCorrelation(f *testing.F) {
	f.Add([]byte("rrr"), "corr-1")
	f.Add([]byte("rd"), "")
	f.Add([]byte("rsd"), "x")
	f.Add([]byte("t"), "")
	f.Add([]byte("rtrd"), "0")
	f.Add([]byte("sssss"), "\x00\x01")
	f.Add([]byte(""), "")
	f.Add([]byte("rrrrdddd"), "...")
	f.Add([]byte("s"), "1")

	f.Fuzz(func(t *testing.T, script []byte, strayID string) {
		if len(script) > 24 {
			script = script[:24]
		}
		cl, b := memory.NewForTest(t, memory.TestTopics("rpc", "replies"))
		var unmatched atomic.Int64
		rr, err := reqreply.New(context.Background(), cl, reqreply.Config{
			ReplyTopic:  "replies",
			Capacity:    64,
			OnUnmatched: func(context.Context, *mq.Delivery) { unmatched.Add(1) },
		}, reqreply.WithEmulatedRequestReply())
		if err != nil {
			t.Fatalf("New = %v", err)
		}
		defer func() { _ = rr.Close() }()

		rc, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "rpc", Subscription: "svc"})
		if err != nil {
			t.Fatalf("opening the request consumer: %v", err)
		}
		defer func() { _ = rc.Close() }()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// lastReply records the id and body of the most recent reply, so
		// that 'd' can replay it byte for byte.
		var lastMu atomic.Pointer[replyRecord]
		answer := reqreply.Serve(cl, func(_ context.Context, d *mq.Delivery) (*mq.Message, error) {
			if string(d.Body) == "never" {
				return noReply, nil
			}
			lastMu.Store(&replyRecord{
				id:   d.Properties.CorrelationID,
				body: append([]byte("re: "), d.Body...),
			})
			return &mq.Message{Body: append([]byte("re: "), d.Body...)}, nil
		})
		go func() { _ = rc.Run(ctx, answer) }()

		wantUnmatched := int64(0)
		for i, op := range script {
			switch op {
			case 's':
				// A stray reply: an id that is not one of ours.
				m := &mq.Message{Body: []byte("stray"), Properties: mq.Properties{CorrelationID: strayID}}
				if err := cl.Publish(ctx, "replies", m); err != nil {
					t.Fatalf("op %d: publishing a stray: %v", i, err)
				}
				wantUnmatched++
			case 'd':
				last := lastMu.Load()
				if last == nil {
					continue
				}
				m := &mq.Message{Body: last.body, Properties: mq.Properties{CorrelationID: last.id}}
				if err := cl.Publish(ctx, "replies", m); err != nil {
					t.Fatalf("op %d: replaying a reply: %v", i, err)
				}
				wantUnmatched++
			case 't':
				rctx, rcancel := context.WithTimeout(ctx, 5*time.Millisecond)
				_, err := rr.Request(rctx, "rpc", &mq.Message{Body: []byte("never")})
				rcancel()
				if err == nil {
					// The responder may have answered before the deadline;
					// that is allowed and not a failure of the invariant.
					continue
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("op %d: a request nothing answers = %v, want context.DeadlineExceeded", i, err)
				}
			default:
				body := "req-" + strconv.Itoa(i)
				rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
				reply, err := rr.Request(rctx, "rpc", &mq.Message{Body: []byte(body)})
				rcancel()
				if err != nil {
					t.Fatalf("op %d: Request = %v, want the reply", i, err)
				}
				if got, want := string(reply.Body), "re: "+body; got != want {
					t.Fatalf("op %d: reply = %q, want %q: a reply reached the wrong waiter", i, got, want)
				}
			}
			// Timeout cleanup, after every operation and not only at the
			// end: an entry that outlives its request is the leak.
			if p := rr.Pending(); p != 0 {
				t.Fatalf("op %d (%q): %d correlation entries outstanding, want 0", i, string(op), p)
			}
		}

		// Every stray and every replay must have been REFUSED -- a match
		// would be a reply delivered twice or to the wrong waiter -- and
		// every one of them settled.
		waitUntil(t, 5*time.Second, func() bool { return unmatched.Load() >= wantUnmatched })
		if got := unmatched.Load(); got != wantUnmatched {
			t.Fatalf("%d replies matched nothing, want %d: a stray or a replayed reply woke a waiter", got, wantUnmatched)
		}
		waitUntil(t, 5*time.Second, func() bool {
			s, err := b.Inspect("replies", "replies-rr")
			return err == nil && s.InFlight == 0 && s.Ready == 0
		})
		s, err := b.Inspect("replies", "replies-rr")
		if err != nil {
			t.Fatalf("inspecting the reply subscription: %v", err)
		}
		if s.InFlight != 0 {
			t.Fatalf("reply subscription = %+v, want nothing in flight: every reply is settled, matched or not", s)
		}
		if p := rr.Pending(); p != 0 {
			t.Fatalf("%d correlation entries survived the script, want 0", p)
		}
	})
}

// replyRecord is the last reply the responder produced, so that a 'd'
// operation can replay it exactly.
type replyRecord struct {
	id   string
	body []byte
}

// waitUntil polls cond until it holds or the deadline passes. It does not
// fail: the caller asserts on the condition itself, so that the failure
// message is the invariant's and not the wait's.
func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
}
