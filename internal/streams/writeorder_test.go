package streams_test

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
)

// A script is not a transaction: Redis keeps what a script wrote before a
// command that fails, and an ACL can refuse any one command inside it. So
// every script that moves a message has to write the copy before it removes
// or acknowledges the original, and these tests refuse each command in turn
// (fakestreams.RefuseInScripts, the reply Redis 7 sends) and look at where
// the message is afterwards. A refusal may leave it in two places, which
// at-least-once allows; it must never leave it in none.

// parkOne publishes one entry, reads it and nacks it under zsetmover with no
// delay, which parks it, and returns the member.
func parkOne(t *testing.T, r *rig) string {
	t.Helper()
	r.publish(t, "payload")
	ds := r.read(t)
	if len(ds) != 1 {
		t.Fatalf("read %d deliveries, want 1", len(ds))
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	v, err := r.srv.Do(r.ctx, streams.ZRangeDue(r.keys.Delayed(0), r.srv.Now().UnixMilli(), 8))
	if err != nil {
		t.Fatalf("ZRANGEBYSCORE: %v", err)
	}
	members, err := streams.ParseMembers(v)
	if err != nil || len(members) != 1 {
		t.Fatalf("parked members %v, %v; want one", members, err)
	}
	return members[0]
}

// release runs the release script over member.
func release(r *rig, member string) (streams.Value, error) {
	d, err := streams.UnpackDelayed(member)
	if err != nil {
		return streams.Nil(), err
	}
	args := append([]string{member, "0"}, d.Fields...)
	return r.srv.Do(r.ctx, streams.EvalSHA(streams.SHAOf(streams.ScriptDelayRelease),
		[]string{r.keys.Delayed(0), r.keys.Stream(0)}, args))
}

func refusedInScript(err error) bool {
	return err != nil && strings.Contains(err.Error(), "ACL failure in script")
}

// TestTheReleaseWritesTheCopyBeforeItRemovesTheMember: the release used to
// ZREM the parked member and then XADD it back into the stream, so a user the
// ACL let ZREM and not XADD lost the message: gone from the sorted set,
// never in the stream. It now checks the member is there, asks the ACL the
// ZREM's question by removing nothing, writes the copy and removes the member
// last, so a refused XADD leaves the message parked, and a refused ZREM
// leaves it parked with no copy (rather than a copy every pass).
func TestTheReleaseWritesTheCopyBeforeItRemovesTheMember(t *testing.T) {
	for _, tc := range []struct {
		refuse       string
		wantParked   int
		wantReleased int
	}{
		{"XADD", 1, 0},
		{"ZREM", 1, 0},
		// The read the release starts with: a user the ACL lets run the old
		// release (ZREM, XADD) and not ZSCORE releases nothing, and loses
		// nothing either.
		{"ZSCORE", 1, 0},
		{"", 0, 1},
	} {
		t.Run("refuse "+tc.refuse, func(t *testing.T) {
			r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.ZSetMover })
			member := parkOne(t, r)
			before := len(r.srv.Entries(r.keys.Stream(0)))
			if tc.refuse != "" {
				r.srv.RefuseInScripts(tc.refuse)
			}
			_, err := release(r, member)
			if tc.refuse != "" && !refusedInScript(err) {
				t.Fatalf("the release with %s refused answered %v, want the ACL's refusal", tc.refuse, err)
			}
			if tc.refuse == "" && err != nil {
				t.Fatalf("release: %v", err)
			}
			parked := r.srv.ZLen(r.keys.Delayed(0))
			released := len(r.srv.Entries(r.keys.Stream(0))) - before
			if parked != tc.wantParked || released != tc.wantReleased {
				t.Errorf("with %q refused: %d parked and %d released, want %d and %d (0 and 0 is a lost message)",
					tc.refuse, parked, released, tc.wantParked, tc.wantReleased)
			}
			// A second release of the same member is a racing mover's: it
			// must add nothing once the first has released it.
			if tc.refuse == "" {
				if v, err := release(r, member); err != nil || !v.IsNil() {
					t.Errorf("a second release answered %v, %v, want nothing", v, err)
				}
				if n := len(r.srv.Entries(r.keys.Stream(0))) - before; n != 1 {
					t.Errorf("two releases of one member added %d entries, want 1", n)
				}
			}
		})
	}
}

// TestASettlementScriptWritesTheCopyBeforeItAcknowledges: the re-add, the
// delayed park and the dead-letter move each write the message somewhere
// before they acknowledge the original. A refused write leaves the entry
// pending and writes nothing; a refused acknowledgement leaves the copy
// written and the entry still pending: a duplicate, never a loss.
func TestASettlementScriptWritesTheCopyBeforeItAcknowledges(t *testing.T) {
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	dlq := keys.Stream(0) + ":dlq"
	for _, tc := range []struct {
		name   string
		kind   streams.Kind
		action mq.NackAction
		write  string
		copies func(r *rig) int
	}{
		{"re-add", streams.ReAdd, mq.NackActionRequeue, "XADD", func(r *rig) int { return len(r.srv.Entries(r.keys.Stream(0))) - 1 }},
		{"park", streams.ZSetMover, mq.NackActionRequeue, "ZADD", func(r *rig) int { return r.srv.ZLen(r.keys.Delayed(0)) }},
		{"dead-letter", streams.LeavePending, mq.NackActionDeadLetter, "XADD", func(r *rig) int { return len(r.srv.Entries(dlq)) }},
	} {
		for _, refuse := range []string{tc.write, "XACK"} {
			t.Run(tc.name+"/refuse "+refuse, func(t *testing.T) {
				r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) {
					c.Nack = tc.kind
					c.DeadLetterStream = dlq
				})
				r.publish(t, "payload")
				ds := r.read(t)
				r.srv.RefuseInScripts(refuse)
				err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{Action: tc.action}, r.srv.Now().Add(time.Millisecond))
				if !refusedInScript(err) {
					t.Fatalf("the nack with %s refused answered %v, want the ACL's refusal", refuse, err)
				}
				pending := len(r.srv.Pending(r.keys.Stream(0), testGroup))
				copies := tc.copies(r)
				if pending != 1 {
					t.Errorf("with %s refused the entry is not pending any more (%d copies elsewhere): the "+
						"acknowledgement ran", refuse, copies)
				}
				if want := map[bool]int{true: 0, false: 1}[refuse == tc.write]; copies != want {
					t.Errorf("with %s refused there are %d copies, want %d", refuse, copies, want)
				}
			})
		}
	}
}
