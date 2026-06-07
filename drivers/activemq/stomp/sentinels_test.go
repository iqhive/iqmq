package stomp

import (
	"errors"
	"strings"
	"testing"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "bitbucket.org/iqhive/iqmq"
)

// gostompClosedUnexpectedly is the library's own "the connection went" error,
// named so that the table below cannot spell it as a string this test invented.
func gostompClosedUnexpectedly() error { return gostomp.ErrClosedUnexpectedly }

// The refusals nothing else references, asserted here because `admin.go` asks
// for exactly this.
//
// Three of this driver's sentinels are unreachable from the portable API by
// design: the topology refusal is not wired to anything ("the point is that
// nothing is"), the lease refusal belongs to a `DeadlineExtender` the driver
// deliberately does not implement, and the token-table refusal is the
// unreachable arm of a bound the buffer and the table share. Each of them is
// still a sentence an operator may one day read, and `admin.go` says why it
// wants one checked: "so that a test can assert the sentence rather than the
// absence".
//
// Asserting them from the driver's own package is also what keeps
// `golangci-lint`'s `unused` check honest. The alternative is a `//nolint` on
// each, which would say "nobody reads this" where the intent is "nobody CALLS
// this, and here is what it says".

// TestTopologyRefusalNamesWhatDoesCreateTheTopology holds the one refusal
// `admin.go` exists for.
//
// STOMP has no topology frame: nothing creates a destination, deletes one,
// purges one or reports its depth, in any version of the specification. An
// operator who reaches `Client.Admin()` and is told "unsupported" needs to be
// told what DOES create the topology, because on Classic the answer is a
// SUBSCRIBE that has to happen before the first publication and on Artemis it
// is another module.
func TestTopologyRefusalNamesWhatDoesCreateTheTopology(t *testing.T) {
	t.Parallel()
	err := adminUnsupported()
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("the topology refusal is %v (code %v), want ErrUnsupported: the specification makes that the answer for a "+
			"feature a driver does not claim", err, mq.CodeOf(err))
	}
	if !errors.Is(err, errNoTopology) {
		t.Errorf("adminUnsupported answers %v rather than the driver's own topology sentinel", err)
	}
	for _, must := range [...]string{"drivers/activemq", "console"} {
		if !strings.Contains(err.Error(), must) {
			t.Errorf("the refusal does not mention %q, so an operator is told what cannot be done and not what "+
				"can: %q", must, err.Error())
		}
	}
}

// TestLeaseRefusalSaysThereIsNoLeaseToExtend is the sentence a caller who
// found `mq.DeadlineExtender` somewhere else would look for here.
//
// STOMP grants no lease at all: a delivery is this subscription's until the
// subscription goes away, so there is no deadline and nothing expires. That is
// a different statement from "extending is not implemented", and the
// difference decides whether a caller should shorten its handlers.
func TestLeaseRefusalSaysThereIsNoLeaseToExtend(t *testing.T) {
	t.Parallel()
	if !errors.Is(errNoExtend, mq.ErrUnsupported) {
		t.Errorf("the lease refusal is %v (code %v), want ErrUnsupported", errNoExtend, mq.CodeOf(errNoExtend))
	}
	if !strings.Contains(errNoExtend.Error(), "no lease") {
		t.Errorf("the refusal does not say that there is no lease: %q", errNoExtend.Error())
	}
}

// TestTokenTableRefusalIsBackpressureAndNotAFailure is the arm the accounting
// makes unreachable, and it is worth a sentence anyway.
//
// The bounded buffer's capacity and the token table's are the same number, so
// a delivery can only find every slot in use if a settlement is stuck. The
// code that meets it drops the delivery rather than acknowledging it, which is
// right — it stays the broker's, unacknowledged — but the sentinel is what a
// future caller of the table would report, and it has to name the prefetch as
// the thing to change.
func TestTokenTableRefusalIsBackpressureAndNotAFailure(t *testing.T) {
	t.Parallel()
	if got := mq.CodeOf(errTokenTableFull); got != mq.CodeResourceExhausted {
		t.Errorf("the token-table refusal's code is %v, want %v: a caller retries an exhausted resource and gives "+
			"up on an invalid argument", got, mq.CodeResourceExhausted)
	}
	if !strings.Contains(errTokenTableFull.Error(), "prefetch") {
		t.Errorf("the refusal does not name the prefetch, which is the only thing a caller can change: %q",
			errTokenTableFull.Error())
	}
}

// TestConnectionDeathIsNotABrokersSentence pins `isConnGone`'s rule, which is
// the whole of this driver's diagnosis and is unusual enough to be worth a
// test of its own.
//
// go-stomp turns a broker's ERROR frame into a plain error carrying the
// frame's `message` header, so there is no type to match and no code to read:
// STOMP has neither. A broker's refusal is therefore identified NEGATIVELY --
// the broker said something, and what it said is not one of the sentences a
// dead connection produces -- and this table is the second half of that rule.
//
// It matters because the answer decides what a failure means. A connection
// that is gone is retryable on a new one and, on the publish path, AMBIGUOUS;
// a broker's refusal is neither, because the broker answered. A driver that
// mistook the one for the other would either retry a refusal for ever or
// report `Closed` for a message the broker may be holding.
func TestConnectionDeathIsNotABrokersSentence(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nothing at all":                {err: nil, want: false},
		"Artemis's own refusal":         {err: errors.New("AMQ339001 Destination does not exist orders"), want: false},
		"Classic's poison-ack refusal":  {err: errors.New("Poison ack cannot be transacted"), want: false},
		"a connection that closed":      {err: errors.New("connection already closed"), want: true},
		"go-stomp's synthetic sentence": {err: errors.New("connection closed"), want: true},
		"a socket that went":            {err: errors.New("use of closed network connection"), want: true},
		"a peer that reset":             {err: errors.New("connection reset by peer"), want: true},
		"an EOF":                        {err: errors.New("unexpected EOF"), want: true},
		"go-stomp's unexpected close":   {err: gostompClosedUnexpectedly(), want: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := isConnGone(tc.err); got != tc.want {
				t.Errorf("isConnGone(%v) is %v, want %v. The answer decides whether the driver reports the "+
					"BROKER's sentence or a transport failure, and STOMP offers no other diagnosis at all",
					tc.err, got, tc.want)
			}
		})
	}
}
