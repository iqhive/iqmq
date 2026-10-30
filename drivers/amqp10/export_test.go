package amqp10

import (
	"context"

	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
)

// ResolvedForTest is the defaulting [Config.DSN] renders from, for this
// package's tests only.
//
// A rendering writes every default out, so a configuration parsed back from
// one differs from its source wherever the source left a setting at zero.
// Comparing the two RESOLVED configurations field by field is what says the
// rendering lost nothing, where comparing renderings cannot fail when the
// rendering itself is what dropped the setting.
func ResolvedForTest(c Config) Config { return c.resolve() }

// HoldSlotRecordsForTest takes the lock a source keeps its slot records under
// and returns the function that lets it go, for this package's tests only.
//
// Dropping every slot record is the last step of a link loss's invalidation, so
// a test holding this lock stops a link loss part way through it, which is how
// it can ask what a fetch may do before the invalidation is complete.
func HoldSlotRecordsForTest(s *Source) (letGo func()) {
	s.hmu.Lock()
	return s.hmu.Unlock
}

// CreditEpochForTest is the source's credit window epoch, which every reset
// moves on, for this package's tests only.
//
// A connection loss resets the window on the supervisor's goroutine and
// without any lock a test can hold, and the reset changes no number
// [Source.CreditStats] reports when the window held nothing, so the epoch is
// how a test waits for it rather than sleeping.
func CreditEpochForTest(s *Source) uint64 { return s.window.current() }

// TopUpLaterForTest is the first of a Fetch's two steps on its link, for this
// package's tests only: it attaches the source's link, or finds the one it has,
// and returns the second, which issues that link's credit. A non-nil issue
// stands in for the link's own IssueCredit.
//
// A settlement that drops the link can run between the two steps of a Fetch,
// and nothing outside the package can stop a Fetch there, so a test runs the
// steps itself with the settlement in between.
func TopUpLaterForTest(ctx context.Context, s *Source, issue func(n uint32) error) (topUp func() error, err error) {
	link, epoch, err := s.receiver(ctx)
	if err != nil {
		return nil, err
	}
	rcv := link.rcv
	if issue != nil {
		rcv = issueOverride{Receiver: link.rcv, issue: issue}
	}
	return func() error {
		_, err := s.window.topUp(rcv, epoch)
		return err
	}, nil
}

// issueOverride is a receiving link whose IssueCredit is a test's.
type issueOverride struct {
	client.Receiver
	issue func(n uint32) error
}

// IssueCredit is the test's stand-in.
func (r issueOverride) IssueCredit(n uint32) error { return r.issue(n) }
