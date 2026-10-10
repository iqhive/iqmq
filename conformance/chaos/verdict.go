package chaos

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Run is everything one chaos scenario observed, in a form the verdict is a
// pure function of.
//
// The separation is the point. A chaos scenario is expensive — a broker, a
// Toxiproxy and a workload — and its verdict is the part most likely to be
// wrong, so the verdict is decided by functions of this struct and those
// functions have a table of good and bad Runs beside them in verdict_test.go.
// A verdict nobody has watched reject a defect is assumed not to work.
type Run struct {
	// Scenario is the scenario's name.
	Scenario string
	// Report is the publish-and-delivery accounting.
	Report *Report
	// Faulted is how many operations the injected fault made fail, in any
	// way. It is the anti-vacuity counter: a chaos run in which nothing
	// went wrong has tested nothing, and a Toxiproxy toxic that did not
	// bite is the commonest reason.
	Faulted int
	// Unknown is how many settlements returned ErrSettlementUnknown: the
	// ambiguous acknowledgement.
	Unknown int
	// SettleFailed is how many settlements failed definitely — a lost
	// lease, a refused acknowledgement.
	SettleFailed int
	// Unclassified is how many publish errors the driver reported as
	// neither ambiguous nor one of the definite refusals.
	//
	// The specification requires a driver to "classify retryability and ambiguity
	// accurately", so a non-zero count here is a finding about the driver.
	// The ledger reads such an outcome as AMBIGUOUS, because a connection
	// that died around a write may well have written.
	Unclassified int
	// Resumed is whether the workload made progress again after the fault
	// was removed.
	Resumed bool
	// ResumeAfter is how long the first success after removal took.
	ResumeAfter time.Duration
	// PeakPending is the high-water mark of the publisher's own pending
	// buffer, for the scenario that checks the bound.
	PeakPending int
	// PendingLimit is the bound that buffer was configured with.
	PendingLimit int
	// Codes counts the error codes the workload saw.
	Codes map[mq.ErrorCode]int
	// Degraded is whether Client.Health reported anything but healthy
	// while the fault was in place.
	Degraded bool
	// Drained is whether the final drain CONVERGED: it ended because the
	// topic answered empty on a usable client, rather than because it ran
	// out of budget or the client was closed.
	//
	// It is what makes the ledger's Missing column readable. Against a live
	// JetStream under a reset storm the run reported "missing=8" beside
	// "resumed=false": the client never came back, so the drain could not
	// fetch anything, and eight messages were counted as LOST that had
	// merely never been fetched. A loss claim needs a drain that finished.
	Drained bool
}

// trackPending raises the pending high-water mark from a publisher's own
// statistics, which is what [PendingBounded] reads.
func (r *Run) trackPending(pub interface{ Stats() mq.PublisherStats }) {
	if p := int(pub.Stats().Pending); p > r.PeakPending {
		r.PeakPending = p
	}
}

// Code records one error code the workload saw.
func (r *Run) Code(c mq.ErrorCode) {
	if r.Codes == nil {
		r.Codes = map[mq.ErrorCode]int{}
	}
	r.Codes[c]++
}

// CodeNames is every code seen, sorted, for a failure message.
func (r *Run) CodeNames() []string {
	out := make([]string, 0, len(r.Codes))
	for k := range r.Codes {
		out = append(out, k.String())
	}
	sort.Strings(out)
	return out
}

// String is the line a scenario logs whether or not it passed.
func (r *Run) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: faulted=%d unknown=%d settle-failed=%d unclassified=%d resumed=%v drained=%v",
		r.Scenario, r.Faulted, r.Unknown, r.SettleFailed, r.Unclassified, r.Resumed, r.Drained)
	if r.Resumed {
		fmt.Fprintf(&b, " after=%v", r.ResumeAfter.Round(time.Millisecond))
	}
	if r.PendingLimit > 0 {
		fmt.Fprintf(&b, " pending=%d/%d", r.PeakPending, r.PendingLimit)
	}
	if len(r.Codes) > 0 {
		fmt.Fprintf(&b, " codes=%v", r.CodeNames())
	}
	if r.Report != nil {
		fmt.Fprintf(&b, " ledger(accepted=%d ambiguous=%d rejected=%d delivered=%d duplicates=%d missing=%d)",
			r.Report.Published[Accepted], r.Report.Published[Ambiguous], r.Report.Published[Rejected],
			r.Report.Delivered, r.Report.Duplicates, len(r.Report.Missing))
	}
	return b.String()
}

// Check is one requirement of a Run. It returns nil when the Run satisfies it.
type Check func(*Run) error

// All applies every check and joins the failures, so that one run reports
// everything wrong with it rather than the first thing.
func All(r *Run, checks ...Check) error {
	var errs []error
	for _, c := range checks {
		if err := c(r); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// FaultBit requires that the injected fault actually made something fail.
//
// It is the first check of every scenario, and the reason is that a Toxiproxy
// toxic silently does nothing more often than it does anything: a latency
// toxic on the wrong stream, a proxy name that does not match, a workload
// whose own timeouts are longer than the toxic — all of them produce a green
// run in which nothing was injected. `n` is the minimum, so a scenario can
// require several.
func FaultBit(n int) Check {
	return func(r *Run) error {
		if r.Faulted < n {
			return fmt.Errorf("the fault made %d operation(s) fail and the scenario needs at least %d: nothing was injected, so nothing was tested", r.Faulted, n)
		}
		return nil
	}
}

// NoLoss requires that every accepted publish was delivered.
//
// This is the one property no fault excuses: the specification says an accepted publish is
// the broker's, so a message the broker accepted and never delivered is data
// loss whatever the network did. Duplicates are not loss and are permitted.
func NoLoss() Check {
	return func(r *Run) error {
		if r.Report == nil {
			return errors.New("no ledger: the scenario recorded no publishes at all")
		}
		if !r.Drained && len(r.Report.Missing) > 0 {
			// The drain did not converge, so "missing" means "never
			// fetched" and not "lost". The run is still RED — a client
			// that never came back fails [ResumedWithin], which is the
			// finding — but it must not be red for a loss nobody can
			// substantiate: that is a number three orders of magnitude
			// away from its expectation being reported instead of
			// investigated.
			//
			// Everything else the ledger knows is still checked: a
			// rejected publish that was delivered and an id nobody
			// published are defects whatever the drain did.
			partial := *r.Report
			partial.Missing = nil
			return partial.Error()
		}
		return r.Report.Error()
	}
}

// DrainConverged requires that the final drain ended because the topic was
// empty on a usable client.
//
// It is what a scenario adds when its verdict depends on the ledger's Missing
// column meaning "lost".
func DrainConverged() Check {
	return func(r *Run) error {
		if !r.Drained {
			return errors.New("the final drain did not converge: the client was closed or the budget ran out, so the ledger cannot tell a lost message from one never fetched")
		}
		return nil
	}
}

// Accounted requires that the ledger holds enough to be worth reading.
//
// A ledger with two accepted publishes cannot show duplicate-not-loss, and a
// scenario whose workload never got going produces exactly that.
func Accounted(minAccepted int) Check {
	return func(r *Run) error {
		if r.Report == nil {
			return errors.New("no ledger")
		}
		if got := r.Report.Published[Accepted]; got < minAccepted {
			return fmt.Errorf("the ledger holds %d accepted publish(es) and the scenario needs %d to say anything", got, minAccepted)
		}
		return nil
	}
}

// SettlementUnknownSeen requires at least one ambiguous settlement.
//
// It is the scenario-specific half of "latency > timeout during ack": the
// whole point is to make an acknowledgement's fate unknown, so a run that
// produced no ErrSettlementUnknown injected the wrong thing. The specification says such a
// settlement stays unknown for ever, which is why the companion check is
// [NoLoss] and not "the message was acked".
func SettlementUnknownSeen() Check {
	return func(r *Run) error {
		if r.Unknown == 0 {
			return fmt.Errorf("no settlement returned ErrSettlementUnknown (%d failed definitely): the latency toxic did not straddle the settlement timeout, so the ambiguous-acknowledgement path was never taken", r.SettleFailed)
		}
		return nil
	}
}

// ResumedWithin requires that the workload made progress again within d of the
// fault being removed.
//
// "Bounded reconnect" is the specification, and a bound is the only form the
// claim can take: a driver that reconnects eventually and a driver that never
// reconnects look the same to a test with no deadline.
func ResumedWithin(d time.Duration) Check {
	return func(r *Run) error {
		switch {
		case !r.Resumed:
			return fmt.Errorf("the workload never made progress again after the fault was removed (waited %v)", r.ResumeAfter.Round(time.Millisecond))
		case r.ResumeAfter > d:
			return fmt.Errorf("the workload resumed after %v, over the %v bound", r.ResumeAfter.Round(time.Millisecond), d)
		}
		return nil
	}
}

// PendingBounded requires that the publisher's own buffer never exceeded the
// bound it was configured with.
//
// A bandwidth toxic is the honest way to test it: the publisher cannot get rid
// of messages faster than the pipe, so either the bound holds and the caller
// blocks or is refused, or the buffer grows without limit.
func PendingBounded() Check {
	return func(r *Run) error {
		switch {
		case r.PendingLimit <= 0:
			return errors.New("the scenario did not record the buffer's bound, so it cannot say whether the bound held")
		case r.PeakPending > r.PendingLimit:
			return fmt.Errorf("the publisher held %d pending message(s) against a bound of %d: the buffer is not bounded", r.PeakPending, r.PendingLimit)
		}
		return nil
	}
}

// CodeSeen requires that the workload saw that error code at least once.
//
// The parameter is an [github.com/iqhive/iqmq.ErrorCode] and not its name: a
// scenario that named a code as a string would stop matching silently if the
// code were renamed, and a scenario looking for a code that no longer exists
// is a check that cannot pass.
func CodeSeen(c mq.ErrorCode) Check {
	return func(r *Run) error {
		if r.Codes[c] == 0 {
			return fmt.Errorf("the workload never saw %s; it saw %v", c, r.CodeNames())
		}
		return nil
	}
}

// AnyCodeSeen requires one of several codes, for a property two drivers
// express differently — a deleted topic is ErrTopicNotFound on one broker and
// a degraded health report on another, and both are honest answers.
func AnyCodeSeen(codes ...mq.ErrorCode) Check {
	return func(r *Run) error {
		for _, c := range codes {
			if r.Codes[c] > 0 {
				return nil
			}
		}
		return fmt.Errorf("the workload saw none of %v; it saw %v", names(codes), r.CodeNames())
	}
}

// names renders a code list for a failure message.
func names(codes []mq.ErrorCode) []string {
	out := make([]string, 0, len(codes))
	for _, c := range codes {
		out = append(out, c.String())
	}
	return out
}

// DegradedOrCode requires that a deleted topology is either refused by code or
// reported by Health, which is the specification "topology deletion →
// NotFound/Degraded".
func DegradedOrCode(codes ...mq.ErrorCode) Check {
	return func(r *Run) error {
		if r.Degraded {
			return nil
		}
		if err := AnyCodeSeen(codes...)(r); err != nil {
			return fmt.Errorf("the topology was deleted and the client neither refused an operation nor reported itself degraded: %w", err)
		}
		return nil
	}
}
