package memory

import (
	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// engineErrors maps what the engine returns to the portable error model, the
// way every other driver maps its client's errors.
//
// The engine reports a portable code and nothing more. Only an *mq.OpError
// carries a judgement of retryability, so before this mapping mq.IsRetryable
// answered false for every engine error the driver passed on, including the
// ones the engine documents as retryable. Wrapping keeps the code, so
// errors.Is against a sentinel and mq.CodeOf answer as they did, and the
// engine's error stays reachable through Unwrap.
//
// Every engine refusal is definite: nothing was admitted or settled. The
// mapper still asks mq.IsAmbiguous of each error, so an error whose own code
// says its outcome is unknown stays ambiguous.
var engineErrors = driverkit.ErrorMapper{
	Driver: DriverName,
	Fallback: []driverkit.Rule{
		// A refusal the engine expects a retry to get past: a publication
		// whose dedup key another publication is still admitting (the
		// retry finds the key taken or free), and the publication and
		// settlement refusals of Faults.RejectPublish and
		// Faults.FailSettlement, which admit nothing and leave the lease
		// intact.
		{Match: codeIs(mq.CodeUnavailable), Code: mq.CodeUnavailable, Retryable: true},
		// A lane or the engine at capacity under OverflowReject. Nothing
		// was admitted, and the room comes back as the subscription settles
		// what it holds.
		{Match: codeIs(mq.CodeResourceExhausted), Code: mq.CodeResourceExhausted, Retryable: true},
	},
}

// codeIs returns a rule predicate that matches an error carrying code c.
func codeIs(c mq.ErrorCode) func(error) bool {
	return func(err error) bool { return mq.CodeOf(err) == c }
}

// engineErr maps an error the engine returned for op on topic (and, for a
// consumer-side operation, sub). A nil error stays nil.
func engineErr(op, topic, sub string, err error) error {
	return engineErrors.Map(op, topic, sub, err)
}
