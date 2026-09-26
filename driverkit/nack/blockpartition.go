package nack

import mq "github.com/iqhive/iqmq"

// blockPartition is the reference kind and its default: the record
// is held in process, the partition is paused, and the offset is left
// uncommitted until the record succeeds.
//
// It is the only one of the four that changes neither ordering nor
// durability, which is why ADR-010's SafeOnly emulation policy reaches it and
// not the others. The cost is the one the criterion column measures: the
// partition makes no progress while the record is being retried, so the
// subscription's lag grows by whatever the handler's retries take.
//
// The pause matters as much as the uncommitted offset. Without it the driver
// keeps fetching a partition it is not going to dispatch from, which turns a
// slow retry into a full fetch buffer and then into memory growth the specification
// forbids.
type blockPartition struct{ cfg Config }

// Plan holds the record in place, or takes ADR-011's failure route once its
// attempts are exhausted.
func (s blockPartition) Plan(d Delivery, o mq.NackOptions) (Plan, error) {
	if p, done, err := terminal(s.cfg, o); done {
		return p, err
	}
	if s.cfg.exhausted(d) {
		return failureRoute(s.cfg, o, "attempts exhausted"), nil
	}
	return Plan{
		Op:      OpRetryInPlace,
		Attempt: d.attempt() + 1,
		Delay:   s.cfg.delay(o),
		Pause:   true,
		Reason:  o.Reason,
	}, nil
}

// Kind returns BlockPartition.
func (s blockPartition) Kind() Kind { return BlockPartition }
