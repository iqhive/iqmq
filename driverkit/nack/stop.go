package nack

import mq "github.com/iqhive/iqmq"

// stop stops the consumer on the first refused record.
//
// It is the strategy for a subscription where a record that cannot be
// processed is an operational problem rather than a routine one: nothing is
// reordered, nothing is committed past the failure, and the subscription stops
// making progress until somebody looks at it. On a restart the record is
// delivered again, so a poison record is an outage that repeats rather than a
// message that disappears — which is the trade the specification makes explicit.
//
// It still honours an explicit dead-letter or drop action: those are the
// caller saying what to do with this record, not the strategy choosing what to
// do with a failure.
type stop struct{ cfg Config }

// Plan stops the consumer, leaving the offset uncommitted.
func (s stop) Plan(_ Delivery, o mq.NackOptions) (Plan, error) {
	if p, done, err := terminal(s.cfg, o); done {
		return p, err
	}
	return Plan{Op: OpStop, Reason: o.Reason}, nil
}

// Kind returns Stop.
func (s stop) Kind() Kind { return Stop }
