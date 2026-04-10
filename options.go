package iqmq

import (
	"math"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// PublishOptions are the per-message publication controls, as a struct so
// that a batch can carry a different set for every message. The value tokens
// below are sugar over it.
//
// Priority and Partition are paired with an explicit Set flag rather than
// relying on a zero value, for the same reason Limit carries Known: priority
// zero and partition zero are real values a caller may name, and "not named"
// must not be the same answer.
type PublishOptions struct {
	// Delay holds the publication back for this long before it becomes
	// visible.
	Delay time.Duration
	// DeliverAt makes the publication visible at an absolute time. It is an
	// alternative to Delay, not an addition to it.
	DeliverAt time.Time
	// TTL is how long the message stays deliverable.
	TTL time.Duration
	// DedupKey is the key the broker de-duplicates publications by.
	DedupKey string
	// Priority is the message priority, meaningful only when PrioritySet.
	Priority uint8
	// PrioritySet reports whether the caller named a priority.
	PrioritySet bool
	// Partition is the target partition, meaningful only when PartitionSet.
	Partition int32
	// PartitionSet reports whether the caller named a partition.
	PartitionSet bool
}

// publishField names the control a PublishOption sets. The zero value names
// none, which is what makes a stray PublishOption{} an error rather than a
// silent "delay of nothing".
type publishField uint8

const (
	// fieldNone is the zero value and names no control.
	fieldNone publishField = iota
	fieldDelay
	fieldTTL
	fieldPriority
	fieldPartition
	fieldDedupKey

	publishFieldCount
)

// PublishOption is one per-call publication control.
//
// It is a value token and not a closure, which is the winner (ADR-006,
// Value): a call site such as Publish(ctx, m, Delay(time.Minute)) allocates
// nothing, because the option carries no captured environment and the variadic
// array of a call whose arguments do not escape is built on the stack.
//
// The zero PublishOption names no control and is reported as invalid rather
// than applied.
type PublishOption struct {
	field publishField
	num   int64
	str   string
}

// Delay holds the publication back for d before it becomes visible. A
// driver that cannot delay a publication natively rejects it before any I/O
// under the default strict policy.
func Delay(d time.Duration) PublishOption {
	return PublishOption{field: fieldDelay, num: int64(d)}
}

// TTL sets how long the message stays deliverable.
func TTL(d time.Duration) PublishOption {
	return PublishOption{field: fieldTTL, num: int64(d)}
}

// Priority sets the message priority. Valid priorities are 0 to 255;
// applying a priority outside that range fails with CodeInvalidArgument.
func Priority(p int) PublishOption {
	return PublishOption{field: fieldPriority, num: int64(p)}
}

// Partition names the target partition. Valid partitions are 0 to
// math.MaxInt32; applying one outside that range fails with
// CodeInvalidArgument.
func Partition(n int) PublishOption {
	return PublishOption{field: fieldPartition, num: int64(n)}
}

// DedupKey sets the key the broker de-duplicates publications by.
func DedupKey(s string) PublishOption {
	return PublishOption{field: fieldDedupKey, str: s}
}

// Apply folds the tokens into o, left to right, and validates the result. A
// later token for the same control replaces an earlier one.
//
// It reports CodeInvalidArgument for a zero PublishOption and for a value
// outside its range, and it leaves o unchanged when it reports an error, so a
// rejected option set is never half applied.
//
// Apply allocates nothing, and neither does a call site that hands it tokens
// built in the call.
func (o *PublishOptions) Apply(opts ...PublishOption) error {
	next := *o
	for _, opt := range opts {
		switch opt.field {
		case fieldDelay:
			next.Delay = time.Duration(opt.num)
		case fieldTTL:
			next.TTL = time.Duration(opt.num)
		case fieldPriority:
			if opt.num < 0 || opt.num > math.MaxUint8 {
				return errOptionRange
			}
			next.Priority, next.PrioritySet = uint8(opt.num), true
		case fieldPartition:
			if opt.num < 0 || opt.num > math.MaxInt32 {
				return errOptionRange
			}
			next.Partition, next.PartitionSet = int32(opt.num), true
		case fieldDedupKey:
			next.DedupKey = opt.str
		case fieldNone, publishFieldCount:
			return errNoOption
		default:
			return errNoOption
		}
	}
	*o = next
	return nil
}

// The option failures. Like the header failures they have no exported name:
// the sentinel list names none for an option, and errors.Is matches by
// code, so a caller compares CodeOf(err) against CodeInvalidArgument.
var (
	errNoOption    = errcode.New(errcode.InvalidArgument, "iqmq: publish option names no control")
	errOptionRange = errcode.New(errcode.InvalidArgument, "iqmq: publish option value out of range")
)
