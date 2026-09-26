package iqmq_test

import (
	"math"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// TestPublishOptions holds the per-call controls and the value-token
// discipline the specification chose (ADR-006).
func TestPublishOptions(t *testing.T) {
	t.Run("EachTokenSetsItsOwnField", func(t *testing.T) {
		var o mq.PublishOptions
		if err := o.Apply(
			mq.Delay(time.Minute), mq.TTL(time.Hour),
			mq.Priority(7), mq.Partition(3), mq.DedupKey("order-42"),
		); err != nil {
			t.Fatal(err)
		}
		want := mq.PublishOptions{
			Delay: time.Minute, TTL: time.Hour,
			Priority: 7, PrioritySet: true,
			Partition: 3, PartitionSet: true,
			DedupKey: "order-42",
		}
		if o != want {
			t.Fatalf("Apply produced %+v, want %+v", o, want)
		}
	})

	t.Run("SetFlagsDistinguishZeroFromUnset", func(t *testing.T) {
		// Priority zero and partition zero are real values. Without the Set
		// flags a caller who asked for priority 0 would be indistinguishable
		// from one who asked for nothing, and the publisher would either pin
		// every message to partition 0 or ignore the caller.
		var none mq.PublishOptions
		if err := none.Apply(); err != nil {
			t.Fatal(err)
		}
		if none.PrioritySet || none.PartitionSet {
			t.Fatalf("no options set a Set flag: %+v", none)
		}
		var zero mq.PublishOptions
		if err := zero.Apply(mq.Priority(0), mq.Partition(0)); err != nil {
			t.Fatal(err)
		}
		if !zero.PrioritySet || !zero.PartitionSet {
			t.Fatalf("Priority(0) and Partition(0) did not set their flags: %+v", zero)
		}
		if none == zero {
			t.Fatal("asking for priority 0 and asking for nothing are the same option set")
		}
	})

	t.Run("LastTokenWins", func(t *testing.T) {
		var o mq.PublishOptions
		if err := o.Apply(mq.Delay(time.Minute), mq.Delay(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if o.Delay != 2*time.Minute {
			t.Fatalf("Delay = %v, want the last token's 2m", o.Delay)
		}
	})

	t.Run("AppliesOntoDefaults", func(t *testing.T) {
		// PublisherConfig.Defaults: a per-call option overrides one
		// field of a configured default and leaves the rest alone.
		o := mq.PublishOptions{Delay: time.Minute, TTL: time.Hour, DedupKey: "default"}
		if err := o.Apply(mq.TTL(time.Second)); err != nil {
			t.Fatal(err)
		}
		if o.Delay != time.Minute || o.DedupKey != "default" {
			t.Fatalf("Apply cleared a field no token named: %+v", o)
		}
		if o.TTL != time.Second {
			t.Fatalf("TTL = %v, want the token's 1s", o.TTL)
		}
	})

	t.Run("ZeroTokenIsRejected", func(t *testing.T) {
		// the rule: a value token's zero value must not silently mean
		// "a delay of nothing". A slice sized ahead of the options that fill
		// it would otherwise apply a zero token unnoticed.
		var o mq.PublishOptions
		err := o.Apply(mq.PublishOption{})
		if !isCode(err, mq.CodeInvalidArgument) {
			t.Fatalf("the zero PublishOption = %v, want CodeInvalidArgument", err)
		}
		if o != (mq.PublishOptions{}) {
			t.Fatalf("a rejected option set was half applied: %+v", o)
		}
	})

	t.Run("RangesAreChecked", func(t *testing.T) {
		cases := []struct {
			name string
			opt  mq.PublishOption
			ok   bool
		}{
			{"priority low", mq.Priority(0), true},
			{"priority high", mq.Priority(math.MaxUint8), true},
			{"priority over", mq.Priority(math.MaxUint8 + 1), false},
			{"priority under", mq.Priority(-1), false},
			{"partition low", mq.Partition(0), true},
			{"partition high", mq.Partition(math.MaxInt32), true},
			{"partition over", mq.Partition(math.MaxInt32 + 1), false},
			{"partition under", mq.Partition(-1), false},
		}
		for _, c := range cases {
			var o mq.PublishOptions
			err := o.Apply(c.opt)
			if (err == nil) != c.ok {
				t.Errorf("%s: Apply = %v, want ok=%v", c.name, err, c.ok)
			}
			if err != nil && !isCode(err, mq.CodeInvalidArgument) {
				t.Errorf("%s: code = %v, want CodeInvalidArgument", c.name, mq.CodeOf(err))
			}
		}
	})

	t.Run("RejectionIsAllOrNothing", func(t *testing.T) {
		// A half-applied option set is worse than a rejected one: the caller
		// sees an error and the publisher sees a delay it was never asked
		// for.
		o := mq.PublishOptions{TTL: time.Hour}
		err := o.Apply(mq.Delay(time.Minute), mq.Priority(-1), mq.DedupKey("x"))
		if err == nil {
			t.Fatal("Apply accepted an out-of-range priority")
		}
		if o.Delay != 0 || o.DedupKey != "" {
			t.Fatalf("a rejected Apply left %+v: the fold must be all or nothing", o)
		}
		if o.TTL != time.Hour {
			t.Fatalf("a rejected Apply disturbed a field it was not given: %+v", o)
		}
	})
}

// optionFields are the fields a fuzzed option set can name.
const (
	fieldDelay = iota
	fieldTTL
	fieldPriority
	fieldPartition
	fieldDedupKey
	fieldZeroToken

	fuzzFieldCount
)

// dedupKeys is the dedup-key corpus.
var dedupKeys = []string{"", "order-42", "a", "\xff\x00"}

// FuzzPublishOptions is target 12, re-homed to the root from
// x/apibench/b5: an arbitrary sequence of value tokens folded by Apply,
// compared with an independent restatement of the fold.
//
// The invariants: the fold is left-to-right with the last token winning; a
// zero token or an out-of-range value is rejected and leaves the option set
// exactly as it was; and nothing but the fields the tokens named is touched.
func FuzzPublishOptions(f *testing.F) {
	seed := func(fields ...int) []byte {
		w := fuzzutil.NewWriter()
		w.Int(0, 8, len(fields))
		for _, fl := range fields {
			w.Choice(fuzzFieldCount, fl)
			w.Int(math.MinInt32, math.MaxInt32, 42)
			w.Choice(len(dedupKeys), 1)
		}
		return w.Data()
	}
	f.Add(seed())
	f.Add(seed(fieldDelay))
	f.Add(seed(fieldDelay, fieldTTL, fieldPriority, fieldPartition, fieldDedupKey))
	f.Add(seed(fieldZeroToken))
	f.Add(seed(fieldPriority, fieldPriority, fieldPriority))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		n := r.Int(0, 8)
		opts := make([]mq.PublishOption, 0, n)
		// want is the independent restatement of the fold, and wantErr the
		// first refusal it reaches.
		start := mq.PublishOptions{TTL: time.Minute, DedupKey: "start"}
		want := start
		var wantErr bool
		for range n {
			field := r.Choice(fuzzFieldCount)
			num := r.Int(math.MinInt32, math.MaxInt32)
			str := dedupKeys[r.Choice(len(dedupKeys))]
			switch field {
			case fieldDelay:
				opts = append(opts, mq.Delay(time.Duration(num)))
				if !wantErr {
					want.Delay = time.Duration(num)
				}
			case fieldTTL:
				opts = append(opts, mq.TTL(time.Duration(num)))
				if !wantErr {
					want.TTL = time.Duration(num)
				}
			case fieldPriority:
				opts = append(opts, mq.Priority(num))
				if !wantErr {
					if num < 0 || num > math.MaxUint8 {
						wantErr = true
					} else {
						want.Priority, want.PrioritySet = uint8(num), true
					}
				}
			case fieldPartition:
				opts = append(opts, mq.Partition(num))
				if !wantErr {
					if num < 0 {
						wantErr = true
					} else {
						want.Partition, want.PartitionSet = int32(num), true
					}
				}
			case fieldDedupKey:
				opts = append(opts, mq.DedupKey(str))
				if !wantErr {
					want.DedupKey = str
				}
			case fieldZeroToken:
				opts = append(opts, mq.PublishOption{})
				wantErr = true
			}
		}
		got := start
		err := got.Apply(opts...)
		if (err != nil) != wantErr {
			t.Fatalf("Apply over %d tokens = %v, the rules say error=%v", len(opts), err, wantErr)
		}
		if err != nil {
			if mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Fatalf("the refusal carries code %v, want CodeInvalidArgument", mq.CodeOf(err))
			}
			if got != start {
				t.Fatalf("a rejected Apply left %+v, want the option set untouched at %+v", got, start)
			}
			return
		}
		if got != want {
			t.Fatalf("Apply over %d tokens produced %+v, the rules say %+v", len(opts), got, want)
		}
	})
}
