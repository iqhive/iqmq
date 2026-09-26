// Package differential is the exported oracle for the differential fuzz
// target of the test suite (FuzzDifferential): one operation
// script, run against the memory reference model and against a real broker,
// and a comparison of what the two settled.
//
// The three things such a harness needs, and none of which the per-model
// packages could give it on their own, are here:
//
//   - a Script that is one broker-independent operation alphabet, decoded
//     from fuzz bytes so that the same input produces the same script on
//     both sides, and re-encodable so an interesting input can be committed
//     as a seed;
//   - an Oracle that runs a script against internal/model/memq and returns
//     the Observation the broker must match, with the model's invariants
//     checked after every step; and
//   - Compare, which holds a broker's Observation to the oracle's under the
//     guarantees that broker claims: the settled multiset, the per-key
//     order, and whether duplicates are allowed at all.
//
// The package is importable from every module of this repository, the
// conformance module included: Go's internal rule is applied to the
// importer's import path, and a module of this repository is
// inside the tree rooted at the parent of `internal`. It is *not* part of the
// frozen public API, which is deliberate — a test-only oracle has no business
// in the v1 surface.
//
// The harness that drives a real broker through the public API lives in
// `conformance/differential` and is not this package's business: what the
// broker does with an operation is the driver's semantics, and what may be
// concluded from the difference is Compare's.
package differential

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/model/memq"
)

// Kind is one operation of the portable script alphabet. Every kind is
// something a driver can be asked to do through the public API, which is what
// makes the script broker-independent: there is no Advance of a broker's
// clock, and no inspection of its internals.
type Kind uint8

// The operations.
const (
	// Publish sends one record to the topic under a routing key.
	Publish Kind = iota
	// Fetch takes up to Max deliveries.
	Fetch
	// Ack settles the oldest delivery the run holds.
	Ack
	// Nack returns it for redelivery.
	Nack
	// Expire lets every lease the run holds expire, which a broker does by
	// the passage of time and the model does on demand.
	Expire
	// Wait passes time: the lease deadline on a broker, the model clock in
	// the oracle.
	Wait

	kindCount
)

var kindNames = [kindCount]string{"publish", "fetch", "ack", "nack", "expire", "wait"}

// String returns the lower-case operation name.
func (k Kind) String() string {
	if k < kindCount {
		return kindNames[k]
	}
	return "kind(?)"
}

// Op is one scripted operation.
type Op struct {
	// Kind selects the operation.
	Kind Kind
	// Key indexes the routing key of a Publish: records of one key are
	// delivered in publication order by a broker that claims per-key
	// ordering.
	Key int
	// Size is the payload size of a Publish, which the model accounts and a
	// broker charges against its own limits.
	Size int
	// Max bounds the deliveries a Fetch takes.
	Max int
	// Dt is how far a Wait moves time.
	Dt time.Duration
}

// Limits bound a decoded script, so that a fuzz input cannot ask for a run
// that neither side can finish.
type Limits struct {
	// Keys is the number of routing keys, which is also the number of
	// partitions the oracle gives the model. At least one.
	Keys int
	// Ops bounds the script length.
	Ops int
	// MaxSize bounds a record's payload size.
	MaxSize int
	// MaxWait bounds one Wait.
	MaxWait time.Duration
}

// withDefaults fills in the zero values.
func (l Limits) withDefaults() Limits {
	if l.Keys <= 0 {
		l.Keys = 2
	}
	if l.Ops <= 0 {
		l.Ops = 32
	}
	if l.MaxSize <= 0 {
		l.MaxSize = 64
	}
	if l.MaxWait <= 0 {
		l.MaxWait = time.Second
	}
	return l
}

// Script is a decoded operation script and the limits it was decoded under.
type Script struct {
	Limits Limits
	Ops    []Op
}

// Decode turns fuzz bytes into a script. The same bytes always produce the
// same script, which is what lets one input drive two implementations.
func Decode(data []byte, lim Limits) Script {
	lim = lim.withDefaults()
	r := fuzzutil.NewReader(data)
	s := Script{Limits: lim}
	for !r.Exhausted() && len(s.Ops) < lim.Ops {
		op := Op{Kind: Kind(r.Choice(int(kindCount)))}
		switch op.Kind {
		case Publish:
			op.Key = r.Int(0, lim.Keys-1)
			op.Size = r.Int(0, lim.MaxSize)
		case Fetch:
			op.Max = r.Int(1, 4)
		case Wait:
			op.Dt = r.Duration(lim.MaxWait)
		default:
			// Ack, Nack and Expire carry no argument: they apply to the
			// oldest delivery the run holds, or to every lease.
		}
		s.Ops = append(s.Ops, op)
	}
	return s
}

// Encode writes a script back to the byte form Decode reads, so that a script
// a run found interesting can be committed as a seed.
func (s Script) Encode() []byte {
	lim := s.Limits.withDefaults()
	w := fuzzutil.NewWriter()
	for _, op := range s.Ops {
		w.Choice(int(kindCount), int(op.Kind))
		switch op.Kind {
		case Publish:
			w.Int(0, lim.Keys-1, op.Key)
			w.Int(0, lim.MaxSize, op.Size)
		case Fetch:
			w.Int(1, 4, op.Max)
		case Wait:
			w.Duration(lim.MaxWait, op.Dt)
		default:
			// The argument-free operations, as in Decode.
		}
	}
	return w.Data()
}

// Delivery is one settlement a run observed.
type Delivery struct {
	// Key is the routing key the record was published under.
	Key int
	// Record identifies the record across both implementations: the
	// publication sequence the model assigns and the engine reports, or the
	// identity the harness put in the payload of a broker's record.
	Record uint64
	// Attempt counts deliveries of that record including this one, so an
	// at-least-once broker's redelivery is visible rather than silent.
	Attempt int
}

// Observation is what one side of the differential run produced.
type Observation struct {
	// Settled lists the acknowledged deliveries in the order they were
	// settled.
	Settled []Delivery
	// Published lists the records the script published, per key, in
	// publication order. It is what per-key order is compared against.
	Published map[int][]uint64
	// Nacked counts the deliveries the run returned for redelivery, and
	// Expired the leases it let expire; neither is compared, both are
	// reported, because a broker may redeliver more often than the model.
	Nacked, Expired int
}

// Ordering is the order guarantee the broker under test claims.
type Ordering uint8

// The order guarantees.
const (
	// PerKey means records of one routing key are settled in publication
	// order. It is what the ordered profile claims.
	PerKey Ordering = iota
	// Unordered makes no claim about order, so only the multiset is
	// compared.
	Unordered
)

// Requirements say what the broker under test claims, so that Compare holds
// it to that and not to the model's own determinism.
//
// The zero value is the strictest reading — per-key order, no duplicates,
// nothing missing — because a harness that forgets to fill it in should get
// the strict check and a failure to read, not a comparison that passes
// because it asked for nothing.
type Requirements struct {
	// Ordering is the order guarantee to hold the broker to.
	Ordering Ordering
	// AllowDuplicates permits a record to be settled more than once, which
	// every at-least-once broker may do.
	AllowDuplicates bool
	// AllowMissing permits the broker to have settled fewer records than the
	// oracle: a run that ended with deliveries still in flight. It does not
	// permit a record the oracle never published.
	AllowMissing bool
}

// ErrDifference reports that the two runs disagree; every error Compare
// returns wraps it, so a harness can tell a difference from a harness fault.
var ErrDifference = errors.New("differential: the runs disagree")

// ErrVacuous reports that the comparison examined nothing, which `go test`
// cannot tell from a comparison that found nothing.
//
// Compare returned nil for `Compare(Observation{}, Observation{},
// Requirements{})` and — with AllowMissing, which every unfinished run sets —
// for a broker that settled NOTHING against an oracle that settled records.
// Both are runs in which the relation was never applied, and both used to read
// as a pass (found by the broker harness in conformance/differential).
var ErrVacuous = errors.New("differential: the comparison examined nothing")

// Compare holds a broker's observation to the oracle's.
//
// It compares three things and says which: the multiset of settled records,
// the per-key settlement order, and whether a record was settled that was
// never published. What it deliberately does not compare is *when* a record
// was settled relative to another key's, or how many attempts it took: those
// differ between brokers by design and are reported by the observation rather
// than judged here.
func Compare(oracle, got Observation, req Requirements) error {
	// Vacuity first. Everything below iterates over the two settled sets,
	// so an empty one makes every check trivially true; AllowMissing then
	// removes the one loop that would have noticed. A run the relation was
	// never applied to has to say so rather than report success.
	switch {
	case len(oracle.Settled) == 0:
		return fmt.Errorf("%w: the oracle settled no record, so there was nothing to hold the broker to", ErrVacuous)
	case len(got.Settled) == 0:
		return fmt.Errorf("%w: the broker settled no record of the oracle's %d, so no record was compared",
			ErrVacuous, len(oracle.Settled))
	}
	want := multiset(oracle.Settled)
	have := multiset(got.Settled)
	for _, rec := range sortedKeys(have) {
		n := have[rec]
		w, ok := want[rec]
		switch {
		case !ok:
			return fmt.Errorf("%w: record %d was settled %d time(s) but the oracle never settled it",
				ErrDifference, rec, n)
		case n > w && !req.AllowDuplicates:
			return fmt.Errorf("%w: record %d was settled %d time(s), the oracle once, and the broker claims no duplicates",
				ErrDifference, rec, n)
		}
	}
	if !req.AllowMissing {
		for _, rec := range sortedKeys(want) {
			if have[rec] == 0 {
				return fmt.Errorf("%w: record %d was settled by the oracle and lost by the broker",
					ErrDifference, rec)
			}
		}
	}
	if req.Ordering != PerKey {
		return nil
	}
	for _, key := range sortedIntKeys(oracle.Published) {
		order := oracle.Published[key]
		pos := make(map[uint64]int, len(order))
		for i, rec := range order {
			pos[rec] = i
		}
		last, lastRec := -1, uint64(0)
		for _, d := range got.Settled {
			if d.Key != key || d.Attempt > 1 {
				// A redelivery is settled out of publication order by
				// definition: the record went back to its lane. Per-key
				// order is a claim about first deliveries.
				continue
			}
			i, ok := pos[d.Record]
			if !ok {
				return fmt.Errorf("%w: record %d was settled under key %d, which never published it",
					ErrDifference, d.Record, key)
			}
			if i < last {
				return fmt.Errorf("%w: key %d settled record %d before record %d, published after it",
					ErrDifference, key, d.Record, lastRec)
			}
			last, lastRec = i, d.Record
		}
	}
	return nil
}

func multiset(ds []Delivery) map[uint64]int {
	out := make(map[uint64]int, len(ds))
	for _, d := range ds {
		out[d.Record]++
	}
	return out
}

func sortedKeys(m map[uint64]int) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedIntKeys(m map[int][]uint64) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// Oracle runs a script against the memory reference model.
type Oracle struct {
	lim   Limits
	model *memq.Model
	// held is the deliveries the run holds, oldest first.
	held []held
	obs  Observation
	keys map[uint64]int // record -> the key it was published under
}

// held is one delivery the run has not settled.
type held struct {
	seq     uint64
	origin  uint64
	gen     uint32
	attempt int
	key     int
}

// Config configures the oracle's model. The defaults mirror the profile a
// driver conformance run uses: one subscription, one consumer, a lease
// deadline the script can let expire, and a lane per routing key.
type Config struct {
	// Capacity bounds the records one lane holds. Zero means unbounded,
	// which is what a real broker's queue is for a script this small.
	Capacity int
	// MaxAttempts bounds redeliveries. Zero means unbounded.
	MaxAttempts uint32
	// AckDeadline is the lease duration. Zero selects one second.
	AckDeadline time.Duration
}

// NewOracle returns an oracle for scripts decoded under lim.
func NewOracle(lim Limits, cfg Config) *Oracle {
	lim = lim.withDefaults()
	if cfg.AckDeadline <= 0 {
		cfg.AckDeadline = time.Second
	}
	m := memq.New(memq.Config{
		Partitions:       lim.Keys,
		CapacityMessages: cfg.Capacity,
		MaxAttempts:      cfg.MaxAttempts,
		AckDeadline:      cfg.AckDeadline,
		Ordered:          true,
	})
	o := &Oracle{
		lim:   lim,
		model: m,
		keys:  map[uint64]int{},
		obs:   Observation{Published: map[int][]uint64{}},
	}
	m.Step(memq.Op{Kind: memq.Subscribe, Sub: subName})
	m.Step(memq.Op{Kind: memq.Attach, Sub: subName, Consumer: consumerID})
	return o
}

const (
	subName    = "differential"
	consumerID = 1
)

// Run applies the whole script and returns what the model settled. The
// model's invariants are checked after every operation: a difference the
// oracle itself caused is not a finding about the broker.
func (o *Oracle) Run(s Script) (Observation, error) {
	for i, op := range s.Ops {
		if err := o.step(op); err != nil {
			return o.obs, fmt.Errorf("op %d (%v): %w", i, op.Kind, err)
		}
		if err := o.model.Invariants(); err != nil {
			return o.obs, fmt.Errorf("op %d (%v): the oracle broke its own invariant: %w", i, op.Kind, err)
		}
	}
	return o.obs, nil
}

// Observation returns what the oracle has settled so far.
func (o *Oracle) Observation() Observation { return o.obs }

func (o *Oracle) step(op Op) error {
	switch op.Kind {
	case Publish:
		key := op.Key % o.lim.Keys
		res := o.model.Step(memq.Op{
			Kind: memq.Publish, Partition: key, Size: int64(op.Size),
		})
		if res.Err != nil {
			// A refusal is a legitimate outcome of a bounded queue, and the
			// harness compares what was settled, not what was accepted: a
			// record the oracle never admitted is not in Published, so a
			// broker that settles it is a difference.
			return nil
		}
		if res.Dropped || res.Orphan || res.Deduped {
			return nil
		}
		seq := o.model.Sequence()
		o.keys[seq] = key
		o.obs.Published[key] = append(o.obs.Published[key], seq)
	case Fetch:
		res := o.model.Step(memq.Op{
			Kind: memq.Fetch, Sub: subName, Consumer: consumerID, Max: op.Max,
		})
		if res.Err != nil {
			return nil
		}
		for _, c := range res.Claims {
			o.held = append(o.held, held{
				seq: c.Seq, origin: c.Origin, gen: c.Gen,
				attempt: int(c.Attempts), key: o.keys[c.Origin],
			})
		}
	case Ack:
		h, ok := o.take()
		if !ok {
			return nil
		}
		res := o.model.Step(memq.Op{
			Kind: memq.Ack, Sub: subName, Consumer: consumerID, Seq: h.seq, Gen: h.gen,
		})
		if res.Err != nil {
			return nil
		}
		o.obs.Settled = append(o.obs.Settled, Delivery{
			Key: h.key, Record: h.origin, Attempt: h.attempt,
		})
	case Nack:
		h, ok := o.take()
		if !ok {
			return nil
		}
		if res := o.model.Step(memq.Op{
			Kind: memq.Nack, Sub: subName, Consumer: consumerID, Seq: h.seq, Gen: h.gen,
		}); res.Err == nil {
			o.obs.Nacked++
		}
	case Expire:
		res := o.model.Step(memq.Op{Kind: memq.ExpireLeases})
		o.obs.Expired += res.N
		if res.N > 0 {
			o.held = nil
		}
	case Wait:
		o.model.Step(memq.Op{Kind: memq.Advance, Dt: op.Dt})
	default:
		return fmt.Errorf("undefined operation %d", op.Kind)
	}
	return nil
}

// take removes the oldest delivery the run holds.
func (o *Oracle) take() (held, bool) {
	if len(o.held) == 0 {
		return held{}, false
	}
	h := o.held[0]
	o.held = o.held[1:]
	return h, true
}
