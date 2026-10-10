// # Why this is not an equality check
//
// A network broker is allowed to do things the memory engine does not. It may
// interleave two keys differently, it may redeliver a message whose settlement
// it did not see, and it may answer a publish ambiguously. So a differential
// test that demands the two arms produced the same SEQUENCE has exactly two
// fates: it fails on every run, or somebody weakens it until it passes, and
// after that it is a test that cannot fail. Both outcomes cost more than not
// having the test.
//
// What is compared instead is a RELATION, written down before the first run
// and parameterised by what the subject is allowed to do
// ([Permissions]):
//
//   - every message both arms ACCEPTED is delivered at least once by both;
//   - no message the subject REJECTED is delivered, and no delivery carries an
//     id nobody published;
//   - the multiset of settled ids agrees: an id the oracle acked is acked by
//     the subject too, and an id the oracle dropped is not;
//   - per key, the order in which ids are FIRST delivered is the same on both
//     arms, unless the subject's capability table says it need not be;
//   - a message the script nacked for redelivery comes back, and one it
//     dropped does not.
//
// Redelivery is permitted and counted rather than compared, which is what
// makes the relation stable: a redelivery cannot change a FIRST delivery, so
// the ordering half of the relation is insensitive to exactly the thing
// brokers differ on.
//
// The relation is a pure function of two [Observation]s, so it is tested
// without a broker: [Transform] holds the transformations a broker is allowed
// to make and the defects it is not, and relation_test.go requires the
// relation to accept every one of the first and reject every one of the
// second. An instrument nobody has watched fail is assumed not to work.
package differential

import (
	"fmt"
	"sort"
	"strings"
)

// Outcome is what a publish returned.
type Outcome uint8

// Outcome values.
const (
	// Accepted means Publish returned nil: the message must be delivered.
	Accepted Outcome = iota + 1
	// Rejected means the publish definitely failed: delivery is a defect.
	Rejected
	// Ambiguous means acceptance is unknown ([github.com/iqhive/iqmq.ErrSettlementUnknown]
	// and the ambiguous publish errors): delivery is neither
	// required nor a defect.
	Ambiguous
)

// String implements fmt.Stringer.
func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case Rejected:
		return "rejected"
	case Ambiguous:
		return "ambiguous"
	}
	return "unknown"
}

// Settlement is what the consumer did with one delivery.
type Settlement uint8

// Settlement values.
const (
	// Unsettled is a delivery the run never settled: it was outstanding when
	// the script ended. Both arms may hold different sets of these, so the
	// relation only reports them.
	Unsettled Settlement = iota
	// Acked is Delivery.Ack.
	Acked
	// Nacked is Delivery.Nack asking for redelivery.
	Nacked
	// Dropped is Delivery.Nack(NackDrop) or Reject: the message is
	// discarded and must not come back.
	Dropped
)

// String implements fmt.Stringer.
func (s Settlement) String() string {
	switch s {
	case Unsettled:
		return "unsettled"
	case Acked:
		return "acked"
	case Nacked:
		return "nacked"
	case Dropped:
		return "dropped"
	}
	return "invalid"
}

// Pub is one publish the script performed, in script order.
//
// ID is the script's own serial number for the message and is carried in the
// body, so that a delivery can be matched to its publication without trusting
// any broker-assigned identifier: a broker that loses headers, rewrites ids or
// reorders would otherwise be compared against its own bookkeeping.
type Pub struct {
	// Key is the ordering key, "" for none.
	Key string
	// ID is the script's serial number for the message.
	ID string
	// Outcome is what Publish returned.
	Outcome Outcome
}

// Del is one delivery the consumer observed, in observation order.
type Del struct {
	// Key is the key the delivery carried, as the arm reported it. The
	// ordering half of the relation groups by the key the message was
	// PUBLISHED under instead; see [Observation.keyMismatches].
	Key string
	// ID is the script serial the body carried.
	ID string
	// Settlement is what the run did with it.
	Settlement Settlement
	// Redelivered is the arm's own redelivery flag, recorded for the report
	// and never compared: drivers disagree about whether the first delivery
	// after a reconnect is "redelivered".
	Redelivered bool
}

// Observation is one arm's whole record of one run of one script.
type Observation struct {
	// Arm names the side, for failure messages ("memory", "jetstream").
	Arm string
	// Pubs is every publish, in script order.
	Pubs []Pub
	// Dels is every delivery, in the order the consumer saw it.
	Dels []Del
}

// Clone returns a deep copy, which is what a [Transform] mutates.
func (o Observation) Clone() Observation {
	out := Observation{Arm: o.Arm}
	if o.Pubs != nil {
		out.Pubs = append([]Pub(nil), o.Pubs...)
	}
	if o.Dels != nil {
		out.Dels = append([]Del(nil), o.Dels...)
	}
	return out
}

// outcome is the recorded outcome of id, or 0 when this arm never published
// it. A repeated publish of one id keeps the strongest outcome, which is what
// a retry that eventually succeeded means.
func (o Observation) outcome(id string) Outcome {
	best := Outcome(0)
	for i := range o.Pubs {
		if o.Pubs[i].ID != id {
			continue
		}
		if rank(o.Pubs[i].Outcome) > rank(best) {
			best = o.Pubs[i].Outcome
		}
	}
	return best
}

func rank(o Outcome) int {
	switch o {
	case Accepted:
		return 3
	case Ambiguous:
		return 2
	case Rejected:
		return 1
	}
	return 0
}

// publishedKey is the key id was published with on this arm.
func (o Observation) publishedKey(id string) (string, bool) {
	for i := range o.Pubs {
		if o.Pubs[i].ID == id {
			return o.Pubs[i].Key, true
		}
	}
	return "", false
}

// ids is every published id, in script order and without repetition.
func (o Observation) ids() []string {
	seen := make(map[string]bool, len(o.Pubs))
	out := make([]string, 0, len(o.Pubs))
	for i := range o.Pubs {
		if !seen[o.Pubs[i].ID] {
			seen[o.Pubs[i].ID] = true
			out = append(out, o.Pubs[i].ID)
		}
	}
	return out
}

// deliveries is how many times id was delivered.
func (o Observation) deliveries(id string) int {
	n := 0
	for i := range o.Dels {
		if o.Dels[i].ID == id {
			n++
		}
	}
	return n
}

// settlements is how many deliveries of id ended in each settlement.
func (o Observation) settlements(id string) map[Settlement]int {
	out := map[Settlement]int{}
	for i := range o.Dels {
		if o.Dels[i].ID == id {
			out[o.Dels[i].Settlement]++
		}
	}
	return out
}

// firstOrder is the order in which ids were FIRST delivered, restricted to
// keep, and grouped by the key the delivery carried.
//
// It is the ordering half of the relation, and the reason it reads first
// deliveries rather than the whole sequence is in the package comment: a
// redelivery cannot move a first delivery, so this is insensitive to exactly
// the behaviour brokers are allowed to differ on.
func (o Observation) firstOrder(keep map[string]bool, keyOf func(id string) string) map[string][]string {
	out := map[string][]string{}
	seen := map[string]bool{}
	for i := range o.Dels {
		d := o.Dels[i]
		if seen[d.ID] || (keep != nil && !keep[d.ID]) {
			continue
		}
		seen[d.ID] = true
		out[keyOf(d.ID)] = append(out[keyOf(d.ID)], d.ID)
	}
	return out
}

// keyMismatches is how many deliveries carried a key other than the one this
// arm published the message under.
//
// It is reported and never compared. Grouping the ordering check by the
// DELIVERED key would make an ordering comparison impossible against the four
// drivers whose transport has no key at all: every delivery would land in the
// "" group on one arm and in three groups on the other, and the relation would
// fail for a reason that is not about order. Key fidelity is the conformance
// kit's "key round trip" scenario, and this counter is here so that a
// differential run still says when the two disagree.
func (o Observation) keyMismatches() int {
	n := 0
	for i := range o.Dels {
		if k, ok := o.publishedKey(o.Dels[i].ID); ok && k != o.Dels[i].Key {
			n++
		}
	}
	return n
}

// globalOrder is firstOrder with every key merged, for a subject that claims
// order across the whole topic.
func (o Observation) globalOrder(keep map[string]bool) []string {
	var out []string
	seen := map[string]bool{}
	for i := range o.Dels {
		d := o.Dels[i]
		if seen[d.ID] || (keep != nil && !keep[d.ID]) {
			continue
		}
		seen[d.ID] = true
		out = append(out, d.ID)
	}
	return out
}

// keys is every key either arm delivered on, sorted.
func keysOf(a, b map[string][]string) []string {
	set := map[string]bool{}
	for k := range a {
		set[k] = true
	}
	for k := range b {
		set[k] = true
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// String is a one-line summary, for a failure message.
func (o Observation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d publish(es), %d deliver(ies)", o.Arm, len(o.Pubs), len(o.Dels))
	return b.String()
}
