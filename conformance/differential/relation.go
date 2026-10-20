package differential

import (
	"fmt"
	"sort"
	"strings"
)

// Kind names one way the two arms can diverge. Every kind is a defect in the
// subject unless the [Permissions] say otherwise, and the two that are not —
// [KindScript] and [KindOracle] — are defects in the HARNESS, which is why
// they are separate kinds rather than folded into the others: an instrument is
// part of the code under test, and a differential run whose
// two arms did not execute the same script has measured nothing.
type Kind uint8

// Kind values.
const (
	// KindScript means the two arms do not agree about what was published:
	// the script did not run the same way on both sides. A harness defect.
	KindScript Kind = iota + 1
	// KindOracle means the oracle arm did something the memory engine is
	// not allowed to do — delivered an accepted message twice, delivered
	// something nobody published, lost one. A harness or engine defect.
	KindOracle
	// KindLost means an id both arms accepted was never delivered by the
	// subject. At-least-once was breached.
	KindLost
	// KindUnknown means the subject delivered an id nobody published.
	KindUnknown
	// KindRejectedDelivered means the subject delivered a publish it had
	// definitely refused.
	KindRejectedDelivered
	// KindSettlement means the settled multisets disagree: an id the oracle
	// acked the subject never acked, or one the oracle dropped the subject
	// acked.
	KindSettlement
	// KindDuplicate means the subject delivered an id more than once and its
	// capability table does not allow duplicates.
	KindDuplicate
	// KindOrderWithinKey means the first-delivery order within one key
	// differs and the subject claims order within a key.
	KindOrderWithinKey
	// KindOrderAcrossKeys means the global first-delivery order differs and
	// the subject claims order across the whole topic.
	KindOrderAcrossKeys
	// KindRedelivery means a nacked message did not come back, or a dropped
	// one did.
	KindRedelivery
	// KindVacuous means the run compared nothing: no id was accepted by both
	// arms, or nothing was delivered at all. A run that proves nothing is
	// reported as a divergence rather than as a pass, because `go test`
	// cannot tell the two apart and neither can a reader.
	KindVacuous
)

// String implements fmt.Stringer.
func (k Kind) String() string {
	switch k {
	case KindScript:
		return "script-disagreement"
	case KindOracle:
		return "oracle-defect"
	case KindLost:
		return "lost"
	case KindUnknown:
		return "unknown-id"
	case KindRejectedDelivered:
		return "rejected-delivered"
	case KindSettlement:
		return "settlement-differs"
	case KindDuplicate:
		return "unexpected-duplicate"
	case KindOrderWithinKey:
		return "order-within-key"
	case KindOrderAcrossKeys:
		return "order-across-keys"
	case KindRedelivery:
		return "redelivery"
	case KindVacuous:
		return "vacuous"
	}
	return "invalid"
}

// Harness reports whether this kind blames the measuring apparatus rather than
// the subject.
func (k Kind) Harness() bool { return k == KindScript || k == KindOracle }

// Divergence is one way the subject departed from the relation.
type Divergence struct {
	// Kind is what went wrong.
	Kind Kind
	// Key is the ordering key involved, "" when the kind is not per key.
	Key string
	// ID is the message involved, "" when the kind is not per message.
	ID string
	// Detail says what was expected and what was seen.
	Detail string
}

// Error implements error so that one divergence can be returned on its own.
func (d Divergence) Error() string {
	var b strings.Builder
	b.WriteString(d.Kind.String())
	if d.ID != "" {
		fmt.Fprintf(&b, " id=%s", d.ID)
	}
	if d.Key != "" {
		fmt.Fprintf(&b, " key=%q", d.Key)
	}
	if d.Detail != "" {
		fmt.Fprintf(&b, ": %s", d.Detail)
	}
	return b.String()
}

// Permissions says what the subject arm is allowed to do that the oracle did
// not. Every field that is FALSE makes the relation stricter, so the zero
// value is the strictest reading — an exact match of settled multiset and of
// global first-delivery order — and [PermissionsFor] is what loosens it from a
// driver's own capability table.
//
// The direction matters. A permission is granted because a driver ADVERTISES
// the weaker guarantee, so a driver that claims order within a key and then
// reorders fails; the way to make this test pass is to change the claim, which
// the conformance suite then checks in the other direction.
type Permissions struct {
	// Duplicates allows a message to be delivered more than once.
	Duplicates bool
	// ReorderWithinKey allows the first-delivery order within one key to
	// differ from the oracle's.
	ReorderWithinKey bool
	// ReorderAcrossKeys allows the global first-delivery order to differ.
	ReorderAcrossKeys bool
	// AmbiguousMayVanish allows a publish that returned an ambiguous error
	// never to arrive.
	AmbiguousMayVanish bool
	// AmbiguousMayArrive allows one to arrive after all.
	AmbiguousMayArrive bool
	// LateRedelivery allows a nacked message not to come back within the
	// run's drain window — a broker whose redelivery delay is longer than
	// the window, not one that lost it.
	LateRedelivery bool
}

// Summary is what a comparison saw, whether or not it diverged. It exists so
// that a green run says what it actually proved: "no divergence" over zero
// compared ids is not a result.
type Summary struct {
	// Compared is how many ids both arms accepted and the relation
	// therefore held the subject to.
	Compared int
	// SubjectDeliveries is every delivery the subject observed.
	SubjectDeliveries int
	// Duplicates is how many deliveries the subject made that the oracle
	// did not — the broker's own redeliveries, not the ones the script
	// asked both arms for.
	Duplicates int
	// AmbiguousArrived and AmbiguousVanished account for the publishes whose
	// outcome the subject could not determine.
	AmbiguousArrived  int
	AmbiguousVanished int
	// Keys is how many distinct keys the subject delivered on.
	Keys int
	// OrderedKeys is how many of those the relation checked the order of.
	OrderedKeys int
	// KeyMismatches is how many subject deliveries carried a key other
	// than the published one. Reported, never compared: see
	// [Observation.keyMismatches].
	KeyMismatches int
}

// String implements fmt.Stringer.
func (s Summary) String() string {
	return fmt.Sprintf("compared=%d deliveries=%d duplicates=%d keys=%d ordered-keys=%d key-mismatches=%d ambiguous(arrived=%d vanished=%d)",
		s.Compared, s.SubjectDeliveries, s.Duplicates, s.Keys, s.OrderedKeys, s.KeyMismatches,
		s.AmbiguousArrived, s.AmbiguousVanished)
}

// Result is the whole verdict.
type Result struct {
	// Summary is what the comparison SAW, filled in whether or not anything
	// diverged. It is what gives a green run a SIZE -- how many ids both
	// arms accepted, how many deliveries the subject made -- so that a
	// caller can report what the run proved and not merely that nothing
	// broke. [KindVacuous] catches the extreme of a run that compared
	// nothing at all; Summary is what shows a run that compared four.
	Summary Summary
	// Divergences is every way the subject's observation was NOT a legal
	// reading of the oracle's, one entry per violation rather than one per
	// run: a subject that loses ten ids reports ten [KindLost] entries.
	// The slice is in [Compare]'s own check order, so a disagreement about
	// the script or a misbehaving oracle comes before anything blamed on
	// the subject.
	//
	// An empty slice is the pass, which is all [Result.OK] tests -- and it
	// is why a run that compared nothing appends [KindVacuous] rather than
	// returning empty: an empty Divergences has to mean the relation held
	// and not that it never ran.
	Divergences []Divergence
}

// OK reports whether the subject is a legal reading of the oracle.
func (r Result) OK() bool { return len(r.Divergences) == 0 }

// Err returns every divergence as one error, or nil.
func (r Result) Err() error {
	if r.OK() {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d divergence(s) [%s]", len(r.Divergences), r.Summary)
	for _, d := range r.Divergences {
		fmt.Fprintf(&b, "\n  - %s", d.Error())
	}
	return fmt.Errorf("%s", b.String())
}

// Has reports whether any divergence is of that kind.
func (r Result) Has(k Kind) bool {
	for _, d := range r.Divergences {
		if d.Kind == k {
			return true
		}
	}
	return false
}

// Kinds is every kind present, sorted, for a failure message.
func (r Result) Kinds() []string {
	set := map[string]bool{}
	for _, d := range r.Divergences {
		set[d.Kind.String()] = true
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Compare decides whether subject is a legal reading of oracle under perm.
//
// The order of the checks is deliberate: the two harness kinds come first, so
// that a run whose arms disagree about the script, or whose oracle misbehaved,
// says so instead of blaming the broker for it.
func Compare(oracle, subject Observation, perm Permissions) Result {
	var res Result
	add := func(k Kind, key, id, format string, args ...any) {
		res.Divergences = append(res.Divergences, Divergence{
			Kind: k, Key: key, ID: id, Detail: fmt.Sprintf(format, args...),
		})
	}

	// 1. Both arms must have run the same script.
	oIDs, sIDs := oracle.ids(), subject.ids()
	if len(oIDs) != len(sIDs) {
		add(KindScript, "", "", "%s published %d distinct id(s) and %s published %d",
			oracle.Arm, len(oIDs), subject.Arm, len(sIDs))
	}
	oSet := map[string]bool{}
	for _, id := range oIDs {
		oSet[id] = true
	}
	for _, id := range sIDs {
		if !oSet[id] {
			add(KindScript, "", id, "%s published it and %s did not", subject.Arm, oracle.Arm)
		}
	}
	sSet := map[string]bool{}
	for _, id := range sIDs {
		sSet[id] = true
	}
	for _, id := range oIDs {
		if !sSet[id] {
			add(KindScript, "", id, "%s published it and %s did not", oracle.Arm, subject.Arm)
		}
		if ok, os, ss := sameKey(oracle, subject, id); !ok {
			add(KindScript, "", id, "published under key %q on %s and %q on %s", os, oracle.Arm, ss, subject.Arm)
		}
	}

	// 2. The oracle is the memory engine with no faults: it delivers every
	// accepted message exactly once and nothing else. A run whose oracle
	// broke that has measured nothing, and saying so here is what stops the
	// subject being blamed for it.
	for _, id := range oIDs {
		n := oracle.deliveries(id)
		// The oracle redelivers exactly what the script nacked, so the
		// ceiling is one delivery plus one per nack and NOT one delivery
		// flat. Written flat, this check called the fixture's own
		// nacked-then-acked message an oracle defect, which failed every
		// legal row of the transform table and left the negative control
		// asserting nothing: "the table proved nothing: 0 legal row(s), 11
		// defect row(s)".
		allowed := 1 + oracle.settlements(id)[Nacked]
		switch out := oracle.outcome(id); {
		case out == Accepted && n == 0:
			add(KindOracle, "", id, "accepted by the oracle and never delivered by it")
		case out == Accepted && n > allowed:
			add(KindOracle, "", id, "delivered %d times by the oracle, which redelivers only what the script nacked (%d nack(s) here)", n, allowed-1)
		case out == Rejected && n > 0:
			add(KindOracle, "", id, "rejected by the oracle and delivered by it anyway")
		}
	}
	for i := range oracle.Dels {
		if !oSet[oracle.Dels[i].ID] {
			add(KindOracle, oracle.Dels[i].Key, oracle.Dels[i].ID, "delivered by the oracle and never published to it")
		}
	}

	// 3. Loss, unknown ids and rejected-but-delivered, on the subject.
	both := map[string]bool{}
	for _, id := range oIDs {
		if oracle.outcome(id) == Accepted && subject.outcome(id) == Accepted {
			both[id] = true
		}
	}
	for _, id := range sIDs {
		n := subject.deliveries(id)
		switch subject.outcome(id) {
		case Accepted:
			if n == 0 {
				add(KindLost, keyOrEmpty(subject, id), id, "accepted by %s and never delivered", subject.Arm)
			}
		case Rejected:
			if n > 0 {
				add(KindRejectedDelivered, keyOrEmpty(subject, id), id, "refused by %s and delivered %d time(s)", subject.Arm, n)
			}
		case Ambiguous:
			switch {
			case n > 0:
				res.Summary.AmbiguousArrived++
				if !perm.AmbiguousMayArrive {
					add(KindLost, keyOrEmpty(subject, id), id,
						"ambiguous publish arrived and %s does not permit that", subject.Arm)
				}
			default:
				res.Summary.AmbiguousVanished++
				if !perm.AmbiguousMayVanish {
					add(KindLost, keyOrEmpty(subject, id), id,
						"ambiguous publish vanished and %s does not permit that", subject.Arm)
				}
			}
		}
	}
	for i := range subject.Dels {
		if !sSet[subject.Dels[i].ID] {
			add(KindUnknown, subject.Dels[i].Key, subject.Dels[i].ID, "delivered by %s and never published to it", subject.Arm)
		}
	}

	// 4. Settled multisets, duplicates and the redelivery contract.
	for id := range both {
		os, ss := oracle.settlements(id), subject.settlements(id)
		// A duplicate is a delivery the SUBJECT made and the oracle did
		// not, rather than any delivery after the first: the script's own
		// nack asks both arms to redeliver, and counting that as a
		// duplicate made the count report two where one message had been
		// duplicated and reported a KindDuplicate against a driver that
		// forbids duplicates and had made none.
		if extra := subject.deliveries(id) - oracle.deliveries(id); extra > 0 {
			res.Summary.Duplicates += extra
			if !perm.Duplicates {
				add(KindDuplicate, keyOrEmpty(subject, id), id,
					"delivered %d time(s) against the oracle's %d and %s does not permit duplicates",
					subject.deliveries(id), oracle.deliveries(id), subject.Arm)
			}
		}
		if os[Acked] > 0 && ss[Acked] == 0 {
			add(KindSettlement, keyOrEmpty(subject, id), id,
				"acked on %s and settled %s on %s", oracle.Arm, describe(ss), subject.Arm)
		}
		if os[Dropped] > 0 && os[Acked] == 0 && ss[Acked] > 0 {
			add(KindSettlement, keyOrEmpty(subject, id), id,
				"dropped on %s and acked on %s", oracle.Arm, subject.Arm)
		}
		// The redelivery contract, read from the ORACLE's script actions:
		// a message the script nacked must come back, and one it dropped
		// must not. Both are observable on a real broker and neither
		// depends on timing beyond the drain window.
		if os[Nacked] > 0 && os[Acked] > 0 && ss[Nacked] > 0 && ss[Acked] == 0 && !perm.LateRedelivery {
			add(KindRedelivery, keyOrEmpty(subject, id), id,
				"nacked for redelivery on %s and never came back", subject.Arm)
		}
		if os[Dropped] > 0 && os[Acked] == 0 && subject.deliveries(id) > oracle.deliveries(id) {
			add(KindRedelivery, keyOrEmpty(subject, id), id,
				"dropped and redelivered %d time(s) on %s against %d on %s",
				subject.deliveries(id), subject.Arm, oracle.deliveries(id), oracle.Arm)
		}
	}

	// 5. Per-key first-delivery order.
	// Both arms group by the key the SCRIPT published under, read from the
	// oracle, so that the two sides are grouped identically whatever the
	// subject's transport did with the key.
	keyOf := func(id string) string { k, _ := oracle.publishedKey(id); return k }
	oOrder, sOrder := oracle.firstOrder(both, keyOf), subject.firstOrder(both, keyOf)
	res.Summary.Keys = len(sOrder)
	res.Summary.KeyMismatches = subject.keyMismatches()
	if !perm.ReorderWithinKey {
		for _, k := range keysOf(oOrder, sOrder) {
			res.Summary.OrderedKeys++
			if eq(oOrder[k], sOrder[k]) {
				continue
			}
			add(KindOrderWithinKey, k, "", "%s delivered %v first and %s delivered %v",
				oracle.Arm, oOrder[k], subject.Arm, sOrder[k])
		}
	}
	if !perm.ReorderAcrossKeys {
		og, sg := oracle.globalOrder(both), subject.globalOrder(both)
		if !eq(og, sg) {
			add(KindOrderAcrossKeys, "", "", "%s delivered %v first and %s delivered %v",
				oracle.Arm, og, subject.Arm, sg)
		}
	}

	// 6. Vacuity. Last, because a run that also diverged is not vacuous —
	// it found something — and reporting both would be noise.
	res.Summary.Compared = len(both)
	res.Summary.SubjectDeliveries = len(subject.Dels)
	if len(res.Divergences) == 0 && (len(both) == 0 || len(subject.Dels) == 0) {
		add(KindVacuous, "", "", "the relation held %s to %d id(s) over %d deliver(ies): nothing was compared",
			subject.Arm, len(both), len(subject.Dels))
	}
	return res
}

// sameKey reports whether both arms published id under the same key.
func sameKey(a, b Observation, id string) (bool, string, string) {
	ak, aok := a.publishedKey(id)
	bk, bok := b.publishedKey(id)
	if !aok || !bok {
		return true, ak, bk
	}
	return ak == bk, ak, bk
}

// keyOrEmpty is the key the subject published id under, for a message.
func keyOrEmpty(o Observation, id string) string {
	k, _ := o.publishedKey(id)
	return k
}

// describe renders a settlement tally.
func describe(m map[Settlement]int) string {
	if len(m) == 0 {
		return "not at all (never delivered)"
	}
	parts := make([]string, 0, len(m))
	for s := Unsettled; s <= Dropped; s++ {
		if n := m[s]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s×%d", s, n))
		}
	}
	return strings.Join(parts, ",")
}

// eq compares two id sequences.
func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
