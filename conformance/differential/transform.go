package differential

import "sort"

// Transform is one edit to a subject observation, together with whether
// [Compare] must accept the result.
//
// The table these come from is the negative control for the relation. A
// differential test is only worth running if its comparison can fail, and the
// way to know that is to hand it observations that a broker is allowed to
// produce (Legal) and observations that encode a real defect (not Legal) and
// require the verdict each way. Six of the defects here were the ones a
// "compare the two sequences for equality" relation would have had to be
// weakened past, one at a time, until it accepted everything.
//
// Apply takes BOTH observations and finds its own target in them rather than
// naming an id from the fixture. That is not tidiness: the first version named
// the fixture's ids, so applied to an observation a real run produced,
// "redeliver-a-dropped-message" appended a second delivery of a message that
// run had ACKED — a legal duplicate, which the relation accepted, and the row
// reported the relation as broken. A transform that cannot
// find a target returns false and is skipped, which is also how the table
// avoids asserting on a no-op.
type Transform struct {
	// Name identifies the transform in a subtest name.
	Name string
	// Legal is whether a conforming broker may do this.
	Legal bool
	// Kind is the divergence [Compare] must report for an illegal
	// transform. Ignored when Legal.
	Kind Kind
	// Perm is the permission set to compare under. A legal transform is
	// legal only under the permissions that allow it, and that pairing is
	// the point: the same edit is a defect under stricter permissions.
	Perm Permissions
	// Apply edits a copy of the subject, given the oracle to find its
	// target in. The bool is whether it found one and changed anything.
	Apply func(oracle, subject Observation) (Observation, bool)
}

// Permissive is the loosest honest permission set: an at-least-once broker
// that orders within a key, reorders across keys and may answer a publish
// ambiguously. It is what most drivers in this repository advertise.
func Permissive() Permissions {
	return Permissions{
		Duplicates:         true,
		ReorderAcrossKeys:  true,
		AmbiguousMayVanish: true,
		AmbiguousMayArrive: true,
	}
}

// strictDuplicates is Permissive minus the duplicate allowance: what a driver
// claiming exactly-once delivery advertises.
func strictDuplicates() Permissions {
	p := Permissive()
	p.Duplicates = false
	return p
}

// strictGlobalOrder is Permissive minus the cross-key allowance.
func strictGlobalOrder() Permissions {
	p := Permissive()
	p.ReorderAcrossKeys = false
	return p
}

// strictAmbiguousVanish is Permissive minus the permission for an ambiguous
// publish to disappear: what a driver with broker-side producer
// de-duplication and a retrying publisher could claim.
func strictAmbiguousVanish() Permissions {
	p := Permissive()
	p.AmbiguousMayVanish = false
	return p
}

// strictAmbiguousArrive is Permissive minus the permission for one to arrive
// after all.
func strictAmbiguousArrive() Permissions {
	p := Permissive()
	p.AmbiguousMayArrive = false
	return p
}

// Fixture is the oracle observation the table's own test edits: three keys,
// nine messages, one nacked-then-acked, one dropped, one ambiguous and one
// rejected, so that every branch of [Compare] has something to bite on.
//
// The ids are deliberately not in key order — a fixture whose keys and ids
// rise together cannot tell a per-key comparison from a global one.
func Fixture() Observation {
	o := Observation{Arm: "oracle"}
	pub := func(key, id string, out Outcome) {
		o.Pubs = append(o.Pubs, Pub{Key: key, ID: id, Outcome: out})
	}
	del := func(key, id string, s Settlement) {
		o.Dels = append(o.Dels, Del{Key: key, ID: id, Settlement: s})
	}
	pub("a", "m1", Accepted)
	pub("b", "m2", Accepted)
	pub("a", "m3", Accepted)
	pub("c", "m4", Accepted)
	pub("b", "m5", Accepted)
	pub("a", "m6", Accepted) // nacked then acked
	pub("c", "m7", Accepted) // dropped
	pub("b", "m8", Ambiguous)
	pub("c", "m9", Rejected)

	del("a", "m1", Acked)
	del("b", "m2", Acked)
	del("a", "m3", Acked)
	del("c", "m4", Acked)
	del("a", "m6", Nacked)
	del("b", "m5", Acked)
	del("c", "m7", Dropped)
	del("a", "m6", Acked) // the redelivery the nack asked for
	return o
}

// Subject is Fixture renamed: the identity subject, which the relation must
// accept under any permissions. The table's own test starts from it.
func Subject() Observation {
	s := Fixture().Clone()
	s.Arm = "subject"
	return s
}

// Transforms is the negative-control table.
func Transforms() []Transform {
	return []Transform{
		// ---- legal: a conforming broker may do each of these ----
		{
			Name: "identity", Legal: true, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) { return s, true },
		},
		{
			Name: "redeliver-an-acked-message", Legal: true, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := anyAcked(s)
				if !ok {
					return s, false
				}
				return insertAfterFirst(s, id, Del{Key: keyOfDel(s, id), ID: id, Settlement: Acked, Redelivered: true})
			},
		},
		{
			Name: "interleave-two-keys-differently", Legal: true, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) { return moveKeyLast(s, someKey(s)) },
		},
		{
			Name: "an-ambiguous-publish-arrives", Legal: true, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := anyOutcome(s, Ambiguous)
				if !ok {
					return s, false
				}
				return append1(s, Del{Key: keyOfPub(s, id), ID: id, Settlement: Acked})
			},
		},
		{
			Name: "an-extra-nack-before-the-ack", Legal: true, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := anyAcked(s)
				if !ok {
					return s, false
				}
				return insertAfterFirst(s, id, Del{Key: keyOfDel(s, id), ID: id, Settlement: Nacked})
			},
		},
		{
			Name: "a-redelivery-left-unsettled", Legal: true, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := anyAcked(s)
				if !ok {
					return s, false
				}
				return insertAfterFirst(s, id, Del{Key: keyOfDel(s, id), ID: id, Settlement: Unsettled})
			},
		},

		// ---- illegal: each encodes a real defect ----
		{
			Name: "drop-a-settlement", Legal: false, Kind: KindSettlement, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := onlySettled(s, Acked)
				if !ok {
					return s, false
				}
				return resettle(s, id, Acked, Unsettled)
			},
		},
		{
			Name: "lose-a-message", Legal: false, Kind: KindLost, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := anyAcked(s)
				if !ok {
					return s, false
				}
				return removeDeliveries(s, id)
			},
		},
		{
			Name: "reorder-within-a-key", Legal: false, Kind: KindOrderWithinKey, Perm: Permissive(),
			Apply: func(o, s Observation) (Observation, bool) {
				a, b, ok := twoFirstsOfOneKey(o, s)
				if !ok {
					return s, false
				}
				return swapFirst(s, a, b)
			},
		},
		{
			Name: "deliver-an-id-nobody-published", Legal: false, Kind: KindUnknown, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) {
				return append1(s, Del{Key: someKey(s), ID: "forged", Settlement: Acked})
			},
		},
		{
			Name: "deliver-a-refused-publish", Legal: false, Kind: KindRejectedDelivered, Perm: Permissive(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := anyOutcome(s, Rejected)
				if !ok {
					return s, false
				}
				return append1(s, Del{Key: keyOfPub(s, id), ID: id, Settlement: Acked})
			},
		},
		{
			Name: "ack-a-message-the-oracle-dropped", Legal: false, Kind: KindSettlement, Perm: Permissive(),
			Apply: func(o, s Observation) (Observation, bool) {
				id, ok := onlySettled(o, Dropped)
				if !ok {
					return s, false
				}
				return resettle(s, id, Dropped, Acked)
			},
		},
		{
			Name: "redeliver-a-dropped-message", Legal: false, Kind: KindRedelivery, Perm: Permissive(),
			Apply: func(o, s Observation) (Observation, bool) {
				id, ok := onlySettled(o, Dropped)
				if !ok {
					return s, false
				}
				return append1(s, Del{Key: keyOfPub(s, id), ID: id, Settlement: Acked})
			},
		},
		{
			Name: "swallow-a-nack", Legal: false, Kind: KindRedelivery, Perm: Permissive(),
			Apply: func(o, s Observation) (Observation, bool) {
				id, ok := nackedThenAcked(o)
				if !ok {
					return s, false
				}
				return removeSettled(s, id, Acked)
			},
		},

		// ---- the same edits, illegal because the permissions are stricter ----
		{
			Name: "duplicate-when-duplicates-are-forbidden", Legal: false, Kind: KindDuplicate,
			Perm: strictDuplicates(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := anyAcked(s)
				if !ok {
					return s, false
				}
				return insertAfterFirst(s, id, Del{Key: keyOfDel(s, id), ID: id, Settlement: Acked, Redelivered: true})
			},
		},
		{
			Name: "interleave-when-global-order-is-claimed", Legal: false, Kind: KindOrderAcrossKeys,
			Perm:  strictGlobalOrder(),
			Apply: func(_, s Observation) (Observation, bool) { return moveKeyLast(s, someKey(s)) },
		},
		{
			// The vanish direction is not a transform: the fixture's
			// ambiguous message has already vanished, so an Apply that
			// removes its deliveries changes nothing and the row
			// asserts nothing — "the transform changed nothing, so the
			// assertion below could not fail" is what the table's own
			// test said about it. It is covered by
			// TestRelationHoldsAmbiguousPublishesBothWays instead.
			Name: "an-ambiguous-publish-arrives-when-that-is-not-permitted", Legal: false, Kind: KindLost,
			Perm: strictAmbiguousArrive(),
			Apply: func(_, s Observation) (Observation, bool) {
				id, ok := anyOutcome(s, Ambiguous)
				if !ok {
					return s, false
				}
				return append1(s, Del{Key: keyOfPub(s, id), ID: id, Settlement: Acked})
			},
		},
	}
}

// ---- finding a target in an observation ----

// anyAcked is some id with an acked delivery, the first in delivery order so
// that the choice is deterministic.
//
// It is specialised to Acked rather than taking a Settlement: every caller
// wants an acked delivery, and a parameter only one value is ever passed
// reads as a generality nothing uses.
func anyAcked(o Observation) (string, bool) {
	for i := range o.Dels {
		if o.Dels[i].Settlement == Acked {
			return o.Dels[i].ID, true
		}
	}
	return "", false
}

// onlySettled is some id EVERY delivery of which is settled want. It is what a
// transform needs when the edit must be unambiguous: resettling an id that was
// also acked elsewhere would not change the settled multiset.
func onlySettled(o Observation, want Settlement) (string, bool) {
	for _, id := range o.ids() {
		tally := o.settlements(id)
		if tally[want] > 0 && len(tally) == 1 {
			return id, true
		}
	}
	return "", false
}

// nackedThenAcked is some id the oracle nacked and later acked, which is what
// the redelivery contract is read from.
func nackedThenAcked(o Observation) (string, bool) {
	for _, id := range o.ids() {
		t := o.settlements(id)
		if t[Nacked] > 0 && t[Acked] > 0 {
			return id, true
		}
	}
	return "", false
}

// anyOutcome is some id published with that outcome.
func anyOutcome(o Observation, want Outcome) (string, bool) {
	for _, id := range o.ids() {
		if o.outcome(id) == want {
			return id, true
		}
	}
	return "", false
}

// twoFirstsOfOneKey is two ids first-delivered under one key, in that order,
// so that swapping them is a reorder within a key and nothing else.
func twoFirstsOfOneKey(oracle, subject Observation) (string, string, bool) {
	keyOf := func(id string) string { k, _ := oracle.publishedKey(id); return k }
	order := subject.firstOrder(nil, keyOf)
	keys := make([]string, 0, len(order))
	for k := range order {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if len(order[k]) >= 2 {
			return order[k][0], order[k][1], true
		}
	}
	return "", "", false
}

// someKey is a key the subject delivered on, chosen deterministically.
func someKey(o Observation) string {
	keys := map[string]bool{}
	for i := range o.Dels {
		keys[o.Dels[i].Key] = true
	}
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return ""
	}
	return out[0]
}

// keyOfDel is the key the first delivery of id carried.
func keyOfDel(o Observation, id string) string {
	for i := range o.Dels {
		if o.Dels[i].ID == id {
			return o.Dels[i].Key
		}
	}
	return ""
}

// keyOfPub is the key id was published under.
func keyOfPub(o Observation, id string) string {
	k, _ := o.publishedKey(id)
	return k
}

// ---- the edit primitives, each returning whether it changed anything ----

// append1 appends one delivery.
func append1(o Observation, d Del) (Observation, bool) {
	o.Dels = append(o.Dels, d)
	return o, true
}

// insertAfterFirst inserts d immediately after the first delivery of id, which
// is what keeps a redelivery from moving a FIRST delivery and so from being an
// ordering change as well as a duplicate.
func insertAfterFirst(o Observation, id string, d Del) (Observation, bool) {
	for i := range o.Dels {
		if o.Dels[i].ID != id {
			continue
		}
		out := make([]Del, 0, len(o.Dels)+1)
		out = append(out, o.Dels[:i+1]...)
		out = append(out, d)
		out = append(out, o.Dels[i+1:]...)
		o.Dels = out
		return o, true
	}
	return o, false
}

// removeDeliveries deletes every delivery of id.
func removeDeliveries(o Observation, id string) (Observation, bool) {
	out := make([]Del, 0, len(o.Dels))
	for i := range o.Dels {
		if o.Dels[i].ID != id {
			out = append(out, o.Dels[i])
		}
	}
	changed := len(out) != len(o.Dels)
	o.Dels = out
	return o, changed
}

// removeSettled deletes the deliveries of id whose settlement is s.
func removeSettled(o Observation, id string, s Settlement) (Observation, bool) {
	out := make([]Del, 0, len(o.Dels))
	for i := range o.Dels {
		if o.Dels[i].ID == id && o.Dels[i].Settlement == s {
			continue
		}
		out = append(out, o.Dels[i])
	}
	changed := len(out) != len(o.Dels)
	o.Dels = out
	return o, changed
}

// resettle changes the settlement of every delivery of id that is from to to.
func resettle(o Observation, id string, from, to Settlement) (Observation, bool) {
	changed := false
	for i := range o.Dels {
		if o.Dels[i].ID == id && o.Dels[i].Settlement == from {
			o.Dels[i].Settlement = to
			changed = true
		}
	}
	return o, changed
}

// swapFirst exchanges the positions of the first deliveries of a and b, which
// reorders them relative to each other and to nothing else.
func swapFirst(o Observation, a, b string) (Observation, bool) {
	ia, ib := -1, -1
	for i := range o.Dels {
		if o.Dels[i].ID == a && ia < 0 {
			ia = i
		}
		if o.Dels[i].ID == b && ib < 0 {
			ib = i
		}
	}
	if ia < 0 || ib < 0 || ia == ib {
		return o, false
	}
	o.Dels[ia], o.Dels[ib] = o.Dels[ib], o.Dels[ia]
	return o, true
}

// moveKeyLast moves every delivery carrying key to the end, preserving their
// relative order: the within-key order is untouched and the global order is
// not.
func moveKeyLast(o Observation, key string) (Observation, bool) {
	var rest, moved []Del
	for i := range o.Dels {
		if o.Dels[i].Key == key {
			moved = append(moved, o.Dels[i])
			continue
		}
		rest = append(rest, o.Dels[i])
	}
	if len(moved) == 0 || len(rest) == 0 {
		return o, false
	}
	// Already last: nothing changed, and saying so is what stops an
	// assertion on a no-op.
	if sameDels(o.Dels[len(o.Dels)-len(moved):], moved) {
		return o, false
	}
	out := make([]Del, 0, len(rest)+len(moved))
	out = append(out, rest...)
	out = append(out, moved...)
	o.Dels = out
	return o, true
}

func sameDels(a, b []Del) bool {
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
