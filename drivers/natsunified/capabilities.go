package natsunified

import mq "github.com/iqhive/iqmq"

// This file holds the facade's capability DECISION and no capability table. The
// distinction is the whole of it.
//
// The specification makes a capability table a set of claims about a live deployment, and
// the two profiles do not have the same one:
//
//   - Core cannot settle at all. `Source.Settler()` is nil, so the core answers
//     every Ack, Nack and Reject with ErrNotSettleable (AckNone).
//   - JetStream settles, leases, redelivers, replays, seeks, dead-letters and
//     de-duplicates, and two of its claims flip on the stream's own flags.
//
// So a facade has three things it could return from `Capabilities()`:
//
//  1. **The union.** It would claim FeatureExplicitAck, FeatureAtLeastOnce and
//     FeatureDurableStorage on a broker that has none of them, which is the one
//     thing exists to prevent — a caller reads the table to decide whether
//     it may rely on a behaviour, and on the Core profile this table would say
//     "yes" to fifteen behaviours that answer ErrNotSettleable or silently drop
//     a message.
//  2. **The intersection.** It would withdraw those same fifteen claims on the
//     JetStream profile, where they are true and native. A table that
//     understates is safer than one that overstates and it is still false: a
//     caller that read it would emulate at-least-once delivery on a broker that
//     provides it, and `RequireAtLeast(FeatureExplicitAck, Native)` would refuse
//     a client that can honour it.
//  3. **The selected profile's own table, unchanged.** Which is what this
//     facade does, and it does it by not having a table at all: [Open] returns
//     the profile driver's client, so `Capabilities()` is that driver's method
//     on that driver's value. There is no copy to drift and no merge to be
//     wrong. TestCapabilitiesAreTheProfilesOwn proves the equality feature by
//     feature, level, scope and note, against both profiles.
//
// What a caller loses by choosing a profile at run time is therefore not
// accuracy — the table is exact once the client exists — but the ability to
// know the answer BEFORE it exists. [Common] is what this package offers for
// that, and it is deliberately not a [github.com/iqhive/iqmq.Capabilities]: a
// capability value carries levels, scopes, notes and limits, and every one of
// those differs between the profiles even for a feature they share.

// Common is the features BOTH profiles advertise at some level, on every
// deployment: what a caller who selects a profile from a DSN at run time may
// rely on being able to call at all.
//
// It is a feature list and never a capability table. Three things about it have
// to be read carefully, and they are the honest answer to "what does a
// DSN-selected profile guarantee":
//
//   - **A feature in this list may mean different things.**
//     [github.com/iqhive/iqmq.FeaturePublisherConfirm] is native on both
//     profiles: on Core it is a PING/PONG round trip that proves the server
//     parsed the publication and stored nothing, and on JetStream it is a
//     PubAck the server sends after committing the message to the stream. Both
//     are honest "publisher confirm" claims and they are not interchangeable.
//     Only `Capabilities().Support(f).Note` says which one a caller has.
//   - **A feature in this list may be at a different LEVEL.**
//     [github.com/iqhive/iqmq.FeatureBatchPublish] is emulated on both, and it
//     is EmulatedVolatile on Core (one flush confirms the batch; nothing is
//     stored) and EmulatedDurable on JetStream (every message's PubAck is
//     awaited). The specification makes volatile emulation never satisfy a durability
//     requirement, so a `RequireAtLeast(FeatureBatchPublish, EmulatedDurable)`
//     that passes on one profile fails on the other — from a feature both
//     "have".
//   - **A feature NOT in this list is not necessarily absent.** Nine of the
//     features one profile advertises are missing here because the OTHER
//     profile does not have them, including everything about settlement
//     (JetStream's) and request/reply (Core's).
//
// The list is the intersection of the two live tables and not a hand-written
// guess: TestCommonIsTheLiveIntersection opens both profiles against a real
// server and compares, so a claim added to or removed from either driver fails
// this package rather than silently widening or narrowing what the facade
// promises.
//
// The returned slice is freshly allocated, so a caller may sort or filter it.
func Common() []mq.Feature {
	out := make([]mq.Feature, len(commonFeatures))
	copy(out, commonFeatures)
	return out
}

// commonFeatures is [Common]'s data, in the declaration order.
//
// Every entry is unconditional in BOTH drivers' tables — none of them is read
// off a stream flag or a server version — which is what makes "on every
// deployment" true rather than "on the deployment this list was written
// against". The two claims JetStream reads off its stream (MessageTTL,
// ScheduledPublish) and the two it reads off its storage (DurableStorage,
// DurableConfirm) are all absent from Core's table anyway, so they could not
// have reached this list; the note is here because a future profile that made
// one of these conditional would break the promise without changing the list.
var commonFeatures = []mq.Feature{
	// Fan-out and consumer groups are the two subscription shapes, and
	// both profiles have them natively: a subscription name is a queue group
	// on Core and a durable's name on JetStream.
	mq.FeatureConsumerGroups,
	mq.FeatureFanOut,
	// Ordering. All three features are conditional on both profiles, and both
	// conditions reduce to the same sentence, because neither broker has
	// partitions or message keys: one publisher's messages on one subject,
	// delivered to a single consumer.
	mq.FeatureOrderedByKey,
	mq.FeatureOrderedByPartition,
	mq.FeatureOrderedGlobally,
	// NATS headers are text lines on both profiles, which is why binary
	// headers are on neither list and duplicate ones are on both.
	mq.FeatureNativeHeaders,
	mq.FeatureDuplicateHeaders,
	// A batch publication is the driver's own on both, and at DIFFERENT
	// levels: see the doc comment above.
	mq.FeatureBatchPublish,
	mq.FeatureBatchReceive,
	mq.FeatureWildcardSubscribe,
	// A publisher confirm with two very different boundaries: see the doc
	// comment above.
	mq.FeaturePublisherConfirm,
}

// CommonHas reports whether f is one of [Common]'s features, without allocating
// the slice. It is the form a gate in a caller's own code wants.
func CommonHas(f mq.Feature) bool {
	for _, c := range commonFeatures {
		if c == f {
			return true
		}
	}
	return false
}
