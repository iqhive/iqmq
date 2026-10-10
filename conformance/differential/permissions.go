package differential

import (
	"fmt"
	"strings"

	mq "github.com/iqhive/iqmq"
)

// PermissionsFor derives the relation's permissions from what the subject
// advertises.
//
// This direction is the whole design. A permission is not a concession the
// test makes to get to green; it is the driver's own claim read back at it. A
// driver that advertises FeatureOrderedByKey is held to per-key order, and the
// only way to make a failing run pass is to change the claim — at which point
// the conformance suite's negative probes take over and require the driver to
// refuse what it no longer claims. A run whose permissions came from a
// hand-written table would have neither property.
//
// The four brokers whose ordering claim is nothing at all — SQS Standard,
// Pub/Sub without ordering keys, NSQ, Redis with several consumers — get
// ReorderWithinKey, and for them the differential is a loss, duplication and
// settlement test rather than an ordering one. That is the honest reading and
// it is reported: [Summary.OrderedKeys] is zero for those runs, so a reader
// can see which half of the relation actually bound the subject.
func PermissionsFor(caps mq.Capabilities) Permissions {
	ordered := caps.Has(mq.FeatureOrderedByKey) ||
		caps.Has(mq.FeatureOrderedByPartition) ||
		caps.Has(mq.FeatureOrderedGlobally)
	return Permissions{
		// At-least-once permits duplicates; exactly-once delivery does
		// not, and a driver claiming it is held to it.
		Duplicates:       !caps.Has(mq.FeatureExactlyOnceDelivery),
		ReorderWithinKey: !ordered,
		// Global order is permitted to differ unless the driver claims it
		// across the whole topic, which almost nothing does.
		ReorderAcrossKeys: !caps.Has(mq.FeatureOrderedGlobally),
		// An ambiguous publish is ambiguous in both directions by
		// definition: CodeSettlementUnknown says the outcome is
		// unknown, so neither arrival nor absence is a defect. A driver
		// with broker-side producer de-duplication is still allowed both,
		// because de-duplication bounds the DUPLICATE and not the loss.
		AmbiguousMayVanish: true,
		AmbiguousMayArrive: true,
		// A driver whose nack cannot ask for redelivery at all cannot be
		// held to the redelivery contract.
		LateRedelivery: !caps.Has(mq.FeatureNack),
	}
}

// Explain renders the permissions as the claims they came from, so that a run
// logs what it held the subject to rather than only its verdict.
func (p Permissions) Explain() string {
	var held, unchecked []string
	// `on` is the PERMISSION, and the string names the PROPERTY, so a
	// granted permission means the property is not checked. Rendering that
	// as "permitted" read backwards in the first run's log line —
	// "permitted [no-duplicates]" against an arm that permits duplicates.
	add := func(on bool, property string) {
		if on {
			unchecked = append(unchecked, property)
		} else {
			held = append(held, property)
		}
	}
	add(p.Duplicates, "no-duplicates")
	add(p.ReorderWithinKey, "order-within-key")
	add(p.ReorderAcrossKeys, "order-across-keys")
	add(p.LateRedelivery, "redelivery-within-the-drain-window")
	add(p.AmbiguousMayVanish && p.AmbiguousMayArrive, "ambiguous-publishes-resolve")
	var b strings.Builder
	fmt.Fprintf(&b, "held to [%s]", strings.Join(held, " "))
	if len(unchecked) > 0 {
		fmt.Fprintf(&b, ", not checked [%s]", strings.Join(unchecked, " "))
	}
	return b.String()
}
