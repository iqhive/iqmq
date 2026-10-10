package differential

import (
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

func caps(t *testing.T, features ...mq.Feature) mq.Capabilities {
	t.Helper()
	var c mq.Capabilities
	for _, f := range features {
		if err := c.Set(f, mq.Support{Level: mq.Native}); err != nil {
			t.Fatalf("setting %v: %v", f, err)
		}
	}
	return c
}

// TestPermissionsFollowTheClaims is the check that keeps the relation from
// being loosened by hand. Each row names a claim and the permission it must
// and must not grant; a table that only asserted the loose end would pass for
// a PermissionsFor that returned Permissive() unconditionally.
func TestPermissionsFollowTheClaims(t *testing.T) {
	t.Run("a driver that claims nothing is held to nothing but loss", func(t *testing.T) {
		p := PermissionsFor(mq.Capabilities{})
		if !p.Duplicates || !p.ReorderWithinKey || !p.ReorderAcrossKeys {
			t.Fatalf("an empty capability table was held to %+v", p)
		}
	})
	t.Run("ordered by key is held to per-key order", func(t *testing.T) {
		p := PermissionsFor(caps(t, mq.FeatureOrderedByKey, mq.FeatureAtLeastOnce, mq.FeatureNack))
		if p.ReorderWithinKey {
			t.Error("a driver claiming FeatureOrderedByKey was permitted to reorder within a key")
		}
		if !p.ReorderAcrossKeys {
			t.Error("a driver claiming only per-key order was held to global order")
		}
		if !p.Duplicates {
			t.Error("an at-least-once driver was forbidden duplicates")
		}
		if p.LateRedelivery {
			t.Error("a driver claiming FeatureNack was excused the redelivery contract")
		}
	})
	t.Run("ordered globally is held to both", func(t *testing.T) {
		p := PermissionsFor(caps(t, mq.FeatureOrderedGlobally))
		if p.ReorderWithinKey || p.ReorderAcrossKeys {
			t.Fatalf("a driver claiming global order was permitted to reorder: %+v", p)
		}
	})
	t.Run("exactly-once delivery forbids duplicates", func(t *testing.T) {
		if PermissionsFor(caps(t, mq.FeatureExactlyOnceDelivery)).Duplicates {
			t.Error("a driver claiming exactly-once delivery was permitted duplicates")
		}
	})
	t.Run("a driver with no nack cannot be held to redelivery", func(t *testing.T) {
		if !PermissionsFor(caps(t, mq.FeatureAtLeastOnce)).LateRedelivery {
			t.Error("a driver with no FeatureNack was held to the redelivery contract")
		}
	})
}

// TestExplainNamesTheClaimsHeld keeps the log line honest: a run that reports
// only "ok" does not say whether the ordering half of the relation bound
// anything.
func TestExplainNamesTheClaimsHeld(t *testing.T) {
	got := PermissionsFor(caps(t, mq.FeatureOrderedByKey, mq.FeatureAtLeastOnce, mq.FeatureNack)).Explain()
	for _, want := range []string{"order-within-key", "not checked", "order-across-keys"} {
		if !strings.Contains(got, want) {
			t.Errorf("Explain() = %q, missing %q", got, want)
		}
	}
	if strings.Index(got, "order-within-key") > strings.Index(got, "not checked") {
		t.Errorf("Explain() = %q: order-within-key is reported as unchecked, not held", got)
	}
	// And the other way: a driver that permits reordering within a key must
	// not have it listed as held, or the log line claims a check nobody ran.
	loose := PermissionsFor(mq.Capabilities{}).Explain()
	if strings.Index(loose, "order-within-key") < strings.Index(loose, "not checked") {
		t.Errorf("Explain() = %q: an unordered driver is reported as held to per-key order", loose)
	}
}
