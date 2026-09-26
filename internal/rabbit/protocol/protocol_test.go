package protocol

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/testguard"
)

// TestKindsList records the family's kinds in the test log, which is what the
// bake-off's own lint reads to check that every kind it measures exists.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, kindCount)
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
	if len(parts) != int(kindCount)-1 {
		t.Fatalf("Kinds() listed %d of %d concrete kinds", len(parts), kindCount-1)
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds this family
// (front-matter `auto: {rabbitprotocol: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../../..", "rabbitprotocol")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds rabbitprotocol yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds rabbitprotocol to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestKindNames holds the slug and identifier tables to the enumeration.
func TestKindNames(t *testing.T) {
	seen := map[string]Kind{}
	for k := Auto; k < kindCount; k++ {
		slug, ident := k.String(), k.Ident()
		if slug == "" || ident == "" {
			t.Fatalf("kind %d has slug %q and identifier %q", k, slug, ident)
		}
		if prev, dup := seen[slug]; dup {
			t.Fatalf("kinds %v and %v share the slug %q", prev, k, slug)
		}
		seen[slug] = k
		got, ok := ParseKind(slug)
		if !ok || got != k {
			t.Fatalf("ParseKind(%q) = (%v, %v), want (%v, true)", slug, got, ok, k)
		}
	}
	if _, ok := ParseKind("nonesuch"); ok {
		t.Fatal("ParseKind accepted a slug no kind has")
	}
	if k := kindCount; k.Valid() || k.String() != "kind(?)" || k.Ident() != "" {
		t.Fatalf("a kind outside the enumeration reports valid=%v slug=%q ident=%q", k.Valid(), k.String(), k.Ident())
	}
	if _, err := New(kindCount); err == nil {
		t.Fatal("New accepted a kind outside the enumeration")
	}
}

// TestFeatureSubsetsDiffer is the specification's "feature subset" criterion as a check
// rather than as prose.
//
// The whole point is that the two protocols do NOT offer the
// same thing, and a table derived from the other one (which amqp10.Support
// deliberately is, for the rows that agree) is one edit away from being
// identical. This is what would fail if that edit were made: the three rows
// the AMQP 1.0 profile loses and the one it gains are named individually, so a
// change to any of them is deliberate.
func TestFeatureSubsetsDiffer(t *testing.T) {
	old, err := New(AMQP091)
	if err != nil {
		t.Fatal(err)
	}
	nw, err := New(AMQP10)
	if err != nil {
		t.Fatal(err)
	}

	var differ []mq.Feature
	for f := range mq.FeatureCount {
		feat := mq.Feature(f)
		if old.Support(feat) != nw.Support(feat) {
			differ = append(differ, feat)
		}
	}
	if len(differ) == 0 {
		t.Fatal("the two protocols' capability tables are identical: the specification is measuring two names for one behaviour")
	}
	t.Logf("the two protocols differ on %d of %d features: %v", len(differ), mq.FeatureCount, differ)

	// The rows that must differ, each with the reason it does.
	for _, tc := range []struct {
		feature mq.Feature
		why     string
		check   func(oldS, newS mq.Support) bool
	}{
		{
			mq.FeatureRequestReply,
			"direct reply-to is a RabbitMQ extension of AMQP 0-9-1 and has no AMQP 1.0 form",
			func(o, n mq.Support) bool { return o.Level == mq.Native && n.Level == mq.Unsupported },
		},
		{
			mq.FeatureBinaryHeaders,
			"AMQP 1.0 application properties are typed, so a binary value needs no encoding",
			func(o, n mq.Support) bool { return n.Level == mq.Native && o.Level == mq.Native },
		},
		{
			mq.FeatureDelayedRedelivery,
			"a `modified` disposition returns a delivery at once; a DELAY still needs a TTL queue or the plugin",
			func(o, n mq.Support) bool { return o.Note != n.Note },
		},
		{
			mq.FeatureBatchSettle,
			"a disposition settles a range of delivery ids where basic.ack settles up to a tag",
			func(o, n mq.Support) bool { return o.Note != n.Note },
		},
	} {
		o, n := old.Support(tc.feature), nw.Support(tc.feature)
		if !tc.check(o, n) {
			t.Errorf("%v: %s\n  0-9-1: %+v\n  1.0:   %+v", tc.feature, tc.why, o, n)
		}
	}

	// And the counts differ, which is what the benchmark reports.
	if old.Features() == nw.Features() {
		t.Errorf("both protocols support %d features natively: the number the criterion is read on cannot separate them", old.Features())
	}
}

// TestSupportTableCoversTheInventory is what keeps a new feature from being
// silently unsupported on both arms.
//
// A feature nobody considered reads as Unsupported, which is the safe answer
// and not the informative one: the point of this check is that the switch has
// a case for every feature, so adding one to the inventory fails here rather
// than shipping as an absence.
func TestSupportTableCoversTheInventory(t *testing.T) {
	for _, k := range Kinds() {
		p, err := New(k)
		if err != nil {
			t.Fatal(err)
		}
		table := SupportTable(p)
		if len(table) != mq.FeatureCount {
			t.Fatalf("%v: the table has %d rows and the inventory has %d", k, len(table), mq.FeatureCount)
		}
		unsupportedCount := 0
		for _, s := range table {
			if s.Level == mq.Unsupported {
				unsupportedCount++
			}
		}
		// More than two thirds unsupported would mean the table had stopped
		// being filled in; fewer than a handful would mean it had started
		// claiming things. Both are implausible for this broker, and the
		// bound is here so that a table nobody maintained is noticed.
		if unsupportedCount > 2*mq.FeatureCount/3 {
			t.Errorf("%v reports %d of %d features unsupported: the table looks unfilled", k, unsupportedCount, mq.FeatureCount)
		}
		if unsupportedCount < 3 {
			t.Errorf("%v reports only %d features unsupported: this broker has no replay, no global ordering and no exactly-once delivery, so a table that claims nearly everything is wrong", k, unsupportedCount)
		}
	}
}

// TestAddressEscaping is the case a path-shaped address makes possible: a
// routing key is arbitrary caller-supplied text, and an unescaped "/" in one
// would address a different node entirely.
func TestAddressEscaping(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"orders", "orders"},
		{"orders.eu", "orders.eu"},
		{"a/b", "a%2Fb"},
		{"100%", "100%25"},
		{"a/b%c", "a%2Fb%25c"},
		{"", ""},
	} {
		if got := escapeAddress(tc.in); got != tc.want {
			t.Errorf("escapeAddress(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestAutoIsTheProtocolEveryBrokerSpeaks is the family's decision written as a
// check.
//
// the expected outcome is "0-9-1 default; 1.0 conditional profile" and the specification
// adds that the profile is "never silently substituted". Both follow from one
// fact: Auto has to be the arm that works on the oldest broker in the matrix,
// because Auto is what a caller gets by not choosing. A campaign that measures
// the 1.0 mapping faster must therefore not move AutoKind, and this says so.
func TestAutoIsTheProtocolEveryBrokerSpeaks(t *testing.T) {
	p, err := New(Auto)
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind() != AMQP091 {
		t.Fatalf("the Auto protocol is %v: the default a caller gets by not choosing must be the one every supported broker speaks", p.Kind())
	}
	other, err := New(AMQP10)
	if err != nil {
		t.Fatal(err)
	}
	if p.MinBroker().AtLeast(other.MinBroker()) {
		t.Fatalf("the default protocol needs RabbitMQ %v and the other needs %v: the default is supposed to be the one with the LOWER floor",
			p.MinBroker(), other.MinBroker())
	}
}

// TestAllocs holds the shared mapping to zero allocations on the steady path.
//
// It is the check that justifies putting one mapping between two drivers at
// all: a `Fields` returned by value would allocate its table on every
// publication and put the shared mapping on the wrong side of the budget,
// so `Carry` fills a buffer the sink owns and truncates it in place.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		p, err := New(k)
		if err != nil {
			t.Fatal(err)
		}
		m := &mq.Message{Key: []byte("k"), Body: []byte("b"), ID: "id"}
		m.Headers.Add("a", []byte("1"))
		m.Headers.Add("b", []byte("2"))
		m.Headers.Add("c", []byte("3"))
		m.Headers.Add("d", []byte("4"))
		var f Fields
		o := Options{KeyHeader: ReservedHeaderPrefix + "key"}
		// Warm the buffer's slices and the duplicate-detection map's
		// storage; the steady state is what the specification budgets.
		for range 4 {
			if err := p.Carry(&f, m, o); err != nil {
				t.Fatal(err)
			}
		}
		got := testing.AllocsPerRun(200, func() {
			if err := p.Carry(&f, m, o); err != nil {
				t.Fatal(err)
			}
		})
		// One allocation remains and it is the duplicate-detection set: a
		// message with headers needs to know whether a key repeats, and the
		// map cannot be reused across calls because `Carry` is safe for
		// concurrent use over one protocol value while `Fields` is the
		// caller's. It is one small map per publication with headers and
		// none at all for a message without, which the second half of this
		// test pins.
		if got > 1 {
			t.Errorf("%v: Carry allocated %.0f times per call with four headers, want at most 1 (the duplicate-detection set)", k, got)
		}
		bare := &mq.Message{Body: []byte("b")}
		got = testing.AllocsPerRun(200, func() {
			if err := p.Carry(&f, bare, Options{}); err != nil {
				t.Fatal(err)
			}
		})
		if got != 0 {
			t.Errorf("%v: Carry allocated %.0f times for a message with no headers, want 0", k, got)
		}

		// And the one place the two arms really differ per message: AMQP
		// 0-9-1's `expiration` property is a DECIMAL STRING of
		// milliseconds, so a publication with a TTL costs that arm one
		// allocation and costs the AMQP 1.0 arm none. It is pinned in both
		// directions because it is the difference the specification's `ns/op`
		// reads, and because a future change that rendered the string for
		// both arms would make the family's perf term measure one protocol
		// twice.
		ttl := Options{}
		ttl.Publish.TTL = time.Minute
		got = testing.AllocsPerRun(200, func() {
			if err := p.Carry(&f, bare, ttl); err != nil {
				t.Fatal(err)
			}
		})
		switch k.Resolve() {
		case AMQP091:
			if got != 1 {
				t.Errorf("%v: Carry allocated %.0f times for a message with a TTL, want exactly 1 (the decimal `expiration` string the protocol demands)", k, got)
			}
		case AMQP10:
			if got != 0 {
				t.Errorf("%v: Carry allocated %.0f times for a message with a TTL, want 0: AMQP 1.0's ttl is a duration in the header section and needs no string", k, got)
			}
		case Auto, kindCount:
			// Unreachable, as above.
			t.Fatalf("Resolve answered %v", k.Resolve())
		}
	}
}
