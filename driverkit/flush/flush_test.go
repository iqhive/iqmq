package flush

import (
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
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
// (front-matter `auto: {flush: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "flush")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds flush yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds flush to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestKindNames holds the slug and identifier tables to the enumeration: a
// kind whose slug is empty is one a benchmark name and a DSN cannot express,
// and a slug that round-trips to a different kind is a configuration silently
// changing meaning.
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
			t.Fatalf("ParseKind(%q) = %v, %v; want %v", slug, got, ok, k)
		}
		if !k.Valid() {
			t.Fatalf("%v is not Valid", k)
		}
	}
	if _, ok := ParseKind("nonesuch"); ok {
		t.Fatal("ParseKind accepted a slug no kind has")
	}
	if kindCount.Valid() {
		t.Fatal("the count sentinel reports itself as a kind")
	}
	if got := kindCount.String(); got != "kind(?)" {
		t.Fatalf("out-of-range String() = %q", got)
	}
	if got := kindCount.Ident(); got != "" {
		t.Fatalf("out-of-range Ident() = %q", got)
	}
	if got := Slugs(); len(got) != int(kindCount)-1 || got[0] != Immediate.String() {
		t.Fatalf("Slugs() = %v", got)
	}
}

// TestAutoResolves checks that Auto never escapes into the kit.
//
// A driver that handed driverkit a kind meaning "ask the bake-off" would get
// the kit's own zero value, which is Immediate — the right answer today and
// silently the wrong one the moment the campaign moves AutoKind.
func TestAutoResolves(t *testing.T) {
	if Auto.Resolve() != AutoKind {
		t.Fatalf("Auto resolved to %v, want %v", Auto.Resolve(), AutoKind)
	}
	if AutoKind == Auto {
		t.Fatal("AutoKind is Auto, so Resolve does not terminate on a concrete kind")
	}
	p, err := New(Auto, Config{})
	if err != nil {
		t.Fatalf("New(Auto): %v", err)
	}
	if p.Kind() != AutoKind {
		t.Fatalf("New(Auto) built %v, want %v", p.Kind(), AutoKind)
	}
	if got, want := Auto.Batching(), AutoKind.Batching(); got != want {
		t.Fatalf("Auto.Batching() = %v, want %v", got, want)
	}
}

// TestBatchingIsOneToOne holds [Kind.Batching] to the kit's enumeration.
//
// Every concrete kind must map to a DIFFERENT driverkit strategy. Two kinds
// sharing one strategy would make the bake-off measure the same mechanism
// twice under two names and report a winner between them, which is how a family decides nothing while appearing to decide something.
func TestBatchingIsOneToOne(t *testing.T) {
	seen := map[driverkit.SettleBatching]Kind{}
	for _, k := range Kinds() {
		b := k.Batching()
		if !b.Valid() {
			t.Fatalf("%v maps to %v, which driverkit does not know", k, b)
		}
		if prev, dup := seen[b]; dup {
			t.Fatalf("kinds %v and %v both map to driverkit.%v", prev, k, b)
		}
		seen[b] = k
		if b.String() != k.String() {
			t.Fatalf("%v maps to driverkit.%v; the two enumerations share their slugs and a mismatch means one of them was renamed alone", k, b)
		}
	}
	if len(seen) != int(kindCount)-1 {
		t.Fatalf("%d kinds cover %d driverkit strategies", kindCount-1, len(seen))
	}
}

// TestNewRefusals checks that a policy is never substituted for another, which
// is the specification's "none silently chosen".
func TestNewRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind Kind
		cfg  Config
		want error
	}{
		{"unknown kind", kindCount, Config{}, ErrInvalidKind},
		{"unknown kind above the count", Kind(200), Config{}, ErrInvalidKind},
		{"negative items", Hybrid, Config{MaxItems: -1}, ErrInvalidConfig},
		{"negative interval", Hybrid, Config{Interval: -time.Second}, ErrInvalidConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(tc.kind, tc.cfg)
			if !errors.Is(err, tc.want) {
				t.Fatalf("New = %v, want %v", err, tc.want)
			}
			if p != nil {
				t.Fatalf("New returned a policy %v beside its error", p.Kind())
			}
			if mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("code = %v, want CodeInvalidConfig", mq.CodeOf(err))
			}
		})
	}
}

// TestDefaultsMatchTheKit is the other half a policy and
// the [driverkit.BatchSettler] it is bound to may not disagree about what a
// zero means, or a configuration that named neither would produce a policy
// deciding at ten and a settler splitting at something else.
func TestDefaultsMatchTheKit(t *testing.T) {
	if DefaultMaxItems != driverkit.DefaultSettleBatch {
		t.Fatalf("DefaultMaxItems=%d, driverkit.DefaultSettleBatch=%d", DefaultMaxItems, driverkit.DefaultSettleBatch)
	}
	if DefaultInterval != driverkit.DefaultSettleInterval {
		t.Fatalf("DefaultInterval=%v, driverkit.DefaultSettleInterval=%v", DefaultInterval, driverkit.DefaultSettleInterval)
	}
	p, err := New(Hybrid, Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.MaxItems() != DefaultMaxItems {
		t.Fatalf("a zero Config produced MaxItems=%d", p.MaxItems())
	}
	if d, ok := p.Ticker(); !ok || d != DefaultInterval {
		t.Fatalf("a zero Config produced Ticker=%v,%v", d, ok)
	}
}
