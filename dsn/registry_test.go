package dsn_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/memory"
)

// fakeDriver is a Driver whose Open records the DSN it was handed.
type fakeDriver struct {
	name    string
	schemes []string
	got     string
}

func (d *fakeDriver) Name() string      { return d.name }
func (d *fakeDriver) Schemes() []string { return d.schemes }

func (d *fakeDriver) Open(_ context.Context, s string) (*mq.Client, error) {
	d.got = s
	return nil, nil //nolint:nilnil // the fake opens nothing; the test reads d.got
}

// noopOpen is an opener a registration test never calls.
func noopOpen(context.Context, string) (*mq.Client, error) {
	return nil, nil //nolint:nilnil // never called; the test only registers it
}

// TestRegistry pins ADR-012's registration rules: a scheme is claimed once, a
// DSN reaches the driver that claimed its scheme whole, and an unclaimed scheme
// is ErrDriverNotRegistered rather than a parse failure.
func TestRegistry(t *testing.T) {
	ctx := context.Background()
	var r dsn.Registry // the zero value is usable
	d := &fakeDriver{name: "memory", schemes: []string{"memory", "mem"}}
	if err := r.RegisterDriver(d); err != nil {
		t.Fatalf("RegisterDriver: %v", err)
	}
	if got := r.Schemes(); !reflect.DeepEqual(got, []string{"mem", "memory"}) {
		t.Errorf("Schemes is %q", got)
	}
	if name, ok := r.DriverName("mem"); !ok || name != "memory" {
		t.Errorf("DriverName(mem) is %q, %v", name, ok)
	}
	const want = "mem://?autocreate=true&x=1"
	if _, err := r.Open(ctx, want); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if d.got != want {
		t.Errorf("the driver was handed %q, want the whole DSN %q", d.got, want)
	}
	t.Run("an unclaimed scheme", func(t *testing.T) {
		_, err := r.Open(ctx, "kafka://b1:9092")
		if !errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Fatalf("Open: %v", err)
		}
	})
	t.Run("a malformed DSN is not an unregistered driver", func(t *testing.T) {
		_, err := r.Open(ctx, "memory")
		if mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Fatalf("Open: %v (code %v)", err, mq.CodeOf(err))
		}
	})
	t.Run("a scheme is claimed once", func(t *testing.T) {
		err := r.Register("mem", noopOpen)
		if mq.CodeOf(err) != mq.CodeConflict {
			t.Fatalf("a second Register: %v (code %v)", err, mq.CodeOf(err))
		}
	})
	t.Run("refusals", func(t *testing.T) {
		if err := r.Register("kafka", nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("a nil opener: %v", err)
		}
		if err := r.Register("", noopOpen); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("an empty scheme: %v", err)
		}
		if err := r.Register("not a scheme", noopOpen); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("a malformed scheme: %v", err)
		}
		if err := r.RegisterDriver(nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("a nil driver: %v", err)
		}
		if err := r.RegisterDriver(&fakeDriver{name: "x"}); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("a driver with no schemes: %v", err)
		}
	})
	t.Run("a colliding driver registers nothing", func(t *testing.T) {
		var reg dsn.Registry
		if err := reg.Register("a", noopOpen); err != nil {
			t.Fatalf("Register: %v", err)
		}
		clash := &fakeDriver{name: "clash", schemes: []string{"b", "a"}}
		if err := reg.RegisterDriver(clash); mq.CodeOf(err) != mq.CodeConflict {
			t.Fatalf("RegisterDriver: %v", err)
		}
		if _, ok := reg.Lookup("b"); ok {
			t.Error("a driver whose second scheme collided left its first scheme registered")
		}
	})
}

// TestTheRegistryCanonicalisesTheScheme holds the registry to the grammar's one
// spelling of a scheme.
//
// A scheme is case-insensitive (RFC 3986) and Parse lower-cases the one a DSN
// carries, so Open looks a DSN up by "kafka" whatever it was written as. The
// registry used to STORE a registration as it was spelled: `Register("Kafka")`
// succeeded, no DSN could reach it, and a later `Register("kafka")` was not
// the conflict it is.
func TestTheRegistryCanonicalisesTheScheme(t *testing.T) {
	ctx := context.Background()
	var r dsn.Registry
	d := &fakeDriver{name: "mixed", schemes: []string{"MiXed+V1"}}
	if err := r.RegisterDriver(d); err != nil {
		t.Fatalf("RegisterDriver: %v", err)
	}
	if got := r.Schemes(); !reflect.DeepEqual(got, []string{"mixed+v1"}) {
		t.Errorf("Schemes is %q, want the canonical spelling", got)
	}
	for _, spelling := range []string{"mixed+v1", "MIXED+V1", "MiXed+V1"} {
		if _, ok := r.Lookup(spelling); !ok {
			t.Errorf("Lookup(%q) found nothing", spelling)
		}
		if name, ok := r.DriverName(spelling); !ok || name != "mixed" {
			t.Errorf("DriverName(%q) is %q, %v", spelling, name, ok)
		}
	}
	for _, s := range []string{"mixed+v1://h", "MIXED+V1://h"} {
		d.got = ""
		if _, err := r.Open(ctx, s); err != nil {
			t.Fatalf("Open(%q): %v", s, err)
		}
		if d.got != s {
			t.Errorf("Open(%q) reached the driver with %q", s, d.got)
		}
	}
	if err := r.Register("mixed+v1", noopOpen); mq.CodeOf(err) != mq.CodeConflict {
		t.Errorf("a second registration in another spelling: %v (code %v), want a conflict", err, mq.CodeOf(err))
	}
	// A string that is a scheme followed by more of a DSN is not a scheme.
	// Appending "://" and splitting at the first one read "a://b" as "a".
	if err := r.Register("a://b", noopOpen); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Errorf("Register(%q): %v (code %v), want it refused", "a://b", err, mq.CodeOf(err))
	}
	if _, ok := r.Lookup("not a scheme"); ok {
		t.Error("Lookup found something for a string that is not a scheme")
	}

	// Register, the one-scheme route and the one dsn/global exposes, is the
	// route the defect was named on: `Register("Kafka", f)` stored "Kafka",
	// Open looked up "kafka", and f was unreachable.
	var one dsn.Registry
	var reached string
	if err := one.Register("KaFka", func(_ context.Context, s string) (*mq.Client, error) {
		reached = s
		return nil, nil //nolint:nilnil // the opener records what it was handed; the test reads it
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := one.Schemes(); !reflect.DeepEqual(got, []string{"kafka"}) {
		t.Errorf("after Register(%q) Schemes is %q, want [kafka]", "KaFka", got)
	}
	for _, spelling := range []string{"kafka", "KAFKA", "KaFka"} {
		if _, ok := one.Lookup(spelling); !ok {
			t.Errorf("after Register(%q) Lookup(%q) found nothing", "KaFka", spelling)
		}
		if name, ok := one.DriverName(spelling); !ok || name != "kafka" {
			t.Errorf("after Register(%q) DriverName(%q) is %q, %v, want the canonical scheme", "KaFka", spelling, name, ok)
		}
	}
	if _, err := one.Open(ctx, "kafka://h"); err != nil || reached != "kafka://h" {
		t.Errorf("Open(%q) after Register(%q) = %v and reached the opener with %q", "kafka://h", "KaFka", err, reached)
	}
}

// TestRegisterDriverRefusesOneSchemeTwice holds RegisterDriver to the rule
// Register already keeps: a scheme is claimed once. A driver listing the same
// scheme twice -- in one spelling or two -- is the same programming error as
// two drivers claiming it, and it used to register silently, because the check
// looked only at what was registered BEFORE the call.
func TestRegisterDriverRefusesOneSchemeTwice(t *testing.T) {
	for _, schemes := range [][]string{{"kafka", "KAFKA"}, {"kafka", "kafka"}, {"a", "b", "A"}} {
		var r dsn.Registry
		err := r.RegisterDriver(&fakeDriver{name: "twice", schemes: schemes})
		if mq.CodeOf(err) != mq.CodeConflict {
			t.Errorf("RegisterDriver(%q) = %v (code %v), want a conflict", schemes, err, mq.CodeOf(err))
		}
		if got := r.Schemes(); len(got) != 0 {
			t.Errorf("RegisterDriver(%q) failed and left %q registered", schemes, got)
		}
	}
}

// memory.Register takes the real dsn.Registrar rather than an interface of its
// own, which is the placeholder retired. The assignment is the
// check: a driver that re-declared the interface locally would still work, and
// would leave two spellings of one contract in the repository.
var _ func(dsn.Registrar) error = memory.Register

// TestMemoryRegistersWithTheRealRegistry is the seam closed: the
// memory driver's Register takes dsn.Registrar, and *dsn.Registry satisfies it.
func TestMemoryRegistersWithTheRealRegistry(t *testing.T) {
	ctx := context.Background()
	r := dsn.NewRegistry()
	if err := memory.Register(r); err != nil {
		t.Fatalf("memory.Register: %v", err)
	}
	for _, scheme := range []string{memory.Scheme, memory.SchemeAlias} {
		if _, ok := r.Lookup(scheme); !ok {
			t.Fatalf("the %q scheme was not registered", scheme)
		}
	}
	client, err := r.Open(ctx, "mem://?autocreate=true")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish through a registry-opened client: %v", err)
	}
	var reg dsn.Registrar = r
	if err := memory.Register(reg); mq.CodeOf(err) != mq.CodeConflict {
		t.Fatalf("a second memory.Register: %v (code %v)", err, mq.CodeOf(err))
	}
}
