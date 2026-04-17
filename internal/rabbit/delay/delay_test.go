package delay

import (
	"errors"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/testguard"
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
// (front-matter `auto: {rabbitdelay: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../../..", "rabbitdelay")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds rabbitdelay yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds rabbitdelay to %q but AutoKind is %q", slug, AutoKind)
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
}

// TestAutoIsTheArmThatNeedsNoPlugin is the family's decision written as a
// check.
//
// the expected outcome is "plugin when detected, TTL+DLX fallback", and the
// reason the better arm is not the default is that Auto is what a caller gets
// by NOT choosing: the plugin is not in the official image, so a default that
// assumed it would fail at open against a stock broker. A campaign that
// measures the plugin faster on every metric must therefore not move AutoKind,
// and this is what says so.
func TestAutoIsTheArmThatNeedsNoPlugin(t *testing.T) {
	st, err := New(Auto, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Requires(); got != RequiresNothing {
		t.Fatalf("the Auto delay strategy requires %v: the default a caller gets by not choosing must work on a stock RabbitMQ image", got)
	}
	// And the other arm really does require something, or this check is
	// vacuous.
	other, err := New(Plugin, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if other.Requires() != RequiresDelayedExchange {
		t.Fatalf("the plugin strategy requires %v, want %v", other.Requires(), RequiresDelayedExchange)
	}
}

// TestConfigValidation holds each refusal to an input that triggers it and one
// that does not.
func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind Kind
		cfg  Config
		want error
	}{
		{"default", TTLDLX, Config{}, nil},
		{"explicit ladder", TTLDLX, Config{Ladder: []time.Duration{time.Second, time.Minute}}, nil},
		{"queue ttl", TTLDLX, Config{QueueTTL: time.Hour}, nil},
		{"unknown kind", kindCount, Config{}, ErrInvalidKind},
		{"ladder not increasing", TTLDLX, Config{Ladder: []time.Duration{time.Minute, time.Second}}, ErrInvalidLadder},
		{"ladder repeats", TTLDLX, Config{Ladder: []time.Duration{time.Second, time.Second}}, ErrInvalidLadder},
		{"ladder has a zero", TTLDLX, Config{Ladder: []time.Duration{0, time.Second}}, ErrInvalidLadder},
		{"ladder has a negative", TTLDLX, Config{Ladder: []time.Duration{-time.Second}}, ErrInvalidLadder},
		{"negative queue ttl", TTLDLX, Config{QueueTTL: -time.Second}, ErrInvalidQueueTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate(tc.kind)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
			st, nerr := New(tc.kind, tc.cfg)
			if (nerr == nil) != (tc.want == nil) {
				t.Fatalf("New = (%v, %v) and Validate said %v", st, nerr, err)
			}
		})
	}
}

// TestExplicitLadderRemovesTheRoundingError is the escape hatch the default
// ladder's error exists to make unnecessary, checked rather than described: a
// deployment that names its own delays gets them exactly AND gets one queue
// per delay it really uses.
func TestExplicitLadderRemovesTheRoundingError(t *testing.T) {
	delays := []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute}
	st, err := New(TTLDLX, Config{Ladder: delays})
	if err != nil {
		t.Fatal(err)
	}
	reqs := make([]Request, 0, len(delays)*10)
	for range 10 {
		for _, d := range delays {
			p, perr := st.Plan(request(d))
			if perr != nil {
				t.Fatalf("delay %v refused: %v", d, perr)
			}
			if p.Granted != d {
				t.Errorf("a named delay of %v was granted %v: a ladder that contains the delay exactly must round nothing", d, p.Granted)
			}
			reqs = append(reqs, request(d))
		}
	}
	if got := st.Queues(reqs); got != len(delays) {
		t.Errorf("thirty requests over three named delays need %d queues, want %d", got, len(delays))
	}
	// And the default ladder really does round these, so the comparison is
	// not vacuous.
	def, err := New(TTLDLX, Config{})
	if err != nil {
		t.Fatal(err)
	}
	rounded := 0
	for _, d := range delays {
		p, perr := def.Plan(request(d))
		if perr != nil {
			t.Fatal(perr)
		}
		if p.Granted != d {
			rounded++
		}
	}
	if rounded == 0 {
		t.Error("the default ladder granted all three delays exactly, so this test is not comparing anything")
	}
}

// TestDelayTooLongIsUnsupportedAndNotInvalid pins the error code, because a
// caller acts on it: a delay this broker cannot express is Unsupported
// and not a malformed argument, so a middleware that retries invalid arguments
// does not retry it for ever.
func TestDelayTooLongIsUnsupportedAndNotInvalid(t *testing.T) {
	st := newStrategy(t, TTLDLX)
	_, err := st.Plan(request(st.MaxDelay() + time.Second))
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a delay beyond the ceiling = %v, want ErrUnsupported", err)
	}
	if code := mq.CodeOf(err); code != mq.CodeUnsupported {
		t.Fatalf("the refusal carries code %v, want %v", code, mq.CodeUnsupported)
	}
	_, err = st.Plan(request(-time.Second))
	if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
		t.Fatalf("a negative delay carries code %v, want %v: that one IS a malformed argument", code, mq.CodeInvalidArgument)
	}
}

// TestPluginRefusesARequestWithNoDelayExchange is the configuration mistake
// this kind can be made with: the exchange is per topic, so the request
// carries it, and a plan built without it would publish to the default
// exchange with a routing key that reaches no queue and lose the message
// silently.
func TestPluginRefusesARequestWithNoDelayExchange(t *testing.T) {
	st := newStrategy(t, Plugin)
	req := request(time.Minute)
	req.DelayExchange = ""
	if _, err := st.Plan(req); !errors.Is(err, ErrNoDelayExchange) {
		t.Fatalf("a plugin plan with no delay exchange = %v, want ErrNoDelayExchange", err)
	}
	// But a request for NO delay needs no exchange, because it does not go
	// through one.
	req.Delay = 0
	if _, err := st.Plan(req); err != nil {
		t.Fatalf("an immediate redelivery needs no delay exchange: %v", err)
	}
}

// TestDefaultLadderShape pins the two numbers the accuracy and the queue count
// both follow from, so that a change to either is a deliberate one.
func TestDefaultLadderShape(t *testing.T) {
	l := DefaultLadder()
	if len(l) != LadderSteps {
		t.Fatalf("the default ladder has %d buckets, want %d", len(l), LadderSteps)
	}
	if l[0] != LadderBase {
		t.Fatalf("the default ladder starts at %v, want %v", l[0], LadderBase)
	}
	for i := 1; i < len(l); i++ {
		if l[i] != 2*l[i-1] {
			t.Fatalf("bucket %d is %v and bucket %d is %v: the ladder doubles, which is what bounds the rounding error at a factor of two", i-1, l[i-1], i, l[i])
		}
	}
	if last := l[len(l)-1]; last < 24*time.Hour {
		t.Fatalf("the default ladder reaches only %v: a day is the shortest ceiling a retry schedule needs", last)
	}
	// A caller must not be able to mutate the package's own ladder through
	// the slice it is handed.
	l[0] = time.Hour
	if again := DefaultLadder(); again[0] != LadderBase {
		t.Fatal("DefaultLadder returns a slice that aliases package state: a caller could rewrite every other caller's ladder")
	}
}
