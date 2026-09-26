package dsn_test

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

// TestParse pins the grammar the specification writes DSNs in: a scheme, an optional
// authority with an optional credential and one or more endpoints, an optional
// path and ordered query parameters.
func TestParse(t *testing.T) {
	t.Run("a full DSN", func(t *testing.T) {
		cfg, err := dsn.Parse("kafka://alice:s3cr3t@b1.example:9092,b2.example:9093/orders?acks=all&tls=true")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Scheme != "kafka" {
			t.Errorf("Scheme is %q", cfg.Scheme)
		}
		if cfg.Username != "alice" {
			t.Errorf("Username is %q", cfg.Username)
		}
		if !cfg.HasPassword || cfg.Password.Reveal() != "s3cr3t" {
			t.Errorf("Password is %q (has=%v)", cfg.Password.Reveal(), cfg.HasPassword)
		}
		want := []string{"b1.example:9092", "b2.example:9093"}
		if got := cfg.Addrs(); !reflect.DeepEqual(got, want) {
			t.Errorf("Addrs is %q, want %q", got, want)
		}
		if cfg.Addr() != "b1.example:9092" {
			t.Errorf("Addr is %q", cfg.Addr())
		}
		if cfg.Path != "orders" {
			t.Errorf("Path is %q", cfg.Path)
		}
		if cfg.Params.Len() != 2 {
			t.Errorf("Params.Len is %d", cfg.Params.Len())
		}
	})
	t.Run("percent escapes and IPv6", func(t *testing.T) {
		cfg, err := dsn.Parse("amqp://us%20er:p%40ss@[2001:db8::1]:5672/a%2Fb?q=%20x")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Username != "us er" {
			t.Errorf("Username is %q", cfg.Username)
		}
		if cfg.Password.Reveal() != "p@ss" {
			t.Errorf("Password is %q", cfg.Password.Reveal())
		}
		if len(cfg.Hosts) != 1 || cfg.Hosts[0].Host != "2001:db8::1" || cfg.Hosts[0].Port != "5672" {
			t.Errorf("Hosts is %+v", cfg.Hosts)
		}
		if cfg.Addr() != "[2001:db8::1]:5672" {
			t.Errorf("Addr is %q", cfg.Addr())
		}
		if cfg.Path != "a/b" {
			t.Errorf("Path is %q", cfg.Path)
		}
		if v, _ := cfg.Params.Lookup("q"); v != " x" {
			t.Errorf("q is %q", v)
		}
	})
	t.Run("an IPv6 literal with no port", func(t *testing.T) {
		// The port is optional in brackets exactly as it is outside them
		// ; "[::1]:" still names an empty one and is
		// refused below.
		cfg, err := dsn.Parse("nats://[::1],[2001:db8::1]:4222")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		want := []dsn.Host{{Host: "::1"}, {Host: "2001:db8::1", Port: "4222"}}
		if !reflect.DeepEqual(cfg.Hosts, want) {
			t.Errorf("Hosts is %+v, want %+v", cfg.Hosts, want)
		}
		if got := cfg.Addrs(); !reflect.DeepEqual(got, []string{"[::1]", "[2001:db8::1]:4222"}) {
			t.Errorf("Addrs is %q", got)
		}
	})
	t.Run("no authority", func(t *testing.T) {
		cfg, err := dsn.Parse("memory://?autocreate=true")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Scheme != "memory" || len(cfg.Hosts) != 0 || cfg.Path != "" {
			t.Errorf("Config is %+v", cfg)
		}
		if cfg.Addr() != "" {
			t.Errorf("Addr is %q, want empty", cfg.Addr())
		}
	})
	t.Run("an empty password is not no password", func(t *testing.T) {
		with, err := dsn.Parse("nats://u:@h:4222")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		without, err := dsn.Parse("nats://u@h:4222")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if !with.HasPassword || with.Password != "" {
			t.Errorf("u:@ gave has=%v password=%q", with.HasPassword, with.Password.Reveal())
		}
		if without.HasPassword {
			t.Error("u@ was read as carrying a password")
		}
	})
	t.Run("the scheme is lower-cased and the rest is not", func(t *testing.T) {
		cfg, err := dsn.Parse("KAFKA://Host:9092/Topic")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Scheme != "kafka" || cfg.Hosts[0].Host != "Host" || cfg.Path != "Topic" {
			t.Errorf("Config is %+v", cfg)
		}
	})
	t.Run("refusals", func(t *testing.T) {
		for _, s := range []string{
			"",
			"kafka",
			"://h",
			"1kafka://h",
			"ka fka://h",
			"kafka:/h",
			"kafka://h#frag",
			"kafka://h?a=1&a=2",
			"kafka://h?=1",
			"kafka://h?a=%zz",
			"kafka://%zz@h",
			"kafka://h:9092:9093",
			"kafka://u:p:q@h",
			"kafka://[2001:db8::1",
			"kafka://[2001:db8::1]:",
			"kafka://[2001:db8::1]x",
			"kafka://[]",
			"kafka://,h",
			"kafka://h,",
			strings.Repeat("k", 9<<10) + "://h",
		} {
			if _, err := dsn.Parse(s); err == nil {
				t.Errorf("Parse(%q) was accepted", s)
			} else if mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Errorf("Parse(%q) failed with code %v, want CodeInvalidConfig", s, mq.CodeOf(err))
			}
		}
	})
}

// TestScheme pins the cheap half a registry uses: the scheme alone, without
// parsing the rest, and the same refusals.
func TestScheme(t *testing.T) {
	s, err := dsn.Scheme("kafka://b1:9092?x=1")
	if err != nil || s != "kafka" {
		t.Fatalf("Scheme: %q, %v", s, err)
	}
	if _, err := dsn.Scheme("nonsense"); err == nil {
		t.Error("Scheme accepted a string with no scheme")
	}
}

// TestConfigStringRedacts is the rule at the one place a DSN is printed: the
// rendering round-trips everything except the credential.
func TestConfigStringRedacts(t *testing.T) {
	cfg, err := dsn.Parse("kafka://alice:s3cr3t@b1:9092,b2:9093/orders?acks=all&tls=true")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := cfg.String()
	if strings.Contains(got, "s3cr3t") {
		t.Fatalf("String leaked the password: %s", got)
	}
	for _, want := range []string{"kafka://", "alice", dsn.Redacted, "b1:9092", "b2:9093", "/orders", "acks=all", "tls=true"} {
		if !strings.Contains(got, want) {
			t.Errorf("String is %q, want it to contain %q", got, want)
		}
	}
	// And the whole struct, however it is printed.
	for _, rendered := range []string{cfg.Password.String(), cfg.Password.GoString()} {
		if strings.Contains(rendered, "s3cr3t") {
			t.Errorf("a Secret rendered as %q", rendered)
		}
	}
	if b, err := cfg.Password.MarshalText(); err != nil || string(b) != dsn.Redacted {
		t.Errorf("MarshalText gave %q, %v", b, err)
	}
	if dsn.Secret("").String() != "" {
		t.Error("an empty Secret rendered as a redaction")
	}
	if got := dsn.Scrub("dial b1:9092: bad password s3cr3t", cfg.Password); strings.Contains(got, "s3cr3t") {
		t.Errorf("Scrub left the secret: %q", got)
	}
	if got := dsn.Scrub("anything", dsn.Secret("")); got != "anything" {
		t.Errorf("Scrub with an empty secret rewrote the message: %q", got)
	}
}

// TestParamsStrict is ADR-010 a parameter nothing read is a
// construction error naming it, not a warning.
func TestParamsStrict(t *testing.T) {
	cfg, err := dsn.Parse("kafka://h?acks=all&capacty=100000&tls=true")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	var acks string
	var tls bool
	if err := cfg.Params.String("acks", &acks); err != nil {
		t.Fatalf("String: %v", err)
	}
	if err := cfg.Params.Bool("tls", &tls); err != nil {
		t.Fatalf("Bool: %v", err)
	}
	if acks != "all" || !tls {
		t.Fatalf("acks=%q tls=%v", acks, tls)
	}
	if got := cfg.Params.Unread(); !reflect.DeepEqual(got, []string{"capacty"}) {
		t.Fatalf("Unread is %q, want [capacty]", got)
	}
	err = cfg.Params.Strict()
	if err == nil {
		t.Fatal("Strict accepted a DSN with an unread parameter")
	}
	if !strings.Contains(err.Error(), "capacty") {
		t.Errorf("Strict did not name the parameter: %v", err)
	}
	if mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("Strict failed with code %v", mq.CodeOf(err))
	}
	cfg.Params.MarkRead("capacty")
	if err := cfg.Params.Strict(); err != nil {
		t.Errorf("Strict after MarkRead: %v", err)
	}
}

// TestParamsTyped pins each accessor: absent leaves the destination alone, a
// bad value is refused and names the parameter, and reading marks it read.
func TestParamsTyped(t *testing.T) {
	cfg, err := dsn.Parse("x://h?n=7&big=9000000000&d=1m30s&b=false&s=hello&e=strict")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	n, big, d, b, s := 1, int64(1), time.Second, true, "keep"
	e := -1
	if err := cfg.Params.Int("n", &n); err != nil || n != 7 {
		t.Errorf("Int: %d, %v", n, err)
	}
	if err := cfg.Params.Int64("big", &big); err != nil || big != 9000000000 {
		t.Errorf("Int64: %d, %v", big, err)
	}
	if err := cfg.Params.Duration("d", &d); err != nil || d != 90*time.Second {
		t.Errorf("Duration: %v, %v", d, err)
	}
	if err := cfg.Params.Bool("b", &b); err != nil || b {
		t.Errorf("Bool: %v, %v", b, err)
	}
	if err := cfg.Params.String("s", &s); err != nil || s != "hello" {
		t.Errorf("String: %q, %v", s, err)
	}
	if err := cfg.Params.Enum("e", []string{"relaxed", "strict"}, func(i int) { e = i }); err != nil || e != 1 {
		t.Errorf("Enum: %d, %v", e, err)
	}
	if err := cfg.Params.Strict(); err != nil {
		t.Errorf("Strict: %v", err)
	}
	// Absent parameters leave the destination alone and are not an error.
	keep := 42
	if err := cfg.Params.Int("absent", &keep); err != nil || keep != 42 {
		t.Errorf("an absent parameter changed the destination: %d, %v", keep, err)
	}
	if cfg.Params.Has("absent") {
		t.Error("Has reported an absent parameter")
	}
	// Has does not consume: a parameter only Has looked at is still unread.
	bad, err := dsn.Parse("x://h?n=nope&b=1&d=nope&e=nope&big=nope")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	var in int
	var i64 int64
	var du time.Duration
	var bo bool
	for name, f := range map[string]func() error{
		"n":   func() error { return bad.Params.Int("n", &in) },
		"big": func() error { return bad.Params.Int64("big", &i64) },
		"d":   func() error { return bad.Params.Duration("d", &du) },
		"b":   func() error { return bad.Params.Bool("b", &bo) },
		"e":   func() error { return bad.Params.Enum("e", []string{"a", "b"}, func(int) {}) },
	} {
		err := f()
		if err == nil {
			t.Errorf("%s=nonsense was accepted", name)
			continue
		}
		if !strings.Contains(err.Error(), name) {
			t.Errorf("%s: the error does not name the parameter: %v", name, err)
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("%s: code %v", name, mq.CodeOf(err))
		}
	}
	// Only the two spellings a DSN writes booleans in are accepted: "1" and
	// "t" are strconv's notion, not a connection string's.
	if !bad.Params.Has("b") {
		t.Error("Has lost a parameter")
	}
	// All iterates in the order written, duplicates already refused.
	var order []string
	for k := range cfg.Params.All() {
		order = append(order, k)
	}
	if !reflect.DeepEqual(order, []string{"n", "big", "d", "b", "s", "e"}) {
		t.Errorf("All iterated %q", order)
	}
}

// TestParseIsPure is the rule for every parser here: the same input gives
// the same answer, and a copy of a Config shares the read marks.
func TestParseIsPure(t *testing.T) {
	const s = "kafka://u:p@h:9092/t?a=1&b=2"
	first, err := dsn.Parse(s)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	second, err := dsn.Parse(s)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if first.String() != second.String() {
		t.Errorf("Parse is not a function: %q then %q", first.String(), second.String())
	}
	cp := first
	var v string
	if err := cp.Params.String("a", &v); err != nil {
		t.Fatalf("String: %v", err)
	}
	if got := first.Params.Unread(); !reflect.DeepEqual(got, []string{"b"}) {
		t.Errorf("a copy's read marks did not reach the original: Unread is %q", got)
	}
}

// TestStrictNamesAreRenderable holds a property every failure in this package
// has to have and one of them did not: an error is valid UTF-8 and printable,
// whatever bytes the DSN carried.
func TestStrictNamesAreRenderable(t *testing.T) {
	for _, in := range []string{
		"x://h?%93",
		"x://h?a%00b=1",
		"x://h?%ff=%fe",
		"x://h?ok=1&%93=2",
	} {
		cfg, err := dsn.Parse(in)
		if err != nil {
			continue
		}
		serr := cfg.Params.Strict()
		if serr == nil {
			t.Errorf("Strict accepted %q with nothing read", in)
			continue
		}
		if !utf8.ValidString(serr.Error()) {
			t.Errorf("the refusal of %q is not valid UTF-8: %q", in, serr.Error())
		}
		for _, r := range serr.Error() {
			if r < 0x20 || r == 0x7f {
				t.Errorf("the refusal of %q carries a control character: %q", in, serr.Error())
				break
			}
		}
	}
	// A plain name is still interpolated plainly: a typo reads better as
	// `stream` than as `"stream"`.
	cfg, err := dsn.Parse("x://h?strem=1")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	serr := cfg.Params.Strict()
	if serr == nil || !strings.Contains(serr.Error(), "does not have: strem") {
		t.Errorf("the refusal does not name the typo plainly: %v", serr)
	}
}
