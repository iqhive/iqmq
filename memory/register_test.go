package memory_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/memory"
)

// fakeRegistry is a dsn.Registrar of the size this test needs: Register takes
// the real interface now that the specification has built it, and this
// records what a driver claimed without building a registry.
type fakeRegistry struct {
	open map[string]func(ctx context.Context, dsn string) (*mq.Client, error)
	err  error
}

// Register records a scheme's opener, or fails when the registry is set to.
func (r *fakeRegistry) Register(scheme string, open dsn.OpenFunc) error {
	if r.err != nil {
		return r.err
	}
	if r.open == nil {
		r.open = map[string]func(ctx context.Context, dsn string) (*mq.Client, error){}
	}
	r.open[scheme] = open
	return nil
}

// TestRegisterTakesBothSchemes pins ADR-012's registration shape: a driver
// exposes Register and nothing else, and this one answers to both spellings
// The specification names.
func TestRegisterTakesBothSchemes(t *testing.T) {
	ctx := context.Background()
	var reg dsn.Registrar = &fakeRegistry{}
	if err := memory.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, scheme := range []string{memory.Scheme, memory.SchemeAlias} {
		open, ok := reg.(*fakeRegistry).open[scheme]
		if !ok {
			t.Fatalf("Register did not take the %q scheme", scheme)
		}
		client, err := open(ctx, scheme+"://?autocreate=true")
		if err != nil {
			t.Fatalf("opening %q: %v", scheme, err)
		}
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("Publish through a DSN-opened client: %v", err)
		}
		if err := client.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if err := memory.Register(nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Register(nil): %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestParseDSN pins the parameters the specification names and the rule ADR-010 row
// The specification fixes: an unknown parameter is an error rather than a warning, because
// a DSN that silently ignored "capacity=100" would give a caller a broker
// they did not ask for.
func TestParseDSN(t *testing.T) {
	t.Run("the's example", func(t *testing.T) {
		cfg, err := memory.ParseDSN("memory://?partitions=8&capacity=100000&overflow=block&wildcards=true&autocreate=true")
		if err != nil {
			t.Fatalf("ParseDSN: %v", err)
		}
		if !cfg.AutoCreate {
			t.Error("autocreate=true did not set AutoCreate")
		}
		if cfg.DefaultTopology.Partitions != 8 {
			t.Errorf("Partitions is %d, want 8", cfg.DefaultTopology.Partitions)
		}
		if cfg.DefaultTopology.CapacityMessages != 100000 {
			t.Errorf("CapacityMessages is %d, want 100000", cfg.DefaultTopology.CapacityMessages)
		}
		if cfg.DefaultTopology.Overflow != memory.OverflowBlock {
			t.Errorf("Overflow is %v, want block", cfg.DefaultTopology.Overflow)
		}
		if !cfg.DefaultTopology.Wildcards {
			t.Error("wildcards=true did not set Wildcards")
		}
	})
	t.Run("every parameter", func(t *testing.T) {
		cfg, err := memory.ParseDSN("mem://?kind=stream&partitions=auto&capacity=unbounded&capacitybytes=4096" +
			"&fanout=independent&ordering=strict&ackdeadline=1m30s&maxattempts=5&deadletter=dlq" +
			"&maxmessagesize=1024&dedupwindow=10s&priority=true&retentionmessages=64&retentionbytes=8192" +
			"&retentionage=1h&maxtotalmessages=100&maxtotalbytes=200&zeroonrelease=true&engine=safe")
		if err != nil {
			t.Fatalf("ParseDSN: %v", err)
		}
		if cfg.DefaultTopology.Kind != memory.TopologyStream {
			t.Errorf("Kind is %v", cfg.DefaultTopology.Kind)
		}
		if cfg.DefaultTopology.Partitions != -1 {
			t.Errorf("partitions=auto gave %d, want the negative opt-out", cfg.DefaultTopology.Partitions)
		}
		if cfg.DefaultTopology.CapacityMessages != -1 {
			t.Errorf("capacity=unbounded gave %d, want the negative opt-out", cfg.DefaultTopology.CapacityMessages)
		}
		if cfg.DefaultTopology.AckDeadline != 90*time.Second {
			t.Errorf("AckDeadline is %v", cfg.DefaultTopology.AckDeadline)
		}
		if cfg.DefaultTopology.MaxAttempts != 5 || cfg.DefaultTopology.DeadLetterTopic != "dlq" {
			t.Errorf("attempts %d, dead letter %q", cfg.DefaultTopology.MaxAttempts, cfg.DefaultTopology.DeadLetterTopic)
		}
		if !cfg.Engine.Safe || !cfg.ZeroOnRelease {
			t.Errorf("engine=safe or zeroonrelease=true was dropped: %+v", cfg.Engine)
		}
		if cfg.DefaultTopology.Retention.MaxMessages != 64 || cfg.DefaultTopology.Retention.MaxBytes != 8192 ||
			cfg.DefaultTopology.Retention.MaxAge != time.Hour {
			t.Errorf("retention %+v", cfg.DefaultTopology.Retention)
		}
		// ackdeadline=never is the other spelling of the asymmetry.
		never, err := memory.ParseDSN("memory://?ackdeadline=never")
		if err != nil {
			t.Fatalf("ackdeadline=never: %v", err)
		}
		if never.DefaultTopology.AckDeadline != -1 {
			t.Errorf("ackdeadline=never gave %v, want the negative opt-out", never.DefaultTopology.AckDeadline)
		}
	})
	t.Run("refusals", func(t *testing.T) {
		for _, dsn := range []string{
			"memory://?unknown=1",
			"memory://?capacity=0",
			"memory://?capacity=-1",
			"memory://?capacity=abc",
			"memory://?autocreate=1",
			"memory://?autocreate=yes",
			"memory://?overflow=default",
			"memory://?overflow=nonsense",
			"memory://?ordering=default",
			"memory://?fanout=auto",
			"memory://?kind=default",
			"memory://?ackdeadline=0s",
			"memory://?ackdeadline=-1s",
			"memory://?engine=fast",
			"memory://?deadletter=",
			"memory://?partitions=1&partitions=2",
			"memory://host/path",
			"memory://user:pass@host",
			"memory:///topic",
			"memory://?x=1#frag",
			"memory:opaque",
			"kafka://b1:9092",
			"",
			"://",
			// A stream retention on a topic topology is a configuration
			// error, and the DSN is where it has to be caught: the
			// alternative is a broker that quietly retains nothing.
			"memory://?kind=topic&retentionmessages=8",
		} {
			if _, err := memory.ParseDSN(dsn); err == nil {
				t.Errorf("ParseDSN(%q) was accepted", dsn)
			}
		}
		long := "memory://?autocreate=true&pad=" + string(make([]byte, 9<<10))
		if _, err := memory.ParseDSN(long); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("a 9 KiB DSN: %v (code %v)", err, mq.CodeOf(err))
		}
	})
	t.Run("the empty DSN is the default broker", func(t *testing.T) {
		cfg, err := memory.ParseDSN("memory://")
		if err != nil {
			t.Fatalf("ParseDSN: %v", err)
		}
		// Config carries Hooks, which holds funcs and is therefore not
		// comparable; a DSN never sets them, so the rest of the struct is
		// what the comparison is about.
		cfg.Hooks, cfg.Clock = mq.Hooks{}, nil
		if !reflect.DeepEqual(cfg, memory.Config{}) {
			t.Fatalf("a bare DSN gave %+v, want the zero configuration", cfg)
		}
		// And with AutoCreate off it behaves like every network driver.
		client, err := memory.OpenDSN(context.Background(), "memory://")
		if err != nil {
			t.Fatalf("OpenDSN: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Publish(context.Background(), "orders", &mq.Message{Body: []byte("x")}); mq.CodeOf(err) != mq.CodeNotFound {
			t.Fatalf("Publish with autocreate off: %v (code %v)", err, mq.CodeOf(err))
		}
	})
}

// FuzzParseDSN is the parser's own target: the rule is that every parser is
// pure, has explicit limits, never panics and never allocates unboundedly,
// and the bytes of a DSN may come from an environment variable a stranger
// set.
//
// The invariant is the strong one a pure parser allows: a configuration the
// parser accepted must be one NewBroker can build, so a DSN can never be a
// way to reach a configuration the validator would have refused.
func FuzzParseDSN(f *testing.F) {
	f.Add("memory://")
	f.Add("mem://?autocreate=true")
	f.Add("memory://?partitions=8&capacity=100000&overflow=block&wildcards=true&autocreate=true")
	f.Add("memory://?kind=stream&retentionmessages=4&ackdeadline=never&engine=safe")
	f.Add("memory://?capacity=unbounded&partitions=auto")
	f.Add("memory://?unknown=1")
	f.Add("memory://host/path?x=1#frag")
	f.Add("")
	f.Fuzz(func(t *testing.T, dsn string) {
		cfg, err := memory.ParseDSN(dsn)
		if err != nil {
			return
		}
		b, berr := memory.NewBroker(cfg)
		if berr != nil {
			t.Fatalf("ParseDSN accepted %q and NewBroker refused the configuration it produced: %v", dsn, berr)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// Parsing is pure: the same input gives the same configuration.
		again, aerr := memory.ParseDSN(dsn)
		if aerr != nil {
			t.Fatalf("ParseDSN(%q) succeeded and then failed: %v", dsn, aerr)
		}
		// Hooks holds funcs and is not comparable; a DSN never sets them.
		if !reflect.DeepEqual(again, cfg) {
			t.Fatalf("ParseDSN(%q) is not a function: %+v then %+v", dsn, cfg, again)
		}
	})
}
