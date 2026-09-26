package memory_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// TestConfigImplementsBothRenderings holds memory.Config to the API contract
// the specification's "Config implements safe String/LogValue".
func TestConfigImplementsBothRenderings(t *testing.T) {
	var cfg any = memory.Config{AutoCreate: true}
	if _, ok := cfg.(fmt.Stringer); !ok {
		t.Errorf("memory.Config does not implement fmt.Stringer; the specification requires a safe String")
	}
	if _, ok := cfg.(slog.LogValuer); !ok {
		t.Errorf("memory.Config does not implement slog.LogValuer; the specification requires a safe LogValue")
	}
}

// everyDSNParameter is a Config with every field the DSN grammar carries set
// to a non-default value, and nothing else.
//
// "And nothing else" is the point: the round trip below is only a round trip
// for the settings a DSN can express, and a field added to Config that the
// DSN cannot carry belongs in Config.String's fragment rather than here.
func everyDSNParameter() memory.Config {
	return memory.Config{
		AutoCreate:       true,
		MaxTotalMessages: 4096,
		MaxTotalBytes:    1 << 20,
		ZeroOnRelease:    true,
		DefaultTopology: memory.TopologyConfig{
			Kind:             memory.TopologyStream,
			CapacityMessages: 512,
			CapacityBytes:    1 << 18,
			Partitions:       4,
			Ordering:         memory.OrderingStrict,
			Overflow:         memory.OverflowDropOldest,
			AckDeadline:      7 * time.Second,
			MaxAttempts:      3,
			DeadLetterTopic:  "dead",
			MaxMessageSize:   1 << 14,
			DedupWindow:      2 * time.Minute,
			Priority:         true,
			Wildcards:        true,
			Retention: memory.Retention{
				MaxMessages: 100,
				MaxBytes:    1 << 16,
				MaxAge:      time.Hour,
			},
		},
	}
}

// TestDSNRoundTripsEveryParameter is what keeps the rendering and the parser
// from drifting apart.
//
// A rendering written beside a parser, each knowing its own set of parameter
// names, is the defect class this repository keeps finding: the 259-byte
// header key accepted at publish and undecodable afterwards, the specification's
// MaxHeaderKey of 259 against the decoder's 256. Here it would show up as a
// setting that survives being written down and comes back missing.
func TestDSNRoundTripsEveryParameter(t *testing.T) {
	want := everyDSNParameter()
	rendered := want.DSN()
	got, err := memory.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", rendered, err)
	}
	// Config holds mq.Hooks, whose fields are functions, so the struct is not
	// comparable and the round trip is checked field by field over the parts a
	// DSN carries. Rendering the PARSED configuration again is the other half:
	// it catches a parameter this rendering writes and the parser reads into a
	// different field.
	if got.DefaultTopology != want.DefaultTopology {
		t.Errorf("topology round trip:\n rendered %s\n  parsed  %+v\n  wanted  %+v",
			rendered, got.DefaultTopology, want.DefaultTopology)
	}
	if got.AutoCreate != want.AutoCreate || got.MaxTotalMessages != want.MaxTotalMessages ||
		got.MaxTotalBytes != want.MaxTotalBytes || got.ZeroOnRelease != want.ZeroOnRelease ||
		got.Engine != want.Engine {
		t.Errorf("broker round trip:\n rendered %s\n  parsed  autocreate=%v total=%d/%d zero=%v engine=%+v",
			rendered, got.AutoCreate, got.MaxTotalMessages, got.MaxTotalBytes, got.ZeroOnRelease, got.Engine)
	}
	if again := got.DSN(); again != rendered {
		t.Errorf("rendering is not idempotent:\n first  %s\n second %s", rendered, again)
	}
}

// TestStringNamesWhatNoDSNCarries pins the other half: a setting a DSN cannot
// express is still rendered, and is rendered somewhere that cannot be pasted
// back as a DSN.
func TestStringNamesWhatNoDSNCarries(t *testing.T) {
	cfg := memory.Config{
		SchedulerResolution: 5 * time.Millisecond,
		DebugOwnership:      true,
		Hooks:               mq.Hooks{OnReceive: func(context.Context, mq.ReceiveEvent) {}, OnSettle: func(context.Context, mq.SettleEvent) {}},
		Engine:              memory.EngineConfig{Queue: memory.QueueMutexRing},
	}
	s := cfg.String()
	for _, want := range []string{"schedulerresolution=5ms", "debugownership=true", "hooks=2", "engine.queue="} {
		if !strings.Contains(s, want) {
			t.Errorf("Config.String() = %q, missing %q", s, want)
		}
	}
	// The fragment is what says "not a connection string". ParseDSN refuses
	// a fragment, so a reader who pastes this back is told rather than given
	// a Config missing half its settings.
	if !strings.Contains(s, "#") {
		t.Errorf("Config.String() = %q; the non-DSN settings must be behind a '#'", s)
	}
	if _, err := memory.ParseDSN(s); err == nil {
		t.Errorf("ParseDSN(%q) accepted a rendering that is not a DSN", s)
	}
}

// TestZeroConfigRendersAsTheBareScheme keeps the common case short: the zero
// Config is the documented usable one, and a rendering that spelled out
// twenty defaults for it would not be read.
func TestZeroConfigRendersAsTheBareScheme(t *testing.T) {
	if got, want := (memory.Config{}).String(), memory.Scheme+"://"; got != want {
		t.Errorf("(memory.Config{}).String() = %q, want %q", got, want)
	}
}
