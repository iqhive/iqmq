package driverkit_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/backoff"
	"github.com/iqhive/iqmq/wire"
)

// TestCapabilityBuilderRefusesADishonestTable is the rule made structural: a
// conditional support must state its condition and an emulation must say what it
// is, so the builder refuses to produce a table that does not.
func TestCapabilityBuilderRefusesADishonestTable(t *testing.T) {
	caps, err := driverkit.NewCapabilities().
		Native(mq.FeatureAtLeastOnce, mq.ScopeTopic).
		Conditional(mq.FeaturePriority, mq.ScopeTopic, "TopologyConfig.Priority").
		EmulatedVolatile(mq.FeatureDeduplication, mq.ScopeTopic, "in process, inside the window").
		Limits(mq.Limits{MaxMessageBytes: mq.Limit[int64]{Value: 1 << 20, Known: true}}).
		Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !caps.Has(mq.FeatureAtLeastOnce) || caps.Support(mq.FeaturePriority).Note == "" {
		t.Fatalf("the table is %v", caps.Support(mq.FeaturePriority))
	}
	if lim := caps.Limit(mq.LimitMaxMessageBytes); !lim.Known || lim.Value != 1<<20 {
		t.Errorf("the limit is %+v", lim)
	}
	for name, b := range map[string]*driverkit.CapabilityBuilder{
		"a conditional support with no condition": driverkit.NewCapabilities().Conditional(mq.FeaturePriority, mq.ScopeTopic, ""),
		"a volatile emulation with no note":       driverkit.NewCapabilities().EmulatedVolatile(mq.FeatureReplay, mq.ScopeTopic, ""),
		"a durable emulation with no note":        driverkit.NewCapabilities().EmulatedDurable(mq.FeatureDeadLetter, mq.ScopeTopic, ""),
		"a feature outside the inventory":         driverkit.NewCapabilities().Native(mq.Feature(9999), mq.ScopeTopic),
		"one feature advertised twice": driverkit.NewCapabilities().
			Native(mq.FeatureNack, mq.ScopeTopic).Unsupported(mq.FeatureNack),
	} {
		if _, err := b.Build(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// Missing is not an error; it is the record of what nobody considered.
	b := driverkit.NewCapabilities().Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	if got := len(b.Missing()); got != mq.FeatureCount-1 {
		t.Errorf("Missing named %d features, want %d", got, mq.FeatureCount-1)
	}
}

// TestHealthStateMachine pins the five states and the one rule that makes
// the report worth reading: Closed is terminal.
func TestHealthStateMachine(t *testing.T) {
	var seen []mq.HealthState
	h := driverkit.NewHealth(func(s mq.HealthSnapshot) { seen = append(seen, s.State) })
	if h.State() != mq.HealthConfigured {
		t.Fatalf("a new reporter is %v", h.State())
	}
	boom := errors.New("the broker went away")
	h.Connecting()
	h.Ready()
	h.Degraded(boom)
	if snap := h.Snapshot(); snap.State != mq.HealthDegraded || !errors.Is(snap.Err, boom) {
		t.Fatalf("Snapshot is %+v", snap)
	}
	h.Ready()
	if snap := h.Snapshot(); snap.Err != nil {
		t.Errorf("Ready left a stale error behind: %v", snap.Err)
	}
	h.Close()
	h.Ready()
	if h.State() != mq.HealthClosed {
		t.Fatal("a closed reporter went back to ready: a caller can poll a dead client for ever")
	}
	if !h.Closed() {
		t.Error("Closed reported false")
	}
	want := []mq.HealthState{mq.HealthConnecting, mq.HealthReady, mq.HealthDegraded, mq.HealthReady, mq.HealthClosed}
	if len(seen) != len(want) {
		t.Fatalf("the hook saw %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("the hook saw %v, want %v", seen, want)
		}
	}
}

// TestOwnershipClosesOnlyWhatItOwns is the borrowed/owned rule: iqmq closes a
// native client it dialled and never one it was handed.
func TestOwnershipClosesOnlyWhatItOwns(t *testing.T) {
	for _, tc := range []struct {
		own       driverkit.Ownership
		wantClose bool
	}{
		{driverkit.ClientBorrowed, false},
		{driverkit.ClientOwned, true},
	} {
		closed := false
		if err := tc.own.Close(func() error { closed = true; return nil }); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if closed != tc.wantClose {
			t.Errorf("%v closed the native client: %v, want %v", tc.own, closed, tc.wantClose)
		}
	}
	if driverkit.ClientBorrowed != 0 {
		t.Error("the zero Ownership is not ClientBorrowed: iqmq would close a client it was handed by default")
	}
	if err := driverkit.CheckOwnership(driverkit.Ownership(7)); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("an ownership outside the enumeration: %v", err)
	}
}

// TestBaseBackend pins the bookkeeping: constant answers after close, a teardown
// that runs once, and the native client closed only when iqmq owns it.
func TestBaseBackend(t *testing.T) {
	ctx := context.Background()
	caps := mustBuild(t, driverkit.NewCapabilities().Native(mq.FeatureAtLeastOnce, mq.ScopeTopic))
	native := new(int)
	teardowns, closes := 0, 0
	boom := errors.New("teardown failed")
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         mq.DriverInfo{Name: "fake"},
		Capabilities: caps,
		Native:       native,
		Ownership:    driverkit.ClientOwned,
		CloseNative:  func() error { closes++; return nil },
	}, func() error { teardowns++; return boom })
	if err != nil {
		t.Fatalf("NewBase: %v", err)
	}
	if err := base.CheckOpen(ctx); err != nil {
		t.Fatalf("CheckOpen: %v", err)
	}
	if !errors.Is(base.Close(), boom) {
		t.Fatal("Close did not report the teardown's error")
	}
	if !errors.Is(base.Close(), boom) {
		t.Fatal("a second Close reported something different from the first")
	}
	if teardowns != 1 || closes != 1 {
		t.Fatalf("teardown ran %d times and the native close %d, want one each", teardowns, closes)
	}
	// The specification: Info, Capabilities and Native answer after Close.
	if base.Info().Name != "fake" || !base.Capabilities().Has(mq.FeatureAtLeastOnce) || base.Native() != any(native) {
		t.Error("a closed backend stopped answering the questions the specification says it always answers")
	}
	if base.Health().State != mq.HealthClosed {
		t.Errorf("a closed backend is %v", base.Health().State)
	}
	if !errors.Is(base.CheckOpen(ctx), mq.ErrClosed) {
		t.Error("CheckOpen accepted a closed backend")
	}
	if _, err := driverkit.NewBase(driverkit.BaseConfig{Ownership: driverkit.Ownership(9)}, nil); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("an invalid ownership: %v", err)
	}
}

// TestErrorMapper pins the first matching rule decides, the native error
// survives for errors.As, and a post-send transport failure is ambiguous.
func TestErrorMapper(t *testing.T) {
	native := errors.New("TOPIC_AUTHORIZATION_FAILED")
	m := &driverkit.ErrorMapper{
		Driver: "fake",
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: {
				{Match: driverkit.MatchIs(native), Code: mq.CodeForbidden},
				{Match: driverkit.MatchContains("throttl"), Code: mq.CodeThrottled, Retryable: true},
			},
		},
		Fallback: []driverkit.Rule{
			{Match: driverkit.MatchContains("no such topic"), Code: mq.CodeNotFound},
		},
	}
	err := m.Map(mq.OpPublish, "orders", "", native)
	if mq.CodeOf(err) != mq.CodeForbidden {
		t.Fatalf("the first rule did not win: %v (code %v)", err, mq.CodeOf(err))
	}
	if !errors.Is(err, native) {
		t.Fatal("the mapper threw the native error away: errors.As can no longer reach the client's own type")
	}
	var op *mq.OpError
	if !errors.As(err, &op) || op.Driver != "fake" || op.Operation != mq.OpPublish || op.Topic != "orders" {
		t.Fatalf("the *OpError is %+v", op)
	}
	if got := m.Map(mq.OpPublish, "t", "", errors.New("request was throttled")); !mq.IsRetryable(got) {
		t.Errorf("a throttle was not retryable: %v", got)
	}
	if got := m.Map(mq.OpFetch, "t", "s", errors.New("no such topic: t")); mq.CodeOf(got) != mq.CodeNotFound {
		t.Errorf("the fallback did not apply: %v", got)
	}
	if m.Map(mq.OpPublish, "t", "", nil) != nil {
		t.Error("a nil error was mapped to something")
	}
	t.Run("a post-send failure is ambiguous", func(t *testing.T) {
		timeout := &net.OpError{Op: "write", Err: &timeoutError{}}
		before := m.MapNet(mq.OpPublish, "t", "", timeout, false)
		after := m.MapNet(mq.OpPublish, "t", "", timeout, true)
		if mq.IsAmbiguous(before) {
			t.Error("a timeout before the write was reported as ambiguous")
		}
		if !mq.IsAmbiguous(after) {
			t.Fatal("a timeout after the write was reported as a definite failure: a retry can duplicate and the caller cannot tell")
		}
	})
	t.Run("ClassifyNet", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			err       error
			sent      bool
			code      mq.ErrorCode
			retryable bool
			ambiguous bool
		}{
			{"cancelled", context.Canceled, false, mq.CodeCanceled, false, false},
			{"cancelled after the send", context.Canceled, true, mq.CodeCanceled, false, true},
			{"deadline", context.DeadlineExceeded, false, mq.CodeDeadlineExceeded, true, false},
			{"eof", io.EOF, true, mq.CodeUnavailable, true, true},
			{"closed connection", net.ErrClosed, false, mq.CodeUnavailable, true, false},
			{"a portable error passes through", mq.ErrMessageTooLarge, false, mq.CodeMessageTooLarge, false, false},
			{"nil", nil, false, mq.CodeUnknown, false, false},
		} {
			code, retryable, ambiguous := driverkit.ClassifyNet(tc.err, tc.sent)
			if code != tc.code || retryable != tc.retryable || ambiguous != tc.ambiguous {
				t.Errorf("%s: %v/%v/%v, want %v/%v/%v", tc.name, code, retryable, ambiguous, tc.code, tc.retryable, tc.ambiguous)
			}
		}
	})
}

// timeoutError is a net.Error that times out.
type timeoutError struct{}

func (*timeoutError) Error() string { return "i/o timeout" }
func (*timeoutError) Timeout() bool { return true }

func (*timeoutError) Temporary() bool { return true }

// TestReconnector pins the specification's "reconnect with backoff, never busy-spin": the loop
// waits on the injected clock, reports every attempt, and bumps a generation
// when the connection is lost so that a settlement from before it is stale.
func TestReconnector(t *testing.T) {
	ctx := context.Background()
	var events []mq.ReconnectEvent
	h := driverkit.NewHealth(nil)
	attempts := 0
	r, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
		Driver: "fake",
		Dial: func(context.Context) error {
			attempts++
			if attempts < 3 {
				return errors.New("connection refused")
			}
			return nil
		},
		Base:        time.Nanosecond,
		Max:         time.Microsecond,
		Policy:      backoff.Auto,
		Health:      h,
		OnReconnect: func(_ context.Context, e mq.ReconnectEvent) { events = append(events, e) },
	})
	if err != nil {
		t.Fatalf("NewReconnector: %v", err)
	}
	if err := r.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if attempts != 3 || len(events) != 3 {
		t.Fatalf("%d dials, %d events", attempts, len(events))
	}
	if events[0].Err == nil || events[2].Err != nil {
		t.Error("the hook did not report the failures and then the success")
	}
	if h.State() != mq.HealthReady {
		t.Errorf("health is %v after a successful connect", h.State())
	}
	gen := r.Generation()
	if next := r.Lost(errors.New("peer went away")); next == gen {
		t.Fatal("a lost connection did not bump the generation: a stale settlement would land on a live delivery")
	}
	if h.State() != mq.HealthDegraded {
		t.Errorf("health is %v after a loss", h.State())
	}
	t.Run("MaxAttempts bounds the loop", func(t *testing.T) {
		boom := errors.New("nope")
		bounded, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
			Dial: func(context.Context) error { return boom }, MaxAttempts: 2,
			Base: time.Nanosecond, Max: time.Nanosecond,
		})
		if err != nil {
			t.Fatalf("NewReconnector: %v", err)
		}
		if err := bounded.Connect(ctx); !errors.Is(err, boom) {
			t.Fatalf("Connect: %v", err)
		}
		if bounded.Attempts() != 2 {
			t.Errorf("made %d attempts, want 2", bounded.Attempts())
		}
	})
	t.Run("a cancelled context ends the loop", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		never, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
			Dial: func(context.Context) error { return errors.New("nope") },
		})
		if err != nil {
			t.Fatalf("NewReconnector: %v", err)
		}
		if err := never.Connect(cctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Connect: %v", err)
		}
	})
	t.Run("refusals", func(t *testing.T) {
		if _, err := driverkit.NewReconnector(driverkit.ReconnectConfig{}); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("no Dial: %v", err)
		}
		dial := func(context.Context) error { return nil }
		if _, err := driverkit.NewReconnector(driverkit.ReconnectConfig{Dial: dial, MaxAttempts: -1}); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("a negative MaxAttempts: %v", err)
		}
		if _, err := driverkit.NewReconnector(driverkit.ReconnectConfig{Dial: dial, Policy: backoff.Kind(99)}); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("an unknown policy: %v", err)
		}
	})
}

// TestEnvelope pins the four modes: strict never wraps, portable always
// wraps, adaptive wraps what the broker cannot carry, and a round trip through
// the envelope reproduces the message.
func TestEnvelope(t *testing.T) {
	headerless := mustBuild(t, driverkit.NewCapabilities().Native(mq.FeatureAtLeastOnce, mq.ScopeTopic))
	full := mustBuild(t, driverkit.NewCapabilities().
		Native(mq.FeatureNativeHeaders, mq.ScopeTopic).
		Native(mq.FeatureBinaryHeaders, mq.ScopeTopic).
		Native(mq.FeatureDuplicateHeaders, mq.ScopeTopic))

	msg := &mq.Message{Topic: "orders", Key: []byte("k"), Body: []byte("body"), ID: "id-1"}
	msg.Headers.Add("trace", []byte{0x00, 0xff})
	msg.Headers.Add("trace", []byte("second"))

	t.Run("strict never wraps", func(t *testing.T) {
		e, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataNativeStrict, Capabilities: headerless})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		out, err := e.Wrap(msg)
		if err != nil {
			t.Fatalf("Wrap: %v", err)
		}
		if out != msg {
			t.Fatal("strict mode wrapped a message")
		}
	})
	t.Run("portable round trip", func(t *testing.T) {
		e, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope, Capabilities: headerless})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		out, err := e.Wrap(msg)
		if err != nil {
			t.Fatalf("Wrap: %v", err)
		}
		if out.Headers.Len() != 0 || out.Key != nil {
			t.Fatal("an enveloped message kept metadata beside the envelope: half of it would be lost on a header-less broker")
		}
		if _, ok := wire.Detect(out.Body); !ok {
			t.Fatal("the body is not an envelope")
		}
		var back mq.Message
		if err := e.Unwrap(&back, out); err != nil {
			t.Fatalf("Unwrap: %v", err)
		}
		if back.ID != msg.ID || !bytes.Equal(back.Key, msg.Key) || !bytes.Equal(back.Body, msg.Body) {
			t.Fatalf("the round trip gave %+v", back)
		}
		if back.Headers.Len() != 2 {
			t.Fatalf("the round trip kept %d headers, want the duplicate pair", back.Headers.Len())
		}
		if back.Topic != "orders" {
			t.Errorf("the topic is %q", back.Topic)
		}
		// The mode says every message is an envelope, so one that is not is
		// a corrupt record rather than a native publication.
		if err := e.Unwrap(&back, &mq.Message{Body: []byte("not an envelope")}); err == nil {
			t.Error("portable mode accepted a body that is not an envelope")
		}
	})
	t.Run("adaptive wraps only what the broker cannot carry", func(t *testing.T) {
		plain := &mq.Message{Topic: "t", Body: []byte("b")}
		for name, tc := range map[string]struct {
			caps mq.Capabilities
			msg  *mq.Message
			want bool
		}{
			"a header-less broker with headers":         {headerless, msg, true},
			"a header-less broker with a plain message": {headerless, plain, false},
			"a capable broker with binary duplicates":   {full, msg, false},
		} {
			e, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataAdaptive, Capabilities: tc.caps})
			if err != nil {
				t.Fatalf("NewEnvelope: %v", err)
			}
			if got := e.Needed(tc.msg); got != tc.want {
				t.Errorf("%s: Needed is %v, want %v", name, got, tc.want)
			}
			out, err := e.Wrap(tc.msg)
			if err != nil {
				t.Fatalf("%s: Wrap: %v", name, err)
			}
			if wrapped := out != tc.msg; wrapped != tc.want {
				t.Errorf("%s: wrapped=%v, want %v", name, wrapped, tc.want)
			}
			// Adaptive decodes an envelope and passes a native message
			// through, which is the mixed stream the mode produces.
			var back mq.Message
			if err := e.Unwrap(&back, out); err != nil {
				t.Errorf("%s: Unwrap: %v", name, err)
			}
		}
	})
	t.Run("refusals", func(t *testing.T) {
		if _, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataMode(9)}); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("an unknown mode: %v", err)
		}
		if _, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Layout: wire.Kind(99)}); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("an unknown layout: %v", err)
		}
	})
}

func mustBuild(tb testing.TB, b *driverkit.CapabilityBuilder) mq.Capabilities {
	tb.Helper()
	caps, err := b.Build()
	if err != nil {
		tb.Fatalf("Build: %v", err)
	}
	return caps
}
