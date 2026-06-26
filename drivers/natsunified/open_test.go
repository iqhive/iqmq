package natsunified_test

import (
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/jetstream"
	"bitbucket.org/iqhive/iqmq/drivers/natsunified"
)

// TestBackendAccessorsAnswerBothWays holds [natsunified.CoreBackend] and
// [natsunified.JetStreamBackend], which are the facade's one unavoidable cost
// made reachable: OpenBackend returns an interface, so the concrete escape
// hatches are behind an assertion.
//
// Both directions are checked. An accessor that answered true for everything
// would compile, pass a happy-path test and hand a caller a nil pointer.
func TestBackendAccessorsAnswerBothWays(t *testing.T) {
	core, err := natsunified.OpenBackend(t.Context(), coreConfig(t))
	if err != nil {
		t.Fatalf("OpenBackend (core): %v", err)
	}
	defer func() { _ = core.Close() }()
	js, err := natsunified.OpenBackend(t.Context(), jsConfig(t))
	if err != nil {
		t.Fatalf("OpenBackend (jetstream): %v", err)
	}
	defer func() { _ = js.Close() }()

	cb, ok := natsunified.CoreBackend(core)
	if !ok || cb == nil {
		t.Fatalf("CoreBackend on a Core backend: ok=%v, value=%v", ok, cb)
	}
	if cb.Conn() == nil {
		t.Error("the Core backend's Conn() is nil; the escape hatch is the point of the accessor")
	}
	if _, ok := natsunified.JetStreamBackend(core); ok {
		t.Error("JetStreamBackend accepted a Core backend")
	}

	jb, ok := natsunified.JetStreamBackend(js)
	if !ok || jb == nil {
		t.Fatalf("JetStreamBackend on a JetStream backend: ok=%v, value=%v", ok, jb)
	}
	if jb.Stream() == nil || jb.JetStream() == nil {
		t.Error("the JetStream backend's Stream() or JetStream() is nil")
	}
	if _, ok := natsunified.CoreBackend(js); ok {
		t.Error("CoreBackend accepted a JetStream backend")
	}
	if _, ok := natsunified.CoreBackend(nil); ok {
		t.Error("CoreBackend accepted nil")
	}
	if _, ok := natsunified.JetStreamBackend(nil); ok {
		t.Error("JetStreamBackend accepted nil")
	}
}

// TestProfileOfReadsTheDriversOwnAnswer holds [natsunified.ProfileOf] against
// both profiles and against a driver that is neither.
//
// It reads `Info().Name`, which is the profile driver's own answer, so this
// is checking that the facade did not rename the driver underneath it — the
// thing the specification is about. A facade that wrapped and reported
// `Name: "natsunified"` would make ProfileOf answer ProfileUnset for every
// client it ever opened.
func TestProfileOfReadsTheDriversOwnAnswer(t *testing.T) {
	for _, cfg := range []natsunified.Config{coreConfig(t), jsConfig(t)} {
		client := openFacade(t, cfg)
		info := client.Info()
		if got := natsunified.ProfileOf(info); got != cfg.Profile {
			t.Errorf("ProfileOf(Info{Name:%q, Profile:%q}) = %v, want %v", info.Name, info.Profile, got, cfg.Profile)
		}
		if info.Name == natsunified.DriverName {
			t.Errorf("Info().Name is %q; the facade must report the driver that is really serving the caller", info.Name)
		}
		if info.ClientName == "" || info.BrokerVersion == "" {
			t.Errorf("Info() lost the profile driver's client or broker version: %+v", info)
		}
	}
	if got := natsunified.ProfileOf(mq.DriverInfo{Name: "kafka"}); got != natsunified.ProfileUnset {
		t.Errorf("ProfileOf a foreign driver = %v, want ProfileUnset", got)
	}
	if got := natsunified.ProfileOf(mq.DriverInfo{}); got != natsunified.ProfileUnset {
		t.Errorf("ProfileOf the zero DriverInfo = %v, want ProfileUnset", got)
	}
}

// TestOpenWithClientServesBothProfilesOnOneConnection is the facade's clearest
// technical win, and the reason a single entry point is not merely cosmetic:
// NATS Core and JetStream are the same protocol on the same socket, so one
// *nats.Conn serves both profiles.
//
// A program that holds its own connection — because its authentication needs an
// NKey signing callback, or a custom dialler — therefore does not need two.
// Ownership is `ClientBorrowed` for both, so neither client closes the caller's
// connection, which is the invariant this test would catch: after both clients
// are closed the connection must still be usable.
func TestOpenWithClientServesBothProfilesOnOneConnection(t *testing.T) {
	stream, root := names(t)
	nc, err := natsgo.Connect(jsURL(t))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer nc.Close()

	coreClient, err := natsunified.OpenWithClient(t.Context(), nc, driverkit.ClientBorrowed, natsunified.Config{
		Profile: natsunified.ProfileCore,
		Core:    natsCoreConfig(jsURL(t)),
	})
	if err != nil {
		t.Fatalf("OpenWithClient (core): %v", err)
	}
	jsClient, err := natsunified.OpenWithClient(t.Context(), nc, driverkit.ClientBorrowed, natsunified.Config{
		Profile: natsunified.ProfileJetStream,
		JetStream: jetstream.Config{
			Addrs: []string{jsURL(t)}, Stream: stream,
			SubjectPrefix: root, ManageStream: true, AckWait: 5 * time.Second,
		},
	})
	if err != nil {
		_ = coreClient.Close()
		t.Fatalf("OpenWithClient (jetstream) on the same connection: %v", err)
	}

	if coreClient.Info().Name == jsClient.Info().Name {
		t.Errorf("both clients report driver %q; one connection must still be two profiles", coreClient.Info().Name)
	}
	if err := coreClient.Close(); err != nil {
		t.Errorf("closing the Core client: %v", err)
	}
	if err := jsClient.Close(); err != nil {
		t.Errorf("closing the JetStream client: %v", err)
	}
	if nc.IsClosed() {
		t.Fatal("a borrowed connection was closed by a client that did not own it")
	}
	if err := nc.Flush(); err != nil {
		t.Errorf("the borrowed connection is no longer usable: %v", err)
	}

	if _, err := natsunified.OpenWithClient(t.Context(), nil, driverkit.ClientBorrowed, coreConfig(t)); err == nil {
		t.Error("OpenWithClient(nil) returned no error")
	}
	if _, err := natsunified.BackendWithClient(t.Context(), nil, driverkit.ClientBorrowed, coreConfig(t)); err == nil {
		t.Error("BackendWithClient(nil) returned no error")
	}
}

// TestValidateDelegatesToTheSelectedProfileOnly holds the specification's "validate config
// before starting background work" through the facade: [Config.Validate] adds
// one check of its own — a profile was named — and delegates the rest to the
// selected profile's own pure Validate.
//
// It asserts on Validate directly and NOT through Open, and that distinction is
// the whole reason this test exists in this shape. The first version went
// through Open, which is a check that cannot fail: both profile drivers validate
// again inside their own Open, so a facade whose Validate returned nil for
// everything still produced the same error from the same input. What the facade
// owns is the exported pure method, so that is what is checked — and a caller
// that validates a configuration before a deployment window, which is what a
// pure Validate is for, only ever calls this.
//
// The unselected half is deliberately NOT validated, which is what makes the
// both-filled-in shape — one Config, two deployments, flip the profile — usable.
// The bad JetStream configuration in the Core case is what proves it.
func TestValidateDelegatesToTheSelectedProfileOnly(t *testing.T) {
	bad := coreConfig(t)
	bad.Core.Addrs = []string{"\x00not an address"}
	err := bad.Validate()
	if err == nil {
		t.Error("Config.Validate accepted a Core configuration drivers/nats refuses; the specification asks a driver to refuse before any I/O and the facade must not lose that by delegating nothing")
	} else if code := mq.CodeOf(err); code != mq.CodeInvalidConfig {
		t.Errorf("an invalid Core address carries %v, want CodeInvalidConfig", code)
	}

	badJS := jsConfig(t)
	badJS.JetStream.Stream = "not a stream name"
	if err := badJS.Validate(); err == nil {
		t.Error("Config.Validate accepted a JetStream configuration drivers/jetstream refuses")
	}

	// A Core profile with a JetStream half that would not validate: it is not
	// going to be read, so it must not be refused.
	mixed := coreConfig(t)
	mixed.JetStream = jetstream.Config{Addrs: []string{"\x00also invalid"}, Stream: "nope"}
	if err := mixed.JetStream.Validate(); err == nil {
		t.Fatal("the JetStream half of the mixed configuration is not actually invalid, so this test says nothing")
	}
	if err := mixed.Validate(); err != nil {
		t.Errorf("Validate refused a Core configuration for a fault in the JetStream half it will never read: %v", err)
	}
	client, err := natsunified.Open(t.Context(), mixed)
	if err != nil {
		t.Fatalf("Open refused a Core configuration for a fault in the unselected half: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("closing: %v", err)
	}
	// And the end-to-end path still refuses the invalid selected half, which
	// is what a caller who never calls Validate sees.
	if _, err := natsunified.Open(t.Context(), bad); err == nil {
		t.Error("Open accepted a Core configuration its own Validate refuses")
	}
}

// TestInfoAndNativeAfterClose is the conformance kit's `client/info-and-native`
// scenario, held here because the kit cannot run it against a facade.
//
// That scenario asserts `Info().Name == Harness.Driver`, which is true of every
// driver and false of a facade by construction: the report is keyed
// `natsunified/<profile>` and the client reports `nats` or `jetstream`, because
// The specification forbids renaming the driver that is really serving the
// caller. The scenario is therefore documented as inapplicable in the facade's
// harness and the gap is recorded in the maintenance notes with the kit as its
// owner.
//
// A skip is never a pass, so everything else that scenario checks is checked
// here instead, on both profiles: the name is not empty, the native escape
// hatch is there, and Info and Health both answer after Close. The one
// assertion that is NOT reproduced is the one that cannot hold — and that one is
// inverted, because a facade whose client named the facade would be the defect.
func TestInfoAndNativeAfterClose(t *testing.T) {
	for _, cfg := range []natsunified.Config{coreConfig(t), jsConfig(t)} {
		client, err := natsunified.Open(t.Context(), cfg)
		if err != nil {
			t.Fatalf("%v: Open: %v", cfg.Profile, err)
		}
		info := client.Info()
		if info.Name == "" {
			t.Errorf("%v: DriverInfo.Name is empty: the specification makes an empty field \"the driver did not say\", and a driver always knows its own name", cfg.Profile)
		}
		if info.Name == natsunified.DriverName {
			t.Errorf("%v: DriverInfo.Name is the facade's own name; a caller reading a log line has to be able to tell which driver is serving it", cfg.Profile)
		}
		if client.Native() == nil {
			t.Errorf("%v: Native() is nil; the facade must not hide the profile driver's escape hatch", cfg.Profile)
		}
		if err := client.Close(); err != nil {
			t.Fatalf("%v: Close: %v", cfg.Profile, err)
		}
		if client.Info().Name != info.Name {
			t.Errorf("%v: a closed client stopped naming its driver", cfg.Profile)
		}
		if h := client.Health(); h.State != mq.HealthClosed {
			t.Errorf("%v: a closed client reports health %v, want closed", cfg.Profile, h.State)
		}
	}
}
