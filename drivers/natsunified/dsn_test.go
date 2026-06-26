package natsunified_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/natsunified"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestParseDSNSelectsTheProfileFromTheScheme is the facade's reason to exist,
// checked in both directions: one entry point, two profiles, and the scheme is
// what decides.
func TestParseDSNSelectsTheProfileFromTheScheme(t *testing.T) {
	core, err := natsunified.ParseDSN("nats://user:secret@example:4222/acme?noecho=true&flushtimeout=3s")
	if err != nil {
		t.Fatalf("ParseDSN of a nats:// DSN: %v", err)
	}
	if core.Profile != natsunified.ProfileCore {
		t.Errorf("nats:// selected %v, want ProfileCore", core.Profile)
	}
	if got := core.Core.SubjectPrefix; got != "acme" {
		t.Errorf("the Core configuration's SubjectPrefix is %q, want acme", got)
	}
	if !core.Core.NoEcho {
		t.Error("noecho=true did not reach the Core configuration")
	}
	if got := core.JetStream.Stream; got != "" {
		t.Errorf("a nats:// DSN filled the JetStream configuration in (Stream=%q); the unselected half must stay zero", got)
	}

	js, err := natsunified.ParseDSN("natsjs://example:4222/acme?stream=ORDERS&doubleack=true&maxdeliver=5")
	if err != nil {
		t.Fatalf("ParseDSN of a natsjs:// DSN: %v", err)
	}
	if js.Profile != natsunified.ProfileJetStream {
		t.Errorf("natsjs:// selected %v, want ProfileJetStream", js.Profile)
	}
	if got := js.JetStream.Stream; got != "ORDERS" {
		t.Errorf("stream=ORDERS did not reach the JetStream configuration (Stream=%q)", got)
	}
	if !js.JetStream.DoubleAck || js.JetStream.MaxDeliver != 5 {
		t.Errorf("doubleack and maxdeliver did not reach the JetStream configuration: %+v", js.JetStream)
	}
	if len(js.Core.Addrs) != 0 {
		t.Errorf("a natsjs:// DSN filled the Core configuration in (Addrs=%v); the unselected half must stay zero", js.Core.Addrs)
	}
}

// TestParseDSNRefusesTheOtherProfilesParameter holds the
// scheme is the only spelling of the profile, so a parameter that belongs to the
// other profile is a construction error that NAMES the parameter.
//
// The refusal is not the facade's own work and that is the point of checking it
// here: it comes from each profile driver's `dsn.Params.Strict`, which is
// reachable only because the facade hands the whole string to one parser rather
// than parsing the union of both parameter tables itself. A facade with its own
// merged table would have accepted every one of these.
func TestParseDSNRefusesTheOtherProfilesParameter(t *testing.T) {
	for _, tc := range []struct{ dsn, names string }{
		// JetStream parameters under the Core scheme.
		{"nats://host:4222?stream=ORDERS", "stream"},
		{"nats://host:4222?managestream=true", "managestream"},
		{"nats://host:4222?doubleack=true", "doubleack"},
		{"nats://host:4222?deadlettertopic=dlq", "deadlettertopic"},
		// A Core parameter under the JetStream scheme. There are only two,
		// and both are refused.
		{"natsjs://host:4222?noecho=true", "noecho"},
		{"natsjs://host:4222?flushtimeout=2s", "flushtimeout"},
		// The profile has no parameter spelling at all.
		{"nats://host:4222?profile=jetstream", "profile"},
		{"natsjs://host:4222?profile=core", "profile"},
	} {
		cfg, err := natsunified.ParseDSN(tc.dsn)
		if err == nil {
			t.Errorf("ParseDSN(%q) returned %v with no error; a parameter the selected profile does not have must be refused", tc.dsn, cfg.Profile)
			continue
		}
		if !strings.Contains(err.Error(), tc.names) {
			t.Errorf("ParseDSN(%q) error %q does not name the offending parameter %q", tc.dsn, err, tc.names)
		}
	}
}

// TestParseDSNRefusesAnUnknownScheme holds the other half of the dispatch: a
// scheme the facade does not claim is refused with the error a registry gives,
// and a malformed DSN is refused by the shared parser before the scheme is even
// looked at.
func TestParseDSNRefusesAnUnknownScheme(t *testing.T) {
	for _, s := range []string{"kafka://host:9092", "memory://", "natsjetstream://host"} {
		if _, err := natsunified.ParseDSN(s); !errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) error is %v, want ErrDriverNotRegistered", s, err)
		}
		if _, err := natsunified.ProfileForDSN(s); !errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ProfileForDSN(%q) error is %v, want ErrDriverNotRegistered", s, err)
		}
	}
	for _, s := range []string{"", "not a dsn", "://host"} {
		if _, err := natsunified.ParseDSN(s); err == nil {
			t.Errorf("ParseDSN(%q) returned no error", s)
		}
	}
}

// TestProfileForDSNAgreesWithParseDSN holds the two entry points against each
// other: a caller that asks only for the profile must get the same answer as one
// that parses the whole string, or the cheap question is a different question.
func TestProfileForDSNAgreesWithParseDSN(t *testing.T) {
	for _, s := range []string{
		"nats://host:4222",
		"nats://user:pw@a:4222,b:4222/acme?tls=true",
		"natsjs://host:4222?stream=S",
		"natsjs://host/acme?stream=S&storage=memory",
		// A scheme is case-insensitive (RFC 3986), and these two are the
		// shape of the nightly crasher "nAtsjs://000000".
		"NATS://host:4222",
		"nAtsjs://host?stream=S",
	} {
		p, err := natsunified.ProfileForDSN(s)
		if err != nil {
			t.Fatalf("ProfileForDSN(%q): %v", s, err)
		}
		cfg, err := natsunified.ParseDSN(s)
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", s, err)
		}
		if p != cfg.Profile {
			t.Errorf("ProfileForDSN(%q) = %v but ParseDSN selected %v", s, p, cfg.Profile)
		}
		scheme, _, _ := strings.Cut(s, "://")
		want := strings.ToLower(scheme)
		if got := cfg.Profile.Scheme(); got != want {
			t.Errorf("ParseDSN(%q) selected %v, whose scheme %q is not the one in the DSN", s, cfg.Profile, got)
		}
	}
}

// TestRegisterTakesBothSchemesAtomically holds the one thing a two-scheme driver
// can get wrong that a one-scheme driver cannot.
//
// `dsn.Registry.RegisterDriver` refuses the whole driver when either scheme is
// taken; two `Register` calls do not, and leave the first scheme registered
// pointing at a facade the program then believes is fully available. The facade
// prefers the atomic route, and this is what proves it took it: with `natsjs`
// already taken, `nats` must NOT become resolvable.
func TestRegisterTakesBothSchemesAtomically(t *testing.T) {
	var r dsn.Registry
	if err := natsunified.Register(&r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, scheme := range natsunified.Schemes() {
		if _, ok := r.Lookup(scheme); !ok {
			t.Errorf("after Register, the registry does not have %q", scheme)
		}
		if name, ok := r.DriverName(scheme); !ok || name != natsunified.DriverName {
			t.Errorf("the registry names %q's driver %q (ok=%v), want %q", scheme, name, ok, natsunified.DriverName)
		}
	}

	var clash dsn.Registry
	if err := clash.Register(natsunified.SchemeJetStream, func(context.Context, string) (*mq.Client, error) {
		return nil, mq.ErrUnsupported
	}); err != nil {
		t.Fatalf("seeding the clashing registry: %v", err)
	}
	if err := natsunified.Register(&clash); err == nil {
		t.Fatal("Register into a registry that already has natsjs returned no error")
	}
	if _, ok := clash.Lookup(natsunified.SchemeCore); ok {
		t.Error("a failed Register left nats registered; a two-scheme driver must register both or neither")
	}
}

// TestRegisterFallsBackForASingleSchemeRegistrar holds the other half: a
// registrar that can only take one scheme at a time still gets both.
//
// The fallback is not as good — a collision on the second scheme leaves the
// first registered — and it is here because the specification asks a driver to expose
// `Register(dsn.Registrar)` and a facade that refused every registrar but one
// concrete type would not compose.
func TestRegisterFallsBackForASingleSchemeRegistrar(t *testing.T) {
	rec := &recordingRegistrar{}
	if err := natsunified.Register(rec); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// The expectation is the two scheme CONSTANTS and not Schemes(), because
	// the fallback is built from Schemes() — comparing them would be a check
	// that cannot fail, which is how a facade that lost a scheme would still
	// have passed this test.
	if got, want := strings.Join(rec.schemes, ","), natsunified.SchemeCore+","+natsunified.SchemeJetStream; got != want {
		t.Errorf("a single-scheme registrar was given %q, want %q", got, want)
	}
	if err := natsunified.Register(nil); err == nil {
		t.Error("Register(nil) returned no error")
	}
}

// recordingRegistrar is a dsn.Registrar and deliberately NOT a
// driverRegistrar, so that it exercises the fallback path.
type recordingRegistrar struct{ schemes []string }

func (r *recordingRegistrar) Register(scheme string, _ dsn.OpenFunc) error {
	r.schemes = append(r.schemes, scheme)
	return nil
}

// TestDriverIsARegisterableDriver pins the dsn.Driver surface: the name is the
// facade's, the schemes are the two profiles' and the canonical one is first.
func TestDriverIsARegisterableDriver(t *testing.T) {
	d := natsunified.Driver()
	if got := d.Name(); got != natsunified.DriverName {
		t.Errorf("Driver().Name() = %q, want %q", got, natsunified.DriverName)
	}
	got := d.Schemes()
	if len(got) != 2 || got[0] != natsunified.SchemeCore || got[1] != natsunified.SchemeJetStream {
		t.Errorf("Driver().Schemes() = %v, want [%s %s]", got, natsunified.SchemeCore, natsunified.SchemeJetStream)
	}
	if _, err := d.Open(t.Context(), "kafka://host"); !errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Errorf("Driver().Open of a foreign scheme: %v, want ErrDriverNotRegistered", err)
	}
}

// TestOpenDSNOpensBothProfiles runs the DSN entry point against both real
// servers, which is the end-to-end form of the facade's whole claim: one
// function, one string, either broker.
func TestOpenDSNOpensBothProfiles(t *testing.T) {
	stream, root := names(t)
	coreDSN := "nats://" + strings.TrimPrefix(coreURL(t), "nats://") + "/" + root
	jsDSN := "natsjs://" + strings.TrimPrefix(jsURL(t), "nats://") + "/" + root +
		"?stream=" + stream + "&managestream=true&streamsubjects=" + root + ".%3E"

	for _, tc := range []struct {
		dsn     string
		profile natsunified.Profile
		driver  string
	}{
		{coreDSN, natsunified.ProfileCore, "nats"},
		{jsDSN, natsunified.ProfileJetStream, "jetstream"},
	} {
		client, err := natsunified.OpenDSN(t.Context(), tc.dsn)
		if err != nil {
			t.Fatalf("OpenDSN(%q): %v", tc.dsn, err)
		}
		info := client.Info()
		if info.Name != tc.driver {
			t.Errorf("OpenDSN(%q): Info().Name = %q, want %q — the facade must not rename the driver serving the caller", tc.dsn, info.Name, tc.driver)
		}
		if got := natsunified.ProfileOf(info); got != tc.profile {
			t.Errorf("OpenDSN(%q): ProfileOf(Info()) = %v, want %v", tc.dsn, got, tc.profile)
		}
		if err := client.Close(); err != nil {
			t.Errorf("closing the %v client: %v", tc.profile, err)
		}
	}
}
