package natsunified_test

import (
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/drivers/jetstream"
	"bitbucket.org/iqhive/iqmq/drivers/nats"
	"bitbucket.org/iqhive/iqmq/drivers/natsunified"
)

// The measurement ADR-016 records, held as data so that the arithmetic in
// [natsunified.Config]'s documentation is checked rather than counted once.
//
// These are the numbers a unified field set would have cost: `sharedFields` is
// what the two driver configurations have in common and would have been retyped
// once and assigned twice, and the two exclusive counts are the profile-only
// groups. TestSharedConfigurationFields recomputes all three by reflection, so a
// field added to either driver fails this package — which is the whole point.
// The facade is then the increment that finds out, rather than the one that
// silently stops carrying a setting.
const (
	wantShared          = 19
	wantCoreOnly        = 2
	wantJetStreamOnly   = 22
	wantSharedRendering = "Addrs,ConnectTimeout,CredentialsFile,DrainTimeout,Hooks,InsecureSkipVerify,MaxPingsOut,MaxReconnects,Name,Password,PendingBytes,PendingMessages,PingInterval,ReconnectWait,SubjectPrefix,TLS,Token,Username,Clock"
)

// TestSharedConfigurationFields is the facade's cost, measured.
//
// ADR-016 asks what a facade over two drivers costs. The answer this increment
// gives is arithmetic: the two configurations share nineteen connection fields
// and disagree about twenty-four more, so a facade with ONE field set is
// nineteen retyped fields plus two profile-only groups, thirty-eight assignments
// and two validation paths to keep in step with two other modules by hand. The
// facade this increment shipped carries the two typed configurations instead, so
// the cost is zero and stays zero when either driver grows a field.
//
// This test is what makes the number a measurement. It fails when either driver
// adds, removes or renames a configuration field, and the failure message says
// which — so the next person reading ADR-016 gets the current number rather than
// the one that was true in September.
func TestSharedConfigurationFields(t *testing.T) {
	core := fieldNames(reflect.TypeFor[nats.Config]())
	js := fieldNames(reflect.TypeFor[jetstream.Config]())

	shared, coreOnly, jsOnly := partition(core, js)
	t.Logf("shared=%d coreonly=%d jetstreamonly=%d (nats.Config has %d fields, jetstream.Config has %d)",
		len(shared), len(coreOnly), len(jsOnly), len(core), len(js))
	t.Logf("shared: %s", strings.Join(shared, ","))
	t.Logf("core only: %s", strings.Join(coreOnly, ","))
	t.Logf("jetstream only: %s", strings.Join(jsOnly, ","))

	if len(shared) != wantShared || len(coreOnly) != wantCoreOnly || len(jsOnly) != wantJetStreamOnly {
		t.Errorf("the configuration partition moved: shared=%d (want %d), core-only=%d (want %d), jetstream-only=%d (want %d).\n"+
			"A driver grew, lost or renamed a configuration field. Update these constants AND the arithmetic in Config's\n"+
			"documentation and in adr/ADR-016-naming-autocreate-nats-packaging.md, which quote them as the facade's measured cost.\n"+
			"shared: %s\ncore only: %s\njetstream only: %s",
			len(shared), wantShared, len(coreOnly), wantCoreOnly, len(jsOnly), wantJetStreamOnly,
			strings.Join(shared, ","), strings.Join(coreOnly, ","), strings.Join(jsOnly, ","))
	}

	// The names as well as the count: two fields renamed in step would keep
	// the count and change what a unified struct would have had to carry.
	want := strings.Split(wantSharedRendering, ",")
	sort.Strings(want)
	if got := strings.Join(shared, ","); got != strings.Join(want, ",") {
		t.Errorf("the shared field NAMES moved:\n got %s\nwant %s", got, strings.Join(want, ","))
	}

	// Every shared field has to have the same TYPE in both, or a unified
	// struct could not have carried it with one field at all — which would
	// make the "nineteen retyped fields" figure an understatement.
	ct, jt := reflect.TypeFor[nats.Config](), reflect.TypeFor[jetstream.Config]()
	for _, name := range shared {
		a, _ := ct.FieldByName(name)
		b, _ := jt.FieldByName(name)
		if a.Type != b.Type {
			t.Errorf("shared field %s has type %v in nats.Config and %v in jetstream.Config; a unified struct could not carry it with one field",
				name, a.Type, b.Type)
		}
	}
}

// TestFacadeConfigCarriesBothProfilesAndNothingElse pins the facade's own
// configuration shape.
//
// It is four fields and it is deliberately not a merged field set.
// The check is here so that a later increment that "tidies"
// this into a unified struct has to change a test that says why it is not one.
func TestFacadeConfigCarriesBothProfilesAndNothingElse(t *testing.T) {
	got := fieldNames(reflect.TypeFor[natsunified.Config]())
	want := []string{"Core", "JetStream", "Profile", "Require"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("natsunified.Config fields = %s, want %s.\nThe facade carries the profile, the two typed driver configurations and the requirements;\nsee Config's documentation and for why it is not a merged field set.",
			strings.Join(got, ","), strings.Join(want, ","))
	}
}

// TestConfigDSNRendersTheSelectedProfileRedacted holds through the facade:
// a Config is safe to log, and what it renders is the scheme that would select
// the same profile again.
func TestConfigDSNRendersTheSelectedProfileRedacted(t *testing.T) {
	// A DSN naming both a user and a token is refused by drivers/nats, which
	// is checked here so that the configuration below is known to be the one
	// shape that carries a credential at all.
	if _, err := natsunified.ParseDSN("nats://user:topsecret@host:4222/acme?token=alsosecret"); err == nil {
		t.Fatal("a DSN naming both a user and a token was accepted; drivers/nats refuses it")
	}
	core, err := natsunified.ParseDSN("nats://user:topsecret@host:4222/acme")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	js, err := natsunified.ParseDSN("natsjs://user:alsotopsecret@host:4222/acme?stream=S")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}

	for _, tc := range []struct {
		name   string
		cfg    natsunified.Config
		scheme string
		secret string
	}{
		{"core", core, natsunified.SchemeCore, "topsecret"},
		{"jetstream", js, natsunified.SchemeJetStream, "alsotopsecret"},
	} {
		for label, rendered := range map[string]string{
			"DSN":      tc.cfg.DSN(),
			"String":   tc.cfg.String(),
			"LogValue": tc.cfg.LogValue().String(),
		} {
			if strings.Contains(rendered, tc.secret) {
				t.Errorf("%s: %s() leaked the credential: %s", tc.name, label, rendered)
			}
			if !strings.HasPrefix(rendered, tc.scheme+"://") {
				t.Errorf("%s: %s() = %q, want the %s:// scheme that selects this profile again", tc.name, label, rendered, tc.scheme)
			}
		}
	}

	// A configuration with no profile has no connection string, and says so
	// rather than rendering one profile's.
	var unset natsunified.Config
	if got := unset.DSN(); !strings.Contains(got, "unset") {
		t.Errorf("the unset Config renders as %q; it must not look like a usable connection string", got)
	}
	// And it renders through slog without panicking, which is what LogValuer
	// is for.
	_ = slog.StringValue(unset.String())
}

// fieldNames returns a struct's exported field names, sorted.
func fieldNames(t reflect.Type) []string {
	out := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		if f := t.Field(i); f.IsExported() {
			out = append(out, f.Name)
		}
	}
	sort.Strings(out)
	return out
}

// partition splits two sorted name lists into shared, a-only and b-only.
func partition(a, b []string) (shared, aOnly, bOnly []string) {
	in := map[string]bool{}
	for _, n := range b {
		in[n] = true
	}
	seen := map[string]bool{}
	for _, n := range a {
		seen[n] = true
		if in[n] {
			shared = append(shared, n)
		} else {
			aOnly = append(aOnly, n)
		}
	}
	for _, n := range b {
		if !seen[n] {
			bOnly = append(bOnly, n)
		}
	}
	return shared, aOnly, bOnly
}
