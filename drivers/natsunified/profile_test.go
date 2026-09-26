package natsunified_test

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/drivers/natsunified"
)

// TestProfileDriverNamesMatchTheDrivers holds the two names this package copies
// from the drivers it delegates to against the drivers themselves.
//
// `Profile.Driver()` is what tells a caller which driver is really serving it,
// and it is also what `ProfileOf` matches `Info().Name` against, so a copy that
// went stale would make `ProfileOf` answer ProfileUnset for a perfectly good
// client. A constant copied from another module is a constant that can drift;
// this is the check that notices.
func TestProfileDriverNamesMatchTheDrivers(t *testing.T) {
	if got, want := natsunified.ProfileCore.Driver(), nats.DriverName; got != want {
		t.Errorf("ProfileCore.Driver() = %q, want drivers/nats.DriverName %q", got, want)
	}
	if got, want := natsunified.ProfileJetStream.Driver(), jetstream.DriverName; got != want {
		t.Errorf("ProfileJetStream.Driver() = %q, want drivers/jetstream.DriverName %q", got, want)
	}
	if natsunified.ProfileUnset.Driver() != "" {
		t.Errorf("ProfileUnset.Driver() = %q, want the empty string: an unset profile names no driver", natsunified.ProfileUnset.Driver())
	}
}

// TestProfileUnsetIsNotAProfile holds the zero Profile is not
// a default, and every surface refuses it rather than resolving it.
//
// The reason is in the ledger and it is worth restating where the check is: the
// two profiles disagree about at-most-once versus at-least-once delivery, so a
// zero value that resolved to either would pick the most consequential property
// a queue has by omission.
func TestProfileUnsetIsNotAProfile(t *testing.T) {
	if natsunified.ProfileUnset.Valid() {
		t.Error("ProfileUnset.Valid() is true; the zero Profile must not name a profile")
	}
	if natsunified.Profile(200).Valid() {
		t.Error("Profile(200).Valid() is true; a value outside the enumeration must not name a profile")
	}
	for _, p := range natsunified.Profiles() {
		if !p.Valid() {
			t.Errorf("Profiles() returned %v, which is not Valid()", p)
		}
		if p == natsunified.ProfileUnset {
			t.Error("Profiles() includes ProfileUnset")
		}
	}
	if n := len(natsunified.Profiles()); n != 2 {
		t.Errorf("Profiles() returned %d profiles, want 2", n)
	}
	err := natsunified.Config{}.Validate()
	if err == nil {
		t.Fatal("Config{}.Validate() returned nil; a configuration with no profile must be refused")
	}
	if code := mq.CodeOf(err); code != mq.CodeInvalidConfig {
		t.Errorf("Config{}.Validate() carries %v, want CodeInvalidConfig", code)
	}
	if _, err := natsunified.Open(t.Context(), natsunified.Config{}); err == nil {
		t.Error("Open with no profile returned no error")
	}
	if _, err := natsunified.OpenBackend(t.Context(), natsunified.Config{}); err == nil {
		t.Error("OpenBackend with no profile returned no error")
	}
}

// TestProfileStringAndScheme pins the renderings a report, a log line and a DSN
// are keyed by. They are part of the facade's contract: the conformance report's
// Profile field is Profile.String(), so a rename would silently rename a column
// of the generated capability matrix.
func TestProfileStringAndScheme(t *testing.T) {
	for _, tc := range []struct {
		p      natsunified.Profile
		name   string
		scheme string
	}{
		{natsunified.ProfileUnset, "unset", ""},
		{natsunified.ProfileCore, "core", natsunified.SchemeCore},
		{natsunified.ProfileJetStream, "jetstream", natsunified.SchemeJetStream},
	} {
		if got := tc.p.String(); got != tc.name {
			t.Errorf("Profile(%d).String() = %q, want %q", tc.p, got, tc.name)
		}
		if got := tc.p.Scheme(); got != tc.scheme {
			t.Errorf("%v.Scheme() = %q, want %q", tc.p, got, tc.scheme)
		}
	}
	if got := natsunified.Profile(200).String(); got != "Profile(?)" {
		t.Errorf("Profile(200).String() = %q, want Profile(?)", got)
	}
	if got := natsunified.Profile(200).Scheme(); got != "" {
		t.Errorf("Profile(200).Scheme() = %q, want the empty string", got)
	}
}

// TestProfileForSchemeRefusesAnythingElse holds the facade's dispatch table.
//
// The refusal is ErrDriverNotRegistered and not a bespoke error, because that is
// what a registry answers for a scheme nobody claimed and therefore the error a
// caller already handles.
//
// A scheme is case-insensitive (RFC 3986), so "NATS" is Core: it is what
// ProfileForDSN has always answered for "NATS://host", and "NATS" used to be in
// the refused list below, which pinned the one comparison in the facade that
// disagreed with the grammar. "natsjs://x" is refused
// because it is more than a scheme.
func TestProfileForSchemeRefusesAnythingElse(t *testing.T) {
	for scheme, want := range map[string]natsunified.Profile{
		natsunified.SchemeCore:      natsunified.ProfileCore,
		natsunified.SchemeJetStream: natsunified.ProfileJetStream,
		"NATS":                      natsunified.ProfileCore,
		"NatsJS":                    natsunified.ProfileJetStream,
	} {
		got, err := natsunified.ProfileForScheme(scheme)
		if err != nil {
			t.Errorf("ProfileForScheme(%q): %v", scheme, err)
		}
		if got != want {
			t.Errorf("ProfileForScheme(%q) = %v, want %v", scheme, got, want)
		}
		if dsnProfile, err := natsunified.ProfileForDSN(scheme + "://host"); err != nil || dsnProfile != got {
			t.Errorf("ProfileForScheme(%q) = %v but ProfileForDSN(%q) = %v, %v", scheme, got, scheme+"://host", dsnProfile, err)
		}
	}
	for _, scheme := range []string{"", "kafka", "natsjetstream", "nats2", "natsj", "natsjs://x", "nats://", "ﬁnats", "NATſ"} {
		got, err := natsunified.ProfileForScheme(scheme)
		if err == nil {
			t.Errorf("ProfileForScheme(%q) = %v with no error; the facade claims two schemes and no others", scheme, got)
			continue
		}
		if !errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ProfileForScheme(%q) error is %v, want ErrDriverNotRegistered", scheme, err)
		}
		if got != natsunified.ProfileUnset {
			t.Errorf("ProfileForScheme(%q) returned %v beside its error, want ProfileUnset", scheme, got)
		}
	}
}
