package natsunified_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"bitbucket.org/iqhive/iqmq/drivers/natsunified"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// FuzzUnifiedDSN fuzzes the facade's DSN dispatch.
//
// The invariants are the facade's own and there are four of them:
//
//  1. Nothing panics, and nothing accepted by the facade was rejected by the
//     driver it dispatched to (or the other way round). The facade parses the
//     grammar twice — once for the scheme, once inside the driver — so the two
//     passes have to agree on every input; a divergence would be a DSN the
//     facade accepts and the driver cannot open, or a scheme the facade rejects
//     for a reason the driver would not.
//  2. A parsed Config's Profile is valid and its scheme is the one in the
//     input. This is the dispatch itself: a `nats://` string must never produce
//     a JetStream configuration.
//  3. The UNSELECTED profile's configuration stays zero, so nothing leaks
//     between the two halves.
//  4. The rendering is safe to log: valid UTF-8, and the parsed password never
//     appears in it.
func FuzzUnifiedDSN(f *testing.F) {
	f.Add("nats://host:4222")
	f.Add("natsjs://host:4222?stream=S")
	f.Add("nats://u:p@a:4222,b:4222/acme?tls=true&noecho=true")
	f.Add("natsjs://u:p@host/acme?stream=S&storage=memory&managestream=true")
	f.Add("nats://host?stream=S")
	f.Add("natsjs://host?noecho=true")
	f.Add("natsunified://host")
	f.Add("nats://")
	f.Add("")
	f.Add("://")
	f.Add("nats://0?%93")
	// Components the parser decodes, which the renderers must escape back:
	// the shape of FuzzJetStreamDSN's "nAtsjs://%2C0" on both profiles --
	// refused now, because nats.go would split a host holding a comma into
	// two servers  -- and a user, a host and a parameter
	// that each carry bytes the grammar splits on.
	f.Add("nats://%2C0")
	f.Add("natsjs://%2C0")
	f.Add("nats://u%3Av%2Fw@a%3A%5Db:4222,[::1]:4223?name=a%26b%2Bc")
	// Every setting a profile renders, at a value that is not its default, so
	// that a setting the renderer drops is a field that comes back different.
	f.Add("nats://h:4222/acme?name=svc&flushtimeout=7s&pinginterval=2s&maxpingsout=4&noecho=true&maxreconnects=7")
	f.Add("natsjs://h:4222/acme?name=svc&stream=S&domain=hub&ackwait=31s&maxdeliver=5&pinginterval=2s&maxpingsout=4")
	// A token on each profile, so that a rendering that carried one is a
	// failure rather than a case no seed reaches.
	f.Add("nats://h:4222?token=t0k3n")
	f.Add("natsjs://h:4222?token=t0k3n&name=token%3Dt0k3n")

	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := natsunified.ParseDSN(s)
		p, perr := natsunified.ProfileForDSN(s)
		if (err == nil) != (perr == nil) {
			// ProfileForDSN may succeed where ParseDSN fails — a well formed
			// scheme with a bad parameter — but never the other way round: a
			// DSN the facade can fully parse has a profile.
			if err == nil {
				t.Fatalf("ParseDSN(%q) succeeded but ProfileForDSN failed: %v", s, perr)
			}
		}
		if err != nil {
			return
		}
		if !cfg.Profile.Valid() {
			t.Fatalf("ParseDSN(%q) returned the invalid profile %v", s, cfg.Profile)
		}
		if p != cfg.Profile {
			t.Fatalf("ParseDSN(%q) selected %v but ProfileForDSN said %v", s, cfg.Profile, p)
		}
		// The input's scheme is read the way the grammar reads it, which is
		// case-insensitively (RFC 3986): "NATSJS://" names JetStream exactly
		// as "natsjs://" does. Comparing the raw prefix instead failed on
		// "nAtsjs://000000", a DSN the facade dispatched correctly.
		// dsn.Scheme is held to lower-casing the input's
		// scheme and nothing else by FuzzDSNParse.
		scheme := cfg.Profile.Scheme()
		if in, serr := dsn.Scheme(s); serr != nil || in != scheme {
			t.Fatalf("ParseDSN(%q) selected %v, whose scheme is %q, for a DSN whose scheme is %q (%v)",
				s, cfg.Profile, scheme, in, serr)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("ParseDSN(%q) returned a configuration its own Validate refuses: %v", s, err)
		}

		// Every endpoint the DSN named reached the selected profile and
		// none reached the other. Counting them is the precise form: a DSN
		// with no authority at all ("nats://") legitimately carries no
		// address, and resolves to the driver's DefaultAddr at open.
		parsed, perr2 := dsn.Parse(s)
		if perr2 != nil {
			t.Fatalf("ParseDSN(%q) succeeded but dsn.Parse failed: %v", s, perr2)
		}
		switch cfg.Profile {
		case natsunified.ProfileCore:
			if cfg.JetStream.Stream != "" || len(cfg.JetStream.Addrs) != 0 || len(cfg.JetStream.StreamSubjects) != 0 {
				t.Fatalf("ParseDSN(%q) selected Core and filled the JetStream half in: %+v", s, cfg.JetStream)
			}
			if len(cfg.Core.Addrs) != len(parsed.Hosts) {
				t.Fatalf("ParseDSN(%q) selected Core with %d addresses for %d endpoints", s, len(cfg.Core.Addrs), len(parsed.Hosts))
			}
		case natsunified.ProfileJetStream:
			if len(cfg.Core.Addrs) != 0 || cfg.Core.NoEcho {
				t.Fatalf("ParseDSN(%q) selected JetStream and filled the Core half in: %+v", s, cfg.Core)
			}
			if len(cfg.JetStream.Addrs) != len(parsed.Hosts) {
				t.Fatalf("ParseDSN(%q) selected JetStream with %d addresses for %d endpoints", s, len(cfg.JetStream.Addrs), len(parsed.Hosts))
			}
		case natsunified.ProfileUnset:
			t.Fatalf("ParseDSN(%q) returned ProfileUnset with no error", s)
		}

		rendered := cfg.DSN()
		if !utf8.ValidString(rendered) {
			t.Fatalf("ParseDSN(%q).DSN() is not valid UTF-8: %q", s, rendered)
		}
		if rendered != cfg.String() || rendered != cfg.LogValue().String() {
			t.Fatalf("the three renderings of %q disagree: %q, %q, %q", s, rendered, cfg.String(), cfg.LogValue().String())
		}
		// The rendering is a DSN the facade reads back, to a configuration
		// that renders the same. This is the property that holds BOTH profile
		// drivers' renderers to the escaping their parsers need: the Core
		// driver's had no fuzz property that could see it, and JetStream's
		// was found writing a host of ",0" raw.
		//
		// A rendering past dsn.MaxLen is exempt, as FuzzDSNParse's is: the
		// profile drivers write every default out, so a DSN near the bound
		// renders past it, and that is the bound doing its job.
		if len(rendered) <= dsn.MaxLen {
			back, berr := natsunified.ParseDSN(rendered)
			if berr != nil {
				t.Fatalf("ParseDSN(%q) renders as %q, which does not parse: %v", s, rendered, berr)
			}
			if again := back.DSN(); again != rendered {
				t.Fatalf("ParseDSN(%q) renders as %q, which renders as %q", s, rendered, again)
			}
			// And to the same SETTINGS, compared field by field: comparing
			// renderings cannot see a setting the renderer dropped, because
			// the rendering is what dropped it. A field the input left at
			// zero comes back as the default the rendering wrote out, so
			// only the fields the input FILLED are compared.
			var diffs []string
			switch cfg.Profile {
			case natsunified.ProfileCore:
				diffs = filledDiffs(cfg.Core, back.Core)
			case natsunified.ProfileJetStream:
				diffs = filledDiffs(cfg.JetStream, back.JetStream)
			case natsunified.ProfileUnset:
			}
			if len(diffs) > 0 {
				t.Fatalf("ParseDSN(%q) renders as %q, which parses back to different settings: %s",
					s, rendered, strings.Join(diffs, "; "))
			}
		}
		// The credential POSITION is what is checked, not a substring
		// search: a one-character password matches a port number, and
		// "p" matches the "p" of "pendingmessages" — which is how the
		// first version of this property failed on a perfectly correct
		// rendering, exactly as drivers/nats's own fuzz target records.
		// The second version still searched, for ":<pw>@", and failed the
		// same way on "nAts://0:4222@0?name=:4222@", whose NAME holds
		// ":4222@": it reads the userinfo and the `token` parameter out of
		// the rendering's own structure now.
		for _, pw := range []string{cfg.Core.Password.Reveal(), cfg.JetStream.Password.Reveal()} {
			if pw == "" {
				continue
			}
			if got, ok := renderedPassword(rendered); !ok || got != dsn.Redacted {
				t.Fatalf("ParseDSN(%q).DSN() = %q carries %q in the credential position, not the redaction", s, rendered, got)
			}
		}
		for _, tok := range []string{cfg.Core.Token.Reveal(), cfg.JetStream.Token.Reveal()} {
			if tok == "" {
				continue
			}
			if got, ok := renderedParam(rendered, "token"); !ok || got != dsn.Redacted {
				t.Fatalf("ParseDSN(%q).DSN() = %q carries %q as the token, not the redaction", s, rendered, got)
			}
		}
	})
}

// filledDiffs names every field that want FILLED and got does not carry,
// leaving out the two credentials, which a rendering redacts on purpose.
func filledDiffs(want, got any) []string {
	vw, vg := reflect.ValueOf(want), reflect.ValueOf(got)
	var diffs []string
	for i := range vw.NumField() {
		name := vw.Type().Field(i).Name
		if name == "Password" || name == "Token" || vw.Field(i).IsZero() {
			continue
		}
		if !reflect.DeepEqual(vw.Field(i).Interface(), vg.Field(i).Interface()) {
			diffs = append(diffs, fmt.Sprintf("%s %#v, want %#v", name, vg.Field(i).Interface(), vw.Field(i).Interface()))
		}
	}
	return diffs
}

// renderedPassword is what a rendering carries in the credential position --
// the authority's userinfo after its first colon -- read from the rendering's
// structure rather than searched for, and whether it carries one at all.
func renderedPassword(rendered string) (string, bool) {
	_, rest, ok := strings.Cut(rendered, "://")
	if !ok {
		return "", false
	}
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	at := strings.LastIndexByte(rest, '@')
	if at < 0 {
		return "", false
	}
	_, pw, ok := strings.Cut(rest[:at], ":")
	return pw, ok
}

// renderedParam is the raw value a rendering carries for one query
// parameter, and whether it carries the parameter at all.
func renderedParam(rendered, key string) (string, bool) {
	_, query, ok := strings.Cut(rendered, "?")
	if !ok {
		return "", false
	}
	for _, pair := range strings.Split(query, "&") {
		if k, v, _ := strings.Cut(pair, "="); k == key {
			return v, true
		}
	}
	return "", false
}
