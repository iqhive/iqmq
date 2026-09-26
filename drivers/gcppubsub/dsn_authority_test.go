package gcppubsub_test

import (
	"reflect"
	"testing"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
)

// TestTheAuthorityIsReadAsWhatItNames holds ParseDSN to reading the authority
// as the one thing it names in each position.
//
// With no path the authority is the PROJECT, which is a name and not a dial
// address: it was read through dsn.Host.String, which brackets any host with a
// colon, so the domain-scoped project example.com:my-project came back as
// "[example.com:my-project]" whichever way it was written -- and once the
// shared parser accepted a bracketed host with no port, the bracketed spelling
// was accepted too, naming a project that does not exist. A port has no
// meaning on a project and was kept as part of its name.
//
// With a path the authority is the ENDPOINT, a dial address, and a host that
// decodes to a byte the gRPC target grammar gives a meaning to dials somewhere
// else: "a%3Fb:8085" reached "dns:///a", measured. A Pub/Sub client dials one
// endpoint, so a second host was silently dropped in either position -- and so
// was the authority whenever the `project` or `endpoint` parameter named the
// same setting again.
func TestTheAuthorityIsReadAsWhatItNames(t *testing.T) {
	for in, want := range map[string]gps.Config{
		"gcppubsub://my-project":                 {ProjectID: "my-project"},
		"gcppubsub://[example.com:my-project]":   {ProjectID: "example.com:my-project"},
		"gcppubsub://example.com%3Amy-project":   {ProjectID: "example.com:my-project"},
		"gcppubsub:///example.com:my-project":    {ProjectID: "example.com:my-project"},
		"gcppubsub://[::1]":                      {ProjectID: "::1"},
		"gcppubsub://127.0.0.1:8085/my-project":  {Endpoint: "127.0.0.1:8085", ProjectID: "my-project"},
		"gcppubsub://[::1]:8085/my-project":      {Endpoint: "[::1]:8085", ProjectID: "my-project"},
		"gcppubsub://[::1]/my-project":           {Endpoint: "[::1]", ProjectID: "my-project"},
		"gcppubsub://p?endpoint=a%3Fb:8085":      {Endpoint: "a?b:8085", ProjectID: "p"},
		"gcppubsub://?project=p&endpoint=h:8085": {Endpoint: "h:8085", ProjectID: "p"},
	} {
		got, err := gps.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if got.ProjectID != want.ProjectID || got.Endpoint != want.Endpoint {
			t.Errorf("ParseDSN(%q) = project %q endpoint %q, want project %q endpoint %q",
				in, got.ProjectID, got.Endpoint, want.ProjectID, want.Endpoint)
		}
		back, err := gps.ParseDSN(got.DSN())
		if err != nil {
			t.Errorf("ParseDSN(%q) renders as %q, which does not parse: %v", in, got.DSN(), err)
			continue
		}
		if !reflect.DeepEqual(back, got) {
			t.Errorf("ParseDSN(%q) renders as %q, which parses to project %q endpoint %q",
				in, got.DSN(), back.ProjectID, back.Endpoint)
		}
	}
	for _, in := range []string{
		"gcppubsub://my-project:8085",
		"gcppubsub://[example.com:my-project]:8085",
		"gcppubsub://a,b",
		"gcppubsub://a:1,b:2/p",
		"gcppubsub://tok%40real-host:8085/p",
		"gcppubsub://a%2Fb:8085/p",
		"gcppubsub://a%3Fb:8085/p",
		"gcppubsub://a%23b:8085/p",
		"gcppubsub://a%20b:8085/p",
		"gcppubsub://a%09b:8085/p",
		"gcppubsub://a%00b:8085/p",
		"gcppubsub://a%7Fb:8085/p",
		"gcppubsub://a%5Cb:8085/p",
		"gcppubsub://a%2Cb:8085/p",
		// One setting named twice. The parameter used to win silently, so
		// "gcppubsub://emulator-host?project=p" dropped the emulator and
		// dialled the managed service; cmd/iqmq's own describe test wrote
		// its emulator that way.
		"gcppubsub://emulator-host?project=p",
		"gcppubsub://h:8085/p?project=q",
		"gcppubsub://h:8085/p?endpoint=e:1",
	} {
		cfg, err := gps.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted as project %q endpoint %q", in, cfg.ProjectID, cfg.Endpoint)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
}
