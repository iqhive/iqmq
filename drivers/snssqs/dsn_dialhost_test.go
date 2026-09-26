package snssqs_test

import (
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/snssqs"
)

// TestADSNHostCannotRedirectTheDial holds ParseDSN to refusing a host that
// decodes to a byte the endpoint URL it builds would give a meaning to.
//
// The shared parser percent-decodes a host and ParseDSN writes the result into
// "http[s]://<host>:<port>" as [sq.Config.Endpoint], which reaches BOTH AWS SDK
// clients as BaseEndpoint and is parsed there as a URI. Measured against
// aws-sdk-go-v2 on drivers/sqs, which builds the endpoint the same way, by
// capturing the HTTP request a GetQueueUrl call produced:
//
//	k:s@tok%40real-host:9324  requested http://real-host:9324/
//	a%2Fb:9324                requested http://a/b:9324 -- host a, no port
//	a%3Fb:9324                requested http://a/       -- host a, no port
//	a%23b:9324                requested http://a/       -- host a, no port
//	a%5Cb:9324                "failed to resolve service endpoint,
//	                           Failed to parse uri: http://a\b:9324"
//
// The last one is the shape the refusal exists for: the configuration was
// accepted and failed on the first API call with an error about a string the
// operator never wrote. A host name or an IP literal holds none of these
// bytes.
//
// A comma is NOT in the set: the same measurement requested
// "http://a,b:9324/", host and all, because an endpoint is one URI and not a
// list. That is where this driver differs from the NATS ones, whose client
// splits a server string on ",".
func TestADSNHostCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"snssqs://k:s@tok%40real-host:4566?region=r",
		"snssqs://a%2Fb:4566?region=r",
		"snssqs://a%3Fb:4566?region=r",
		"snssqs://a%23b:4566?region=r",
		"snssqs://a%5Cb:4566?region=r",
		"snssqs://a%20b:4566?region=r",
		"snssqs://a%09b:4566?region=r",
		"snssqs://a%7Fb:4566?region=r",
		// "a%3A%2F%2Fb" decodes to "a://b", which would put a second scheme
		// inside the endpoint URL. The "/" refusal covers it.
		"snssqs://a%3A%2F%2Fb:4566?region=r",
		// A double-encoded host puts the escape back.
		"snssqs://tok%2540real-host:4566?region=r",
		"snssqs://a%25b:4566?region=r",
	} {
		_, err := sq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: its host decodes to a byte the endpoint URL gives a meaning to, so "+
				"the SDK would call somewhere the DSN does not name", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
	// What a host legitimately holds still parses: an IPv6 literal, one with a
	// zone, a name, and a comma -- which an endpoint is not split on.
	for in, want := range map[string]string{
		"snssqs://[::1]:4566?region=r":                  "https://[::1]:4566",
		"snssqs://[fe80::1%25eth0]:4566?region=r":       "https://[fe80::1%25eth0]:4566",
		"snssqs://sns.eu-west-1.amazonaws.com?region=r": "https://sns.eu-west-1.amazonaws.com",
		"snssqs://a%2Cb:4566?region=r":                  "https://a,b:4566",
	} {
		cfg, err := sq.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if cfg.Endpoint != want {
			t.Errorf("ParseDSN(%q) calls %q, want %q", in, cfg.Endpoint, want)
		}
	}
}

// TestATypedEndpointCannotRedirectTheDial is the typed twin of the test
// above, which nothing had: `dialableHost` ran only inside ParseDSN, so
// writing the struct instead of the string carried a host straight past the
// guard the DSN refuses.
func TestATypedEndpointCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{
		"https://tok@real-host", "https://a?b", "https://a#b",
		"https://a\\b", "https://a b", "https://a%b",
	} {
		cfg := sq.Config{Region: "r", Endpoint: endpoint}
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate accepted the endpoint %q: the SDK parses it as a URI, so the call would go "+
				"somewhere the configuration does not name", endpoint)
		}
	}
	for _, endpoint := range []string{
		"https://h", "https://h:4566", "http://localhost:4566/000000000000",
		"https://[::1]:4566", "https://[fe80::1%25eth0]:4566", "https://a,b:4566",
	} {
		cfg := sq.Config{Region: "r", Endpoint: endpoint, InsecureTransport: strings.HasPrefix(endpoint, "http://")}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate refused the endpoint %q: %v", endpoint, err)
		}
	}
}
