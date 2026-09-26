package amqp10_test

import (
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
)

// TestParseDSNStrictRefusesAnUnknownParameter is ADR-010 the rule, and
// the one every URL-based configuration in the wild gets wrong: a DSN that
// quietly ignored "capacty=100000" would hand a caller a broker they did not
// ask for, and nobody would find it.
func TestParseDSNStrictRefusesAnUnknownParameter(t *testing.T) {
	_, err := aq.ParseDSN("amqp10://host:5672?maxcredit=8&capacty=100000")
	if err == nil {
		t.Fatal("a DSN with a typo'd parameter was accepted")
	}
	assertCode(t, err, mq.CodeInvalidConfig, "a typo'd parameter")
	if !strings.Contains(err.Error(), "capacty") {
		t.Fatalf("the refusal does not name the parameter: %v", err)
	}
}

// TestParseDSNReadsEveryParameterItDocuments holds the table against the
// documentation, because a parameter that is documented and not read is worse
// than one that is neither: the DSN accepts it and nothing honours it.
func TestParseDSNReadsEveryParameterItDocuments(t *testing.T) {
	cfg, err := aq.ParseDSN("amqp10://user:pass@one:5672,two/" +
		"?containerid=cid&hostname=vh&sasl=external&tls=true&insecureskipverify=true" +
		"&insecureplainsasl=true&sendersettle=mixed&receiversettle=second&backoff=equaljitter" +
		"&idletimeout=45s&maxframesize=32768&maxsessions=128&maxcredit=64&maxpending=256" +
		"&nondurable=true&dialtimeout=3s&operationtimeout=7s&writetimeout=11s" +
		"&reconnectbase=150ms&reconnectmax=20s&reconnectgrowth=1.5&reconnectattempts=9&noreconnect=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	for _, tc := range []struct {
		name      string
		got, want any
	}{
		{"ContainerID", cfg.ContainerID, "cid"},
		{"HostName", cfg.HostName, "vh"},
		{"SASL", cfg.SASL, aq.SASLExternal},
		{"InsecureSkipVerify", cfg.InsecureSkipVerify, true},
		{"InsecurePlainSASL", cfg.InsecurePlainSASL, true},
		{"SenderSettle", cfg.SenderSettle, aq.SenderSettleMixed},
		{"ReceiverSettle", cfg.ReceiverSettle, aq.ReceiverSettleSecond},
		{"Backoff", cfg.Backoff, aq.BackoffEqualJitter},
		{"MaxFrameSize", cfg.MaxFrameSize, uint32(32768)},
		{"MaxSessions", cfg.MaxSessions, uint16(128)},
		{"MaxCredit", cfg.MaxCredit, 64},
		{"MaxPending", cfg.MaxPending, 256},
		{"NonDurable", cfg.NonDurable, true},
		{"ReconnectGrowth", cfg.ReconnectGrowth, 1.5},
		{"ReconnectAttempts", cfg.ReconnectAttempts, 9},
		{"NoReconnect", cfg.NoReconnect, true},
		{"Username", cfg.Username, "user"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
	// tls=true turns the addresses into amqps and gives the one with no port
	// the TLS default, which is the protocol's own.
	if len(cfg.Addrs) != 2 || cfg.Addrs[0] != "amqps://one:5672" || cfg.Addrs[1] != "amqps://two:5671" {
		t.Errorf("the addresses parsed as %v, want amqps with the port the DSN gave and the TLS default for the "+
			"one that gave none", cfg.Addrs)
	}
	if cfg.TLS == nil {
		t.Error("tls=true did not produce a TLS config")
	}
	if cfg.Password.Reveal() != "pass" {
		t.Error("the password did not reach the configuration")
	}
}

// TestParseDSNDefaultPorts holds the two ports the protocol assigns, because a
// DSN that named none and got the wrong one would dial a port nothing is
// listening on.
func TestParseDSNDefaultPorts(t *testing.T) {
	plain, err := aq.ParseDSN("amqp10://host")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if plain.Addrs[0] != "amqp://host:"+aq.DefaultPort {
		t.Errorf("a plaintext address with no port became %q, want port %s", plain.Addrs[0], aq.DefaultPort)
	}
	secure, err := aq.ParseDSN("amqp10://host?tls=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if secure.Addrs[0] != "amqps://host:"+aq.DefaultTLSPort {
		t.Errorf("a TLS address with no port became %q, want port %s", secure.Addrs[0], aq.DefaultTLSPort)
	}
}

// TestParseDSNRefusals holds the values the table will not take, each with a
// message that names the parameter: a failure that does not say which parameter
// is a failure a caller has to guess at.
func TestParseDSNRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
	}{
		{"a scheme this core does not claim", "amqp://host:5672"},
		{"a SASL mechanism that is not one of the five", "amqp10://host?sasl=kerberos"},
		{"a sender settle mode that is not one of the three", "amqp10://host?sendersettle=maybe"},
		{"a receiver settle mode that is not one of the two", "amqp10://host?receiversettle=third"},
		{"a backoff policy that is not one of the five", "amqp10://host?backoff=linear"},
		{"a duration that is not one", "amqp10://host?idletimeout=soon"},
		{"a credit cap of zero, which the struct already means by omission", "amqp10://host?maxcredit=0"},
		{"a negative growth factor", "amqp10://host?reconnectgrowth=-1"},
		{"a growth factor that is not a number", "amqp10://host?reconnectgrowth=lots"},
		{"insecureskipverify with no TLS", "amqp10://host?insecureskipverify=true"},
		{"a boolean spelled the way strconv would take it", "amqp10://host?tls=1"},
		{"a frame size below the protocol's floor", "amqp10://host?maxframesize=100"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := aq.ParseDSN(tc.text); err == nil {
				t.Fatalf("%q was accepted with %s", tc.text, tc.name)
			}
		})
	}
}

// TestParseDSNSchemeIsTheProfilesOwn is the seam's half of the DSN: a
// broker-specific module registers ITS scheme, and this core's is only the
// generic AMQP 1.0 profile.
func TestParseDSNSchemeIsTheProfilesOwn(t *testing.T) {
	if _, err := aq.ParseDSNScheme("activemq", "activemq://host:5672?maxcredit=8"); err != nil {
		t.Fatalf("a profile's own scheme was refused: %v", err)
	}
	if _, err := aq.ParseDSNScheme("activemq", "amqp10://host:5672"); err == nil {
		t.Fatal("a profile that claims \"activemq\" accepted this core's generic scheme; a DSN that answered both " +
			"would be ambiguous about which profile's capabilities the caller gets")
	}
}

// TestParseIntoLeavesStrictToTheProfile is the reason [aq.ParseInto] exists: a
// broker-specific module reads the core's parameters, then its own, and calls
// Strict once — so a parameter neither half claims is still a construction error
// naming it, and neither half has to know the other's table.
func TestParseIntoLeavesStrictToTheProfile(t *testing.T) {
	parsed, err := dsn.Parse("activemq://host:5672?maxcredit=8&anycast=true")
	if err != nil {
		t.Fatalf("dsn.Parse: %v", err)
	}
	cfg, err := aq.ParseInto(&parsed)
	if err != nil {
		t.Fatalf("ParseInto rejected a parameter that belongs to the profile: %v; a module that had to call "+
			"ParseDSNScheme would have its own parameters refused by the core's Strict", err)
	}
	if cfg.MaxCredit != 8 {
		t.Fatalf("the core's own parameter did not reach the configuration: %+v", cfg)
	}
	// The profile then reads its own, and Strict is satisfied because the read
	// marks are shared.
	var anycast bool
	if err := parsed.Params.Bool("anycast", &anycast); err != nil {
		t.Fatalf("the profile's parameter: %v", err)
	}
	if err := parsed.Params.Strict(); err != nil {
		t.Fatalf("Strict refused a DSN both halves read: %v", err)
	}
	if !anycast {
		t.Fatal("the profile's own parameter did not reach it")
	}
	// And a parameter NEITHER half reads is still an error, which is the whole
	// point of leaving Strict to the caller.
	parsed2, err := dsn.Parse("activemq://host:5672?nobody-reads-this=1")
	if err != nil {
		t.Fatalf("dsn.Parse: %v", err)
	}
	if _, err := aq.ParseInto(&parsed2); err != nil {
		t.Fatalf("ParseInto: %v", err)
	}
	if err := parsed2.Params.Strict(); err == nil {
		t.Fatal("Strict accepted a parameter neither half read")
	}
}

// TestRegisterRegistersTheProfileAndNotTheCore holds the registry seam: this
// module has no capabilities of its own to register, so a registry entry needs
// a profile or it would open a client that cannot address anything.
func TestRegisterRegistersTheProfileAndNotTheCore(t *testing.T) {
	r := dsn.NewRegistry()
	if err := aq.Register(r, "activemq", testProfile()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, ok := r.Lookup("activemq"); !ok {
		t.Fatal("the scheme was not registered")
	}
	// An empty scheme falls back to this core's own, which is the generic
	// AMQP 1.0 profile.
	r2 := dsn.NewRegistry()
	if err := aq.Register(r2, "", testProfile()); err != nil {
		t.Fatalf("Register with no scheme: %v", err)
	}
	if _, ok := r2.Lookup(aq.Scheme); !ok {
		t.Fatalf("an empty scheme did not register %q", aq.Scheme)
	}
	// A profile that cannot be used is refused at REGISTRATION, which is the
	// earliest a program can hear about it: the alternative is a registry
	// entry whose first use fails.
	bad := testProfile()
	bad.SourceAddress = nil
	if err := aq.Register(dsn.NewRegistry(), "activemq", bad); err == nil {
		t.Fatal("an unusable profile was registered; the failure would then happen on the first Open instead")
	}
	if err := aq.Register(nil, "activemq", testProfile()); err == nil {
		t.Fatal("Register accepted a nil registry")
	}
}

// TestParseDSNIsBoundedAndTotal is the rule for a parser whose bytes may
// come from an environment variable a stranger set: it never panics, and it
// reads at most the shared parser's bound.
func TestParseDSNIsBoundedAndTotal(t *testing.T) {
	for _, text := range []string{
		"",
		"amqp10://",
		"amqp10://?",
		"amqp10://?=",
		"amqp10://:@:",
		"amqp10://" + strings.Repeat("h", 16<<10),
		"amqp10://host?" + strings.Repeat("a=1&", 4096),
		"amqp10://host?\x93",
		"amqp10://[::1]:5672",
		"amqp10://%zz@host",
	} {
		// The contract is that it returns rather than panics; whether it
		// accepts is the table's business.
		_, _ = aq.ParseDSN(text)
	}
}
