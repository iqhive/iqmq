package amqp10_test

import (
	"crypto/tls"
	"log/slog"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestConfigValidateIsPure is the specification's "validate config before starting background
// work": every refusal below happens with no I/O at all.
func TestConfigValidateIsPure(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  aq.Config
	}{
		{"an empty address", aq.Config{Addrs: []string{"   "}}},
		{"an address that is not printable ASCII", aq.Config{Addrs: []string{"amqp://h\x00st:5672"}}},
		{"an address with a scheme go-amqp cannot dial", aq.Config{Addrs: []string{"http://host:5672"}}},
		{"a SASL mechanism outside the enumeration", aq.Config{SASL: aq.SASLMechanism(99)}},
		{"a sender settle mode outside the enumeration", aq.Config{SenderSettle: aq.SenderSettleMode(99)}},
		{"a receiver settle mode outside the enumeration", aq.Config{ReceiverSettle: aq.ReceiverSettleMode(99)}},
		{"a backoff policy outside the enumeration", aq.Config{Backoff: aq.BackoffPolicy(99)}},
		{"a frame size below the protocol's floor", aq.Config{MaxFrameSize: 511}},
		{"a negative credit cap", aq.Config{MaxCredit: -1}},
		{"a negative pending bound", aq.Config{MaxPending: -1}},
		{"a negative reconnect attempt count", aq.Config{ReconnectAttempts: -1}},
		{"a negative dial timeout", aq.Config{DialTimeout: -time.Second}},
		{"a base delay longer than the cap", aq.Config{ReconnectBase: time.Minute, ReconnectMax: time.Second}},
		{"a container id with a control character", aq.Config{ContainerID: "a\x01b"}},
		{"a password with no user", aq.Config{Password: dsn.Secret("p")}},
		{"InsecureSkipVerify with no TLS", aq.Config{InsecureSkipVerify: true}},
		{"SASL PLAIN with no user", aq.Config{SASL: aq.SASLPlain}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatalf("a configuration with %s validated", tc.name)
			}
			assertCode(t, err, mq.CodeInvalidConfig, "a configuration with "+tc.name)
		})
	}
	if err := (aq.Config{}).Validate(); err != nil {
		t.Fatalf("the zero configuration was refused: %v", err)
	}
}

// TestConfigRefusesPlainSASLInTheClear is the rule reaching the one place a
// driver can leak every credential it is given.
//
// SASL PLAIN sends the password in the clear. A driver whose default did that
// would leak every one, so the combination is refused unless the operator has
// said so in a field named the way the specification asks — greppable and obviously unsafe.
func TestConfigRefusesPlainSASLInTheClear(t *testing.T) {
	plain := aq.Config{Addrs: []string{"amqp://host:5672"}, Username: "u", Password: dsn.Secret("p")}
	if err := plain.Validate(); err == nil {
		t.Fatal("SASL PLAIN over a plaintext address validated; the specification forbids leaking a credential and this leaks " +
			"every one")
	}
	// Any of the three ways out is accepted, and each is explicit.
	for name, cfg := range map[string]aq.Config{
		"an amqps address": {Addrs: []string{"amqps://host:5671"}, Username: "u", Password: dsn.Secret("p")},
		// A TLS config beside an address that states NO scheme: resolve
		// writes "amqps://" in, so this really is TLS on the wire.
		"a TLS config with an unscheme'd address": {
			Addrs: []string{"host:5672"}, Username: "u", Password: dsn.Secret("p"),
			TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		},
		"the operator saying so": {
			Addrs: []string{"amqp://host:5672"}, Username: "u", Password: dsn.Secret("p"), InsecurePlainSASL: true,
		},
	} {
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s was refused: %v", name, err)
		}
	}
	// A TLS config beside an address that says "amqp://" is NOT a way out,
	// and used to be listed here as one. go-amqp picks the transport from the
	// scheme alone -- `dialConn` switches on u.Scheme and never reads
	// ConnOptions.TLSConfig (v1.6.0 conn.go:238-246) -- so that combination
	// dialled in the CLEAR and put the password on it, while satisfying this
	// very guard. Measured on a socket: "amqp://host" with a TLSConfig set
	// writes 41 4d 51 50 ("AMQP"), the cleartext protocol header, where
	// "amqps://host" writes a TLS ClientHello. It is refused now.
	lying := aq.Config{
		Addrs: []string{"amqp://host:5672"}, Username: "u", Password: dsn.Secret("p"),
		TLS: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	if err := lying.Validate(); err == nil {
		t.Error("a TLS config beside an explicit amqp:// address validated; go-amqp dials that in the clear, so " +
			"SASL PLAIN puts the password on a cleartext wire while this guard reads as satisfied")
	}
	// "Every address" and not "any": a credential goes to whichever peer
	// answers, so a list with one plaintext address in it leaks the password
	// to that one.
	mixed := aq.Config{
		Addrs:    []string{"amqps://a:5671", "amqp://b:5672"},
		Username: "u", Password: dsn.Secret("p"),
	}
	if err := mixed.Validate(); err == nil {
		t.Fatal("a failover list with one plaintext address in it validated; the credential goes to whichever peer " +
			"answers, so the plaintext one gets it in the clear")
	}
}

// TestConfigRedactsEverywhere is the and the rule over the three
// surfaces a Go value can be printed through.
func TestConfigRedactsEverywhere(t *testing.T) {
	const secret = "s3cr3t-do-not-print"
	cfg := aq.Config{
		Addrs:    []string{"amqps://host:5671"},
		Username: "user",
		Password: dsn.Secret(secret),
	}
	for name, rendered := range map[string]string{
		"String": cfg.String(),
		"DSN":    cfg.DSN(),
		"LogValue": func() string {
			var b strings.Builder
			h := slog.NewTextHandler(&b, nil)
			slog.New(h).Info("cfg", "config", cfg)
			return b.String()
		}(),
	} {
		if strings.Contains(rendered, secret) {
			t.Errorf("%s rendered the password: %s", name, rendered)
		}
		if !strings.Contains(rendered, dsn.Redacted) {
			t.Errorf("%s did not render the redaction, so a reader cannot tell a password-less config from one with "+
				"a password: %s", name, rendered)
		}
	}
	// And the compile-time surfaces exist, which is what keeps a future field
	// from escaping through %v.
	var _ interface{ String() string } = cfg
	var _ interface{ LogValue() slog.Value } = cfg
}

// TestConfigDSNRoundTrip is the specification's "redacted round trip": what
// [aq.Config.DSN] renders parses back into a configuration that names the same
// peer and the same behaviour, with the credential gone on purpose.
func TestConfigDSNRoundTrip(t *testing.T) {
	cfg := aq.Config{
		Addrs:             []string{"amqps://one:5671", "amqps://two:5671"},
		Username:          "user",
		Password:          dsn.Secret("s3cr3t"),
		ContainerID:       "iqmq-1",
		HostName:          "vhost",
		SASL:              aq.SASLPlain,
		TLS:               &tls.Config{MinVersion: tls.VersionTLS12},
		IdleTimeout:       45 * time.Second,
		MaxFrameSize:      32 << 10,
		MaxSessions:       128,
		SenderSettle:      aq.SenderSettleMixed,
		ReceiverSettle:    aq.ReceiverSettleSecond,
		MaxCredit:         64,
		MaxPending:        256,
		NonDurable:        true,
		DialTimeout:       3 * time.Second,
		OperationTimeout:  7 * time.Second,
		WriteTimeout:      11 * time.Second,
		Backoff:           aq.BackoffDecorrelated,
		ReconnectBase:     150 * time.Millisecond,
		ReconnectMax:      20 * time.Second,
		ReconnectGrowth:   1.5,
		ReconnectAttempts: 9,
	}
	rendered := cfg.DSN()
	back, err := aq.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("parsing this driver's own rendering %q: %v", rendered, err)
	}
	if back.Password.Reveal() == "s3cr3t" {
		t.Fatal("the round trip carried the password; the rendering exists in order to remove it")
	}
	for _, tc := range []struct {
		name      string
		got, want any
	}{
		{"ContainerID", back.ContainerID, cfg.ContainerID},
		{"HostName", back.HostName, cfg.HostName},
		{"Username", back.Username, cfg.Username},
		{"SASL", back.SASL, cfg.SASL},
		{"IdleTimeout", back.IdleTimeout, cfg.IdleTimeout},
		{"MaxFrameSize", back.MaxFrameSize, cfg.MaxFrameSize},
		{"MaxSessions", back.MaxSessions, cfg.MaxSessions},
		{"SenderSettle", back.SenderSettle, cfg.SenderSettle},
		{"ReceiverSettle", back.ReceiverSettle, cfg.ReceiverSettle},
		{"MaxCredit", back.MaxCredit, cfg.MaxCredit},
		{"MaxPending", back.MaxPending, cfg.MaxPending},
		{"NonDurable", back.NonDurable, cfg.NonDurable},
		{"DialTimeout", back.DialTimeout, cfg.DialTimeout},
		{"OperationTimeout", back.OperationTimeout, cfg.OperationTimeout},
		{"WriteTimeout", back.WriteTimeout, cfg.WriteTimeout},
		{"Backoff", back.Backoff, cfg.Backoff},
		{"ReconnectBase", back.ReconnectBase, cfg.ReconnectBase},
		{"ReconnectMax", back.ReconnectMax, cfg.ReconnectMax},
		{"ReconnectGrowth", back.ReconnectGrowth, cfg.ReconnectGrowth},
		{"ReconnectAttempts", back.ReconnectAttempts, cfg.ReconnectAttempts},
	} {
		if tc.got != tc.want {
			t.Errorf("%s round-tripped as %v, want %v (from %q)", tc.name, tc.got, tc.want, rendered)
		}
	}
	if len(back.Addrs) != 2 || back.Addrs[0] != "amqps://one:5671" || back.Addrs[1] != "amqps://two:5671" {
		t.Errorf("the addresses round-tripped as %v", back.Addrs)
	}
	if back.TLS == nil {
		t.Error("tls=true did not round-trip into a TLS config")
	}
}

// TestConfigResolveFillsEveryBoundWithABound is the rule: every internal
// buffer is bounded by default, so no default may mean "unlimited".
func TestConfigResolveFillsEveryBoundWithABound(t *testing.T) {
	rendered := (aq.Config{}).DSN()
	for _, want := range []string{
		"idletimeout=1m0s",
		"maxframesize=65536",
		"dialtimeout=10s",
		"operationtimeout=30s",
		"writetimeout=30s",
		"reconnectbase=200ms",
		"reconnectmax=30s",
		"maxpending=1000",
		// The backoff is named explicitly rather than left to
		// backoff.Auto, whose provisional winner is no delay at all.
		"backoff=fulljitter",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the zero configuration renders as %q, which does not contain %q", rendered, want)
		}
	}
}

// TestConfigDefaultBackoffIsJittered is the specification's own
// check: the specification requires a dial loop that never busy-spins, and the specification's
// provisional Auto winner is `none`, which is no delay at all.
func TestConfigDefaultBackoffIsJittered(t *testing.T) {
	if aq.DefaultBackoff != aq.BackoffFullJitter {
		t.Fatalf("DefaultBackoff is %v; a dial loop on anything without jitter is answered by every client in a "+
			"fleet at the same instant, which is what the \"reconnect with backoff (never busy-spin)\" forbids",
			aq.DefaultBackoff)
	}
	if got := aq.BackoffDefault.Resolve(); got != aq.BackoffFullJitter {
		t.Fatalf("the zero policy resolves to %v", got)
	}
	f := open(t, testProfile(), aq.Config{})
	// The schedule really grows, which is what a jittered exponential policy
	// has to do however the random draw falls: the CAP rises with the attempt
	// number, so the tenth attempt's ceiling is far above the first's.
	var maxEarly, maxLate time.Duration
	for i := range 200 {
		_ = i
		if d := f.b.Backoff(1, 0); d > maxEarly {
			maxEarly = d
		}
		if d := f.b.Backoff(8, 0); d > maxLate {
			maxLate = d
		}
	}
	if maxLate <= maxEarly {
		t.Fatalf("the backoff's eighth attempt draws from at most %v and its first from at most %v; an exponential "+
			"policy's ceiling has to rise", maxLate, maxEarly)
	}
	if maxLate > aq.DefaultReconnectMax {
		t.Fatalf("a draw of %v exceeds the configured cap of %v", maxLate, aq.DefaultReconnectMax)
	}
}

// TestConfigSettleModeCombinationsAreRefusedAtOpen holds the two combinations
// that cannot work, at the one place a caller can act on them.
func TestConfigSettleModeCombinationsAreRefusedAtOpen(t *testing.T) {
	t.Run("ConfirmNone on an unsettled link", func(t *testing.T) {
		f := open(t, testProfile(), aq.Config{SenderSettle: aq.SenderSettleUnsettled})
		_, err := f.b.OpenSink(sinkCtx(), mq.SinkConfig{Topic: "topic", Confirmation: mq.ConfirmNone})
		assertIs(t, err, mq.ErrUnsupported, "ConfirmNone on a link attached as unsettled")
	})
	t.Run("ConfirmAccepted on a pre-settling link", func(t *testing.T) {
		f := open(t, testProfile(), aq.Config{SenderSettle: aq.SenderSettleSettled})
		_, err := f.b.OpenSink(sinkCtx(), mq.SinkConfig{Topic: "topic", Confirmation: mq.ConfirmAccepted})
		assertIs(t, err, mq.ErrUnsupported, "ConfirmAccepted on a link that pre-settles everything")
	})
}

// TestConfigEnumStringsAreStable holds the names the DSN and the golden file are
// written in, because a renamed enumeration value silently changes both.
func TestConfigEnumStringsAreStable(t *testing.T) {
	for got, want := range map[string]string{
		aq.SASLAuto.String():              "auto",
		aq.SASLNone.String():              "none",
		aq.SASLAnonymous.String():         "anonymous",
		aq.SASLPlain.String():             "plain",
		aq.SASLExternal.String():          "external",
		aq.SenderSettleMixed.String():     "mixed",
		aq.ReceiverSettleFirst.String():   "first",
		aq.BackoffFullJitter.String():     "fulljitter",
		aq.SlotMessageAnnotation.String(): "messageannotation",
		aq.AttemptFromHeader.String():     "header",
	} {
		if got != want {
			t.Errorf("an enumeration renders as %q, want %q", got, want)
		}
	}
	for _, v := range []interface{ Valid() bool }{
		aq.SASLMechanism(99), aq.SenderSettleMode(99), aq.ReceiverSettleMode(99),
		aq.BackoffPolicy(99), aq.Slot(99), aq.AttemptSource(99),
	} {
		if v.Valid() {
			t.Errorf("%T(99) reports itself valid", v)
		}
	}
}
