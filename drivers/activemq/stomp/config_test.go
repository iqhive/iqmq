package stomp_test

import (
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
)

// TestConfigValidate holds the specification's "refuse before any I/O" to every refusal
// `config.go` has, in both directions.
//
// Both directions is the point. A gate that refused everything would pass a
// table of refusals and be useless, so every row that must be refused is
// paired with one that must be accepted, and the two measured refusals — the
// Artemis selector and the plaintext passcode — carry the reason in the
// failure message rather than only in the source.
func TestConfigValidate(t *testing.T) {
	t.Parallel()
	base := func() stomp.Config {
		return stomp.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}}
	}
	for name, tc := range map[string]struct {
		cfg  stomp.Config
		want error // nil accepts
		code mq.ErrorCode
		says string // a substring the refusal must carry
	}{
		"a broker outside the enumeration is refused": {
			// The two products' destination forms name nothing in common,
			// and on Classic a destination's own shape is the only thing
			// that says whether a name is a queue or a topic -- so a wrong
			// answer publishes into a queue every consumer is subscribed to
			// and none of them receives anything from, with no error
			// anywhere.
			//
			cfg:  stomp.Config{Broker: activemq.Broker(9), Addrs: []string{"localhost:61613"}},
			want: activemq.ErrNoBroker,
		},
		"a caller who set no broker at all is refused, because the zero value names none": {
			// The hazard this closes: `activemq.Artemis` USED to be the
			// zero value, so `Broker.Valid()` was true for the field
			// nobody filled in and a Classic deployment that forgot the
			// line silently got the Artemis mapping -- which publishes to
			// `VirtualTopic.orders` on a broker with no such convention.
			// `activemq.BrokerUnset` is the zero value now and the DSN path
			// is no longer the only one that enforces the field.
			cfg:  stomp.Config{Addrs: []string{"localhost:61613"}},
			want: activemq.ErrNoBroker,
		},
		"artemis is accepted": {cfg: base()},
		"classic is accepted": {cfg: stomp.Config{Broker: activemq.Classic, Addrs: []string{"localhost:61613"}}},
		"no addresses at all": {cfg: stomp.Config{Broker: activemq.Artemis}},
		"a metadata mode must be one of the four": {
			cfg:  func() stomp.Config { c := base(); c.Metadata = mq.MetadataMode(99); return c }(),
			code: mq.CodeInvalidConfig,
			says: "",
		},
		"every mode is accepted": {
			cfg: func() stomp.Config { c := base(); c.Metadata = mq.MetadataPortableEnvelope; return c }(),
		},
		"receipts cannot be both on and off": {
			cfg:  func() stomp.Config { c := base(); c.Receipts, c.NoReceipts = true, true; return c }(),
			code: mq.CodeInvalidConfig,
			says: "Receipts and NoReceipts",
		},
		"receipts off alone is accepted": {
			cfg: func() stomp.Config { c := base(); c.NoReceipts = true; return c }(),
		},
		"a selector on Artemis is refused, and the reason is measured": {
			// Measured: Artemis IGNORES a `selector` header when the
			// subscription is a fully qualified queue name, which is the
			// only form this driver's durable shared subscription takes. So
			// a caller who set one would be told nothing and filtered
			// nothing, which is why it is refused rather than accepted and
			// dropped.
			cfg:  func() stomp.Config { c := base(); c.Selector = "colour = 'red'"; return c }(),
			code: mq.CodeUnsupported,
			says: "fully qualified queue name",
		},
		"a selector on Classic is accepted, and it is the row STOMP wins": {
			// Measured on Classic: a `colour = 'red'` selector kept a blue
			// message out of the subscription. No AMQP 1.0 filter is
			// reachable through go-amqp, so this is the one row that makes
			// The specification end "STOMP kept for Classic".
			cfg: func() stomp.Config {
				c := stomp.Config{Broker: activemq.Classic, Addrs: []string{"localhost:61613"}}
				c.Selector = "colour = 'red'"
				return c
			}(),
		},
		"a prefix has to be a name both brokers accept": {
			cfg:  func() stomp.Config { c := base(); c.Prefix = "has a space"; return c }(),
			code: mq.CodeInvalidArgument,
		},
		"an ordinary prefix is accepted": {
			cfg: func() stomp.Config { c := base(); c.Prefix = "tenant-a."; return c }(),
		},
		"a dead-letter topic has to be a topic": {
			cfg:  func() stomp.Config { c := base(); c.DeadLetterTopic = "bad topic"; return c }(),
			code: mq.CodeInvalidArgument,
		},
		"a dead-letter topic is accepted": {
			cfg: func() stomp.Config { c := base(); c.DeadLetterTopic = "orders-dead"; return c }(),
		},
		"an empty address in the list is refused": {
			// The resolved default is localhost, so an EMPTY slice is fine
			// and an empty STRING in the slice is not: a caller who built
			// the list from configuration and got a blank line should be
			// told rather than dialling localhost.
			cfg:  func() stomp.Config { c := base(); c.Addrs = []string{"localhost:61613", ""}; return c }(),
			code: mq.CodeInvalidConfig,
			says: "empty",
		},
		"an address with whitespace is refused": {
			cfg:  func() stomp.Config { c := base(); c.Addrs = []string{"local host:61613"}; return c }(),
			code: mq.CodeInvalidConfig,
			says: "whitespace",
		},
		"a passcode over a plaintext address is refused": {
			// It travels in the CONNECT frame in clear. The specification's `Insecure*`
			// convention is that the default refuses and the caller says so
			// deliberately.
			cfg:  func() stomp.Config { c := base(); c.Passcode = "hunter2"; return c }(),
			code: mq.CodeInvalidConfig,
			says: "InsecurePlainLogin",
		},
		"a passcode with NO address is refused too, which an early return used to skip": {
			// The defect: `Validate` returned nil as soon as the address
			// list was empty, so every check below it -- the plaintext
			// refusal and each negative-value guard -- was skipped. The
			// resolved default is plaintext localhost, so this is the
			// configuration that leaked the credential.
			cfg: func() stomp.Config {
				c := base()
				c.Addrs, c.Passcode = nil, "hunter2"
				return c
			}(),
			code: mq.CodeInvalidConfig,
			says: "InsecurePlainLogin",
		},
		"a negative timeout with NO address is refused too": {
			cfg: func() stomp.Config {
				c := base()
				c.Addrs, c.ReceiptTimeout = nil, -time.Second
				return c
			}(),
			code: mq.CodeInvalidConfig,
		},
		"a passcode over a plaintext address is accepted once it is said deliberately": {
			cfg: func() stomp.Config {
				c := base()
				c.Passcode, c.InsecurePlainLogin = "hunter2", true
				return c
			}(),
		},
		"a passcode over TLS is accepted": {
			cfg: func() stomp.Config {
				c := base()
				c.Passcode, c.TLS = "hunter2", &tls.Config{MinVersion: tls.VersionTLS12}
				return c
			}(),
		},
		"a passcode over an ssl:// address is accepted": {
			cfg: func() stomp.Config {
				c := base()
				c.Addrs, c.Passcode = []string{"stomp+ssl://localhost:61614"}, "hunter2"
				return c
			}(),
		},
		"a login with no passcode needs no TLS": {
			cfg: func() stomp.Config { c := base(); c.Login = "artemis"; return c }(),
		},
		"a negative count is refused": {
			cfg:  func() stomp.Config { c := base(); c.PrefetchBytes = -1; return c }(),
			code: mq.CodeInvalidConfig,
			says: "negative",
		},
		"a negative duration is refused": {
			cfg:  func() stomp.Config { c := base(); c.HeartbeatSend = -time.Second; return c }(),
			code: mq.CodeInvalidConfig,
			says: "negative",
		},
		"a negative reconnect attempt count is refused": {
			cfg:  func() stomp.Config { c := base(); c.ReconnectAttempts = -1; return c }(),
			code: mq.CodeInvalidConfig,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := tc.cfg.Validate()
			if tc.want == nil && tc.code == 0 {
				if err != nil {
					t.Fatalf("Validate refused a configuration it must accept: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate accepted %q; the specification refuses an unrepresentable configuration BEFORE any I/O, and a "+
					"STOMP broker answers a frame it refuses with an ERROR frame that closes the connection -- so "+
					"one bad value costs every other publisher on that connection its link", name)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("the refusal is %v, want one matching %v", err, tc.want)
			}
			if tc.code != 0 && mq.CodeOf(err) != tc.code {
				t.Errorf("the refusal's code is %v, want %v (%v)", mq.CodeOf(err), tc.code, err)
			}
			if tc.says != "" && !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the refusal does not say %q, so a caller cannot tell what to change: %v", tc.says, err)
			}
		})
	}
}

// TestConfigCredentialCheckReadsEveryAddress is the failover list, and it is
// its own test because reading only the first address is the plausible bug.
//
// A list that mixes a TLS node and a plaintext one sends the passcode in clear
// AS SOON AS IT FAILS OVER, which is the one moment nobody is watching: the
// first dial is protected, the check passes, and the credential leaks on the
// day the primary goes down. So the rule is that EVERY endpoint must be
// protected, and the order of the list must not matter.
func TestConfigCredentialCheckReadsEveryAddress(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		addrs  []string
		refuse bool
	}{
		"every address protected": {
			addrs: []string{"stomp+ssl://a:61614", "ssl://b:61614"}, refuse: false,
		},
		"the plaintext one is last": {
			addrs: []string{"stomp+ssl://a:61614", "b:61613"}, refuse: true,
		},
		"the plaintext one is first": {
			addrs: []string{"b:61613", "stomp+ssl://a:61614"}, refuse: true,
		},
		"none protected": {
			addrs: []string{"a:61613", "b:61613"}, refuse: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := stomp.Config{Broker: activemq.Artemis, Addrs: tc.addrs, Passcode: "hunter2"}
			err := cfg.Validate()
			switch {
			case tc.refuse && err == nil:
				t.Fatalf("Validate accepted a passcode over the failover list %v. A list that mixes a protected "+
					"node and a plaintext one sends the passcode in clear the moment it fails over, and a check "+
					"that read only the first address would pass this configuration for months", tc.addrs)
			case tc.refuse && mq.CodeOf(err) != mq.CodeInvalidConfig:
				t.Errorf("the refusal's code is %v, want %v (%v)", mq.CodeOf(err), mq.CodeInvalidConfig, err)
			case !tc.refuse && err != nil:
				t.Fatalf("Validate refused a passcode over %v, where every endpoint is protected: %v", tc.addrs, err)
			}
		})
	}
}

// TestConfigResolveDefaults pins what a configuration that names nothing gets.
//
// The defaults are read back through the DSN because that is the only exported
// view of a resolved configuration, and they are worth pinning rather than
// assuming: `ConsumerWindow` is Artemis's own default in BYTES while the
// portable prefetch is a COUNT, so the pair is the one place the portable
// option cannot be expressed and a silent change to either would move the
// broker's flow-control window without moving the buffer that bounds it.
func TestConfigResolveDefaults(t *testing.T) {
	t.Parallel()
	got, err := stomp.ParseDSN(stomp.Config{Broker: activemq.Artemis}.DSN())
	if err != nil {
		t.Fatalf("ParseDSN of the rendered default configuration: %v", err)
	}
	if len(got.Addrs) != 1 || got.Addrs[0] != "localhost:"+stomp.DefaultPort {
		t.Errorf("the default address list is %v, want [localhost:%s]", got.Addrs, stomp.DefaultPort)
	}
	if got.Prefetch != stomp.DefaultPrefetch {
		t.Errorf("the default prefetch is %d, want %d", got.Prefetch, stomp.DefaultPrefetch)
	}
	if got.ConsumerWindow != stomp.DefaultConsumerWindow {
		t.Errorf("the default consumer window is %d, want %d: it is Artemis's own default and it is BYTES, not "+
			"messages", got.ConsumerWindow, stomp.DefaultConsumerWindow)
	}
	if got.ReceiptTimeout != stomp.DefaultReceiptTimeout {
		t.Errorf("the default receipt timeout is %v, want %v", got.ReceiptTimeout, stomp.DefaultReceiptTimeout)
	}
	if got.DialTimeout != stomp.DefaultDialTimeout {
		t.Errorf("the default dial timeout is %v, want %v", got.DialTimeout, stomp.DefaultDialTimeout)
	}
	if got.NoReceipts {
		t.Error("the default configuration turns receipts OFF. It must not: without a receipt a STOMP publication " +
			"is fire-and-forget and ConfirmAccepted cannot be honoured at all, so the zero Config has to be " +
			"the safe one and a publisher that wants the round trip back asks for it")
	}
}

// TestConfigRedaction is a credential must not reach a log through any
// rendering a driver offers.
func TestConfigRedaction(t *testing.T) {
	t.Parallel()
	const secret = "hunter2-do-not-log"
	cfg := stomp.Config{
		Broker:             activemq.Artemis,
		Addrs:              []string{"localhost:61613"},
		Login:              "artemis",
		Passcode:           dsn.Secret(secret),
		InsecurePlainLogin: true,
	}
	for name, rendered := range map[string]string{
		"DSN()":         cfg.DSN(),
		"String()":      cfg.String(),
		"LogValue()":    cfg.LogValue().String(),
		"the secret's":  cfg.Passcode.String(),
		"%v on Secret":  string(dsn.Redacted),
		"GoString-ish":  cfg.Passcode.GoString(),
		"the login row": cfg.Login,
	} {
		if strings.Contains(rendered, secret) {
			t.Errorf("%s carries the passcode in clear: %q", name, rendered)
		}
	}
	if !strings.Contains(cfg.DSN(), dsn.Redacted) {
		t.Errorf("the rendered DSN does not mark where the passcode was (%q), so a reader cannot tell a "+
			"credentialled configuration from an anonymous one", cfg.DSN())
	}
}
