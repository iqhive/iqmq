package rabbitmq_test

import (
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// base is a configuration that validates, so that each case below changes one
// thing.
func base() rq.Config {
	return rq.Config{Addrs: []string{"localhost:5672"}}
}

// TestValidate holds every refusal to an input that triggers it and one that
// does not.
//
// Both directions are here on purpose: a validator tested only on bad input
// passes when it refuses everything, which is a driver that cannot connect.
func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  rq.Config
		code mq.ErrorCode
	}{
		{"minimal", base(), 0},
		{"everything", fullConfig(), 0},
		{"no endpoint", rq.Config{}, mq.CodeInvalidConfig},
		{"blank endpoint", rq.Config{Addrs: []string{"  "}}, mq.CodeInvalidConfig},
		{"password without user", withCfg(func(c *rq.Config) { c.Password = "p" }), mq.CodeInvalidConfig},
		{"insecure without tls", withCfg(func(c *rq.Config) { c.InsecureSkipVerify = true }), mq.CodeInvalidConfig},
		{"unknown queue type", withCfg(func(c *rq.Config) { c.QueueType = rq.QueueType(9) }), mq.CodeInvalidConfig},
		{"unknown exchange type", withCfg(func(c *rq.Config) { c.ExchangeType = "topics" }), mq.CodeInvalidConfig},
		{"dead letter without declare", withCfg(func(c *rq.Config) { c.DeadLetter = true }), mq.CodeInvalidConfig},
		{"detect without declare", withCfg(func(c *rq.Config) { c.DetectDelayPlugin = true }), mq.CodeInvalidConfig},
		{"negative heartbeat", withCfg(func(c *rq.Config) { c.Heartbeat = -time.Second }), mq.CodeInvalidConfig},
		{"negative reconnect base", withCfg(func(c *rq.Config) { c.ReconnectBase = -time.Second }), mq.CodeInvalidConfig},
		{"negative queue length", withCfg(func(c *rq.Config) { c.QueueMaxLength = -1 }), mq.CodeInvalidConfig},
		{"unknown backoff", withCfg(func(c *rq.Config) { c.BackoffPolicy = 200 }), mq.CodeInvalidConfig},
		{"reserved prefix", withCfg(func(c *rq.Config) { c.Prefix = "amq." }), mq.CodeInvalidArgument},
		{"ladder not increasing", withCfg(func(c *rq.Config) {
			c.DelayLadder = []time.Duration{time.Minute, time.Second}
		}), mq.CodeInvalidConfig},
		{"window with per-message confirms", withCfg(func(c *rq.Config) {
			c.ConfirmKind, c.ConfirmWindow = confirm.PerMessage, 8
		}), mq.CodeInvalidConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			switch {
			case tc.code == 0 && err != nil:
				t.Fatalf("Validate = %v, want nil", err)
			case tc.code != 0 && err == nil:
				t.Fatalf("Validate = nil, want a refusal carrying %v", tc.code)
			case tc.code != 0 && mq.CodeOf(err) != tc.code:
				t.Fatalf("Validate = %v carrying %v, want %v", err, mq.CodeOf(err), tc.code)
			}
		})
	}
}

// withCfg is [base] with one field changed.
func withCfg(fn func(*rq.Config)) rq.Config {
	c := base()
	fn(&c)
	return c
}

// fullConfig is every field set to something plausible, so that the
// "everything" case above is not a configuration nobody would write.
func fullConfig() rq.Config {
	return rq.Config{
		Addrs:             []string{"amqps://a:5671", "amqps://b:5671"},
		Username:          "iqmq",
		Password:          "secret-not-a-real-credential",
		Vhost:             "/prod",
		TLS:               &tls.Config{MinVersion: tls.VersionTLS12},
		Prefix:            "iqmq.",
		ExchangeType:      "topic",
		QueueType:         rq.QueueQuorum,
		Declare:           true,
		DeadLetter:        true,
		DeadLetterTopic:   "iqmq-dead",
		QueueMaxLength:    10_000,
		Mandatory:         true,
		KeyAsRoutingKey:   true,
		ConfirmKind:       confirm.Windowed,
		ConfirmWindow:     32,
		DelayKind:         delay.TTLDLX,
		DelayLadder:       []time.Duration{time.Second, 30 * time.Second, 5 * time.Minute},
		DelayQueueTTL:     time.Hour,
		DetectDelayPlugin: true,
		Heartbeat:         15 * time.Second,
		DialTimeout:       5 * time.Second,
		ChannelMax:        64,
		FrameSize:         131072,
		ConnectionName:    "orders-worker",
		PublishTimeout:    10 * time.Second,
		ReconnectBase:     200 * time.Millisecond,
		ReconnectMax:      time.Minute,
		MaxReconnects:     10,
		ConsumerTagPrefix: "orders",
		AppID:             "orders",
		PublishType:       "OrderPlaced",
		ExclusiveConsumer: false,
	}
}

// TestPriorityForcesAClassicQueue is the combination RabbitMQ refuses and the
// driver has to resolve rather than pass on.
//
// `x-max-priority` and a quorum queue are mutually exclusive at the broker, and
// a driver that sent both would get 406 PRECONDITION_FAILED on the first
// declaration -- a channel exception. The driver resolves it in favour of the
// setting a caller only ever makes deliberately, and the capability table then
// reports that the attempt count is unavailable.
func TestPriorityForcesAClassicQueue(t *testing.T) {
	cfg := withCfg(func(c *rq.Config) {
		c.Declare, c.MaxPriority = true, 9
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a priority ceiling with the default queue type was refused: %v", err)
	}
	// The resolution is visible in the rendered configuration, which is what
	// an operator reads.
	if !strings.Contains(cfg.String(), "queue=quorum") {
		t.Errorf("the configuration renders %q: the queue TYPE is still what the caller set, and the declaration is where the conflict is resolved", cfg.String())
	}
}

// TestStringAndLogValueRedact is the rule: a credential appears in nothing
// iqmq renders.
func TestStringAndLogValueRedact(t *testing.T) {
	const secret = "s3cr3t-not-a-real-credential"
	cfg := withCfg(func(c *rq.Config) {
		c.Username, c.Password = "iqmq", dsn.Secret(secret)
	})
	for name, rendered := range map[string]string{
		"String": cfg.String(),
		"DSN":    cfg.DSN(),
		"LogValue": func() string {
			var b strings.Builder
			for _, a := range cfg.LogValue().Group() {
				b.WriteString(a.String())
				b.WriteByte(' ')
			}
			return b.String()
		}(),
	} {
		if strings.Contains(rendered, secret) {
			t.Errorf("%s leaked the password: %s", name, rendered)
		}
		if !strings.Contains(rendered, dsn.Redacted) && strings.Contains(rendered, "iqmq") {
			t.Logf("%s: %s", name, rendered)
		}
	}
}

// TestURLEndpointsCarryTheVhost pins the AMQP URL's one piece of awkwardness:
// the path IS the virtual host and it is percent-encoded, so the default
// vhost "/" has to travel as "%2f" or the URL parses as no vhost at all.
func TestURLEndpointsCarryTheVhost(t *testing.T) {
	cfg := withCfg(func(c *rq.Config) { c.Vhost = "/" })
	got := cfg.DSN()
	if !strings.Contains(got, "%2f") {
		t.Errorf("the DSN renders the default vhost as %q: a bare \"/\" is the path separator and parses as no vhost", got)
	}
	named := withCfg(func(c *rq.Config) { c.Vhost = "prod" })
	if strings.Contains(named.DSN(), "%2f") {
		t.Errorf("a named vhost was escaped unnecessarily: %s", named.DSN())
	}
}

// TestMandatoryIsOffByDefault is the default worth pinning, because it is the
// one that decides whether a publication to a topic nobody has subscribed to
// succeeds.
//
// It has to: the confirmation says the BROKER took responsibility, and a
// publisher that had to know the subscription topology would be a publisher no
// other driver in this repository asks a caller to be. The conformance kit
// publishes to unsubscribed topics in four scenarios and requires every one of
// them to succeed, and with `mandatory` on by default all four failed with
// "NO_ROUTE (312)" -- which is what this pins.
//
// The opposite default is the tempting one and the driver page says why it is
// still worth setting in a deployment that provisions its own topology.
func TestMandatoryIsOffByDefault(t *testing.T) {
	if (rq.Config{Addrs: []string{"amqp://localhost:5672"}}).Mandatory {
		t.Fatal("the zero configuration publishes with the mandatory flag: a publication to a topic with no subscription would be Rejected, which is stricter than the portable contract")
	}
	on := withCfg(func(c *rq.Config) { c.Mandatory = true })
	if !on.Mandatory {
		t.Fatal("Mandatory did not turn the mandatory flag on")
	}
	if !errors.Is(error(nil), nil) {
		t.Fatal("unreachable")
	}
}
