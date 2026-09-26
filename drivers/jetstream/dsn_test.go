package jetstream_test

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/dsn"
)

// TestParseDSN holds the driver's parameter table.
func TestParseDSN(t *testing.T) {
	cfg, err := nq.ParseDSN("natsjs://user:pass@host-1:4222,host-2/acme?" +
		"stream=ORDERS&streamsubjects=acme.%3E%3Biqmqsched.ORDERS.%3E&managestream=true&storage=memory" +
		"&replicas=3&maxage=1h&maxmsgs=10&maxbytes=1048576&duplicatewindow=5m" +
		"&consumerprefix=svc-&ackwait=45s&maxdeliver=7&inactivethreshold=2m" +
		"&deadlettertopic=orders%2Fdlq&schedulesubject=sched&doubleack=true" +
		"&publishasyncmaxpending=64&publishtimeout=3s&apiprefix=%24JS.acme.API" +
		"&pendingmessages=2048&pendingbytes=4096&connecttimeout=2s&reconnectwait=1s" +
		"&maxreconnects=-1&pinginterval=30s&maxpingsout=2&draintimeout=9s&name=svc")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if got := strings.Join(cfg.Addrs, ","); got != "nats://host-1:4222,nats://host-2:4222" {
		t.Errorf("addrs = %q; an endpoint that names no port is dialled on %s", got, nq.DefaultPort)
	}
	for _, c := range []struct {
		name string
		got  any
		want any
	}{
		{"username", cfg.Username, "user"},
		{"password", string(cfg.Password), "pass"},
		{"subject prefix", cfg.SubjectPrefix, "acme"},
		{"stream", cfg.Stream, "ORDERS"},
		{"manage stream", cfg.ManageStream, true},
		{"storage", cfg.Storage, nq.StorageMemory},
		{"replicas", cfg.Replicas, 3},
		{"max age", cfg.MaxAge, time.Hour},
		{"max msgs", cfg.MaxMsgs, int64(10)},
		{"max bytes", cfg.MaxBytes, int64(1 << 20)},
		{"duplicate window", cfg.DuplicateWindow, 5 * time.Minute},
		{"consumer prefix", cfg.ConsumerPrefix, "svc-"},
		{"ack wait", cfg.AckWait, 45 * time.Second},
		{"max deliver", cfg.MaxDeliver, 7},
		{"inactive threshold", cfg.ConsumerInactiveThreshold, 2 * time.Minute},
		{"dead letter topic", cfg.DeadLetterTopic, "orders/dlq"},
		{"schedule subject", cfg.ScheduleSubject, "sched"},
		{"double ack", cfg.DoubleAck, true},
		{"async pending", cfg.PublishAsyncMaxPending, 64},
		{"publish timeout", cfg.PublishTimeout, 3 * time.Second},
		{"api prefix", cfg.APIPrefix, "$JS.acme.API"},
		{"pending messages", cfg.PendingMessages, 2048},
		{"pending bytes", cfg.PendingBytes, 4096},
		{"connect timeout", cfg.ConnectTimeout, 2 * time.Second},
		{"reconnect wait", cfg.ReconnectWait, time.Second},
		{"max reconnects", cfg.MaxReconnects, -1},
		{"ping interval", cfg.PingInterval, 30 * time.Second},
		{"max pings out", cfg.MaxPingsOut, 2},
		{"drain timeout", cfg.DrainTimeout, 9 * time.Second},
		{"name", cfg.Name, "svc"},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if got := strings.Join(cfg.StreamSubjects, "|"); got != "acme.>|iqmqsched.ORDERS.>" {
		t.Errorf("stream subjects = %q; \";\" separates them because \",\" separates the endpoints", got)
	}
}

// TestParseDSNRefusals holds what the parser will not accept. The one that earns
// its keep is the unknown parameter: a typo'd DSN is a construction error naming
// it, where the alternative every URL-based configuration in the wild ships is a
// silently ignored setting (ADR-010).
func TestParseDSNRefusals(t *testing.T) {
	for _, c := range []struct{ name, in string }{
		{"wrong scheme", "nats://host:4222"},
		{"unknown parameter", "natsjs://host:4222?strem=ORDERS"},
		{"unknown parameter beside known ones", "natsjs://host:4222?stream=A&doubleack=true&nonsense=1"},
		{"bool that is not true or false", "natsjs://host:4222?doubleack=1"},
		{"storage outside the enumeration", "natsjs://host:4222?storage=disk"},
		{"duration that is not a duration", "natsjs://host:4222?ackwait=soon"},
		{"integer that is not an integer", "natsjs://host:4222?replicas=three"},
		{"insecure without tls", "natsjs://host:4222?insecureskipverify=true"},
		{"stream name with a dot", "natsjs://host:4222?stream=a.b"},
		{"managed stream with no subjects", "natsjs://host:4222?managestream=true"},
		{"not a dsn", "://"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := nq.ParseDSN(c.in); err == nil {
				t.Fatalf("ParseDSN(%q) returned no error", c.in)
			}
		})
	}
}

func TestParseDSNRefusesZeroWherePositiveIsMeant(t *testing.T) {
	for _, key := range []string{
		"replicas", "maxdeliver", "publishasyncmaxpending",
		"pendingmessages", "pendingbytes", "maxpingsout",
	} {
		for _, val := range []string{"0", "-1"} {
			in := "natsjs://host:4222?" + key + "=" + val
			_, err := nq.ParseDSN(in)
			if err == nil {
				t.Errorf("ParseDSN(%q) accepted a value the struct reads as \"not set\"", in)
				continue
			}
			if !strings.Contains(err.Error(), key) {
				t.Errorf("the refusal of %q does not name the parameter: %v", in, err)
			}
		}
	}
	// maxreconnects is the exception and it is deliberate: a negative value
	// there MEANS something ("for ever"), so it is read with the plain
	// accessor.
	cfg, err := nq.ParseDSN("natsjs://host:4222?maxreconnects=-1")
	if err != nil {
		t.Fatalf("maxreconnects=-1: %v", err)
	}
	if cfg.MaxReconnects != -1 {
		t.Errorf("maxreconnects = %d, want -1", cfg.MaxReconnects)
	}
}

// TestRegisterAndOpenDSN holds the registration surface: a driver registers with
// a registry the caller owns and never with a global one of its own (ADR-012).
func TestRegisterAndOpenDSN(t *testing.T) {
	var r dsn.Registry
	if err := nq.Register(&r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := nq.Register(nil); err == nil {
		t.Error("Register(nil) returned no error")
	}
	if _, err := r.Open(t.Context(), "nats://127.0.0.1:4222"); err == nil {
		t.Error("the registry opened a DSN whose scheme belongs to the Core driver")
	}
	name := streamName(t)
	c, err := r.Open(t.Context(), "natsjs://"+strings.TrimPrefix(sharedAddr, "nats://")+
		"/"+strings.ToLower(name)+"?stream="+name+"&managestream=true")
	if err != nil {
		t.Fatalf("opening through the registry: %v", err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := c.Info().Name; got != nq.DriverName {
		t.Errorf("driver = %q, want %q", got, nq.DriverName)
	}
}

// TestDSNRoundTripIsNotOne states the deliberate asymmetry: the rendering drops
// the credential, so it is not a round trip and must not be read as one — but
// what comes back is still a DSN this parser accepts.
func TestDSNRoundTripIsNotOne(t *testing.T) {
	cfg := nq.Config{
		Addrs:    []string{"nats://host:4222"},
		Username: "u",
		Password: dsn.Secret("p"),
		Stream:   "ORDERS",
	}
	back, err := nq.ParseDSN(cfg.DSN())
	if err != nil {
		t.Fatalf("the rendered DSN does not parse: %v (%s)", err, cfg.DSN())
	}
	if back.Password == cfg.Password {
		t.Fatal("the rendered DSN carried the password through a round trip")
	}
	if back.Stream != "ORDERS" {
		t.Errorf("the round trip lost the stream: %q", back.Stream)
	}
	if mq.CodeOf(err) != mq.CodeUnknown {
		t.Errorf("unexpected code %v", mq.CodeOf(err))
	}
}
