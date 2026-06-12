package kafka_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/nack"
	"bitbucket.org/iqhive/iqmq/driverkit/offset"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestParseDSN is the grammar and every parameter, including the one thing a
// connection string must never do: ignore something the operator wrote.
func TestParseDSN(t *testing.T) {
	for _, tc := range []struct {
		name  string
		in    string
		check func(*testing.T, kq.Config)
		fail  bool
	}{
		{
			name: "minimal",
			in:   "kafka://kafka-1:9092",
			check: func(t *testing.T, c kq.Config) {
				if len(c.Brokers) != 1 || c.Brokers[0] != "kafka-1:9092" {
					t.Fatalf("brokers = %v", c.Brokers)
				}
			},
		},
		{
			name: "default port",
			in:   "kafka://kafka-1",
			check: func(t *testing.T, c kq.Config) {
				if c.Brokers[0] != "kafka-1:9092" {
					t.Fatalf("brokers = %v; a DSN endpoint with no port is dialled on %s", c.Brokers, kq.DefaultPort)
				}
			},
		},
		{
			name: "several brokers and a prefix",
			in:   "kafka://a:9092,b:9092,c:9092/acme.",
			check: func(t *testing.T, c kq.Config) {
				if len(c.Brokers) != 3 || c.TopicPrefix != "acme." {
					t.Fatalf("brokers = %v prefix = %q", c.Brokers, c.TopicPrefix)
				}
			},
		},
		{
			name: "credentials",
			in:   "kafka://svc:pw@a:9092?sasl=scram-sha-512",
			check: func(t *testing.T, c kq.Config) {
				if c.Username != "svc" || c.Password.Reveal() != "pw" || c.SASL != kq.SASLScramSHA512 {
					t.Fatalf("credentials = %q/%q/%v", c.Username, c.Password.Reveal(), c.SASL)
				}
			},
		},
		{
			name: "tls",
			in:   "kafka://a:9092?tls=true",
			check: func(t *testing.T, c kq.Config) {
				if c.TLS == nil || c.InsecureSkipVerify {
					t.Fatalf("tls = %v insecure = %v", c.TLS != nil, c.InsecureSkipVerify)
				}
			},
		},
		{
			name: "insecure with tls",
			in:   "kafka://a:9092?tls=true&insecureskipverify=true",
			check: func(t *testing.T, c kq.Config) {
				if c.TLS == nil || !c.InsecureSkipVerify || !c.TLS.InsecureSkipVerify {
					t.Fatalf("tls = %v insecure = %v", c.TLS != nil, c.InsecureSkipVerify)
				}
			},
		},
		{
			name: "variant kinds",
			in:   "kafka://a:9092?tracker=segmented&nack=retrytopic&retries=4",
			check: func(t *testing.T, c kq.Config) {
				if c.Tracker != offset.Segmented || c.Nack != nack.RetryTopic || c.NackConfig.Retries != 4 {
					t.Fatalf("tracker = %v nack = %v retries = %d", c.Tracker, c.Nack, c.NackConfig.Retries)
				}
			},
		},
		{
			name: "timings and bounds",
			in:   "kafka://a:9092?linger=20ms&commitinterval=2s&revokewait=7s&fetchmaxbytes=1048576&maxbufferedrecords=500&batchmaxbytes=65536",
			check: func(t *testing.T, c kq.Config) {
				if c.Linger != 20*time.Millisecond || c.CommitInterval != 2*time.Second || c.RevokeWait != 7*time.Second {
					t.Fatalf("timings = %v/%v/%v", c.Linger, c.CommitInterval, c.RevokeWait)
				}
				if c.FetchMaxBytes != 1<<20 || c.MaxBufferedRecords != 500 || c.BatchMaxBytes != 1<<16 {
					t.Fatalf("bounds = %d/%d/%d", c.FetchMaxBytes, c.MaxBufferedRecords, c.BatchMaxBytes)
				}
			},
		},
		{
			name: "group timings and static membership",
			in:   "kafka://a:9092?sessiontimeout=30s&rebalancetimeout=45s&heartbeatinterval=2s&instanceid=pod-7&readcommitted=true&transactionalid=tx-1",
			check: func(t *testing.T, c kq.Config) {
				if c.InstanceID != "pod-7" || !c.ReadCommitted || c.TransactionalID != "tx-1" {
					t.Fatalf("membership = %q/%v/%q", c.InstanceID, c.ReadCommitted, c.TransactionalID)
				}
			},
		},
		{"wrong scheme", "natsjs://a:4222", nil, true},
		{"unknown parameter", "kafka://a:9092?capacty=10", nil, true},
		{"unknown tracker kind", "kafka://a:9092?tracker=nonesuch", nil, true},
		{"unknown nack kind", "kafka://a:9092?nack=nonesuch", nil, true},
		{"unknown sasl mechanism", "kafka://a:9092?sasl=kerberos", nil, true},
		{"insecure without tls", "kafka://a:9092?insecureskipverify=true", nil, true},
		{"negative bound", "kafka://a:9092?fetchmaxbytes=-1", nil, true},
		{"bad duration", "kafka://a:9092?linger=soon", nil, true},
		{"bad boolean", "kafka://a:9092?tls=1", nil, true},
		{"custom nack needs a function", "kafka://a:9092?nack=custom", nil, true},
		{"prefix with a slash in it", "kafka://a:9092/acme/orders", nil, true},
		{"too long", "kafka://a:9092?clientid=" + strings.Repeat("x", dsn.MaxLen), nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := kq.ParseDSN(tc.in)
			if tc.fail {
				if err == nil {
					t.Fatalf("ParseDSN(%q) was accepted", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.in, err)
			}
			tc.check(t, cfg)
		})
	}
}

// TestParseDSNIsStrict is ADR-010 a parameter nothing read is a
// construction error naming it, because a DSN that quietly ignored
// "comitinterval=2s" would hand a caller a broker they did not ask for and
// nobody would find it.
func TestParseDSNIsStrict(t *testing.T) {
	_, err := kq.ParseDSN("kafka://a:9092?comitinterval=2s")
	if err == nil {
		t.Fatal("a misspelt parameter was ignored")
	}
	if !strings.Contains(err.Error(), "comitinterval") {
		t.Fatalf("the refusal does not name the parameter: %v", err)
	}
}

// TestRegister is ADR-012: a driver registers with a registry the caller owns
// and never reaches for a global one.
func TestRegister(t *testing.T) {
	r := dsn.NewRegistry()
	if err := kq.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := kq.Register(r); err == nil {
		t.Fatal("registering the same scheme twice was accepted")
	}
	if err := kq.Register(nil); err == nil {
		t.Fatal("Register(nil) was accepted")
	}
	// The registered opener refuses a DSN for another scheme rather than
	// dialling it.
	if _, err := kq.OpenDSN(context.Background(), "nats://127.0.0.1:4222"); !errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Fatalf("OpenDSN with another scheme = %v, want ErrDriverNotRegistered", err)
	}
}

// TestOpenDSNAgainstAFakeCluster proves the registration is wired end to end
// rather than merely accepted: the scheme opens a working client.
func TestOpenDSNAgainstAFakeCluster(t *testing.T) {
	topic := newTopic("dsn")
	c := cluster(t, 1, []string{topic})
	r := dsn.NewRegistry()
	if err := kq.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	url := "kafka://" + strings.Join(c.ListenAddrs(), ",") + "?linger=0s&commitinterval=50ms"
	client, err := r.Open(t.Context(), url)
	if err != nil {
		t.Fatalf("opening %q: %v", url, err)
	}
	defer func() { _ = client.Close() }()
	cons := consumerOn(t, client, topic, "dsn-group")
	publish(t, client, topic, &mq.Message{Body: []byte("via-dsn")})
	d := receiveOne(t, cons, 10*time.Second)
	if string(d.Body) != "via-dsn" {
		t.Fatalf("body %q", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}
