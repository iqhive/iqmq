package kafkago_test

import (
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/nack"
	"bitbucket.org/iqhive/iqmq/driverkit/offset"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestParseDSN reads every parameter this driver has, so that a parameter
// added to Config without a DSN entry is visible here rather than in a
// deployment whose connection string was silently ignored.
func TestParseDSN(t *testing.T) {
	cfg, err := kq.ParseDSN("kafkago://u:p@b1:9092,b2/acme.?" + strings.Join([]string{
		"clientid=svc",
		"sasl=scram-sha-256",
		"tls=true",
		"balancer=roundrobin",
		"autocreatetopics=true",
		"readcommitted=true",
		"watchpartitionchanges=true",
		"batchmaxrecords=64",
		"batchtimeout=5ms",
		"batchmaxbytes=131072",
		"maxattempts=5",
		"writetimeout=7s",
		"metadatattl=2s",
		"writebackoffmin=50ms",
		"writebackoffmax=500ms",
		"fetchminbytes=16",
		"fetchmaxbytes=65536",
		"fetchmaxwait=100ms",
		"queuecapacity=32",
		"sessiontimeout=20s",
		"rebalancetimeout=30s",
		"heartbeatinterval=2s",
		"joinbackoff=3s",
		"partitionwatchinterval=9s",
		"commitinterval=1s",
		"revokewait=4s",
		"requesttimeout=11s",
		"partitions=6",
		"replicationfactor=3",
		"tracker=bitset",
		"nack=retrytopic",
		"deadlettertopic=dead",
		"nackmaxattempts=4",
		"retries=2",
		"nackdelay=250ms",
	}, "&"))
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if got, want := strings.Join(cfg.Brokers, ","), "b1:9092,b2:9092"; got != want {
		t.Errorf("Brokers = %q, want %q: an endpoint with no port is dialled on 9092", got, want)
	}
	if cfg.TopicPrefix != "acme." {
		t.Errorf("TopicPrefix = %q", cfg.TopicPrefix)
	}
	if cfg.Username != "u" || cfg.Password.Reveal() != "p" {
		t.Errorf("credential = %q/%q", cfg.Username, cfg.Password.Reveal())
	}
	for _, c := range []struct {
		name string
		got  any
		want any
	}{
		{"ClientID", cfg.ClientID, "svc"},
		{"SASL", cfg.SASL, kq.SASLScramSHA256},
		{"Balancer", cfg.Balancer, kq.BalancerRoundRobin},
		{"AutoCreateTopics", cfg.AutoCreateTopics, true},
		{"ReadCommitted", cfg.ReadCommitted, true},
		{"WatchPartitionChanges", cfg.WatchPartitionChanges, true},
		{"BatchMaxRecords", cfg.BatchMaxRecords, 64},
		{"BatchTimeout", cfg.BatchTimeout, 5 * time.Millisecond},
		{"BatchMaxBytes", cfg.BatchMaxBytes, int64(131072)},
		{"MaxAttempts", cfg.MaxAttempts, 5},
		{"WriteTimeout", cfg.WriteTimeout, 7 * time.Second},
		{"MetadataTTL", cfg.MetadataTTL, 2 * time.Second},
		{"WriteBackoffMin", cfg.WriteBackoffMin, 50 * time.Millisecond},
		{"WriteBackoffMax", cfg.WriteBackoffMax, 500 * time.Millisecond},
		{"FetchMinBytes", cfg.FetchMinBytes, 16},
		{"FetchMaxBytes", cfg.FetchMaxBytes, 65536},
		{"FetchMaxWait", cfg.FetchMaxWait, 100 * time.Millisecond},
		{"QueueCapacity", cfg.QueueCapacity, 32},
		{"SessionTimeout", cfg.SessionTimeout, 20 * time.Second},
		{"RebalanceTimeout", cfg.RebalanceTimeout, 30 * time.Second},
		{"HeartbeatInterval", cfg.HeartbeatInterval, 2 * time.Second},
		{"JoinBackoff", cfg.JoinBackoff, 3 * time.Second},
		{"PartitionWatchInterval", cfg.PartitionWatchInterval, 9 * time.Second},
		{"CommitInterval", cfg.CommitInterval, time.Second},
		{"RevokeWait", cfg.RevokeWait, 4 * time.Second},
		{"RequestTimeout", cfg.RequestTimeout, 11 * time.Second},
		{"Partitions", cfg.Partitions, int32(6)},
		{"ReplicationFactor", cfg.ReplicationFactor, int16(3)},
		{"Tracker", cfg.Tracker, offset.Bitset},
		{"Nack", cfg.Nack, nack.RetryTopic},
		{"DeadLetterTopic", cfg.NackConfig.DeadLetterTopic, "dead"},
		{"NackMaxAttempts", cfg.NackConfig.MaxAttempts, uint32(4)},
		{"Retries", cfg.NackConfig.Retries, 2},
		{"NackDelay", cfg.NackConfig.Delay, 250 * time.Millisecond},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if cfg.TLS == nil {
		t.Error("tls=true did not turn TLS on")
	}
}

// TestParseDSNRefuses is ADR-010 a parameter this driver does not
// have is a construction error naming it, and not a setting nobody notices was
// ignored.
func TestParseDSNRefuses(t *testing.T) {
	for _, tc := range []struct{ name, dsnStr string }{
		{"wrong-scheme", "kafka://host:9092"},
		{"unknown-parameter", "kafkago://host:9092?nosuchthing=1"},
		{"unknown-sasl", "kafkago://u:p@host:9092?sasl=gssapi"},
		{"unknown-balancer", "kafkago://host:9092?balancer=cooperative-sticky"},
		{"unknown-tracker", "kafkago://host:9092?tracker=nosuchkind"},
		{"unknown-nack", "kafkago://host:9092?nack=nosuchkind"},
		{"insecure-without-tls", "kafkago://host:9092?insecureskipverify=true"},
		{"bad-duration", "kafkago://host:9092?commitinterval=soon"},
		{"bad-topic-prefix", "kafkago://host:9092/a%2Fb"},
		// The three the franz-go driver has and this one does not. They are
		// refused by name, which is the whole value of Strict: a caller
		// porting a connection string between the two drivers is told, rather
		// than getting a client with no idempotence it asked to disable.
		{"disableidempotency", "kafkago://host:9092?disableidempotency=true"},
		{"transactionalid", "kafkago://host:9092?transactionalid=tx"},
		{"instanceid", "kafkago://host:9092?instanceid=pod-0"},
		{"unboundeddelivery", "kafkago://host:9092?unboundeddelivery=true"},
		{"linger", "kafkago://host:9092?linger=5ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := kq.ParseDSN(tc.dsnStr); err == nil {
				t.Fatalf("ParseDSN accepted %q", tc.dsnStr)
			}
		})
	}
}

// TestRegisterUsesItsOwnScheme is what keeps the two Kafka drivers apart in a
// program that wants both: they register different schemes, so one registry
// can hold them at once and a DSN says which client it means.
func TestRegisterUsesItsOwnScheme(t *testing.T) {
	var r dsn.Registry
	if err := kq.Register(&r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := kq.Register(nil); err == nil {
		t.Error("Register accepted a nil Registrar")
	}
	if _, err := r.Open(t.Context(), "kafka://host:9092"); err == nil {
		t.Error("the registry opened a kafka:// DSN after only kafkago was registered")
	} else if mq.CodeOf(err) != mq.CodeDriverNotRegistered {
		t.Logf("opening a kafka:// DSN reported %v (%v)", err, mq.CodeOf(err))
	}
}
