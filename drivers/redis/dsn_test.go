package redis_test

import (
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	rd "bitbucket.org/iqhive/iqmq/drivers/redis"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// TestParseDSN covers the parameter table and the two enumerations.
func TestParseDSN(t *testing.T) {
	// One address, because the database number and a cluster are mutually
	// exclusive and the parser says so; the two-address case is in the
	// refusals below.
	cfg, err := rd.ParseDSN("redis://acme:hunter2@db1:6380/3?" +
		"partitions=4&maxlen=10000&autocreate=true&ackdeadline=45s&claiminterval=10s&" +
		"claimcount=64&block=2s&nack=zsetmover&ackguard=none&consumerprefix=worker&" +
		"moverinterval=100ms&movercount=32&durablelocal=1&durablereplicas=2&" +
		"durabletimeout=3s&dialtimeout=1s&deadlettersuffix=:dead&keyprefix=iq.")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if len(cfg.Addrs) != 1 || cfg.Addrs[0] != "db1:6380" {
		t.Fatalf("Addrs = %v", cfg.Addrs)
	}
	// An endpoint that names no port is dialled on DefaultPort, which is the
	// convenience a connection string is for.
	seeds, err := rd.ParseDSN("redis://db1,db2:7000?nack=readd")
	if err != nil {
		t.Fatalf("ParseDSN with two seeds: %v", err)
	}
	if len(seeds.Addrs) != 2 || seeds.Addrs[0] != "db1:6379" || seeds.Addrs[1] != "db2:7000" {
		t.Fatalf("cluster seeds = %v", seeds.Addrs)
	}
	if cfg.Username != "acme" || cfg.Password.Reveal() != "hunter2" {
		t.Fatalf("credentials = %q / %q", cfg.Username, cfg.Password.Reveal())
	}
	if cfg.DB != 3 {
		t.Fatalf("DB = %d, want 3 (the DSN path is the logical database)", cfg.DB)
	}
	want := rd.Config{
		Partitions: 4, MaxLen: 10000, AutoCreate: true,
		AckDeadline: 45 * time.Second, ClaimInterval: 10 * time.Second, ClaimCount: 64,
		Block: 2 * time.Second, Nack: streams.ZSetMover, AckGuard: streams.GuardNone,
		ConsumerPrefix: "worker", MoverInterval: 100 * time.Millisecond, MoverCount: 32,
		DurableLocal: 1, DurableReplicas: 2, DurableTimeout: 3 * time.Second,
		DialTimeout: time.Second, DeadLetterSuffix: ":dead", KeyPrefix: "iq.",
	}
	switch {
	case cfg.Partitions != want.Partitions:
		t.Fatalf("Partitions = %d", cfg.Partitions)
	case cfg.MaxLen != want.MaxLen:
		t.Fatalf("MaxLen = %d", cfg.MaxLen)
	case !cfg.AutoCreate:
		t.Fatal("AutoCreate = false")
	case cfg.AckDeadline != want.AckDeadline:
		t.Fatalf("AckDeadline = %v", cfg.AckDeadline)
	case cfg.ClaimInterval != want.ClaimInterval:
		t.Fatalf("ClaimInterval = %v", cfg.ClaimInterval)
	case cfg.ClaimCount != want.ClaimCount:
		t.Fatalf("ClaimCount = %d", cfg.ClaimCount)
	case cfg.Block != want.Block:
		t.Fatalf("Block = %v", cfg.Block)
	case cfg.Nack != want.Nack:
		t.Fatalf("Nack = %v", cfg.Nack)
	case cfg.AckGuard != want.AckGuard:
		t.Fatalf("AckGuard = %v", cfg.AckGuard)
	case cfg.ConsumerPrefix != want.ConsumerPrefix:
		t.Fatalf("ConsumerPrefix = %q", cfg.ConsumerPrefix)
	case cfg.MoverInterval != want.MoverInterval:
		t.Fatalf("MoverInterval = %v", cfg.MoverInterval)
	case cfg.MoverCount != want.MoverCount:
		t.Fatalf("MoverCount = %d", cfg.MoverCount)
	case cfg.DurableReplicas != want.DurableReplicas:
		t.Fatalf("DurableReplicas = %d", cfg.DurableReplicas)
	case cfg.DurableTimeout != want.DurableTimeout:
		t.Fatalf("DurableTimeout = %v", cfg.DurableTimeout)
	case cfg.DialTimeout != want.DialTimeout:
		t.Fatalf("DialTimeout = %v", cfg.DialTimeout)
	case cfg.DeadLetterSuffix != want.DeadLetterSuffix:
		t.Fatalf("DeadLetterSuffix = %q", cfg.DeadLetterSuffix)
	case cfg.KeyPrefix != want.KeyPrefix:
		t.Fatalf("KeyPrefix = %q", cfg.KeyPrefix)
	}
}

// TestParseDSNRefusals is the strict half: a parameter this driver does not
// have is a construction error naming it, rather than a setting nobody
// notices was ignored.
func TestParseDSNRefusals(t *testing.T) {
	cases := []struct {
		name, dsn string
	}{
		{"unknown-parameter", "redis://h:6379?partitons=4"},
		{"wrong-scheme", "rediss://h:6379"},
		{"bad-database", "redis://h:6379/notanumber"},
		{"bad-nack-kind", "redis://h:6379?nack=nosuchstrategy"},
		{"nack-auto-is-not-a-choice", "redis://h:6379?nack=auto"},
		{"bad-ack-guard", "redis://h:6379?ackguard=maybe"},
		{"insecure-without-tls", "redis://h:6379?insecureskipverify=true"},
		{"negative-partitions", "redis://h:6379?partitions=-1"},
		{"brace-in-keyprefix", "redis://h:6379?keyprefix=%7Bx%7D"},
		{"user-without-password", "redis://acme@h:6379"},
		{"database-with-cluster", "redis://a:6379,b:6379/2"},
		{"negative-block", "redis://h:6379?block=-1s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := rd.ParseDSN(tc.dsn); err == nil {
				t.Fatalf("ParseDSN(%q) was accepted; a setting that is ignored silently is worse "+
					"than one that fails at construction", tc.dsn)
			}
		})
	}
}

// TestDSNRoundTripRedacts is the rule: a configuration is safe to log.
//
// It is not a round trip and is not meant to be one — the password is gone
// from the result on purpose — but everything else has to survive, or the
// rendering names a different broker from the one the client is talking to.
func TestDSNRoundTripRedacts(t *testing.T) {
	cfg, err := rd.ParseDSN("redis://acme:hunter2@db1:6380/2?partitions=4&nack=readd&autocreate=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	rendered := cfg.DSN()
	if strings.Contains(rendered, "hunter2") {
		t.Fatalf("the rendered DSN carries the password: %s", rendered)
	}
	if !strings.Contains(rendered, dsn.Redacted) {
		t.Fatalf("the rendered DSN does not mark the redaction: %s", rendered)
	}
	for _, want := range []string{"acme", "db1:6380", "/2", "partitions=4", "nack=readd", "autocreate=true"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("the rendered DSN lost %q: %s", want, rendered)
		}
	}
	// And what comes back parses, which is what makes the rendering usable in
	// a diagnostic rather than only readable.
	again, err := rd.ParseDSN(strings.Replace(rendered, dsn.Redacted, "hunter2", 1))
	if err != nil {
		t.Fatalf("the rendered DSN does not parse: %v (%s)", err, rendered)
	}
	if again.Partitions != 4 || again.Nack != streams.ReAdd || !again.AutoCreate {
		t.Fatalf("the re-parsed configuration differs: %+v", again)
	}
	// The two other rendering surfaces the specification names.
	if strings.Contains(cfg.String(), "hunter2") {
		t.Fatal("String() carries the password")
	}
	if strings.Contains(cfg.LogValue().String(), "hunter2") {
		t.Fatal("LogValue() carries the password")
	}
}

// TestRegisterAndOpenDSN is the registration surface.
func TestRegisterAndOpenDSN(t *testing.T) {
	r := dsn.NewRegistry()
	if err := rd.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, ok := r.Lookup(rd.Scheme); !ok {
		t.Fatalf("the registry does not know %q after Register", rd.Scheme)
	}
	if err := rd.Register(nil); err == nil {
		t.Fatal("Register(nil) was accepted")
	}
	// A DSN whose scheme is not this driver's is refused by the parser rather
	// than dialled: a driver that answered another scheme would let a caller
	// confuse one broker's capabilities with another's.
	if _, err := rd.OpenDSN(t.Context(), "natsjs://h:4222"); err == nil {
		t.Fatal("OpenDSN accepted another driver's scheme")
	}
}

// TestValidateIsPure: Validate performs no I/O, so a typo is an immediate
// refusal rather than a five-second timeout.
//
// "Pure" is asserted the only way it can be: the address is one nothing can
// dial, and Validate still returns promptly with the right answer.
func TestValidateIsPure(t *testing.T) {
	cfg := rd.Config{Addrs: []string{"192.0.2.1:6379"}, Partitions: -1}
	start := time.Now()
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate accepted a negative partition count")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Validate took %v, so it dialled: the specification requires a configuration to be refused "+
			"before any background work", elapsed)
	}
	if code := mq.CodeOf(err); code != mq.CodeInvalidConfig {
		t.Fatalf("Validate's refusal carries code %v, want CodeInvalidConfig", code)
	}
}
