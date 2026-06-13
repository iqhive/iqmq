package nats_test

import (
	"crypto/tls"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nats"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestValidate is the specification's "validate config before starting background work": every
// refusal below happens with no I/O at all, so a typo is immediate rather than
// a dial timeout.
func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     nq.Config
		wantErr bool
	}{
		{name: "zero value", cfg: nq.Config{}},
		{name: "an address", cfg: nq.Config{Addrs: []string{"nats://h:4222"}}},
		{name: "empty address", cfg: nq.Config{Addrs: []string{"  "}}, wantErr: true},
		{name: "address with a control byte", cfg: nq.Config{Addrs: []string{"h\x01"}}, wantErr: true},
		{name: "address that is not ASCII", cfg: nq.Config{Addrs: []string{"hö"}}, wantErr: true},
		{name: "name with a control byte", cfg: nq.Config{Name: "a\nb"}, wantErr: true},
		{name: "prefix that is a subject", cfg: nq.Config{SubjectPrefix: "acme.eu"}},
		{name: "prefix that is a wildcard", cfg: nq.Config{SubjectPrefix: "acme.*"}, wantErr: true},
		{name: "negative pending limit", cfg: nq.Config{PendingMessages: -1}, wantErr: true},
		{name: "negative timeout", cfg: nq.Config{ConnectTimeout: -time.Second}, wantErr: true},
		{name: "password without a user", cfg: nq.Config{Password: dsn.Secret("p")}, wantErr: true},
		{name: "user and token", cfg: nq.Config{Username: "u", Token: dsn.Secret("t")}, wantErr: true},
		{name: "token and credentials", cfg: nq.Config{Token: dsn.Secret("t"), CredentialsFile: "/x"}, wantErr: true},
		{name: "insecure without TLS", cfg: nq.Config{InsecureSkipVerify: true}, wantErr: true},
		{name: "insecure with TLS", cfg: nq.Config{InsecureSkipVerify: true, TLS: &tls.Config{MinVersion: tls.VersionTLS12}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("Validate code = %v, want InvalidConfig", mq.CodeOf(err))
			}
		})
	}
}

// TestOpenRefusesBeforeDialling is the other half: an invalid configuration
// never reaches the network, which is what makes Validate worth having.
func TestOpenRefusesBeforeDialling(t *testing.T) {
	// An address that would take the full connect timeout to fail, with a
	// configuration error in front of it.
	cfg := nq.Config{
		Addrs:          []string{"nats://198.51.100.1:4222"},
		ConnectTimeout: 30 * time.Second,
		SubjectPrefix:  "not a subject",
	}
	start := time.Now()
	if _, err := nq.Open(t.Context(), cfg); err == nil {
		t.Fatal("an invalid configuration was accepted")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Open took %v: it dialled before validating", elapsed)
	}
}
