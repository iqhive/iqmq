package snssqs_test

import (
	"log/slog"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/snssqs"
)

// TestACredentialInATypedEndpointIsRefused holds Validate to refusing an
// endpoint that carries user information.
//
// Config.String and Config.LogValue ARE Config.DSN, and the DSN's authority is
// the endpoint's host, so "https://k:s3cret@sns.example.com" put the bytes of
// a password into every rendering and every log record this driver writes --
// The specification forbids a credential in either. The AWS SDK signs with AccessKeyID and
// SecretAccessKey and reads no credential out of the endpoint, so it was a
// credential that leaked and did nothing.
func TestACredentialInATypedEndpointIsRefused(t *testing.T) {
	t.Parallel()
	const secret = "s3cret-not-in-a-log"
	cfg := sq.Config{
		Region:   "eu-west-1",
		Endpoint: "https://k:" + secret + "@sns.example.com",
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatalf("Validate accepted an endpoint carrying a credential; it renders as %q", cfg.DSN())
	}
	if mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("Validate = %v (code %v), want an InvalidConfig refusal", err, mq.CodeOf(err))
	}
	// The refusal must be the one that NAMES the credential, not the generic
	// dial-host guard that also catches an "@". Both refuse this endpoint, so
	// removing the credential check alone left every test green (review round
	// 2's mutation audit) and the operator would be told their host holds a
	// forbidden byte rather than where the key goes.
	if !strings.Contains(err.Error(), "AccessKeyID") {
		t.Errorf("Validate = %v; the refusal should name where the credential goes, not describe the host", err)
	}
	// And the rendering of one, if a caller builds it anyway, never carries
	// the secret. This is the assertion that would have caught the leak: it
	// looks at every rendering surface the specification names.
	for name, got := range map[string]string{
		"DSN":      cfg.DSN(),
		"String":   cfg.String(),
		"LogValue": renderLogValue(cfg),
	} {
		if strings.Contains(got, secret) {
			t.Errorf("%s carries the secret: %s", name, got)
		}
	}
	// An endpoint with no credential still validates.
	ok := sq.Config{Region: "eu-west-1", Endpoint: "https://sns.example.com"}
	if err := ok.Validate(); err != nil {
		t.Errorf("Validate refused a plain endpoint: %v", err)
	}
}

// renderLogValue is the slog record a Config produces, as text.
func renderLogValue(cfg sq.Config) string {
	var b strings.Builder
	slog.New(slog.NewTextHandler(&b, nil)).Info("cfg", "config", cfg)
	return b.String()
}
