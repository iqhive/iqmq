package snssqs

import (
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestEveryInnerRefusalNamesThisDriver is.
//
// `Config.Validate` ends by validating the configuration the inner SQS driver
// will be built with, which is deliberate: it keeps the refusals from having a
// second copy here to keep in step. What it also did was report 28 of them under
// the inner driver's name -- `iqmq/sqs: MaxMessages cannot exceed the service's
// ten messages per ReceiveMessage`, about a field the caller set on an
// `snssqs.Config`, naming a driver their code never mentions. `sqsConfig`
// forwards every one of those fields one for one, so the setting a refusal is
// about is always the caller's own.
//
// The test is an INTERNAL one on purpose. It asks both questions of the same
// configuration -- what the inner driver says, and what this driver says -- which
// is what makes each row prove the mechanism rather than an example of it: the
// inner error must carry the inner prefix, the outer must carry this driver's,
// and the two must be the same error by identity and code.
//
// The rename substitutes the PREFIX and nothing else, so it covers every refusal
// the inner driver can make rather than a list that would go stale. The rows
// below are one per distinct inner rule reachable through a forwarded field, so
// a rename that worked for one shape and not another fails here.
func TestEveryInnerRefusalNamesThisDriver(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"a negative poller count":             {Region: "eu-west-1", Pollers: -1},
		"too many messages per receive":       {Region: "eu-west-1", MaxMessages: 11},
		"too many entries per settle batch":   {Region: "eu-west-1", SettleMaxItems: 11},
		"a negative duration":                 {Region: "eu-west-1", SettleInterval: -time.Second},
		"a long poll past twenty seconds":     {Region: "eu-west-1", WaitTime: 21 * time.Second},
		"an ack deadline past twelve hours":   {Region: "eu-west-1", AckDeadline: 13 * time.Hour},
		"a secret with no access key id":      {Region: "eu-west-1", SecretAccessKey: "s"},
		"an access key id with no secret":     {Region: "eu-west-1", AccessKeyID: "k"},
		"an endpoint with no scheme":          {Region: "eu-west-1", Endpoint: "localhost:4566"},
		"an http endpoint left implicit":      {Region: "eu-west-1", Endpoint: "http://localhost:4566"},
		"insecure transport with no endpoint": {Region: "eu-west-1", InsecureTransport: true},
		"a redrive count with no queue":       {Region: "eu-west-1", MaxReceiveCount: 3},
	} {
		inner := cfg.sqsConfig().Validate()
		if inner == nil {
			t.Errorf("%s: the inner SQS configuration was ACCEPTED, so this row measures nothing; the "+
				"forwarding in sqsConfig or the inner rule has changed", name)
			continue
		}
		if !strings.HasPrefix(inner.Error(), innerRefusalPrefix) {
			t.Errorf("%s: the inner refusal is %q, which does not begin with %q -- the rename substitutes that "+
				"prefix, so a refusal without it is passed through and reaches a caller unnamed",
				name, inner, innerRefusalPrefix)
			continue
		}
		outer := cfg.Validate()
		if outer == nil {
			t.Errorf("%s: the inner driver refused %v and this driver accepted the same configuration", name, inner)
			continue
		}
		if !strings.HasPrefix(outer.Error(), ownRefusalPrefix) {
			t.Errorf("%s: Validate = %q, which does not begin with %q; a caller of THIS driver set the field, so "+
				"this is the driver they have to go and look at", name, outer, ownRefusalPrefix)
		}
		if strings.Contains(outer.Error(), "iqmq/"+innerDriverName+":") {
			t.Errorf("%s: Validate = %q, which still names the inner driver", name, outer)
		}
		if !errors.Is(outer, inner) {
			t.Errorf("%s: Validate = %v, which is not errors.Is the inner refusal %v; the rename must keep the "+
				"sentinel reachable or every caller matching on the inner driver's errors breaks", name,
				outer, inner)
		}
		if got, want := mq.CodeOf(outer), mq.CodeOf(inner); got != want {
			t.Errorf("%s: Validate's code is %v and the inner refusal's is %v; the wrapper must carry no Code of "+
				"its own so that mq.CodeOf walks to the inner error", name, got, want)
		}
		if mq.CodeOf(outer) != mq.CodeInvalidConfig {
			t.Errorf("%s: Validate's code is %v, want InvalidConfig", name, mq.CodeOf(outer))
		}
	}
}

// innerDriverName is the inner driver's registered name, spelled here rather
// than imported so that the assertion above reads the string a caller would see
// even if the constant moved.
const innerDriverName = "sqs"

// TestARefusalOfThisDriversOwnIsUntouched is the other direction: the rename
// must not reach a refusal this driver made itself, and must not reach an error
// that carries no prefix at all.
//
// The second half is what keeps the rename honest rather than cosmetic: it
// substitutes a prefix it FOUND, so an error shaped unlike the inner driver's
// passes through unchanged instead of acquiring a name that is a guess.
func TestARefusalOfThisDriversOwnIsUntouched(t *testing.T) {
	t.Parallel()
	own := Config{Region: "eu-west-1", MaxMessageBytes: -1}.Validate()
	if own == nil {
		t.Fatal("a negative MaxMessageBytes was accepted; this row measures nothing")
	}
	if !strings.HasPrefix(own.Error(), ownRefusalPrefix) {
		t.Errorf("this driver's own refusal is %q, want the %q prefix", own, ownRefusalPrefix)
	}
	var wrapped innerRefusal
	if errors.As(own, &wrapped) {
		t.Errorf("this driver's own refusal was wrapped as an inner one: %q", own)
	}
	plain := errors.New("something with no driver prefix")
	// IDENTITY is the assertion here, so the comparison is `!=` and not
	// errors.Is: an error carrying no prefix has to come back UNCHANGED, and
	// errors.Is would be satisfied by a wrapper, which is exactly the outcome
	// this line exists to rule out.
	//nolint:errorlint // an identity comparison is the assertion; errors.Is cannot tell a wrapper from the error itself
	if got := renameInnerRefusal(plain); got != plain {
		t.Errorf("renameInnerRefusal rewrote an error carrying no prefix: %q", got)
	}
	if renameInnerRefusal(nil) != nil {
		t.Error("renameInnerRefusal turned a nil error into a non-nil one")
	}
}
