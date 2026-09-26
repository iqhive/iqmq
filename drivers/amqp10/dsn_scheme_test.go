package amqp10_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
)

// TestRegisterCanonicalisesTheScheme holds this core's two scheme-taking entry
// points to the shared grammar's one spelling of a scheme.
//
// A scheme is case-insensitive (RFC 3986): the registry files "MyBroker" under
// "mybroker" and dsn.Parse reads "mybroker" out of "MyBroker://h" and
// "mybroker://h" alike. Register handed the opener the scheme as the CALLER
// spelled it and ParseDSNScheme compared it exactly, so a registry that listed
// "mybroker" answered every DSN of it with ErrDriverNotRegistered, and
// ParseDSNScheme("MyBroker", "MyBroker://…") refused its own scheme.
//
// The DSNs carry a parameter nobody has, so the opener fails at Strict -- after
// the scheme check and before any dial -- and the refusal says which of the two
// it got past.
func TestRegisterCanonicalisesTheScheme(t *testing.T) {
	ctx := context.Background()
	r := dsn.NewRegistry()
	if err := aq.Register(r, "MyBroker", testProfile()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := r.Schemes(); !reflect.DeepEqual(got, []string{"mybroker"}) {
		t.Fatalf("Schemes is %q, want the canonical spelling", got)
	}
	for _, s := range []string{"mybroker://host:5672?nosuch=1", "MyBroker://host:5672?nosuch=1", "MYBROKER://host:5672?nosuch=1"} {
		_, err := r.Open(ctx, s)
		if errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Fatalf("Open(%q) reached the opener and was refused as an unregistered scheme: the registry "+
				"and the opener disagree about what the scheme is", s)
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || !strings.Contains(err.Error(), "nosuch") {
			t.Fatalf("Open(%q) = %v, want the Strict refusal naming nosuch", s, err)
		}
	}
	for _, s := range []string{"mybroker://host:5672", "MyBroker://host:5672"} {
		if _, err := aq.ParseDSNScheme("MyBroker", s); err != nil {
			t.Errorf("ParseDSNScheme(%q, %q) refused its own scheme: %v", "MyBroker", s, err)
		}
	}
	if _, err := aq.ParseDSNScheme("MyBroker", "other://host:5672"); !errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Errorf("ParseDSNScheme accepted another scheme: %v", err)
	}
	// A string that is more than a scheme is not one, on either entry point.
	if err := aq.Register(dsn.NewRegistry(), "a://b", testProfile()); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Errorf("Register(%q) = %v (code %v), want it refused", "a://b", err, mq.CodeOf(err))
	}
	if _, err := aq.ParseDSNScheme("a://b", "a://host:5672"); !errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Errorf("ParseDSNScheme with a scheme of %q = %v, want ErrDriverNotRegistered", "a://b", err)
	}
}
