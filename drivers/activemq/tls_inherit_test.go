package activemq_test

import (
	"crypto/tls"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	amq "bitbucket.org/iqhive/iqmq/drivers/activemq"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// TestATLSConfigCannotSitBesideAPlaintextScheme pins the refusal this driver
// INHERITS rather than owns.
//
// go-amqp reads the transport from the address scheme, so a *tls.Config beside
// an "amqp://" address does not make the connection TLS -- measured on the
// wire in drivers/amqp10. Validate here ends in `return cc.Validate()` on the
// translated core config, so the refusal arrives by delegation; nothing but
// this test notices if that delegation is replaced by a check of its own that
// forgets the scheme.
//
// The credential-free row is the one that matters: with a Username and
// Password set the SASL PLAIN guard refuses this pairing too, so a credentialed
// row alone stays green even when the scheme check is gone.
func TestATLSConfigCannotSitBesideAPlaintextScheme(t *testing.T) {
	t.Parallel()
	for _, lying := range []amq.Config{
		{
			Broker: activemq.Artemis, Addrs: []string{"amqp://h:5672"},
			TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		},
		{
			Broker: activemq.Artemis, Addrs: []string{"amqp://h:5672"},
			Username: "u", Password: "s3cret",
			TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	} {
		c := lying
		err := c.Validate()
		if err == nil {
			t.Fatalf("a TLS config beside an explicit amqp:// address validated (Username=%q); go-amqp dials "+
				"that in the clear, so anything on the connection goes out unprotected", c.Username)
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("Validate = %v (code %v), want an InvalidConfig refusal", err, mq.CodeOf(err))
		}
		if !strings.Contains(err.Error(), `"amqp://"`) {
			t.Errorf("Validate(Username=%q) = %v, want the refusal that names the amqp:// scheme", c.Username, err)
		}
	}
	// The coherent spellings still validate.
	for _, cfg := range []amq.Config{
		{
			Broker: activemq.Artemis, Addrs: []string{"amqps://h:5671"},
			TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		},
		{Broker: activemq.Artemis, Addrs: []string{"amqps://h:5671"}},
		{
			Broker: activemq.Artemis, Addrs: []string{"h:5672"},
			TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		},
		{Broker: activemq.Artemis, Addrs: []string{"amqp://h:5672"}},
	} {
		c := cfg
		if err := c.Validate(); err != nil {
			t.Errorf("Validate(%v, TLS=%v) = %v", c.Addrs, c.TLS != nil, err)
		}
	}
}
