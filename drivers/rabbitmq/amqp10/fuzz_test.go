package amqp10_test

import (
	"net/url"
	"strings"
	"testing"

	rmq10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/rabbit"
)

// FuzzRabbitAMQP10Address is the address form, and it is this module's sharpest
// subject because the two halves pull in opposite directions.
//
// RabbitMQ 4's AMQP 1.0 address is PATH-shaped —
// `/exchanges/<exchange>/<routing-key>` and `/queues/<queue>` — and a routing
// key is arbitrary caller-supplied text: a topic may legally contain a "/" or a
// "%" as far as `internal/rabbit` is concerned, because RabbitMQ stores the
// bytes. So an unescaped "/" in a name would END A SEGMENT and the link would
// attach to a different node: the topic "orders/eu" would reach the exchange
// "orders" with the routing key "eu", which is somebody else's traffic, and the
// publisher would never know.
//
// Four properties, and the third is the one worth fuzzing for:
//
//  1. nothing panics, whatever a caller names;
//  2. an address this profile produces has exactly the separators the form has,
//     so it names the node it was asked for and no other;
//  3. the escaping is REVERSIBLE and therefore injective: decoding each segment
//     returns the name `internal/rabbit` derived, byte for byte. Two different
//     name pairs cannot produce one address, which is the property that makes
//     "a different node" impossible rather than unlikely;
//  4. a name this broker cannot carry produces no address at all.
func FuzzRabbitAMQP10Address(f *testing.F) {
	f.Add("", "orders", "workers")
	f.Add("acme.", "orders.eu", "workers")
	f.Add("", "orders/eu", "workers")
	f.Add("", "100%", "workers")
	f.Add("", "a%2Fb", "workers")
	f.Add("pre/fix.", "orders", "work_ers")
	f.Add("", "orders.#", "workers")
	f.Add("", "", "")
	f.Add("", "orders", "dlq")
	f.Add("", strings.Repeat("x", 250), "workers")

	f.Fuzz(func(t *testing.T, prefix, topic, subscription string) {
		cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}, Prefix: prefix}
		sink, source, err := rmq10.AddressForTest(cfg, topic, subscription)
		if err != nil {
			// Property 4. A refusal is an answer, and it must not come with
			// an address a caller could use anyway.
			if sink != "" || source != "" {
				t.Fatalf("the derivation refused %q/%q and produced the addresses %q and %q", topic, subscription, sink, source)
			}
			return
		}
		names, derr := rabbit.Derive(rabbit.Config{Prefix: prefix, Topic: topic, Subscription: subscription})
		if derr != nil {
			t.Fatalf("this profile produced the addresses %q and %q for names `internal/rabbit` refuses: %v",
				sink, source, derr)
		}

		// Property 2. The form has three separators and two; an address with
		// more names a node nobody asked for.
		if n := strings.Count(sink, "/"); n != 3 {
			t.Fatalf("the sink address %q has %d separators; /exchanges/<exchange>/<routing-key> has three, so this "+
				"address names a different node", sink, n)
		}
		if n := strings.Count(source, "/"); n != 2 {
			t.Fatalf("the source address %q has %d separators; /queues/<queue> has two", source, n)
		}
		if !strings.HasPrefix(sink, "/exchanges/") {
			t.Fatalf("the sink address %q does not address an exchange", sink)
		}
		if !strings.HasPrefix(source, "/queues/") {
			t.Fatalf("the source address %q does not address a queue", source)
		}

		// Property 3. Decoding the segments returns the derived names, which
		// is what makes the encoding injective: no two name pairs can reach
		// one address.
		exchange, routingKey := splitSink(t, sink)
		if exchange != names.Exchange {
			t.Fatalf("the sink address's exchange segment decodes to %q and `internal/rabbit` derived %q",
				exchange, names.Exchange)
		}
		if routingKey != names.RoutingKey {
			t.Fatalf("the sink address's routing-key segment decodes to %q and `internal/rabbit` derived %q",
				routingKey, names.RoutingKey)
		}
		queue := unescapeSegment(t, strings.TrimPrefix(source, "/queues/"))
		if queue != names.Queue {
			t.Fatalf("the source address's queue segment decodes to %q and `internal/rabbit` derived %q",
				queue, names.Queue)
		}
	})
}

// splitSink splits a publishing address into its two decoded segments.
func splitSink(t *testing.T, sink string) (exchange, routingKey string) {
	t.Helper()
	rest := strings.TrimPrefix(sink, "/exchanges/")
	left, right, ok := strings.Cut(rest, "/")
	if !ok {
		t.Fatalf("the sink address %q has no routing-key segment", sink)
	}
	return unescapeSegment(t, left), unescapeSegment(t, right)
}

// unescapeSegment percent-decodes one address segment with the standard
// library, deliberately NOT with the driver's own code: a decoder written
// against the same misunderstanding as the encoder would agree with it.
func unescapeSegment(t *testing.T, segment string) string {
	t.Helper()
	out, err := url.PathUnescape(segment)
	if err != nil {
		t.Fatalf("the address segment %q is not valid percent-encoding: %v", segment, err)
	}
	return out
}

// FuzzRabbitAMQP10DSN is the connection string, whose two halves have to agree
// about every byte a stranger chose.
//
// [rmq10.Config.DSN] exists so that a configuration can be logged and pasted,
// and a rendering its own parser refuses would make it useless for exactly
// that — which is the failure the shared core recorded for an IPv6 literal and
// for a host containing the "%" that begins an escape. So the property is
// idempotence from the first rendering onwards, plus the one rule a
// rendering must never break: the password is not in it.
func FuzzRabbitAMQP10DSN(f *testing.F) {
	f.Add("rabbitmq+amqp10://rabbit-1:5672")
	f.Add("rabbitmq+amqp10://iqmq:pw@rabbit-1:5672,rabbit-2:5672?vhost=acme&credit=32")
	f.Add("rabbitmq+amqp10://[::1]:5672?queuetype=classic&deadletter=true")
	f.Add("rabbitmq+amqp10://rab%25bit:5672?prefix=acme.")
	f.Add("rabbitmq+amqp10://h:5672?maxpriority=9&nondurable=true&receiversettle=second")
	f.Add("rabbitmq+amqp10://h:5672/acme")
	f.Add("rabbitmq+amqp10://h:5672?declare=true")
	f.Add("rabbitmq://h:5672")

	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := rmq10.ParseDSN(s)
		if err != nil {
			return
		}
		first := cfg.DSN()
		// These two hold for EVERY rendering, so they are checked before the
		// round trip and outside the length bound below.
		if secret := cfg.Password.Reveal(); secret != "" && strings.Contains(first, secret) {
			t.Fatalf("the rendered DSN carries the password.\ninput: %q", s)
		}
		// The rendering is what a person pastes, so it has to be printable:
		// a parameter value comes off the wire after percent decoding and can
		// hold any byte at all.
		for i := range len(first) {
			if c := first[i]; c < 0x20 || c == 0x7f {
				t.Fatalf("the rendered DSN carries a control byte at offset %d.\ninput: %q", i, s)
			}
		}
		// A rendering is LONGER than the DSN it came from: it writes out every
		// default the parse filled in — 122 bytes of them for a one-endpoint
		// configuration — and escapes what the authority cannot carry
		// literally. So a DSN accepted within 122 bytes of
		// [dsn.MaxLen] renders past it, and `ParseDSN` refuses what is past it
		// by design, bounding what it will read at all. That is the length
		// bound doing its job and not a round-trip failure: a configuration
		// whose rendering does not fit has NO rendering this parser could
		// accept, whatever the renderer did. The round trip is therefore a
		// claim about renderings that fit, exactly as the shared core's own
		// target for the same property states it (`dsn/fuzz_test.go:109-118`).
		//
		// This was issue #67, found with a 3,626-byte host of "=" that escaping
		// tripled; `make fuzz-seeds` replays it as `e5290fc400a6c047`. The
		// overshoot is NOT an escaping artefact — a maximal DSN of plain
		// letters, nothing escaped at all, renders 122 bytes over — so there is
		// no renderer fix hiding behind this guard.
		if len(first) > dsn.MaxLen {
			return
		}
		again, err := rmq10.ParseDSN(first)
		if err != nil {
			t.Fatalf("the DSN this driver rendered does not parse back: %v\ninput:    %q\nrendered: %q", err, s, first)
		}
		if second := again.DSN(); second != first {
			t.Fatalf("the rendering is not stable.\ninput:  %q\nfirst:  %q\nsecond: %q", s, first, second)
		}
	})
}
