package rabbit

import (
	"strings"
	"testing"
	"unicode/utf8"

	mq "bitbucket.org/iqhive/iqmq"
)

// What it checks is not "does Derive crash": a pure function over two strings
// would be a poor fuzz target if that were all. It checks the four properties
// the rest of the driver rests on, each of which is a way for a caller's topic
// name to produce a topology that quietly misbehaves:
//
//  1. Every name Derive returns is one the broker will accept. A name over 255
//     bytes, or under "amq.", is refused by the broker with a CHANNEL
//     exception — and on a publisher-confirm channel that takes every
//     publication whose confirm has not arrived with it. So Derive
//     either refuses or returns names that are all declarable.
//  2. The names point at each other. The dead-letter queue is the queue's name
//     plus ".dlq" and the dead-letter exchange the queue's plus ".dlx"; a set
//     where one was derived from a different input builds a topology whose
//     parts miss.
//  3. A derived name is never mistaken for a subscription's queue. That is the
//     ambiguity "<topic>.dlq" splits into a topic and a
//     subscription exactly as well as "<topic>.<sub>" does.
//  4. Nothing Derive accepts can bind a queue to a topic the caller did not
//     name. A topic is its own routing key, so the queue's binding key must
//     match the topic's routing key and nothing broader.
func FuzzRabbitNaming(f *testing.F) {
	f.Add("", "orders", "workers")
	f.Add("iqmq.", "orders.eu.west", "workers")
	f.Add("amq.", "orders", "workers")
	f.Add("", "orders.#", "workers")
	f.Add("", "orders", "dlq")
	f.Add("", "orders", "eu.workers")
	f.Add("", strings.Repeat("a", MaxNameLen), "w")
	f.Add("", strings.Repeat("a", MaxNameLen-3), "w")
	f.Add("", "ord\xffers", "w")
	f.Add("", "orders\n", "w")
	f.Add("\x00", "orders", "w")
	f.Add("", "", "")
	f.Add("", "*", "*")
	f.Add("", ".", ".")
	f.Add("", "orders", strings.Repeat("w", MaxNameLen))

	f.Fuzz(func(t *testing.T, prefix, topic, sub string) {
		// The dead-letter topic is drawn from the SAME corpus as the topic,
		// which is what makes the override's own rules fall out of the
		// existing seeds: a reserved prefix, a wildcard, an over-long name and
		// invalid UTF-8 all reach it, and a run where it happens to equal the
		// topic is the self-referential route worth having. It is exercised
		// through a second Derive rather than a fourth fuzz argument, so that
		// every property below is checked for both shapes of the name set.
		checkDeadLetterTopic(t, prefix, topic, sub)

		n, err := Derive(Config{Prefix: prefix, Topic: topic, Subscription: sub})
		if err != nil {
			// A refusal must be a portable one: the caller named something
			// this broker cannot name, which the specification makes
			// CodeInvalidArgument. A refusal carrying CodeUnknown would
			// reach a caller as "something went wrong".
			if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
				t.Fatalf("Derive(%q, %q, %q) refused with code %v, want %v: %v",
					prefix, topic, sub, code, mq.CodeInvalidArgument, err)
			}
			return
		}

		// (1) Every name is declarable.
		for what, name := range map[string]string{
			"exchange":             n.Exchange,
			"queue":                n.Queue,
			"dead-letter exchange": n.DeadLetterExchange,
			"dead-letter queue":    n.DeadLetterQueue,
			"delay exchange":       n.DelayExchange,
		} {
			if name == "" {
				continue
			}
			if err := checkDeclarable(name, what); err != nil {
				t.Fatalf("Derive(%q, %q, %q) returned the %s %q, which the broker refuses: %v",
					prefix, topic, sub, what, name, err)
			}
		}
		if n.RoutingKey == "" || len(n.RoutingKey) > MaxRoutingKeyLen || !utf8.ValidString(n.RoutingKey) {
			t.Fatalf("Derive(%q, %q, %q) returned the routing key %q, which is not a usable shortstr",
				prefix, topic, sub, n.RoutingKey)
		}

		// (2) The names point at each other.
		if n.Queue != "" {
			if n.DeadLetterQueue != n.Queue+DLQSuffix {
				t.Fatalf("the dead-letter queue %q is not the queue %q plus %q", n.DeadLetterQueue, n.Queue, DLQSuffix)
			}
			if n.DeadLetterExchange != n.Queue+DLXSuffix {
				t.Fatalf("the dead-letter exchange %q is not the queue %q plus %q", n.DeadLetterExchange, n.Queue, DLXSuffix)
			}
			if !strings.HasPrefix(n.Queue, n.Exchange+".") {
				t.Fatalf("the queue %q is not under the exchange %q: an operator cannot find the subscription's queue from the topic", n.Queue, n.Exchange)
			}
		}
		if n.DelayExchange != n.Exchange+DelayExchangeSuffix {
			t.Fatalf("the delay exchange %q is not the exchange %q plus %q", n.DelayExchange, n.Exchange, DelayExchangeSuffix)
		}

		// (3) A derived name is never a subscription's queue.
		for _, name := range []string{n.DeadLetterQueue, n.DeadLetterExchange} {
			if name == "" {
				continue
			}
			if gotTopic, gotSub, ok := ParseQueue(prefix, name); ok {
				t.Fatalf("ParseQueue read the derived name %q as the subscription %q of the topic %q", name, gotSub, gotTopic)
			}
		}
		if n.Queue != "" {
			gotTopic, gotSub, ok := ParseQueue(prefix, n.Queue)
			if !ok || gotTopic != topic || gotSub != sub {
				t.Fatalf("ParseQueue(%q, %q) = (%q, %q, %v), want (%q, %q, true)",
					prefix, n.Queue, gotTopic, gotSub, ok, topic, sub)
			}
		}

		// (4) The binding reaches the topic and nothing broader. With the
		// topic as the routing key the binding key IS the topic, so the
		// match is exact; the only accepted alternative is "#", which is
		// KeyAsRoutingKey's, and Derive was not asked for it here.
		if n.RoutingKey != topic {
			t.Fatalf("the routing key %q is not the topic %q", n.RoutingKey, topic)
		}
		if !Matches(n.RoutingKey, topic) {
			t.Fatalf("the binding key %q does not match the topic's own routing key %q: the queue would receive nothing", n.RoutingKey, topic)
		}
	})
}

// checkDeadLetterTopic holds [Config.DeadLetterTopic]'s own three rules, which
// the default name set cannot reach.
//
// The override changes three derived names TOGETHER , and
// each of them can be wrong in a way that loses messages rather than failing:
// a dead-letter exchange that is not the named topic's sends them somewhere
// else, a routing key the named topic's subscriptions do not bind sends them
// nowhere, and a dead-letter QUEUE the driver declares here would be a queue
// nothing consumes that receives every failure.
func checkDeadLetterTopic(t *testing.T, prefix, topic, sub string) {
	t.Helper()
	if sub == "" {
		// A publish-only name set has no dead-letter route at all.
		return
	}
	dl := topic
	n, err := Derive(Config{Prefix: prefix, Topic: topic, Subscription: sub, DeadLetterTopic: dl})
	if err != nil {
		if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
			t.Fatalf("Derive with DeadLetterTopic %q refused with code %v, want %v: %v",
				dl, code, mq.CodeInvalidArgument, err)
		}
		return
	}
	if n.DeadLetterQueue != "" {
		t.Fatalf("the override derived the dead-letter queue %q; the destination's queues belong to that topic's own subscriptions, and a queue declared here receives every failure and is consumed by nothing",
			n.DeadLetterQueue)
	}
	want, werr := Derive(Config{Prefix: prefix, Topic: dl})
	if werr != nil {
		t.Fatalf("Derive accepted %q as a dead-letter topic and refuses it as a topic: %v", dl, werr)
	}
	if n.DeadLetterExchange != want.Exchange {
		t.Fatalf("the dead-letter exchange is %q and the topic %q maps to the exchange %q: a rejected message would be routed somewhere else entirely",
			n.DeadLetterExchange, dl, want.Exchange)
	}
	if !Matches(want.RoutingKey, n.DeadLetterRoutingKey) {
		t.Fatalf("the dead-letter routing key %q does not match the binding key %q the destination's subscriptions use: the broker would route the rejected message nowhere and drop it",
			n.DeadLetterRoutingKey, want.RoutingKey)
	}
}

// FuzzRabbitVersion holds the version parser to the three properties a
// capability gate reads off it.
//
// A version gate is the one kind of capability claim this driver pair cannot
// check against the broker in every direction — the conformance suite runs
// against one image at a time — so the arithmetic under the gate is checked
// here instead: the parse never panics, Compare is a total order, and an
// unparseable version opens no gate.
func FuzzRabbitVersion(f *testing.F) {
	f.Add("4.3.5", "3.13.7")
	f.Add("3.9.0", "3.13.0")
	f.Add("4.1.8-rc.1", "4.1.8")
	f.Add("", "4.0.0")
	f.Add("unknown", "0.0.0")
	f.Add("99999999999999999999.0.0", "4.0.0")
	f.Add("4.", ".4")
	f.Add("4.3.5 (Erlang 27)", "4.3.5+deb1")

	f.Fuzz(func(t *testing.T, a, b string) {
		va, aok := ParseVersion(a)
		vb, bok := ParseVersion(b)
		_ = bok

		// Compare is antisymmetric and reflexive, which is what a gate rests
		// on.
		//nolint:gocritic // dupArg: comparing a version with ITSELF is the reflexivity half of the total order, which is the property under test
		if va.Compare(va) != 0 || vb.Compare(vb) != 0 {
			t.Fatalf("Compare is not reflexive: %v %v", va, vb)
		}
		if c, r := va.Compare(vb), vb.Compare(va); c != -r {
			t.Fatalf("Compare(%v, %v) = %d but Compare(%v, %v) = %d", va, vb, c, vb, va, r)
		}
		// A parse that failed is the zero version, and the zero version
		// satisfies no gate. The second half is the one that matters: a gate
		// that an unreported version opens is no gate.
		if !aok {
			if !va.IsZero() {
				t.Fatalf("ParseVersion(%q) failed but returned %v", a, va)
			}
			if va.AtLeast(MinBrokerAMQP10) || va.AtLeast(MinBrokerSupported) {
				t.Fatalf("the unparseable version %q satisfies a gate", a)
			}
			if err := CheckAtLeast(va, MinBrokerAMQP10, "x"); err == nil {
				t.Fatalf("CheckAtLeast accepted the unparseable version %q", a)
			}
		}
		// AtLeast agrees with Compare, which is the only relation the gates
		// are written in terms of.
		if got, want := va.AtLeast(vb), va.Compare(vb) >= 0; got != want {
			t.Fatalf("%v.AtLeast(%v) = %v but Compare says %d", va, vb, got, va.Compare(vb))
		}
		// String round-trips for anything that parsed, which is what a
		// driver page and an error message print.
		if aok {
			again, ok := ParseVersion(va.String())
			if !ok || again != va {
				t.Fatalf("ParseVersion(%q).String() = %q does not round-trip (%v, %v)", a, va.String(), again, ok)
			}
		}
	})
}
