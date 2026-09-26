package activemq

import (
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// FuzzActiveMQNames holds [Derive] and [ParseQueue] to being each other's
// inverse over arbitrary names, on both products.
//
// The property is what Admin rests on: a driver that lists a broker's queues
// has to say which of them are its own, and the only way to know is to be able
// to take one apart. It has two halves and the SECOND is the dangerous one:
//
//  1. every queue `Derive` produces is one `ParseQueue` reads back, with the
//     same topic and the same subscription. A name `Derive` builds and
//     `ParseQueue` does not recognise is a queue this driver created and can
//     no longer see, so an Admin that reconciled a topology would leave it
//     behind for ever.
//  2. `ParseQueue` recognises NOTHING ELSE. Claiming somebody else's queue is
//     how a driver deletes a topology it does not own, and on Classic that is
//     not even an unlikely accident: every queue on the broker is in one flat
//     namespace, so a driver's `Consumer.<sub>.VirtualTopic.<topic>` sits
//     beside whatever else the deployment has.
//
// The third property is that a name `Derive` ACCEPTS is one both brokers can
// carry. A refusal at the broker costs more than tidiness here: on Artemis an
// attach to a name the broker will not accept detaches the link with
// amqp:not-found and takes the session with it, and on Classic a bad
// destination is accepted and CREATED, so the caller's typo becomes a queue
// nothing consumes. Neither failure names the typo.
func FuzzActiveMQNames(f *testing.F) {
	f.Add(uint8(0), "", "orders", "workers", "")
	f.Add(uint8(1), "", "orders", "workers", "")
	f.Add(uint8(0), "iqmq.", "orders.eu", "workers", "orders.dlq")
	f.Add(uint8(1), "iqmq.", "orders.eu", "workers", "orders.dlq")
	f.Add(uint8(0), "", "a", "b", "")
	f.Add(uint8(1), "", "VirtualTopic.orders", "workers", "")
	f.Add(uint8(0), "", "a::b", "workers", "")
	f.Add(uint8(1), "", "a/b", "workers", "")
	f.Add(uint8(0), "", "a.b", "c.d", "")
	f.Add(uint8(1), "Consumer.", "orders", "workers", "")
	f.Add(uint8(0), "", "orders", "", "")
	f.Add(uint8(0), "", "\xff", "\xff", "")
	f.Add(uint8(1), ".", ".", ".", ".")

	f.Fuzz(func(t *testing.T, brokerByte uint8, prefix, topic, sub, dlq string) {
		brokers := Brokers()
		b := brokers[int(brokerByte)%len(brokers)]
		cfg := Config{Broker: b, Prefix: prefix, Topic: topic, Subscription: sub, DeadLetterTopic: dlq}
		n, err := Derive(cfg)
		if err != nil {
			// A refusal has to be one of the codes a caller acts on: the specification
			// makes "this broker cannot do that" and "your name is wrong"
			// different answers, and a refusal that was neither would be
			// untestable and unactionable.
			switch code := mq.CodeOf(err); code {
			case mq.CodeInvalidArgument, mq.CodeInvalidConfig, mq.CodeUnsupported:
			default:
				t.Fatalf("Derive refused with code %v: %v", code, err)
			}
			return
		}

		// (3) Every name it produced is one both brokers can carry.
		for what, name := range map[string]string{"address": n.Address, "queue": n.Queue, "publish": n.Publish, "consume": n.Consume} {
			if name == "" {
				continue
			}
			// The decorated forms carry the separators CheckName refuses, so
			// only the bare names are checked against it -- which is the
			// point: `topic://VirtualTopic.orders` is a legal DESTINATION and
			// an illegal NAME.
			if what == "address" || what == "queue" {
				if cerr := CheckName(name, what); cerr != nil {
					t.Fatalf("Derive produced the %s %q, which CheckName refuses: %v", what, name, cerr)
				}
			}
			if strings.ContainsAny(name, "\x00\r\n") {
				t.Fatalf("Derive produced the %s %q, which contains a character that terminates a STOMP frame", what, name)
			}
		}
		if n.Publish == "" {
			t.Fatal("Derive produced a name set with nothing to publish to")
		}
		// A subscription was asked for or it was not; there is no third
		// answer, because a consumer with no queue would attach to whatever
		// the broker gave it.
		if (sub == "") != (n.Queue == "") {
			t.Fatalf("subscription %q produced the queue %q", sub, n.Queue)
		}
		if (n.Queue == "") != (n.Consume == "") {
			t.Fatalf("the queue is %q and the consume destination is %q; the two are derived together for a reason", n.Queue, n.Consume)
		}

		if n.Queue == "" {
			return
		}
		// (1) ParseQueue reads back what Derive wrote.
		gotTopic, gotSub, ok := ParseQueue(b, prefix, n.Queue)
		if !ok {
			t.Fatalf("%v: ParseQueue(%q, %q) did not recognise a queue Derive produced from topic %q and subscription %q: an Admin that reconciled a topology would leave it behind for ever",
				b, prefix, n.Queue, topic, sub)
		}
		if gotTopic != topic || gotSub != sub {
			t.Fatalf("%v: ParseQueue(%q, %q) = (%q, %q), want (%q, %q)", b, prefix, n.Queue, gotTopic, gotSub, topic, sub)
		}
		// (2) And it does not claim the queue under the OTHER product's
		// convention, nor under a prefix that is not the caller's. Both are
		// ways a driver comes to delete a queue it does not own.
		other := Artemis
		if b == Artemis {
			other = Classic
		}
		if _, _, claimed := ParseQueue(other, prefix, n.Queue); claimed {
			// This is not always wrong -- an Artemis queue name and a
			// Classic one can coincide in principle -- so the check is that
			// the other product's reading does not produce THIS topic and
			// subscription, which would mean the two conventions had become
			// one.
			o1, o2, _ := ParseQueue(other, prefix, n.Queue)
			if o1 == topic && o2 == sub && b != other {
				t.Fatalf("%v's queue %q reads identically under %v's convention (%q, %q): the two products' mappings are supposed to share no name", b, n.Queue, other, o1, o2)
			}
		}
		// A queue read under a DIFFERENT prefix must not come back as the
		// same topic and subscription. It may well parse -- stripping a
		// shorter prefix off `0x0.0` leaves something that still looks like
		// `<topic>.<subscription>`, which the fuzzer found at once -- and
		// that is fine, because a driver asks `ParseQueue` about its OWN
		// prefix. What would not be fine is two prefixes agreeing on the
		// same topic and subscription, because then a driver could adopt
		// another deployment's queue as one of its own while both of them
		// shared a broker, which is the whole reason Config.Prefix exists.
		if otherTopic, otherSub, claimed := ParseQueue(b, prefix+"x", n.Queue); claimed && otherTopic == topic && otherSub == sub {
			t.Fatalf("%v: the queue %q reads as (%q, %q) under both the prefix %q and the prefix %q: two deployments sharing this broker would adopt each other's queues",
				b, n.Queue, topic, sub, prefix, prefix+"x")
		}
	})
}

// FuzzActiveMQVersion holds [ParseVersion] and [Version.String] to being each
// other's inverse, and holds the version gate to never refusing a broker it
// cannot place.
//
// The round trip is what a driver's refusal message rests on: it prints the
// version it read, so a version that rendered differently from how it parsed
// would put a number in a log that nothing else in the system agrees with.
//
// The gate's half is the one that would break a whole broker rather than a
// message: `CheckAtLeast` ACCEPTS a broker that said nothing, because Classic
// sends no product and no version in its AMQP 1.0 open performative at all, so
// refusing silence would refuse one of the four combinations the specification names.
// A fuzz target that only fed it real versions would never
// reach that branch.
func FuzzActiveMQVersion(f *testing.F) {
	f.Add("ActiveMQ-Artemis/2.44.0", "apache-activemq-artemis", uint8(0))
	f.Add("ActiveMQ/6.2.0", "ActiveMQ/6.2.0", uint8(1))
	f.Add("", "", uint8(0))
	f.Add("2.45.0-SNAPSHOT", "ActiveMQ-Artemis", uint8(0))
	f.Add("6.2.0 (localhost)", "ActiveMQ", uint8(1))
	f.Add("99999999999.0.0", "rabbitmq", uint8(0))
	f.Add("2", "ACTIVEMQ/6", uint8(1))
	f.Add("/", "/", uint8(0))
	f.Add("\xff.\xff", "\xff", uint8(1))
	f.Add("0.0.0", "apache-ACTIVEMQ-ARTEMIS", uint8(0))

	f.Fuzz(func(t *testing.T, version, product string, brokerByte uint8) {
		brokers := Brokers()
		b := brokers[int(brokerByte)%len(brokers)]

		v, ok := ParseVersion(version)
		if ok {
			// The round trip: what it renders is what it reads back.
			again, ok2 := ParseVersion(v.String())
			if v.IsZero() {
				// The zero version renders as "unknown", which the specification makes
				// distinct from any claim -- so it deliberately does NOT
				// round-trip, and that asymmetry is the point rather than a
				// gap.
				if ok2 {
					t.Fatalf("the zero version rendered as %q and parsed back as %v: \"the broker did not say\" must not become a claim", v.String(), again)
				}
			} else if !ok2 || again != v {
				t.Fatalf("ParseVersion(%q) = %v, which renders as %q and parses back as (%v, %v)", version, v, v.String(), again, ok2)
			}
			// Every version is at least an EQUAL one, and the comparison is
			// antisymmetric. A Compare that was not would make the version
			// gate's verdict depend on which side it was asked from. The
			// equal value is rebuilt field by field rather than being `v`
			// itself, so that the check is a comparison of two values rather
			// than of one with itself.
			equal := Version{Major: v.Major, Minor: v.Minor, Patch: v.Patch}
			if !v.AtLeast(equal) || v.Compare(equal) != 0 || !equal.AtLeast(v) {
				t.Fatalf("%v does not compare equal to %v", v, equal)
			}
			for _, floor := range []Version{MinArtemis, MinArtemisFQQN, MinClassicAMQP, MinClassicSTOMP} {
				if c, r := v.Compare(floor), floor.Compare(v); c != -r {
					t.Fatalf("%v.Compare(%v) = %d and the reverse is %d", v, floor, c, r)
				}
			}
		}

		// The gate accepts silence and refuses only what it can place below
		// the floor.
		floor := MinArtemisFQQN
		if b == Classic {
			floor = MinClassicSTOMP
		}
		err := CheckAtLeast(v, floor, "the test's floor")
		switch {
		case v.IsZero():
			if err != nil {
				t.Fatalf("a broker that said nothing (%q) was refused: %v", version, err)
			}
		case v.AtLeast(floor):
			if err != nil {
				t.Fatalf("%v is at least %v and was refused: %v", v, floor, err)
			}
		default:
			if err == nil {
				t.Fatalf("%v is below %v and was accepted", v, floor)
			}
			if code := mq.CodeOf(err); code != mq.CodeUnsupported {
				t.Fatalf("the version refusal's code is %v, want %v: the broker is real and reachable and does not have the feature", code, mq.CodeUnsupported)
			}
			// The message names both versions, or nobody can act on it.
			msg := err.Error()
			if !contains(msg, v.String()) || !contains(msg, floor.String()) {
				t.Fatalf("the refusal %q does not name both %q and %q", msg, v.String(), floor.String())
			}
		}

		// And the product gate, whose one job is to be right in BOTH
		// directions: Artemis's STOMP server header starts with Classic's, so
		// a prefix match reads Artemis as Classic -- and a client configured
		// for Classic and pointed at Artemis does not fail, it auto-creates
		// the wrong topology and delivers nothing.
		perr := CheckProduct(b, product)
		named, placed := productBroker(product)
		switch {
		case strings.TrimSpace(product) == "":
			if perr != nil {
				t.Fatalf("silence was refused for %v: %v", b, perr)
			}
		case !placed:
			if perr == nil {
				t.Fatalf("%v accepted the unplaceable product %q", b, product)
			}
		case named == b:
			if perr != nil {
				t.Fatalf("%v refused its own product %q: %v", b, product, perr)
			}
		default:
			if perr == nil {
				t.Fatalf("%v accepted %q, which is %v: the two products' topic mappings name nothing in common", b, product, named)
			}
			if code := mq.CodeOf(perr); code != mq.CodeUnsupported {
				t.Fatalf("the product refusal's code is %v, want %v", code, mq.CodeUnsupported)
			}
		}
	})
}
