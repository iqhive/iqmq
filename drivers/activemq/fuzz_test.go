package activemq_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	amq "bitbucket.org/iqhive/iqmq/drivers/activemq"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// # What it is about
//
// The two products read a publication's controls out of DIFFERENT SECTIONS of
// an AMQP 1.0 message, and the difference is silent in both directions.
// Measured against the pinned images: Artemis honours a scheduled delivery
// written as the MESSAGE ANNOTATION `x-opt-delivery-delay` and ignores
// `AMQ_SCHEDULED_DELAY` written as an annotation; Classic honours
// `AMQ_SCHEDULED_DELAY` written as an APPLICATION PROPERTY and ignores the
// annotation form. A message whose control went into the wrong section is not
// refused by anything -- it is delivered at once, with the control passed
// through to the consumer as an ordinary header.
//
// So what has to hold for every input is:
//
//   - a delayed publication's control lands in the section THIS broker reads,
//     under the key it reads, and nowhere else;
//   - a caller's own header never lands in a section the broker reads as a
//     control, and never shadows one of the broker's own names;
//   - what the mapping wrote comes back out of a decode unchanged, or is
//     refused -- never mangled;
//   - nothing panics, whatever the bytes.
//
// The reserved-prefix rule is the sharp edge. This profile declares
// `AnnotationPrefix = "x-opt-"`, so a caller's header beginning `x-opt-` is
// written to the annotation section by design -- which is what lets a caller
// reach a broker control the portable API has no field for, and which means a
// caller's `x-opt-delivery-delay` header would BE a delay. That is the case
// this target is pointed at.
func FuzzActiveMQAnnotations(f *testing.F) {
	// The seeds are the cases worth keeping, each one a thing that was
	// either measured against a broker or would be a silent defect.
	f.Add(uint8(0), "colour", "red", int64(0), "")                 // an ordinary header
	f.Add(uint8(1), "colour", "red", int64(0), "")                 // the same on the other product
	f.Add(uint8(0), "x-opt-delivery-delay", "99999", int64(0), "") // a caller reaching a control
	f.Add(uint8(1), "AMQ_SCHEDULED_DELAY", "99999", int64(0), "")  // the same on Classic
	f.Add(uint8(0), "x-opt-anything", "v", int64(2000), "")        // annotation header and a real delay
	f.Add(uint8(1), "plain", "v", int64(2000), "")                 // application property and a real delay
	f.Add(uint8(0), "_AMQ_DUPL_ID", "forged", int64(0), "dedup-1") // a caller forging the de-duplication key
	f.Add(uint8(0), "x-iqmq-key", "forged", int64(0), "")          // a caller forging a reserved entry
	f.Add(uint8(0), "", "v", int64(0), "")                         // an empty header name
	f.Add(uint8(0), "k", "", int64(0), "")                         // an empty header value
	f.Add(uint8(0), "k", strings.Repeat("v", 300), int64(0), "")   // over the advertised header ceiling
	f.Add(uint8(0), strings.Repeat("k", 300), "v", int64(0), "")   // a name over the ceiling
	f.Add(uint8(0), "k", "\x00\xff\xfe", int64(0), "")             // a value that is not text
	f.Add(uint8(0), "k", "v", int64(1), "")                        // a delay under a millisecond
	f.Add(uint8(1), "k", "v", int64(-5), "")                       // a negative delay
	f.Add(uint8(0), "group-id", "forged", int64(0), "")            // a caller forging the group

	f.Fuzz(func(t *testing.T, which uint8, key, value string, delayMillis int64, dedup string) {
		broker := activemq.Artemis
		if which%2 == 1 {
			broker = activemq.Classic
		}
		cfg := amq.Config{
			Broker:            broker,
			Addrs:             []string{"localhost:5672"},
			InsecurePlainSASL: true,
			Scheduler:         true,
		}
		profile, err := cfg.Profile()
		if err != nil {
			t.Fatalf("Profile(%v): %v", broker, err)
		}
		caps, err := amq.CapabilitiesForTest(cfg, productOf(broker), "2.44.0")
		if err != nil {
			t.Fatalf("capabilities: %v", err)
		}
		mapper, err := core.MapperForTest(profile, mq.MetadataNativeStrict, caps, true)
		if err != nil {
			t.Fatalf("mapper: %v", err)
		}

		m := &mq.Message{Body: []byte("b"), Key: []byte("k")}
		if key != "" {
			m.Headers.Add(key, []byte(value))
		}
		o := &mq.PublishOptions{DedupKey: dedup}
		if delayMillis > 0 {
			o.Delay = time.Duration(delayMillis) * time.Millisecond
		}

		b := core.NewBuf()
		out, err := mapper.Encode(b, m, o)
		if err != nil {
			// A refusal is a legitimate answer and the only other one:
			// what must never happen is a message that went out with a
			// control the caller did not ask for.
			return
		}
		if out == nil {
			t.Fatal("Encode returned no message and no error")
		}

		delayKey, delaySection := expectedDelay(broker)
		// 1. A delay lands where the broker reads it, and only there.
		if o.Delay > 0 {
			got, where := findControl(out, delayKey)
			if where == "" {
				t.Fatalf("a %v publication with a %v delay carries no %q anywhere: annotations=%v properties=%v",
					broker, o.Delay, delayKey, out.Annotations, out.ApplicationProperties)
			}
			if where != delaySection {
				t.Fatalf("a %v publication's %q is in the %s section and this broker reads the %s one; measured, "+
					"a control in the wrong section is not refused -- the message is delivered at once and the "+
					"control travels through to the consumer",
					broker, delayKey, where, delaySection)
			}
			if ms, ok := millisOf(got); !ok || ms < delayMillis {
				t.Fatalf("a %v delay became %v (%T) under %q; a delay rounded DOWN releases the publication earlier "+
					"than the caller asked", o.Delay, got, got, delayKey)
			}
		}
		// 2. A caller's header never becomes a control the caller did not
		//    ask for, on the OTHER key: the only key a publication may
		//    carry as a control is this broker's own, and only when a delay
		//    was asked for.
		if o.Delay == 0 {
			otherKey, _ := expectedDelay(otherBroker(broker))
			if _, where := findControl(out, otherKey); where != "" && key != otherKey {
				t.Fatalf("a publication with no delay carries %q in the %s section", otherKey, where)
			}
		}
		// 3. A header under the profile's annotation prefix is in the
		//    annotation section, and one that is not is an application
		//    property. That is the seam a caller reaches a broker control
		//    through, and getting it backwards would either hide a caller's
		//    header from the broker or turn an ordinary header into a
		//    control.
		if key != "" {
			wantAnnotation := strings.HasPrefix(key, "x-opt-")
			_, inAnnos := lookup(out.Annotations, key)
			_, inProps := out.ApplicationProperties[key]
			switch {
			case wantAnnotation && !inAnnos && !isControl(key, broker):
				t.Fatalf("the header %q is under the profile's annotation prefix and is not in the annotation "+
					"section: annotations=%v properties=%v", key, out.Annotations, out.ApplicationProperties)
			case !wantAnnotation && !inProps && !isControl(key, broker) && !reserved(key):
				t.Fatalf("the header %q is not under the annotation prefix and is not an application property: "+
					"annotations=%v properties=%v", key, out.Annotations, out.ApplicationProperties)
			}
		}
		// 4. What the mapping wrote comes back unchanged.
		var back mq.Message
		if _, derr := mapper.Decode(&back, out); derr != nil {
			t.Fatalf("Decode of this driver's own encoding failed: %v", derr)
		}
		if key != "" && !reserved(key) && !isControl(key, broker) {
			if got, ok := back.Headers.Get(key); !ok {
				t.Fatalf("the header %q did not survive the round trip: %v", key, back.Headers)
			} else if string(got) != value {
				t.Fatalf("the header %q round-tripped as %q, want %q", key, got, value)
			}
		}
	})
}

// FuzzActiveMQDestinations is the other half of the same question: a
// destination in the wrong FORM is not refused by Classic either, it addresses
// something else.
//
// Measured: Classic decides whether a name is a queue or a topic from the
// destination's own shape, so `VirtualTopic.orders` with no scheme is a QUEUE
// that every consumer queue is subscribed to and none of them receives
// anything from. This target holds the derivation to its own inverse and to
// the scheme rule for every input.
func FuzzActiveMQDestinations(f *testing.F) {
	f.Add(uint8(0), "orders", "workers", "")
	f.Add(uint8(1), "orders", "workers", "")
	f.Add(uint8(1), "orders", "workers", "iqmq.")
	f.Add(uint8(0), "orders.eu", "workers", "")
	f.Add(uint8(0), "orders", "work.ers", "")
	f.Add(uint8(0), "a::b", "workers", "")
	f.Add(uint8(0), "orders", "", "")
	f.Add(uint8(0), "", "workers", "")
	f.Add(uint8(1), "VirtualTopic.orders", "workers", "")
	f.Add(uint8(0), "orders", "workers", "pre:fix")
	f.Add(uint8(0), "#", "workers", "")
	f.Add(uint8(0), strings.Repeat("t", 300), "workers", "")

	f.Fuzz(func(t *testing.T, which uint8, topic, sub, prefix string) {
		broker := activemq.Artemis
		if which%2 == 1 {
			broker = activemq.Classic
		}
		cfg := amq.Config{
			Broker:            broker,
			Addrs:             []string{"localhost:5672"},
			Prefix:            prefix,
			InsecurePlainSASL: true,
		}
		if cfg.Validate() != nil {
			return
		}
		n, err := cfg.Names(topic, sub)
		if err != nil {
			return
		}
		if broker == activemq.Classic {
			// Every Classic destination carries its scheme, or it addresses
			// the other namespace.
			if !strings.HasPrefix(n.Publish, "topic://") {
				t.Fatalf("a Classic publish destination %q carries no topic:// scheme; an undecorated name is a "+
					"QUEUE that every consumer queue is subscribed to and none of them receives anything from",
					n.Publish)
			}
			if n.Consume != "" && !strings.HasPrefix(n.Consume, "queue://") {
				t.Fatalf("a Classic consume destination %q carries no queue:// scheme", n.Consume)
			}
			if !strings.Contains(n.Publish, activemq.VirtualTopicPrefix) {
				t.Fatalf("a Classic publish destination %q is not under %q, so the broker copies nothing into the "+
					"consumer queues", n.Publish, activemq.VirtualTopicPrefix)
			}
		} else if n.Consume != "" {
			// Artemis addresses a shared durable subscription by the fully
			// qualified queue name, and the address half must be the
			// publish destination or a consumer reads a different address.
			addr, queue, found := strings.Cut(n.Consume, activemq.FQQN)
			if !found {
				t.Fatalf("an Artemis consume destination %q is not a fully qualified queue name", n.Consume)
			}
			if addr != n.Publish {
				t.Fatalf("the Artemis FQQN's address half is %q and the publish destination is %q: a consumer would "+
					"read an address nothing publishes to", addr, n.Publish)
			}
			if queue != n.Queue {
				t.Fatalf("the FQQN names the queue %q and Names.Queue is %q", queue, n.Queue)
			}
		}
		if n.Queue == "" {
			return
		}
		// The derivation is its own inverse, which is what Admin's
		// ListTopics rests on.
		gotTopic, gotSub, ok := activemq.ParseQueue(broker, prefix, n.Queue)
		if !ok {
			t.Fatalf("the derived queue %q cannot be parsed back on %v", n.Queue, broker)
		}
		if gotTopic != topic || gotSub != sub {
			t.Fatalf("the queue %q parses back as topic %q subscription %q, want %q and %q",
				n.Queue, gotTopic, gotSub, topic, sub)
		}
	})
}

// expectedDelay is the key and the section THIS broker reads a scheduled
// delivery from, as measured.
func expectedDelay(b activemq.Broker) (key, section string) {
	if b == activemq.Classic {
		return amq.PropertyScheduledDelay, "application-properties"
	}
	return amq.AnnotationDeliveryDelay, "message-annotations"
}

// otherBroker is the product this one is not.
func otherBroker(b activemq.Broker) activemq.Broker {
	if b == activemq.Classic {
		return activemq.Artemis
	}
	return activemq.Classic
}

// productOf is what the broker calls itself.
func productOf(b activemq.Broker) string {
	if b == activemq.Classic {
		// Measured: Classic sends no product at all over AMQP 1.0.
		return ""
	}
	return activemq.ProductArtemis
}

// isControl reports whether key is one of the brokers' own control names.
func isControl(key string, b activemq.Broker) bool {
	_ = b
	switch key {
	case amq.AnnotationDeliveryDelay, amq.AnnotationDeliveryTime,
		amq.PropertyScheduledDelay, amq.PropertyScheduledTime,
		amq.PropertyDuplicateID:
		return true
	}
	return false
}

// reserved reports whether key is one of iqmq's own reserved entries.
func reserved(key string) bool { return strings.HasPrefix(key, "x-iqmq-") }

// findControl looks for key in both sections and says which one it was in.
func findControl(m *amqp.Message, key string) (any, string) {
	if v, ok := lookup(m.Annotations, key); ok {
		return v, "message-annotations"
	}
	if v, ok := m.ApplicationProperties[key]; ok {
		return v, "application-properties"
	}
	if v, ok := lookup(m.DeliveryAnnotations, key); ok {
		return v, "delivery-annotations"
	}
	return nil, ""
}

// lookup reads an annotation under both the string and the symbol spelling,
// because go-amqp's map is keyed by `any` and a broker may send either.
func lookup(a amqp.Annotations, key string) (any, bool) {
	if v, ok := a[key]; ok {
		return v, true
	}
	if v, ok := a[amqp.Symbol(key)]; ok {
		return v, true
	}
	return nil, false
}

// millisOf reads a delay however it was encoded.
func millisOf(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case uint64:
		return int64(n), true
	case uint32:
		return int64(n), true
	case string:
		m, err := strconv.ParseInt(n, 10, 64)
		return m, err == nil
	}
	return 0, false
}
