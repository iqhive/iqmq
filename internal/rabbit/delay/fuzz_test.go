package delay

import (
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/rabbit"
)

// FuzzRabbitDelay holds both delay strategies to the rules put
// on any delayed redelivery, over arbitrary delays, targets and ladders.
//
// The properties, each of which is a way for a delay to go wrong silently:
func FuzzRabbitDelay(f *testing.F) {
	f.Add(uint8(0), int64(0), int64(0), uint8(0))
	f.Add(uint8(1), int64(30_000), int64(0), uint8(0))
	f.Add(uint8(0), int64(1), int64(0), uint8(1))
	f.Add(uint8(0), int64(-1), int64(0), uint8(0))
	f.Add(uint8(0), int64(1<<40), int64(0), uint8(0))
	f.Add(uint8(1), int64(1<<40), int64(0), uint8(0))
	f.Add(uint8(0), int64(86_400_000), int64(3_600_000), uint8(0))
	f.Add(uint8(0), int64(100), int64(0), uint8(2))
	f.Add(uint8(1), int64(100), int64(0), uint8(2))

	f.Fuzz(func(t *testing.T, kindByte uint8, delayMS, queueTTLMS int64, targetKind uint8) {
		k := Kinds()[int(kindByte)%len(Kinds())]
		cfg := Config{}
		if queueTTLMS >= 0 && queueTTLMS < int64(time.Hour/time.Millisecond) {
			cfg.QueueTTL = time.Duration(queueTTLMS) * time.Millisecond
		}
		st, err := New(k, cfg)
		if err != nil {
			t.Fatalf("New(%v, %+v) = %v: the configuration is within its own bounds", k, cfg, err)
		}

		var tgt Target
		switch targetKind % 3 {
		case 0:
			tgt = RedeliveryTarget("iqmq.orders.workers")
		case 1:
			tgt = PublishTarget("iqmq.orders", "orders")
		default:
			// A target whose name is at the naming ceiling, so that the
			// derived delay queue name is over it and the plan has to
			// refuse rather than return a name the broker rejects.
			tgt = RedeliveryTarget(longName(rabbit.MaxNameLen))
		}

		// A delay of more than a thousand years is not a case any strategy
		// needs to serve, and time.Duration overflows above about 292 years;
		// the fuzzer reaches both, and a delay that has wrapped negative
		// would be testing the wrap rather than the strategy.
		d := time.Duration(delayMS) * time.Millisecond
		if delayMS != 0 && d/time.Millisecond != time.Duration(delayMS) {
			return
		}

		req := Request{Target: tgt, Delay: d, DelayExchange: "iqmq.orders.delayed"}
		p, perr := st.Plan(req)
		if perr != nil {
			// (3) Every refusal is one of the portable codes a caller acts
			// on.
			switch code := mq.CodeOf(perr); code {
			case mq.CodeUnsupported, mq.CodeInvalidArgument, mq.CodeInvalidConfig:
			default:
				t.Fatalf("Plan(%v) refused with code %v: %v", d, code, perr)
			}
			return
		}

		// (1) Never early.
		if p.Granted < d {
			t.Fatalf("a request for %v was granted %v: the message contract makes a delay a minimum", d, p.Granted)
		}
		if d <= 0 && p.Granted != 0 {
			t.Fatalf("a request for no delay was granted %v", p.Granted)
		}

		// (2) The plan is performable.
		if p.Declare != nil {
			if err := rabbit.CheckName(p.Declare.Name); err != nil {
				t.Fatalf("the plan declares the queue %q, which the broker refuses: %v", p.Declare.Name, err)
			}
			if p.Declare.TTLMillis != millis(p.Granted) {
				t.Fatalf("the plan granted %v and declared an x-message-ttl of %d ms", p.Granted, p.Declare.TTLMillis)
			}
			if p.Declare.DeadLetterExchange != tgt.Exchange || p.Declare.DeadLetterRoutingKey != tgt.RoutingKey {
				t.Fatalf("the delay queue dead-letters to (%q, %q), want the target (%q, %q): the message would reappear somewhere else",
					p.Declare.DeadLetterExchange, p.Declare.DeadLetterRoutingKey, tgt.Exchange, tgt.RoutingKey)
			}
			if p.Exchange != "" || p.RoutingKey != p.Declare.Name {
				t.Fatalf("the plan publishes to (%q, %q) and the delay queue is %q: the message would not reach the queue that holds it",
					p.Exchange, p.RoutingKey, p.Declare.Name)
			}
			if p.Declare.ExpiresMillis < 0 {
				t.Fatalf("the delay queue's x-expires is %d", p.Declare.ExpiresMillis)
			}
		}
		if p.Header != "" {
			if p.Header != DelayHeader {
				t.Fatalf("the plan sets the header %q, want %q", p.Header, DelayHeader)
			}
			if p.HeaderMillis != millis(p.Granted) {
				t.Fatalf("the plan granted %v and set %s to %d ms", p.Granted, p.Header, p.HeaderMillis)
			}
		}
		if d > 0 && p.Declare == nil && p.Header == "" {
			t.Fatalf("a request for %v produced a plan that neither parks the message nor holds it, so nothing delays it: %+v", d, p)
		}
		if p.Hops < 1 || p.Hops > 2 {
			t.Fatalf("the plan reports %d broker hops", p.Hops)
		}
		if p.RoutingKey == "" && p.Exchange == "" {
			t.Fatal("the plan publishes nowhere")
		}

		// (4) The queue count follows from the plans.
		reqs := []Request{req, req}
		if got := st.Queues(reqs); (p.Declare == nil) != (got == 0) {
			t.Fatalf("Queues reported %d for a workload whose plan declares %v", got, p.Declare)
		}
	})
}

// longName is a name of exactly n bytes, for the case where the derived delay
// queue name is over the broker's ceiling.
func longName(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'q'
	}
	return string(b)
}
