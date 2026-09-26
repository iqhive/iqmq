package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	otelmq "github.com/iqhive/iqmq/integrations/otel"
	"github.com/iqhive/iqmq/memory"
)

// wantOutput is what the example prints, with the driver name and the
// messaging.system value substituted per arm.
//
// The topic is NOT in the output -- it carries a timestamp -- so the line that
// is about the destination says "<topic>" and the assertion that it is the
// right topic lives in describe(), where the value is known.
const wantOutput = "instrumented: driver %[1]s as messaging.system=%[1]s\n" +
	"trace: checkout -> publish <topic> -> process <topic>, one trace id across the broker\n" +
	"beside it: \"receive\" and \"ack\" are ROOT spans — mq.Hooks carries no context.Context, so the two hook spans cannot be parented, and the settle span is named for the settlement rather than \"settle\" \n" +
	"attributes: messaging.system=%[1]s, messaging.destination.name=<topic>, semantic conventions pinned at " + otelmq.SemconvVersion + "\n" +
	"instruments: iqmq_publish_duration, iqmq_handler_duration, iqmq_end_to_end_duration recorded a point each, with no unit suffix — the exporter appends it \n"

// TestRunInProcess is the arm that never skips.
//
// The memory broker is a real iqmq backend with real headers, so the whole
// span tree -- including the traceparent crossing the broker -- is exercised
// with no container at all. Nothing about tracing is broker-specific: what
// carries the trace is a header on the message.
func TestRunInProcess(t *testing.T) {
	reportArm(t, armInProcess, statusRan, "memory broker")
	runExample(t, "memory", func(ctx context.Context, hooks mq.Hooks) (*mq.Client, error) {
		return memory.Open(ctx, memory.Config{AutoCreate: true, Hooks: hooks})
	})
}

// TestRunAgainstABroker is the container arm, gated on IQMQ_REDIS_ADDR.
//
// It asserts the same tree as the in-process arm, which is the interesting
// claim: a real broker has to carry a header it knows nothing about, through
// whatever its native metadata is, or the process span loses its parent and
// the trace breaks in two. Redis Streams carries it as a field of the entry.
func TestRunAgainstABroker(t *testing.T) {
	addr, ok := brokerAddr(t, "IQMQ_REDIS_ADDR")
	if !ok {
		t.Skip("IQMQ_REDIS_ADDR is unset: start a Redis container and set it to host:port to run this example against one (see README.md)")
	}
	reportArm(t, armBroker, statusRan, "IQMQ_REDIS_ADDR="+addr)
	runExample(t, "redis", func(ctx context.Context, hooks mq.Hooks) (*mq.Client, error) {
		return rdOpen(ctx, addr, hooks)
	})
}

// runExample holds the example to its output. One body for both arms.
func runExample(t *testing.T, system string, open func(context.Context, mq.Hooks) (*mq.Client, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out, system, open); err != nil {
		t.Fatalf("run: %v\noutput so far:\n%s", err, out.String())
	}
	want := expand(wantOutput, system)
	got := redactTopic(out.String())
	if got != want {
		t.Errorf("the example printed\n%s\nwant\n%s", got, want)
	}
}

// expand substitutes the driver name into the expected output.
func expand(tmpl, system string) string {
	return strings.ReplaceAll(tmpl, "%[1]s", system)
}

// redactTopic replaces the run's unique topic with "<topic>".
//
// The topic carries a timestamp so that two runs against one broker cannot
// collide, which means the printed lines that name it cannot be compared
// literally. Redacting here rather than printing "<topic>" from the program
// keeps the program's own output honest: `go run .` shows the real topic.
func redactTopic(s string) string {
	var out []string
	for line := range strings.SplitSeq(s, "\n") {
		for {
			i := strings.Index(line, topicBase+"-")
			if i < 0 {
				break
			}
			end := i + len(topicBase) + 1
			for end < len(line) && isTopicRune(line[end]) {
				end++
			}
			line = line[:i] + "<topic>" + line[end:]
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func isTopicRune(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z')
}

// TestTheDisabledInstrumentationIsRefusedRatherThanSilent is the check on the
// example's own instrument.
//
// An otelmq.Config with neither provider is the disabled path: Hooks() is
// the zero mq.Hooks and both middlewares are the identity. A program that had
// accidentally built that would record nothing, print "no publish span was
// recorded" and look like a broken broker — so newTelemetry refuses it by
// name, and this is the test that says the refusal exists.
func TestTheDisabledInstrumentationIsRefusedRatherThanSilent(t *testing.T) {
	inst, err := otelmq.New(otelmq.Config{})
	if err != nil {
		t.Fatalf("a Config with neither provider was refused at construction: %v", err)
	}
	if inst.Enabled() {
		t.Fatal("an otelmq.Config with no providers reports itself enabled; the disabled path is what this asserts")
	}
	h := inst.Hooks()
	if h.OnPublishStart != nil || h.OnPublishDone != nil || h.OnReceive != nil || h.OnSettle != nil {
		t.Error("a disabled instrumentation wires an observation point, so the core would build an event per message; the disabled path is reached by installing nothing")
	}
}

// TestSystemIsRequired pins the one configuration error otelmq refuses
// outright, which is also the attribute the example prints.
//
// `messaging.system` is required by the convention and is what every messaging
// dashboard groups by, so a span without it looks right and cannot be queried.
func TestSystemIsRequired(t *testing.T) {
	tel, err := newTelemetry("")
	if err == nil {
		_ = tel.tp.Shutdown(t.Context())
		t.Fatal("newTelemetry accepted an empty messaging.system")
	}
	if !strings.Contains(err.Error(), "System") {
		t.Errorf("the refusal is %q; it should name Config.System", err)
	}
}

// TestTheTopicRedactionOnlyTouchesTheTopic is the check on the test's own
// instrument.
//
// redactTopic is the reason both arms can compare one expected output, so a
// redaction that ate too much -- or nothing -- would make every assertion
// above weaker than it looks. This is the case that would have hidden it: a
// line that mentions the topic base without a suffix, and one that mentions
// something else entirely.
func TestTheTopicRedactionOnlyTouchesTheTopic(t *testing.T) {
	in := "trace: checkout -> publish orders-mfk3j2 -> process orders-mfk3j2, one trace id\norders is not a topic here\n"
	want := "trace: checkout -> publish <topic> -> process <topic>, one trace id\norders is not a topic here\n"
	if got := redactTopic(in); got != want {
		t.Errorf("redactTopic produced\n%q\nwant\n%q", got, want)
	}
	// And the real thing: a generated topic is fully consumed.
	topic := newTopic()
	if got := redactTopic("publish " + topic + " end"); got != "publish <topic> end" {
		t.Errorf("redactTopic(%q) left %q", topic, got)
	}
}

// TestWithoutTimestampTheEndToEndHistogramIsSilent is, as a
// check somebody has watched fail.
//
// the end-to-end histogram is measured from Properties.CreatedAt to the
// handler being called, and the specification says "Publish never generates IDs or
// timestamps on the hot path unless configured". So a publisher without
// PublisherConfig.Timestamp stamps nothing, the consumer has no origin to
// measure from, and iqmq_end_to_end_duration records NO POINT — with no error,
// no log line and nothing in the export to distinguish it from a consumer that
// is not running.
func TestWithoutTimestampTheEndToEndHistogramIsSilent(t *testing.T) {
	t.Cleanup(func() { stampTimestamps = true })
	stampTimestamps = false
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var out bytes.Buffer
	err := run(ctx, &out, "memory", func(c context.Context, hooks mq.Hooks) (*mq.Client, error) {
		return memory.Open(c, memory.Config{AutoCreate: true, Hooks: hooks})
	})
	if err == nil {
		t.Fatalf("the example passed with no CreatedAt stamped; if the end-to-end histogram no longer needs PublisherConfig.Timestamp then the specification is closed.\noutput:\n%s", out.String())
	}
	if !strings.Contains(err.Error(), otelmq.InstrumentEndToEndDuration) {
		t.Fatalf("the failure is %q; it should name the instrument that recorded nothing", err)
	}
	// And the rest of the telemetry is unaffected: the failure is one
	// histogram's silence and not a broken run.
	if !strings.Contains(out.String(), "trace: "+callerSpan) {
		t.Errorf("the span tree was not printed, so this test is failing for some other reason:\n%s", out.String())
	}
}
