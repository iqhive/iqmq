package natsunified_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	sjs "github.com/nats-io/nats.go/jetstream"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/jetstream"
	"bitbucket.org/iqhive/iqmq/drivers/natsunified"
)

// TestFacadeNamesAStreamItDoesNotCreate is ADR-016's auto-create question
// answered with a check rather than a sentence.
//
// A DSN that names a stream nobody created fails at [natsunified.OpenDSN] with
// CodeNotFound, naming the stream, and the stream is still absent afterwards.
// Both halves matter:
//
//   - Failing at Open is what makes it a refusal and not a
//     per-message one. The alternative — a facade that turned the missing stream
//     into `ManageStream: true` — would be a convenience that provisions
//     production topology from a connection string, which is exactly what the specification
//     forbids and what ADR-016 the specification already rejected for `memory.Open`.
//   - Checking the stream is still absent is what proves the failure was not
//     "created it and then something else went wrong".
func TestFacadeNamesAStreamItDoesNotCreate(t *testing.T) {
	missing, root := names(t)
	addr := strings.TrimPrefix(jsURL(t), "nats://")
	uri := "natsjs://" + addr + "/" + root + "?stream=" + missing

	client, err := natsunified.OpenDSN(t.Context(), uri)
	if err == nil {
		_ = client.Close()
		t.Fatalf("OpenDSN(%q) created or found a stream nobody provisioned; the facade must not create topology ", uri)
	}
	if code := mq.CodeOf(err); code != mq.CodeNotFound {
		t.Errorf("a DSN naming a missing stream carries %v, want CodeNotFound", code)
	}
	// A caller has to be able to tell WHICH stream is missing, or the error is
	// unactionable in a deployment with more than one.
	if !strings.Contains(err.Error(), missing) {
		t.Errorf("the refusal %q does not name the stream %q", err, missing)
	}
	waitForNoClients(t, jsServer)
	assertStreamAbsent(t, missing)
}

// TestFacadeCreatesTheStreamWhenTheDSNSaysTo is the other direction: auto-create
// is available and it is explicit.
//
// `managestream=true` is the operator writing it down, in the connection string,
// where it is reviewable. That is the whole of ADR-016's naming and auto-create
// policy for this facade: nothing implicit, one spelling, and the same default
// (`false`) the memory driver has.
func TestFacadeCreatesTheStreamWhenTheDSNSaysTo(t *testing.T) {
	stream, root := names(t)
	addr := strings.TrimPrefix(jsURL(t), "nats://")
	uri := "natsjs://" + addr + "/" + root + "?stream=" + stream + "&managestream=true"

	assertStreamAbsent(t, stream)
	client, err := natsunified.OpenDSN(t.Context(), uri)
	if err != nil {
		t.Fatalf("OpenDSN(%q) with managestream=true: %v", uri, err)
	}
	defer func() { _ = client.Close() }()

	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "t"})
	if err != nil {
		t.Fatalf("opening a publisher on the created stream: %v", err)
	}
	defer func() { _ = pub.Close() }()
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("created")}); err != nil {
		t.Fatalf("publishing to the created stream: %v", err)
	}
}

// TestCoreProfileNeedsNoTopologyAtAll is the Core half of the same question, and
// it is the asymmetry ADR-016 has to record.
//
// There is nothing to create: a NATS Core subject exists because somebody
// published to it, so a topic name that no configuration mentions works
// immediately. A caller reading one capability table and one auto-create policy
// for "NATS" would be reading two different brokers, which is why this facade's
// page states the policy per profile.
func TestCoreProfileNeedsNoTopologyAtAll(t *testing.T) {
	client := openFacade(t, coreConfig(t))
	topic := coreTopic(t)
	con, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "never-provisioned"})
	if err != nil {
		t.Fatalf("a Core consumer on a subject nobody provisioned: %v", err)
	}
	defer func() { _ = con.Close() }()
	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("a Core publisher on a subject nobody provisioned: %v", err)
	}
	defer func() { _ = pub.Close() }()
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("no topology")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if d := receiveOne(t, con, 10*time.Second); string(d.Body) != "no topology" {
		t.Errorf("received %q, want \"no topology\"", d.Body)
	}
}

// assertStreamAbsent fails when the JetStream server holds a stream of that
// name.
//
// It opens a client of its own through the facade — with a stream it manages, so
// that the check itself provisions nothing it is testing — and asks the
// JetStream context reached through [natsunified.JetStreamBackend], which is the
// escape hatch a facade caller has instead of a concrete backend type.
func assertStreamAbsent(tb testing.TB, name string) {
	tb.Helper()
	probe, root := names(tb)
	b, err := natsunified.OpenBackend(tb.Context(), natsunified.Config{
		Profile: natsunified.ProfileJetStream,
		JetStream: jetstream.Config{
			Addrs: []string{jsURL(tb)}, Stream: probe,
			SubjectPrefix: root, ManageStream: true,
		},
	})
	if err != nil {
		tb.Fatalf("opening the probe backend: %v", err)
	}
	defer func() { _ = b.Close() }()
	jb, ok := natsunified.JetStreamBackend(b)
	if !ok {
		tb.Fatalf("JetStreamBackend did not recognise a JetStream backend (%T)", b)
	}
	if _, err := jb.JetStream().Stream(tb.Context(), name); !errors.Is(err, sjs.ErrStreamNotFound) {
		tb.Fatalf("stream %q: %v, want ErrStreamNotFound — the facade must not have created it", name, err)
	}
}
