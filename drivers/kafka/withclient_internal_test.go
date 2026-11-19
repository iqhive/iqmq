package kafka

import (
	"context"
	"net"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/iqhive/iqmq/driverkit"
)

// besideHook is a hook of the caller's client, which no client beside it may
// take.
type besideHook struct{}

func (besideHook) OnNewClient(*kgo.Client) {}

// hooksOf is the hooks a client was built with. franz-go keeps them in a slice
// of an unexported type, so only reflection can read it.
func hooksOf(cl *kgo.Client) []any {
	var out []any
	for _, v := range cl.OptValues(kgo.WithHooks) {
		hs := reflect.ValueOf(v)
		if hs.Kind() != reflect.Slice {
			continue
		}
		for i := range hs.Len() {
			out = append(out, hs.Index(i).Interface())
		}
	}
	return out
}

// TestAClientBesideTheCallersTakesItsClientOptionsAndNoOthers: a consumer's
// group client and the mover are built from the options of the client the
// caller gave OpenWithClient that apply to a whole client: its seed brokers,
// client id, dialler, SASL and the rest. They take none of its producer,
// consumer or group options, which are the producer's own, but its rack,
// none of its hooks, which are bound to the client they were built with, and
// not AllowAutoTopicCreation, so that a consumer creates no topic.
func TestAClientBesideTheCallersTakesItsClientOptionsAndNoOthers(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "beside"),
		kfake.EnableSASL(), kfake.Superuser("PLAIN", "admin", "adminpass"))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	// A function of the test's own: franz-go's default dialler is a
	// net.Dialer's DialContext too, and a method value of it would compare
	// equal to that one.
	var (
		d     net.Dialer
		dials atomic.Int32
	)
	dial := func(ctx context.Context, network, host string) (net.Conn, error) {
		dials.Add(1)
		return d.DialContext(ctx, network, host)
	}
	versions := kversion.V3_0_0()
	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ClientID("callers-client"),
		kgo.Dialer(dial),
		kgo.SASL(plain.Auth{User: "admin", Pass: "adminpass"}.AsMechanism()),
		kgo.MaxVersions(versions),
		kgo.MetadataMinAge(1500*time.Millisecond),
		kgo.WithHooks(besideHook{}),
		kgo.AllowAutoTopicCreation(),
		kgo.TransactionalID("callers-txn"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(7),
		kgo.ConsumeTopics("callers-topic"),
		kgo.FetchMaxBytes(4096),
		kgo.Rack("az-1"),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	b, err := BackendWithClient(t.Context(), kcl, driverkit.ClientOwned, Config{RequestTimeout: 3 * time.Second})
	if err != nil {
		kcl.Close()
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	// The options every client beside the producer starts from.
	probe, err := kgo.NewClient(b.reach...)
	if err != nil {
		t.Fatalf("a client from the options beside the caller's: %v", err)
	}
	defer probe.Close()
	reached(t, "a client from the options beside the caller's", probe, c.ListenAddrs(), dial, versions)
	if v := probe.OptValues(kgo.TransactionalID); len(v) == 2 && v[1] == true {
		t.Errorf("it took the caller's transactional id %v", v[0])
	}
	if reflect.TypeOf(probe.OptValue(kgo.RecordPartitioner)) == reflect.TypeOf(kgo.ManualPartitioner()) {
		t.Error("it took the caller's record partitioner")
	}
	if n, _ := probe.OptValue(kgo.MaxBufferedRecords).(int64); n == 7 {
		t.Error("it took the caller's MaxBufferedRecords")
	}
	if topics := reflect.ValueOf(probe.OptValue(kgo.ConsumeTopics)); topics.Len() != 0 {
		t.Errorf("it took the caller's ConsumeTopics, %v", topics)
	}
	if n, _ := probe.OptValue(kgo.FetchMaxBytes).(int32); n == 4096 {
		t.Error("it took the caller's FetchMaxBytes")
	}
	if auto, _ := probe.OptValue(kgo.AllowAutoTopicCreation).(bool); auto {
		t.Error("it took the caller's AllowAutoTopicCreation, so a consumer would create the topic it names")
	}
	if hs := hooksOf(probe); len(hs) != 0 {
		t.Errorf("it took the caller's hooks %v", hs)
	}

	// The mover the transactional producer's nacks go through.
	mover, err := b.mover()
	if err != nil {
		t.Fatalf("mover: %v", err)
	}
	reached(t, "the mover", mover, c.ListenAddrs(), dial, versions)
	if transactional(mover) {
		t.Error("the mover took the caller's transactional id")
	}
	if _, ours := mover.OptValue(kgo.RecordPartitioner).(partitioner); !ours {
		t.Errorf("the mover partitions with %T, want the driver's", mover.OptValue(kgo.RecordPartitioner))
	}
	hs := hooksOf(mover)
	if len(hs) != 1 {
		t.Fatalf("the mover's hooks are %v, want the driver's own alone", hs)
	}
	if _, ours := hs[0].(*clientHooks); !ours {
		t.Errorf("the mover's hook is %T, want the driver's own", hs[0])
	}
}

// reached checks that cl reaches the cluster as the caller's client does.
func reached(t *testing.T, what string, cl *kgo.Client, seeds []string, dial func(context.Context, string, string) (net.Conn, error), versions *kversion.Versions) {
	t.Helper()
	if got, _ := cl.OptValue(kgo.SeedBrokers).([]string); !slices.Equal(got, seeds) {
		t.Errorf("%s seeds %v, want the caller's %v", what, got, seeds)
	}
	if id, _ := cl.OptValue(kgo.ClientID).(string); id != "callers-client" {
		t.Errorf("%s has client id %q, want the caller's", what, id)
	}
	if got := reflect.ValueOf(cl.OptValue(kgo.Dialer)).Pointer(); got != reflect.ValueOf(dial).Pointer() {
		t.Errorf("%s dials with another dialler than the caller's", what)
	}
	if m, _ := cl.OptValue(kgo.SASL).([]sasl.Mechanism); len(m) != 1 || m[0].Name() != "PLAIN" {
		t.Errorf("%s authenticates with %v, want the caller's PLAIN", what, m)
	}
	if got, _ := cl.OptValue(kgo.MaxVersions).(*kversion.Versions); got != versions {
		t.Errorf("%s caps the protocol at other versions than the caller's", what)
	}
	if age, _ := cl.OptValue(kgo.MetadataMinAge).(time.Duration); age != 1500*time.Millisecond {
		t.Errorf("%s has MetadataMinAge %v, want the caller's 1.5s", what, age)
	}
	// A consumer option it takes all the same: where the client runs.
	if rack, _ := cl.OptValue(kgo.Rack).(string); rack != "az-1" {
		t.Errorf("%s has rack %q, want the caller's az-1", what, rack)
	}
}

// TestAClientBesideTheDriversOwnProducerIsBuiltFromConfig: with no client of
// the caller's, the clients beside the producer reach the cluster as the
// configuration says, as they always did.
func TestAClientBesideTheDriversOwnProducerIsBuiltFromConfig(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	b, err := OpenBackend(t.Context(), Config{Brokers: c.ListenAddrs(), ClientID: "from-config", RequestTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	probe, err := kgo.NewClient(b.reach...)
	if err != nil {
		t.Fatalf("a client from the options beside the producer: %v", err)
	}
	defer probe.Close()
	if got, _ := probe.OptValue(kgo.SeedBrokers).([]string); !slices.Equal(got, c.ListenAddrs()) {
		t.Errorf("it seeds %v, want Config.Brokers %v", got, c.ListenAddrs())
	}
	if id, _ := probe.OptValue(kgo.ClientID).(string); id != "from-config" {
		t.Errorf("it has client id %q, want Config.ClientID", id)
	}
	if hs := hooksOf(probe); len(hs) != 0 {
		t.Errorf("it took the producer's hooks %v", hs)
	}
}
