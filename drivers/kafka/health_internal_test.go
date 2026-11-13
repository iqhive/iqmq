package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// hooked is a backend with nothing but its health reporter, Ready, and the
// client hooks that drive it.
func hooked() (*Backend, *clientHooks) {
	b := &Backend{errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	b.health.Ready()
	return b, &clientHooks{b: b}
}

// disconnect runs h as franz-go does when it closes a connection: every hook
// that takes OnBrokerDisconnect is given it.
func disconnect(h kgo.Hook, meta kgo.BrokerMetadata) {
	if d, ok := h.(kgo.HookBrokerDisconnect); ok {
		d.OnBrokerDisconnect(meta, nil)
	}
}

// TestAnIdleConnectionsReapIsNotALostLink: franz-go closes a connection that
// has been idle for its ConnIdleTimeout, thirty seconds by default, and runs
// OnBrokerDisconnect for it, as it does for every connection it closes. The
// hook read every disconnect as a lost link, and only a new connection set
// Ready back. The seed connection Open's Ping used goes idle once the client
// has learned the brokers, so a client that published successfully the whole
// time reported Degraded from its first reap on, and the next connection it
// dialled counted a reconnection that never happened.
func TestAnIdleConnectionsReapIsNotALostLink(t *testing.T) {
	b, h := hooked()
	seed := kgo.BrokerMetadata{NodeID: -1}
	broker := kgo.BrokerMetadata{NodeID: 0}
	h.OnBrokerConnect(seed, time.Millisecond, nil, nil)
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	disconnect(h, seed)
	if s := b.health.Snapshot(); s.State != mq.HealthReady {
		t.Errorf("Health after an idle connection's reap = %v (%v), want Ready", s.State, s.Err)
	}
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	if n := b.Reconnects(); n != 0 {
		t.Errorf("a connection dialled after a reap counted %d reconnections, want 0", n)
	}
	if n := b.DegradedTransitions(); n != 0 {
		t.Errorf("a reap counted %d degraded transitions, want 0", n)
	}
}

// TestAnOutageOfFailedDialsIsAReconnection: a client whose connections
// franz-go has reaped has nothing in flight when the broker goes away, so
// the outage reaches it only as dials that fail. The dial that succeeds after
// them is a reconnection, Ready and counted, with OnReconnect fired. A dial
// that fails before the client has ever connected, such as Open's first
// attempt, is not a lost link, and the dial after it is the first connection.
func TestAnOutageOfFailedDialsIsAReconnection(t *testing.T) {
	broker := kgo.BrokerMetadata{NodeID: 0}
	refused := syscall.ECONNREFUSED
	b, h := hooked()
	var fired []mq.ReconnectEvent
	b.cfg.Hooks.OnReconnect = func(_ context.Context, e mq.ReconnectEvent) { fired = append(fired, e) }
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	h.OnBrokerConnect(broker, time.Millisecond, nil, refused)
	if s := b.health.Snapshot(); s.State != mq.HealthDegraded {
		t.Errorf("Health after a dial that failed = %v, want Degraded", s.State)
	}
	h.OnBrokerConnect(broker, time.Millisecond, nil, refused)
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	if s := b.health.Snapshot(); s.State != mq.HealthReady {
		t.Errorf("Health after the broker came back = %v (%v), want Ready", s.State, s.Err)
	}
	if n := b.Reconnects(); n != 1 || len(fired) != 1 {
		t.Errorf("an outage of failed dials counted %d reconnections and fired OnReconnect %d times, want 1 and 1", n, len(fired))
	}

	b, h = hooked()
	h.OnBrokerConnect(broker, time.Millisecond, nil, refused)
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	if n := b.Reconnects(); n != 0 {
		t.Errorf("the first connection after a first dial that failed counted %d reconnections, want 0", n)
	}
}

// TestAConnectionItsOwnClientCutShortIsNotALostLink: franz-go runs
// OnBrokerConnect for a connection's initialisation as well as its dial, and
// a client that closes while one of its connections is being made, a
// consumer's group client among them, fails that attempt with
// kgo.ErrClientClosed; a request cancelled while it dials fails it with the
// cancellation. The hook read every failed attempt as a lost link, so closing
// a consumer while its client connected left the backend Degraded until some
// later dial, and the next dial counted a reconnection. A dial that runs out
// of time is still a lost link: it is how an unreachable broker shows.
func TestAConnectionItsOwnClientCutShortIsNotALostLink(t *testing.T) {
	broker := kgo.BrokerMetadata{NodeID: 0}
	for _, err := range []error{
		kgo.ErrClientClosed,
		fmt.Errorf("unable to dial: %w", &net.OpError{Op: "dial", Net: "tcp", Err: context.Canceled}),
	} {
		b, h := hooked()
		h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
		h.OnBrokerConnect(broker, time.Millisecond, nil, err)
		if s := b.health.Snapshot(); s.State != mq.HealthReady {
			t.Errorf("Health after a connection attempt that failed with %v = %v (%v), want Ready", err, s.State, s.Err)
		}
		h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
		if n := b.Reconnects(); n != 0 {
			t.Errorf("the dial after an attempt that failed with %v counted %d reconnections, want 0", err, n)
		}
		if n := b.DegradedTransitions(); n != 0 {
			t.Errorf("an attempt that failed with %v counted %d degraded transitions, want 0", err, n)
		}
	}
	b, h := hooked()
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	h.OnBrokerConnect(broker, time.Millisecond, nil, fmt.Errorf("unable to dial: %w", &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}))
	if s := b.health.Snapshot(); s.State != mq.HealthDegraded {
		t.Errorf("Health after a dial that ran out of time = %v, want Degraded", s.State)
	}
}

// TestABackgroundSuccessClearsOnlyItsOwnPath: a background failure is
// cleared by the next success of the path it happened on, and by nothing else:
// not by another path's success, which would hide a path that keeps failing,
// whichever of the two failed first, and not by a new connection, which clears
// a lost link only. The health names the latest failure of a path still
// failing.
func TestABackgroundSuccessClearsOnlyItsOwnPath(t *testing.T) {
	broker := kgo.BrokerMetadata{NodeID: 0}
	errA, errB := errors.New("refused A"), errors.New("refused B")
	b, h := hooked()
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	var sinkA, sinkB healthPath
	state := func(want mq.HealthState, wantErr error, after string) {
		t.Helper()
		s := b.health.Snapshot()
		if s.State != want || !errors.Is(s.Err, wantErr) {
			t.Fatalf("Health after %s = %v (%v), want %v (%v)", after, s.State, s.Err, want, wantErr)
		}
	}
	b.failed(&sinkA, errA)
	b.recovered(&sinkB)
	state(mq.HealthDegraded, errA, "another path's success")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "the failing path's success")

	b.failed(&sinkA, errA)
	b.failed(&sinkB, errB)
	b.recovered(&sinkA)
	state(mq.HealthDegraded, errB, "the earlier failing path's success while the later one still fails")
	b.recovered(&sinkB)
	state(mq.HealthReady, nil, "both paths' successes")

	b.failed(&sinkA, errA)
	b.failed(&sinkB, errB)
	b.recovered(&sinkB)
	state(mq.HealthDegraded, errA, "the later failing path's success while the earlier one still fails")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "both paths' successes")

	b.failed(&sinkA, errA)
	h.OnBrokerE2E(broker, int16(kmsg.Produce), kgo.BrokerE2E{ReadErr: io.EOF})
	b.recovered(&sinkA)
	state(mq.HealthDegraded, errDisconnected, "a background success while a link is lost")
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	state(mq.HealthReady, nil, "a new connection")
	if n := b.Reconnects(); n != 1 {
		t.Errorf("%d reconnections counted, want 1: a background success is not one", n)
	}

	b.failed(&sinkA, errA)
	h.OnBrokerE2E(broker, int16(kmsg.Produce), kgo.BrokerE2E{ReadErr: io.EOF})
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	state(mq.HealthDegraded, errA, "a new connection while another path still fails")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "the failing path's success")
}

// TestAFailedRequestIsALostLink is the other half: a request whose write or
// read fails is the link failing, so it is Degraded, and the next connection
// is Ready and a reconnection. A request that ends because its own context
// did, or because its client is closing, is neither.
func TestAFailedRequestIsALostLink(t *testing.T) {
	broker := kgo.BrokerMetadata{NodeID: 0}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, kgo.ErrClientClosed} {
		b, h := hooked()
		h.OnBrokerE2E(broker, int16(kmsg.Produce), kgo.BrokerE2E{ReadErr: err})
		if s := b.health.Snapshot(); s.State != mq.HealthReady {
			t.Errorf("Health after a request that ended with %v = %v (%v), want Ready", err, s.State, s.Err)
		}
	}
	for _, e2e := range []kgo.BrokerE2E{{ReadErr: io.EOF}, {WriteErr: syscall.ECONNRESET}, {ReadErr: os.ErrDeadlineExceeded}} {
		b, h := hooked()
		h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
		h.OnBrokerE2E(broker, int16(kmsg.Fetch), e2e)
		if s := b.health.Snapshot(); s.State != mq.HealthDegraded {
			t.Errorf("Health after a request that failed with %v = %v, want Degraded", e2e.Err(), s.State)
		}
		h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
		if s := b.health.Snapshot(); s.State != mq.HealthReady {
			t.Errorf("Health after the connection was dialled again = %v (%v), want Ready", s.State, s.Err)
		}
		if n := b.Reconnects(); n != 1 {
			t.Errorf("the dial after a request that failed with %v counted %d reconnections, want 1", e2e.Err(), n)
		}
	}
}

// TestEnsureSubscriptionKeepsAPathOnlyWhileTheSubscriptionFails:
// EnsureSubscription made a health path for every subscription name it
// ensured, clean runs and runs on a topic that does not exist included, and
// only DeleteSubscription forgot one, so a long-lived backend that ensures
// generated names grew the map for its whole life. A subscription's path is
// kept only while its start position is failing, and forgotten once the
// subscription works.
func TestEnsureSubscriptionKeepsAPathOnlyWhileTheSubscriptionFails(t *testing.T) {
	const topic = "subpaths"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err := OpenBackend(t.Context(), Config{Brokers: c.ListenAddrs(), RequestTimeout: 3 * time.Second, Linger: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	adm, err := b.Admin()
	if err != nil {
		t.Fatal(err)
	}
	kept := func() int {
		b.healthMu.Lock()
		defer b.healthMu.Unlock()
		return len(b.subPaths)
	}
	const n = 200
	for i := range n {
		if err := adm.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "tenant-" + strconv.Itoa(i)}); err != nil {
			t.Fatalf("EnsureSubscription %d: %v", i, err)
		}
	}
	// A topic that does not exist is refused, and keeps no path either.
	if err := adm.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: "no-such-topic", Name: "tenant-missing"}); !errors.Is(err, mq.ErrTopicNotFound) {
		t.Fatalf("EnsureSubscription on a topic that does not exist = %v, want ErrTopicNotFound", err)
	}
	if h := b.Health(); h.State != mq.HealthReady {
		t.Fatalf("Health = %v (%v), want Ready: every run was clean", h.State, h.Err)
	}
	if k := kept(); k != 0 {
		t.Errorf("after %d EnsureSubscription calls with distinct names, clean or refused, %d subscription paths are kept, want 0", n+1, k)
	}

	// The next start-position commit is refused, and only that one.
	c.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r := req.(*kmsg.OffsetCommitRequest)
		resp := r.ResponseKind().(*kmsg.OffsetCommitResponse)
		resp.Version = r.Version
		for _, rt := range r.Topics {
			st := kmsg.NewOffsetCommitResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.UnknownServerError.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	spec := mq.SubscriptionSpec{Topic: topic, Name: "tenant-refused"}
	if err := adm.EnsureSubscription(t.Context(), spec); err != nil {
		t.Fatalf("EnsureSubscription with its commit refused: %v", err)
	}
	if h := b.Health(); h.State != mq.HealthDegraded {
		t.Fatalf("Health after a refused start position = %v, want Degraded", h.State)
	}
	if k := kept(); k != 1 {
		t.Errorf("%d subscription paths are kept while one subscription fails, want 1", k)
	}
	if err := adm.EnsureSubscription(t.Context(), spec); err != nil {
		t.Fatalf("EnsureSubscription again: %v", err)
	}
	if h := b.Health(); h.State != mq.HealthReady {
		t.Errorf("Health after the subscription's next clean run = %v (%v), want Ready", h.State, h.Err)
	}
	if k := kept(); k != 0 {
		t.Errorf("%d subscription paths are kept once the failing subscription works, want 0", k)
	}
}

// TestASequenceClampThatCannotReadTheLogIsReported: the clamp the assignment
// makes for a sequence start the open could not read reads the log once, and
// its failure was dropped without a word. The partition keeps the sequence
// itself, and the failure is reported on the source's commit path, as the
// open's own failed read is; a read the end of the group session cut short is
// not reported.
func TestASequenceClampThatCannotReadTheLogIsReported(t *testing.T) {
	const topic = "seqclamp"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err := OpenBackend(t.Context(), Config{Brokers: c.ListenAddrs(), RequestTimeout: 3 * time.Second, Linger: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	start := mq.StartAtSequence(1 << 40)
	s := &source{b: b, topic: topic, group: "g-seqclamp"}
	clamp := func(ctx context.Context) kgo.Offset {
		parts := map[int32]kgo.Offset{0: startOffset(start)}
		s.clampSequence(ctx, parts, []int32{0}, start)
		return parts[0]
	}

	if got := clamp(t.Context()); got != kgo.NewOffset().At(0) {
		t.Fatalf("a sequence beyond the end of an empty log clamped to %v, want At(0)", got)
	}
	if h := b.Health(); h.State != mq.HealthReady {
		t.Fatalf("Health after a clean clamp = %v (%v), want Ready", h.State, h.Err)
	}

	f := c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Topic: topic, Err: kerr.LeaderNotAvailable, Count: -1})
	defer f.Remove()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if got := clamp(cancelled); got != startOffset(start) {
		t.Errorf("a clamp whose session had ended moved the start to %v", got)
	}
	if h := b.Health(); h.State != mq.HealthReady {
		t.Errorf("Health after a clamp the end of the session cut short = %v (%v), want Ready", h.State, h.Err)
	}
	if got := clamp(t.Context()); got != startOffset(start) {
		t.Errorf("a clamp that could not read the log moved the start to %v", got)
	}
	if f.Hits() == 0 {
		t.Fatal("the clamp never met the refused listing, so the test proves nothing")
	}
	if h := b.Health(); h.State != mq.HealthDegraded || h.Err == nil {
		t.Errorf("Health after a clamp that could not read the log = %v (%v), want Degraded with the reason", h.State, h.Err)
	}
}
