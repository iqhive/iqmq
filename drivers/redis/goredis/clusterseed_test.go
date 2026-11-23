package goredis_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// clusterMaster is one of the two masters of an in-memory Redis Cluster whose
// masters share one data set. It owns the slots lo to hi, answers MOVED to
// the other master for a key on any other slot, and answers INFO cluster and
// CLUSTER SLOTS as a cluster node does. mu guards the addresses, which are
// known only once both masters listen.
type clusterMaster struct {
	*fakestreams.Server
	lo, hi int

	mu    sync.Mutex
	other string
	slots streams.Value
}

func (m *clusterMaster) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	m.mu.Lock()
	other, slots := m.other, m.slots
	m.mu.Unlock()
	switch verb := strings.ToUpper(c.Tokens[0]); {
	case verb == "INFO" && len(c.Tokens) > 1 && strings.EqualFold(c.Tokens[1], "cluster"):
		return streams.Str("# Cluster\r\ncluster_enabled:1\r\n"), nil
	case verb == "CLUSTER" && len(c.Tokens) > 1 && strings.EqualFold(c.Tokens[1], "slots"):
		return slots, nil
	}
	if key := firstKeyOf(c.Tokens); key != "" {
		if slot := slotOf(key); slot < m.lo || slot > m.hi {
			return streams.Nil(), errors.New("MOVED " + strconv.Itoa(slot) + " " + other)
		}
	}
	return m.Server.Do(ctx, c)
}

// firstKeyOf is the key a command the driver sends names first, or "" for
// a command that names none.
func firstKeyOf(tokens []string) string {
	switch strings.ToUpper(tokens[0]) {
	case "XADD", "XAUTOCLAIM", "XACK", "XPENDING", "XCLAIM", "XLEN", "XRANGE", "EXISTS", "DEL":
		if len(tokens) > 1 {
			return tokens[1]
		}
	case "XGROUP", "XINFO":
		if len(tokens) > 2 {
			return tokens[2]
		}
	case "EVALSHA", "EVAL":
		if len(tokens) > 3 && tokens[2] != "0" {
			return tokens[3]
		}
	case "XREADGROUP":
		for i, tok := range tokens {
			if strings.EqualFold(tok, "STREAMS") && i+1 < len(tokens) {
				return tokens[i+1]
			}
		}
	}
	return ""
}

// slotOf is Redis Cluster's hash slot of key: CRC16 (XMODEM) of its hash tag,
// or of the whole key when it has none, modulo 16384.
func slotOf(key string) int {
	if open := strings.IndexByte(key, '{'); open >= 0 {
		if n := strings.IndexByte(key[open+1:], '}'); n > 0 {
			key = key[open+1 : open+1+n]
		}
	}
	var crc uint16
	for i := range len(key) {
		crc ^= uint16(key[i]) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return int(crc) % 16384
}

// serveCluster starts the two masters and returns the address of the first,
// the seed, which owns the slot of the probe the driver opens with, as the
// seed of the real cluster that reproduced this did.
func serveCluster(t *testing.T) (seed string, fake *fakestreams.Server, onSeed func(topic string) bool) {
	t.Helper()
	fake = fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	a, b := &clusterMaster{Server: fake, lo: 0, hi: 8191}, &clusterMaster{Server: fake, lo: 8192, hi: 16383}
	if slotOf("{iqmq-xautoclaim-probe}") > 8191 {
		a, b = b, a
	}
	addr := func(m *clusterMaster) (string, string) {
		srv, err := respsrv.Serve(m, respsrv.Options{Version: "7.4.2"})
		if err != nil {
			t.Fatalf("respsrv.Serve: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		host, port, _ := strings.Cut(srv.Addr(), ":")
		return host, port
	}
	aHost, aPort := addr(a)
	bHost, bPort := addr(b)
	node := func(m *clusterMaster, host, port string) streams.Value {
		return streams.Arr(streams.Int(int64(m.lo)), streams.Int(int64(m.hi)),
			streams.Arr(streams.Str(host), streams.Str(port), streams.Str("node-"+port)))
	}
	slots := streams.Arr(node(a, aHost, aPort), node(b, bHost, bPort))
	for _, m := range []struct {
		m     *clusterMaster
		other string
	}{{a, bHost + ":" + bPort}, {b, aHost + ":" + aPort}} {
		m.m.mu.Lock()
		m.m.other, m.m.slots = m.other, slots
		m.m.mu.Unlock()
	}
	return aHost + ":" + aPort, fake, func(topic string) bool {
		slot := slotOf("{" + topic + "}")
		return slot >= a.lo && slot <= a.hi
	}
}

// TestOneAddressOfAClusterIsACluster: Config.Addrs says one address is a
// standalone server or a cluster entry point, and drivers/redis finds the
// cluster from it. This driver gave the one address to go-redis's
// NewUniversalClient without IsClusterMode, which builds a single-node client,
// so every command for a topic whose slot another master owns was answered
// MOVED, which that client cannot follow, and reported as Unavailable and
// retryable on every retry. It now asks the one address whether it is a
// cluster node, and dials a cluster client when it is.
func TestOneAddressOfAClusterIsACluster(t *testing.T) {
	seed, fake, onSeed := serveCluster(t)
	cfg := baseConfig(fake)
	cfg.Addrs = []string{seed}
	b, err := goredis.OpenBackend(t.Context(), cfg)
	if err != nil {
		t.Fatalf("OpenBackend with one cluster seed: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	const topics = 12
	failed, elsewhere := 0, 0
	var sample error
	for i := range topics {
		topic := "seed.topic." + strconv.Itoa(i)
		if !onSeed(topic) {
			elsewhere++
		}
		sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
		if err == nil {
			err = sink.Publish(t.Context(), &mq.Message{Topic: topic, Body: []byte("x")}, nil)
			_ = sink.Close()
		}
		if err != nil {
			failed++
			sample = err
		}
	}
	if elsewhere == 0 {
		t.Fatal("every topic is on the seed's slots, so the test proves nothing")
	}
	if failed > 0 {
		t.Errorf("one cluster seed: %d of %d topics, %d of them on the other master, could not be published to; "+
			"e.g. %v (code %v, retryable %v)", failed, topics, elsewhere, sample, mq.CodeOf(sample), mq.IsRetryable(sample))
	}
}

// TestARedirectASingleNodeClientCannotFollowIsInvalidConfig: a single-node
// go-redis client handed to the driver for a cluster node cannot follow a
// MOVED, so the redirect fails every command for that slot whatever is
// retried. It was reported as the MOVED a cluster client meets while a slot
// moves, Unavailable and retryable, and a caller retried it for good. It is a
// definite CodeInvalidConfig that names the client to use, and a topic on the
// node's own slots still works.
func TestARedirectASingleNodeClientCannotFollowIsInvalidConfig(t *testing.T) {
	seed, fake, onSeed := serveCluster(t)
	single := redis.NewClient(goredis.UniversalOptions(rd.Config{Addrs: []string{seed}}).Simple())
	t.Cleanup(func() { _ = single.Close() })
	b, err := goredis.BackendWithClient(t.Context(), single, driverkit.ClientBorrowed, baseConfig(fake))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	here, there := "", ""
	for i := 0; here == "" || there == ""; i++ {
		topic := "single.topic." + strconv.Itoa(i)
		if onSeed(topic) {
			here = topic
		} else {
			there = topic
		}
	}
	publish := func(topic string) error {
		sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
		if err != nil {
			return err
		}
		defer func() { _ = sink.Close() }()
		return sink.Publish(t.Context(), &mq.Message{Topic: topic, Body: []byte("x")}, nil)
	}
	if err := publish(here); err != nil {
		t.Fatalf("a topic on the node's own slots: %v", err)
	}
	err = publish(there)
	if mq.CodeOf(err) != mq.CodeInvalidConfig || mq.IsRetryable(err) || mq.IsAmbiguous(err) ||
		!strings.Contains(err.Error(), "ClusterClient") {
		t.Errorf("a topic on another master's slot over a single-node client: %v (code %v, retryable %v, ambiguous %v), "+
			"want a definite CodeInvalidConfig naming the cluster client", err, mq.CodeOf(err), mq.IsRetryable(err), mq.IsAmbiguous(err))
	}
}
