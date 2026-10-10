// Package redisclient is Which client library the Redis
// Streams driver should be built.
//
// # Why the family lives here
//
// Its two arms are two MODULES. `drivers/redis` is the driver over rueidis and
// `drivers/redis/goredis` is the same driver over go-redis, so a package that
// can construct both has to be able to import both — and no package in the
// root module can, because the root module is standard-library-only
// (`make stdlib-check`, `depguard`'s stdlib-only rule). Every other family in
// the campaign is a set of types in one root-module package; this one cannot
// be, and therefore declares `MODDIR` so that the
// harness runs its `go test` in this module. the maintenance notes records the
// constraint from the other side (the specification is the same shape for Kafka).
//
// The `benchmarks` module is where it lands rather than a new one: it is the
// benchmarks module, it already depends on iqmq, and adding a module would add
// a `go.work` entry, a `replace` block per dependant and a second place to keep
// the client pins in step. It is deliberately NOT
// `benchmarks/native-baselines`, which was the first guess and is wrong on
// purpose: that module measures RAW clients, and `TestNoRootImport` there
// refuses any import of `github.com/iqhive/iqmq` at all, because a native
// baseline that reached into iqmq would no longer be measuring the client. This
// family's arms are the driver's own adapters, so they import iqmq by
// definition; the guard caught it, which is what it is for.
//
// # What the arms are, and what is held constant
//
// The seam is [github.com/iqhive/iqmq/internal/streams.Commander], which is
// the whole of what either driver needs from a client library. Above it —
// which key a topic's shard lives under, which fields an entry carries, which
// command a settlement sends, whether an acknowledgement is still the
// acknowledgement of the delivery it was handed — is identical code shared by
// both drivers, so measuring the DRIVER would measure that shared code twice
// and add its noise to the answer. The family therefore measures the adapter
// and the library: the same [github.com/iqhive/iqmq/internal/streams] command
// values, the same server, the same commands in the same order.
//
// The server is in-process: `internal/streams/fakestreams` behind
// `internal/streams/respsrv`, over loopback TCP. That is deliberate and it is
// what makes the numbers about the client. A container puts a real server's
// own work — its radix tree, its fsync policy, its scheduler — in front of
// every measurement, and that work is IDENTICAL for the two arms, so it is
// pure noise added to the quantity being compared; the criterion is
// "throughput/connection & allocs", both of which are the client library's.
// What the in-process server cannot measure is a real network's latency, and
// the driver page says so.
package redisclient

import (
	"errors"

	"github.com/iqhive/iqmq/internal/streams"
)

// Client is the family's interface core: what either client library provides
// to the driver above it, which is exactly [streams.Commander].
type Client = streams.Commander

// Kind names one client library.
type Kind uint8

// The client libraries compares. Rueidis is the reference and the
// expected winner; go-redis is the challenger. Both SHIP — the specification asks for both
// — so the family's decision is which one the driver page recommends and
// which one the version matrix is run against, not which one is deleted.
const (
	// Rueidis is github.com/redis/rueidis: one auto-pipelined connection per
	// node, every command multiplexed onto it, with a separate pool for
	// blocking commands.
	Rueidis Kind = iota
	// GoRedis is github.com/redis/go-redis/v9: a connection pool, one
	// connection taken per command, pipelining only when asked for.
	GoRedis

	kindCount
)

var kindNames = [kindCount]string{"rueidis", "goredis"}

var kindIdents = [kindCount]string{"Rueidis", "GoRedis"}

// String returns the kind's slug, which is the benchmark-name segment and the
// bake-off's identifier for the arm.
func (k Kind) String() string {
	if k < kindCount {
		return kindNames[k]
	}
	return "Kind(?)"
}

// Ident returns the Go identifier of the kind's constant, which is what
// `bakeoff decide` writes into the generated binding.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return "Kind(?)"
}

// Valid reports whether k names a client library.
func (k Kind) Valid() bool { return k < kindCount }

// Kinds returns every kind in declaration order.
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount)
	for k := Kind(0); k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// ParseKind reads a kind's slug.
func ParseKind(s string) (Kind, bool) {
	for k := Kind(0); k < kindCount; k++ {
		if kindNames[k] == s {
			return k, true
		}
	}
	return 0, false
}

// New dials addr with the named client library and returns the
// [streams.Commander] the driver would use.
//
// It returns the SHIPPED adapter in both cases — `drivers/redis.NewCommander`
// and `drivers/redis/goredis.NewCommander` — rather than an adapter written
// for the benchmark. An arm that measured a benchmark's own adapter would
// measure code nobody ships, and the adapter is half of what the family is
// comparing.
func New(k Kind, addr string) (Client, error) {
	switch k {
	case Rueidis:
		return newRueidis(addr)
	case GoRedis:
		return newGoRedis(addr)
	case kindCount:
	}
	return nil, ErrUnknownKind
}

// ErrUnknownKind is a kind outside the enumeration.
var ErrUnknownKind = errors.New("redisclient: unknown kind")
