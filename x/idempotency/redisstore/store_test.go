package redisstore_test

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"

	"github.com/iqhive/iqmq/x/idempotency"
	"github.com/iqhive/iqmq/x/idempotency/redisstore"
	"github.com/iqhive/iqmq/x/idempotency/storetest"
)

// brokerEnv is conformance/harness's convention: IQMQ_<BROKER>_ADDR names an
// externally managed broker as host:port.
const brokerEnv = "IQMQ_REDIS_ADDR"

// TestContractAgainstMiniredis runs the whole store contract with no container
// and no network broker, so this package's behaviour is checked on every
// `make ci`.
//
// miniredis is an instrument and is therefore part of the code under test. Two
// things keep it honest. It executes the SAME Lua scripts this package sends,
// through the same rueidis client, so the compare-and-set is really being
// evaluated rather than simulated; and TestContractAgainstARealRedis below
// runs the identical suite against a real server, which is what says
// miniredis did not lie. The specification ran that arm against the pinned
// redis@sha256:ff02b58f image once and recorded the result -- a run here with the variable set repeats it.
func TestContractAgainstMiniredis(t *testing.T) {
	storetest.Run(t, storetest.Harness{
		Name: "miniredis",
		New: func(tb testing.TB) idempotency.Store {
			srv := miniredis.RunT(tb.(*testing.T))
			return openStore(tb, srv)
		},
		// miniredis has no clock of its own that rueidis can see, and its
		// FastForward is the method that expires keys, so Elapse reaches for
		// the server. It is keyed off the store because the suite builds one
		// per case.
		Elapse: func(tb testing.TB, s idempotency.Store, d time.Duration) {
			tb.Helper()
			srv := serverFor(tb, s)
			srv.FastForward(d)
		},
	})
}

// TestContractAgainstARealRedis is the broker arm, gated on IQMQ_REDIS_ADDR.
//
// It skips when no server is configured and asserts EXACTLY what the miniredis
// arm asserts when one is, so a real Redis that behaves differently is a
// failure here and not a skip. The window really passes -- the suite's Elapse
// is a wall-clock sleep, because Redis's TTL is Redis's clock and there is no
// other way to be on the far side of it.
func TestContractAgainstARealRedis(t *testing.T) {
	addr := os.Getenv(brokerEnv)
	if addr == "" {
		if os.Getenv("IQMQ_REQUIRE_BROKERS") == "1" {
			t.Fatalf("%s is unset and IQMQ_REQUIRE_BROKERS=1", brokerEnv)
		}
		t.Skipf("%s is unset: start a Redis (see README.md) and set it to host:port to run the same contract against a real server", brokerEnv)
	}
	storetest.Run(t, storetest.Harness{
		Name: "redis",
		New: func(tb testing.TB) idempotency.Store {
			// A prefix per case, because a real server is shared between
			// cases AND between runs: a key an earlier run left behind would
			// be read as this run's duplicate.
			return openPrefixed(tb, addr, "iqmqtest:"+strings.ReplaceAll(tb.Name(), "/", ":")+":"+
				time.Now().Format("150405.000000000")+":")
		},
		// Nil Elapse: the suite sleeps, which is what a real TTL needs.
	})
}

// servers maps a store to the miniredis it was built over. The suite hands
// Elapse the Store and nothing else, and a package-level map is the smallest
// thing that closes the gap; it is written and read from the test goroutine of
// one case at a time, and each case builds exactly one store.
var servers = map[idempotency.Store]*miniredis.Miniredis{}

func openStore(tb testing.TB, srv *miniredis.Miniredis) idempotency.Store {
	tb.Helper()
	s := openPrefixed(tb, srv.Addr(), redisstore.DefaultPrefix)
	servers[s] = srv
	tb.Cleanup(func() { delete(servers, s) })
	return s
}

func openPrefixed(tb testing.TB, addr, prefix string) idempotency.Store {
	tb.Helper()
	s, err := redisstore.Open(tb.Context(), redisstore.Options{
		Addrs:  []string{addr},
		Prefix: prefix,
	})
	if err != nil {
		tb.Fatalf("redisstore.Open(%q): %v", addr, err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

func serverFor(tb testing.TB, s idempotency.Store) *miniredis.Miniredis {
	tb.Helper()
	srv, ok := servers[s]
	if !ok {
		tb.Fatalf("no miniredis was recorded for %T", s)
	}
	return srv
}

// TestOpenOwnsItsClientAndNewDoesNot is the specification's ownership rule, and it is a
// test because getting it wrong is silent in one direction: a store that
// closed a borrowed client would break whatever else the process was using it
// for, and nothing in this package would notice.
func TestOpenOwnsItsClientAndNewDoesNot(t *testing.T) {
	srv := miniredis.RunT(t)
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{srv.Addr()}, DisableCache: true,
	})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	defer client.Close()

	borrowed, err := redisstore.New(client, redisstore.Options{Prefix: "borrowed:"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := borrowed.Claim(t.Context(), "k", time.Minute); err != nil {
		t.Fatalf("Claim through the borrowed client: %v", err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := borrowed.Close(); err != nil {
		t.Errorf("a second Close reported %v, want nil", err)
	}
	// The client must still work: the store borrowed it.
	if err := client.Do(t.Context(), client.B().Ping().Build()).Error(); err != nil {
		t.Fatalf("the borrowed client is unusable after the store's Close (%v); the specification closes what a handle opened and nothing else", err)
	}

	owned, err := redisstore.Open(t.Context(), redisstore.Options{Addrs: []string{srv.Addr()}, Prefix: "owned:"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := owned.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := owned.Claim(t.Context(), "k", time.Minute); !errors.Is(err, idempotency.ErrClosed) {
		t.Errorf("Claim on a closed owned store = %v, want ErrClosed", err)
	}
}

// TestAPrefixCollisionIsReportedAndNotGuessed is the one direction of this
// store that could DISCARD work, held to reporting instead.
//
// A value under this store's prefix that this store did not write means two
// things share a key space. Reading it as a duplicate would ack a message
// nobody processed; reading it as "first" would hand out a claim on somebody
// else's key. So it is an error, and the caller's OnUnavailable policy decides.
func TestAPrefixCollisionIsReportedAndNotGuessed(t *testing.T) {
	srv := miniredis.RunT(t)
	s, err := redisstore.Open(t.Context(), redisstore.Options{
		Addrs: []string{srv.Addr()}, Prefix: "shared:",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	// Somebody else's value, in this store's key space.
	if err := srv.Set("shared:order-1", "{\"cached\":true}"); err != nil {
		t.Fatalf("seeding the collision: %v", err)
	}
	got, err := s.Claim(t.Context(), "order-1", time.Minute)
	if err == nil {
		t.Fatalf("Claim over a foreign value answered %v with no error", got.Status)
	}
	if got.Status != idempotency.StatusUnknown {
		t.Errorf("Claim over a foreign value answered %v, want StatusUnknown so that OnUnavailable decides", got.Status)
	}
	if !strings.Contains(err.Error(), "did not write") {
		t.Errorf("the error is %q; it has to name the collision, because the fix is a Prefix and nothing else", err)
	}
}

// TestEvictionPolicyIsReadFromTheServer covers the guard a program uses to
// refuse to start against a Redis that will silently delete its claims, and it
// covers BOTH answers, because the interesting one is the refusal.
//
// miniredis implements no CONFIG command, which is the same shape as the
// managed Redis offerings that disable it: EvictionPolicy must report that as
// an error rather than returning "noeviction", because inventing the safe
// answer is how a program comes to believe it checked.
func TestEvictionPolicyIsReadFromTheServer(t *testing.T) {
	t.Run("a server that will not say", func(t *testing.T) {
		srv := miniredis.RunT(t)
		s, err := redisstore.Open(t.Context(), redisstore.Options{Addrs: []string{srv.Addr()}})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = s.Close() }()
		policy, err := s.EvictionPolicy(t.Context())
		if err == nil {
			t.Fatalf("EvictionPolicy answered %q against a server with no CONFIG command; a caller who cannot ask must be told so", policy)
		}
		if policy != "" {
			t.Errorf("EvictionPolicy returned %q beside its error", policy)
		}
		// And RequireSafeEviction must then refuse to build the store at all,
		// rather than carrying on with an unchecked server.
		if _, err := redisstore.Open(t.Context(), redisstore.Options{
			Addrs: []string{srv.Addr()}, RequireSafeEviction: true,
		}); err == nil {
			t.Fatal("Open with RequireSafeEviction succeeded against a server that would not answer CONFIG GET")
		}
	})
	t.Run("a real server", func(t *testing.T) {
		addr := os.Getenv(brokerEnv)
		if addr == "" {
			t.Skipf("%s is unset: this arm reads a real server's maxmemory-policy", brokerEnv)
		}
		s, err := redisstore.Open(t.Context(), redisstore.Options{Addrs: []string{addr}})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = s.Close() }()
		policy, err := s.EvictionPolicy(t.Context())
		if err != nil {
			t.Fatalf("EvictionPolicy: %v", err)
		}
		if policy == "" {
			t.Fatal("EvictionPolicy answered the empty string with no error")
		}
		t.Logf("maxmemory-policy = %q, safe for a claim store = %v", policy, redisstore.SafeEvictionPolicy(policy))
		if !redisstore.SafeEvictionPolicy(policy) {
			// Not a failure: the server is whatever the operator configured,
			// and this is the finding rather than the defect.
			t.Logf("NOTE: this server can evict a claim inside its window; Options.RequireSafeEviction would refuse to start against it")
		}
	})
}
