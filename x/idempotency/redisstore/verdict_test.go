package redisstore

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/x/idempotency"
)

// TestVerdictOf is the reply-to-verdict table.
//
// It is the half of this package that runs with no Redis at all, and it covers
// the shapes a server cannot produce as well as the three it can: a decoder
// that fell through to a zero Claim on an unexpected reply would report
// StatusUnknown with a nil error, which the middleware reads as "the store
// could not answer" and — on the default policy — processes. So each of these
// must be an ERROR and not a silent status.
func TestVerdictOf(t *testing.T) {
	const (
		key   = "order-1"
		token = "deadbeef"
	)
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	window := 30 * time.Second
	cases := []struct {
		name    string
		reply   []string
		want    idempotency.Status
		wantErr string // a substring, or "" for no error
	}{
		{
			name:  "a fresh key is first and carries the token and the window",
			reply: []string{"first"},
			want:  idempotency.StatusFirst,
		},
		{
			name:  "a committed value is a duplicate",
			reply: []string{"exists", "d:" + token},
			want:  idempotency.StatusDuplicate,
		},
		{
			name:  "an open value is in flight and never a duplicate",
			reply: []string{"exists", "p:someone-else"},
			want:  idempotency.StatusInFlight,
		},
		{
			name:    "a value this store did not write is an error, not a guess",
			reply:   []string{"exists", "some other service's value"},
			want:    idempotency.StatusUnknown,
			wantErr: "did not write",
		},
		{
			name:    "exists with no value is an error",
			reply:   []string{"exists"},
			want:    idempotency.StatusUnknown,
			wantErr: "returned no value",
		},
		{
			name:    "the gone branch is mapped rather than dropped",
			reply:   []string{"gone"},
			want:    idempotency.StatusUnknown,
			wantErr: "script atomicity",
		},
		{
			name:    "an empty reply is an error",
			reply:   nil,
			want:    idempotency.StatusUnknown,
			wantErr: "empty reply",
		},
		{
			name:    "an unknown verdict is an error",
			reply:   []string{"probably-fine"},
			want:    idempotency.StatusUnknown,
			wantErr: "unknown verdict",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := verdictOf(key, token, window, now, c.reply)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("verdictOf(%q) reported %v", c.reply, err)
				}
			} else {
				if err == nil {
					t.Fatalf("verdictOf(%q) reported no error, want one containing %q", c.reply, c.wantErr)
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("verdictOf(%q) reported %q, want it to contain %q", c.reply, err, c.wantErr)
				}
			}
			if got.Status != c.want {
				t.Fatalf("verdictOf(%q) answered %v, want %v", c.reply, got.Status, c.want)
			}
			if c.want != idempotency.StatusFirst {
				if got.Token != "" || got.Window != 0 {
					t.Errorf("a %v verdict carries token %q and window %v; only a held claim has either", got.Status, got.Token, got.Window)
				}
				return
			}
			if got.Token != token {
				t.Errorf("the held claim's token is %q, want %q", got.Token, token)
			}
			if got.Window != window {
				t.Errorf("the held claim's window is %v, want %v: Commit re-bases on it", got.Window, window)
			}
			if !got.Expires.Equal(now.Add(window)) {
				t.Errorf("the held claim expires at %v, want %v", got.Expires, now.Add(window))
			}
		})
	}
}

// TestCasResult pins the mapping the whole two-phase claim rests on: the
// script's 0 is ErrClaimLost and nothing else.
func TestCasResult(t *testing.T) {
	if err := casResult(1, nil); err != nil {
		t.Errorf("casResult(1) = %v, want nil", err)
	}
	if err := casResult(0, nil); !errors.Is(err, idempotency.ErrClaimLost) {
		t.Errorf("casResult(0) = %v, want ErrClaimLost", err)
	}
	boom := errors.New("connection refused")
	err := casResult(0, boom)
	if !errors.Is(err, boom) {
		t.Errorf("casResult with a transport error = %v, want it to wrap the transport error", err)
	}
	if errors.Is(err, idempotency.ErrClaimLost) {
		t.Error("a transport error was reported as ErrClaimLost: a store that cannot answer must not say the claim was taken")
	}
}

// TestWindowMillisRoundsUp is the rounding rule, in the direction the
// documentation promises.
//
// Rounding DOWN would give a window shorter than the caller asked for, which
// is a duplicate the caller was told would be caught. `PX 0` is an error at
// the server and a negative `PX` deletes the key, so a non-positive duration
// becomes one millisecond.
func TestWindowMillisRoundsUp(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want int64
	}{
		{30 * time.Second, 30_000},
		{time.Millisecond, 1},
		{time.Millisecond + time.Nanosecond, 2},
		{1500 * time.Microsecond, 2},
		{time.Nanosecond, 1},
		{0, 1},
		{-time.Second, 1},
	}
	for _, c := range cases {
		if got := windowMillis(c.in); got != c.want {
			t.Errorf("windowMillis(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestSafeEvictionPolicy is the surprising half of the eviction story: only
// `noeviction` is safe, and every `volatile-*` policy is not.
//
// "volatile" reads like the careful choice. It means "evict only keys that
// have a TTL", and every key this store writes has one, so a volatile policy
// evicts exactly this store's keys and leaves a cache's un-expiring ones
// alone — the opposite of what a claim store needs.
func TestSafeEvictionPolicy(t *testing.T) {
	safe := []string{"noeviction", "NOEVICTION", " noeviction "}
	for _, p := range safe {
		if !SafeEvictionPolicy(p) {
			t.Errorf("SafeEvictionPolicy(%q) = false, want true", p)
		}
	}
	unsafe := []string{
		"allkeys-lru", "allkeys-lfu", "allkeys-random",
		"volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl",
		"", "something-new",
	}
	for _, p := range unsafe {
		if SafeEvictionPolicy(p) {
			t.Errorf("SafeEvictionPolicy(%q) = true, want false: it can delete a claim inside its window", p)
		}
	}
}

// TestPrefixIsNamespacedAndDefaulted covers the one option whose absence makes
// two services delete each other's claims.
func TestPrefixIsNamespacedAndDefaulted(t *testing.T) {
	if got := prefixOf(Options{}); got != DefaultPrefix {
		t.Errorf("prefixOf(zero) = %q, want %q", got, DefaultPrefix)
	}
	if got := prefixOf(Options{Prefix: "svc:"}); got != "svc:" {
		t.Errorf("prefixOf = %q, want %q", got, "svc:")
	}
	if !strings.HasPrefix(DefaultPrefix, "iqmq:") {
		t.Errorf("DefaultPrefix is %q; a SCAN MATCH iqmq:* has to find this store's keys", DefaultPrefix)
	}
}

// TestConstructorRefusals: neither constructor invents a connection.
func TestConstructorRefusals(t *testing.T) {
	if _, err := New(nil, Options{}); !errors.Is(err, ErrNoClient) {
		t.Errorf("New(nil) = %v, want ErrNoClient", err)
	}
	if _, err := Open(t.Context(), Options{}); !errors.Is(err, ErrNoAddrs) {
		t.Errorf("Open with no address = %v, want ErrNoAddrs", err)
	}
}

// TestTheScriptsAreCompareAndSet reads the scripts themselves, because the
// token comparison is the whole reason they are scripts.
//
// A well-meaning simplification — a plain SET in Commit, a plain DEL in
// Abort — would pass every other test in this package against a store nobody
// else was using, and would corrupt another worker's claim in production. This
// is the check that notices.
func TestTheScriptsAreCompareAndSet(t *testing.T) {
	for _, s := range []struct{ name, text string }{
		{"commit", commitScript},
		{"abort", abortScript},
	} {
		if !strings.Contains(s.text, "redis.call('GET', KEYS[1])") {
			t.Errorf("the %s script does not read the current value, so it cannot compare the token", s.name)
		}
		if !strings.Contains(s.text, "ARGV[1]") {
			t.Errorf("the %s script does not mention the token argument", s.name)
		}
		if !strings.Contains(s.text, "return 0") {
			t.Errorf("the %s script has no refusal branch, so casResult can never report ErrClaimLost", s.name)
		}
	}
	if !strings.Contains(claimScript, "'NX'") {
		t.Error("the claim script's SET is not conditional, so two workers would both be told they were first")
	}
	if !strings.Contains(claimScript, "'PX'") {
		t.Error("the claim script's SET sets no expiry, so the claim table grows without bound")
	}
}
