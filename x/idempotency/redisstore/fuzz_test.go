package redisstore

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/x/idempotency"
)

// FuzzVerdictOf holds the claim script's reply decoder to its contract over
// arbitrary bytes.
//
// The decoder is the one place in this package where a REMOTE answer becomes a
// verdict, and the specification makes broker-delivered data untrusted — a Redis somebody
// else is also writing to, a proxy that rewrites a reply, a script cache that
// served a different script's SHA. The invariant that matters is the one that
// cannot be read off the code: a verdict and an error are exclusive, and the
// only status that may carry a claim is [idempotency.StatusFirst]. A decoder
// that fell through to a zero Claim with a nil error would report
// StatusUnknown as a VERDICT, which the middleware's default policy turns into
// "process it unguarded" — silently, for every message.
//
// The reply is decoded from the fuzz input rather than through
// internal/fuzzutil, which lives in the root module and cannot be imported
// from this one.
func FuzzVerdictOf(f *testing.F) {
	// The four replies a correct server produces, plus the shapes that make
	// the decoder choose. Each is written the way the script returns it: a
	// NUL-separated list of reply elements.
	for _, seed := range []string{
		"first",
		"exists\x00p:deadbeef",
		"exists\x00d:deadbeef",
		"exists\x00" + phaseOpen + "somebody-else",
		"exists\x00somebody else's cached value",
		"exists",
		"gone",
		"",
		"FIRST",
		"first\x00extra",
		"exists\x00p",
		"exists\x00d",
		"exists\x00:",
	} {
		f.Add([]byte(seed))
	}
	const (
		key    = "order-1"
		token  = "0123456789abcdef"
		window = 30 * time.Second
	)
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, data []byte) {
		// A reply is a list; the fuzzer supplies it NUL-separated. The count
		// is bounded because the decoder reads at most two elements and a
		// million of them would only slow the fuzzer down.
		parts := bytes.Split(data, []byte{0})
		if len(parts) > 8 {
			parts = parts[:8]
		}
		reply := make([]string, 0, len(parts))
		for _, p := range parts {
			reply = append(reply, string(p))
		}
		// bytes.Split of an empty input yields one empty element, which is a
		// reply of one empty string; the genuinely empty reply is the other
		// case the decoder has to answer, so both are exercised.
		if len(data) == 0 {
			reply = nil
		}

		got, err := verdictOf(key, token, window, now, reply)

		// 1. A verdict and an error are exclusive, in both directions.
		if err != nil && got.Status != idempotency.StatusUnknown {
			t.Fatalf("verdictOf(%q) reported status %v WITH an error (%v); a failure is not a verdict", reply, got.Status, err)
		}
		if err == nil {
			switch got.Status {
			case idempotency.StatusFirst, idempotency.StatusDuplicate, idempotency.StatusInFlight:
			default:
				t.Fatalf("verdictOf(%q) answered %v with a nil error; StatusUnknown as a verdict is read as 'the store answered' and the middleware then processes the message unguarded",
					reply, got.Status)
			}
		}

		// 2. Only a held claim carries a token and a window, and it carries
		//    exactly the ones it was given.
		if got.Status == idempotency.StatusFirst {
			if got.Token != token {
				t.Fatalf("verdictOf(%q) held a claim with token %q, want %q", reply, got.Token, token)
			}
			if got.Window != window {
				t.Fatalf("verdictOf(%q) held a claim with window %v, want %v: Commit re-bases on it", reply, got.Window, window)
			}
			if !got.Expires.Equal(now.Add(window)) {
				t.Fatalf("verdictOf(%q) held a claim expiring at %v, want %v", reply, got.Expires, now.Add(window))
			}
		} else if got.Token != "" || got.Window != 0 || !got.Expires.IsZero() {
			t.Fatalf("verdictOf(%q) answered %v carrying token=%q window=%v expires=%v; only a held claim has any of them",
				reply, got.Status, got.Token, got.Window, got.Expires)
		}

		// 3. The key is echoed unchanged, whatever the answer, so that a log
		//    line about a failure names the key it was about.
		if got.Key != "" && got.Key != key {
			t.Fatalf("verdictOf(%q) answered about key %q, want %q", reply, got.Key, key)
		}

		// 4. Only "first" can hold a claim. This is the property a decoder
		//    that read an unknown verdict optimistically would break, and it
		//    is the one that would hand out a claim on somebody else's key.
		if got.Status == idempotency.StatusFirst && (len(reply) == 0 || reply[0] != verdictFirst) {
			t.Fatalf("verdictOf(%q) held a claim without the server having said %q", reply, verdictFirst)
		}

		// 5. An error message never contains a NUL and never grows without
		//    bound: a value under this store's prefix that it did not write
		//    is somebody else's DATA, and the specification keeps payloads out of what
		//    iqmq reports.
		if err != nil {
			msg := err.Error()
			if strings.ContainsRune(msg, 0) {
				t.Fatalf("verdictOf(%q) reported an error containing a NUL byte: %q", reply, msg)
			}
			if len(msg) > 512 {
				t.Fatalf("verdictOf(%q) reported a %d-byte error; a foreign value is elided, not quoted at length", reply, len(msg))
			}
		}
	})
}
