package goredis

import (
	"strings"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
)

// TestABorrowedClientsReadTimeoutBoundsTheBlock is the idle fetch that came
// back as an i/o timeout.
//
// A client this driver dials gets a read timeout that clears the block, and a
// borrowed one was never checked: go-redis's default read timeout and
// rd.DefaultBlock are both five seconds, so every fetch that waited the whole
// block failed. A default block is now shortened to fit, and a block the
// caller chose that cannot fit is refused rather than quietly changed.
func TestABorrowedClientsReadTimeoutBoundsTheBlock(t *testing.T) {
	for _, tc := range []struct {
		name      string
		read      time.Duration
		block     time.Duration
		wantBlock time.Duration
		wantErr   bool
	}{
		{"go-redis default, default block", 0, 0, 4 * time.Second, false},
		{"short timeout, default block", time.Second, 0, 500 * time.Millisecond, false},
		{"room to spare, default block", 30 * time.Second, 0, 0, false},
		{"explicit block that fits", 0, 2 * time.Second, 2 * time.Second, false},
		{"explicit block that does not fit", 0, 5 * time.Second, 0, true},
		{"no read timeout", -1, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", ReadTimeout: tc.read})
			t.Cleanup(func() { _ = client.Close() })
			got, err := fitBlock(client, rd.Config{Block: tc.block})
			if tc.wantErr {
				if mq.CodeOf(err) != mq.CodeInvalidConfig {
					t.Fatalf("fitBlock = %v, want an invalid-config refusal", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("fitBlock: %v", err)
			}
			if got.Block != tc.wantBlock {
				t.Fatalf("Block = %v, want %v", got.Block, tc.wantBlock)
			}
		})
	}
}

// wrappedClient is a caller's own UniversalClient around a *redis.Client: a
// client of a type this driver cannot read the options of.
type wrappedClient struct{ *redis.Client }

// TestFitBlockAssumesTheDefaultForAWrapper is the idle fetch that came back as
// an i/o timeout through a client of a type the driver cannot read the options
// of. fitBlock read it as "no read timeout" and kept the five-second block, so
// every idle fetch raced go-redis's five-second socket deadline and failed. It
// now counts as go-redis's default, and only a block the caller left at its
// default is changed for a client whose timeout is a guess.
func TestFitBlockAssumesTheDefaultForAWrapper(t *testing.T) {
	wrapped := func(read time.Duration) redis.UniversalClient {
		return wrappedClient{redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", ReadTimeout: read})}
	}
	for _, tc := range []struct {
		name      string
		client    func(time.Duration) redis.UniversalClient
		read      time.Duration
		block     time.Duration
		wantBlock time.Duration
		wantErr   bool
	}{
		{"wrapper, default block", wrapped, 0, 0, 4 * time.Second, false},
		{"wrapper, explicit block is the caller's", wrapped, 30 * time.Second, 10 * time.Second, 10 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := tc.client(tc.read)
			t.Cleanup(func() { _ = client.Close() })
			got, err := fitBlock(client, rd.Config{Block: tc.block})
			if tc.wantErr {
				if mq.CodeOf(err) != mq.CodeInvalidConfig {
					t.Fatalf("fitBlock = %v, want an invalid-config refusal", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("fitBlock: %v", err)
			}
			if got.Block != tc.wantBlock {
				t.Fatalf("Block = %v, want %v", got.Block, tc.wantBlock)
			}
		})
	}
}

// TestABorrowedClientsReadTimeoutBoundsTheDurableWait is a ConfirmDurable
// publication's WAITAOF through a client the caller built.
//
// go-redis reads the WAITAOF's reply under the client's ReadTimeout, and
// go-redis's default and rd.DefaultDurableTimeout are both five seconds, so a
// WAITAOF that did not reach its target answered just as the read gave up: an
// i/o timeout, reported ambiguous, and a dropped connection, in place of the
// server's own answer. A default DurableTimeout is now shortened to fit, by
// fitBlock's rule, and one the caller chose that cannot fit is refused.
func TestABorrowedClientsReadTimeoutBoundsTheDurableWait(t *testing.T) {
	plain := func(read time.Duration) redis.UniversalClient {
		return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", ReadTimeout: read})
	}
	cluster := func(read time.Duration) redis.UniversalClient {
		return redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}, ReadTimeout: read})
	}
	wrapped := func(read time.Duration) redis.UniversalClient {
		return wrappedClient{redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", ReadTimeout: read})}
	}
	for _, tc := range []struct {
		name    string
		client  func(time.Duration) redis.UniversalClient
		read    time.Duration
		durable time.Duration
		want    time.Duration
		wantErr bool
	}{
		{"go-redis default, default DurableTimeout", plain, 0, 0, 4 * time.Second, false},
		{"short timeout, default DurableTimeout", plain, time.Second, 0, 500 * time.Millisecond, false},
		{"room to spare, default DurableTimeout", plain, 30 * time.Second, 0, 0, false},
		{"explicit DurableTimeout that fits", plain, 0, 2 * time.Second, 2 * time.Second, false},
		{"explicit DurableTimeout that does not fit", plain, 0, 5 * time.Second, 0, true},
		{"cluster client, explicit DurableTimeout that does not fit", cluster, 3 * time.Second, 10 * time.Second, 0, true},
		{"no read timeout", plain, -1, 0, 0, false},
		{"no read timeout, explicit DurableTimeout", plain, -1, time.Minute, time.Minute, false},
		{"wrapper, default DurableTimeout", wrapped, 0, 0, 4 * time.Second, false},
		{"wrapper, explicit DurableTimeout is the caller's", wrapped, 0, 10 * time.Second, 10 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := tc.client(tc.read)
			t.Cleanup(func() { _ = client.Close() })
			got, err := fitDurable(client, rd.Config{DurableTimeout: tc.durable})
			if tc.wantErr {
				if mq.CodeOf(err) != mq.CodeInvalidConfig {
					t.Fatalf("fitDurable = %v, want an invalid-config refusal", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("fitDurable: %v", err)
			}
			if got.DurableTimeout != tc.want {
				t.Fatalf("DurableTimeout = %v, want %v", got.DurableTimeout, tc.want)
			}
		})
	}
}

// TestBackendWithClientRefusesADurableTimeoutItsClientCannotWaitFor: the
// refusal is made by the way in, before anything is sent, and leaves the
// caller's client open.
func TestBackendWithClientRefusesADurableTimeoutItsClientCannotWaitFor(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	_, err := BackendWithClient(t.Context(), client, driverkit.ClientBorrowed, rd.Config{DurableTimeout: 5 * time.Second})
	if mq.CodeOf(err) != mq.CodeInvalidConfig || !strings.Contains(err.Error(), "DurableTimeout") {
		t.Fatalf("BackendWithClient(a default client, DurableTimeout 5s) = %v, want an invalid-config refusal "+
			"naming DurableTimeout", err)
	}
}

// TestTheAssumedReadTimeoutIsGoRedisDefault holds the guess fitBlock makes for
// a wrapper to the go-redis release this module requires: a default client's
// initialised ReadTimeout.
func TestTheAssumedReadTimeoutIsGoRedisDefault(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	if got := client.Options().ReadTimeout; got != goRedisReadTimeout {
		t.Fatalf("go-redis's default ReadTimeout is %v and fitBlock assumes %v", got, goRedisReadTimeout)
	}
}
