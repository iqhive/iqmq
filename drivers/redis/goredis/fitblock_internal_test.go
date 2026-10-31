package goredis

import (
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
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

// TestFitBlockReadsARingsTimeoutAndAssumesTheDefaultForAWrapper is the idle
// fetch that came back as an i/o timeout through a Ring or a wrapper.
//
// A Ring's Options are the raw ones it was built with, and a ReadTimeout left
// at zero there means go-redis's five seconds on every shard. fitBlock read
// the zero as "no read timeout" and kept the five-second block, so every idle
// fetch through a default Ring raced its socket deadline and failed. A client
// of a type the driver cannot read was treated the same way. Both now count
// as go-redis's default, and only a block the caller left at its default is
// changed for a client whose timeout is a guess.
func TestFitBlockReadsARingsTimeoutAndAssumesTheDefaultForAWrapper(t *testing.T) {
	ring := func(read time.Duration) redis.UniversalClient {
		return redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"a": "127.0.0.1:1"}, ReadTimeout: read})
	}
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
		{"ring, go-redis default, default block", ring, 0, 0, 4 * time.Second, false},
		{"ring, short timeout, default block", ring, time.Second, 0, 500 * time.Millisecond, false},
		{"ring, explicit block that does not fit", ring, 0, 5 * time.Second, 0, true},
		{"ring, no read timeout", ring, -1, 0, 0, false},
		{"ring, no read deadline", ring, -2, 0, 0, false},
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

// TestTheAssumedReadTimeoutIsGoRedisDefault holds the guess fitBlock makes for
// a Ring left at zero and for a wrapper to the go-redis release this module
// requires: a default client's initialised ReadTimeout.
func TestTheAssumedReadTimeoutIsGoRedisDefault(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	if got := client.Options().ReadTimeout; got != goRedisReadTimeout {
		t.Fatalf("go-redis's default ReadTimeout is %v and fitBlock assumes %v", got, goRedisReadTimeout)
	}
}
