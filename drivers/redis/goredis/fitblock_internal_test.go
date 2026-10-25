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
