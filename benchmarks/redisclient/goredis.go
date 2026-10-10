package redisclient

import (
	goredisclient "github.com/redis/go-redis/v9"

	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
)

// newGoRedis builds the go-redis arm through the driver's own constructor,
// with the driver's own options.
//
// Two of those options are decisions the driver had to make and are part of
// what is being measured: retries are off (a retried XADD is a duplicate the
// caller is told is one publication) and the read timeout clears the longest
// BLOCK (go-redis's default is five seconds and so is the driver's block, and
// the two raced). The pool size is deliberately go-redis's own default,
// because the connection model IS the difference this family measures.
func newGoRedis(addr string) (Client, error) {
	client := goredisclient.NewUniversalClient(goredis.UniversalOptions(rd.Config{Addrs: []string{addr}}))
	cmder, err := goredis.NewCommander(client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return cmder, nil
}
