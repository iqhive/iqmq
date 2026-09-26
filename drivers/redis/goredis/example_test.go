package goredis_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/dsn"
)

// Example shows the shortest path to a client over go-redis.
//
// The configuration type is `drivers/redis`'s, so a program that already
// configures the Redis driver changes one import and one call to change client
// library, and nothing else.
func Example() {
	cfg := goredis.Config{
		Addrs:       []string{"127.0.0.1:6379"},
		Password:    dsn.Secret("hunter2"),
		Partitions:  4,
		AckDeadline: 30 * time.Second,
		AutoCreate:  true,
	}
	if err := cfg.Validate(); err != nil {
		fmt.Println("configuration:", err)
		return
	}
	// The rendering is the configuration with the credential redacted, which
	// is the rule and `drivers/redis`'s implementation of it: the same
	// Config type means the same rendering. Nack prints "auto" because an
	// unset strategy is the bake-off's binding rather than a caller's choice
	// a DSN cannot name it.
	fmt.Println(strings.Contains(cfg.DSN(), "hunter2"), cfg.Partitions, cfg.Nack)
	// Output: false 4 auto
}

// Example_dsn shows that the DSN is the broker's and not the client library's:
// a string naming "redis" parses here and there to the same configuration, and
// which library serves it is the import you chose.
func Example_dsn() {
	cfg, err := goredis.ParseDSN("redis://127.0.0.1:6379/0?partitions=2&nack=zsetmover")
	if err != nil {
		fmt.Println("dsn:", err)
		return
	}
	fmt.Println(cfg.Partitions, cfg.Nack)
	// Output: 2 zsetmover
}

// Example_openWithClient is the way in for anything the typed configuration
// cannot carry: a Sentinel or Ring topology, a pool size, a custom dialler, a
// hook. Build the client the deployment needs and say who closes it.
func Example_openWithClient() {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:      []string{"127.0.0.1:6379"},
		PoolSize:   32,
		MaxRetries: -1, // a retried XADD is a duplicate;
	})
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := goredis.OpenWithClient(ctx, client, driverkit.ClientBorrowed, goredis.Config{AutoCreate: true})
	if err != nil {
		// Unreachable in a doc example with no server; printing the shape is
		// the point.
		fmt.Println("open:", mq.CodeOf(err) != 0)
		return
	}
	defer func() { _ = c.Close() }()
	fmt.Println(c.Info().ClientName)
}

var _ = rd.DriverName
