// The Redis-backed idempotency store is its own module so that the root
// module stays stdlib-only: x/idempotency defines the interface and the
// in-process store with no dependencies at all, and only this module links a
// Redis client.
module github.com/iqhive/iqmq/x/idempotency/redisstore

go 1.27.3

require (
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/iqhive/iqmq v1.0.8
	github.com/redis/rueidis v1.0.79
)

require (
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)
