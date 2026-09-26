// The Redis-backed idempotency store is its own module so that the root
// module stays stdlib-only: x/idempotency defines the interface and the
// in-process store with no dependencies at all, and only this module links a
// Redis client.
module github.com/iqhive/iqmq/x/idempotency/redisstore

go 1.27.0

require (
	github.com/iqhive/iqmq v0.0.0
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/redis/rueidis v1.0.75
)

require (
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
)


replace github.com/iqhive/iqmq => ../../..
