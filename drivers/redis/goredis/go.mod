module github.com/iqhive/iqmq/drivers/redis/goredis

go 1.27.0

require (
	github.com/iqhive/iqmq v0.0.0
	github.com/iqhive/iqmq/drivers/redis v0.0.0
	github.com/redis/go-redis/v9 v9.20.0
	github.com/redis/rueidis v1.0.75
	go.uber.org/goleak v1.3.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
)



replace github.com/iqhive/iqmq => ../../..

replace github.com/iqhive/iqmq/drivers/redis => ./..
