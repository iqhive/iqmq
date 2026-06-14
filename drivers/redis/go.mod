module bitbucket.org/iqhive/iqmq/drivers/redis

go 1.26.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	github.com/redis/rueidis v1.0.75
	go.uber.org/goleak v1.3.0
)

require (
	github.com/stretchr/testify v1.11.1 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
)


replace bitbucket.org/iqhive/iqmq => ../..
