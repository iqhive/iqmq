module bitbucket.org/iqhive/iqmq/drivers/nsq

go 1.27.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	github.com/nsqio/go-nsq v1.1.0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/golang/snappy v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
)


replace bitbucket.org/iqhive/iqmq => ../..
