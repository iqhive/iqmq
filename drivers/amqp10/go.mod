module bitbucket.org/iqhive/iqmq/drivers/amqp10

go 1.27.0

require (
	github.com/Azure/go-amqp v1.7.0
	bitbucket.org/iqhive/iqmq v0.0.0
	go.uber.org/goleak v1.3.0
)

require github.com/stretchr/testify v1.12.1 // indirect


replace bitbucket.org/iqhive/iqmq => ../..
