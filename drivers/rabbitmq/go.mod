module bitbucket.org/iqhive/iqmq/drivers/rabbitmq

go 1.26.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	github.com/rabbitmq/amqp091-go v1.11.0
	go.uber.org/goleak v1.3.0
)

require github.com/stretchr/testify v1.11.1 // indirect


replace bitbucket.org/iqhive/iqmq => ../..
