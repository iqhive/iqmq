module bitbucket.org/iqhive/iqmq/drivers/rabbitmq/amqp10

go 1.27.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	bitbucket.org/iqhive/iqmq/drivers/amqp10 v0.0.0
	github.com/Azure/go-amqp v1.6.0
	go.uber.org/goleak v1.3.0
)




replace bitbucket.org/iqhive/iqmq => ../../..

replace bitbucket.org/iqhive/iqmq/drivers/amqp10 => ../../amqp10
