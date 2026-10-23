// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/rabbitmq-dlq

go 1.27.0

require (
	github.com/iqhive/iqmq v1.0.6
	github.com/iqhive/iqmq/drivers/rabbitmq v1.0.6
)

require github.com/rabbitmq/amqp091-go v1.15.0 // indirect

replace github.com/iqhive/iqmq => ../../

replace github.com/iqhive/iqmq/drivers/rabbitmq => ../../drivers/rabbitmq
