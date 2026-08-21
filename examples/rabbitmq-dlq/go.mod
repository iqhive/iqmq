// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module bitbucket.org/iqhive/iqmq/examples/rabbitmq-dlq

go 1.27.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	bitbucket.org/iqhive/iqmq/drivers/rabbitmq v0.0.0
)

require github.com/rabbitmq/amqp091-go v1.11.0 // indirect



replace bitbucket.org/iqhive/iqmq => ../..

replace bitbucket.org/iqhive/iqmq/drivers/rabbitmq => ../../drivers/rabbitmq
