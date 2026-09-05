module bitbucket.org/iqhive/iqmq/drivers/activemq

go 1.27.0

require (
	github.com/Azure/go-amqp v1.7.0
	bitbucket.org/iqhive/iqmq v0.0.0
	bitbucket.org/iqhive/iqmq/drivers/amqp10 v0.0.0
	go.uber.org/goleak v1.3.0
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect



replace bitbucket.org/iqhive/iqmq => ../..

replace bitbucket.org/iqhive/iqmq/drivers/amqp10 => ../amqp10
