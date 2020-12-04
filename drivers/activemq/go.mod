module bitbucket.org/iqhive/iqmq/drivers/activemq

go 1.15

require bitbucket.org/iqhive/iqmq v0.0.0

replace bitbucket.org/iqhive/iqmq => ../..

require pack.ag/amqp v0.11.0

require bitbucket.org/iqhive/iqmq/drivers/amqp10 v0.0.0

replace bitbucket.org/iqhive/iqmq/drivers/amqp10 => ../amqp10
