module bitbucket.org/iqhive/iqmq/drivers/snssqs

go 1.19

require bitbucket.org/iqhive/iqmq v0.7.0

replace bitbucket.org/iqhive/iqmq => ../..

require (
	github.com/aws/aws-sdk-go-v2 v1.3.0
	github.com/aws/aws-sdk-go-v2/service/sns v1.2.0
)
