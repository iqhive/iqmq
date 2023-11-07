module bitbucket.org/iqhive/iqmq/drivers/sqs

go 1.21.0

require bitbucket.org/iqhive/iqmq v0.7.0

replace bitbucket.org/iqhive/iqmq => ../..

require (
	github.com/aws/aws-sdk-go-v2 v1.3.1
	github.com/aws/aws-sdk-go-v2/service/sqs v1.3.0
)
