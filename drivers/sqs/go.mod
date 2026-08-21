module bitbucket.org/iqhive/iqmq/drivers/sqs

go 1.27.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	github.com/aws/aws-sdk-go-v2 v1.41.9
	github.com/aws/aws-sdk-go-v2/credentials v1.19.19
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.29
	github.com/aws/smithy-go v1.26.0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.25 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
)


replace bitbucket.org/iqhive/iqmq => ../..
