module github.com/iqhive/iqmq/drivers/snssqs

go 1.27.0

require (
	github.com/iqhive/iqmq v0.0.0
	github.com/iqhive/iqmq/drivers/sqs v0.0.0
	github.com/aws/aws-sdk-go-v2 v1.41.9
	github.com/aws/aws-sdk-go-v2/credentials v1.19.19
	github.com/aws/aws-sdk-go-v2/service/sns v1.39.19
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.29
	github.com/aws/smithy-go v1.26.0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.25 // indirect
)



replace github.com/iqhive/iqmq => ../..

replace github.com/iqhive/iqmq/drivers/sqs => ../sqs
