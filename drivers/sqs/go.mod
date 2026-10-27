module github.com/iqhive/iqmq/drivers/sqs

go 1.27.3

require (
	github.com/aws/aws-sdk-go-v2 v1.47.3
	github.com/aws/aws-sdk-go-v2/credentials v1.20.9
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.4
	github.com/aws/smithy-go v1.28.5
	github.com/iqhive/iqmq v1.0.10
	go.uber.org/goleak v1.3.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.6 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
)
