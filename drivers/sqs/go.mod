module github.com/iqhive/iqmq/drivers/sqs

go 1.27.0

require (
	github.com/aws/aws-sdk-go-v2 v1.47.1
	github.com/aws/aws-sdk-go-v2/credentials v1.20.7
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.2
	github.com/aws/smithy-go v1.28.4
	github.com/iqhive/iqmq v0.9.1
	go.uber.org/goleak v1.3.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.4 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
)
