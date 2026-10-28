module github.com/iqhive/iqmq/drivers/sqs

go 1.27.3

require (
	github.com/aws/aws-sdk-go-v2 v1.47.5
	github.com/aws/aws-sdk-go-v2/credentials v1.20.12
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.7
	github.com/aws/smithy-go v1.28.7
	github.com/iqhive/iqmq v1.1.0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.8 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
)
