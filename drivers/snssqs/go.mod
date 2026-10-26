module github.com/iqhive/iqmq/drivers/snssqs

go 1.27.3

require (
	github.com/aws/aws-sdk-go-v2 v1.47.4
	github.com/aws/aws-sdk-go-v2/credentials v1.20.10
	github.com/aws/aws-sdk-go-v2/service/sns v1.47.6
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.6
	github.com/aws/smithy-go v1.28.6
	github.com/iqhive/iqmq v1.0.8
	github.com/iqhive/iqmq/drivers/sqs v1.0.8
	go.uber.org/goleak v1.3.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.7 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)
