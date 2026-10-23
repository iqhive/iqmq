// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/sqs-fifo

go 1.27.0

require (
	github.com/iqhive/iqmq v1.0.6
	github.com/iqhive/iqmq/drivers/sqs v1.0.6
)

require (
	github.com/aws/aws-sdk-go-v2 v1.47.3 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.4 // indirect
	github.com/aws/smithy-go v1.28.5 // indirect
)

replace github.com/iqhive/iqmq => ../../

replace github.com/iqhive/iqmq/drivers/sqs => ../../drivers/sqs
