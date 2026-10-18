// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/sqs-fifo

go 1.27.0

require (
	github.com/iqhive/iqmq v0.9.1
	github.com/iqhive/iqmq/drivers/sqs v0.9.1
)

require (
	github.com/aws/aws-sdk-go-v2 v1.47.1 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.2 // indirect
	github.com/aws/smithy-go v1.28.4 // indirect
)

replace github.com/iqhive/iqmq => ../../

replace github.com/iqhive/iqmq/drivers/sqs => ../../drivers/sqs
