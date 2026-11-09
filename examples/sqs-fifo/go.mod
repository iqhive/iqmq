// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/sqs-fifo

go 1.27.3

require (
	github.com/iqhive/iqmq v1.1.2
	github.com/iqhive/iqmq/drivers/sqs v1.1.2
)

require (
	github.com/aws/aws-sdk-go-v2 v1.47.7 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.10 // indirect
	github.com/aws/smithy-go v1.28.8 // indirect
)

replace github.com/iqhive/iqmq => ../../

replace github.com/iqhive/iqmq/drivers/sqs => ../../drivers/sqs
