// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/sqs-fifo

go 1.27.0

require (
	github.com/iqhive/iqmq v0.0.0
	github.com/iqhive/iqmq/drivers/sqs v0.0.0
)

require (
	github.com/aws/aws-sdk-go-v2 v1.41.9 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.19 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.29 // indirect
	github.com/aws/smithy-go v1.26.0 // indirect
)



replace github.com/iqhive/iqmq => ../..

replace github.com/iqhive/iqmq/drivers/sqs => ../../drivers/sqs
