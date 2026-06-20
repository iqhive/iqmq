// The S3-compatible claim-check store is its own module so that the root
// module stays stdlib-only: x/claimcheck defines the Store interface, the
// reference and the file and memory stores with no dependencies at all, and
// only this module links the AWS SDK.
module bitbucket.org/iqhive/iqmq/x/claimcheck/s3store

go 1.26.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	github.com/aws/aws-sdk-go-v2 v1.41.9
	github.com/aws/aws-sdk-go-v2/credentials v1.19.19
	github.com/aws/aws-sdk-go-v2/service/s3 v1.102.2
	github.com/aws/smithy-go v1.26.0
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.26 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.18 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.25 // indirect
)


replace bitbucket.org/iqhive/iqmq => ../../..
