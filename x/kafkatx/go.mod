module bitbucket.org/iqhive/iqmq/x/kafkatx

go 1.26.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	bitbucket.org/iqhive/iqmq/drivers/kafka v0.0.0
	github.com/twmb/franz-go v1.21.2
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260529224503-19f7dbb20cb0
)

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)

require golang.org/x/crypto v0.51.0 // indirect



replace bitbucket.org/iqhive/iqmq => ../..

replace bitbucket.org/iqhive/iqmq/drivers/kafka => ../../drivers/kafka
