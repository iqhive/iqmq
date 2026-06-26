module bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago

go 1.26.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	bitbucket.org/iqhive/iqmq/drivers/kafka v0.0.0
	github.com/segmentio/kafka-go v0.4.51
	github.com/twmb/franz-go v1.21.2
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260529224503-19f7dbb20cb0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	golang.org/x/text v0.37.0 // indirect
)

require (
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.51.0 // indirect
)



replace bitbucket.org/iqhive/iqmq => ../../..

replace bitbucket.org/iqhive/iqmq/drivers/kafka => ./..
