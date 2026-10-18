module github.com/iqhive/iqmq/drivers/kafka

go 1.27.0

require (
	github.com/iqhive/iqmq v0.9.1
	github.com/twmb/franz-go v1.21.7
	github.com/twmb/franz-go/pkg/kadm v1.18.0
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260911174156-65d23a567563
	github.com/twmb/franz-go/pkg/kmsg v1.13.1
	go.uber.org/goleak v1.3.0
)

require (
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
)

require golang.org/x/crypto v0.51.0 // indirect
