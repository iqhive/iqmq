// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/kafka-orders

go 1.27.3

require (
	github.com/iqhive/iqmq v1.1.1
	github.com/iqhive/iqmq/drivers/kafka v1.1.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20261022190701-123ec885bc97
)

replace github.com/iqhive/iqmq => ../../

replace github.com/iqhive/iqmq/drivers/kafka => ../../drivers/kafka

require (
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twmb/franz-go v1.22.2 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.19.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.0 // indirect
)
