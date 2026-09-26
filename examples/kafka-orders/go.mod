// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/kafka-orders

go 1.27.0

require (
	github.com/iqhive/iqmq v0.0.0
	github.com/iqhive/iqmq/drivers/kafka v0.0.0
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260529224503-19f7dbb20cb0
)

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/twmb/franz-go v1.21.2 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)



require golang.org/x/crypto v0.51.0 // indirect

replace github.com/iqhive/iqmq => ../..

replace github.com/iqhive/iqmq/drivers/kafka => ../../drivers/kafka
