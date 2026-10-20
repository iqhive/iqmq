// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/typed-json

go 1.27.0

require (
	github.com/iqhive/iqmq v1.0.4
	github.com/iqhive/iqmq/codec/jsonv2 v1.0.4
)

replace github.com/iqhive/iqmq => ../../

replace github.com/iqhive/iqmq/codec/jsonv2 => ../../codec/jsonv2
