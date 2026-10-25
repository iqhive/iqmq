// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/testing-with-synctest

go 1.27.3

require github.com/iqhive/iqmq v1.0.8

replace github.com/iqhive/iqmq => ../../
