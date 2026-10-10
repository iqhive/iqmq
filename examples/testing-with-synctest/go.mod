// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/testing-with-synctest

go 1.27.0

require github.com/iqhive/iqmq v0.9.0

replace github.com/iqhive/iqmq => ../../
