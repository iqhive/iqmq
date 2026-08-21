// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module bitbucket.org/iqhive/iqmq/examples/custom-driver

go 1.27.0

require bitbucket.org/iqhive/iqmq v0.0.0


replace bitbucket.org/iqhive/iqmq => ../..
