// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/bridge

go 1.27.0

require (
	github.com/iqhive/iqmq v0.0.0
	github.com/iqhive/iqmq/drivers/jetstream v0.0.0
	github.com/nats-io/nats-server/v2 v2.12.9
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.7.0 // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.1 // indirect
	github.com/nats-io/nats.go v1.52.0 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.52.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)



replace github.com/iqhive/iqmq => ../..

replace github.com/iqhive/iqmq/drivers/jetstream => ../../drivers/jetstream
