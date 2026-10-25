// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/nats-reqreply

go 1.27.3

require (
	github.com/iqhive/iqmq v1.0.8
	github.com/iqhive/iqmq/drivers/nats v1.0.8
	github.com/nats-io/nats-server/v2 v2.15.1
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.8.0-default-no-op // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.2 // indirect
	github.com/nats-io/nats.go v1.54.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/time v0.16.0 // indirect
)

replace github.com/iqhive/iqmq => ../../

replace github.com/iqhive/iqmq/drivers/nats => ../../drivers/nats
