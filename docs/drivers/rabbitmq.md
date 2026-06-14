# rabbitmq

This module maps the broker client to iqmq handles and capabilities.

Run its unit tests with `go test -short ./...` from `drivers/rabbitmq`.
The module's `go.mod` records its client dependencies.

Client frame-too-large sentinel classification is added with a later client upgrade.

Generate conformance results for the source revision and broker being assessed.
