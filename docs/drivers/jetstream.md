# jetstream

This module maps the broker client to iqmq handles and capabilities.

Run its unit tests with `go test -short ./...` from `drivers/jetstream`.
The module's `go.mod` records its client dependencies.

The native asynchronous sink is not exposed with the current client pin; use ordinary publishing or the core worker pool.

Generate conformance results for the source revision and broker being assessed.
