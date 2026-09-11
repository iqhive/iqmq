# iqmq

One Go API for publishing and consuming messages across broker backends. The core and memory backend depend on the standard library. Broker SDKs stay in separate modules.

```go
client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
if err != nil { return err }
defer client.Shutdown(ctx)
return client.Publish(ctx, "orders", &mq.Message{Body: []byte("hello")})
```

Import `bitbucket.org/iqhive/iqmq` as `mq` and `bitbucket.org/iqhive/iqmq/memory`. Requires Go 1.27.0 Run `go test -short ./...` for basic checks. Run the module tests before upgrading.

Concrete client, publisher and consumer handles share a capability-negotiated SPI. The core owns retries, backpressure, settlement, lanes and shutdown. Adapters map native behaviour and declare unsupported guarantees. The memory backend provides deterministic time, inspection and fault injection.

The earlier API is temporarily available under `legacy` to support migration. This is a pre-1.0 API change; construct clients and handles using the new signatures when upgrading. Generate fresh benchmark and broker integration results for the revision being assessed. Apache-2.0; see LICENSE and NOTICE.

* Integrate NSQ with the concrete client and driver SPI. Modules: `drivers/nsq`.

* Integrate AMQP 1.0 and ActiveMQ with the current driver SPI. Modules: `drivers/amqp10`, `drivers/activemq`.

* Integrate STOMP with bounded receive and settlement helpers. Modules: `drivers/activemq/stomp`.
