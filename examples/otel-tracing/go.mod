// The example modules are separate from the repository on purpose: an example
// that compiled only because it shared the root module's go.mod would not
// prove that somebody outside this repository can build it.
module github.com/iqhive/iqmq/examples/otel-tracing

go 1.27.0

require (
	github.com/iqhive/iqmq v0.0.0
	github.com/iqhive/iqmq/drivers/redis v0.0.0
	github.com/iqhive/iqmq/integrations/otel v0.0.0
	go.opentelemetry.io/otel/sdk v1.44.0
	go.opentelemetry.io/otel/sdk/metric v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/redis/rueidis v1.0.75 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.37.0 // indirect
)




replace github.com/iqhive/iqmq => ../..

replace github.com/iqhive/iqmq/drivers/redis => ../../drivers/redis

replace github.com/iqhive/iqmq/integrations/otel => ../../integrations/otel
