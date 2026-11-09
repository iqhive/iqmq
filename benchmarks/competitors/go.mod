module github.com/iqhive/iqmq/benchmarks/competitors

go 1.27.3

require (
	github.com/ThreeDotsLabs/watermill v1.5.3
	github.com/ThreeDotsLabs/watermill-nats/v2 v2.2.0
	github.com/iqhive/iqmq v1.1.2
	github.com/iqhive/iqmq/benchmarks/opbench v0.0.0-00010101000000-000000000000
	github.com/iqhive/iqmq/drivers/nats v1.1.2
	github.com/nats-io/nats.go v1.55.0
	gocloud.dev v0.47.0
	gocloud.dev/pubsub/natspubsub v0.47.0
)

require (
	cloud.google.com/go/auth v0.24.2 // indirect
	cloud.google.com/go/iam v1.15.1 // indirect
	cloud.google.com/go/pubsub/v2 v2.8.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/gax-go/v2 v2.27.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/lithammer/shortuuid/v3 v3.0.7 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.72.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/api v0.311.0 // indirect
	google.golang.org/genproto v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/grpc v1.85.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/iqhive/iqmq => ../../

replace github.com/iqhive/iqmq/benchmarks/opbench => ../opbench

replace github.com/iqhive/iqmq/drivers/nats => ../../drivers/nats
