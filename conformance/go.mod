module github.com/iqhive/iqmq/conformance

go 1.27.3

require (
	github.com/iqhive/iqmq v1.1.2
	github.com/iqhive/iqmq/drivers/activemq v1.1.2
	github.com/iqhive/iqmq/drivers/activemq/stomp v1.1.2
	github.com/iqhive/iqmq/drivers/gcppubsub v1.1.2
	github.com/iqhive/iqmq/drivers/jetstream v1.1.2
	github.com/iqhive/iqmq/drivers/kafka v1.1.2
	github.com/iqhive/iqmq/drivers/kafka/kafkago v1.1.2
	github.com/iqhive/iqmq/drivers/nats v1.1.2
	github.com/iqhive/iqmq/drivers/natsunified v1.1.2
	github.com/iqhive/iqmq/drivers/nsq v1.1.2
	github.com/iqhive/iqmq/drivers/pulsar v1.1.2
	github.com/iqhive/iqmq/drivers/rabbitmq v1.1.2
	github.com/iqhive/iqmq/drivers/rabbitmq/amqp10 v1.1.2
	github.com/iqhive/iqmq/drivers/redis v1.1.2
	github.com/iqhive/iqmq/drivers/redis/goredis v1.1.2
	github.com/iqhive/iqmq/drivers/snssqs v1.1.2
	github.com/iqhive/iqmq/drivers/sqs v1.1.2
	github.com/moby/moby/api v1.56.1
	github.com/rabbitmq/amqp091-go v1.16.0
	github.com/testcontainers/testcontainers-go v0.44.0
	github.com/testcontainers/testcontainers-go/modules/gcloud v0.44.0
	github.com/testcontainers/testcontainers-go/modules/redpanda v0.44.0
	github.com/testcontainers/testcontainers-go/modules/toxiproxy v0.44.0
	go.uber.org/goleak v1.3.0
)

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.24.2 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.3.1 // indirect
	cloud.google.com/go/compute/metadata v0.10.0 // indirect
	cloud.google.com/go/iam v1.15.1 // indirect
	cloud.google.com/go/pubsub v1.51.1 // indirect
	cloud.google.com/go/pubsub/v2 v2.8.0 // indirect
	dario.cat/mergo v1.0.2 // indirect
	github.com/AthenZ/athenz v1.12.51 // indirect
	github.com/Azure/go-amqp v1.7.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20261013165250-51075c6ea6bc // indirect
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/Microsoft/go-winio v0.6.4 // indirect
	github.com/RoaringBitmap/roaring/v2 v2.30.0 // indirect
	github.com/apache/pulsar-client-go v0.21.0 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/aws/aws-sdk-go-v2 v1.47.5 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.47.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.8 // indirect
	github.com/aws/smithy-go v1.28.7 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.29.0 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.2.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/cpuguy83/dockercfg v0.3.2 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/docker/go-connections v0.8.2 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/ebitengine/purego v0.11.2 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/fxamacker/cbor/v2 v2.10.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.7 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-stomp/stomp/v3 v3.2.1 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/s2a-go v0.1.11 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.24 // indirect
	github.com/googleapis/gax-go/v2 v2.27.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.33.0 // indirect
	github.com/hamba/avro/v2 v2.31.0 // indirect
	github.com/iqhive/go-nsq v1.2.0 // indirect
	github.com/iqhive/iqmq/drivers/amqp10 v1.1.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/lufia/plan9stats v0.0.0-20261101121723-be84dd9509a0 // indirect
	github.com/magiconair/properties v1.18.12 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/go-archive v0.3.3 // indirect
	github.com/moby/moby/client v0.6.2 // indirect
	github.com/moby/patternmatcher v0.6.1 // indirect
	github.com/moby/sys/sequential v0.7.0 // indirect
	github.com/moby/sys/user v0.5.0 // indirect
	github.com/moby/sys/userns v0.2.1 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nats-io/nats.go v1.55.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pandatix/go-abnf v0.5.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/power-devops/perfstat v0.0.0-20260916203055-22a1a467d9f0 // indirect
	github.com/prometheus/client_golang v1.25.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.73.1 // indirect
	github.com/prometheus/procfs v0.23.0 // indirect
	github.com/redis/go-redis/v9 v9.23.0 // indirect
	github.com/redis/rueidis v1.0.79 // indirect
	github.com/segmentio/kafka-go v0.4.51 // indirect
	github.com/shirou/gopsutil/v4 v4.26.10 // indirect
	github.com/sirupsen/logrus v1.10.2 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/theparanoids/crypki v1.21.4 // indirect
	github.com/tklauser/go-sysconf v0.4.0 // indirect
	github.com/tklauser/numcpus v0.12.0 // indirect
	github.com/twmb/franz-go v1.22.2 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.19.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.72.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/mod v0.42.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/oauth2 v0.37.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	google.golang.org/api v0.310.0 // indirect
	google.golang.org/genproto v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/grpc v1.85.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/apimachinery v0.37.2 // indirect
	k8s.io/client-go v0.37.2 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	k8s.io/kube-openapi v0.0.0-20261014044854-ca3452a79603 // indirect
	k8s.io/utils v0.0.0-20261023123148-ca04ba58ecab // indirect
	sigs.k8s.io/json v0.0.0-20260909141634-11ed52e25bc5 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.4.2 // indirect
)

replace github.com/iqhive/iqmq => ../

replace github.com/iqhive/iqmq/drivers/nats => ../drivers/nats

replace github.com/iqhive/iqmq/drivers/gcppubsub => ../drivers/gcppubsub

replace github.com/iqhive/iqmq/drivers/gcppubsub/v1compat => ../drivers/gcppubsub/v1compat

replace github.com/iqhive/iqmq/drivers/jetstream => ../drivers/jetstream

replace github.com/iqhive/iqmq/drivers/kafka => ../drivers/kafka

replace github.com/iqhive/iqmq/drivers/kafka/kafkago => ../drivers/kafka/kafkago

replace github.com/iqhive/iqmq/drivers/natsunified => ../drivers/natsunified

replace github.com/iqhive/iqmq/drivers/nsq => ../drivers/nsq

replace github.com/iqhive/iqmq/drivers/pulsar => ../drivers/pulsar

replace github.com/iqhive/iqmq/drivers/redis => ../drivers/redis

replace github.com/iqhive/iqmq/drivers/redis/goredis => ../drivers/redis/goredis

replace github.com/iqhive/iqmq/drivers/sqs => ../drivers/sqs

replace github.com/iqhive/iqmq/drivers/snssqs => ../drivers/snssqs

replace github.com/iqhive/iqmq/drivers/amqp10 => ../drivers/amqp10

replace github.com/iqhive/iqmq/drivers/activemq => ../drivers/activemq

replace github.com/iqhive/iqmq/drivers/activemq/stomp => ../drivers/activemq/stomp

replace github.com/iqhive/iqmq/drivers/rabbitmq => ../drivers/rabbitmq

replace github.com/iqhive/iqmq/drivers/rabbitmq/amqp10 => ../drivers/rabbitmq/amqp10
