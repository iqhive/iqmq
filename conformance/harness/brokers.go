package harness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/gcloud/pubsub"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PubSubProject is the project ID the Pub/Sub emulator is started with.
const PubSubProject = "iqmq-test"

// Redpanda starts a single-node Redpanda with auto topic creation. Addr is
// the Kafka seed broker; Addrs["admin"] is the admin API (http://...).
func Redpanda(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("redpanda"); ok {
		return b
	}
	requireDocker(tb)
	image := Image("redpanda")
	ctx, cancel := startCtx(tb)
	tb.Cleanup(cancel)
	ctr, err := redpanda.Run(ctx, image, append([]testcontainers.ContainerCustomizer{redpanda.WithAutoCreateTopics()}, opts...)...)
	testcontainers.CleanupContainer(tb, ctr)
	if err != nil {
		tb.Fatalf("harness: start redpanda (%s): %v", image, err)
	}
	b := wrap("redpanda", image, ctr)
	seed, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		tb.Fatalf("harness: redpanda seed broker: %v", err)
	}
	admin, err := ctr.AdminAPIAddress(ctx)
	if err != nil {
		tb.Fatalf("harness: redpanda admin api: %v", err)
	}
	b.Addr = seed
	b.Addrs["admin"] = admin
	return b
}

// Kafka starts a single-node Apache Kafka (KRaft, combined broker and
// controller). The broker advertises hostAddr():<port> on a fixed host port
// because clients learn broker addresses from metadata, not from the
// bootstrap address.
func Kafka(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("kafka"); ok {
		return b
	}
	port, err := freePort()
	if err != nil {
		tb.Fatalf("harness: free port: %v", err)
	}
	adv := fmt.Sprintf("PLAINTEXT://%s:%d", hostAddr(), port)
	b, _ := run(tb, "kafka", opts,
		testcontainers.WithExposedPorts(tcpPort(port)),
		fixedPorts(port),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_LISTENERS":                                fmt.Sprintf("PLAINTEXT://0.0.0.0:%d,CONTROLLER://0.0.0.0:9093", port),
			"KAFKA_ADVERTISED_LISTENERS":                     adv,
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "true",
			"KAFKA_NUM_PARTITIONS":                           "1",
			"KAFKA_LOG_DIRS":                                 "/tmp/kraft-combined-logs",
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("Kafka Server started").WithStartupTimeout(3*time.Minute)),
	)
	b.Addr = fmt.Sprintf("%s:%d", hostAddr(), port)
	return b
}

// NATS starts nats-server with JetStream and the monitoring endpoint.
// Addrs["monitor"] is the HTTP monitoring address (port 8222).
func NATS(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("nats"); ok {
		return b
	}
	b, ctx := run(tb, "nats", opts,
		testcontainers.WithExposedPorts("4222/tcp", "8222/tcp"),
		testcontainers.WithCmd("--jetstream", "--http_port", "8222", "--store_dir", "/data"),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForListeningPort("4222/tcp"),
			wait.ForHTTP("/healthz").WithPort("8222/tcp"),
		)),
	)
	b.Addr = endpoint(ctx, tb, b.ctr, "4222/tcp")
	b.Addrs["monitor"] = endpoint(ctx, tb, b.ctr, "8222/tcp")
	return b
}

// NSQ starts nsqlookupd and one nsqd on a private network. Addr is the nsqd
// TCP address; Addrs holds "http" (nsqd HTTP), "lookupd" (TCP) and
// "lookupd-http". nsqd advertises hostAddr():<fixed port> so lookupd-based
// discovery works from the test process.
func NSQ(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("nsq"); ok {
		return b
	}
	requireDocker(tb)
	image := Image("nsq")
	ctx, cancel := startCtx(tb)
	tb.Cleanup(cancel)
	nw, err := network.New(ctx)
	testcontainers.CleanupNetwork(tb, nw)
	if err != nil {
		tb.Fatalf("harness: nsq network: %v", err)
	}
	lookupd, err := testcontainers.Run(ctx, image,
		testcontainers.WithExposedPorts("4160/tcp", "4161/tcp"),
		testcontainers.WithCmd("/nsqlookupd", "--broadcast-address", "nsqlookupd"),
		network.WithNetwork([]string{"nsqlookupd"}, nw),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/ping").WithPort("4161/tcp")),
	)
	testcontainers.CleanupContainer(tb, lookupd)
	if err != nil {
		tb.Fatalf("harness: start nsqlookupd (%s): %v", image, err)
	}
	tcp, err := freePort()
	if err != nil {
		tb.Fatalf("harness: free port: %v", err)
	}
	httpPort, err := freePort()
	if err != nil {
		tb.Fatalf("harness: free port: %v", err)
	}
	nsqd, err := testcontainers.Run(ctx, image, append([]testcontainers.ContainerCustomizer{
		testcontainers.WithExposedPorts(tcpPort(tcp), tcpPort(httpPort)),
		fixedPorts(tcp, httpPort),
		testcontainers.WithCmd("/nsqd",
			"--lookupd-tcp-address", "nsqlookupd:4160",
			"--broadcast-address", hostAddr(),
			"--tcp-address", "0.0.0.0:"+strconv.Itoa(tcp),
			"--http-address", "0.0.0.0:"+strconv.Itoa(httpPort),
			"--broadcast-tcp-port", strconv.Itoa(tcp),
			"--broadcast-http-port", strconv.Itoa(httpPort),
		),
		network.WithNetwork([]string{"nsqd"}, nw),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/ping").WithPort(tcpPort(httpPort))),
	}, opts...)...)
	testcontainers.CleanupContainer(tb, nsqd)
	if err != nil {
		tb.Fatalf("harness: start nsqd (%s): %v", image, err)
	}
	b := wrap("nsq", image, nsqd)
	b.Addr = fmt.Sprintf("%s:%d", hostAddr(), tcp)
	b.Addrs["http"] = fmt.Sprintf("%s:%d", hostAddr(), httpPort)
	b.Addrs["lookupd"] = endpoint(ctx, tb, lookupd, "4160/tcp")
	b.Addrs["lookupd-http"] = endpoint(ctx, tb, lookupd, "4161/tcp")
	return b
}

// PubSub starts the Google Cloud Pub/Sub emulator for PubSubProject. Addr is
// host:port for PUBSUB_EMULATOR_HOST.
func PubSub(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("pubsub"); ok {
		return b
	}
	requireDocker(tb)
	image := Image("pubsub")
	ctx, cancel := startCtx(tb)
	tb.Cleanup(cancel)
	ctr, err := pubsub.Run(ctx, image, append([]testcontainers.ContainerCustomizer{pubsub.WithProjectID(PubSubProject)}, opts...)...)
	testcontainers.CleanupContainer(tb, ctr)
	if err != nil {
		tb.Fatalf("harness: start pubsub emulator (%s): %v", image, err)
	}
	b := wrap("pubsub", image, ctr)
	b.Addr = ctr.URI()
	b.Addrs["project"] = ctr.ProjectID()
	return b
}

// ElasticMQ starts ElasticMQ (SQS-compatible). Addr is the SQS endpoint
// host:port; Addrs["ui"] is the statistics/UI port.
func ElasticMQ(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("elasticmq"); ok {
		return b
	}
	b, ctx := run(tb, "elasticmq", opts,
		testcontainers.WithExposedPorts("9324/tcp", "9325/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9324/tcp")),
	)
	b.Addr = endpoint(ctx, tb, b.ctr, "9324/tcp")
	b.Addrs["ui"] = endpoint(ctx, tb, b.ctr, "9325/tcp")
	return b
}

// LocalStack starts LocalStack with SQS and SNS. Addr is the edge endpoint
// host:port.
func LocalStack(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("localstack"); ok {
		return b
	}
	env := map[string]string{"SERVICES": "sqs,sns", "DEBUG": "0", "EAGER_SERVICE_LOADING": "1"}
	if tok := os.Getenv("LOCALSTACK_AUTH_TOKEN"); tok != "" {
		env["LOCALSTACK_AUTH_TOKEN"] = tok
	}
	b, ctx := run(tb, "localstack", opts,
		testcontainers.WithExposedPorts("4566/tcp"),
		testcontainers.WithEnv(env),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/_localstack/health").WithPort("4566/tcp").WithStartupTimeout(3*time.Minute)),
	)
	b.Addr = endpoint(ctx, tb, b.ctr, "4566/tcp")
	b.Auth = Auth{User: "test", Password: "test"}
	return b
}

// RabbitMQ starts RabbitMQ with the management plugin. Addr is the AMQP
// endpoint; Addrs["management"] is the HTTP management API. Credentials are
// guest/guest.
func RabbitMQ(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("rabbitmq"); ok {
		return b
	}
	b, ctx := run(tb, "rabbitmq", opts, rabbitOpts()...)
	finishRabbit(ctx, tb, b)
	return b
}

// RabbitMQDelayed builds hack/images/rabbitmq-delayed (RabbitMQ plus the
// delayed-message exchange plugin) with the docker CLI when the image is not
// present locally, then starts it. The image is kept between runs; set
// IQMQ_REBUILD_IMAGES=1 to force a rebuild.
func RabbitMQDelayed(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("rabbitmq-delayed"); ok {
		return b
	}
	requireDocker(tb)
	ctx, cancel := startCtx(tb)
	tb.Cleanup(cancel)
	image := RabbitMQDelayedRepo + ":" + RabbitMQDelayedTag
	dir := filepath.Join(repoRoot(), "hack", "images", "rabbitmq-delayed")
	if err := ensureLocalImage(ctx, image, dir); err != nil {
		tb.Fatalf("harness: build %s from %s: %v", image, dir, err)
	}
	ctr, err := testcontainers.Run(ctx, image, append(rabbitOpts(), opts...)...)
	testcontainers.CleanupContainer(tb, ctr)
	if err != nil {
		tb.Fatalf("harness: start rabbitmq-delayed (%s): %v", image, err)
	}
	b := wrap("rabbitmq-delayed", image, ctr)
	finishRabbit(ctx, tb, b)
	return b
}

// ensureLocalImage builds image from dir with the docker CLI unless it is
// already present. The CLI is used instead of the testcontainers build path
// because the latter resolves credentials for every registry in the docker
// config and fails when any credential helper is broken.
func ensureLocalImage(ctx context.Context, image, dir string) error {
	if os.Getenv("IQMQ_REBUILD_IMAGES") != "1" {
		if err := exec.CommandContext(ctx, "docker", "image", "inspect", image).Run(); err == nil {
			return nil
		}
	}
	cmd := exec.CommandContext(ctx, "docker", "build", "-t", image, dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, trunc(out))
	}
	return nil
}

func rabbitOpts() []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		testcontainers.WithExposedPorts("5672/tcp", "15672/tcp"),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForListeningPort("5672/tcp"),
			wait.ForHTTP("/api/overview").WithPort("15672/tcp").WithBasicAuth("guest", "guest"),
		)),
	}
}

func finishRabbit(ctx context.Context, tb testing.TB, b *Broker) {
	tb.Helper()
	b.Addr = endpoint(ctx, tb, b.ctr, "5672/tcp")
	b.Addrs["management"] = endpoint(ctx, tb, b.ctr, "15672/tcp")
	b.Auth = Auth{User: "guest", Password: "guest"}
}

// Artemis starts ActiveMQ Artemis. Addr is the AMQP 1.0 port (5672); Addrs
// holds "stomp" (61613), "core" (61616) and "console" (8161). Credentials
// are artemis/artemis; anonymous login is disabled.
func Artemis(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("artemis"); ok {
		return b
	}
	b, ctx := run(tb, "artemis", opts,
		testcontainers.WithExposedPorts("5672/tcp", "61613/tcp", "61616/tcp", "8161/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ARTEMIS_USER":     "artemis",
			"ARTEMIS_PASSWORD": "artemis",
			"ANONYMOUS_LOGIN":  "false",
			"EXTRA_ARGS":       "--http-host 0.0.0.0 --relax-jolokia --nio",
		}),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("AMQ221007"), // Server is now active
			wait.ForListeningPort("61616/tcp"),
		)),
	)
	b.Addr = endpoint(ctx, tb, b.ctr, "5672/tcp")
	b.Addrs["stomp"] = endpoint(ctx, tb, b.ctr, "61613/tcp")
	b.Addrs["core"] = endpoint(ctx, tb, b.ctr, "61616/tcp")
	b.Addrs["console"] = endpoint(ctx, tb, b.ctr, "8161/tcp")
	b.Auth = Auth{User: "artemis", Password: "artemis"}
	return b
}

// ArtemisNoAutoCreate is the customizer that turns Artemis's auto-creation
// OFF, which is what a conformance run against `drivers/activemq` needs.
//
// The specification makes implicit topology creation a defect, and on Artemis it is a
// defect with teeth: a driver that attaches to a destination that does not
// exist gets one CREATED for it, so a typo in a topic name becomes an address
// that receives every publication and delivers nothing, with no error
// anywhere. With auto-creation off the attach is refused instead --
// "AMQ119010: source address does not exist" for a receiver and "AMQ119002:
// target address <name> does not exist" for a sender, measured against the
// pinned image -- which is what makes a driver's "creates no topology" claim
// PROVABLE rather than merely stated.
//
// It is a separate customizer rather than the constructor's default because
// turning it on changes what every other suite would see, and because the
// pinned image ships with auto-creation ON: a harness that quietly differed
// from the image's own defaults would make a conformance report say something
// about a broker nobody runs.
//
// `EXTRA_ARGS` is the image's own hook -- the entry point passes it to
// `artemis create` -- so this replaces the constructor's value rather than
// adding to it, and the two halves of the default are kept.
func ArtemisNoAutoCreate() testcontainers.ContainerCustomizer {
	return testcontainers.WithEnv(map[string]string{
		"EXTRA_ARGS": "--http-host 0.0.0.0 --relax-jolokia --nio --no-autocreate",
	})
}

// ArtemisPassword starts Artemis with a password of the caller's choosing and
// records it on the Broker, for a suite that needs a credential the driver's
// own vocabulary does not also contain.
//
// It exists because of a failure that is worth reading twice. The image's
// default credentials are `artemis`/`artemis`, and the conformance kit checks
// that no credential appears in any string the driver renders -- so with the
// default password the check failed on `DriverInfo`, whose PROFILE NAME is
// also "artemis". The credential was not leaked; the check was right anyway,
// because a password that is a word the driver says about itself cannot be
// told from one it leaked, and a deployment using it has no way to audit its
// own logs. A distinct password is better hygiene and it makes the check mean
// what it says.
func ArtemisPassword(password string) testcontainers.ContainerCustomizer {
	return testcontainers.WithEnv(map[string]string{"ARTEMIS_PASSWORD": password})
}

// ActiveMQClassicPassword starts Classic with a connection password of the
// caller's choosing and records it on the Broker.
//
// It exists for the reason [ArtemisPassword] does, and Classic's default is
// worse: the image's credentials are configured from
// `ACTIVEMQ_CONNECTION_USER`/`_PASSWORD`, which this harness sets to
// `iqmq`/`iqmq` — and "iqmq" is the prefix of every error string this library
// renders, so the redaction scenario failed on `iqmq: invalid topic name`.
// Nothing leaked and the check was right anyway: a password that is a word the
// library prints cannot be told from one it leaked.
func ActiveMQClassicPassword(password string) testcontainers.ContainerCustomizer {
	return testcontainers.WithEnv(map[string]string{"ACTIVEMQ_CONNECTION_PASSWORD": password})
}

// ActiveMQClassicWithScheduler is the customizer that configures ActiveMQ
// Classic for a conformance run: its MESSAGE SCHEDULER on, and PRIORITISED
// DISPATCH on its queues.
//
// Both are off in the image and both are off SILENTLY, which is why a
// conformance run has to turn them on rather than withdraw the claims:
//
//   - `schedulerSupport` is absent from the `<broker>` element, and measured
//     with it off, a publication carrying `AMQ_SCHEDULED_DELAY` is delivered
//     immediately with the property passed through to the consumer.
//   - the image's `<destinationPolicy>` has one `policyEntry` for topics and
//     none for queues, so a queue does not reorder by priority. Measured with
//     it off, the priority probe reads the low-priority message first --
//     "the driver advertises Priority at conditional and the feature did not
//     work: the first delivery is \"low\"" -- and a virtual topic's consumer
//     queues are QUEUES, so the topic entry does not reach them.
//
// Both are one `sed` each rather than a broker.xml copied into this
// repository: the file is 130 lines of the broker's own defaults and a copy
// would have to be re-checked against every image bump. The image's own entry
// point patches the same file for authentication, and it runs after this.
//
// The scheduler is OFF in the image's own configuration -- `schedulerSupport`
// is absent from the `<broker>` element and defaults to false -- and its
// absence is SILENT: measured against the pinned image, a publication
// carrying `AMQ_SCHEDULED_DELAY` is then delivered immediately and the
// property travels through to the consumer untouched. So a driver that
// claimed scheduled publication against the image as it ships would promise a
// delay the broker discards, and the only honest options are to withdraw the
// claim or to turn the scheduler on. Both are exercised:
// `drivers/activemq`'s capability table withdraws it unless
// `Config.Scheduler` says otherwise, and this is how a suite says otherwise.
//
// It patches the configuration with `sed` at start-up rather than copying a
// broker.xml into the repository, because the file is 130 lines of the
// broker's own defaults and a copy would have to be re-checked against every
// image bump. The image's own entry point patches the same file for
// authentication, and it runs after this.
func ActiveMQClassicWithScheduler() testcontainers.ContainerCustomizer {
	const scheduler = `sed -i 's|<broker xmlns=|<broker schedulerSupport="true" xmlns=|' "$ACTIVEMQ_CONF/activemq.xml"`
	// The queue entry goes before the existing topic one, inside
	// <policyEntries>, so that a queue reorders by priority.
	// The "&" is escaped because `sed` reads an unescaped one in a
	// REPLACEMENT as the whole match: the first draft produced
	// `queue="<policyEntries>gt;"`, which is not XML at all and which the
	// broker answered by exiting with a Spring parse error three seconds
	// into the run.
	const prioritised = `sed -i 's|<policyEntries>|<policyEntries><policyEntry queue="\&gt;" prioritizedMessages="true"/>|' ` +
		`"$ACTIVEMQ_CONF/activemq.xml"`
	return &classicScheduler{patch: scheduler + " && " + prioritised +
		` && exec /usr/local/bin/entrypoint.sh activemq console`}
}

// classicScheduler applies the start-up override, and it is a type rather than
// a composition of customizers because a ContainerCustomizer is one function
// and the entry point and the command have to change together -- two of them
// would leave the container running `sh -c activemq console`.
type classicScheduler struct{ patch string }

// Customize implements testcontainers.ContainerCustomizer.
func (c *classicScheduler) Customize(req *testcontainers.GenericContainerRequest) error {
	req.Entrypoint = []string{"/bin/sh", "-c"}
	req.Cmd = []string{c.patch}
	return nil
}

// ActiveMQClassic starts ActiveMQ Classic. Addr is the OpenWire port (61616);
// Addrs holds "amqp" (5672), "stomp" (61613) and "console" (8161).
// Connection credentials are iqmq/iqmq.
func ActiveMQClassic(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("activemq-classic"); ok {
		return b
	}
	b, ctx := run(tb, "activemq-classic", opts,
		testcontainers.WithExposedPorts("5672/tcp", "61613/tcp", "61616/tcp", "8161/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ACTIVEMQ_CONNECTION_USER":     "iqmq",
			"ACTIVEMQ_CONNECTION_PASSWORD": "iqmq",
			"ACTIVEMQ_WEB_USER":            "admin",
			"ACTIVEMQ_WEB_PASSWORD":        "admin",
		}),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("Apache ActiveMQ .* started").AsRegexp(),
			wait.ForListeningPort("61616/tcp"),
		)),
	)
	b.Addr = endpoint(ctx, tb, b.ctr, "61616/tcp")
	b.Addrs["amqp"] = endpoint(ctx, tb, b.ctr, "5672/tcp")
	b.Addrs["stomp"] = endpoint(ctx, tb, b.ctr, "61613/tcp")
	b.Addrs["console"] = endpoint(ctx, tb, b.ctr, "8161/tcp")
	b.Auth = Auth{User: "iqmq", Password: "iqmq"}
	return b
}

// Redis starts Redis. Addr is host:port.
func Redis(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	return respServer(tb, "redis", opts)
}

// Valkey starts Valkey (Redis-compatible). Addr is host:port.
func Valkey(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	return respServer(tb, "valkey", opts)
}

func respServer(tb testing.TB, name string, opts []testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external(name); ok {
		return b
	}
	b, ctx := run(tb, name, opts,
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("Ready to accept connections"),
			wait.ForListeningPort("6379/tcp"),
		)),
	)
	b.Addr = endpoint(ctx, tb, b.ctr, "6379/tcp")
	return b
}

// Pulsar starts Pulsar standalone (no function worker, no stream storage).
// Addr is the binary protocol port (6650); Addrs["admin"] is the HTTP admin
// API (8080).
//
// Direct memory is 1g because 256m ran out under the native baseline's 64KiB
// publishes with a 2048-message receiver queue, and the broker halts when it
// cannot allocate direct memory rather than slowing down.
func Pulsar(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("pulsar"); ok {
		return b
	}
	b, ctx := run(tb, "pulsar", opts,
		testcontainers.WithExposedPorts("6650/tcp", "8080/tcp"),
		testcontainers.WithCmd("bin/pulsar", "standalone", "-nfw", "-nss"),
		testcontainers.WithEnv(map[string]string{
			"PULSAR_MEM":                      "-Xms512m -Xmx512m -XX:MaxDirectMemorySize=1g",
			"PULSAR_PREFIX_advertisedAddress": "localhost",
		}),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForHTTP("/admin/v2/brokers/health").WithPort("8080/tcp").WithStartupTimeout(StartTimeout),
			wait.ForListeningPort("6650/tcp"),
		)),
	)
	b.Addr = endpoint(ctx, tb, b.ctr, "6650/tcp")
	b.Addrs["admin"] = endpoint(ctx, tb, b.ctr, "8080/tcp")
	return b
}

// Postgres starts PostgreSQL with database, user and password "iqmq".
// Addr is host:port.
func Postgres(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *Broker {
	tb.Helper()
	if b, ok := external("postgres"); ok {
		return b
	}
	b, ctx := run(tb, "postgres", opts,
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_USER":     "iqmq",
			"POSTGRES_PASSWORD": "iqmq",
			"POSTGRES_DB":       "iqmq",
		}),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		)),
	)
	b.Addr = endpoint(ctx, tb, b.ctr, "5432/tcp")
	b.Auth = Auth{User: "iqmq", Password: "iqmq"}
	return b
}

// repoRoot locates the repository root from this source file's location.
func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "../.."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
