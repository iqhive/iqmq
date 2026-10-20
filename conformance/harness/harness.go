package harness

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
)

// StartTimeout bounds how long a single container may take to become ready.
// Pulsar and LocalStack are the slow ones; everything else starts in seconds.
const StartTimeout = 5 * time.Minute

// Broker is a running broker: either a container started by this package or
// an externally managed instance named by IQMQ_<BROKER>_ADDR.
type Broker struct {
	// Name is the harness name ("redpanda", "activemq-classic", ...).
	Name string
	// Image is the image reference the container was started from ("" when
	// External). Without an IQMQ_<NAME>_IMAGE override it is the
	// `repository@sha256:...` conformance/harness/images.lock pins, so a
	// report carries the bytes that ran and not a tag.
	Image string
	// Version is the broker's version ("" when External): the tag of the
	// reference started when it has one, and otherwise -- a digest
	// reference has none -- the tag [Images] declares for Name.
	Version string
	// Addr is the primary client endpoint as host:port (or a URL for HTTP-only
	// brokers such as the Pub/Sub emulator, where it is still host:port).
	Addr string
	// Addrs holds named secondary endpoints, for example "admin", "http",
	// "lookupd-http", "management", "stomp", "amqp".
	Addrs map[string]string
	// External is true when the broker was supplied via IQMQ_<BROKER>_ADDR.
	External bool
	// Auth holds credentials the constructor configured (user, password).
	Auth Auth

	ctr testcontainers.Container
}

// Auth is a username/password pair configured on a broker.
//
// Password is a [Secret], so a suite that logs its Broker on failure -- `%+v`,
// `%#v`, slog's text or JSON handler -- writes "redacted" into the CI log and
// not the password. Read the bytes with Reveal, which is the one greppable
// place a password leaves the type; a driver Config field that is a dsn.Secret
// takes `dsn.Secret(b.Auth.Password)`, which copies it without revealing it.
type Auth struct {
	// User is the login name the broker will accept: the fixed one the
	// container constructor provisioned ("guest" for RabbitMQ, "artemis"
	// for Artemis), or the one IQMQ_<BROKER>_USER named for an external
	// broker. A suite passes it to its client's own username field, beside
	// Password.Reveal. It is empty when the broker needs no login, which is
	// how the constructors leave it for a broker they start without one.
	User string
	// Password is the secret that goes with User, and is a [Secret] rather
	// than a string for the reason above. An external broker may name a
	// User with no password at all -- IQMQ_<BROKER>_PASSWORD is read
	// whether or not it is set -- so an empty Password is not on its own a
	// sign that no login is configured; User is.
	Password Secret
}

// Endpoint returns the named secondary endpoint or Addr when name is "".
func (b *Broker) Endpoint(name string) string {
	if name == "" {
		return b.Addr
	}
	return b.Addrs[name]
}

// Container returns the underlying testcontainers container (nil when
// External).
func (b *Broker) Container() testcontainers.Container { return b.ctr }

// Terminate stops the container. It is a no-op for external brokers and is
// registered automatically with t.Cleanup by every constructor.
func (b *Broker) Terminate(ctx context.Context) error {
	if b == nil || b.ctr == nil {
		return nil
	}
	return b.ctr.Terminate(ctx)
}

// Logs returns the container log (empty for external brokers). It is meant
// for failure diagnostics.
func (b *Broker) Logs(ctx context.Context) string {
	if b == nil || b.ctr == nil {
		return ""
	}
	rc, err := b.ctr.Logs(ctx)
	if err != nil {
		return "logs: " + err.Error()
	}
	defer func() { _ = rc.Close() }()
	var sb strings.Builder
	buf := make([]byte, 32<<10)
	for {
		n, err := rc.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return sb.String()
}

// external returns a Broker for IQMQ_<NAME>_ADDR when set.
func external(name string) (*Broker, bool) {
	addr := os.Getenv(envKey(name, "ADDR"))
	if addr == "" {
		return nil, false
	}
	b := &Broker{Name: name, Addr: addr, External: true, Addrs: map[string]string{}}
	for k, v := range parseAddrs(os.Getenv(envKey(name, "ADDRS"))) {
		b.Addrs[k] = v
	}
	if u := os.Getenv(envKey(name, "USER")); u != "" {
		b.Auth = Auth{User: u, Password: Secret(os.Getenv(envKey(name, "PASSWORD")))}
	}
	return b, true
}

// parseAddrs parses "name=host:port,name=host:port".
func parseAddrs(s string) map[string]string {
	out := map[string]string{}
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || k == "" || v == "" {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out
}

// DockerAvailable reports whether a Docker daemon is reachable. It asks the
// daemon on every call rather than caching the answer for the process: the
// question is asked once per broker start, which costs far more than the
// ping.
func DockerAvailable(ctx context.Context) error {
	p, err := testcontainers.NewDockerProvider()
	if err != nil {
		return err
	}
	defer func() { _ = p.Close() }()
	return p.Health(ctx)
}

// requireDocker skips the test when Docker is unreachable, unless
// IQMQ_REQUIRE_BROKERS=1 in which case it fails.
func requireDocker(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := DockerAvailable(ctx); err != nil {
		if os.Getenv("IQMQ_REQUIRE_BROKERS") == "1" {
			tb.Fatalf("harness: docker unavailable and IQMQ_REQUIRE_BROKERS=1: %v", err)
		}
		tb.Skipf("harness: docker unavailable: %v", err)
	}
}

// startCtx returns the context used to start a container.
func startCtx(tb testing.TB) (context.Context, context.CancelFunc) {
	tb.Helper()
	return context.WithTimeout(context.Background(), StartTimeout)
}

// run starts the image for name with defaults followed by caller opts,
// registers cleanup, and wraps the result.
func run(tb testing.TB, name string, opts []testcontainers.ContainerCustomizer, defaults ...testcontainers.ContainerCustomizer) (*Broker, context.Context) {
	tb.Helper()
	requireDocker(tb)
	image := imageRef(tb, name)
	ctx, cancel := startCtx(tb)
	tb.Cleanup(cancel)
	ctr, err := testcontainers.Run(ctx, image, append(defaults, opts...)...)
	testcontainers.CleanupContainer(tb, ctr)
	if err != nil {
		tb.Fatalf("harness: start %s (%s): %v", name, image, err)
	}
	return wrap(name, image, ctr), ctx
}

// wrap builds a Broker around a started container. The version comes from
// [brokerVersion] and not [Version] directly, because [Image] resolves through
// images.lock and a digest reference has no tag to read.
func wrap(name, image string, ctr testcontainers.Container) *Broker {
	return &Broker{Name: name, Image: image, Version: brokerVersion(name, image), Addrs: map[string]string{}, ctr: ctr}
}

// endpoint resolves a container port to host:port and fails the test on error.
func endpoint(ctx context.Context, tb testing.TB, ctr testcontainers.Container, port string) string {
	tb.Helper()
	ep, err := ctr.PortEndpoint(ctx, port, "")
	if err != nil {
		tb.Fatalf("harness: endpoint %s: %v", port, err)
	}
	return ep
}

// hostAddr is the address at which containers' published ports are reachable
// from the test process. Brokers that advertise their own address (Kafka,
// NSQ) need it. Override with IQMQ_DOCKER_HOST_ADDR.
func hostAddr() string {
	if v := os.Getenv("IQMQ_DOCKER_HOST_ADDR"); v != "" {
		return v
	}
	return "localhost"
}

// freePort asks the kernel for an unused TCP port on the host.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, errors.New("harness: not a TCP listener")
	}
	return addr.Port, nil
}

// fixedPorts binds each container port to the same numbered host port, so a
// broker can advertise hostAddr():port to clients. The ports must have been
// obtained with freePort.
func fixedPorts(ports ...int) testcontainers.CustomizeRequestOption {
	return testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
		if hc.PortBindings == nil {
			hc.PortBindings = network.PortMap{}
		}
		for _, p := range ports {
			cp, err := network.ParsePort(strconv.Itoa(p) + "/tcp")
			if err != nil {
				continue
			}
			hc.PortBindings[cp] = []network.PortBinding{{HostIP: netip.IPv4Unspecified(), HostPort: strconv.Itoa(p)}}
		}
	})
}

// tcpPort formats a port for testcontainers ("4150/tcp").
func tcpPort(p int) string { return fmt.Sprintf("%d/tcp", p) }
