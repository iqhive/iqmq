package amqp10

import (
	"context"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	core "github.com/iqhive/iqmq/drivers/amqp10"
)

// Open dials the broker and returns a client.
//
// It performs exactly one piece of I/O — the connection — and creates NO
// topology, which on this profile is the broker's own property rather than a
// choice: RabbitMQ 4's AMQP 1.0 endpoint cannot declare an exchange, a queue
// or a binding, so an attach to something nobody provisioned is answered with
// `amqp:not-found` and mapped to CodeNotFound. Everything a caller can get
// wrong in the configuration is refused before the dial.
//
// # The version gate
//
// This is the call that refuses a broker older than RabbitMQ 4.0. The refusal
// happens after the connection and before the client exists, because the
// broker's version is a connection property: the peer says what it is in its
// OPEN frame, [github.com/iqhive/iqmq/drivers/amqp10.Facts] carries it, and
// this profile's [checkBroker] reads it. The error is
// [github.com/iqhive/iqmq.ErrUnsupported] naming both versions, the connection
// is closed, and NOTHING falls back to AMQP 0-9-1 — the specification says this profile is
// "never silently substituted", and a driver that quietly opened the other
// protocol would be that substitution with a different spelling. A caller who
// wants 0-9-1 imports `drivers/rabbitmq` and says so.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.Open(ctx, profile, cc)
}

// OpenBackend is [Open] without the client wrapper, for a caller composing the
// SPI directly.
//
// It returns the shared core's [github.com/iqhive/iqmq/drivers/amqp10.Backend]
// and not a type of this module's own, and that is the whole shape of this
// module: the core owns the links, the credit, the dispositions and the dial
// loop, and this module owns the answers AMQP 1.0 leaves open. A wrapper type
// here would add a layer with nothing in it.
func OpenBackend(ctx context.Context, cfg Config) (*core.Backend, error) {
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.OpenBackend(ctx, profile, cc)
}

// OpenWithConn wraps an *amqp.Conn the caller already has.
//
// own says who closes it and there is no default: a driver that always closed
// would tear down a connection the application still uses, and one that never
// closed would leak the connection it dialled itself
// ([github.com/iqhive/iqmq/driverkit.ClientOwned] and ClientBorrowed).
//
// It is the way in for everything a typed [Config] cannot carry — a Kerberos or
// OAuth SASL layer, a connection over a tunnel, a dialler with a custom
// resolver — and it is also **the seam that makes this module's own tests need
// no broker**. The version gate, the capability table, the addressing and the
// error mapping are all decided from what the peer said in its OPEN frame, so a
// test hands over a connection to a scripted peer and drives every one of them
// without a container.
//
// The version gate applies here exactly as it does to [Open]: a borrowed
// connection to a 3.13 broker is refused, and the caller keeps its connection
// because iqmq closes only what it was asked to close.
//
// A caller who supervises the connection itself usually also wants
// [Config.NoReconnect]; with the dial loop on, a lost borrowed connection is
// re-dialled from [Config.Addrs], which is only right when those addresses
// reach the same broker.
func OpenWithConn(ctx context.Context, conn *amqp.Conn, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.OpenWithClient(ctx, conn, own, profile, cc)
}

// BackendWithConn is [OpenWithConn] without the client wrapper.
func BackendWithConn(ctx context.Context, conn *amqp.Conn, own driverkit.Ownership, cfg Config) (*core.Backend, error) {
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.BackendWithClient(ctx, conn, own, profile, cc)
}

// parts is the conversion every entry point begins with: this profile and the
// shared core's configuration, validated.
//
// Validating HERE rather than inside each entry point is what makes a typo an
// immediate refusal rather than a ten-second dial timeout, and doing it
// once is what keeps the four entry points from disagreeing about which
// configurations are acceptable.
func (c Config) parts() (core.Profile, core.Config, error) {
	if err := c.Validate(); err != nil {
		return core.Profile{}, core.Config{}, err
	}
	profile, err := c.Profile()
	if err != nil {
		return core.Profile{}, core.Config{}, err
	}
	cc, err := c.CoreConfig()
	if err != nil {
		return core.Profile{}, core.Config{}, err
	}
	return profile, cc, nil
}
