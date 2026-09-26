package activemq

import (
	"context"

	"github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Open dials the broker and returns a client.
//
// It validates the configuration, builds the profile and hands both to the
// shared core, which dials once before it returns: a client that came back
// ready and then failed on its first publish would be the specification's "validate before
// starting background work" broken at the one place it matters.
//
// Nothing here creates topology. The specification makes implicit creation a defect, and on
// Artemis with auto-creation disabled a destination that does not exist is
// refused at attach with [mq.CodeNotFound]; `Client.Admin()` is how topology
// is made, and only Artemis has one (admin.go).
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.Open(ctx, profile, cc)
}

// OpenBackend returns the SPI backend rather than a client, for a caller that
// composes the SPI itself.
//
// It returns the CORE's backend and not a wrapper of this module's own,
// deliberately: a wrapper here would add a layer with nothing in it, and every
// method a caller wants — `Stats`, `Facts`, `Generation`, `Ping`, `Native` —
// is already the core's.
func OpenBackend(ctx context.Context, cfg Config) (*core.Backend, error) {
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.OpenBackend(ctx, profile, cc)
}

// OpenWithClient opens a client over a connection the caller already has
// (the specification's `OpenWithClient`).
//
// own says whether iqmq closes the connection: [driverkit.ClientBorrowed]
// leaves it alone, which is what a caller sharing one connection between a
// client and its own AMQP code needs, and [driverkit.ClientOwned] closes it
// with the client.
//
// A borrowed connection is NOT redialled. The core's reconnect loop dials, and
// dialling would replace a connection the caller still holds — so a borrowed
// client goes Degraded and stays there, which is the honest answer and is what
// the core's own documentation says.
func OpenWithClient(ctx context.Context, conn *amqp.Conn, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	if conn == nil {
		return nil, errNoConn
	}
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.OpenWithClient(ctx, conn, own, profile, cc)
}

// BackendWithClient is [OpenWithClient] at the SPI.
func BackendWithClient(ctx context.Context, conn *amqp.Conn, own driverkit.Ownership, cfg Config) (*core.Backend, error) {
	if conn == nil {
		return nil, errNoConn
	}
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.BackendWithClient(ctx, conn, own, profile, cc)
}

// BackendWithPeer opens a backend over the core's scripted peer, which is what
// makes this module's own tests need no broker.
//
// It is the seam the specification recorded as missing and this increment added to the core
// (`drivers/amqp10/peer.go`): three of the behaviours this driver has to get
// right cannot be provoked from a container at all — a disposition sent on a
// link that has since detached, a consumer cancelled while a fetch is parked,
// and a publication whose disposition never arrives.
func BackendWithPeer(ctx context.Context, cfg Config, p *core.Peer) (*core.Backend, error) {
	profile, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.BackendWithPeer(ctx, profile, cc, p)
}

// ClientForBackend builds the client a backend belongs to, so that a test
// which needs both can have both.
func ClientForBackend(b *core.Backend, cfg Config) (*mq.Client, error) {
	if b == nil {
		return nil, errNoBackend
	}
	_, cc, err := cfg.parts()
	if err != nil {
		return nil, err
	}
	return core.ClientForBackend(b, cc), nil
}

// parts is the single point at which a configuration becomes a profile and a
// core configuration, so that every entry point validates the same way.
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

// The construction refusals.
var (
	errNoConn = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: OpenWithClient was given no connection")
	errPathUnsupported = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: neither ActiveMQ has virtual hosts and AMQP 1.0 has no virtual-host field, so a DSN path names nothing")
	errDSNNoBroker = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: the DSN does not say which ActiveMQ: add broker=artemis or broker=classic, because the two products' topic mappings name nothing in common")
	errDSNBadBroker = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: the DSN's broker parameter is neither \"artemis\" nor \"classic\"")
)
