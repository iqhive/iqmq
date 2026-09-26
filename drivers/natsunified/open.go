package natsunified

import (
	"context"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/nats"
)

// Open opens a client on the profile the configuration names.
//
// What comes back is the profile driver's own client, built by the profile
// driver's own Open: the clock, the hooks, the capability table, the error
// mapping, `Native()` and every optional SPI surface are that driver's,
// untouched. There is no wrapper, so there is nothing here that can lose a
// claim or a surface.
//
// [Config.Require] is checked after the profile's own Open and before this
// function returns, against the table the connected broker really produced. A
// client that cannot meet a requirement is closed and the requirement's error
// is returned, which carries CodeUnsupported. That the check costs a dial is
// not avoidable and is not the expensive half: JetStream's table is read off
// the bound stream, so there is no honest answer before the connection exists —
// and the alternative is discovering the same thing at the first publisher,
// which costs the same dial plus however long the application took to get
// there.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var (
		client *mq.Client
		err    error
	)
	switch cfg.Profile {
	case ProfileCore:
		client, err = nats.Open(ctx, cfg.Core)
	case ProfileJetStream:
		client, err = jetstream.Open(ctx, cfg.JetStream)
	case ProfileUnset:
		return nil, errNoProfile
	default:
		return nil, errNoProfile
	}
	if err != nil {
		return nil, err
	}
	//nolint:contextcheck // requireClient closes the client it refuses, and the specification deliberately gives Close no context: a prompt close of a client nobody has a name for is not an operation a caller cancels (the same finding testkit excludes wholesale)
	if err := requireClient(client, cfg.Require); err != nil {
		return nil, err
	}
	return client, nil
}

// OpenWithClient opens a client on a *nats.Conn the caller already has.
//
// own says who closes it, and there is no default: the two profile drivers both
// insist on that and the facade has no business softening it. It is also the way
// in for an authentication scheme a typed Config cannot carry — NKey with a
// signing callback, a custom dialler, a websocket URL — and the one place where
// the facade's shape pays for itself twice over: the same *nats.Conn serves both
// profiles, because NATS Core and JetStream are the same protocol on the same
// connection.
func OpenWithClient(ctx context.Context, nc *natsgo.Conn, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	if nc == nil {
		return nil, errNilConn
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var (
		client *mq.Client
		err    error
	)
	switch cfg.Profile {
	case ProfileCore:
		client, err = nats.OpenWithClient(ctx, nc, own, cfg.Core)
	case ProfileJetStream:
		client, err = jetstream.OpenWithClient(ctx, nc, own, cfg.JetStream)
	case ProfileUnset:
		return nil, errNoProfile
	default:
		return nil, errNoProfile
	}
	if err != nil {
		return nil, err
	}
	//nolint:contextcheck // requireClient closes the client it refuses, and the specification deliberately gives Close no context: a prompt close of a client nobody has a name for is not an operation a caller cancels (the same finding testkit excludes wholesale)
	if err := requireClient(client, cfg.Require); err != nil {
		return nil, err
	}
	return client, nil
}

// OpenBackend opens the [github.com/iqhive/iqmq.Backend] itself, for a caller
// composing the SPI directly.
//
// It returns the INTERFACE and not a concrete type, and that is the facade's
// one unavoidable cost: a function that dispatches between two backend types
// cannot name both in its signature, so `*nats.Backend.Conn()`,
// `*jetstream.Backend.Stream()` and `*jetstream.Backend.Facts()` are reachable
// only through a type assertion. [CoreBackend] and [JetStreamBackend] are that
// assertion under a name a caller can find; the profile driver's own
// `OpenBackend` is the way to avoid it altogether, and a program that already
// knows which profile it wants should use that rather than this.
func OpenBackend(ctx context.Context, cfg Config) (mq.Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var (
		b   mq.Backend
		err error
	)
	switch cfg.Profile {
	case ProfileCore:
		b, err = nats.OpenBackend(ctx, cfg.Core)
	case ProfileJetStream:
		b, err = jetstream.OpenBackend(ctx, cfg.JetStream)
	case ProfileUnset:
		return nil, errNoProfile
	default:
		return nil, errNoProfile
	}
	if err != nil {
		return nil, err
	}
	if err := requireBackend(b, cfg.Require); err != nil {
		return nil, err
	}
	return b, nil
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, nc *natsgo.Conn, own driverkit.Ownership, cfg Config) (mq.Backend, error) {
	if nc == nil {
		return nil, errNilConn
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var (
		b   mq.Backend
		err error
	)
	switch cfg.Profile {
	case ProfileCore:
		b, err = nats.BackendWithClient(ctx, nc, own, cfg.Core)
	case ProfileJetStream:
		b, err = jetstream.BackendWithClient(ctx, nc, own, cfg.JetStream)
	case ProfileUnset:
		return nil, errNoProfile
	default:
		return nil, errNoProfile
	}
	if err != nil {
		return nil, err
	}
	if err := requireBackend(b, cfg.Require); err != nil {
		return nil, err
	}
	return b, nil
}

// CoreBackend is the type assertion for a Core backend, under a name a caller
// can find. It answers false for a JetStream backend and for anything else.
func CoreBackend(b mq.Backend) (*nats.Backend, bool) {
	cb, ok := b.(*nats.Backend)
	return cb, ok
}

// JetStreamBackend is the type assertion for a JetStream backend, under a name
// a caller can find. It answers false for a Core backend and for anything else.
func JetStreamBackend(b mq.Backend) (*jetstream.Backend, bool) {
	jb, ok := b.(*jetstream.Backend)
	return jb, ok
}

// ProfileOf reports which profile a client or backend came from, read off
// `Info().Profile` rather than remembered here.
//
// Reading it back is deliberate. The facade could have returned the profile it
// selected, which would agree with itself whatever the driver did; asking the
// driver instead means TestProfileOfMatchesTheSelectedProfile is checking the
// profile driver's own answer and not this package's memory of its own
// switch statement.
func ProfileOf(info mq.DriverInfo) Profile {
	for _, p := range Profiles() {
		if info.Name == p.Driver() {
			return p
		}
	}
	return ProfileUnset
}

// requireClient closes the client when it cannot meet the requirements.
//
// Closing is the only correct answer: a client that is returned with an error is
// a connection the caller has no name for and will never close, and the specification makes
// Close the only thing that releases it.
func requireClient(c *mq.Client, reqs []mq.Requirement) error {
	if len(reqs) == 0 {
		return nil
	}
	if err := c.Capabilities().Require(reqs...); err != nil {
		_ = c.Close()
		return err
	}
	return nil
}

// requireBackend is requireClient for the SPI route.
func requireBackend(b mq.Backend, reqs []mq.Requirement) error {
	if len(reqs) == 0 {
		return nil
	}
	if err := b.Capabilities().Require(reqs...); err != nil {
		_ = b.Close()
		return err
	}
	return nil
}
