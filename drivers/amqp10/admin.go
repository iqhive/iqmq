package amqp10

import (
	"context"

	mq "github.com/iqhive/iqmq"
)

// Topology, and why this file is mostly a refusal.
//
// AMQP 1.0 has no portable topology API. The protocol can ATTACH a link to an
// address and it cannot create one, delete one, purge one or report its depth:
// every broker that does those things does them over a management interface of
// its own — RabbitMQ's HTTP API, Artemis's management address and its JMX
// surface, Azure Service Bus's ARM plane — and none of the four resembles
// another.
//
// So [github.com/iqhive/iqmq.Client.Admin] answers ErrUnsupported unless the
// profile supplies one, and the SEAM is [Profile.Admin]. That is the honest
// shape: a core that invented a management protocol would be wrong on every
// broker, and a core that refused outright would make the four brokers that do
// have one unable to offer it through the portable API at all.
//
// The interface is satisfied CONDITIONALLY, which is the part worth reading the
// code for. The core asserts [github.com/iqhive/iqmq.AdminBackend] once, when
// the client is built, and records the answer; a backend that satisfied the
// interface and then answered ErrUnsupported would make FeatureAdmin look
// available to a caller who asked the client rather than the table. So a
// profile with no Admin produces a *Backend, which is not an AdminBackend, and
// a profile with one produces an *adminBackend, which is — and
// [checkClaim] refuses a profile that claims FeatureAdmin without supplying
// Admin, so the table and the surface cannot disagree.

// adminBackend is the backend of a profile that has a topology surface.
//
// It embeds *Backend rather than wrapping it, so every other method is the
// same one: what it adds is the single method whose PRESENCE is the claim.
type adminBackend struct{ *Backend }

// compile-time proof that the variant really carries the surface, and that the
// plain backend really does not.
var _ mq.AdminBackend = (*adminBackend)(nil)

// Admin returns the profile's topology surface.
func (a *adminBackend) Admin() (mq.Admin, error) {
	if err := a.CheckOpen(context.Background()); err != nil {
		return nil, err
	}
	return a.profile.Admin(context.Background(), a.Backend)
}

// AdminBackend wraps a backend so that it carries
// [github.com/iqhive/iqmq.AdminBackend] when its profile supplies one, and
// returns it unchanged when the profile does not.
//
// It is what [Open] puts the backend through before it builds the client, and it
// is exported because a broker-specific module that composes the SPI itself has
// to do the same thing — and a module that forgot would have a profile with a
// working Admin that Client.Admin could not reach.
func AdminBackend(b *Backend) mq.Backend {
	if b == nil {
		return nil
	}
	if b.profile.Admin == nil {
		return b
	}
	return &adminBackend{Backend: b}
}
