package amqp10

import (
	"context"
	"errors"

	"github.com/Azure/go-amqp"

	"bitbucket.org/iqhive/iqmq/drivers/amqp10/internal/client"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// The scripted peer's own refusals. They are configuration errors rather than
// protocol ones: a test that named a link the peer does not have has a bug in
// the test.
var (
	errNoPeer = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: BackendWithPeer was given no peer")
	errNoSuchLink = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the scripted peer has no link on that address")
)

// Peer is this core's scripted AMQP 1.0 peer, reachable from a profile module.
//
// # Why it exists
//
//	use of internal package bitbucket.org/iqhive/iqmq/drivers/amqp10/internal/client not allowed
//
// `hack/FOLLOWUPS.md` records what that cost the first profile: the
// link-level half of its stale-disposition test had to live in the core's own
// package instead of against the profile, and it named the fix — "a
// constructor in the core package returning a value whose type a caller need
// not name". This is that constructor, plus the handful of accessors a profile
// actually needs, and it is here rather than in `testing.go` because
// `testing.go` is about the conformance kit.
//
// # Why a profile needs it and a broker cannot substitute
//
// Three of the behaviours a profile has to get right cannot be provoked from a
// broker at all, and all three are disqualifiers:
//
//   - a disposition sent on a link that has since detached, which must not
//     settle a delivery on the reattached one;
//   - a consumer cancelled while a fetch is parked;
//   - a send whose disposition never arrives.
//
// A real Artemis or ActiveMQ Classic will not detach a link on request, and a
// test that killed the container instead would lose the link-level detail the
// scenarios are about. A loopback go-amqp peer cannot do it either: go-amqp
// implements the CLIENT role of a link, so the accepted side can dial and
// cannot serve.
//
// # Addressing
//
// Every accessor takes the link's ADDRESS rather than a handle, because that is
// what a profile knows: it computed the address itself. Where two links share
// one address the MOST RECENTLY attached is the one meant, which is what a
// re-attach after a detach produces and is therefore the case these tests are
// written about.
type Peer struct{ f *client.Fake }

// NewPeer returns a scripted peer that announces props in its open
// performative, which is what [Facts] is read from — so a profile's
// CheckBroker can be driven from here.
func NewPeer(props map[string]any) *Peer { return &Peer{f: client.NewFake(props)} }

// BackendWithPeer opens a backend over a scripted peer.
//
// It is [BackendWithDialer] with the seam's type hidden, and it dials
// immediately and once, exactly as the real constructor does, so a profile's
// Validate, capability table and CheckBroker all run on the same path a real
// open takes.
func BackendWithPeer(ctx context.Context, profile Profile, cfg Config, p *Peer) (*Backend, error) {
	if p == nil {
		return nil, errNoPeer
	}
	return BackendWithDialer(ctx, profile, cfg, p.f)
}

// FailDials makes the next len(errs) dials fail with the given errors, which is
// how a profile drives the reconnect loop.
func (p *Peer) FailDials(errs ...error) { p.f.FailDials(errs...) }

// Dials reports how many times the backend has dialled.
func (p *Peer) Dials() int { return p.f.Dials() }

// RefuseAttach makes every attach to addr fail with err, which is how a
// profile drives its own not-found rule: both brokers refuse an attach to a
// destination that does not exist with `amqp:not-found`, and that refusal is
// what makes a driver's "creates no topology" claim provable.
func (p *Peer) RefuseAttach(addr string, err error) { p.f.RefuseAttach(addr, err) }

// AllowAttach undoes RefuseAttach for addr.
func (p *Peer) AllowAttach(addr string) { p.f.AllowAttach(addr) }

// HoldSends makes the next n publications transfer and never receive a
// disposition, which is the ambiguous window held open.
//
// The script is POSITIONAL FROM THE PEER'S FIRST TRANSFER, not from the call:
// entry i answers the peer's (i+1)th transfer ever, so a test that publishes
// once and then asks for a hold has to script the first transfer too
// (`PlanSendStates(&amqp.StateAccepted{})` before `HoldSends(1)`). Running off
// the end of the script accepts the transfer, which is why an unscripted test
// sees every publication succeed.
func (p *Peer) HoldSends(n int) {
	plans := make([]client.SendPlan, n)
	for i := range plans {
		plans[i] = client.SendPlan{Hold: true}
	}
	p.f.PlanSends(plans...)
}

// PlanSendStates makes the next publications resolve with the given
// dispositions, in order. It appends to the same positional script
// [Peer.HoldSends] writes to; see there for what "next" counts from.
func (p *Peer) PlanSendStates(states ...amqp.DeliveryState) {
	plans := make([]client.SendPlan, len(states))
	for i, s := range states {
		plans[i] = client.SendPlan{State: s}
	}
	p.f.PlanSends(plans...)
}

// Kill fails the current connection with err, as a broker restart does.
func (p *Peer) Kill(err error) {
	if c := p.f.LastConn(); c != nil {
		c.Kill(err)
	}
}

// Deliver pushes messages to the receiver attached to addr.
func (p *Peer) Deliver(addr string, msgs ...*amqp.Message) error {
	r := p.receiver(addr)
	if r == nil {
		return errNoSuchLink
	}
	r.Deliver(msgs...)
	return nil
}

// DetachReceiver detaches the receiver attached to addr with err, which is the
// link-level failure a broker's address deletion or a management detach
// produces and which no container can be asked for.
func (p *Peer) DetachReceiver(addr string, err error) error {
	r := p.receiver(addr)
	if r == nil {
		return errNoSuchLink
	}
	r.Detach(linkError(err))
	return nil
}

// DetachSender detaches the sender attached to addr with err.
func (p *Peer) DetachSender(addr string, err error) error {
	s := p.sender(addr)
	if s == nil {
		return errNoSuchLink
	}
	s.Detach(linkError(err))
	return nil
}

// ReceiverDetached reports whether the receiver on addr has detached.
func (p *Peer) ReceiverDetached(addr string) bool {
	r := p.receiver(addr)
	return r != nil && r.Detached()
}

// Receivers reports how many receiving links have ever been attached to addr,
// which is how a test tells a re-attach from a survival.
func (p *Peer) Receivers(addr string) int {
	n := 0
	for _, r := range p.allReceivers() {
		if r.Address() == addr {
			n++
		}
	}
	return n
}

// Credit reports the credit outstanding on the receiver attached to addr, or
// -1 when there is no such link. It is the prefetch the driver is managing by
// hand (credit.go).
func (p *Peer) Credit(addr string) int {
	r := p.receiver(addr)
	if r == nil {
		return -1
	}
	return int(r.Credit())
}

// Granted reports the total credit ever issued on addr.
func (p *Peer) Granted(addr string) int {
	r := p.receiver(addr)
	if r == nil {
		return -1
	}
	return int(r.Granted())
}

// PeerDisposition is one disposition the peer received, in terms a profile
// module can name.
type PeerDisposition struct {
	// Outcome is the disposition's outcome, as one of the Outcome constants
	// in errors.go ("accepted", "released", "rejected", "modified").
	Outcome string
	// DeliveryFailed is `modified`'s delivery-failed flag, which is what
	// makes an attempt count rise across a nack.
	DeliveryFailed bool
	// UndeliverableHere is `modified`'s undeliverable-here flag. Measured on
	// Artemis: setting it stops the message being redelivered to THIS
	// consumer at all, so a single-consumer subscription never sees it again.
	UndeliverableHere bool
	// Annotations is what a `modified` carried, which is where a profile's
	// nack-delay rule writes its key.
	Annotations amqp.Annotations
	// Tag is the delivery tag of the message settled, so that a test can say
	// WHICH delivery was settled rather than how many were.
	Tag string
}

// Dispositions returns the dispositions the receiver on addr has received, in
// order.
func (p *Peer) Dispositions(addr string) []PeerDisposition {
	r := p.receiver(addr)
	if r == nil {
		return nil
	}
	in := r.Dispositions()
	out := make([]PeerDisposition, 0, len(in))
	for i := range in {
		out = append(out, PeerDisposition{
			Outcome:           in[i].Disposition.Outcome.String(),
			DeliveryFailed:    in[i].Disposition.DeliveryFailed,
			UndeliverableHere: in[i].Disposition.UndeliverableHere,
			Annotations:       in[i].Disposition.Annotations,
			Tag:               in[i].Tag,
		})
	}
	return out
}

// Sent returns the messages the sender on addr transferred, in order.
func (p *Peer) Sent(addr string) []*amqp.Message {
	s := p.sender(addr)
	if s == nil {
		return nil
	}
	return s.Sent()
}

// Held reports how many transfers on addr are waiting for a disposition the
// peer will not send.
func (p *Peer) Held(addr string) int {
	s := p.sender(addr)
	if s == nil {
		return -1
	}
	return s.Held()
}

// ResolveHeld resolves n held transfers on addr with state.
func (p *Peer) ResolveHeld(addr string, n int, state amqp.DeliveryState) bool {
	s := p.sender(addr)
	if s == nil {
		return false
	}
	return s.ResolveHeld(n, state)
}

// SetMaxMessageSize sets what the sender on addr advertises as its
// max-message-size, which is the one limit AMQP 1.0 carries on a link.
func (p *Peer) SetMaxMessageSize(addr string, n uint64) error {
	s := p.sender(addr)
	if s == nil {
		return errNoSuchLink
	}
	s.SetMaxMessageSize(n)
	return nil
}

// linkError puts a peer's detach reason in the shape go-amqp reports one.
//
// The library does not pass a peer's error through: it wraps the condition in
// a *amqp.LinkError, and a driver tells a LINK's death from a session's or a
// connection's by that type. A scripted peer that handed back a bare
// *amqp.Error would therefore exercise a shape the library never produces --
// and would let a driver that cannot tell the three apart pass.
func linkError(err error) error {
	if err == nil {
		return &amqp.LinkError{}
	}
	var link *amqp.LinkError
	if errors.As(err, &link) {
		return err
	}
	var aerr *amqp.Error
	if errors.As(err, &aerr) {
		return &amqp.LinkError{RemoteErr: aerr}
	}
	return err
}

// receiver returns the most recently attached receiver on addr.
func (p *Peer) receiver(addr string) *client.FakeReceiver {
	var found *client.FakeReceiver
	for _, r := range p.allReceivers() {
		if r.Address() == addr {
			found = r
		}
	}
	return found
}

// sender returns the most recently attached sender on addr.
func (p *Peer) sender(addr string) *client.FakeSender {
	var found *client.FakeSender
	for _, c := range p.f.Conns() {
		for _, s := range c.Senders() {
			if s.Address() == addr {
				found = s
			}
		}
	}
	return found
}

// allReceivers is every receiver of every connection, oldest first.
func (p *Peer) allReceivers() []*client.FakeReceiver {
	var out []*client.FakeReceiver
	for _, c := range p.f.Conns() {
		out = append(out, c.Receivers()...)
	}
	return out
}
