package testkit

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// collect receives up to n deliveries within budget, calling fn on each where it
// lies in the receive buffer.
//
// The deliveries are never copied out of the slice. The specification makes a copy of a
// Delivery able to settle the original and unable to tell that something else
// already has, so a conformance kit that collected deliveries into a slice of
// its own would be testing a usage the specification warns against.
func collect(ctx context.Context, c *mq.ConsumerHandle, n int, budget time.Duration, fn func(d *mq.Delivery) error) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	dst := make([]mq.Delivery, n)
	bctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	got := 0
	for got < n {
		m, err := c.Receive(bctx, dst[:n-got])
		for i := range m {
			if ferr := fn(&dst[i]); ferr != nil {
				return got + m, ferr
			}
		}
		got += m
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

// collectAck receives n deliveries and settles each of them.
func collectAck(ctx context.Context, c *mq.ConsumerHandle, n int, budget time.Duration, fn func(d *mq.Delivery) error) (int, error) {
	return collect(ctx, c, n, budget, func(d *mq.Delivery) error {
		if fn != nil {
			if err := fn(d); err != nil {
				return err
			}
		}
		return ack(d)
	})
}

// ack settles d positively, treating AckNone answer as a delivery that
// is already complete.
//
// It is what every scenario whose SUBJECT is not settlement uses, and it exists
// because of a driver: NATS Core has no settlement of any kind, so its Source
// returns a nil Settler and the core answers every Ack with ErrNotSettleable.
// Before this, twenty-three scenarios that were about publication, fan-out,
// queue groups, headers, ordering or confirmation failed on the acknowledgement
// they performed on the way past, and a whole class of broker could not be run
// through the kit at all.
//
// A scenario that is really about settlement does NOT use this: it keeps
// d.Ack() and its own expectations, and a driver with no settlement documents
// those scenarios as inapplicable through Harness.Skip. The distinction is the
// point — forgiving ErrNotSettleable everywhere would let a driver that has
// settlement and gets it wrong pass the suite.
func ack(d *mq.Delivery) error {
	if err := d.Ack(); err != nil && !errors.Is(err, mq.ErrNotSettleable) {
		return err
	}
	return nil
}

// expectNothing fails when a delivery arrives within budget. It is how a
// scenario proves a negative — that a delayed message is not deliverable yet,
// that an acked message is not redelivered, that a fan-out subscription did not
// see another one's messages.
func expectNothing(ctx context.Context, c *mq.ConsumerHandle, budget time.Duration, what string) error {
	bctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(bctx, dst)
	if n > 0 {
		body := string(dst[0].Body)
		_ = dst[0].Nack()
		return &arrivedError{what: failf("%s: a delivery arrived when none should have (body %s)", what, strconv.Quote(body)).Error()}
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return failf("%s: Receive reported %v, want the deadline", what, err)
	}
	return nil
}

// arrivedError is "a delivery arrived where none should have", which a probe
// has to tell apart from expectNothing's other failure -- a receive that broke.
//
// The distinction is the same one noDeliveryError makes from the other side: a
// message that arrived is an OBSERVATION about the driver and a receive that
// failed is a probe that could not run, and a negative capability probe that
// read both as one thing could report a broken consumer as the broker's shape.
type arrivedError struct{ what string }

// Error implements error, in the wording the committed reports already carry.
func (a *arrivedError) Error() string { return a.what }

// isUnexpectedArrival reports whether err is a delivery that should not have
// come rather than a receive that failed.
func isUnexpectedArrival(err error) bool {
	var a *arrivedError
	return errors.As(err, &a)
}

// publishN publishes n messages whose bodies are the index, through a bound
// publisher, and returns the publisher for the caller to close.
func publishN(ctx context.Context, p *mq.PublisherHandle, n int, prefix string) error {
	for i := range n {
		m := &mq.Message{Body: []byte(prefix + strconv.Itoa(i))}
		if err := p.Publish(ctx, m); err != nil {
			return failf("publishing %s%d: %v", prefix, i, err)
		}
	}
	return nil
}

// bodiesEqual compares two payloads byte for byte, distinguishing nil from
// empty, which the specification makes observable and a driver is allowed to get wrong.
func bodiesEqual(a, b []byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return bytes.Equal(a, b)
}

// hasCode reports whether err carries the code.
func hasCode(err error, code mq.ErrorCode) bool { return mq.CodeOf(err) == code }

// wantCode fails unless err carries the code.
func wantCode(err error, code mq.ErrorCode, what string) error {
	if hasCode(err, code) {
		return nil
	}
	return failf("%s: %v (code %v), want %v", what, err, mq.CodeOf(err), code)
}

// wantUnsupported fails unless err is a refusal the specification allows for something a
// driver does not claim.
//
// ErrUnsupported is the answer the specification names. ErrNotSettleable is the other one,
// and it is not a driver being sloppy: AckNone model makes it the CORE's
// answer for every settlement on a source with no settler, so a driver that
// correctly advertises no ExplicitAck and no Nack cannot produce ErrUnsupported
// from a probe that acknowledges — the driver is never called. Insisting on
// ErrUnsupported there failed the negative direction of exactly the claims it
// was meant to verify.
func wantUnsupported(err error, what string) error {
	if errors.Is(err, mq.ErrUnsupported) || errors.Is(err, mq.ErrNotSettleable) {
		return nil
	}
	return failf("%s: %v (code %v), want ErrUnsupported or ErrNotSettleable", what, err, mq.CodeOf(err))
}

// settleFailed reports whether err is one of the settlement refusals the specification names,
// which is what a settlement that did not reach the broker looks like.
func settleFailed(err error) bool {
	return errors.Is(err, mq.ErrAlreadySettled) || errors.Is(err, mq.ErrLeaseLost) ||
		errors.Is(err, mq.ErrSettlementInProgress) || errors.Is(err, mq.ErrClosed) ||
		errors.Is(err, mq.ErrNotSettleable) || errors.Is(err, mq.ErrSettlementUnknown)
}
