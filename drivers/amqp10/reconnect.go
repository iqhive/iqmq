package amqp10

import (
	"context"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// The driver-owned dial loop.
//
// go-amqp does not reconnect. It has no reconnect option, no retry policy and
// no re-attach: a connection whose TCP stream dies closes
// [github.com/Azure/go-amqp.Conn.Done], every session and link on it starts
// failing, and that is the end of it. So this core genuinely owns the dial loop
// — which is what [bitbucket.org/iqhive/iqmq/driverkit.Reconnector] was designed
// for and, as recorded in the maintenance notes, what no real client library had
// needed until now.
//
// # What the kit supplies and what it does not
//
// [bitbucket.org/iqhive/iqmq/driverkit.Reconnector] supplies four of the five
// obligations and is used for all four: the jittered backoff, the
// generation counter the staleness guard reads, the
// [bitbucket.org/iqhive/iqmq.Hooks.OnReconnect] event, and the health
// transitions. What it does not supply is the SUPERVISOR — the goroutine that
// watches a connection, notices that it died, and drives the rest — because its
// Connect is one dial-with-retry rather than a loop, and it has no hook that
// runs between a successful dial and the report of readiness. So that goroutine
// is below, and it is the only part of this file that is not the kit's.
//
// # The sequence a lost connection goes through
//
// The order is the whole of it and every step depends on the one before:
//
//  1. The generation is bumped ([Reconnector.Lost]). From this instant every
//     delivery handed out under the old connection is stale.
//  2. Every source's token table is invalidated, so a settlement carrying one
//     of those deliveries reports [bitbucket.org/iqhive/iqmq.ErrLeaseLost] and is
//     never sent — the disqualifier, and the reason step 1 comes before
//     anything that could take time.
//  3. Every sink's outstanding publications are failed AMBIGUOUS: the transfers
//     had left and their dispositions had not arrived, so nobody knows whether
//     the broker holds them. Failing them as definite is the
//     misclassification the specification forbids.
//  4. The health reporter goes Degraded and the hook fires.
//
// Nothing in this file buffers anything, and nothing in it spins: the
// supervisor waits on a channel, and the backoff waits on the injected clock's
// timer.

// supervise is the connection's supervisor: it watches the connection, and when
// it dies it invalidates what the connection owned and re-dials.
//
// It is one goroutine per backend for the life of the backend, which is what
// the specification's "bound all queues and goroutines" asks of a driver's background work.
func (b *Backend) supervise() {
	defer b.superWG.Done()
	for {
		conn, _, err := b.connection()
		if err != nil {
			return
		}
		select {
		case <-conn.Done():
		case <-b.superCtx.Done():
			return
		}
		lost := conn.Err()
		b.losses.Add(1)

		// Steps 1 to 4 in one call, in the order the helper guarantees: the
		// generation first, then `Backend.invalidate`, then Degraded, and all
		// of it before anything that can block. A settlement that arrives
		// while this is running must already be refused.
		b.reconn.Lost(lost)

		// The dead connection is dropped and closed HERE rather than when a
		// replacement arrives, and both halves matter. Closing it releases the
		// library's own reader and writer goroutines, which goleak counts.
		// Dropping it means an operation during the reconnect window answers
		// with the connection's own error — CodeUnavailable and retryable —
		// instead of attaching a link to a corpse, which costs a round trip
		// attempt to reach the same answer and, on a peer that answers an
		// attach on a dying connection, could hand back a link that is stale
		// the instant it exists.
		b.cmu.Lock()
		b.conn, b.connErr = nil, lost
		b.cmu.Unlock()
		_ = conn.Close()

		if b.cfg.NoReconnect {
			// The caller supervises its own connection. Degraded is terminal
			// here: every operation from now on reports the connection's own
			// error, mapped to CodeUnavailable, rather than silently waiting
			// for a dial nobody is going to make.
			b.health.Degraded(lost)
			return
		}
		if err := b.reconn.Connect(b.superCtx); err != nil {
			// Either the client is closing or the attempt ceiling was
			// reached. The connection stays nil and its error stays readable,
			// so every operation afterwards answers with it rather than
			// blocking.
			b.cmu.Lock()
			if b.conn == nil {
				b.connErr = err
			}
			b.cmu.Unlock()
			return
		}
		b.reconnects.Add(1)
	}
}

// redial is the [bitbucket.org/iqhive/iqmq/driverkit.ReconnectConfig.Dial] the
// reconnector calls, and the only place a replacement connection is installed.
//
// The address list is tried round-robin across attempts rather than all of them
// per attempt, so that the backoff is what paces the loop: an attempt that tried
// every address would make the delay between attempts a function of the list's
// length, and a five-address failover set would hammer a down broker five times
// per backoff interval.
func (b *Backend) redial(ctx context.Context) error {
	dctx, cancel := b.dialBound(ctx)
	defer cancel()
	addr := b.nextAddr()
	conn, err := b.dialer.Dial(dctx, addr, b.cfg.connOptions(b.profile))
	if err != nil {
		return err
	}
	b.cmu.Lock()
	old := b.conn
	b.conn, b.connErr = conn, nil
	b.cmu.Unlock()
	if old != nil {
		// The connection this one replaces is already dead — the supervisor
		// only re-dials after Done closed — but closing it releases the
		// library's own reader and writer goroutines, which goleak counts.
		_ = old.Close()
	}
	return nil
}

// nextAddr is the address the next dial attempt uses.
func (b *Backend) nextAddr() string {
	b.cmu.Lock()
	defer b.cmu.Unlock()
	addr := b.addrs[b.addrNext%len(b.addrs)]
	b.addrNext++
	return addr
}

// dialBound bounds one dial attempt.
//
// A dial needs a bound of its own even when the caller gave none, because a host
// that completes the TCP handshake and then says nothing would otherwise hold
// the loop for ever — and a dial loop that never returns from an attempt never
// reaches its backoff, which is the busy-spin's quiet cousin.
func (b *Backend) dialBound(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.cfg.DialTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.cfg.DialTimeout)
}

// invalidate tells every child that the connection under it is gone. It is the
// [bitbucket.org/iqhive/iqmq/driverkit.ReconnectConfig.Invalidate] seam, so the
// reconnector calls it with the generation already bumped and before its first
// dial attempt.
//
// The sources go first and for the reason step 2 exists: their token tables are
// what refuse a stale settlement, and a settlement racing this must find the
// table already invalidated rather than half of it.
func (b *Backend) invalidate(cause error) {
	b.mu.Lock()
	srcs := make([]*Source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	sinks := make([]*Sink, 0, len(b.sinks))
	for s := range b.sinks {
		sinks = append(sinks, s)
	}
	b.mu.Unlock()
	for _, s := range srcs {
		s.connectionLost(cause)
	}
	for _, s := range sinks {
		s.connectionLost()
	}
}

// onReconnect is the hook every dial attempt goes through.
//
// It is wired from [Config.Hooks] and not from the client's, because a driver
// cannot reach its client's hooks: a Backend is built before the Client that
// holds them. That is the gap recorded in the maintenance notes, and taking Hooks in
// the driver's own Config is what every driver in this repository does about it.
func (b *Backend) onReconnect(ctx context.Context, ev mq.ReconnectEvent) {
	if b.cfg.Hooks.OnReconnect != nil {
		b.cfg.Hooks.OnReconnect(ctx, ev)
	}
}

// Backoff is the delay the dial loop would wait before an attempt, given the
// previous delay.
//
// It is exported so that a deployment can print its own reconnection schedule
// and so that a test can assert on the policy without running the loop, which
// is the only way to check a jittered backoff without waiting for it.
//
// The previous delay is a parameter because one of the policies grows
// from it rather than from the attempt number, so a schedule computed without it
// would be wrong for that policy alone.
func (b *Backend) Backoff(attempt int, prev time.Duration) time.Duration {
	return b.reconn.Backoff(attempt, prev)
}
