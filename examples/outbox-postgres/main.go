// Command outbox-postgres is the transactional outbox: a business write and a
// message made atomic by a database transaction, a relay that turns outbox
// rows into publications, and an idempotent consumer that copes with the
// duplicates the relay is guaranteed to produce.
//
// It runs over a Postgres container (via testcontainers, or an externally
// managed server named by IQMQ_POSTGRES_ADDR) and over an in-process
// stand-in, with the in-process memory broker on both arms: the subject is the
// DATABASE and not the broker.
//
// # Why an outbox exists at all
//
// A broker is not in the database transaction. A service that writes an order
// and then publishes "order placed" has two failure windows and no way to
// close either:
//
//   - publish INSIDE the transaction, and a transaction that fails afterwards
//     leaves a message about an order that does not exist. The publication
//     cannot be rolled back. This is the same shape as the specification's
//     defect one level up: an event raised from inside an operation is not
//     evidence that the operation's after-effects have landed.
//   - publish AFTER the commit, and a crash between the two loses the message
//     with no trace at all.
//
// The outbox closes both by making the message part of the transaction: the
// order and a row in an `outbox` table are written together, and a separate
// relay publishes the rows. Nothing is lost, because a committed row is a
// committed promise to publish.
//
// # What it costs, and this is the half people skip
//
// The relay publishes and THEN marks the row sent, and those are two
// operations. A relay that dies between them republishes the row on its next
// pass -- so an outbox is AT-LEAST-ONCE by construction, not by accident, and
// the duplicate is not a rare race but the normal consequence of a restart.
// Reversing the order does not help: marking first and dying loses the
// message, which is worse.
//
// So the consumer has to be idempotent, and this program makes it so with
// github.com/iqhive/iqmq/x/idempotency over the SAME key the outbox row
// carries. That key is the business operation's identifier and not a
// per-publication one, which is the whole reason the republished row can be
// recognised.
//
// # What the two arms do and do not check
//
// The in-process store models the three properties the relay's correctness
// rests on -- an atomic business-write-and-append, a claim two relays cannot
// both win, and a lock that lapses -- so the protocol is exercised on every
// `make ci`. It does NOT check the SQL: the schema, the partial index and the
// `FOR UPDATE SKIP LOCKED` claim are only executed on the Postgres arm.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/idempotency"
)

// The topic, the subscription and the header the idempotency key travels in.
const (
	topic        = "orders"
	subscription = "billing"
	// keyHeader is a caller's header and is deliberately NOT under the specification's
	// mq.ReservedHeaderPrefix: a publisher cannot write one under that
	// prefix, so a key derived from it would never be found (see
	// idempotency.KeyFromHeader).
	keyHeader = "idempotency-key"
)

// orders is what the program places. Three commit and one is rolled back.
var orders = []string{"ord-1001", "ord-1002", "ord-1003"}

// rolledBack is the order whose transaction fails. It is the atomicity
// demonstration: neither the order row nor the outbox row survives.
const rolledBack = "ord-9999"

// window is how long the idempotency store suppresses a duplicate. It is
// generous relative to this program's lifetime because the duplicate arrives
// within milliseconds; a service sets it to longer than the longest
// redelivery its substrate can produce.
const window = time.Hour

func main() {
	if err := demo(); err != nil {
		log.Fatal(err)
	}
}

// demo is main's body as a function that returns.
func demo() error {
	addr := os.Getenv("IQMQ_POSTGRES_ADDR")
	if addr == "" {
		addr = "127.0.0.1:5432"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, err := openPG(ctx, dsnFor(addr))
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	_, err = run(ctx, os.Stdout, store)
	return err
}

// counters is what one run observed. It is returned so that a test can assert
// on the numbers as well as on the printed lines.
type counters struct {
	// published is how many publications the relay made, duplicates
	// included.
	published int
	// handled is how many deliveries reached the business handler.
	handled int
	// skipped is how many the idempotency middleware suppressed.
	skipped int
	// claims is what the claim store counted.
	claims idempotency.Counts
}

// run is the whole program.
func run(ctx context.Context, out io.Writer, store Store) (*counters, error) {
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	_, _ = fmt.Fprintf(out, "store: %s\n", store.Name(ctx))

	// 1. The business transactions. Three commit; one is rolled back after
	//    BOTH writes, which is the property a publication cannot have.
	for _, o := range orders {
		if err := place(ctx, store, o, false); err != nil {
			return nil, err
		}
	}
	if err := place(ctx, store, rolledBack, true); !errors.Is(err, errRolledBack) {
		return nil, fmt.Errorf("the rolled-back transaction reported the wrong error, want errRolledBack: %w", err)
	}
	placed, err := store.Orders(ctx)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(placed, orders) {
		return nil, fmt.Errorf("the orders table holds %v, want %v: the rolled-back transaction left an order behind", placed, orders)
	}
	unsent, err := store.Unsent(ctx)
	if err != nil {
		return nil, err
	}
	if unsent != len(orders) {
		return nil, fmt.Errorf("the outbox holds %d unsent row(s), want %d: the rolled-back transaction left an outbox row behind", unsent, len(orders))
	}
	_, _ = fmt.Fprintf(out, "atomic: %d order(s) and %d outbox row(s) committed together; the rolled-back transaction left neither\n",
		len(placed), unsent)

	// 2. The consumer, opened BEFORE anything is published so that nothing
	//    depends on a start position.
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Shutdown(ctx) }()
	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: subscription})
	if err != nil {
		return nil, err
	}
	claims, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = claims.Close() }()
	guard, err := idempotency.New(idempotency.Config{
		Store: claims,
		// The key is the OUTBOX ROW's key, carried in a header. It is the
		// business operation's identifier: the republished row carries the
		// same one, which is what makes the duplicate recognisable.
		Key:    idempotency.KeyFromHeader(keyHeader),
		Window: window,
		// Fail closed is the right choice HERE and is not the default: the
		// handler below stands for a side effect that must not repeat, and
		// the claim store is in the same process as the handler, so "the
		// store is unreachable" means "the process is broken" rather than
		// "a cache is down".
		OnUnavailable: idempotency.OnUnavailableRetry,
		// Every message an outbox relay publishes carries a key. One that
		// did not would be a relay defect, and processing it unguarded would
		// hide it.
		OnMissingKey: idempotency.MissingKeyFail,
	})
	if err != nil {
		return nil, err
	}

	c := &counters{}
	var (
		mu      sync.Mutex
		handled []string
	)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runErr := make(chan error, 1)
	go func() {
		runErr <- consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			// The business side effect. It is recorded rather than performed,
			// and the assertion below is that it happened once per order.
			mu.Lock()
			handled = append(handled, string(d.Body))
			mu.Unlock()
			return nil
		}), mq.WithMiddleware(guard.Middleware()))
	}()

	// 3. The relay, twice, with a simulated death in between.
	if err := relay(ctx, out, store, client, c); err != nil {
		return nil, err
	}

	// 4. Nothing is counted until the runner has finished. Shutdown drains
	//    and settles and Run then returns, which is where the middleware's
	//    own accounting for the last delivery is complete: reading it when
	//    the handler returns is the defect.
	//
	//    Shutdown alone is not enough, and that is what made this example
	//    fail about four times in thirty: it drains what the consumer HOLDS,
	//    not what the topic still has, so the relay's republished row could
	//    be sitting in the broker unfetched when the tally was taken —
	//    "4 publication(s) produced 3 handler call(s) and 0 skip(s), which
	//    does not add up". WaitIdle is the predicate: IdleDelivered means
	//    every record the broker could hand to a consumer has been handed
	//    over and settled. Waiting on a predicate rather than widening a
	//    budget is what the contributor notes asks of this flake shape.
	if b, ok := client.Native().(*memory.Broker); ok {
		ictx, icancel := context.WithTimeout(ctx, 30*time.Second)
		err := b.WaitIdle(ictx, memory.IdleDelivered)
		icancel()
		if err != nil {
			return nil, fmt.Errorf("waiting for the broker to hand over every publication: %w", err)
		}
	}
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := consumer.Shutdown(sctx); err != nil {
		return nil, fmt.Errorf("shutting the consumer down: %w", err)
	}
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, mq.ErrClosed) {
		return nil, fmt.Errorf("the run ended with %w", err)
	}

	mu.Lock()
	seen := slices.Clone(handled)
	mu.Unlock()
	counts := guard.Counts()
	c.handled, c.skipped, c.claims = int(counts.Processed), int(counts.Skipped), counts

	if !slices.Equal(seen, orders) {
		return nil, fmt.Errorf("the handler saw %v, want %v exactly once each: the idempotency middleware is what makes a republished row harmless", seen, orders)
	}
	if c.published <= len(orders) {
		return nil, fmt.Errorf("the relay published %d time(s) for %d order(s); this example is about the duplicate a relay produces, and there was none", c.published, len(orders))
	}
	if c.skipped != c.published-len(orders) {
		return nil, fmt.Errorf("%d publication(s) produced %d handler call(s) and %d skip(s), which does not add up", c.published, c.handled, c.skipped)
	}
	if counts.Deferred != 0 || counts.Unguarded != 0 || counts.Refused != 0 || counts.NoKey != 0 {
		return nil, fmt.Errorf("the guard reported deferred=%d unguarded=%d refused=%d no-key=%d, want none of them: every publication carries a key and the store is in this process",
			counts.Deferred, counts.Unguarded, counts.Refused, counts.NoKey)
	}
	_, _ = fmt.Fprintf(out, "consumer: %d publication(s) became %d side effect(s); x/idempotency skipped %d duplicate(s)\n",
		c.published, c.handled, c.skipped)

	left, err := store.Unsent(ctx)
	if err != nil {
		return nil, err
	}
	if left != 0 {
		return nil, fmt.Errorf("%d outbox row(s) are still unsent", left)
	}
	_, _ = fmt.Fprintln(out, "outbox: 0 row(s) left unsent")
	return c, nil
}

// place writes one order and its outbox row in one transaction.
func place(ctx context.Context, store Store, order string, rollback bool) error {
	return store.Place(ctx, order, []Outgoing{{
		// The key is the ORDER's identifier. A relay that generated a key
		// per publication would make every republication a new operation,
		// and the consumer could not tell a duplicate from a new order --
		// which is the false-first, produced on purpose.
		Key:  order,
		Body: order,
	}}, rollback)
}

// relay runs two passes over the outbox and prints what each did.
//
// The first pass publishes every row and marks all but ONE sent, which is a
// relay that died between the publication and the mark. The second pass finds
// that row once its lock has lapsed and publishes it again -- which is the
// duplicate the consumer's idempotency exists for.
func relay(ctx context.Context, out io.Writer, store Store, cl *mq.Client, c *counters) error {
	first, err := store.Claim(ctx, len(orders))
	if err != nil {
		return err
	}
	if len(first) != len(orders) {
		return fmt.Errorf("the first relay pass claimed %d row(s), want %d", len(first), len(orders))
	}
	for _, r := range first {
		if err := publishRow(ctx, cl, r); err != nil {
			return err
		}
		c.published++
	}
	// Every row but the last is marked. The last one's relay "died" here,
	// after the publication and before the mark -- the one window an outbox
	// cannot close.
	marked := make([]int64, 0, len(first)-1)
	for _, r := range first[:len(first)-1] {
		marked = append(marked, r.ID)
	}
	if err := store.MarkSent(ctx, marked); err != nil {
		return err
	}
	stranded := first[len(first)-1]
	_, _ = fmt.Fprintf(out, "relay: pass 1 claimed %d row(s), published %d and marked %d sent — the last row's relay died before its mark\n",
		len(first), len(first), len(marked))

	// The lock has to lapse before another relay can take the row, and there
	// is no way to be on the far side of a lock but to wait for it: it is the
	// database's clock on the Postgres arm, and the wall clock on the other.
	// A shorter lock would make this faster and a lock shorter than a publish
	// would produce duplicates a service does not want.
	if err := sleepPast(ctx, pastTheLock); err != nil {
		return err
	}

	second, err := store.Claim(ctx, len(orders))
	if err != nil {
		return err
	}
	if len(second) != 1 || second[0].ID != stranded.ID {
		return fmt.Errorf("the second relay pass claimed %d row(s) (%v), want exactly the one row that was never marked (%d)",
			len(second), ids(second), stranded.ID)
	}
	if second[0].Attempts < 2 {
		return fmt.Errorf("the re-claimed row reports %d attempt(s), want 2 or more: a relay cannot tell a poison row from an unlucky one without that count", second[0].Attempts)
	}
	for _, r := range second {
		if err := publishRow(ctx, cl, r); err != nil {
			return err
		}
		c.published++
	}
	if err := store.MarkSent(ctx, ids(second)); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "relay: pass 2 re-claimed that row once its %v lock lapsed and published it AGAIN, on attempt %d — an outbox is at-least-once by construction\n",
		lockFor, second[0].Attempts)
	return nil
}

// publishRow turns one outbox row into a publication.
func publishRow(ctx context.Context, cl *mq.Client, r Row) error {
	m := &mq.Message{Body: []byte(r.Body), ID: r.Key}
	m.Headers.Set(keyHeader, []byte(r.Key))
	if err := cl.Publish(ctx, topic, m); err != nil {
		return fmt.Errorf("publishing outbox row %d: %w", r.ID, err)
	}
	return nil
}

// ids is the row identifiers.
func ids(rows []Row) []int64 {
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.ID)
	}
	return out
}

// sleepPast waits d out on the wall clock.
func sleepPast(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
