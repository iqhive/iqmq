package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// pinnedPostgres is the image conformance/harness/images.lock pins.
//
// It is the digest and not a tag, and the reason is the whole point of that
// file: `postgres:latest` is a moving target, and an example whose SQL broke
// because the base image moved would be a failure nobody could reproduce.
//
// pinnedMajor is what that digest turns out to be, and it is asserted rather
// than read: the digest's sha256 prefix happens to start "18cfe3ef" and the
// server it runs is PostgreSQL 17, which is exactly the kind of coincidence
// that makes a guessed version look checked. The first run of this arm
// reported `postgres 17` against an expected `postgres 18` and that is how the
// number here was established. Moving images.lock moves this line.
const (
	pinnedPostgres = "postgres@sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73"
	pinnedMajor    = "17"
)

// The credentials the container is created with. They are the container's own
// and are not a secret; dsnFor builds the same pair.
const (
	pgUser = "iqmq"
	pgPass = "iqmq"
	pgDB   = "iqmq"
)

// The environment variables that decide how the Postgres arm gets a database.
const (
	// addrEnv names an externally managed server as host:port, or a whole
	// DSN. It is conformance/harness's own convention and it is the first
	// thing checked: a caller who already has a database should not have a
	// container started for them.
	addrEnv = "IQMQ_POSTGRES_ADDR"
	// containerEnv asks this test to START one with testcontainers.
	//
	// It is opt-in rather than "start one if Docker is available", and that
	// is deliberate. `make examples` runs in every `make ci`, several times
	// per increment, on a shared machine that is running about fifty other
	// people's containers -- so an example that silently started a database
	// whenever it could would be starting one on every run of the gate.
	containerEnv = "IQMQ_POSTGRES_TESTCONTAINERS"
)

// TestRunAgainstPostgres is the container arm.
//
// It skips when neither variable is set and FAILS when one is: it asserts
// exactly what the in-process arm asserts, plus the store's own name line. So
// a Postgres whose behaviour differs from the in-process stand-in is a failure
// here and not a skip, which is what makes that stand-in a legitimate subject
// for `make ci`.
//
// What this arm adds, and nothing else in the module can: the schema, the
// partial index, and the `FOR UPDATE SKIP LOCKED` claim are EXECUTED.
func TestRunAgainstPostgres(t *testing.T) {
	store, ok := postgresStore(t, true)
	if !ok {
		return
	}
	runExample(t, store, "postgres "+pinnedMajor)
}

// TestPostgresClaimsAreDisjoint and TestPostgresRollbackWritesNeither run the
// two store properties against the real database.
//
// They are separate tests rather than part of the run above because each is a
// property of the SQL: the first is what `FOR UPDATE SKIP LOCKED` buys, and
// the second is what the transaction buys. A failure in either names the
// property instead of failing a five-line comparison.
func TestPostgresClaimsAreDisjoint(t *testing.T) {
	store, ok := postgresStore(t, false)
	if !ok {
		return
	}
	runDisjointClaims(t, store)
}

func TestPostgresRollbackWritesNeither(t *testing.T) {
	store, ok := postgresStore(t, false)
	if !ok {
		return
	}
	runRollback(t, store)
}

// postgresStore returns a store over a truncated database, and reports whether
// there is one.
//
// `report` is true for exactly one caller -- TestRunAgainstPostgres, the
// canonical case -- because `make examples` prints one line per reported arm
// and four identical skips say nothing the first does not. The reporting
// contract still bites: a module that mentions IQMQ_EXAMPLE_REPORT and reports
// nothing fails `make examples` by name, so renaming or filtering away that
// one test is a failure rather than a silent green.
func postgresStore(t *testing.T, report bool) (Store, bool) {
	t.Helper()
	say := func(status, detail string) {
		if report {
			reportArm(t, armBroker, status, detail)
		}
	}
	addr := os.Getenv(addrEnv)
	switch {
	case addr != "":
		say(statusRan, addrEnv+"="+addr)
	case os.Getenv(containerEnv) == "1":
		addr = startPostgres(t)
		say(statusRan, "testcontainers "+pinnedPostgres[:24]+"…")
	default:
		if os.Getenv("IQMQ_REQUIRE_BROKERS") == "1" {
			say(statusSkipped, addrEnv+" unset and IQMQ_REQUIRE_BROKERS=1")
			t.Fatalf("%s is unset, %s is not 1, and IQMQ_REQUIRE_BROKERS=1", addrEnv, containerEnv)
		}
		say(statusSkipped, addrEnv+" unset and "+containerEnv+" is not 1")
		t.Skipf("set %s=host:port for a server you manage, or %s=1 to have this test start the pinned container (see README.md)", addrEnv, containerEnv)
		return nil, false
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	store, err := openPG(ctx, dsnFor(addr))
	if err != nil {
		t.Fatalf("openPG(%q): %v", addr, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// Every case starts from an empty database. A run against a server the
	// previous run used would otherwise read the previous run's orders, and
	// this example counts them -- the same hazard the specification recorded
	// for a topic.
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := store.Truncate(ctx); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	return store, true
}

// startPostgres starts the pinned container and returns its host:port.
//
// testcontainers.CleanupContainer registers the termination with the test, so
// the container this test started is the container this test removes -- by its
// own handle and never by a name, a label or an image match. About fifty other
// people's containers run on the machine this repository is developed on.
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	req := testcontainers.ContainerRequest{
		Image:        pinnedPostgres,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     pgUser,
			"POSTGRES_PASSWORD": pgPass,
			"POSTGRES_DB":       pgDB,
		},
		// Postgres logs "ready to accept connections" once while it
		// initialises the data directory and again when it is really
		// listening, so the wait is for the SECOND. Waiting for the first is
		// the classic testcontainers-Postgres flake: the connection is
		// refused a moment later when the init script restarts the server.
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(2 * time.Minute),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("starting %s: %v", pinnedPostgres, err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("container port: %v", err)
	}
	// The mapped port, never a fixed one. On a shared Docker host a port you
	// name may already be held by somebody else's container, and a container
	// that failed to start while your test still passed means your test
	// measured THEIR database.
	return host + ":" + port.Port()
}

// TestPostgresClaimSkipsARowAnotherTransactionHolds is what `FOR UPDATE SKIP
// LOCKED` actually buys, and it is here because the obvious test for it cannot
// fail.
//
// TestTwoRelaysClaimDisjointRows passes with SKIP LOCKED removed: eight one-statement UPDATEs against forty rows serialise, the first
// takes them all, and the rest see locked_until already set. The difference
// SKIP LOCKED makes is what happens when a row is held by a transaction that
// has NOT finished — with it, the claim steps over that row and returns the
// others at once; without it, the claim BLOCKS until the holder commits.
//
// So this test holds a row with a transaction of its own and asserts both
// halves: the claim comes back promptly, and it comes back without that row.
// A relay that blocks here is a relay that stops publishing every other
// message in the outbox because one row is locked, which on a busy database is
// a queue that stalls for no visible reason.
func TestPostgresClaimSkipsARowAnotherTransactionHolds(t *testing.T) {
	store, ok := postgresStore(t, false)
	if !ok {
		return
	}
	pg, isPG := store.(*pgStore)
	if !isPG {
		t.Fatalf("postgresStore returned a %T", store)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	const rows = 3
	for i := range rows {
		key := "row-" + string(rune('a'+i))
		if err := store.Place(ctx, key, []Outgoing{{Key: key, Body: key}}, false); err != nil {
			t.Fatalf("Place: %v", err)
		}
	}

	// A transaction of our own, holding the FIRST row. This is what another
	// relay's claim looks like from the outside while it is still working.
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var held int64
	if err := tx.QueryRow(ctx,
		`SELECT id FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE`).Scan(&held); err != nil {
		t.Fatalf("locking a row: %v", err)
	}

	// The claim, with a deadline well short of anything a blocked claim would
	// wait for. A blocked claim fails here with a context deadline, which is
	// the failure this test exists to produce.
	cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
	defer ccancel()
	start := time.Now()
	claimed, err := pg.Claim(cctx, rows)
	took := time.Since(start)
	if err != nil {
		t.Fatalf("Claim while one row was held by another transaction reported %v after %v; without FOR UPDATE SKIP LOCKED it BLOCKS on the held row, which stalls the whole outbox for one locked message", err, took)
	}
	if len(claimed) != rows-1 {
		t.Fatalf("the claim returned %d row(s), want %d: it should step over the held row and take the rest", len(claimed), rows-1)
	}
	for _, r := range claimed {
		if r.ID == held {
			t.Errorf("the claim returned row %d, which another transaction holds: two relays would publish it", held)
		}
	}
	t.Logf("the claim stepped over the held row %d and returned %d other row(s) in %v", held, len(claimed), took)
}
