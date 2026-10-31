package natsunified_test

import (
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/natsunified"
)

// profileDriversBefore reports the versions of the two profile drivers this
// test binary was built with, and whether either is a release older than
// want.
//
// CI also builds this module with GOWORK=off against the driver releases its
// go.mod requires, so a test of behaviour the drivers gained after those
// releases skips there until the release train moves the requirements on. A
// workspace build uses the drivers in the tree, which the build information
// reports as (devel), and so does a replacement by a directory; neither is
// older than anything. drivers/kafka has the same helper for the root module.
func profileDriversBefore(t testing.TB, want string) (string, bool) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("this test binary carries no build information")
	}
	var seen []string
	older := false
	for _, dep := range info.Deps {
		if dep.Path != "github.com/iqhive/iqmq/drivers/nats" && dep.Path != "github.com/iqhive/iqmq/drivers/jetstream" {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		seen = append(seen, dep.Path+" "+dep.Version)
		have, okHave := releaseNumbers(dep.Version)
		limit, okLimit := releaseNumbers(want)
		older = older || (okHave && okLimit && slices.Compare(have, limit) < 0)
	}
	if len(seen) != 2 {
		t.Fatalf("this test binary's build information lists %v, not both profile drivers", seen)
	}
	return strings.Join(seen, ", "), older
}

// releaseNumbers is the major, minor and patch numbers of a module version
// such as v1.1.1 or v1.1.2-0.20261030010203-0123456789ab, and false for
// anything else, (devel) and the empty version of a directory among them.
func releaseNumbers(version string) ([]int, bool) {
	core, ok := strings.CutPrefix(version, "v")
	if !ok {
		return nil, false
	}
	if i := strings.IndexAny(core, "-+"); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return nil, false
	}
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

// sharedConnection is one borrowed connection serving clients of both
// profiles, whose own reconnect handler reports how deep it ran.
type sharedConnection struct {
	t        *testing.T
	nc       *natsgo.Conn
	depth    chan int
	core, js natsunified.Config
}

// newSharedConnection dials the JetStream server, skipping when the profile
// drivers predate the shared handlers.
func newSharedConnection(t *testing.T) *sharedConnection {
	t.Helper()
	if have, older := profileDriversBefore(t, "v1.2.0"); older {
		t.Skipf("built with %s; the drivers share their handlers on a connection from v1.2.0", have)
	}
	s := &sharedConnection{
		t:     t,
		depth: make(chan int, 1),
		core:  natsunified.Config{Profile: natsunified.ProfileCore, Core: natsCoreConfig(jsURL(t))},
		js:    jsConfig(t),
	}
	nc, err := natsgo.Connect(jsURL(t), natsgo.ReconnectWait(10*time.Millisecond),
		natsgo.ReconnectHandler(func(*natsgo.Conn) {
			pcs := make([]uintptr, 1<<16)
			select {
			case s.depth <- runtime.Callers(0, pcs):
			default:
			}
		}))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	s.nc = nc
	return s
}

// reconnect forces one reconnection and returns how deep the caller's
// reconnect handler ran.
func (s *sharedConnection) reconnect() int {
	s.t.Helper()
	if err := s.nc.ForceReconnect(); err != nil {
		s.t.Fatalf("ForceReconnect: %v", err)
	}
	select {
	case n := <-s.depth:
		return n
	case <-time.After(10 * time.Second):
		s.t.Fatal("the connection never reconnected")
		return 0
	}
}

// open opens a client of cfg's profile on the connection.
func (s *sharedConnection) open(cfg natsunified.Config) *mq.Client {
	s.t.Helper()
	c, err := natsunified.OpenWithClient(s.t.Context(), s.nc, driverkit.ClientBorrowed, cfg)
	if err != nil {
		s.t.Fatalf("OpenWithClient (%v): %v", cfg.Profile, err)
	}
	return c
}

// TestClientsOfBothProfilesDoNotDeepenTheConnectionsHandlers holds that Core
// and JetStream clients opened and closed in turn on one borrowed connection,
// which is how the facade serves both profiles, leave nothing in the chain of
// handlers an event runs through once they have closed. Every backend ever
// opened used to leave its forwarders there, so an event made one nested call
// per backend on nats.go's one callback goroutine, and some millions of
// clients on a long-lived connection overflowed its stack.
func TestClientsOfBothProfilesDoNotDeepenTheConnectionsHandlers(t *testing.T) {
	s := newSharedConnection(t)
	bare := s.reconnect()
	// The first opened is the first closed: the Core handlers stay below the
	// JetStream ones until those are taken off, and then go too.
	for range 200 {
		c, j := s.open(s.core), s.open(s.js)
		_ = c.Close()
		_ = j.Close()
	}
	if got := s.reconnect(); got > bare {
		t.Fatalf("an event ran %d frames deep before any client was opened and %d after 400 clients of both profiles were opened and closed, the first opened first, want no deeper",
			bare, got)
	}
	// The last opened is the first closed: each set of handlers is taken off
	// as its last client closes.
	for range 200 {
		c, j := s.open(s.core), s.open(s.js)
		_ = j.Close()
		_ = c.Close()
	}
	if got := s.reconnect(); got > bare {
		t.Fatalf("an event ran %d frames deep before any client was opened and %d after every client had closed, want no deeper",
			bare, got)
	}
}

// TestReplacingClientsOfBothProfilesDoesNotDeepenTheConnectionsHandlers keeps
// one Core client and one JetStream client open on one borrowed connection and
// replaces each in turn, as a client per tenant or per job does. A replacement
// found the other driver's handlers in front and put a new set of its own in
// front of them, and the set it had served stayed behind them for good, so
// 300 rounds took an event from 7 frames deep to 607, and 606 after both had
// closed.
func TestReplacingClientsOfBothProfilesDoesNotDeepenTheConnectionsHandlers(t *testing.T) {
	s := newSharedConnection(t)
	bare := s.reconnect()
	j, c := s.open(s.js), s.open(s.core)
	start := s.reconnect()
	for range 300 {
		_ = j.Close()
		j = s.open(s.js)
		_ = c.Close()
		c = s.open(s.core)
	}
	if end := s.reconnect(); end > start {
		t.Fatalf("an event ran %d frames deep with one client of each profile open, and %d after 300 replacements of each with one of each still open, want no deeper",
			start, end)
	}
	_ = j.Close()
	_ = c.Close()
	if got := s.reconnect(); got > bare {
		t.Fatalf("an event ran %d frames deep before any client was opened and %d after every client had closed, want no deeper",
			bare, got)
	}
}

// TestClientsOfBothProfilesOpenedAndClosedAtRandomDoNotDeepenTheHandlers opens
// and closes clients of both profiles one at a time in a random order, with up
// to two of each open, and holds that an event never runs through more than
// one set of each driver's handlers, and through none once every client has
// closed. 2,000 such steps took an event from 5 frames deep to 243, and 242
// after every client had closed.
func TestClientsOfBothProfilesOpenedAndClosedAtRandomDoNotDeepenTheHandlers(t *testing.T) {
	s := newSharedConnection(t)
	bare := s.reconnect()
	c := s.open(s.core)
	oneSet := s.reconnect()
	_ = c.Close()
	cfgs := []natsunified.Config{s.core, s.js}
	open := [2][]*mq.Client{}
	rng := rand.New(rand.NewPCG(1, 2))
	deepest := 0
	for step := range 2000 {
		p := rng.IntN(2)
		switch {
		case len(open[p]) == 2 || (len(open[p]) > 0 && rng.IntN(2) == 0):
			i := rng.IntN(len(open[p]))
			_ = open[p][i].Close()
			open[p] = slices.Delete(open[p], i, i+1)
		default:
			open[p] = append(open[p], s.open(cfgs[p]))
		}
		if step%250 == 249 {
			deepest = max(deepest, s.reconnect())
		}
	}
	if limit := bare + 2*(oneSet-bare); deepest > limit {
		t.Fatalf("an event ran %d frames deep with no client open, %d with one set of handlers, and %d at its deepest over 2,000 random opens and closes, want no more than one set of each driver's, %d",
			bare, oneSet, deepest, limit)
	}
	for p := range open {
		for _, c := range open[p] {
			_ = c.Close()
		}
	}
	if got := s.reconnect(); got > bare {
		t.Fatalf("an event ran %d frames deep before any client was opened and %d after every client had closed, want no deeper",
			bare, got)
	}
}
