package natsunified_test

import (
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

// TestClientsOfBothProfilesDoNotDeepenTheConnectionsHandlers holds that Core
// and JetStream clients opened and closed in turn on one borrowed connection,
// which is how the facade serves both profiles, leave no more in the chain of
// handlers an event runs through than one set of a driver's own. Every
// backend ever opened used to leave its forwarders there, so an event made
// one nested call per backend on nats.go's one callback goroutine, and some
// millions of clients on a long-lived connection overflowed its stack.
func TestClientsOfBothProfilesDoNotDeepenTheConnectionsHandlers(t *testing.T) {
	if have, older := profileDriversBefore(t, "v1.2.0"); older {
		t.Skipf("built with %s; the drivers share their handlers on a connection from v1.2.0", have)
	}
	depth := make(chan int, 1)
	nc, err := natsgo.Connect(jsURL(t), natsgo.ReconnectWait(10*time.Millisecond),
		natsgo.ReconnectHandler(func(*natsgo.Conn) {
			pcs := make([]uintptr, 1<<16)
			select {
			case depth <- runtime.Callers(0, pcs):
			default:
			}
		}))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	reconnect := func() int {
		t.Helper()
		if err := nc.ForceReconnect(); err != nil {
			t.Fatalf("ForceReconnect: %v", err)
		}
		select {
		case n := <-depth:
			return n
		case <-time.After(10 * time.Second):
			t.Fatal("the connection never reconnected")
			return 0
		}
	}
	core := natsunified.Config{Profile: natsunified.ProfileCore, Core: natsCoreConfig(jsURL(t))}
	js := jsConfig(t)
	open := func(cfg natsunified.Config) *mq.Client {
		t.Helper()
		c, err := natsunified.OpenWithClient(t.Context(), nc, driverkit.ClientBorrowed, cfg)
		if err != nil {
			t.Fatalf("OpenWithClient (%v): %v", cfg.Profile, err)
		}
		return c
	}

	bare := reconnect()
	c := open(core)
	oneSet := reconnect()
	_ = c.Close()
	// The first opened is the first closed: the Core handlers are left in
	// place below the JetStream ones and serve the next Core client.
	for range 200 {
		c, j := open(core), open(js)
		_ = c.Close()
		_ = j.Close()
	}
	if got := reconnect(); got > oneSet {
		t.Fatalf("an event ran %d frames deep with one Core client open and %d after 400 clients of both profiles were opened and closed in turn, want no deeper",
			oneSet, got)
	}
	// The last opened is the first closed: each set of handlers is taken off
	// as its last client closes.
	for range 200 {
		c, j := open(core), open(js)
		_ = j.Close()
		_ = c.Close()
	}
	if got := reconnect(); got > bare {
		t.Fatalf("an event ran %d frames deep before any client was opened and %d after every client had closed, want no deeper",
			bare, got)
	}
}
