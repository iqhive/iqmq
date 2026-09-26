package global_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/dsn/global"
)

// fuzzScheme is a scheme this file alone registers, so that the fuzz target can
// exercise dispatch without depending on which other schemes a test registered.
const fuzzScheme = "fuzzdrv"

var (
	registerOnce sync.Once
	lastDSN      string
	lastMu       sync.Mutex
)

// errFuzzOpener is what the fuzz driver's opener always reports: the target is
// about the registry's dispatch, not about opening anything.
var errFuzzOpener = errors.New("global_test: the fuzz driver opens nothing")

// registerFuzzDriver registers the fuzz driver exactly once.
func registerFuzzDriver(t *testing.T) {
	t.Helper()
	registerOnce.Do(func() {
		err := global.Register(fuzzScheme, func(_ context.Context, s string) (*mq.Client, error) {
			lastMu.Lock()
			lastDSN = s
			lastMu.Unlock()
			return nil, errFuzzOpener
		})
		if err != nil {
			t.Fatalf("registering the fuzz driver: %v", err)
		}
	})
}

// FuzzGlobalOpen drives the process-wide registry with arbitrary DSNs.
//
// The registry is where a connection string meets the driver that claims it,
// and the specification makes every parser on that path pure, bounded and non-panicking —
// the bytes come from an environment variable a stranger may have set. The
// invariants:
//
//   - Open never panics, whatever the input.
//   - A DSN whose scheme nobody registered reports ErrDriverNotRegistered, and a
//     string that is not a DSN at all reports the parse failure instead. The two
//     must stay distinguishable: "kafak://b1" is a driver nobody registered and
//     "kafka" is not a connection string, and a caller debugging a deployment
//     needs to be told which.
//   - A DSN that does reach a driver reaches it WHOLE. The registry reads the
//     scheme and hands the driver the original string, because only the driver
//     knows what its own parameters mean.
func FuzzGlobalOpen(f *testing.F) {
	for _, s := range []string{
		"",
		"fuzzdrv://",
		"fuzzdrv://host:1234/path?a=1&b=2",
		"fuzzdrv://u:p@h",
		"FUZZDRV://H",
		"memory://?autocreate=true",
		"kafka://b1:9092",
		"not-a-dsn",
		"://",
		"fuzzdrv://h#frag",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		registerFuzzDriver(t)
		ctx := context.Background()
		scheme, serr := dsn.Scheme(s)
		client, err := global.Open(ctx, s)
		if client != nil {
			_ = client.Close()
		}
		switch {
		case serr != nil:
			// Not a connection string: the failure is the parser's, and it
			// is not ErrDriverNotRegistered.
			if err == nil {
				t.Fatalf("Open(%q) succeeded for a string Scheme refuses", s)
			}
			if errors.Is(err, mq.ErrDriverNotRegistered) {
				t.Fatalf("Open(%q) reported an unregistered driver for a string that is not a DSN", s)
			}
			if mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("Open(%q) reported code %v, want CodeInvalidConfig", s, mq.CodeOf(err))
			}
		case scheme == fuzzScheme:
			if !errors.Is(err, errFuzzOpener) {
				t.Fatalf("Open(%q) did not reach the registered driver: %v", s, err)
			}
			lastMu.Lock()
			got := lastDSN
			lastMu.Unlock()
			if got != s {
				t.Fatalf("the driver was handed %q, want the whole DSN %q", got, s)
			}
		default:
			// Some other scheme. It is either registered by another test in
			// this package or it is not; what must never happen is a
			// success that reaches no driver.
			if err == nil && client == nil {
				t.Fatalf("Open(%q) reported success and produced no client", s)
			}
			if err != nil && !errors.Is(err, mq.ErrDriverNotRegistered) &&
				mq.CodeOf(err) == mq.CodeUnknown && !strings.Contains(err.Error(), "iqmq") {
				t.Fatalf("Open(%q) failed with an unclassified error: %v", s, err)
			}
		}
	})
}
