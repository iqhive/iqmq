package main

import (
	"context"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/memory"
)

// TestInspectAsksTheRegistryItWasGiven pins which of the two tables decides
// what this process can open.
//
// `inspect` resolved a scheme through the BUILD-TIME driver table and refused
// anything missing from it with "no driver in this binary claims this
// scheme" — while the open at the end of the same function has always gone
// through `cliEnv.registry`, which a caller or a test may have given its own
// drivers. So `doctor` refused a target the very next statement would have
// opened, and the refusal listed the schemes of a table nobody had asked
// about.
//
// The table is still asked first, because it is what carries the typed
// `describe` and `resolved` steps; it is no longer the authority on what can
// be opened.
func TestInspectAsksTheRegistryItWasGiven(t *testing.T) {
	const scheme = "registryonly"

	reg := dsn.NewRegistry()
	if err := reg.Register(scheme, func(ctx context.Context, target string) (*mq.Client, error) {
		return memory.Open(ctx, memory.Config{AutoCreate: true})
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, inTable := entryForScheme(scheme); inTable {
		t.Fatalf("%q is in the build-time driver table, so this test cannot see its subject", scheme)
	}

	e := &cliEnv{ctx: t.Context(), registry: reg}
	rep, code := e.inspect(scheme+"://somewhere", true, time.Second)

	if rep.ConfigError != "" {
		t.Fatalf("inspect refused a scheme the registry holds: %q (exit %d). The open at the end of "+
			"inspect goes through this same registry", rep.ConfigError, code)
	}
	if code != exitOK {
		t.Errorf("inspect returned exit %d for a scheme the registry holds, want %d", code, exitOK)
	}
	if rep.Driver == "" {
		t.Errorf("the report names no driver; the registry knows one for %q", scheme)
	}

	// And a scheme NEITHER knows is still refused, with the registry's own
	// schemes named rather than the build table's.
	rep, code = e.inspect("nobodyclaimsthis://x", true, time.Second)
	if code != exitUsage || rep.ConfigError == "" {
		t.Fatalf("an unknown scheme was accepted: exit %d, error %q", code, rep.ConfigError)
	}
	if !strings.Contains(rep.ConfigError, scheme) {
		t.Errorf("the refusal does not list the registry's own schemes (it says %q, and the registry "+
			"holds %q)", rep.ConfigError, scheme)
	}
}
