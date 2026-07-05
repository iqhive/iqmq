package main

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestRunPrintsTheOrder runs the quick start and reads what it printed.
//
// A quick start that compiles and deadlocks is the documentation defect this
// project would notice first, so the example is not merely built by
// `make examples`: this test runs it, with a deadline, and compares the output
// byte for byte.
func TestRunPrintsTheOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var out bytes.Buffer
	if err := run(ctx, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	const want = "order cust-42: {\"id\":1}\n"
	if got := out.String(); got != want {
		t.Errorf("the quick start printed %q, want %q", got, want)
	}
}
