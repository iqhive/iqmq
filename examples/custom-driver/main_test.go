package main

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestRunUsesTheToyDriverThroughThePortableAPI runs the program and reads what
// it printed.
//
// The output is checked line for line because every line is a claim about the
// portable API over a third-party driver: the capability table answers, fan-out
// reaches two subscriptions, a nack comes back with its attempt count
// incremented and marked as a redelivery, a Reject lands in the dead-letter
// topic, and a start position the driver does not have is refused rather than
// silently ignored.
func TestRunUsesTheToyDriverThroughThePortableAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	const want = "driver toy, at-least-once=true, replay=false\n" +
		"billing saw o-1 on attempt 1\n" +
		"billing saw o-1 again on attempt 2 (redelivered=true)\n" +
		"analytics saw o-1 on attempt 1\n" +
		"audit found o-2 in the dead-letter topic\n" +
		"a replaying consumer is refused: this broker retains nothing\n"
	if got := out.String(); got != want {
		t.Errorf("the example printed\n%s\nwant\n%s", got, want)
	}
}
