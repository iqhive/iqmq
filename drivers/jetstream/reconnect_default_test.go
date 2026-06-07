package jetstream

import (
	"strings"
	"testing"
)

// TestUnsetMaxReconnectsMeansForEver pins the default that decides whether a
// client survives a network outage longer than a minute.
//
// nats.go's own default is 60 attempts and then a PERMANENT close: the
// connection is gone, every operation on it fails, and nothing in this driver
// revives it. `make chaos DRIVER=jetstream`'s reset-mid-burst scenario is that
// default's consequence — a reset storm outlasts 60 attempts in about fifteen
// seconds at the default ReconnectWait, and the scenario then reported
// `resumed=false` with `codes=[Closed ...]` for about sixty probes published
// after the toxic had been REMOVED and the network was healthy.
//
// driverkit.Reconnector, which every driver that does its own dial loop uses,
// documents the opposite default in its own MaxAttempts: "Zero means it
// retries until the context is done, which is what a long-lived client wants".
// This test is what keeps the two agreeing.
func TestUnsetMaxReconnectsMeansForEver(t *testing.T) {
	got := Config{}.resolve()
	if got.MaxReconnects >= 0 {
		t.Fatalf("an unset MaxReconnects resolved to %d; want a negative value, which is the client "+
			"library's spelling of \"for ever\". A positive budget closes the connection permanently "+
			"once it is spent, and nothing here reopens it", got.MaxReconnects)
	}

	// A caller who asks for a budget still gets one: the change is to the
	// default, not to the knob.
	if got := (Config{MaxReconnects: 7}).resolve(); got.MaxReconnects != 7 {
		t.Errorf("an explicit MaxReconnects of 7 resolved to %d", got.MaxReconnects)
	}
	if got := (Config{MaxReconnects: -1}).resolve(); got.MaxReconnects != -1 {
		t.Errorf("an explicit -1 resolved to %d", got.MaxReconnects)
	}

	// The resolved value is what the rendered DSN says, so a round trip
	// cannot disagree with the connection about what it will do.
	rendered := Config{}.resolve().String()
	if !strings.Contains(rendered, "maxreconnects=-1") {
		t.Errorf("the resolved config renders as %q, which does not state the reconnection budget; a "+
			"reader of a DSN cannot tell whether this connection gives up", rendered)
	}
}
