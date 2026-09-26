package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/codec/jsonv2"
)

// TestRunRoundTripsAndDeadLetters runs the example and reads what it printed.
//
// It is one test over both halves on purpose: the decode-failure path is only
// interesting after the happy path has proved that the codec, the content type
// and the typed publisher agree, and running them in one process is also what
// proves the dead-letter route does not disturb the consumer that is still
// working.
func TestRunRoundTripsAndDeadLetters(t *testing.T) {
	if !jsonv2.Available() {
		// The one legitimate skip in this module. A toolchain without
		// encoding/json/v2 cannot run this example at all, and the check
		// below proves the program says so rather than misbehaving.
		var out bytes.Buffer
		err := run(t.Context(), &out)
		if !errors.Is(err, codec.ErrUnavailable) {
			t.Fatalf("on a toolchain without encoding/json/v2 the example reported %v, want codec.ErrUnavailable", err)
		}
		t.Skip("this toolchain has no encoding/json/v2 (Go 1.27 and later); the example reported it correctly")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	const want = "order o-1: 3 item(s), 4995 cents\n" +
		"dead-lettered o-2 from orders after 1 attempt(s), with a recorded reason\n"
	if got := out.String(); got != want {
		t.Errorf("the example printed\n%q\nwant\n%q", got, want)
	}
}

// TestPoisonBodyDoesNotDecode proves the second half of the example is about
// what it says it is about.
//
// The dead-letter assertion above would pass just as well if the "poison" body
// were rejected for some other reason — a content-type mismatch, say — or if it
// decoded and the handler then failed. This is the check that says the body
// itself is undecodable, and it is the one a well-meaning edit to poisonBody
// would break.
func TestPoisonBodyDoesNotDecode(t *testing.T) {
	if !jsonv2.Available() {
		t.Skip("this toolchain has no encoding/json/v2")
	}
	c, err := jsonv2.Typed[Order]()
	if err != nil {
		t.Fatal(err)
	}
	if got := c.ContentType(); got != codec.ContentTypeJSON {
		t.Errorf("the v2 codec names content type %q, want %q", got, codec.ContentTypeJSON)
	}
	if v, err := c.Decode(poisonBody); err == nil {
		t.Errorf("the poison body decoded into %+v; the dead-letter half of the example would then be measuring something else", v)
	}
}
