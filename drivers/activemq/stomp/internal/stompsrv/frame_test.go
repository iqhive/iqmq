package stompsrv

import (
	"strings"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
)

// The Frame accessors, and the injection points a test reaches the server's
// refusals through.
//
// Every one of these is read by a FAILURE message or by a test that has
// already gone wrong, which is why none of them ran: they are the
// instrument's own reporting surface. A `String` that panics on a frame with
// no headers turns a one-line assertion failure into a stack trace with the
// real answer nowhere in it.

// connected dials the server and returns a live STOMP connection.
func connected(t *testing.T, srv *Server) *gostomp.Conn {
	t.Helper()
	c, err := gostomp.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server: %v", err)
	}
	t.Cleanup(func() { _ = c.Disconnect() })
	return c
}

// serve starts a server and closes it when the test ends.
func serve(t *testing.T, o Options) *Server {
	t.Helper()
	srv, err := New(o)
	if err != nil {
		t.Fatalf("starting the server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// TestTheZeroFrameAnswersEveryAccessorSafely.
//
// A Frame with no header is what a caller gets from an index that does not
// exist, and every accessor is called from a failure message -- so one that
// panicked would replace the assertion's own explanation with a stack trace.
func TestTheZeroFrameAnswersEveryAccessorSafely(t *testing.T) {
	var f Frame

	if got := f.Get("receipt"); got != "" {
		t.Errorf("Get on a frame with no headers is %q, want \"\"", got)
	}
	if f.Has("receipt") {
		t.Error("Has on a frame with no headers reported true")
	}
	if got := f.Len(); got != 0 {
		t.Errorf("Len on a frame with no headers is %d, want 0", got)
	}
	if k, v := f.At(0); k != "" || v != "" {
		t.Errorf("At(0) on a frame with no headers is (%q, %q), want two empty strings", k, v)
	}
	if got := f.Headers(); len(got) != 0 {
		t.Errorf("Headers on a frame with no headers is %v, want empty", got)
	}
	if got := f.String(); !strings.Contains(got, "body=0") {
		t.Errorf("String on a frame with no headers is %q; it is what a failure message prints", got)
	}
}

// TestTheAccessorsReadARealFramesHeadersInWireOrder.
//
// `At` is in wire order and `Headers` is first-occurrence-wins, which is
// STOMP 1.2's own rule for a repeated header. The two differ exactly when a
// header repeats, so a test that only used Headers could not tell a server
// that reordered them from one that did not.
func TestTheAccessorsReadARealFramesHeadersInWireOrder(t *testing.T) {
	srv := serve(t, Options{})
	c := connected(t, srv)
	if err := c.Send("/queue/orders", "text/plain", []byte("hello"),
		gostomp.SendOpt.Header("x-first", "1"),
		gostomp.SendOpt.Header("x-first", "2"),
	); err != nil {
		t.Fatalf("sending: %v", err)
	}
	if err := srv.WaitFrames("SEND", 1, testBudget); err != nil {
		t.Fatalf("waiting for the SEND: %v", err)
	}

	f := srv.FramesOf("SEND")[0]
	if f.Len() == 0 {
		t.Fatal("the received SEND carries no headers at all")
	}
	if !f.Has("destination") {
		t.Error("the received SEND has no destination header")
	}
	if got := f.Get("destination"); got != "/queue/orders" {
		t.Errorf("the SEND's destination is %q", got)
	}
	// The first occurrence wins, in both accessors, and the map keeps one.
	if got := f.Get("x-first"); got != "1" {
		t.Errorf("Get returned %q for a repeated header, want the FIRST value \"1\"", got)
	}
	if got := f.Headers()["x-first"]; got != "1" {
		t.Errorf("Headers kept %q for a repeated header, want the first value \"1\"", got)
	}
	seen := 0
	for i := range f.Len() {
		if k, _ := f.At(i); k == "x-first" {
			seen++
		}
	}
	if seen != 2 {
		t.Errorf("At walked %d occurrences of a header sent twice, want 2: Headers collapses duplicates "+
			"and At is what shows they were there", seen)
	}

	if s := f.String(); !strings.Contains(s, "SEND") || !strings.Contains(s, "body=5") {
		t.Errorf("String rendered the frame as %q; it needs the command and the body length", s)
	}
}

// TestTheServerRefusesTheCredentialsItWasConfiguredWith.
//
// Both directions: the right login connects and the wrong one is refused with
// the broker's own sentence, which differs by product and is the one the
// server header names. A server that accepted anything would let the driver's
// credential-failure tests pass against a driver that sent none, and one that
// refused with a sentence no broker sends let them pass against a driver that
// could not read the real one.
func TestTheServerRefusesTheCredentialsItWasConfiguredWith(t *testing.T) {
	for _, tc := range []struct {
		server, wrong, anonymous string
	}{
		{
			"ActiveMQ-Artemis/2.44.0",
			"Security Error occurred: User name [user] or password is invalid",
			"Security Error occurred: User name [null] or password is invalid",
		},
		{
			"ActiveMQ/6.2.0",
			"User name [user] or password is invalid.",
			"User name [null] or password is invalid.",
		},
	} {
		t.Run(tc.server, func(t *testing.T) {
			srv := serve(t, Options{ServerHeader: tc.server, Login: "user", Passcode: "secret"})

			good, err := gostomp.Dial("tcp", srv.Addr(),
				gostomp.ConnOpt.Login("user", "secret"))
			if err != nil {
				t.Fatalf("connecting with the right credentials: %v", err)
			}
			_ = good.Disconnect()

			// The server's own sentence has to reach the client: it is what
			// the driver's error mapper reads, and a refusal that closed the
			// socket without it would be indistinguishable from the broker
			// being down.
			_, err = gostomp.Dial("tcp", srv.Addr(), gostomp.ConnOpt.Login("user", "wrong"))
			if err == nil {
				t.Fatal("connecting with the wrong passcode succeeded")
			}
			if err.Error() != tc.wrong {
				t.Errorf("the refusal is %q, want the broker's own sentence %q: it is what the driver's error "+
					"mapper reads and what tells a credential failure apart from a broker that is down", err, tc.wrong)
			}
			_, err = gostomp.Dial("tcp", srv.Addr())
			if err == nil {
				t.Fatal("connecting with no credentials succeeded against a server that requires them")
			}
			if err.Error() != tc.anonymous {
				t.Errorf("the refusal of a CONNECT with no login is %q, want %q", err, tc.anonymous)
			}
		})
	}
}

// TestRefuseConnectAnswersEveryConnectWithAnError.
func TestRefuseConnectAnswersEveryConnectWithAnError(t *testing.T) {
	srv := serve(t, Options{RefuseConnect: "AMQ229031: Unable to validate user"})

	if _, err := gostomp.Dial("tcp", srv.Addr()); err == nil {
		t.Error("connecting to a server that refuses every CONNECT succeeded")
	}
}

// TestFailSendAnswersTheNextSendWithAnError, and only the next one.
//
// It is one-shot because a test wants ONE publication to fail and the
// recovery after it to work -- a permanent failure would make the second half
// of every such test unreachable.
func TestFailSendAnswersTheNextSendWithAnError(t *testing.T) {
	srv := serve(t, Options{})
	srv.FailSend("/queue/orders", "AMQ229032: the broker refused it")
	c := connected(t, srv)

	// A RECEIPT makes the send synchronous: without one go-stomp writes the
	// frame and returns, so the server's ERROR surfaces later and an
	// assertion here would pass whatever the server did.
	if err := c.Send("/queue/orders", "text/plain", []byte("one"),
		gostomp.SendOpt.Receipt); err == nil {
		t.Error("the send the server was told to fail succeeded")
	}

	second := connected(t, srv)
	if err := second.Send("/queue/orders", "text/plain", []byte("two"),
		gostomp.SendOpt.Receipt); err != nil {
		t.Errorf("the second send is %v; FailSend is one-shot so that a test can assert on the recovery", err)
	}
}

// TestTheServersOwnCountersAnswerForSomethingThatIsNotThere.
//
// They are read by failure messages, so a count of a destination nobody used
// has to be zero rather than a panic.
func TestTheServersOwnCountersAnswerForSomethingThatIsNotThere(t *testing.T) {
	srv := serve(t, Options{})

	if got := srv.Count("SEND"); got != 0 {
		t.Errorf("Count of a command nobody sent is %d, want 0", got)
	}
	if got := srv.Subscribes("/queue/never"); got != 0 {
		t.Errorf("Subscribes of a destination nobody used is %d, want 0", got)
	}
	if got := srv.Live("/queue/never"); got != 0 {
		t.Errorf("Live of a destination nobody used is %d, want 0", got)
	}
	if got := srv.FramesOf("SEND"); len(got) != 0 {
		t.Errorf("FramesOf a command nobody sent is %v, want empty", got)
	}
	if got := srv.SettledIDs("ACK"); len(got) != 0 {
		t.Errorf("SettledIDs of a command nobody sent is %v, want empty", got)
	}
	if got := srv.Commands(1); len(got) != 0 {
		t.Errorf("Commands from the first frame of an idle server is %v, want empty", got)
	}
	if got := srv.Accepted(); got != 0 {
		t.Errorf("Accepted on a server nobody dialled is %d, want 0", got)
	}
}

// TestWaitingForSomethingThatNeverHappensFailsWithinItsBudget.
//
// Every wait in this package is bounded, and the bound is what stops a broken
// test hanging the package for ten minutes instead of failing in five
// seconds with a sentence.
func TestWaitingForSomethingThatNeverHappensFailsWithinItsBudget(t *testing.T) {
	srv := serve(t, Options{})

	start := time.Now()
	err := srv.WaitFrames("SEND", 1, 100*time.Millisecond)
	if err == nil {
		t.Fatal("waiting for a frame nobody sent returned nil")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("the wait took %s against a 100ms budget", took)
	}
	if !strings.Contains(err.Error(), "SEND") {
		t.Errorf("the timeout is %v; it has to name what it was waiting for", err)
	}

	if err := srv.WaitAccepted(1, 100*time.Millisecond); err == nil {
		t.Error("waiting for a connection nobody made returned nil")
	}
}
