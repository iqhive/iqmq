package respsrv_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// serve starts a server over an in-memory Redis and returns a raw client.
func serve(t *testing.T, cmder streams.Commander) (*respsrv.Server, *rawClient) {
	t.Helper()
	srv, err := respsrv.Serve(cmder, respsrv.Options{Version: "7.4.2"})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	t.Cleanup(func() {
		if cerr := srv.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})
	c := dial(t, srv.Addr())
	return srv, c
}

// rawClient is a RESP client written against the protocol document rather
// than against a client library.
//
// It is the only client in this repository that is not one of the two under
// test, and that is the point: a server tested only through the libraries it
// exists to serve is tested against their reading of the protocol, and the
// one reply shape neither of them asks for is the one where a mistake hides.
type rawClient struct {
	t  *testing.T
	c  net.Conn
	br *bufio.Reader
}

func dial(t *testing.T, addr string) *rawClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	// A deadline, so that a server which answers nothing at all fails the
	// test in two seconds rather than in `go test`'s ten minutes. Every
	// refusal below is "the connection died or said nothing", and without a
	// deadline the second half of that is a hang.
	if derr := conn.SetDeadline(time.Now().Add(5 * time.Second)); derr != nil {
		t.Fatalf("SetDeadline: %v", derr)
	}
	return &rawClient{t: t, c: conn, br: bufio.NewReader(conn)}
}

// send writes one command in the array-of-bulk-strings form.
func (r *rawClient) send(tokens ...string) {
	r.t.Helper()
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(tokens)) + "\r\n")
	for _, tok := range tokens {
		b.WriteString("$" + strconv.Itoa(len(tok)) + "\r\n" + tok + "\r\n")
	}
	if _, err := io.WriteString(r.c, b.String()); err != nil {
		r.t.Fatalf("write: %v", err)
	}
}

// raw writes bytes verbatim, for the malformed-request cases.
func (r *rawClient) raw(s string) {
	r.t.Helper()
	if _, err := io.WriteString(r.c, s); err != nil {
		r.t.Fatalf("write: %v", err)
	}
}

// reply reads one reply and renders it as a comparable string: "+OK",
// "-ERR x", ":3", "$5:hello", "_", "#t", "*2[...]", "%1[...]".
func (r *rawClient) reply() string {
	r.t.Helper()
	s, err := readReply(r.br)
	if err != nil {
		r.t.Fatalf("read reply: %v", err)
	}
	return s
}

func readReply(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return "", errors.New("empty line")
	}
	switch line[0] {
	case '+', '-', ':', ',', '#', '_':
		return line, nil
	case '$':
		n, cerr := strconv.Atoi(line[1:])
		if cerr != nil {
			return "", cerr
		}
		if n < 0 {
			return "$-1", nil
		}
		buf := make([]byte, n+2)
		if _, rerr := io.ReadFull(br, buf); rerr != nil {
			return "", rerr
		}
		return "$" + strconv.Itoa(n) + ":" + string(buf[:n]), nil
	case '*', '%':
		n, cerr := strconv.Atoi(line[1:])
		if cerr != nil {
			return "", cerr
		}
		count := n
		if line[0] == '%' {
			count = 2 * n
		}
		parts := make([]string, 0, count)
		for range count {
			p, perr := readReply(br)
			if perr != nil {
				return "", perr
			}
			parts = append(parts, p)
		}
		return string(line[0]) + strconv.Itoa(n) + "[" + strings.Join(parts, " ") + "]", nil
	}
	return "", errors.New("unknown reply type " + line[:1])
}

func TestSetupCommandsAreAnsweredByTheServer(t *testing.T) {
	_, c := serve(t, fakestreams.New(fakestreams.Options{}))
	c.send("HELLO", "3")
	got := c.reply()
	for _, want := range []string{"%7[", "$6:server", "$5:redis", "$5:7.4.2", "$5:proto", ":3"} {
		if !strings.Contains(got, want) {
			t.Errorf("HELLO 3 reply %q does not contain %q", got, want)
		}
	}
	for _, tc := range []struct {
		tokens []string
		want   string
	}{
		{[]string{"AUTH", "u", "p"}, "+OK"},
		{[]string{"SELECT", "0"}, "+OK"},
		{[]string{"CLIENT", "SETINFO", "LIB-NAME", "go-redis"}, "+OK"},
		{[]string{"CLIENT", "ID"}, ":1"},
		{[]string{"CLIENT", "GETNAME"}, "_"},
		{[]string{"COMMAND", "DOCS"}, "*0[]"},
		{[]string{"RESET"}, "+RESET"},
	} {
		c.send(tc.tokens...)
		if got := c.reply(); got != tc.want {
			t.Errorf("%v: got %q, want %q", tc.tokens, got, tc.want)
		}
	}
}

func TestHelloTwoRendersTheReplyInRESP2(t *testing.T) {
	_, c := serve(t, fakestreams.New(fakestreams.Options{}))
	c.send("HELLO", "2")
	got := c.reply()
	// The reply to HELLO 2 is read by a RESP2 reader, so it must be a flat
	// array of fourteen elements and not a map of seven pairs.
	if !strings.HasPrefix(got, "*14[") {
		t.Errorf("HELLO 2 reply is %q, want a flat 14-element array", got)
	}
	// And every reply after it is RESP2: no map, no typed null, no boolean.
	c.send("XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "nosuch", ">")
	if got := c.reply(); !strings.HasPrefix(got, "-NOGROUP") {
		t.Errorf("XREADGROUP on a missing group: got %q, want a NOGROUP error", got)
	}
}

func TestHelloRefusesAVersionItDoesNotSpeak(t *testing.T) {
	_, c := serve(t, fakestreams.New(fakestreams.Options{}))
	c.send("HELLO", "4")
	if got := c.reply(); !strings.HasPrefix(got, "-NOPROTO") {
		t.Errorf("HELLO 4: got %q, want a NOPROTO error", got)
	}
}

func TestRepliesCarryEveryValueKind(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	_, c := serve(t, srv)
	c.send("HELLO", "3")
	_ = c.reply()
	for _, tc := range []struct {
		name   string
		tokens []string
		want   string
	}{
		{"string", []string{"PING"}, "$4:PONG"},
		{"integer", []string{"XLEN", "nosuch"}, ":0"},
		{"null", []string{"XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "BLOCK", "1", "STREAMS", "s", ">"}, ""},
		{"array", []string{"CONFIG", "GET", "appendonly"}, "*2[$10:appendonly $3:yes]"},
		{"error", []string{"XACK"}, "-ERR wrong number of arguments"},
	} {
		c.send(tc.tokens...)
		got := c.reply()
		if tc.want == "" {
			continue
		}
		if got != tc.want {
			t.Errorf("%s: %v -> %q, want %q", tc.name, tc.tokens, got, tc.want)
		}
	}
}

func TestABulkStringIsBinarySafe(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{})
	_, c := serve(t, srv)
	// A body carrying the protocol's own terminator and a NUL. A server that
	// wrote a reply as a simple string, or that trimmed it, would corrupt
	// exactly the payload the specification promises to return byte for byte.
	body := "a\r\nb\x00c"
	c.send("XADD", "s", "*", "b", body)
	id := c.reply()
	if !strings.HasPrefix(id, "$") {
		t.Fatalf("XADD -> %q, want a bulk string", id)
	}
	c.send("XRANGE", "s", "-", "+")
	got := c.reply()
	if !strings.Contains(got, "$"+strconv.Itoa(len(body))+":"+body) {
		t.Errorf("XRANGE round trip lost the body: %q", got)
	}
}

// errCRLF is a Commander whose failure message carries the protocol's own
// terminator, which is the reply-splitting case.
type errCRLF struct{ streams.Commander }

func (errCRLF) Do(context.Context, streams.Cmd) (streams.Value, error) {
	return streams.Nil(), errors.New("WEIRD one\r\n+INJECTED\r\ntwo")
}

func (errCRLF) DoMulti(context.Context, []streams.Cmd) ([]streams.Result, error) { return nil, nil }

func (errCRLF) Close() error { return nil }

func TestAnErrorReplyCannotSplitIntoTwoReplies(t *testing.T) {
	_, c := serve(t, errCRLF{})
	c.send("XLEN", "s")
	got := c.reply()
	if strings.Contains(got, "INJECTED") && !strings.HasPrefix(got, "-WEIRD") {
		t.Fatalf("the error reply split: %q", got)
	}
	if got != "-WEIRD one  +INJECTED  two" {
		t.Errorf("error reply %q, want the CR and LF replaced", got)
	}
	// The connection must still be synchronised: the next command's reply is
	// the next thing on the wire, not the tail of the last one.
	c.send("PING")
	if next := c.reply(); next != "-WEIRD one  +INJECTED  two" {
		t.Errorf("the connection desynchronised: next reply %q", next)
	}
}

// errPlain is a Commander whose failure is an ordinary Go error: no condition
// word, because it never came from a server at all.
type errPlain struct{ streams.Commander }

func (errPlain) Do(context.Context, streams.Cmd) (streams.Value, error) {
	return streams.Nil(), errors.New("context canceled")
}

func (errPlain) DoMulti(context.Context, []streams.Cmd) ([]streams.Result, error) { return nil, nil }

func (errPlain) Close() error { return nil }

func TestAnErrorWithNoConditionWordIsGivenTheGenericOne(t *testing.T) {
	_, c := serve(t, errPlain{})
	c.send("XLEN", "s")
	// The protocol requires an error reply to carry a condition word, and
	// every driver's error table matches on one. A reply of "-context
	// canceled" makes "context" the condition, which no client recognises
	// and no rule matches.
	if got := c.reply(); got != "-ERR context canceled" {
		t.Errorf("error reply %q, want %q", got, "-ERR context canceled")
	}
}

func TestAServerShapedErrorKeepsItsOwnConditionWord(t *testing.T) {
	_, c := serve(t, fakestreams.New(fakestreams.Options{}))
	// NOGROUP is what drivers/redis's error table matches on, so a server
	// that rewrote it would make every NOGROUP rule in the repository dead.
	c.send("XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "nosuch", ">")
	if got := c.reply(); !strings.HasPrefix(got, "-NOGROUP ") {
		t.Errorf("XREADGROUP on a missing group: got %q, want a NOGROUP error", got)
	}
}

func TestAPipelineGetsOneReplyPerCommandInOrder(t *testing.T) {
	_, c := serve(t, fakestreams.New(fakestreams.Options{}))
	var b strings.Builder
	for i := range 5 {
		key := "s" + strconv.Itoa(i)
		b.WriteString("*2\r\n$4\r\nXLEN\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n")
	}
	c.raw(b.String())
	for i := range 5 {
		if got := c.reply(); got != ":0" {
			t.Errorf("pipelined reply %d: got %q, want :0", i, got)
		}
	}
}

func TestAMalformedRequestIsRefusedRatherThanGuessedAt(t *testing.T) {
	for _, tc := range []struct{ name, req string }{
		{"inline", "PING\r\n"},
		{"not-an-array", "$4\r\nPING\r\n"},
		{"negative-count", "*-1\r\n"},
		{"token-is-not-bulk", "*1\r\n:3\r\n"},
		{"bulk-without-crlf", "*1\r\n$4\r\nPINGxx"},
		{"bulk-length-not-a-number", "*1\r\n$x\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, c := serve(t, fakestreams.New(fakestreams.Options{}))
			c.raw(tc.req)
			// The server drops the connection rather than answering, which is
			// the only safe response to a request it cannot frame: a reply
			// written into a stream whose framing is already lost cannot be
			// read.
			if _, err := readReply(c.br); err == nil {
				t.Fatalf("%q was answered rather than refused", tc.req)
			}
			if len(srv.Errors()) == 0 {
				t.Errorf("%q produced no protocol error on the server", tc.req)
			}
		})
	}
}

func TestATokenCountAboveTheBoundIsRefusedWithoutAllocatingForIt(t *testing.T) {
	srv, err := respsrv.Serve(fakestreams.New(fakestreams.Options{}), respsrv.Options{MaxTokens: 4, MaxBulk: 8})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer func() { _ = srv.Close() }()
	// Both halves assert the SERVER recorded a protocol error, not merely
	// that the client got no reply. Without that distinction the check
	// cannot fail: a server that ignored the bound and waited for five
	// tokens that never came would also send no reply, so the test would
	// pass on the very defect it exists to find (and did, for one run: it
	// took the client's read deadline and reported success).
	c := dial(t, srv.Addr())
	c.raw("*5\r\n")
	if _, rerr := readReply(c.br); rerr == nil {
		t.Fatal("a request above MaxTokens was answered")
	}
	c2 := dial(t, srv.Addr())
	c2.raw("*1\r\n$9\r\n")
	if _, rerr := readReply(c2.br); rerr == nil {
		t.Fatal("a bulk string above MaxBulk was answered")
	}
	errs := srv.Errors()
	if len(errs) != 2 {
		t.Fatalf("the server recorded %d protocol errors, want 2: %v", len(errs), errs)
	}
	for _, err := range errs {
		var pe *respsrv.ProtocolError
		if !errors.As(err, &pe) {
			t.Errorf("recorded %T (%v), want a *respsrv.ProtocolError", err, err)
		}
	}
}

func TestCloseDropsOpenConnectionsAndStopsAccepting(t *testing.T) {
	srv, err := respsrv.Serve(fakestreams.New(fakestreams.Options{}), respsrv.Options{})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	c := dial(t, srv.Addr())
	c.send("PING")
	if got := c.reply(); got != "$4:PONG" {
		t.Fatalf("PING -> %q", got)
	}
	// Close is bounded, and the bound is the check. It waits for every
	// connection handler, and a handler parked in a read that nothing
	// interrupts never returns -- so a Close that did not drop the open
	// connections would hang here rather than fail, and `go test`'s
	// ten-minute timeout would be the only thing that noticed.
	closeWithin(t, srv, 5*time.Second)
	closeWithin(t, srv, 5*time.Second)
	if _, derr := net.Dial("tcp", srv.Addr()); derr == nil {
		t.Error("the listener still accepts after Close")
	}
	if _, rerr := readReply(c.br); rerr == nil {
		t.Error("an open connection survived Close")
	}
}

// closeWithin fails unless Close returns inside d.
func closeWithin(t *testing.T, srv *respsrv.Server, d time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(d):
		t.Fatalf("Close did not return within %v", d)
	}
}

func TestServeRefusesANilCommander(t *testing.T) {
	if _, err := respsrv.Serve(nil, respsrv.Options{}); err == nil {
		t.Fatal("Serve(nil) returned a server")
	}
}
