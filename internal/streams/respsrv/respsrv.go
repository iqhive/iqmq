// Package respsrv is a RESP front-end over a [streams.Commander]: a real TCP
// server, speaking the real protocol, whose command semantics are whatever
// Commander it was given.
//
// It exists because of the one thing a [streams.Commander] cannot be tested
// through. Everything above the seam — the key naming, the field layout, the
// commands, the reclaim accounting — is held to the same tests for both Redis
// drivers by driving the driver over
// [github.com/iqhive/iqmq/internal/streams/fakestreams], which is a Commander
// and needs no network at all. What that cannot reach is the ADAPTER itself:
// how a [streams.Cmd] becomes the client library's own command, and how the
// client library's own reply becomes a [streams.Value]. A client library only
// produces a reply if something spoke RESP to it.
//
// So this is that something. `respsrv.Serve(fakestreams.New(...))` is a Redis
// a client library will talk to, on loopback, in the test process, with the
// fake's settable clock and inspectable pending list still underneath — which
// is what lets both adapters be held to one another's answers, and what lets
// The specification (the Redis client library)
// be measured without a container in the loop.
//
// What it is NOT:
//
//   - a Redis. It answers the connection-setup commands a client library sends
//     before it will do anything (HELLO, AUTH, SELECT, CLIENT …) and delegates
//     everything else. The command semantics are the Commander's, so against
//     `fakestreams` it is exactly as complete as `fakestreams` is.
//   - a complete RESP implementation. It reads the one request shape a client
//     library sends — an array of bulk strings — and refuses anything else
//     rather than guessing. It writes the reply types a stream command
//     produces, in RESP2 or RESP3 as the connection's HELLO chose.
//   - a performance model of a server. The Commander underneath is a map
//     under a mutex; what a benchmark over this measures is the CLIENT.
package respsrv

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/iqhive/iqmq/internal/streams"
)

// Options configure a [Server].
type Options struct {
	// Version is what HELLO reports. It does not change what INFO reports:
	// that is the Commander's answer, and a server whose two version
	// surfaces disagreed would let a driver read the wrong one and pass.
	Version string
	// Software is what HELLO reports as the server name ("redis", "valkey").
	Software string
	// MaxTokens bounds a request's token count; zero means
	// [DefaultMaxTokens].
	MaxTokens int
	// MaxBulk bounds one bulk string's length in bytes; zero means
	// [DefaultMaxBulk].
	MaxBulk int
}

// The request bounds. They exist because the reader allocates from a length
// the peer sent: a server that believed a four-gigabyte bulk header would
// allocate four gigabytes for a five-byte connection.
const (
	// DefaultMaxTokens is the most tokens one command may carry.
	DefaultMaxTokens = 1 << 20
	// DefaultMaxBulk is the largest bulk string one token may carry.
	DefaultMaxBulk = 64 << 20
)

// resolve fills in the defaults.
func (o Options) resolve() Options {
	if o.Version == "" {
		o.Version = "7.4.2"
	}
	if o.Software == "" {
		o.Software = "redis"
	}
	if o.MaxTokens <= 0 {
		o.MaxTokens = DefaultMaxTokens
	}
	if o.MaxBulk <= 0 {
		o.MaxBulk = DefaultMaxBulk
	}
	return o
}

// HandleConn runs one connection to its end over a transport the caller
// already has, and returns when the peer stops or breaks the protocol.
//
// It exists for the fuzz target. Driving arbitrary bytes at the reader through
// a real TCP dial costs a connection setup per input and puts the operating
// system's socket buffers between the input and the code under test, which is
// two reasons for a fuzz target to be slow and one for it to be
// irreproducible; over a [net.Pipe] the reader sees exactly the bytes the
// corpus holds.
//
// The connection handler below is shared with [Serve], whose own context is
// necessarily rooted at context.Background -- a listening server outlives
// every caller -- and contextcheck sees that root through the shared handler
// and reports this entry point, which does derive its context from its
// argument two lines down.
//
//nolint:contextcheck // see the paragraph above: the shared handler's other caller is a listening server
func HandleConn(ctx context.Context, cmder streams.Commander, opts Options, conn net.Conn) error {
	if cmder == nil {
		return errors.New("respsrv: nil Commander")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// A cancellable child, cancelled when this returns: a command parked in
	// the Commander (an XREADGROUP with BLOCK) has to be released when the
	// connection ends, and the caller's own context may outlive it by a long
	// way.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := &Server{cmder: cmder, opts: opts.resolve()}
	return s.handle(ctx, conn)
}

// Server is a listening RESP front-end.
type Server struct {
	cmder streams.Commander
	opts  Options

	ln net.Listener
	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	errs  []error
}

// Serve starts a server on loopback and returns it. The caller closes it.
//
// The Commander is NOT closed by [Server.Close]: the caller made it and may
// still want to read the pending list out of it after the client has gone.
func Serve(cmder streams.Commander, opts Options) (*Server, error) {
	if cmder == nil {
		return nil, errors.New("respsrv: nil Commander")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cmder: cmder, opts: opts.resolve(), ln: ln,
		ctx: ctx, cancel: cancel,
		conns: map[net.Conn]struct{}{},
	}
	s.wg.Add(1)
	go s.accept()
	return s, nil
}

// Addr is the address to dial, "127.0.0.1:<port>".
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops accepting, drops every open connection and waits for the
// handlers. It is idempotent.
func (s *Server) Close() error {
	s.cancel()
	err := s.ln.Close()
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// Errors returns the connection-handling failures the server saw, for a test
// that wants to assert the client spoke the protocol correctly.
//
// A closed connection is not one of them: a client library closes its pool
// whenever it likes, and reporting that as an error would make every test
// that closes its client fail.
func (s *Server) Errors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()
				_ = c.Close()
			}()
			if err := s.handle(s.ctx, c); err != nil && !isGone(err) {
				s.mu.Lock()
				s.errs = append(s.errs, err)
				s.mu.Unlock()
			}
		}()
	}
}

// isGone reports whether err is the peer having gone away, which is not a
// protocol failure.
func isGone(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled)
}

// handle runs one connection to its end.
//
// The flush rule is what makes pipelining work: a reply is written into the
// buffer and flushed only when nothing more has already arrived, so a client
// that wrote ten commands in one packet gets ten replies in one packet. A
// server that flushed after every reply would turn a pipeline into ten round
// trips and would measure the wrong thing entirely.
func (s *Server) handle(ctx context.Context, conn net.Conn) error {
	br := bufio.NewReaderSize(conn, 64<<10)
	bw := bufio.NewWriterSize(conn, 64<<10)
	c := &session{srv: s, r: br, w: &writer{bw: bw, proto: 2}}
	for {
		tokens, err := c.readCommand()
		if err != nil {
			return err
		}
		done, err := c.dispatch(ctx, tokens)
		if err != nil {
			return err
		}
		if done || br.Buffered() == 0 {
			if ferr := bw.Flush(); ferr != nil {
				return ferr
			}
		}
		if done {
			return nil
		}
	}
}

// session is one connection's state: its protocol version and its buffers.
type session struct {
	srv *Server
	r   *bufio.Reader
	w   *writer
}

// readCommand reads one request.
//
// Only the array-of-bulk-strings form is accepted. The inline form exists in
// the protocol and no client library sends it, so accepting it would be
// untested code on the one path where a mistake is a hang.
func (c *session) readCommand() ([]string, error) {
	line, err := readLine(c.r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, &ProtocolError{Want: "an array-of-bulk-strings request", Got: string(line)}
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 || n > c.srv.opts.MaxTokens {
		return nil, &ProtocolError{Want: "a token count in [0," + strconv.Itoa(c.srv.opts.MaxTokens) + "]", Got: string(line)}
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]string, 0, n)
	for range n {
		head, herr := readLine(c.r)
		if herr != nil {
			return nil, herr
		}
		if len(head) == 0 || head[0] != '$' {
			return nil, &ProtocolError{Want: "a bulk-string header", Got: string(head)}
		}
		size, serr := strconv.Atoi(string(head[1:]))
		if serr != nil || size < 0 || size > c.srv.opts.MaxBulk {
			return nil, &ProtocolError{Want: "a bulk length in [0," + strconv.Itoa(c.srv.opts.MaxBulk) + "]", Got: string(head)}
		}
		buf := make([]byte, size+2)
		if _, rerr := io.ReadFull(c.r, buf); rerr != nil {
			return nil, rerr
		}
		if buf[size] != '\r' || buf[size+1] != '\n' {
			return nil, &ProtocolError{Want: "CRLF after a bulk string", Got: string(buf[size:])}
		}
		out = append(out, string(buf[:size]))
	}
	return out, nil
}

// readLine reads one CRLF-terminated line without its terminator.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, &ProtocolError{Want: "a line shorter than the read buffer", Got: "a longer one"}
		}
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, &ProtocolError{Want: "CRLF", Got: string(line)}
	}
	return line[:len(line)-2], nil
}

// ProtocolError is a request this server will not guess at.
type ProtocolError struct{ Want, Got string }

func (e *ProtocolError) Error() string {
	return "respsrv: protocol error: wanted " + e.Want + ", got " + strconv.Quote(e.Got)
}
