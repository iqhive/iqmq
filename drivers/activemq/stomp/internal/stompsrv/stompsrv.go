// Package stompsrv is a scriptable in-process STOMP 1.2 server for the
// `drivers/activemq/stomp` tests.
//
// # The seam is the SOCKET, and that is a decision rather than a default
//
// The thing under test is this driver over a real `go-stomp` client, so the
// seam belongs under the client library and not above it. Contrast
// `drivers/rabbitmq/internal/rabbitfake`, which stops at its own library seam
// and says why: "A socket-level fake would reproduce the PROTOCOL faithfully
// and then hand the frames to the real library, which would be the honest
// thing to do if the library were the thing under test. It is not; the driver
// is." On amqp091-go that reasoning holds, because the library interposes
// behaviour of its own — cumulative confirms it expands, a channel close that
// nacks what was outstanding, notification channels it may drop from.
//
// go-stomp interposes almost nothing. It is a frame codec (`frame.Reader`,
// `frame.Writer`) plus one process loop that routes a RECEIPT to whoever is
// waiting on its identifier and a MESSAGE to whichever subscription's id it
// names. Every decision this driver has to get right is a decision about
// FRAMES: whether a NACK frame is written at all, whether an ACK reaches the
// broker after a subscription reset, whether the RECEIPT for a dead-letter
// publication arrived before the original was acknowledged. A seam above the
// codec could not observe any of them — it would observe the driver's own
// calls, which is the same thing as asserting that the code does what it
// does. So this is a real TCP listener speaking real STOMP 1.2, and what it
// records is the frames.
//
// The three exceptions go-stomp does interpose are the reason the server has
// to answer more than a broker strictly must, and each one is a hang rather
// than an error if it is missed:
//
//   - an UNSUBSCRIBE is tagged with a `receipt` header BY THE LIBRARY, on its
//     own I/O goroutine, and `Subscription.Unsubscribe` then waits on a
//     condition variable until the RECEIPT arrives. A server that ignored it
//     would make every requeue in these tests take 30 seconds
//     (`DefaultUnsubscribeReceiptTimeout`) and leak the goroutine
//     `waitWithTimeout` starts.
//   - a DISCONNECT carries a receipt too, waited on for
//     `DefaultDisconnectReceiptTimeout`, which the driver does not override.
//     So every `Backend.Close` needs an answer.
//   - the CONNECTED frame's `version` header decides the protocol version the
//     library then speaks, and a V12 ACK names the delivery by the `id`
//     header while a V10 one names it by `subscription` plus `message-id`.
//     A server that answered no version would make the library speak 1.0, in
//     which `Conn.Nack` is refused outright — and the NACK test would pass
//     for entirely the wrong reason.
//
// # What it is not
//
// It is not an ActiveMQ. It does not route, store, expire, schedule, filter
// on a selector, or implement a virtual topic: a publication goes nowhere and
// a delivery is something a test hands over with [Server.Deliver]. That is
// deliberate — a server that modelled the brokers' behaviour would be a
// second opinion about the very facts `internal/activemq/protocol` records as
// measured, and the two would drift. What says those facts are true is the
// env-gated conformance run in `conformance_test.go` and the two reports under
// `conformance/reports/latest/`. So a delivery is a step a test takes
// explicitly ([Server.Deliver], [Server.Forward]) rather than a fan-out this
// server decides.
//
// It is also not a STOMP conformance implementation. It accepts the frames
// this driver sends and records anything else as a protocol violation
// ([Server.Violations]) rather than guessing.
//
// # What it can do that a container cannot
//
//   - answer a SEND frame's `receipt` header with NOTHING, for as many
//     publications as a test names, which is the ambiguous window held
//     open. No broker will do this on request.
//   - answer a SUBSCRIBE with an ERROR frame and then close the socket, which
//     is what Artemis does for a destination that does not exist
//     (`AMQ339001 Destination does not exist <name>`) and which needs a
//     broker with auto-creation disabled and a name nobody created.
//   - hand out a delivery whose `redelivered`, `ack` and `message-id` headers
//     a test chose, so that the attempt can be exercised in both states.
//   - drop the socket with no DISCONNECT, at a moment a test picks.
//   - refuse the CONNECT.
package stompsrv

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
)

// Options is what a server is started with. The zero value accepts every
// CONNECT anonymously and answers every receipt.
type Options struct {
	// ServerHeader is the CONNECTED frame's `server` header. Artemis sends
	// "ActiveMQ-Artemis/2.44.0" and Classic sends "ActiveMQ/6.2.0"
	// (`internal/activemq.ServerArtemis` and `ServerClassic` are the two
	// prefixes). Empty sends no header at all, which is what the specification makes
	// distinct from a claim.
	ServerHeader string
	// Session is the CONNECTED frame's `session` header. Empty generates one.
	Session string
	// Login and Passcode are the credentials the server requires. Both
	// empty accepts any CONNECT, including one with no credentials at all,
	// which is what Classic's pinned image allows.
	Login, Passcode string
	// RefuseConnect makes every CONNECT answered with an ERROR frame
	// carrying this sentence, and then the socket. Empty accepts.
	RefuseConnect string
	// UnsubscribeReceiptDelay is how long the server waits before answering
	// an UNSUBSCRIBE frame's receipt. Zero takes
	// [DefaultUnsubscribeReceiptDelay]; see [conn.unsubscribe] for the
	// go-stomp race it exists for.
	UnsubscribeReceiptDelay time.Duration
	// NoUnsubscribeDelay answers the receipt at once, for a test that is
	// about the race itself.
	NoUnsubscribeDelay bool
	// PromiseHeartBeats is the interval the CONNECTED frame's `heart-beat`
	// header promises the server will send at -- and the server then sends
	// none. A client that asked to receive heart-beats arms a read timer
	// from it, and go-stomp closes the connection ITSELF when that timer
	// fires, which is the one way to make the library, rather than the
	// server or the driver, end a connection. Zero sends no header.
	PromiseHeartBeats time.Duration
	// WantHeartBeats is the interval the CONNECTED frame's `heart-beat`
	// header asks the CLIENT to send at, the second of its two numbers.
	// go-stomp then writes a heart-beat whenever it has written nothing for
	// that long, and it writes it from its process loop -- the loop that also
	// hands every MESSAGE to its subscription -- so [Server.HeartBeats] is a
	// measure of whether that loop is still running. Zero asks for none.
	WantHeartBeats time.Duration
	// DropSilentClients closes a connection whose client has sent nothing,
	// not even a heart-beat, for this long, with no DISCONNECT and no ERROR
	// frame: what both brokers do to a client whose heart-beats stop.
	// Measured on both pinned images, a source that stopped heart-beating for
	// eight seconds against a one-second HeartbeatSend found its socket
	// closed and every delivery it had not acknowledged redelivered. It is
	// counted in [Server.SilentDrops]. Zero never drops a client.
	DropSilentClients time.Duration
}

// DefaultUnsubscribeReceiptDelay is how long an UNSUBSCRIBE's receipt is held
// back by default.
//
// It is not a performance knob and not a fidelity one: it is the width of a
// missed-wakeup window inside go-stomp's own `Subscription.Unsubscribe`, and
// five milliseconds is several scheduling opportunities even on a loaded
// machine while being less than a real broker's own answer over a network.
const DefaultUnsubscribeReceiptDelay = 5 * time.Millisecond

// Frame is one frame the server received.
//
// It is a VALUE with its own copy of the header and the body, which is the
// only shape an observation can have: a test holding the library's own frame
// would be watching the thing it is asserting about change under it, because
// `frame.Reader` reuses nothing but the driver does.
type Frame struct {
	// Command is the frame's command (`frame.SEND`, `frame.ACK`, …).
	Command string
	// Body is a copy of the frame's body.
	Body []byte
	// Conn is which accepted connection the frame arrived on, counting from
	// one. It is what makes "the SUBSCRIBE was re-sent on the NEW
	// connection" a checkable statement.
	Conn int
	// Seq is the frame's position in the server's whole log, counting from
	// one, so that a failure message can say which frame it means.
	Seq int

	header *frame.Header
}

// Get is the first value of one header, or "" when the frame has no such
// header. STOMP 1.2 says the first occurrence wins and this answers the same.
func (f Frame) Get(key string) string {
	if f.header == nil {
		return ""
	}
	return f.header.Get(key)
}

// Has reports whether the frame carries the header at all, which is not the
// same question as whether its value is empty.
func (f Frame) Has(key string) bool {
	if f.header == nil {
		return false
	}
	_, ok := f.header.Contains(key)
	return ok
}

// Len is how many header entries the frame carries, duplicates included.
func (f Frame) Len() int {
	if f.header == nil {
		return 0
	}
	return f.header.Len()
}

// At is the header entry at index i, in the order it was on the wire.
func (f Frame) At(i int) (key, value string) {
	if f.header == nil {
		return "", ""
	}
	return f.header.GetAt(i)
}

// Headers is every header as a map, first occurrence winning. It is for a
// failure message and for an assertion over the whole set; [Frame.Get] is what
// an assertion about one header should use.
func (f Frame) Headers() map[string]string {
	out := make(map[string]string, f.Len())
	for i := range f.Len() {
		k, v := f.At(i)
		if _, dup := out[k]; dup {
			continue
		}
		out[k] = v
	}
	return out
}

// String renders the frame the way a failure message wants it: the command,
// the headers in wire order, and the body's length.
func (f Frame) String() string {
	var b strings.Builder
	b.WriteString("#")
	b.WriteString(strconv.Itoa(f.Seq))
	b.WriteString(" conn=")
	b.WriteString(strconv.Itoa(f.Conn))
	b.WriteByte(' ')
	b.WriteString(f.Command)
	b.WriteString(" {")
	for i := range f.Len() {
		if i > 0 {
			b.WriteString(", ")
		}
		k, v := f.At(i)
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(v)
	}
	b.WriteString("} body=")
	b.WriteString(strconv.Itoa(len(f.Body)))
	return b.String()
}

// Message is a MESSAGE frame a test asks the server to push.
//
// Every identifier has a generated default, because the identifiers are what
// an assertion names: the `ack` header is what a STOMP 1.2 ACK frame carries
// as its `id`, so a test that wants to say WHICH delivery was acknowledged
// needs to know it. [Server.Deliver] returns the ones it assigned.
type Message struct {
	// Body is the frame's body.
	Body []byte
	// ContentType is the `content-type` header, or "" for none.
	ContentType string
	// MessageID is the `message-id` header. Empty generates one.
	//
	// Over STOMP this is the BROKER's identifier for the delivery and not
	// the publisher's for the message, which is why the shared mapping does
	// not restore it over a publisher's own.
	MessageID string
	// AckID is the `ack` header, which a STOMP 1.2 ACK or NACK frame names
	// as its `id`. Empty generates one.
	AckID string
	// Redelivered sets `redelivered: true`, which is the whole of what a
	// STOMP delivery says about its history on either broker.
	Redelivered bool
	// Headers are extra headers, in the order given. They are a slice and
	// not a map so that a test can send a DUPLICATE header name, which is
	// a thing a frame can carry and a map cannot express.
	Headers [][2]string
}

// Server is one scriptable STOMP 1.2 server on a loopback listener.
//
// Every method is safe to call from any goroutine. Nothing here starts a
// goroutine that [Server.Close] does not stop: the accept loop ends when the
// listener closes, and each connection's reader ends when its socket does,
// which Close closes explicitly rather than waiting for.
type Server struct {
	opts Options
	ln   net.Listener

	mu sync.Mutex
	// changed is closed and replaced on every recorded change, which is how
	// a waiter blocks on a predicate without polling and without sleeping.
	changed chan struct{}
	// done is closed by Close, so that a deliberate delay cannot hold a
	// connection's reader past the server's own teardown.
	done       chan struct{}
	closed     bool
	frames     []Frame
	violations []error
	conns      []*conn
	accepted   int
	subs       []*subscription
	ids        int
	// heartBeats is per connection, because the backend's own connection
	// heart-beats too: a server-wide count keeps rising while one source's
	// connection is stalled.
	heartBeats map[int]int
	silent     int

	// The script. Each entry is consumed as it fires, except refuseConnect,
	// which stands until it is cleared.
	holdAny       int
	holdOn        map[string]int
	failSend      map[string]string
	refuseSub     map[string]string
	refuseOnce    map[string]bool
	refuseConnect string

	wg sync.WaitGroup
}

// conn is one accepted connection.
type conn struct {
	srv  *Server
	nc   net.Conn
	id   int
	wmu  sync.Mutex
	w    *frame.Writer
	dead bool
	once sync.Once
	// gone is the same fact as dead, recorded under the SERVER's lock so
	// that a selection made while holding it needs no second lock. The two
	// are separate fields rather than one because the lock order is wmu
	// then mu -- [conn.shutdown] takes both, in that order -- so a chooser
	// holding mu must never reach for wmu.
	gone bool
}

// subscription is one SUBSCRIBE frame's live state.
type subscription struct {
	c    *conn
	id   string
	dest string
	live bool
}

// New starts a server on 127.0.0.1 with a port the operating system picks.
//
// The caller owns it and must call [Server.Close]; a test registers that with
// `t.Cleanup` and needs nothing else.
func New(o Options) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Server{
		opts:       o,
		ln:         ln,
		changed:    make(chan struct{}),
		done:       make(chan struct{}),
		holdOn:     map[string]int{},
		failSend:   map[string]string{},
		refuseSub:  map[string]string{},
		refuseOnce: map[string]bool{},
		heartBeats: map[int]int{},
	}
	s.wg.Add(1)
	go s.accept()
	return s, nil
}

// Addr is the endpoint a client dials, as `host:port`.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops the server and every goroutine it started. It is idempotent.
//
// The order is what makes it goleak-clean, and all three steps are needed:
// closing the listener ends the accept loop, closing each socket ends that
// connection's reader (a reader parked in `frame.Reader.Read` has no other
// signal), and the wait is what turns "they will stop" into "they have".
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done)
	conns := make([]*conn, len(s.conns))
	copy(conns, s.conns)
	s.notifyLocked()
	s.mu.Unlock()
	err := s.ln.Close()
	for _, c := range conns {
		c.shutdown()
	}
	s.wg.Wait()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// accept takes connections until the listener closes.
func (s *Server) accept() {
	defer s.wg.Done()
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = nc.Close()
			return
		}
		s.accepted++
		c := &conn{srv: s, nc: nc, id: s.accepted, w: frame.NewWriter(nc)}
		s.conns = append(s.conns, c)
		s.notifyLocked()
		s.wg.Add(1)
		s.mu.Unlock()
		go c.serve()
	}
}

// notifyLocked wakes every waiter. The caller holds s.mu.
func (s *Server) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// record appends one received frame and wakes the waiters.
func (s *Server) record(c *conn, f *frame.Frame) Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := Frame{Command: f.Command, Conn: c.id, Seq: len(s.frames) + 1}
	if f.Header != nil {
		rec.header = f.Header.Clone()
	}
	if len(f.Body) > 0 {
		rec.Body = append([]byte(nil), f.Body...)
	}
	s.frames = append(s.frames, rec)
	s.notifyLocked()
	return rec
}

// violate records a protocol violation the client committed.
//
// It exists because a socket-level server is the only place one can be seen,
// and because the alternative -- guessing what the client meant -- is how a
// test comes to pass against a driver that writes a frame no broker would
// accept. `isGone` keeps an ordinary disconnection out of the list: reporting
// that would make every test that closes its client fail.
func (s *Server) violate(err error) {
	if err == nil || isGone(err) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.violations = append(s.violations, err)
	s.notifyLocked()
}

// isGone reports whether err is a connection ending rather than a fault.
func isGone(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset by peer")
}

// MaxDeclaredBody is the largest body a client may DECLARE in a
// `content-length` header, and so the largest one this server will read.
//
// It is a bound on MEMORY and not on fidelity. `frame.Reader` believes the
// declaration before it has read a byte of the body — `f.Body = make([]byte,
// contentLength)`, go-stomp v3.1.5 `frame/reader.go:108` — and
// `Header.ContentLength` parses a 32-bit value. So the forty-six bytes
// `SEND\ndestination:d\ncontent-length:4294967295\n\n`, which carry no body at
// all, made this server allocate four gibibytes and then wait for bytes that
// were never coming.
//
// A mebibyte is three orders of magnitude above anything this driver sends —
// nothing in the module publishes a body larger than a few hundred bytes — and
// both brokers cap the frame as well (ActiveMQ Classic's
// `wireFormat.maxFrameSize`, Artemis's `stompMaxFramePayloadLength`), so
// refusing a larger declaration is the brokers' own behaviour rather than a
// shortcut. What it refuses, it refuses the way this server refuses everything
// else it will not guess at: a recorded violation and then the socket.
const MaxDeclaredBody = 1 << 20

// boundedFrames hands `frame.Reader` one whole frame at a time, refusing a
// frame that declares more body than [MaxDeclaredBody] BEFORE the reader has
// seen the declaration — which is the only moment the refusal can happen,
// because the reader allocates on the declaration and not on the bytes.
//
// Under the fuzz target, where the `raw` step sends up to sixty-four arbitrary
// bytes, the unbounded version was eight worker processes each allocating four
// gibibytes at once. The kernel's answer is SIGKILL, and Go's fuzzing
// coordinator records whatever input it was MINIMIZING when a worker died as
// the crasher (`internal/fuzz/worker.go:270`) — which is why #53, #59 and #63
// are three committed inputs that do not reproduce and one defect.
//
// It finds a frame's end the way `frame.Reader` does, so that the two agree on
// where the next one begins: a line at a time with `ReadBytes('\n')` (which is
// what `frame.Reader.readLine` uses, so a header line longer than a buffer is
// read by both rather than refused by one), the blank line ends the headers,
// and the body is the declared length plus its terminator or else everything up
// to the first NUL. It reads the FIRST `content-length` header, because
// `Header.ContentLength` goes through `Header.Contains`, which does too.
type boundedFrames struct {
	br      *bufio.Reader
	pending []byte
	err     error
}

// newBoundedFrames wraps r.
func newBoundedFrames(r io.Reader) *boundedFrames {
	return &boundedFrames{br: bufio.NewReader(r)}
}

// Read implements [io.Reader] over whole frames.
func (b *boundedFrames) Read(p []byte) (int, error) {
	for len(b.pending) == 0 {
		if b.err != nil {
			return 0, b.err
		}
		b.pending, b.err = b.frame()
	}
	n := copy(p, b.pending)
	b.pending = b.pending[n:]
	return n, nil
}

// frame reads one frame's bytes and returns them unchanged, or the error that
// ended the stream along with whatever bytes arrived before it.
func (b *boundedFrames) frame() ([]byte, error) {
	var out []byte
	declared := -1
	for {
		line, err := b.br.ReadBytes('\n')
		first := len(out) == 0
		out = append(out, line...)
		if err != nil {
			return out, err
		}
		head := bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))
		if len(head) == 0 {
			if first {
				// A heart-beat newline. There is no body after it.
				return out, nil
			}
			break
		}
		if declared >= 0 {
			continue
		}
		if name, value, ok := bytes.Cut(head, []byte(":")); ok && string(name) == frame.ContentLength {
			if n, err := strconv.ParseUint(string(value), 10, 32); err == nil {
				declared = int(n)
			}
		}
	}
	switch {
	case declared > MaxDeclaredBody:
		// The refused frame's own bytes are DROPPED rather than handed on, and
		// that is the whole guard: `frame.Reader` allocates the moment it has
		// parsed the header section, so a guard that passed the headers
		// through and reported the error afterwards would report it four
		// gibibytes too late.
		return nil, errors.New("stompsrv: the client declared a body of " + strconv.Itoa(declared) +
			" bytes and this server reads at most " + strconv.Itoa(MaxDeclaredBody) +
			". A declaration is believed before the body arrives, so honouring this one would allocate it")
	case declared >= 0:
		// The declared body and the NUL that terminates it. Whether that byte
		// really is a NUL is `frame.Reader`'s own check, on the same bytes.
		body := make([]byte, declared+1)
		n, err := io.ReadFull(b.br, body)
		return append(out, body[:n]...), err
	default:
		// No declaration, so the body is however many bytes arrive before the
		// first NUL: bounded by the stream itself rather than by a number in
		// it, which is the case this guard is not about.
		body, err := b.br.ReadBytes(0)
		return append(out, body...), err
	}
}

// serve reads one connection's frames until it ends.
func (c *conn) serve() {
	defer c.srv.wg.Done()
	defer c.shutdown()
	r := frame.NewReader(newBoundedFrames(c.nc))
	for {
		if d := c.srv.opts.DropSilentClients; d > 0 {
			// Re-armed before every read, so that it measures the silence
			// since the client's last frame or heart-beat, which is what a
			// broker's read check measures.
			_ = c.nc.SetReadDeadline(time.Now().Add(d))
		}
		f, err := r.Read()
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				// The client went quiet. A broker closes the socket and
				// says nothing, and the deferred shutdown is exactly that.
				c.srv.countSilent()
				return
			}
			c.srv.violate(err)
			c.finish()
			return
		}
		if f == nil {
			// A heart-beat newline, which carries nothing and is not a
			// frame. Recording it would put an empty command in the log, so
			// it is counted instead.
			c.srv.countHeartBeat(c.id)
			continue
		}
		if !c.handle(f) {
			c.finish()
			return
		}
	}
}

// handle acts on one frame and reports whether the connection continues.
func (c *conn) handle(f *frame.Frame) bool {
	c.srv.record(c, f)
	switch f.Command {
	case frame.CONNECT, frame.STOMP:
		return c.connect(f)
	case frame.SEND:
		return c.send(f)
	case frame.SUBSCRIBE:
		return c.subscribe(f)
	case frame.UNSUBSCRIBE:
		return c.unsubscribe(f)
	case frame.ACK, frame.NACK:
		return c.settle(f)
	case frame.DISCONNECT:
		c.receipt(f)
		return false
	case frame.BEGIN, frame.COMMIT, frame.ABORT:
		c.receipt(f)
		return true
	default:
		c.srv.violate(errors.New("stompsrv: the client sent a " + f.Command + " frame, which no client of this driver sends"))
		c.errorFrame("unknown command " + f.Command)
		return false
	}
}

// connect answers the CONNECT frame.
func (c *conn) connect(f *frame.Frame) bool {
	if msg := c.srv.opts.RefuseConnect; msg != "" {
		c.errorFrame(msg)
		return false
	}
	c.srv.mu.Lock()
	msg := c.srv.refuseConnect
	c.srv.mu.Unlock()
	if msg != "" {
		c.errorFrame(msg)
		return false
	}
	login, passcode := f.Header.Get(frame.Login), f.Header.Get(frame.Passcode)
	want := c.srv.opts
	if (want.Login != "" || want.Passcode != "") && (login != want.Login || passcode != want.Passcode) {
		// Artemis's own sentence for an anonymous CONNECT, which the error
		// table maps to CodeUnauthorized.
		c.errorFrame("AMQ229031: Unable to validate user: " + login)
		return false
	}
	out := frame.New(frame.CONNECTED, frame.Version, "1.2")
	session := want.Session
	if session == "" {
		session = "stompsrv-" + strconv.Itoa(c.id)
	}
	out.Header.Set(frame.Session, session)
	if want.ServerHeader != "" {
		out.Header.Set(frame.Server, want.ServerHeader)
	}
	// No `heart-beat` header unless the options promise one or ask for one:
	// by default this server sends none and asks for none, so the client
	// arms no timer and starts no goroutine that outlives it. A promise it
	// then breaks is a read timer the client arms and lets fire, and a
	// request is a write timer; go-stomp keeps both as `time.Timer`s in its
	// own process loop, so neither is a goroutine of its own.
	if send, recv := want.PromiseHeartBeats, want.WantHeartBeats; send > 0 || recv > 0 {
		out.Header.Set(frame.HeartBeat, strconv.FormatInt(send.Milliseconds(), 10)+","+strconv.FormatInt(recv.Milliseconds(), 10))
	}
	return c.write(out)
}

// send answers a SEND frame's receipt, or deliberately does not.
func (c *conn) send(f *frame.Frame) bool {
	dest := f.Header.Get(frame.Destination)
	s := c.srv
	s.mu.Lock()
	fail, failing := s.failSend[dest]
	if failing {
		delete(s.failSend, dest)
	}
	held := false
	switch {
	case s.holdOn[dest] > 0:
		s.holdOn[dest]--
		held = true
	case s.holdAny > 0:
		s.holdAny--
		held = true
	}
	s.mu.Unlock()
	if failing {
		// A broker that refuses a SEND answers with an ERROR frame naming
		// the SEND's own `receipt` in its `receipt-id`, when the SEND asked
		// for one -- measured on both pinned images. Artemis then closes the
		// socket and Classic does not, but go-stomp closes the connection
		// itself on any ERROR frame, so ending it here is what a client of
		// either sees.
		out := frame.New(frame.ERROR, frame.Message, fail)
		if id, ok := f.Header.Contains(frame.Receipt); ok {
			out.Header.Set(frame.ReceiptId, id)
		}
		_ = c.write(out)
		return false
	}
	if held {
		// the ambiguous window, held open: the frame left the socket and
		// the broker says nothing, so the publisher cannot know whether it
		// was taken. Nothing is written at all.
		return true
	}
	c.receipt(f)
	return true
}

// subscribe records the subscription and either answers or refuses it.
func (c *conn) subscribe(f *frame.Frame) bool {
	dest := f.Header.Get(frame.Destination)
	id := f.Header.Get(frame.Id)
	s := c.srv
	s.mu.Lock()
	s.subs = append(s.subs, &subscription{c: c, id: id, dest: dest, live: true})
	msg, refusing := s.refuseSub[dest]
	if refusing && s.refuseOnce[dest] {
		delete(s.refuseSub, dest)
		delete(s.refuseOnce, dest)
	}
	s.notifyLocked()
	s.mu.Unlock()
	if refusing {
		// Artemis, with auto-creation disabled: an ERROR frame and then the
		// socket. The subscription is recorded FIRST so that a test can
		// assert the SUBSCRIBE was sent even though it was refused.
		c.errorFrame(msg)
		return false
	}
	// A `receipt` on a SUBSCRIBE is a go-stomp v3.1.5 defect (source.go says
	// so) and this driver never sets one; answering it anyway is what a
	// broker does, and the panic that follows is the library's.
	c.receipt(f)
	return true
}

// unsubscribe retires the subscription and answers the library's receipt.
//
// The answer is not optional. go-stomp sets `receipt: <subscription id>` on an
// UNSUBSCRIBE frame from inside its own process loop and then waits on a
// condition variable for the RECEIPT; a server that said nothing would stall
// every requeue for `DefaultUnsubscribeReceiptTimeout` and leak the goroutine
// that wait starts.
func (c *conn) unsubscribe(f *frame.Frame) bool {
	id := f.Header.Get(frame.Id)
	s := c.srv
	s.mu.Lock()
	for _, sub := range s.subs {
		if sub.c == c && sub.id == id {
			sub.live = false
		}
	}
	s.notifyLocked()
	s.mu.Unlock()
	// The answer is DELAYED, and this is the one place in this file where a
	// deliberate pause is the right thing.
	//
	// `Subscription.Unsubscribe` waits for this receipt through a helper that
	// reads the subscription's state, starts a GOROUTINE to call
	// `cond.Wait()`, and selects between that goroutine and a timer. A
	// Broadcast that lands after the state read and before the goroutine
	// reaches the notify list is MISSED -- and the wait then runs
	// `DefaultUnsubscribeReceiptTimeout`, thirty seconds, which
	// `Backend.dial` does not shorten because it sets `RcvReceiptTimeout` and
	// `MsgSendTimeout` from `Config.ReceiptTimeout` and not
	// `ConnOpt.UnsubscribeReceiptTimeout`. `Source.reset` then returns that
	// as the requeue's answer and never re-subscribes, so the consumer stops
	// receiving: a driver bug's symptom, produced by a test double answering
	// faster than any broker would.
	//
	// A loopback RECEIPT arrives in microseconds and a real broker's takes a
	// millisecond or more over a network, so the delay makes this server LESS
	// artificial rather than more. It bounds the window at the cost of a few
	// milliseconds per requeue, and `closeSource` in the module above still
	// tolerates the timeout for the case a loaded machine loses even this.
	if d := s.unsubDelay(); d > 0 {
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-s.done:
			timer.Stop()
			return false
		}
	}
	c.receipt(f)
	return true
}

// unsubDelay is how long to wait before answering an UNSUBSCRIBE's receipt.
func (s *Server) unsubDelay() time.Duration {
	if s.opts.NoUnsubscribeDelay {
		return 0
	}
	if s.opts.UnsubscribeReceiptDelay > 0 {
		return s.opts.UnsubscribeReceiptDelay
	}
	return DefaultUnsubscribeReceiptDelay
}

// settle records an ACK or a NACK and answers a receipt if one was asked for.
func (c *conn) settle(f *frame.Frame) bool {
	if _, ok := f.Header.Contains(frame.Id); !ok {
		// STOMP 1.2 names the delivery with `id`, taken from the MESSAGE
		// frame's own `ack` header. A settlement with no `id` is a frame no
		// broker can act on, and recording it as a violation is what keeps
		// a test from passing on a settlement that settled nothing.
		c.srv.violate(errors.New("stompsrv: a " + f.Command + " frame carries no `id` header, so it names no delivery"))
	}
	c.receipt(f)
	return true
}

// receipt answers a frame's `receipt` header, if it has one.
func (c *conn) receipt(f *frame.Frame) {
	id, ok := f.Header.Contains(frame.Receipt)
	if !ok {
		return
	}
	_ = c.write(frame.New(frame.RECEIPT, frame.ReceiptId, id))
}

// errorFrame writes an ERROR frame that names no receipt. The caller then
// ends the connection, which is what a client of either broker sees: measured,
// Artemis closes the socket after an ERROR frame, and go-stomp closes the
// connection itself on one even where Classic would have kept it open.
func (c *conn) errorFrame(message string) {
	_ = c.write(frame.New(frame.ERROR, frame.Message, message))
}

// write sends one frame under the connection's own write lock and reports
// whether the connection survives.
//
// The lock is what makes a [Server.Deliver] from a test goroutine safe against
// a RECEIPT from this connection's reader: both write frames, and two
// interleaved frames on one socket is a malformed stream rather than a race
// the detector would see.
func (c *conn) write(f *frame.Frame) bool {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.dead {
		return false
	}
	if err := c.w.Write(f); err != nil {
		c.srv.violate(err)
		return false
	}
	return true
}

// shutdown closes the socket once. It is what ends this connection's reader.
func (c *conn) shutdown() {
	c.once.Do(func() {
		c.retire()
		_ = c.nc.Close()
	})
}

// retire stops the connection writing and takes its subscriptions out of
// circulation, leaving the socket itself open.
func (c *conn) retire() {
	c.wmu.Lock()
	c.dead = true
	c.wmu.Unlock()
	c.srv.mu.Lock()
	c.gone = true
	for _, sub := range c.srv.subs {
		if sub.c == c {
			sub.live = false
		}
	}
	c.srv.notifyLocked()
	c.srv.mu.Unlock()
}

// finish ends the connection from the reader's side without losing what the
// server has written on it.
//
// Closing a socket that still holds unread input makes the kernel answer with
// a reset and drop whatever it has not transmitted yet, so a client that had
// written past a DISCONNECT or a refused frame could lose the RECEIPTs written
// just before it: FuzzSTOMPServer counted 15 receipts for 16 on a loaded
// runner. The write half is closed instead, which sends what is queued and
// then the end of input, and the socket is closed once the client has closed
// its own half or finishBudget has run out.
func (c *conn) finish() {
	c.retire()
	if tc, ok := c.nc.(*net.TCPConn); ok && tc.CloseWrite() == nil {
		_ = tc.SetReadDeadline(time.Now().Add(finishBudget))
		_, _ = io.Copy(io.Discard, tc)
	}
	c.shutdown()
}

// finishBudget bounds how long finish waits for the client to close its half.
const finishBudget = 2 * time.Second

// liveConnLocked is the newest connection that has not been closed, or nil.
// The caller holds s.mu.
func (s *Server) liveConnLocked() *conn {
	for i := len(s.conns) - 1; i >= 0; i-- {
		if !s.conns[i].gone {
			return s.conns[i]
		}
	}
	return nil
}

// --- observation -----------------------------------------------------------

// Frames is every frame the server has received, in order, across every
// connection.
func (s *Server) Frames() []Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Frame(nil), s.frames...)
}

// FramesOf is every frame with one command, in order.
func (s *Server) FramesOf(command string) []Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Frame
	for _, f := range s.frames {
		if f.Command == command {
			out = append(out, f)
		}
	}
	return out
}

// Count is how many frames with one command the server has received.
func (s *Server) Count(command string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, f := range s.frames {
		if f.Command == command {
			n++
		}
	}
	return n
}

// Commands is the sequence of commands the server received from one frame
// onward, which is how a test asserts what the driver did in response to
// something rather than over its whole life.
//
// `from` is a sequence number, 1-based, as [Frame.Seq] reports it; the result
// includes the frame at that position.
func (s *Server) Commands(from int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, f := range s.frames {
		if f.Seq >= from {
			out = append(out, f.Command)
		}
	}
	return out
}

// Next is the sequence number the next received frame will carry, which a test
// takes before it acts so that [Server.Commands] can answer about that action
// alone.
func (s *Server) Next() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.frames) + 1
}

// SettledIDs is the `id` header of every frame with one command, which for an
// ACK or a NACK is the delivery it names.
func (s *Server) SettledIDs(command string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, f := range s.frames {
		if f.Command == command {
			out = append(out, f.Get(frame.Id))
		}
	}
	return out
}

// Accepted is how many connections the server has ever taken.
//
// It counts every one rather than the live ones, because a test about a
// reconnection is a test about the SECOND connection existing and a live count
// would report one either way.
func (s *Server) Accepted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

// HeartBeats is how many heart-beats the server has received on one
// connection, named by the [Frame.Conn] of a frame that arrived on it.
func (s *Server) HeartBeats(conn int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartBeats[conn]
}

// SilentDrops is how many connections the server closed because their client
// had sent nothing for [Options.DropSilentClients].
func (s *Server) SilentDrops() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.silent
}

// countHeartBeat records one heart-beat received on connection conn and wakes
// the waiters.
func (s *Server) countHeartBeat(conn int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartBeats[conn]++
	s.notifyLocked()
}

// countSilent records one connection closed for silence and wakes the waiters.
func (s *Server) countSilent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.silent++
	s.notifyLocked()
}

// Subscribes is how many SUBSCRIBE frames the server has received for one
// destination, refused ones included.
func (s *Server) Subscribes(dest string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sub := range s.subs {
		if sub.dest == dest {
			n++
		}
	}
	return n
}

// Live is how many subscriptions on one destination are still attached.
func (s *Server) Live(dest string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sub := range s.subs {
		if sub.dest == dest && sub.live {
			n++
		}
	}
	return n
}

// Violations is every protocol violation the client committed, which is the
// accessor a socket-level server has and a library-level fake cannot.
func (s *Server) Violations() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.violations...)
}

// --- waiting ---------------------------------------------------------------

// ErrWaitTimeout is a wait that gave up. It names what it was waiting for.
type ErrWaitTimeout struct {
	// What is the condition that never held.
	What string
	// Budget is how long the wait allowed.
	Budget time.Duration
}

// Error implements error.
func (e *ErrWaitTimeout) Error() string {
	return "stompsrv: " + e.What + " did not happen within " + e.Budget.String()
}

// wait blocks until pred holds or the budget runs out.
//
// pred runs under the server's lock, and the wait is on a channel the server
// replaces on every change rather than on a sleep: a test that slept would be
// slow on a quiet machine and flaky on a busy one, and this host runs about
// fifty other people's containers.
func (s *Server) wait(what string, budget time.Duration, pred func() bool) error {
	t := time.NewTimer(budget)
	defer t.Stop()
	for {
		s.mu.Lock()
		ok := pred()
		ch := s.changed
		s.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-ch:
		case <-t.C:
			// One last look: the predicate may have become true between the
			// unlock and the timer firing.
			s.mu.Lock()
			ok := pred()
			s.mu.Unlock()
			if ok {
				return nil
			}
			return &ErrWaitTimeout{What: what, Budget: budget}
		}
	}
}

// WaitAccepted waits until the server has taken at least n connections.
func (s *Server) WaitAccepted(n int, budget time.Duration) error {
	return s.wait(strconv.Itoa(n)+" connections", budget, func() bool { return s.accepted >= n })
}

// WaitHeartBeats waits until the server has received at least n heart-beats on
// one connection.
func (s *Server) WaitHeartBeats(conn, n int, budget time.Duration) error {
	return s.wait(strconv.Itoa(n)+" heart-beats on connection "+strconv.Itoa(conn), budget,
		func() bool { return s.heartBeats[conn] >= n })
}

// WaitFrames waits until the server has received at least n frames with one
// command.
func (s *Server) WaitFrames(command string, n int, budget time.Duration) error {
	return s.wait(strconv.Itoa(n)+" "+command+" frames", budget, func() bool {
		got := 0
		for _, f := range s.frames {
			if f.Command == command {
				got++
			}
		}
		return got >= n
	})
}

// WaitSubscribed waits until the server has received at least n SUBSCRIBE
// frames for one destination.
func (s *Server) WaitSubscribed(dest string, n int, budget time.Duration) error {
	return s.wait(strconv.Itoa(n)+" subscriptions on "+dest, budget, func() bool {
		got := 0
		for _, sub := range s.subs {
			if sub.dest == dest {
				got++
			}
		}
		return got >= n
	})
}

// WaitLive waits until n subscriptions on one destination are attached at
// once, which is not the same question as [Server.WaitSubscribed]: a reset
// unsubscribes and re-subscribes, so the total rises while the live count
// returns to where it was.
func (s *Server) WaitLive(dest string, n int, budget time.Duration) error {
	return s.wait(strconv.Itoa(n)+" live subscriptions on "+dest, budget, func() bool {
		got := 0
		for _, sub := range s.subs {
			if sub.dest == dest && sub.live {
				got++
			}
		}
		return got >= n
	})
}

// --- the script ------------------------------------------------------------

// HoldReceipts makes the next n SEND frames' `receipt` headers go unanswered,
// whatever destination they name.
//
// This is the ambiguous window held open, and it is the one thing in this
// file no broker will do on request: the frame left the socket, so the broker
// MAY have the message, and a publisher told "failed" republishes it.
func (s *Server) HoldReceipts(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdAny += n
}

// HoldReceiptsOn is [Server.HoldReceipts] for one destination, which is what a
// dead-letter test needs: the original's publication must be answered and the
// dead-letter publication must not.
func (s *Server) HoldReceiptsOn(dest string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdOn[dest] += n
}

// FailSend answers the next SEND frame for dest with an ERROR frame carrying
// message, and then closes the connection, which is what a client of either
// broker sees for a SEND the broker refuses. The ERROR names the SEND's
// `receipt` in its `receipt-id` when the SEND asked for one, as both brokers'
// do; [Server.ErrorFrame] is the ERROR that names no receipt.
func (s *Server) FailSend(dest, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failSend[dest] = message
}

// RefuseSubscribe answers the NEXT SUBSCRIBE frame for dest with an ERROR
// frame carrying message, and then closes the connection.
//
// It is Artemis's measured behaviour for a destination that does not exist:
// `AMQ339001 Destination does not exist <name>`, and the socket. It is
// one-shot rather than standing, because a standing refusal turns the driver's
// reconnect loop into a subscribe-refuse-redial spin for as long as the test
// runs, and what a test wants to observe is the refusal reaching a CALLER
// rather than the loop's behaviour under it.
func (s *Server) RefuseSubscribe(dest, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refuseSub[dest] = message
	s.refuseOnce[dest] = true
}

// RefuseConnects answers every CONNECT frame from now on with an ERROR frame
// carrying message, and then the socket, leaving the connections already
// accepted alone. An empty message accepts again.
//
// It is [Options.RefuseConnect] switched on part-way through, which is what a
// test of a re-dial needs: the first connections have to succeed for there to
// be anything to re-dial, and every later one has to fail for the dial loop's
// own limits to be what ends it.
func (s *Server) RefuseConnects(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refuseConnect = message
}

// Deliver pushes one MESSAGE frame per message to the newest live subscription
// on dest, and returns the `ack` identifiers it assigned.
//
// The identifiers are the return value because they are what an assertion
// names: a STOMP 1.2 ACK frame carries the delivery's `ack` header as its
// `id`, so "no ACK reached the server for THAT delivery" is a statement about
// this slice.
func (s *Server) Deliver(dest string, msgs ...Message) ([]string, error) {
	s.mu.Lock()
	var target *subscription
	for _, sub := range s.subs {
		if sub.dest == dest && sub.live {
			target = sub
		}
	}
	if target == nil {
		s.mu.Unlock()
		return nil, errors.New("stompsrv: nothing is subscribed to " + dest +
			"; a delivery needs a live subscription, and `Conn.Subscribe` returns as soon as the frame is queued " +
			"-- wait for the SUBSCRIBE frame to arrive (Server.WaitSubscribed) first")
	}
	out := make([]string, 0, len(msgs))
	frames := make([]*frame.Frame, 0, len(msgs))
	for i := range msgs {
		m := msgs[i]
		s.ids++
		n := strconv.Itoa(s.ids)
		if m.MessageID == "" {
			m.MessageID = "srv-msg-" + n
		}
		if m.AckID == "" {
			m.AckID = "srv-ack-" + n
		}
		f := frame.New(frame.MESSAGE,
			frame.Destination, dest,
			frame.MessageId, m.MessageID,
			frame.Subscription, target.id,
			frame.Ack, m.AckID,
		)
		if m.ContentType != "" {
			f.Header.Set(frame.ContentType, m.ContentType)
		}
		if m.Redelivered {
			f.Header.Set("redelivered", "true")
		}
		for _, kv := range m.Headers {
			f.Header.Add(kv[0], kv[1])
		}
		// The length is set explicitly so that a body containing a null
		// byte is framed rather than truncated: `frame.Reader` falls back to
		// reading up to the null terminator when there is no
		// `content-length`, which is a body a test cannot choose freely.
		f.Header.Set(frame.ContentLength, strconv.Itoa(len(m.Body)))
		f.Body = m.Body
		frames = append(frames, f)
		out = append(out, m.AckID)
	}
	c := target.c
	s.mu.Unlock()
	for _, f := range frames {
		if !c.write(f) {
			return out, errors.New("stompsrv: the connection under the subscription on " + dest + " is gone")
		}
	}
	return out, nil
}

// Forward hands the body of every SEND frame the server has received for
// `from` to the live subscription on `to`, as a MESSAGE frame, and returns the
// `ack` identifiers it assigned.
//
// It is NOT routing, and the name says so deliberately. This server does not
// implement a virtual topic, a multicast address or a selector, and it will
// not decide which subscriptions a publication reaches -- that is the part of
// a broker `conformance/` covers against the real images. What this does is
// let a test or an example say explicitly "now deliver what was published",
// which is the one step a broker would have taken on its own, at a moment the
// caller picks.
func (s *Server) Forward(from, to string) ([]string, error) {
	s.mu.Lock()
	var msgs []Message
	for _, f := range s.frames {
		if f.Command != frame.SEND || f.Get(frame.Destination) != from {
			continue
		}
		msgs = append(msgs, Message{Body: f.Body, ContentType: f.Get(frame.ContentType)})
	}
	s.mu.Unlock()
	if len(msgs) == 0 {
		return nil, errors.New("stompsrv: nothing was published to " + from)
	}
	return s.Deliver(to, msgs...)
}

// DropConnection closes the newest live socket with no DISCONNECT, which is
// what a broker restart, a network partition or an operator's `kill -9` does
// to a client.
//
// It closes the SOCKET and not the STOMP session on purpose: a DISCONNECT is
// an orderly shutdown and what this has to reproduce is a connection that
// simply went away.
func (s *Server) DropConnection() error {
	s.mu.Lock()
	target := s.liveConnLocked()
	s.mu.Unlock()
	if target == nil {
		return errors.New("stompsrv: there is no live connection to drop")
	}
	target.shutdown()
	return nil
}

// ErrorFrame writes an ERROR frame on the newest live connection and then
// closes it.
//
// It is the same answer [Server.RefuseSubscribe] gives, at a moment the test
// picks rather than when a SUBSCRIBE arrives, and that difference is the whole
// reason it exists: an ERROR frame that arrives before the caller's first
// Fetch and one that arrives while a fetch is parked take different paths
// through the driver, and a test that could not choose would be green about
// whichever one it happened to get.
//
// The frame names no receipt. That is how both brokers refuse a frame that
// asked for none, and how both report the connection itself, so to a
// publication waiting on its RECEIPT it is another frame's answer.
func (s *Server) ErrorFrame(message string) error {
	s.mu.Lock()
	target := s.liveConnLocked()
	s.mu.Unlock()
	if target == nil {
		return errors.New("stompsrv: there is no live connection to send an ERROR frame on")
	}
	target.errorFrame(message)
	target.shutdown()
	return nil
}
