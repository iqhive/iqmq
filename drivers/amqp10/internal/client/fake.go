package client

import (
	"context"
	"errors"
	"sync"

	amqp "github.com/Azure/go-amqp"
)

// Compile-time proof that the fake is a complete implementation of the seam and
// not a stub with holes in it. A test that drives the driver through the fake is
// driving every method the driver can call.
var (
	_ Dialer   = (*Fake)(nil)
	_ Conn     = (*FakeConn)(nil)
	_ Session  = (*FakeSession)(nil)
	_ Sender   = (*FakeSender)(nil)
	_ Receiver = (*FakeReceiver)(nil)
	_ Receipt  = (*fakeReceipt)(nil)
)

// Fake is a scripted AMQP 1.0 peer.
//
// It is not a mock and it is not a broker. What it is is a peer whose every
// interesting answer is a value a test sets: which dial fails, which attach is
// refused, what disposition a transfer gets and whether it arrives at all, how
// much credit a receiver really honours, and when a link or a connection dies.
// Those are exactly the inputs of the four scenarios this driver has to get
// right, and every one of them is a race against a real broker and a value
// here.
//
// The zero Fake is a cooperative peer: every dial succeeds, every attach
// succeeds, every transfer is accepted, and a receiver delivers what was queued
// for it as fast as its credit allows. A test scripts only the thing it is
// about.
//
// Every method is safe for concurrent use, because the driver calls them from
// its own goroutines: the dial loop, the asynchronous publication's settlement
// goroutine and the caller's.
type Fake struct {
	mu sync.Mutex

	// dialErrs is consumed one per Dial. Running off the end is a successful
	// dial, so a test that wants "fail twice then work" writes two entries.
	dialErrs []error
	dials    int
	conns    []*FakeConn

	// attachErr refuses an attach by address, for either direction. It is by
	// address because that is what a profile decides and therefore what a
	// naming test wants to assert on.
	attachErr map[string]error

	// props is the peer's connection-properties map, which is how a broker
	// says what version it is and therefore what a version gate reads.
	props map[string]any

	// sendPlan is consumed one per transfer across every sender of every
	// connection, so that a test can script a sequence without reaching for
	// the link object.
	sendPlan []SendPlan
	sends    int
	// hold parks every dial until it is closed. Nil means dials proceed.
	hold chan struct{}
}

// SendPlan is what the peer does with one transfer.
//
// The zero value accepts it, which is the answer the happy path wants. Hold is
// the interesting one: a transfer whose disposition never arrives is the
// Ambiguous publication, and it is the outcome a driver is most likely
// to report as a definite failure.
type SendPlan struct {
	// Err fails the transfer itself. The frame is treated as not having been
	// written, which is what a validation refusal looks like.
	Err error
	// State is the disposition the peer answers with. Nil means accepted.
	State amqp.DeliveryState
	// Hold withholds the disposition: the receipt does not resolve until the
	// test resolves it, the link dies or the context is done.
	Hold bool
}

// NewFake returns a scripted peer with the given connection properties, which
// is what a broker-version gate reads.
func NewFake(props map[string]any) *Fake { return &Fake{props: props} }

// FailDials makes the next len(errs) dials fail with the errors given, in
// order. A nil entry is a successful dial.
func (f *Fake) FailDials(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dialErrs = append(f.dialErrs, errs...)
}

// HoldDials parks every dial from now until the returned function is called,
// and returns that function. Calling it twice is harmless.
//
// It is the dial-side twin of [SendPlan.Hold], and it exists because a test
// that wants to observe a client while its dial loop is RUNNING was otherwise
// racing it. `TestReconnectHealthFollowsTheLoop` polled for "the health has
// left ready" after killing the connection, and with `ReconnectBase` at a
// millisecond and two refused dials the whole not-ready window is about three
// milliseconds wide: a poller descheduled across it sees ready before the
// kill is noticed and ready again after the loop has finished, and reports
// `timed out after 5s waiting for the health to leave ready` -- a failure
// whose budget had nothing to do with it. Holding the dial makes "the client
// is reconnecting" a state a test can stand in rather than a window it might
// miss, which is what `rabbitfake.DeclareGate` does for a topology replay.
func (f *Fake) HoldDials() func() {
	f.mu.Lock()
	if f.hold == nil {
		f.hold = make(chan struct{})
	}
	hold := f.hold
	f.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			if f.hold == hold {
				f.hold = nil
			}
			f.mu.Unlock()
			close(hold)
		})
	}
}

// RefuseAttach refuses every attach to addr with err, in either direction.
func (f *Fake) RefuseAttach(addr string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attachErr == nil {
		f.attachErr = map[string]error{}
	}
	f.attachErr[addr] = err
}

// AllowAttach undoes [Fake.RefuseAttach], which is what a reconnection test
// needs: the attach fails while the broker is down and works when it is back.
func (f *Fake) AllowAttach(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.attachErr, addr)
}

// PlanSends appends the peer's answers to its POSITIONAL send script: entry i
// answers the peer's (i+1)th transfer ever, across every sender, so an entry
// appended at a position the transfers have already passed answers nothing.
// A transfer attempt reads its entry once its context has been found live --
// an attempt whose context is already done returns first and reads nothing --
// and a live attempt the link then refuses has still consumed one, because the
// plan is read before the link's own state. Running off the end accepts the
// transfer.
// `drivers/amqp10.Peer.HoldSends` says what that costs a test that publishes
// before it plans.
func (f *Fake) PlanSends(plans ...SendPlan) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendPlan = append(f.sendPlan, plans...)
}

// Dials is how many dials the peer has answered, which is what a backoff test
// counts.
func (f *Fake) Dials() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials
}

// Conns is every connection the peer has handed out, oldest first.
func (f *Fake) Conns() []*FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*FakeConn(nil), f.conns...)
}

// LastConn is the most recent connection, or nil.
func (f *Fake) LastConn() *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.conns) == 0 {
		return nil
	}
	return f.conns[len(f.conns)-1]
}

// Dial answers one dial from the script.
func (f *Fake) Dial(ctx context.Context, _ string, _ *amqp.ConnOptions) (Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The hold is read under the lock and waited on outside it, so a test
	// can release it from another goroutine without deadlocking against a
	// dial that is already parked.
	f.mu.Lock()
	hold := f.hold
	f.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	f.dials++
	var err error
	if f.dials <= len(f.dialErrs) {
		err = f.dialErrs[f.dials-1]
	}
	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	c := &FakeConn{peer: f, done: make(chan struct{}), props: f.props}
	f.conns = append(f.conns, c)
	f.mu.Unlock()
	return c, nil
}

// nextSendPlan consumes one entry of the transfer script.
func (f *Fake) nextSendPlan() SendPlan {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sends++
	if f.sends <= len(f.sendPlan) {
		return f.sendPlan[f.sends-1]
	}
	return SendPlan{}
}

// attachRefusal is the scripted refusal for an address, or nil.
func (f *Fake) attachRefusal(addr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attachErr[addr]
}

// FakeConn is one connection of a [Fake].
type FakeConn struct {
	peer  *Fake
	props map[string]any

	mu       sync.Mutex
	sessions []*FakeSession
	done     chan struct{}
	err      error
	closed   bool
}

// NewSession begins a session, or fails when the connection is dead.
func (c *FakeConn) NewSession(ctx context.Context, _ *amqp.SessionOptions) (Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	if c.closed {
		return nil, &amqp.ConnError{}
	}
	s := &FakeSession{conn: c}
	c.sessions = append(c.sessions, s)
	return s, nil
}

// Properties is the peer's connection-properties map.
func (c *FakeConn) Properties() map[string]any { return c.props }

// Done is closed when the connection has died or been closed.
func (c *FakeConn) Done() <-chan struct{} { return c.done }

// Err is why the connection died.
func (c *FakeConn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close closes the connection.
func (c *FakeConn) Close() error {
	c.Kill(&amqp.ConnError{})
	return nil
}

// Kill makes the connection die with err, which is what a broker restart looks
// like from a client: Done closes, every link on it starts failing, and the
// driver's supervisor is what notices.
func (c *FakeConn) Kill(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if err == nil {
		err = &amqp.ConnError{}
	}
	c.err = err
	sessions := append([]*FakeSession(nil), c.sessions...)
	close(c.done)
	c.mu.Unlock()
	for _, s := range sessions {
		s.kill(err)
	}
}

// Sessions is every session on this connection.
func (c *FakeConn) Sessions() []*FakeSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*FakeSession(nil), c.sessions...)
}

// Senders is every sending link on this connection, oldest first.
func (c *FakeConn) Senders() []*FakeSender {
	var out []*FakeSender
	for _, s := range c.Sessions() {
		out = append(out, s.Senders()...)
	}
	return out
}

// Receivers is every receiving link on this connection, oldest first.
func (c *FakeConn) Receivers() []*FakeReceiver {
	var out []*FakeReceiver
	for _, s := range c.Sessions() {
		out = append(out, s.Receivers()...)
	}
	return out
}

// FakeSession is one session of a [FakeConn].
type FakeSession struct {
	conn *FakeConn

	mu        sync.Mutex
	senders   []*FakeSender
	receivers []*FakeReceiver
	err       error
}

// NewSender attaches a sending link, or fails when the address is refused.
func (s *FakeSession) NewSender(ctx context.Context, target string, opts *amqp.SenderOptions) (Sender, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.conn.peer.attachRefusal(target); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	snd := &FakeSender{sess: s, addr: target}
	if opts != nil {
		snd.targetCaps = opts.TargetCapabilities
		snd.settled = opts.SettlementMode != nil && *opts.SettlementMode == amqp.SenderSettleModeSettled
	}
	s.senders = append(s.senders, snd)
	return snd, nil
}

// NewReceiver attaches a receiving link, or fails when the address is refused.
func (s *FakeSession) NewReceiver(ctx context.Context, source string, opts *amqp.ReceiverOptions) (Receiver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.conn.peer.attachRefusal(source); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	rcv := &FakeReceiver{sess: s, addr: source, wake: make(chan struct{}, 1)}
	if opts != nil && opts.SettlementMode != nil {
		rcv.second = *opts.SettlementMode == amqp.ReceiverSettleModeSecond
	}
	if opts != nil && opts.Credit > 0 {
		// Automatic credit: the library would have issued this much on
		// attach. The driver does not use it, and honouring it here is what
		// makes a test that forgets to switch to manual credit visible.
		rcv.credit = uint32(opts.Credit)
	}
	s.receivers = append(s.receivers, rcv)
	return rcv, nil
}

// Close ends the session and detaches its links.
func (s *FakeSession) Close(context.Context) error {
	s.kill(&amqp.SessionError{})
	return nil
}

// kill detaches every link on the session.
func (s *FakeSession) kill(err error) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	s.err = err
	senders := append([]*FakeSender(nil), s.senders...)
	receivers := append([]*FakeReceiver(nil), s.receivers...)
	s.mu.Unlock()
	for _, snd := range senders {
		snd.Detach(err)
	}
	for _, rcv := range receivers {
		rcv.Detach(err)
	}
}

// Ended reports whether the session has been ended, by a Close or by the
// connection under it dying.
func (s *FakeSession) Ended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err != nil
}

// Senders is every sending link on this session.
func (s *FakeSession) Senders() []*FakeSender {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*FakeSender(nil), s.senders...)
}

// Receivers is every receiving link on this session.
func (s *FakeSession) Receivers() []*FakeReceiver {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*FakeReceiver(nil), s.receivers...)
}

// FakeSender is one sending link of a [FakeSession].
type FakeSender struct {
	sess *FakeSession
	addr string
	// targetCaps is what the attach asked of the target node, which is how a
	// test sees the routing type a link asked Artemis for.
	targetCaps []string
	// settled is a link attached SenderSettleModeSettled, on which go-amqp
	// refuses SendWithReceipt.
	settled bool

	mu       sync.Mutex
	sent     []*amqp.Message
	held     []*fakeReceipt
	maxSize  uint64
	err      error
	detached bool
}

// SetMaxMessageSize sets the ceiling the peer declared on attach.
func (s *FakeSender) SetMaxMessageSize(n uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxSize = n
}

// Sent is every message this link transferred, in order. The messages are the
// driver's own, so a mapping test reads them here.
func (s *FakeSender) Sent() []*amqp.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*amqp.Message(nil), s.sent...)
}

// Held is how many transfers are waiting for a disposition that has not
// arrived.
func (s *FakeSender) Held() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.held)
}

// ResolveHeld resolves the nth held transfer, counting from zero in the order
// they were sent.
//
// It is what an out-of-order-disposition test uses: hold three transfers,
// resolve the third, then the first, and the driver's publication outcomes must
// follow the transfers rather than the dispositions.
func (s *FakeSender) ResolveHeld(n int, state amqp.DeliveryState) bool {
	s.mu.Lock()
	if n < 0 || n >= len(s.held) {
		s.mu.Unlock()
		return false
	}
	r := s.held[n]
	s.held = append(s.held[:n:n], s.held[n+1:]...)
	s.mu.Unlock()
	r.resolve(state, nil)
	return true
}

// Detach detaches the link with err and fails every held transfer with it,
// which is what a link the broker took away looks like.
func (s *FakeSender) Detach(err error) {
	s.mu.Lock()
	if s.detached {
		s.mu.Unlock()
		return
	}
	s.detached = true
	if err == nil {
		err = &amqp.LinkError{}
	}
	s.err = err
	held := s.held
	s.held = nil
	s.mu.Unlock()
	for _, r := range held {
		r.resolve(nil, err)
	}
}

// Detached reports whether the link has been detached.
func (s *FakeSender) Detached() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.detached
}

// Send transfers a message and waits for its disposition.
func (s *FakeSender) Send(ctx context.Context, m *amqp.Message, opts *amqp.SendOptions) error {
	r, err := s.transfer(ctx, m)
	if err != nil {
		return err
	}
	if opts != nil && opts.Settled {
		// A settled transfer has no disposition to wait for, which is what
		// makes ConfirmNone cost no round trip.
		return nil
	}
	state, err := r.Wait(ctx)
	if err != nil {
		return err
	}
	if rej, ok := state.(*amqp.StateRejected); ok {
		if rej.Error != nil {
			return rej.Error
		}
		return errRejectedNoReason
	}
	return nil
}

// SendWithReceipt transfers a message and defers its disposition.
//
// On a link attached settled it refuses before transferring anything, as
// go-amqp does: a fake that accepted it hid a dead-letter link that could
// never carry its copy.
func (s *FakeSender) SendWithReceipt(ctx context.Context, m *amqp.Message, _ *amqp.SendWithReceiptOptions) (Receipt, error) {
	if s.settled {
		return nil, errReceiptOnSettledLink
	}
	return s.transfer(ctx, m)
}

// transfer is the one place a message leaves for the peer.
func (s *FakeSender) transfer(ctx context.Context, m *amqp.Message) (*fakeReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plan := s.sess.conn.peer.nextSendPlan()
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return nil, s.err
	}
	if s.maxSize > 0 && bodyLen(m) > s.maxSize {
		// A definite refusal the peer makes before the frame is written,
		// which is the half of the ambiguity distinction that must stay
		// definite.
		s.mu.Unlock()
		return nil, &amqp.Error{Condition: amqp.ErrCondMessageSizeExceeded, Description: "message size exceeds the link's maximum"}
	}
	if plan.Err != nil {
		s.mu.Unlock()
		return nil, plan.Err
	}
	s.sent = append(s.sent, m)
	r := &fakeReceipt{tag: m.DeliveryTag, done: make(chan struct{})}
	if plan.Hold {
		s.held = append(s.held, r)
		s.mu.Unlock()
		return r, nil
	}
	s.mu.Unlock()
	state := plan.State
	if state == nil {
		state = &amqp.StateAccepted{}
	}
	r.resolve(state, nil)
	return r, nil
}

// MaxMessageSize is the ceiling the peer declared, or zero.
func (s *FakeSender) MaxMessageSize() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxSize
}

// Address is the link's target.
func (s *FakeSender) Address() string { return s.addr }

// TargetCapabilities is what the link's attach asked of the target node.
func (s *FakeSender) TargetCapabilities() []string { return s.targetCaps }

// Close detaches the link.
func (s *FakeSender) Close(context.Context) error {
	s.Detach(&amqp.LinkError{})
	return nil
}

// bodyLen is the transferred message's payload length, which is all the fake
// needs in order to enforce a size ceiling.
func bodyLen(m *amqp.Message) uint64 {
	var n uint64
	for _, d := range m.Data {
		n += uint64(len(d))
	}
	return n
}

// fakeReceipt is one transfer waiting for its disposition.
type fakeReceipt struct {
	tag  []byte
	done chan struct{}

	mu    sync.Mutex
	state amqp.DeliveryState
	err   error
	set   bool
}

// resolve completes the receipt once.
func (r *fakeReceipt) resolve(state amqp.DeliveryState, err error) {
	r.mu.Lock()
	if r.set {
		r.mu.Unlock()
		return
	}
	r.set, r.state, r.err = true, state, err
	r.mu.Unlock()
	close(r.done)
}

// Wait blocks until the disposition arrives, or the context is done.
func (r *fakeReceipt) Wait(ctx context.Context) (amqp.DeliveryState, error) {
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.state, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// DeliveryTag correlates the receipt with its transfer.
func (r *fakeReceipt) DeliveryTag() []byte { return r.tag }

// FakeReceiver is one receiving link of a [FakeSession].
//
// Its credit accounting is the peer's half of the flow-control contract: a
// message is handed over only when there is credit for it, and the credit is
// consumed by the transfer. A driver whose accounting is wrong therefore stalls
// here rather than in a broker, which is what makes the credit test
// deterministic.
type FakeReceiver struct {
	sess *FakeSession
	addr string
	// second is a link attached ReceiverSettleModeSecond, on which go-amqp
	// writes a disposition and then waits for the peer's confirmation.
	second bool

	mu       sync.Mutex
	queued   []*amqp.Message
	ready    []*amqp.Message
	disposed []Dispositioned
	credit   uint32
	granted  uint32
	drains   int
	err      error
	detached bool
	// refuse is what every Dispose answers while it is non-nil. See
	// [FakeReceiver.RefuseDispositions].
	refuse error

	// hold parks every Dispose while it is non-nil, and parked counts the
	// ones parked on it. See [FakeReceiver.HoldDispositions].
	hold   chan struct{}
	parked int

	// wake carries "something changed" to a parked Receive. It is buffered
	// and written non-blockingly, so a peer that delivers a thousand messages
	// does not block on a driver that is not fetching.
	wake chan struct{}
}

// Dispositioned is one settlement the peer received.
type Dispositioned struct {
	// Tag is the delivery tag of the message settled.
	Tag string
	// Disposition is what the driver asked for.
	Disposition Disposition
}

// Deliver queues messages for the link. They are handed over as credit allows.
func (r *FakeReceiver) Deliver(msgs ...*amqp.Message) {
	r.mu.Lock()
	r.queued = append(r.queued, msgs...)
	r.pumpLocked()
	r.mu.Unlock()
	r.signal()
}

// pumpLocked moves as many queued messages into the ready list as there is
// credit for.
func (r *FakeReceiver) pumpLocked() {
	for r.credit > 0 && len(r.queued) > 0 {
		r.ready = append(r.ready, r.queued[0])
		r.queued = r.queued[1:]
		r.credit--
	}
}

// signal wakes a parked Receive.
func (r *FakeReceiver) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Receive blocks until a message is available, the context is done or the link
// dies.
func (r *FakeReceiver) Receive(ctx context.Context, _ *amqp.ReceiveOptions) (*amqp.Message, error) {
	for {
		if m := r.Prefetched(); m != nil {
			return m, nil
		}
		r.mu.Lock()
		err := r.err
		r.mu.Unlock()
		if err != nil {
			return nil, err
		}
		select {
		case <-r.wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Prefetched returns a message the link already holds, or nil.
func (r *FakeReceiver) Prefetched() *amqp.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ready) == 0 {
		return nil
	}
	m := r.ready[0]
	r.ready = r.ready[1:]
	return m
}

// IssueCredit adds link credit and hands over whatever it now allows.
//
// On a detached link it reports nil and the peer is granted nothing, which is
// what go-amqp v1.7.0 does: its IssueCredit only queues the credit for the
// link's mux, and a mux that has stopped never sends the flow frame. A fake
// that failed here instead let a driver learn of the detach from a call that
// in production succeeds.
func (r *FakeReceiver) IssueCredit(n uint32) error {
	r.mu.Lock()
	if r.err != nil {
		r.mu.Unlock()
		return nil
	}
	r.credit += n
	r.granted += n
	r.pumpLocked()
	r.mu.Unlock()
	r.signal()
	return nil
}

// DrainCredit returns every outstanding credit, which is what a drain means: the
// link is left with none.
func (r *FakeReceiver) DrainCredit(ctx context.Context, _ *amqp.DrainCreditOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.err != nil {
		err := r.err
		r.mu.Unlock()
		return err
	}
	r.credit = 0
	r.drains++
	r.mu.Unlock()
	r.signal()
	return nil
}

// Credit is the link credit the peer still holds, which is what a credit test
// asserts on.
func (r *FakeReceiver) Credit() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.credit
}

// Granted is the total credit the driver has ever issued.
func (r *FakeReceiver) Granted() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.granted
}

// Drains is how many times the driver drained the link.
func (r *FakeReceiver) Drains() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drains
}

// Queued is how many messages the peer holds that credit has not released.
func (r *FakeReceiver) Queued() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.queued) + len(r.ready)
}

// Dispose records one settlement.
//
// It fails when the link has been detached, which is what a real receiver does:
// go-amqp routes a disposition through the message's own link, so a delivery
// from a link that is gone has nowhere to be settled. The driver must refuse
// such a settlement BEFORE it gets here — this is the backstop, not the guard.
func (r *FakeReceiver) Dispose(ctx context.Context, m *amqp.Message, d Disposition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if hold := r.hold; hold != nil {
		r.parked++
		r.mu.Unlock()
		select {
		case <-hold:
		case <-ctx.Done():
			// What go-amqp does under `second` when the confirmation does
			// not arrive in time: the frame has left, and the call reports
			// the context's error rather than the settlement. Under `first`
			// there is no confirmation, and the context's error comes back
			// only when the connection writer found it ended before it
			// wrote the frame, so the peer received nothing.
			r.mu.Lock()
			r.parked--
			if r.second {
				r.disposed = append(r.disposed, Dispositioned{Tag: string(m.DeliveryTag), Disposition: d})
			}
			r.mu.Unlock()
			return ctx.Err()
		}
		r.mu.Lock()
		r.parked--
	}
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.disposed = append(r.disposed, Dispositioned{Tag: string(m.DeliveryTag), Disposition: d})
	return r.refuse
}

// RefuseDispositions makes every Dispose from now on answer err while the link
// stays attached, and a nil err stops it. The disposition is still recorded,
// because the peer received it.
//
// It is the peer answering a disposition with an error of its own under
// `second`, which go-amqp returns as the peer's bare *amqp.Error. That is a
// definite refusal, unlike a detach, whose condition arrives wrapped in a
// *amqp.LinkError and says why the link went.
func (r *FakeReceiver) RefuseDispositions(err error) {
	r.mu.Lock()
	r.refuse = err
	r.mu.Unlock()
}

// HoldDispositions parks every Dispose from now until the returned function is
// called, and returns that function. Calling it twice is harmless.
//
// It is what puts a settlement in flight while something else happens to the
// link -- the driver has passed its own guard and handed the disposition to
// the library, and the answer has not come back -- which is a window a test
// otherwise races.
func (r *FakeReceiver) HoldDispositions() func() {
	r.mu.Lock()
	if r.hold == nil {
		r.hold = make(chan struct{})
	}
	hold := r.hold
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if r.hold == hold {
				r.hold = nil
			}
			r.mu.Unlock()
			close(hold)
		})
	}
}

// ParkedDispositions is how many Dispose calls are parked by
// [FakeReceiver.HoldDispositions].
func (r *FakeReceiver) ParkedDispositions() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.parked
}

// Dispositions is every settlement the peer received, in order.
func (r *FakeReceiver) Dispositions() []Dispositioned {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Dispositioned(nil), r.disposed...)
}

// Detach detaches the link with err, waking anything parked in Receive.
func (r *FakeReceiver) Detach(err error) {
	r.mu.Lock()
	if r.detached {
		r.mu.Unlock()
		return
	}
	r.detached = true
	if err == nil {
		err = &amqp.LinkError{}
	}
	r.err = err
	r.mu.Unlock()
	r.signal()
}

// Detached reports whether the link has been detached.
func (r *FakeReceiver) Detached() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.detached
}

// Address is the link's source.
func (r *FakeReceiver) Address() string { return r.addr }

// Close detaches the link.
func (r *FakeReceiver) Close(context.Context) error {
	r.Detach(&amqp.LinkError{})
	return nil
}

// The fake's own refusals. They are ordinary errors rather than iqmq codes,
// because they stand for things a CLIENT LIBRARY says and the driver's error
// table is what turns those into the model.
var (
	errRejectedNoReason = errors.New("amqp: the peer rejected the message without specifying an error")
	errUnknownOutcome   = errors.New("amqp: the disposition names no terminal outcome")
	// errReceiptOnSettledLink is go-amqp's own words for the refusal.
	errReceiptOnSettledLink = errors.New("SendWithReceipt cannot be called from Senders configured with SenderSettleModeSettled")
)
