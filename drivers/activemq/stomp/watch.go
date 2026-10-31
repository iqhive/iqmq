package stomp

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
)

// watch wraps a socket so that its death is reported the instant it happens.
//
// # Why the driver needs this at all
//
// STOMP has no liveness frame a client may send unprompted: a SEND publishes,
// a SUBSCRIBE changes the broker's state, and go-stomp answers `Session`,
// `Server`, `Version` and `Stats` from memory, so there is nothing to poll.
// The honest signal is the one the SOCKET gives, and
// go-stomp's `readLoop` is the only reader of it -- so a read error observed
// HERE is the connection's death, seen before any operation meets it and
// whether or not one ever does.
//
// # Why go-stomp noticing is not enough
//
// Measured against go-stomp v3.1.5: when the broker drops the socket,
// `readLoop` closes its frame channel and `processLoop` answers every PENDING
// receipt with a synthetic "connection closed" ERROR frame on an UNBUFFERED
// channel. A publisher that has already given up on its receipt -- the specification's
// ambiguity window, which this driver bounds with `Config.ReceiptTimeout` --
// is not reading that channel, so the send parks for ever, `processLoop`'s
// `defer c.MustDisconnect()` never runs, the connection is never marked
// closed, and every later `Send` waits out `MsgSendTimeout` against a write
// channel nobody reads. The failure it produced was
// `TestCriticalSinkRecoversAfterTheConnectionDies`:
//
//	DEBUG err=activemq: publish topic=orders: SettlementUnknown: msg receipt
//	timeout gen=1 reconnects=0 accepted=1
//	DEBUG after 500ms gen=1 reconnects=0 accepted=1 health=ready err=<nil>
//
// -- a dead connection that the client still called Ready, a generation that
// never moved, and a dial loop that was never woken. Watching the socket makes
// all three of those impossible: the notice does not depend on the library
// noticing, on an operation being in flight, or on that operation's error
// being classifiable.
//
// # What a notice may do, which is very little
//
// It runs ON go-stomp's reader goroutine, so it must not block and it must not
// touch the connection. `Conn.Send` holds go-stomp's `closeMutex` for the
// WHOLE of its receipt wait and `MustDisconnect` wants the same mutex, so a
// notice that disconnected inline would block the reader until the in-flight
// publication had timed out -- and would delay the re-dial by exactly the
// receipt timeout it is trying to repair.
//
// It must also not act on a connection that has already been REPLACED. The
// operation that meets a death reports it too, and the two race: the loser
// would find the connection already re-dialled and disconnect one that was
// working. So the backend's notice only wakes the supervisor
// ([Backend.reportLostFor]) and the supervisor does the repair, and a source's
// notice names the connection it is about and does nothing when that is not
// the one the source still holds.
//
// A nil callback leaves the socket unwrapped, so a caller that does not want
// the notice pays nothing for it.
func watch(c net.Conn, dead func(error)) net.Conn {
	if dead == nil {
		return c
	}
	return &watchedConn{Conn: c, dead: dead, closed: make(chan struct{}), born: time.Now()}
}

// watchedConn is a socket with a death notice attached.
type watchedConn struct {
	net.Conn

	// fired is the notice's one-shot, and it is an atomic flag rather than a
	// sync.Once because the notice can close the socket and Close goes
	// through the flag too: a `sync.Once` holds its own mutex while
	// it runs the function, so a notice that reached `Close` -- which a
	// source's does, because dropping a dead connection is the repair --
	// re-entered the same Once and deadlocked the reader. Measured, as a
	// goroutine leak in every test that killed a socket: `readLoop` parked
	// in `watchedConn.Read` -> `notify` -> `socketDied` -> `dropConnection`
	// -> `MustDisconnect` -> `watchedConn.Close`.
	fired atomic.Bool
	dead  func(error)

	// readFailed is set once a read has failed, and closed is closed once
	// the socket is, by whoever closes it. Together they let a repair wait
	// for go-stomp to finish with a socket the broker dropped
	// ([watchedConn.settle]). closeOnce only closes the channel, so it is
	// safe from the re-entry that rules a sync.Once out for fired.
	readFailed atomic.Bool
	closeOnce  sync.Once
	closed     chan struct{}

	// born is what writing counts from, so that it reads the monotonic clock.
	born time.Time
	// writing is when the socket was last handed bytes it then took, in
	// nanoseconds after born plus one, and zero while no Write is in
	// progress. Write hands the socket at most writeChunk at a time and
	// restamps it for each piece, so it moves whenever the broker takes
	// bytes, however large the frame. go-stomp's process loop is the only
	// writer, so a stamp that stops moving is a socket the broker has stopped
	// taking bytes from ([watchedConn.stalled]).
	writing atomic.Int64
	// progress is closed, under pmu, when a Write starts, and is nil until a
	// frame waiting for room in go-stomp's write queue asks for it
	// ([watchedConn.progressed]). A Write starts right after the process
	// loop has taken a frame off the queue, which is the moment a slot frees,
	// so a waiter learns of room without polling.
	pmu      sync.Mutex
	progress chan struct{}

	// owner is the connection go-stomp built on this socket, and writers is
	// the backend's map from that connection to this socket
	// ([Backend.writerOf]), set by [Backend.dialOne] before the socket is
	// used. The socket removes its own entry when it closes, so a connection
	// the driver has let go does not stay in the map.
	owner   atomic.Pointer[gostomp.Conn]
	writers *sync.Map
}

// Read reports a read failure and passes it on unchanged.
func (w *watchedConn) Read(p []byte) (int, error) {
	n, err := w.Conn.Read(p)
	if err != nil {
		w.readFailed.Store(true)
		w.notify(err)
	}
	return n, err
}

// Write reports a write failure and passes it on unchanged.
//
// The read side is what sees a broker's disconnection, but a write failure is
// the same event seen from the other end -- a socket whose peer has gone
// answers `broken pipe` -- and the frame a driver is writing when it happens
// is exactly the one the specification is about.
//
// It also records the write's progress for the frames waiting for room in
// go-stomp's write queue: a wake-up as it starts, and a stamp each time the
// socket takes a piece of it.
//
// The pieces are what make the stamp mean "the broker took bytes". A socket
// Write returns only once all of p is in the kernel's buffer, so a frame of a
// megabyte on a link of 100 KB/s is one Write that stays open for ten seconds
// while the broker reads every byte of it. Timed whole, that read as a broker
// that had stopped, and a working publisher was refused. Measured: one
// sequential publisher of 1000 KiB frames behind a link reading at about
// 100 KB/s, with the default ReceiptTimeout and a quarter of it as the
// threshold, had 344 of 373 publications refused, where go-stomp refused
// none. The kernel still reports progress coarsely, which [stallAfter]
// allows for.
//
// Waiters are woken at the start only. A slot frees when the process loop
// takes a frame, and the first Write of that frame follows at once, so a wake
// at the end would only find the queue as full as it left it.
func (w *watchedConn) Write(p []byte) (int, error) {
	w.wake()
	n := 0
	var err error
	for {
		end := min(n+writeChunk, len(p))
		w.writing.Store(int64(time.Since(w.born)) + 1)
		var k int
		k, err = w.Conn.Write(p[n:end])
		n += k
		if err != nil || n >= len(p) {
			break
		}
	}
	w.writing.Store(0)
	if err != nil {
		w.notify(err)
	}
	return n, err
}

// writeChunk is the most of one Write that [watchedConn.Write] hands the
// socket at once. It is also the TLS record's largest plaintext, so a TLS
// socket writes the same records it would have. How often the stamp moves on
// a slow link is the kernel's to decide, not this size: [stallAfter] says why.
const writeChunk = 16 << 10

// progressed is a channel closed at the next Write that starts on this
// socket. A waiter takes it BEFORE it looks at go-stomp's queue, so that a
// slot freed after the look wakes it.
func (w *watchedConn) progressed() <-chan struct{} {
	w.pmu.Lock()
	defer w.pmu.Unlock()
	if w.progress == nil {
		w.progress = make(chan struct{})
	}
	return w.progress
}

// wake closes the progress channel, when a waiter has taken one. With nobody
// waiting it costs an uncontended lock and allocates nothing.
func (w *watchedConn) wake() {
	w.pmu.Lock()
	if w.progress != nil {
		close(w.progress)
		w.progress = nil
	}
	w.pmu.Unlock()
}

// stalled is how long the socket has taken none of the Write in progress, and
// zero when no Write is in progress.
func (w *watchedConn) stalled() time.Duration {
	began := w.writing.Load()
	if began == 0 {
		return 0
	}
	return time.Since(w.born) - time.Duration(began-1)
}

// own records conn as the connection built on this socket and enters it in
// writers, which was set before the socket was used. It runs after the
// handshake, so the socket may already have closed: then Close found no owner
// to remove, and the entry is removed here.
func (w *watchedConn) own(conn *gostomp.Conn) {
	w.owner.Store(conn)
	w.writers.Store(conn, w)
	select {
	case <-w.closed:
		w.writers.Delete(conn)
	default:
	}
}

// Close delivers the notice as [errSocketClosed], and then closes.
//
// A close is a death too, because the driver is not the only thing that
// closes this socket: go-stomp closes it ITSELF when a heart-beat it was owed
// does not arrive (`processLoop`'s read timeout, then its deferred
// `MustDisconnect`) and when the broker sends an ERROR frame. Close used to
// disarm the notice instead, on the reasoning that every close was this
// driver's own -- so a connection the library had shut was never reported:
// the backend's health stayed Ready, no re-dial was made, and the next
// publication met `ErrAlreadyClosed` instead.
//
// The parked read then fails as well, and the shared one-shot keeps that from
// being a second notice. Which closes are deaths is the CALLBACK's to decide,
// because only it knows whose connection this is, and each callback already
// carries the identity check that decides it:
//
//   - the backend's acts only while the connection is still the one installed
//     ([Backend.reportClosed]), and every close the driver makes of its own
//     accord uninstalls first -- a replaced connection, a torn-down one and
//     the client's own Close all clear `b.conn` before they disconnect, and a
//     dial that failed never installed one;
//   - a source's ignores it, because the notice runs on whatever goroutine
//     closed the socket -- for go-stomp, one holding the connection's
//     `closeMutex` -- and a source's repair disconnects, which takes that
//     mutex. The source does not need it: the library hands every
//     subscription a terminal message before it closes, and the pump that
//     reads it repairs the source.
func (w *watchedConn) Close() error {
	w.notify(errSocketClosed)
	w.closeOnce.Do(func() { close(w.closed) })
	if c := w.owner.Load(); c != nil && w.writers != nil {
		w.writers.Delete(c)
	}
	return w.Conn.Close()
}

// settle waits, for at most d, until the socket is closed, and only when a
// read on it has failed.
//
// A failed read is a socket go-stomp is already closing: its reader ends,
// its process loop hands every frame it had read to whoever waits on it, and
// its own disconnect closes the socket. A repair that disconnected first
// could beat a broker's ERROR frame to the publication it answers, because
// go-stomp v3.2.1 releases its close mutex during a receipt wait and gives the
// wait up when the connection closes: the publication then reported a closed
// connection instead of the broker's refusal. The bound covers a process loop
// that cannot finish, such as one behind a Send waiting out its write timeout.
// Any other death leaves the socket open, so there is nothing to wait for.
func (w *watchedConn) settle(d time.Duration) {
	if w == nil || !w.readFailed.Load() {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-w.closed:
	case <-t.C:
	}
}

// notify delivers the death notice at most once.
//
// Inline, because what a notice is allowed to do is bounded above: it is a
// channel send the backend never blocks on, or a source's own bookkeeping
// under its own mutex. A goroutine here would be one more thing to wait for
// at Close and would reorder the notice against the operation that met the
// same death, which is the race the identity checks exist to settle.
func (w *watchedConn) notify(err error) {
	if w.fired.CompareAndSwap(false, true) {
		w.dead(err)
	}
}
