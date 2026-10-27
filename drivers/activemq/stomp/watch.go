package stomp

import (
	"net"
	"sync/atomic"
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
	return &watchedConn{Conn: c, dead: dead}
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
}

// Read reports a read failure and passes it on unchanged.
func (w *watchedConn) Read(p []byte) (int, error) {
	n, err := w.Conn.Read(p)
	if err != nil {
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
func (w *watchedConn) Write(p []byte) (int, error) {
	n, err := w.Conn.Write(p)
	if err != nil {
		w.notify(err)
	}
	return n, err
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
	return w.Conn.Close()
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
