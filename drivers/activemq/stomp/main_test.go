package stomp_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds every test in this package to the rule that a driver starts
// no goroutine a Close does not stop.
//
// It matters more here than the one line suggests, because these tests run
// over a real socket and there are four separate goroutine populations to
// account for: the driver's own supervisor and one pump per subscription,
// go-stomp's `readLoop` and `processLoop` per connection, the in-process
// server's accept loop and one reader per accepted connection, and — the one
// that is easy to miss — the goroutine go-stomp's `waitWithTimeout` starts for
// every `Subscription.Unsubscribe`, which parks on a condition variable and is
// released only by the RECEIPT the server sends back. A requeue is an
// unsubscribe, so a server that did not answer that receipt would leak one
// goroutine per requeue and take 30 seconds doing it.
//
// # The two ignores, and why both are the library's leaks rather than this
// driver's
//
// go-stomp v3.1.5 leaks a goroutine, permanently, in two places. Both are
// named precisely rather than by a wildcard, so that every other goroutine in
// the process -- the driver's included -- is still checked.
//
// **`sendError`.** `Conn.Send` registers the publication's response channel in
// `processLoop`'s own map and then waits on it with a timeout; when the
// timeout wins, the caller abandons the channel and NOTHING deregisters it.
// The map entry outlives the call, and when the connection later ends,
// `processLoop`'s `sendError` writes an error frame to every registered
// channel with an unbuffered, unguarded `ch <- frame` -- which parks for ever
// on the abandoned one. The goroutine never returns, so its own
// `defer c.MustDisconnect()` never runs either. It is reachable from exactly
// one thing this driver does, and it is the thing the specification is about: a publication
// whose RECEIPT never arrives. Isolating each critical test showed it in
// precisely the two that hold a receipt
// (`TestCriticalPublicationWithNoReceiptIsAmbiguous` and
// `TestCriticalFailedDeadLetterPublishDoesNotAcknowledge`) and in none of the
// other five. There is no driver-side fix: by the time `Conn.Send` returns the
// timeout the entry is already stranded inside a map the library does not
// export, and dropping the connection only makes the parked send happen
// sooner.
//
// **`waitWithTimeout.func1`.** `Subscription.Unsubscribe` waits for the
// RECEIPT the library tagged its own frame with, through a helper that reads
// the state, starts a GOROUTINE to call `cond.Wait()`, and selects between
// that goroutine and a timer. A Broadcast that lands after the state read and
// before the goroutine reaches the notify list is MISSED -- so the wait runs
// its whole timeout AND the goroutine it started stays parked in `cond.Wait()`
// for the life of the process. Observed on two of about thirty tests in one
// `-race` run, because the RECEIPT crosses four goroutine hops and under eight
// parallel tests the closing goroutine can be descheduled for all four.
// `helpers_test.go`'s `closeSource` says what the driver-side mitigation is:
// `Backend.dial` sets `RcvReceiptTimeout` and `MsgSendTimeout` from
// `Config.ReceiptTimeout` and does not set
// `ConnOpt.UnsubscribeReceiptTimeout`, so the wait is thirty seconds whatever
// the configuration says.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("github.com/go-stomp/stomp/v3.sendError"),
		goleak.IgnoreAnyFunction("github.com/go-stomp/stomp/v3.waitWithTimeout.func1"),
	)
}
