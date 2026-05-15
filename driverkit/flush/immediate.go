package flush

import "time"

// immediate is [Immediate]: send every settlement on its own.
//
// It is the reference implementation and the default, and it is the only kind
// that is right for a broker whose settlement is cheap. The other three trade
// a settlement's latency and a window in which a crash loses an
// acknowledgement the application already saw succeed for a round trip, and
// that trade is only worth making where the round trip is priced.
//
// It needs no ticker, and it holds nothing: a Close finds an empty queue, so
// the specification's "shutdown correctness" is trivially satisfied. That is the baseline
// every other kind's shutdown behaviour is measured against.
type immediate struct{ policy }

// Submitted always sends: the batch is the one settlement that was queued.
func (immediate) Submitted(int) bool { return true }

// Ticked never sends, because nothing is ever waiting.
func (immediate) Ticked(int) bool { return false }

// Ticker reports that this kind needs no timer.
func (immediate) Ticker() (time.Duration, bool) { return 0, false }

// MaxHeld is zero: nothing is ever waiting for a batch.
func (immediate) MaxHeld() int { return 0 }
