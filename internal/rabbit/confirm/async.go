package confirm

// async never blocks the publishing goroutine: each outcome is reported to a
// callback.
//
// It is `mq.AsyncSink`'s shape and the only kind that can saturate a
// channel: the publisher's rate is bounded by what it can write rather than by
// the window times the round trip. The other half is why the window is
// still here — "there is no goroutine per message" and no unbounded pending
// set — so the window is BACKPRESSURE rather than a confirmation boundary:
// the publisher is held at the window until outcomes drain, and it never waits
// for a particular publication.
//
// What a caller gives up is any synchronous answer at all. A publication's
// outcome arrives out of order, on another goroutine, possibly after the
// caller has moved on — so this kind cannot serve `mq.Sink.Publish`, which has
// to return an error, and [Strategy.Blocks] is what tells a driver so.
type async struct{ strategy }

var _ Strategy = async{}

// OnSubmit defers below the window and applies backpressure at it.
//
// WaitWindow at the window is not a confirmation boundary: it is the bound on
// the pending set, and the publication that triggered it is still outstanding
// when the wait ends. Below the window nothing waits, which is the whole point
// of the kind.
func (a async) OnSubmit(outstanding int) Action {
	if outstanding >= a.cfg.Window {
		return WaitWindow
	}
	return Defer
}

// NeedsCorrelation is true, for [windowed]'s reason: many publications are
// outstanding and `basic.return` names none of them.
func (async) NeedsCorrelation() bool { return true }

// Blocks is false. It is the one kind for which it is, and a driver reads it
// to decide whether this strategy can serve a synchronous publish at all.
func (async) Blocks() bool { return false }

// MaxAmbiguous is the window, as for [windowed]: the bound on the pending set
// is the bound on what a close can leave unresolved.
func (a async) MaxAmbiguous() int { return a.cfg.Window }
