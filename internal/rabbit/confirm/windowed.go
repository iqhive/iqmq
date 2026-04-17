package confirm

// windowed lets up to Config.Window publications be outstanding and blocks the
// publisher when the window is full.
//
// It is the expected default for a batch. The window amortises the round
// trip: a batch of a thousand messages through a window of sixty-four waits
// sixteen times rather than a thousand, and the broker's own cumulative
// confirms (`multiple=true`) mean it often waits far less than that.
//
// Two things are given up for it, and both are visible in this type's answers.
// A channel that closes can leave the whole window ambiguous rather than one
// publication ([Strategy.MaxAmbiguous]), which is the specification's "ambiguity accuracy"
// term. And a `mandatory` publication needs a correlation token, because
// `basic.return` carries no delivery tag and there are now up to Window
// candidates for it ([Strategy.NeedsCorrelation]).
type windowed struct{ strategy }

var _ Strategy = windowed{}

// OnSubmit carries on below the window and waits for it to drain at the
// window.
//
// The wait is at the window rather than one short of it, and the difference is
// the one a driver must not get wrong: the publication that filled the window
// is ALREADY outstanding when the wait begins, so waiting for "outstanding <
// Window" lets it complete. A strategy that waited one early would keep a slot
// permanently idle; one that never waited would be [Async] without the
// callback, and the pending set would grow without a bound.
func (w windowed) OnSubmit(outstanding int) Action {
	if outstanding >= w.cfg.Window {
		return WaitWindow
	}
	return Continue
}

// NeedsCorrelation is true: up to Window publications can be outstanding, and
// `basic.return` does not say which of them came back.
func (windowed) NeedsCorrelation() bool { return true }

// Blocks is true: the publisher waits when the window is full, and a batch
// waits for its own confirms before it returns.
func (windowed) Blocks() bool { return true }

// MaxAmbiguous is the window: every outstanding publication becomes one
// ambiguous outcome when the channel closes.
func (w windowed) MaxAmbiguous() int { return w.cfg.Window }
