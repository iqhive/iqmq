package confirm

// perMessage keeps one publication outstanding and waits for its confirm.
//
// It is the reference kind and the expected default for
// `Publish`, and the reason is the confirmation boundary rather than the
// speed: ConfirmAccepted says a publication waits for "the strongest
// ordinary acknowledgement the profile offers", and with one publication
// outstanding the `basic.ack` that resolves the call is unambiguously this
// call's. Everything a caller reads from a successful Publish is then true of
// the message they just published and of nothing else.
//
// Its cost is the whole of it: one round trip per message, so a publisher's
// throughput is one message per round trip however much the broker could take.
// That is what the other two kinds buy, and what the criterion measures.
type perMessage struct{ strategy }

var _ Strategy = perMessage{}

// OnSubmit always waits: the window is one, so there is nothing to accumulate.
func (perMessage) OnSubmit(int) Action { return WaitTag }

// NeedsCorrelation is false. With one publication outstanding a
// `basic.return` has exactly one candidate, so the frame's missing delivery
// tag costs nothing and a `mandatory` publication needs no extra header.
func (perMessage) NeedsCorrelation() bool { return false }

// Blocks is true: the publishing goroutine waits for every confirm.
func (perMessage) Blocks() bool { return true }

// MaxAmbiguous is one. A channel that closes can have at most this
// strategy's single outstanding publication in flight, so the worst case of the ambiguity is one message per channel — which is the strongest
// answer any of the three kinds gives.
func (perMessage) MaxAmbiguous() int { return 1 }
