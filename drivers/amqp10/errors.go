package amqp10

import (
	"errors"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// newErrorMapper builds this core rules, with the profile's own tried
// first.
//
// # What the rules are driven by
//
// Every predicate below matches a value go-amqp really produces: an
// [github.com/Azure/go-amqp.Error] carrying one of the protocol's own condition
// symbols, or one of the three transport wrappers
// ([github.com/Azure/go-amqp.ConnError],
// [github.com/Azure/go-amqp.SessionError],
// [github.com/Azure/go-amqp.LinkError]). Not one of them matches a string.
//
// # Order
//
// The condition rules come FIRST and the transport rules last, and that order is
// load-bearing. A peer that refuses an oversized message detaches the link with
// `amqp:link:message-size-exceeded`, and go-amqp reports that as a *LinkError
// whose RemoteErr carries the condition — so a table whose transport rule came
// first would classify a definite, never-retryable refusal as a retryable,
// AMBIGUOUS transport failure, and the caller would republish a message the
// broker will refuse again for ever.
//
// The order decides the CODE. On a publish whose transfer had already left,
// the sink makes the failure ambiguous whatever the rule says
// ([Sink.fail]): the detach ends every transfer outstanding on the link, and
// the condition does not say which one it was about.
//
// # Retryability and ambiguity
//
// Rules are per operation because retryability is (the specification forbids inferring it
// from the native error type alone). The one distinction this protocol makes
// sharper than most is on the publish path: once a transfer has been written,
// everything that goes wrong is a failure to HEAR the disposition rather than a
// failure to write the transfer, so a link or connection failure on a publish is
// ambiguous and a peer's `rejected` disposition is definite. The transport rules
// therefore carry Ambiguous on OpPublish and not on OpFetch, and
// [github.com/iqhive/iqmq/driverkit.ErrorMapper.MapNet] ORs in the rest from the
// caller's own sentAlready.
//
// The settlement operations are the mirror image and the answer depends on the
// receiver settle mode, which is why this function takes it. Under
// ReceiverSettleFirst a disposition is settled the moment it is written: a write
// that failed means the broker never got it, the delivery is still the broker's
// and it will be redelivered — a LOST LEASE, which tells the caller the message
// is coming back. Under ReceiverSettleSecond the sender confirms the
// settlement, so a failure after the write may be a settlement the broker took
// and a confirmation that was lost — an UNKNOWN outcome, which the specification makes
// sticky. Reporting the first as unknown would strand a slot for ever on every
// reconnection; reporting the second as a lost lease would tell a caller a
// message is coming back when it is not.
func newErrorMapper(p Profile, recvSettle ReceiverSettleMode) *driverkit.ErrorMapper {
	// The protocol's own conditions, most specific first.
	auth := []driverkit.Rule{
		{Match: matchCond(amqp.ErrCondUnauthorizedAccess), Code: mq.CodeUnauthorized},
	}
	topology := []driverkit.Rule{
		{Match: matchCond(amqp.ErrCondNotFound), Code: mq.CodeNotFound},
		// A node that was deleted under us. It is NotFound rather than
		// Unavailable and it is retryable: an operator who recreates the
		// address makes the next attempt work, and retrying for ever against
		// a node nobody created is what the NotFound half prevents.
		{Match: matchCond(amqp.ErrCondResourceDeleted), Code: mq.CodeNotFound, Retryable: true},
		// The peer refuses the operation on policy grounds — an attach to an
		// address whose type does not match, a queue that exists with other
		// settings. Conflict is the state-conflict code and the one a
		// caller acts on by changing the request rather than repeating it.
		{Match: matchCond(amqp.ErrCondPreconditionFailed), Code: mq.CodeConflict},
		{Match: matchCond(amqp.ErrCondResourceLocked), Code: mq.CodeConflict, Retryable: true},
		{Match: matchCond(amqp.ErrCondNotAllowed), Code: mq.CodeForbidden},
		{Match: matchCond(amqp.ErrCondNotImplemented), Code: mq.CodeUnsupported},
		{Match: matchCond(amqp.ErrCondIllegalState), Code: mq.CodeConflict, Retryable: true},
	}
	limits := []driverkit.Rule{
		// The size refusal has to come before every transport rule: the peer
		// reports it by DETACHING the link, so it arrives as a *LinkError and
		// a table that read the wrapper first would call a definite refusal a
		// retryable transport failure.
		{Match: matchCond(amqp.ErrCondMessageSizeExceeded), Code: mq.CodeMessageTooLarge},
		{Match: matchCond(amqp.ErrCondResourceLimitExceeded), Code: mq.CodeResourceExhausted, Retryable: true},
		{Match: matchCond(amqp.ErrCondTransferLimitExceeded), Code: mq.CodeResourceExhausted, Retryable: true},
		{Match: matchCond(amqp.ErrCondWindowViolation), Code: mq.CodeResourceExhausted, Retryable: true},
	}
	shape := []driverkit.Rule{
		{Match: matchCond(amqp.ErrCondDecodeError), Code: mq.CodeCorruptMessage},
		{Match: matchCond(amqp.ErrCondInvalidField), Code: mq.CodeInvalidArgument},
	}
	// A link somebody else stole is the one condition that means "your
	// deliveries are not yours any more", whatever the operation and whatever
	// the settle mode: ErrLeaseLost.
	stolen := []driverkit.Rule{
		{Match: matchCond(amqp.ErrCondStolen), Code: mq.CodeLeaseLost},
	}

	// The peer does not speak AMQP 1.0 AT ALL, and it is the one rule in this
	// table that has to match on a STRING.
	//
	// go-amqp reads the protocol header the peer sends back and reports
	// `unexpected protocol version 0.9.1` for a broker that answered with
	// another protocol's header — which is exactly what RabbitMQ 3.13 does,
	// because its AMQP 1.0 support is a plugin that image does not load. There
	// is no `*amqp.Error` and no transport wrapper to match: the handshake
	// failed before either could exist, and the library's own error is
	// unexported and unwrapped.
	//
	// The code matters more than the match. Without this rule the failure is
	// CodeUnknown, so a caller cannot tell "this broker is too old for AMQP
	// 1.0" from "the dial failed" — and a version GATE cannot help, because a
	// peer that cannot speak the protocol never gets as far as telling you its
	// version. `conformance/matrix/rabbitmq_test.go` is what found it, against
	// rabbitmq:3.13-management-alpine, and it is the one direction of the specification's
	// "never silently substituted" that a profile's CheckBroker cannot reach.
	wrongProtocol := []driverkit.Rule{
		{Match: driverkit.MatchContains("unexpected protocol version"), Code: mq.CodeUnsupported},
	}

	// The conditions that say the TRANSPORT died rather than that the peer
	// answered. Every one of them is retryable, because a reconnection is what
	// answers it — and every one of them is AMBIGUOUS on a publish, which is
	// what the two variants below are for.
	transport := []driverkit.Rule{
		{Match: matchCond(amqp.ErrCondDetachForced), Code: mq.CodeUnavailable, Retryable: true},
		{Match: matchCond(amqp.ErrCondConnectionForced), Code: mq.CodeUnavailable, Retryable: true},
		{Match: matchCond(amqp.ErrCondLinkRedirect, amqp.ErrCondConnectionRedirect), Code: mq.CodeUnavailable, Retryable: true},
		{Match: matchCond(amqp.ErrCondErrantLink, amqp.ErrCondHandleInUse, amqp.ErrCondUnattachedHandle), Code: mq.CodeUnavailable, Retryable: true},
		{Match: matchCond(amqp.ErrCondInternalError), Code: mq.CodeUnavailable, Retryable: true},
		{Match: matchCond(amqp.ErrCondFrameSizeTooSmall, amqp.ErrCondFramingError), Code: mq.CodeInvalidConfig},
	}

	// The peer's own non-accepting outcomes. They come after the conditions,
	// so that a `rejected` carrying a condition is classified by the
	// condition, and before the transport rules, so that they are not read as
	// a link that died.
	outcomes := []driverkit.Rule{
		{Match: matchOutcome(OutcomeReleased), Code: mq.CodeUnavailable, Retryable: true},
		{Match: matchOutcome(OutcomeModified), Code: mq.CodeUnsupported},
		{Match: matchOutcome(OutcomeReceived), Code: mq.CodeSettlementUnknown, Ambiguous: true},
	}

	// Everything the peer ANSWERED with. A condition in this set is the broker
	// processing the command and refusing it, so the outcome is definite
	// however the caller reached it. Splitting the conditions this way rather
	// than keeping one table is what makes the publish path's ambiguity and the
	// settlement path's lost lease both reachable from one set of predicates.
	//
	// A transport condition inside a `rejected` outcome is an answer too, and
	// it is the last of them: see [rejectedWith].
	answered := concat(wrongProtocol, stolen, limits, auth, topology, shape, outcomes,
		rejectedWith(transport))

	// The transport wrappers, with no condition of their own: the link, the
	// session or the connection simply closed. They are LAST among the
	// classifying rules.
	gone := []driverkit.Rule{
		{Match: isTransportGone, Code: mq.CodeUnavailable, Retryable: true},
	}
	// Settlement. See the function's own documentation for why the answer
	// depends on the receiver settle mode.
	settleLost := []driverkit.Rule{
		{Match: isTransportGone, Code: mq.CodeLeaseLost},
	}
	settleCode, settleAmbiguous := mq.CodeLeaseLost, false
	if recvSettle.Resolve() == ReceiverSettleSecond {
		settleCode, settleAmbiguous = mq.CodeSettlementUnknown, true
		settleLost = []driverkit.Rule{
			{Match: isTransportGone, Code: mq.CodeSettlementUnknown, Ambiguous: true},
		}
	}

	m := &driverkit.ErrorMapper{
		Driver: p.Driver,
		Rules: map[string][]driverkit.Rule{
			// On a publish the transfer had already been handed to the sender,
			// so every transport failure — condition or no condition — is
			// AMBIGUOUS: the peer may hold the message and its disposition may
			// simply not have reached us. The peer's own answers stay
			// definite, which is the half a driver that marked everything
			// ambiguous would destroy.
			mq.OpPublish: concat(answered, alsoAmbiguous(transport), alsoAmbiguous(gone)),
			mq.OpFetch:   concat(answered, transport, gone),
			// On a settlement a transport failure means the delivery went back
			// to the node, which under `first` is a lost lease and under
			// `second` an unknown outcome. The peer's own answers stay
			// definite here too.
			mq.OpAck:    concat(answered, retarget(settleCode, false, settleAmbiguous, transport), settleLost),
			mq.OpNack:   concat(answered, retarget(settleCode, false, settleAmbiguous, transport), settleLost),
			mq.OpExtend: concat(answered, retarget(settleCode, false, settleAmbiguous, transport), settleLost),
			mq.OpOpen:   concat(answered, transport, gone),
			mq.OpClose:  concat(answered, transport, gone),
			mq.OpAdmin:  concat(answered, transport, gone),
		},
		Fallback: concat(answered, transport, gone),
	}
	if p.ErrorRules != nil {
		// The profile's rules go FIRST for every operation it names: a
		// broker-specific rule is the specific one, and the core's is the
		// general one it would otherwise shadow.
		for op, rules := range p.ErrorRules() {
			m.Rules[op] = concat(rules, m.Rules[op])
		}
	}
	return m
}

// rejectedWith copies rules so that each matches only a `rejected` outcome
// carrying its condition, keeping each rule's code and retryability and never
// ambiguous.
//
// `rejected` is the peer's terminal answer for the transfer: the disposition
// arrived and the node will not deliver the message. The transport rules
// reach a condition through the outcome as well as through a link that died,
// and on a publish they carry Ambiguous for the second case, so a
// rejected{amqp:internal-error} was reported ambiguous and the caller was
// left to reconcile a message the broker had refused. Tried before the
// transport rules, these keep the code a transport condition earns and drop
// the ambiguity an answer does not have.
func rejectedWith(rules []driverkit.Rule) []driverkit.Rule {
	rejected := matchOutcome(OutcomeRejected)
	out := concat(rules)
	for i := range out {
		cond := out[i].Match
		out[i].Match = func(err error) bool { return rejected(err) && cond(err) }
		out[i].Ambiguous = false
	}
	return out
}

// alsoAmbiguous copies rules with Ambiguous set, keeping each rule's own
// predicate, code and retryability.
//
// It is what makes the publish path honest without a second copy of the
// condition table. A transport condition on a publish — the peer forcing the
// connection, a link detached, a redirect — arrives AFTER the transfer was
// handed to the sender, so the peer may hold the message; the specification makes reporting
// that as a definite failure a disqualifier.
//
// It is also the shape of a defect this driver had and a test caught: the
// condition rules are tried before the transport rules, and
// [github.com/iqhive/iqmq/driverkit.ErrorMapper.MapNet] can only OR in the
// ambiguity that ClassifyNet finds — which for a *amqp.ConnError carrying
// `amqp:connection:forced` is none, because go-amqp's wrappers are neither a
// net.Error nor an EOF. So a publish whose connection the peer had forced was
// reported as a DEFINITE Unavailable until these rules carried the ambiguity
// themselves. It is the hole in a new skin, one table along.
func alsoAmbiguous(sets ...[]driverkit.Rule) []driverkit.Rule {
	out := concat(sets...)
	for i := range out {
		out[i].Ambiguous = true
	}
	return out
}

// retarget copies rules with a new code, retryability and ambiguity, keeping
// each rule's own predicate.
//
// The settlement operations need it for the same reason the publish path needs
// [alsoAmbiguous]: a transport condition means something different there. A
// link the peer detached is, on a fetch, a retryable Unavailable; on a
// settlement it means the delivery has gone back to the node, which is a lost
// lease under `first` and an unknown outcome under `second`.
func retarget(code mq.ErrorCode, retryable, ambiguous bool, sets ...[]driverkit.Rule) []driverkit.Rule {
	out := concat(sets...)
	for i := range out {
		out[i].Code, out[i].Retryable, out[i].Ambiguous = code, retryable, ambiguous
	}
	return out
}

// concat joins rule sets in priority order, most specific first.
func concat(sets ...[]driverkit.Rule) []driverkit.Rule {
	n := 0
	for _, s := range sets {
		n += len(s)
	}
	out := make([]driverkit.Rule, 0, n)
	for _, s := range sets {
		out = append(out, s...)
	}
	return out
}

// matchCond returns a rule predicate that matches an AMQP error condition.
//
// It reaches the condition through errors.As, which is what makes it work for
// all four shapes go-amqp produces: the *amqp.Error on its own, and each of the
// three transport wrappers, whose Unwrap returns the peer's *amqp.Error when
// there was one. A wrapper with no remote error unwraps to nil and matches
// nothing here, which is what leaves it to the transport rules.
func matchCond(conds ...amqp.ErrCond) func(error) bool {
	return func(err error) bool {
		c, ok := condOf(err)
		if !ok {
			return false
		}
		for _, want := range conds {
			if c == want {
				return true
			}
		}
		return false
	}
}

// condOf returns the AMQP error condition err carries, if any.
func condOf(err error) (amqp.ErrCond, bool) {
	var aerr *amqp.Error
	if errors.As(err, &aerr) && aerr != nil {
		return aerr.Condition, true
	}
	return "", false
}

// Condition returns the AMQP 1.0 error condition an error carries, and whether
// it carried one.
//
// It is exported because the condition is the only thing an AMQP 1.0 peer really
// tells a client about a refusal, and a caller who wants the broker's own answer
// should not have to know which of go-amqp's three wrappers it arrived in.
func Condition(err error) (string, bool) {
	c, ok := condOf(err)
	return string(c), ok
}

// isLinkGone reports whether err is the death of one link rather than of the
// connection under it.
//
// That is a *LinkError, and it is a *SessionError too, because every link this
// driver attaches has a session of its own (links.go): the peer ending the
// session ends the link in it and leaves the connection, and the connection
// generation, where they were. go-amqp reports a link whose session ended
// with the *SessionError, and matching only the *LinkError left such a link in
// place for ever, so every Fetch or Publish on it failed Unavailable until the
// connection happened to drop.
//
// A connection's death is not matched: the generation moves for it, and the
// reconnect sequence drops every link at once.
func isLinkGone(err error) bool {
	if err == nil {
		return false
	}
	var (
		link *amqp.LinkError
		sess *amqp.SessionError
	)
	return errors.As(err, &link) || errors.As(err, &sess)
}

// isTransportGone reports whether err is a link, session or connection that has
// closed.
//
// The three are one answer for a caller — the path to the broker is not there
// any more — and they are three types because go-amqp reports which scope died.
// What they are NOT is a classification: a *LinkError may carry a peer's
// condition, and that condition is the specific answer, which is why every
// condition rule is tried before this predicate.
func isTransportGone(err error) bool {
	if err == nil {
		return false
	}
	var (
		link *amqp.LinkError
		sess *amqp.SessionError
		conn *amqp.ConnError
	)
	return errors.As(err, &link) || errors.As(err, &sess) || errors.As(err, &conn)
}

// IsTransportGone reports whether err came from a link, session or connection
// that has closed.
//
// It is exported for a profile's own rules: a broker-specific module that wants
// to refine one of these — a redirect it knows how to follow, a detach it knows
// is a failover — needs the same predicate the core uses rather than a second
// copy of it.
func IsTransportGone(err error) bool { return isTransportGone(err) }

// OutcomeError is a publication the peer answered with a terminal delivery
// outcome that is not `accepted`.
//
// It exists because the three non-accepting outcomes are three different
// answers and a caller acts on the difference, which a bare error string would
// lose. The peer's own [github.com/Azure/go-amqp.Error] stays underneath for
// errors.As, so a rejection's condition is still the thing this core's rule
// table classifies on.
type OutcomeError struct {
	// Outcome is the AMQP 1.0 outcome name the peer answered with:
	// "released", "rejected", "modified" or "received".
	Outcome string
	// Err is the peer's own error, when a `rejected` outcome carried one. It
	// is nil otherwise.
	Err error
}

// Error renders the outcome without the payload, the address or any header
func (e *OutcomeError) Error() string {
	msg := "iqmq/amqp10: the peer answered the transfer with the `" + e.Outcome + "` outcome"
	if e.Err != nil {
		return msg + ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns the peer's own error, so errors.As reaches the *amqp.Error and
// the condition rules classify a rejection by its condition.
func (e *OutcomeError) Unwrap() error { return e.Err }

// The AMQP 1.0 terminal outcome names, as [OutcomeError.Outcome] carries them.
const (
	// OutcomeAccepted is the peer taking the transfer, which is not an error.
	OutcomeAccepted = "accepted"
	// OutcomeReleased is the peer declining to act on the transfer: it was
	// neither stored nor refused.
	OutcomeReleased = "released"
	// OutcomeRejected is the peer refusing the transfer as unprocessable.
	OutcomeRejected = "rejected"
	// OutcomeModified is the peer asking for the message to be changed at
	// the node, which a SENDER cannot honour: the outcome exists for a
	// receiver.
	OutcomeModified = "modified"
	// OutcomeReceived is the non-terminal state that says how much of a
	// partial transfer the peer holds. Reaching it as a publication's
	// outcome means the disposition sequence was never finished.
	OutcomeReceived = "received"
)

// matchOutcome returns a rule predicate that matches one terminal outcome.
//
// It matches by TYPE and by the outcome's own name rather than through
// errors.Is against a sentinel, because a sentinel built with
// [github.com/iqhive/iqmq/internal/errcode] matches every error carrying the
// same code — that is the whole point of the matching rule — so
// MatchIs(errReleased) would fire for any CodeUnavailable at all, including a
// transport failure. A rule table has to be able to tell those two apart.
func matchOutcome(outcomes ...string) func(error) bool {
	return func(err error) bool {
		var oerr *OutcomeError
		if !errors.As(err, &oerr) || oerr == nil {
			return false
		}
		for _, want := range outcomes {
			if oerr.Outcome == want {
				return true
			}
		}
		return false
	}
}

// outcomeError turns a terminal delivery state that is not `accepted` into the
// failure a publication reports.
//
// The three are not one answer and the difference is what a caller acts on:
//
//   - `rejected` is the peer refusing the message. It carries a condition, so
//     the rule table classifies it, and it is DEFINITE — the message was seen
//     and will be refused again.
//   - `released` is the peer declining to act on it: it was not stored and it
//     was not refused. That is Unavailable and retryable, and the retry may go
//     to another peer.
//   - `modified` is the peer asking for the message to be changed, which a
//     SENDER has no way to honour: the outcome exists for a receiver. It is
//     reported as a definite refusal rather than a retry, because repeating an
//     unmodified message gets the same answer.
//
// None of the three is ambiguous. The disposition arrived, so the outcome is
// known — which is the other half of the distinction and the half a driver
// that reported everything as ambiguous would destroy.
func outcomeError(state amqp.DeliveryState) error {
	switch s := state.(type) {
	case nil:
		return nil
	case *amqp.StateAccepted:
		return nil
	case *amqp.StateRejected:
		return &OutcomeError{Outcome: OutcomeRejected, Err: s.Error}
	case *amqp.StateReleased:
		return &OutcomeError{Outcome: OutcomeReleased}
	case *amqp.StateModified:
		return &OutcomeError{Outcome: OutcomeModified}
	case *amqp.StateReceived:
		return &OutcomeError{Outcome: OutcomeReceived}
	}
	return &OutcomeError{Outcome: OutcomeReceived}
}
