package nsq

import (
	"errors"
	"strings"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// newErrorMapper builds this driver rules.
//
// Rules are per operation because retryability is. The distinction no native
// error can make is whether the command had already left the socket: go-nsq's
// asynchronous publish returns an error only when the frame was NOT written,
// and a context that expires afterwards is a publication nsqd may well have
// taken. So this driver carries its own [ambiguousError] for the second case
// rather than trying to read it out of a network error, and the mapper's first
// rule is that flag.
//
// The error frames nsqd sends back are strings — `E_BAD_TOPIC`,
// `E_BAD_MESSAGE`, `E_INVALID`, `E_FIN_FAILED`, `E_AUTH_FAILED` — reaching the
// client as an `nsq.ErrProtocol`. They are matched on the code prefix rather
// than the whole string, because nsqd appends detail to several of them.
func newErrorMapper() *driverkit.ErrorMapper {
	link := []driverkit.Rule{
		{Match: driverkit.MatchIs(gonsq.ErrStopped, gonsq.ErrClosing), Code: mq.CodeClosed},
		{Match: driverkit.MatchIs(gonsq.ErrNotConnected), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(gonsq.ErrAlreadyConnected), Code: mq.CodeConflict},
		{Match: driverkit.MatchIs(gonsq.ErrOverMaxInFlight), Code: mq.CodeResourceExhausted, Retryable: true},
		// go-nsq wraps an I/O error or an EOF inside the handshake in the
		// same type as nsqd's refusal of it, so the connection failing on
		// its way up reads as an IDENTIFY failure. It is the link, and the
		// command the caller asked for was never written.
		{Match: identifyBroken, Code: mq.CodeUnavailable, Retryable: true},
	}
	auth := []driverkit.Rule{
		{Match: protocolCode("E_AUTH_FAILED", "E_UNAUTHORIZED"), Code: mq.CodeUnauthorized},
		{Match: identifyCode("E_AUTH_FAILED", "E_UNAUTHORIZED"), Code: mq.CodeUnauthorized},
		// Every other IDENTIFY refusal is about the parameters this client
		// declared — an output-buffer timeout below nsqd's floor, a
		// message timeout past its maximum, a compression nsqd has not
		// enabled — so it is a configuration failure and not an
		// authentication one. Calling it Unauthorized was this driver's
		// first error-mapping mistake: it made "nsqd refuses a 10 ms
		// output-buffer timeout" read as "your credentials are wrong",
		// which is the wrong half of the system to go and look at.
		{Match: identifyRefused, Code: mq.CodeInvalidConfig},
	}
	shape := []driverkit.Rule{
		{Match: protocolCode("E_BAD_TOPIC", "E_BAD_CHANNEL"), Code: mq.CodeInvalidArgument},
		{Match: protocolCode("E_BAD_MESSAGE", "E_BAD_BODY"), Code: mq.CodeMessageTooLarge},
		{Match: protocolCode("E_INVALID", "E_BAD_PROTOCOL"), Code: mq.CodeInvalidArgument},
		// nsqd answers a FIN, REQ or TOUCH for a message it no longer has
		// in flight with one of these, which is exactly a lease that is
		// gone. It is the one settlement answer nsqd does give, and it
		// arrives on the connection rather than as a reply, so it is
		// usually seen only in a log — the local lease check is what
		// catches the case in time.
		{Match: protocolCode("E_FIN_FAILED", "E_REQ_FAILED", "E_TOUCH_FAILED"), Code: mq.CodeLeaseLost},
	}
	publish := []driverkit.Rule{
		{Match: ambiguous, Code: mq.CodeAmbiguous, Retryable: false, Ambiguous: true},
	}
	m := &driverkit.ErrorMapper{
		Driver: DriverName,
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: concat(publish, shape, link, auth),
			mq.OpFetch:   concat(shape, link, auth),
			mq.OpOpen:    concat(shape, link, auth),
			mq.OpClose:   concat(link, auth),
			mq.OpAck:     concat(shape, link),
			mq.OpNack:    concat(shape, link),
			mq.OpExtend:  concat(shape, link),
			mq.OpAdmin:   concat(shape, link, auth),
		},
		Fallback: concat(link, auth, shape),
	}
	return m
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

// protocolCode matches an nsqd error frame by its code.
//
// nsqd's frames are "E_CODE" or "E_CODE some detail", so the match is a prefix
// on the code followed by end-of-string or a space. Matching the substring
// alone would make "E_INVALID" match "E_INVALID_AUTH" and give the wrong code
// to a real authentication failure.
func protocolCode(codes ...string) func(error) bool {
	return func(err error) bool {
		var p gonsq.ErrProtocol
		s := err.Error()
		if errors.As(err, &p) {
			s = p.Reason
		}
		for _, c := range codes {
			if s == c || strings.HasPrefix(s, c+" ") {
				return true
			}
		}
		return false
	}
}

// identifyRefused matches an IDENTIFY handshake that failed over what this
// client declared: an nsqd error frame, which is always "E_" and a code, or a
// TLS handshake that failed over a certificate or a protocol setting.
//
// go-nsq's ErrIdentify carries only the text of what went wrong, so the text is
// what is read. The TLS markers are the ones crypto/tls and crypto/x509 put in
// every error of theirs, and an I/O error under a TLS handshake carries neither.
func identifyRefused(err error) bool {
	var e gonsq.ErrIdentify
	if !errors.As(err, &e) {
		return false
	}
	return strings.HasPrefix(e.Reason, "E_") || strings.Contains(e.Reason, "tls: ") || strings.Contains(e.Reason, "x509: ")
}

// identifyBroken matches an IDENTIFY handshake the connection failed under: an
// EOF, a timeout, a reset, or an answer that was not one. An address that is
// not nsqd at all, its HTTP port for one, fails the same way, and nothing in the
// error tells it from an nsqd that is restarting, so it too reads as retryable.
func identifyBroken(err error) bool {
	var e gonsq.ErrIdentify
	return errors.As(err, &e) && !identifyRefused(err)
}

// identifyCode matches an IDENTIFY refusal by the nsqd error code inside it.
//
// nsqd answers a bad IDENTIFY with an error FRAME whose text go-nsq wraps in
// an ErrIdentify rather than an ErrProtocol, so the two types carry the same
// vocabulary and have to be matched separately.
func identifyCode(codes ...string) func(error) bool {
	return func(err error) bool {
		var e gonsq.ErrIdentify
		if !errors.As(err, &e) {
			return false
		}
		for _, c := range codes {
			if e.Reason == c || strings.HasPrefix(e.Reason, c+" ") {
				return true
			}
		}
		return false
	}
}
