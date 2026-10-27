package driverkit_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// isNetFailure is the rule predicate every driver over a real client writes:
// match anything that came from the transport rather than from the broker.
func isNetFailure(err error) bool {
	var nerr net.Error
	return errors.As(err, &nerr) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.DeadlineExceeded)
}

// TestMapNetKeepsTheAmbiguitySentAlreadyCarries is the finding, on its own,
// with the failure the driver saw.
//
// MapNet's whole reason to exist is sentAlready: a transport failure before
// the write is a clean failure the caller may retry, and the same failure
// after it may have been accepted by the broker. The specification has ErrSettlementUnknown
// for the second and the specification makes "a duplicate presented as a loss" a
// settlement disqualifier — the caller publishes again and the broker holds
// two copies of one message.
//
// A rule table is consulted first and a rule carries its own Ambiguous, so a
// perfectly reasonable link rule — the one every driver writes, here as
// isNetFailure — used to defeat the whole call: the mapper returned the rule's
// definite verdict and never asked ClassifyNet. drivers/redis found it with
// TestTransportFailureAfterAWriteIsAmbiguous and worked around it by keeping
// only the two link conditions whose outcome is definite whatever the caller
// thought.
func TestMapNetKeepsTheAmbiguitySentAlreadyCarries(t *testing.T) {
	// A rule that matches a transport failure and, like almost every such
	// rule, says nothing about ambiguity because retryability is the only
	// thing its author was thinking about.
	linkRule := driverkit.Rule{Match: isNetFailure, Code: mq.CodeUnavailable, Retryable: true}
	// A rule that matches a SERVER reply: the broker processed the command
	// and refused it, so the outcome is definite however the caller reached
	// it. This one must not be made ambiguous by the fix.
	refusal := errors.New("OOM command not allowed when used memory > 'maxmemory'")
	serverRule := driverkit.Rule{
		Match: driverkit.MatchContains("OOM command not allowed"),
		Code:  mq.CodeBackpressure, Retryable: true,
	}
	m := &driverkit.ErrorMapper{
		Driver: "unit",
		Rules: map[string][]driverkit.Rule{
			mq.OpPublish: {serverRule, linkRule},
		},
	}

	for _, tc := range []struct {
		name        string
		err         error
		sentAlready bool
		wantCode    mq.ErrorCode
		wantAmbig   bool
		why         string
	}{
		{
			name: "a matched transport failure after the write", err: &timeoutError{},
			sentAlready: true, wantCode: mq.CodeUnavailable, wantAmbig: true,
			why: "the rule decided the code and the retryability; it cannot decide that a " +
				"publication which may have reached the broker definitely did not",
		},
		{
			name: "a matched transport failure before the write", err: &timeoutError{},
			sentAlready: false, wantCode: mq.CodeUnavailable, wantAmbig: false,
			why: "nothing left the socket, so the failure is definite and a retry is safe",
		},
		{
			name: "an EOF after the write", err: io.EOF,
			sentAlready: true, wantCode: mq.CodeUnavailable, wantAmbig: true,
			why: "the connection went away after the command was written",
		},
		{
			name: "a server refusal after the write", err: refusal,
			sentAlready: true, wantCode: mq.CodeBackpressure, wantAmbig: false,
			why: "a REPLY means the server processed the command and refused it, so the " +
				"outcome is definite: a fix that made every matched rule ambiguous would " +
				"turn every definite refusal in every driver's table into an unknown",
		},
		{
			name: "an unmatched cancellation after the write", err: context.Canceled,
			sentAlready: true, wantCode: mq.CodeCanceled, wantAmbig: true,
			why: "no rule matches it, and the fallback classification has always carried " +
				"sentAlready: this is the path that was right before the fix and must stay right",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := m.MapNet(mq.OpPublish, "orders", "", tc.err, tc.sentAlready)
			var oe *mq.OpError
			if !errors.As(err, &oe) {
				t.Fatalf("MapNet returned %T, want an *mq.OpError", err)
			}
			if oe.Code != tc.wantCode {
				t.Errorf("code is %v, want %v", oe.Code, tc.wantCode)
			}
			if oe.Ambiguous != tc.wantAmbig {
				t.Errorf("Ambiguous is %v, want %v: %s", oe.Ambiguous, tc.wantAmbig, tc.why)
			}
			if !errors.Is(err, tc.err) {
				t.Error("the native error was thrown away: errors.Is no longer reaches it")
			}
		})
	}
}

// TestMapNetConsultsTheFallbackRules holds MapNet to Fallback's documented
// contract — "tried for every operation, after the operation's own rules" —
// which Map honoured and MapNet did not.
//
// A driver that put its link rules in Fallback alone got one mapping from Map
// and another from MapNet for the same error, which is the asymmetry that made
// the ambiguity defect above hard to see.
func TestMapNetConsultsTheFallbackRules(t *testing.T) {
	sentinel := errors.New("TOPIC_AUTHORIZATION_FAILED")
	m := &driverkit.ErrorMapper{
		Driver:   "unit",
		Fallback: []driverkit.Rule{{Match: driverkit.MatchIs(sentinel), Code: mq.CodeForbidden}},
	}
	plain := m.Map(mq.OpPublish, "orders", "", sentinel)
	netted := m.MapNet(mq.OpPublish, "orders", "", sentinel, true)
	var pe, ne *mq.OpError
	if !errors.As(plain, &pe) || !errors.As(netted, &ne) {
		t.Fatalf("Map = %v, MapNet = %v: both must be *mq.OpError", plain, netted)
	}
	if pe.Code != mq.CodeForbidden {
		t.Fatalf("Map read the fallback as %v, want CodeForbidden", pe.Code)
	}
	if ne.Code != pe.Code {
		t.Errorf("MapNet mapped the fallback rule's error to %v and Map mapped it to %v: "+
			"Fallback is documented as tried for every operation", ne.Code, pe.Code)
	}
}

// TestClassifyNetKeepsTheAmbiguityAnErrorAlreadyCarries: an error that
// ALREADY says its outcome is unknown stays unknown when a driver maps it.
//
// drivers/redis builds its short-WAITAOF answer as errcode.Ambiguous and maps
// it through MapNet. No rule matches it, so ClassifyNet's portable-code arm
// answered (CodeAmbiguous, retryable false, ambiguous FALSE), and the
// *OpError came out with Code=Ambiguous and Ambiguous=false: a message that
// is in the stream carried a flag saying its outcome was known. The same arm
// dropped the flag of an *OpError a driver mapped a second time -- the
// nearest *OpError decides IsAmbiguous, so the outer, definite one hid the
// inner, ambiguous one.
func TestClassifyNetKeepsTheAmbiguityAnErrorAlreadyCarries(t *testing.T) {
	short := errcode.New(errcode.Ambiguous, "WAITAOF came back short")
	mapped := &mq.OpError{Driver: "x", Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true, Err: io.EOF}
	for _, tc := range []struct {
		name string
		err  error
		code mq.ErrorCode
	}{
		{"an unmapped CodeAmbiguous error", short, mq.CodeAmbiguous},
		{"the ErrSettlementUnknown sentinel", mq.ErrSettlementUnknown, mq.CodeSettlementUnknown},
		{"an *OpError that is already ambiguous", mapped, mq.CodeUnavailable},
	} {
		for _, sent := range []bool{false, true} {
			code, _, ambiguous := driverkit.ClassifyNet(tc.err, sent)
			if code != tc.code || !ambiguous {
				t.Errorf("ClassifyNet(%s, sent=%v) = %v, ambiguous=%v; want %v, ambiguous", tc.name, sent, code, ambiguous, tc.code)
			}
		}
	}
	m := &driverkit.ErrorMapper{Driver: "redis"}
	// A rule that matches the error decides its code and retryability and
	// must not take its ambiguity away: the link rule every driver writes
	// matches the EOF inside the already-mapped error.
	ruled := &driverkit.ErrorMapper{Driver: "redis", Fallback: []driverkit.Rule{
		{Match: driverkit.MatchIs(io.EOF), Code: mq.CodeUnavailable, Retryable: true},
		{Match: driverkit.MatchIs(short), Code: mq.CodeUnavailable},
	}}
	for name, err := range map[string]error{
		"MapNet of a short WAITAOF":                  m.MapNet(mq.OpPublish, "orders", "", short, true),
		"Map of a short WAITAOF":                     m.Map(mq.OpPublish, "orders", "", short),
		"Map of an ambiguous *OpError":               m.Map(mq.OpPublish, "orders", "", mapped),
		"MapNet of an ambiguous *OpError":            m.MapNet(mq.OpPublish, "orders", "", mapped, false),
		"Map of an ambiguous *OpError, rule matched": ruled.Map(mq.OpPublish, "orders", "", mapped),
		"Map of a short WAITAOF, rule matched":       ruled.Map(mq.OpPublish, "orders", "", short),
		"MapNet of a short WAITAOF, rule matched":    ruled.MapNet(mq.OpPublish, "orders", "", short, false),
	} {
		var oe *mq.OpError
		if !errors.As(err, &oe) || !oe.Ambiguous {
			t.Errorf("%s = %v: the *OpError's Ambiguous flag is not set", name, err)
		}
		if !mq.IsAmbiguous(err) {
			t.Errorf("%s = %v: IsAmbiguous is false", name, err)
		}
	}
	// And CollapseBatch, over the batch the redis sink really builds when one
	// XADD was refused by a reply and the rest were stored but WAITAOF came
	// back short.
	refused := m.MapNet(mq.OpPublish, "orders", "", errors.New("READONLY You can't write against a read only replica"), true)
	durable := m.MapNet(mq.OpPublish, "orders", "", short, true)
	flagless := &mq.OpError{Driver: "redis", Operation: mq.OpPublish, Code: mq.CodeAmbiguous}
	for _, errs := range [][]error{{refused, durable}, {refused, flagless}, {flagless, refused}} {
		got := driverkit.CollapseBatch(errs)
		var be *mq.BatchError
		if !errors.As(got, &be) || !mq.IsAmbiguous(got) {
			t.Errorf("CollapseBatch(%v) = %v: want an ambiguous *BatchError, one entry is in the stream", errs, got)
		}
	}
}

// TestClassifyNetTreatsADNSFailureAsDefinite: a name that did not resolve
// left nothing at the broker, so the failure is definite however far the
// caller believed its request had got.
//
// *net.DNSError is itself a net.Error, so the generic net.Error arm used to
// take it first and carry sentAlready into the answer, and the arm written for
// it was never reached: a settlement or a publication whose redial failed to
// resolve was reported as possibly applied, which is an unknown outcome the
// specification makes permanent.
func TestClassifyNetTreatsADNSFailureAsDefinite(t *testing.T) {
	notFound := &net.DNSError{Err: "no such host", Name: "broker.invalid", IsNotFound: true}
	timedOut := &net.DNSError{Err: "i/o timeout", Name: "broker.invalid", IsTimeout: true}
	for name, err := range map[string]error{
		"a bare DNS failure":            notFound,
		"a DNS timeout":                 timedOut,
		"the dial error net.Dial wraps": &net.OpError{Op: "dial", Net: "tcp", Err: notFound},
	} {
		for _, sent := range []bool{false, true} {
			code, retryable, ambiguous := driverkit.ClassifyNet(err, sent)
			if code != mq.CodeUnavailable || !retryable || ambiguous {
				t.Errorf("ClassifyNet(%s, sent=%v) = %v, retryable=%v, ambiguous=%v; want Unavailable, retryable, definite",
					name, sent, code, retryable, ambiguous)
			}
		}
	}
}
