package codec_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/internal/errcode"
)

// codeErr is a codec's own sentinel: a portable code plus a reason, matching
// any target of the same code the way every errcode value in this repository
// does. Two of them stand for the two refusals one codec has.
type codeErr struct{ msg string }

func (e *codeErr) Error() string { return e.msg }

// Code makes it a corrupt-message error, as a codec's refusals are.
func (e *codeErr) Code() errcode.Code { return errcode.CorruptMessage }

// Is matches by code, which is the behaviour under test one level down.
func (e *codeErr) Is(target error) bool {
	type coder interface{ Code() errcode.Code }
	c, ok := target.(coder)
	return ok && c.Code() == e.Code()
}

var (
	errFraming = &codeErr{msg: "framing is not what this codec was configured for"}
	errLength  = &codeErr{msg: "length prefix overruns the body"}
)

// TestReasonSeparatesTwoRefusalsOfOneCode is the accessor.
//
// The first half of this test asserts the TRAP, not the fix: errors.Is on a
// decode failure answers by portable code, so it says yes to every sentinel a
// codec has. That is deliberate -- it is what makes CodeCorruptMessage hold
// across a module boundary -- and it made a whole class of assertion silently
// vacuous: codec/avro's TestFramingIsConfiguredNotDetected passed with the
// framing refusal it was about REMOVED, because the length error raised
// instead was also a CorruptMessage. Three codec modules each grew a private
// reasonIs helper to work round it.
//
// Reason is what those helpers were. The second half is the assertion the
// next author should write instead.
func TestReasonSeparatesTwoRefusalsOfOneCode(t *testing.T) {
	err := error(&codec.DecodeError{ContentType: "application/avro", Err: errFraming})

	// The trap, asserted so that it is documented rather than rediscovered.
	if !errors.Is(err, errFraming) {
		t.Errorf("errors.Is(framing failure, ErrFraming) = false, want true")
	}
	if !errors.Is(err, errLength) {
		t.Errorf("errors.Is(framing failure, ErrLength) = false: a decode failure matches by CODE, and if this " +
			"ever becomes false the doc on DecodeError.Is and on Reason is wrong")
	}

	// The accessor: which refusal it was.
	if got := codec.Reason(err); got != errFraming { //nolint:errorlint // identity is the question; errors.Is answers a different one and is vacuous here
		t.Errorf("Reason(framing failure) = %v, want the framing sentinel itself", got)
	}
	if got := codec.Reason(err); got == errLength { //nolint:errorlint // identity is the question; errors.Is answers a different one and is vacuous here
		t.Errorf("Reason(framing failure) = the LENGTH sentinel, which is the vacuity Reason exists to end")
	}

	// It reaches through a wrapping, because a decode failure arrives inside
	// whatever the caller's layer added to it.
	wrapped := fmt.Errorf("decoding delivery 7: %w", err)
	if got := codec.Reason(wrapped); got != errFraming { //nolint:errorlint // identity is the question; errors.Is answers a different one and is vacuous here
		t.Errorf("Reason(a wrapped framing failure) = %v, want the framing sentinel", got)
	}
}

// TestReasonIsNilWhenThereIsNoDecodeError keeps the accessor from answering
// about errors it knows nothing of: a nil reason is what says "this is not a
// codec's refusal", and a caller comparing it to a sentinel gets false rather
// than a wrong yes.
func TestReasonIsNilWhenThereIsNoDecodeError(t *testing.T) {
	for name, err := range map[string]error{
		"a nil error":             nil,
		"a plain error":           errors.New("boom"),
		"an encode failure":       fmt.Errorf("marshalling: %w", errFraming),
		"a codec sentinel itself": codec.ErrNoCodec,
		"a nil target":            codec.ErrNilTarget,
	} {
		if got := codec.Reason(err); got != nil {
			t.Errorf("Reason(%s) = %v, want nil", name, got)
		}
	}
}

// TestReasonReturnsWhatDecodeErrorCarries pins the two halves together: the
// reason Reason returns is the same value errors.As reaches by hand, so the
// accessor cannot drift from the field it reads.
func TestReasonReturnsWhatDecodeErrorCarries(t *testing.T) {
	inner := errors.New("a decoder's own error")
	err := error(&codec.DecodeError{ContentType: "application/x-test", Err: inner})

	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatal("errors.As did not reach the *DecodeError")
	}
	if got, want := codec.Reason(err), de.Err; got != want { //nolint:errorlint // identity is the question; errors.Is answers a different one and is vacuous here
		t.Errorf("Reason = %v, want DecodeError.Err = %v", got, want)
	}
	if !errors.Is(err, inner) {
		t.Errorf("errors.Is(decode failure, the decoder's own error) = false: Unwrap must still reach it, " +
			"because that is how *json.SyntaxError is found")
	}
}
