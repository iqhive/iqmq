package encrypt

import (
	"github.com/iqhive/iqmq/internal/errcode"
)

// Magic is the four bytes every envelope begins with. A second, incompatible
// layout would be "IQE2" rather than a flag inside this one.
const Magic = "IQE1"

// FixedHeaderLen is the envelope's fixed prefix: magic, suite, flags and the
// key-id length. The key id, the nonce and the ciphertext follow.
const FixedHeaderLen = len(Magic) + 1 + 1 + 1

// MaxKeyIDLen is the longest key id an envelope can carry, which is what the
// single length byte holds.
const MaxKeyIDLen = 255

// Envelope is one parsed envelope. Every byte slice in it ALIASES the input:
// parsing allocates nothing, which is what [TestAllocsParseEnvelope] pins.
type Envelope struct {
	// Suite is the AEAD suite the body was sealed with.
	Suite Suite
	// Flags is the reserved byte; a parsed Envelope always carries zero.
	Flags uint8
	// KeyID names the key. It is not secret and it is authenticated.
	KeyID []byte
	// Nonce is the per-message nonce, the suite's nonce size long.
	Nonce []byte
	// Ciphertext is the sealed body with the AEAD tag appended.
	Ciphertext []byte
	// AAD is the envelope's own header — everything up to and including the
	// key id — which is what [Seal] authenticated and [Open] must therefore
	// present again. It is the field that makes tampering with the suite or
	// the key id detectable.
	AAD []byte
}

// The errors this package reports.
var (
	// ErrNotSealed (CodeCorruptMessage) reports a body that is not an
	// envelope: too short, wrong magic, or a length field that does not fit
	// the bytes that follow it.
	ErrNotSealed error = errcode.New(errcode.CorruptMessage, "iqmq/x/encrypt: body is not a sealed envelope")
	// ErrBadFlags (CodeCorruptMessage) reports a reserved flag this version
	// does not define. It is refused rather than ignored: a reader that
	// ignores a flag it does not understand accepts an envelope whose
	// meaning it has not got.
	ErrBadFlags error = errcode.New(errcode.CorruptMessage, "iqmq/x/encrypt: envelope sets a reserved flag this version does not define")
	// ErrNotAuthentic (CodeCorruptMessage) reports an envelope that did not
	// open: the ciphertext, the tag, the nonce, the key id or the suite byte
	// was altered, or the key is not the one it was sealed with.
	//
	// The five causes are deliberately one error. An attacker who can tell
	// "wrong key" from "bad tag" learns something, and a defender can do
	// nothing different about either.
	ErrNotAuthentic error = errcode.New(errcode.CorruptMessage, "iqmq/x/encrypt: sealed body does not authenticate")
	// ErrUnknownSuite (CodeUnsupported) reports a suite id this version does
	// not implement. Unsupported and not CorruptMessage: the envelope is
	// well formed and the READER is what is out of date.
	ErrUnknownSuite error = errcode.New(errcode.Unsupported, "iqmq/x/encrypt: no AEAD suite is defined for this envelope's suite id")
	// ErrNoKey (CodeInvalidConfig) reports a key id the keyring does not
	// hold. It is a deployment problem — a consumer that has not been given
	// the key the publisher rotated to — and not a bad message.
	ErrNoKey error = errcode.New(errcode.InvalidConfig, "iqmq/x/encrypt: the keyring holds no key with this id for this suite")
	// ErrKeyIDTooLong (CodeInvalidConfig) reports a key id over
	// [MaxKeyIDLen] bytes.
	ErrKeyIDTooLong error = errcode.New(errcode.InvalidConfig, "iqmq/x/encrypt: the key id is longer than an envelope can carry")
	// ErrInvalidConfig (CodeInvalidConfig) reports a configuration or a key
	// length that cannot work.
	ErrInvalidConfig error = errcode.New(errcode.InvalidConfig, "iqmq/x/encrypt: invalid configuration")
	// ErrAlreadySealed (CodeInvalidArgument) reports a Seal of a body that
	// is already an envelope. Sealing twice is refused rather than done: the
	// outer envelope's key id would name a key that opens an envelope rather
	// than a body, and the consumer would hand a handler an envelope.
	ErrAlreadySealed error = errcode.New(errcode.InvalidArgument, "iqmq/x/encrypt: body is already sealed")
)

// Sealed reports whether b begins with an envelope header this package could
// parse.
//
// It is a SNIFF, like every magic-byte test: a plaintext that happens to
// start with the four bytes answers true. Unlike compression, the consequence
// here is bounded — such a body fails to open with [ErrNotAuthentic] rather
// than being mis-handled — and [Seal] refuses to seal one rather than
// producing something it could not tell apart later.
func Sealed(b []byte) bool {
	return len(b) >= FixedHeaderLen && string(b[:len(Magic)]) == Magic
}

// ParseEnvelope reads an envelope's header, key id, nonce and ciphertext.
//
// It allocates nothing and copies nothing: every slice in the result aliases
// b. It does NOT open the envelope and it does not need a key, which is what
// makes it the fuzzable surface — every length and offset in the layout is
// decided here, before any cryptography, and the whole class of bug this
// guards against is a length field that makes two slices overlap.
func ParseEnvelope(b []byte) (Envelope, error) {
	if !Sealed(b) {
		return Envelope{}, ErrNotSealed
	}
	e := Envelope{Suite: Suite(b[4]), Flags: b[5]}
	if e.Flags != 0 {
		return Envelope{}, ErrBadFlags
	}
	s, ok := suiteOf(e.Suite)
	if !ok {
		return Envelope{}, ErrUnknownSuite
	}
	keyLen := int(b[6])
	// The header, the key id, the nonce and at least the tag must all fit.
	// One comparison over the total, so that no partial arithmetic can
	// produce an offset past the end.
	need := FixedHeaderLen + keyLen + s.nonceSize + s.overhead
	if len(b) < need {
		return Envelope{}, ErrNotSealed
	}
	aadEnd := FixedHeaderLen + keyLen
	nonceEnd := aadEnd + s.nonceSize
	e.AAD = b[:aadEnd]
	e.KeyID = b[FixedHeaderLen:aadEnd]
	e.Nonce = b[aadEnd:nonceEnd]
	e.Ciphertext = b[nonceEnd:]
	return e, nil
}

// appendHeader appends the fixed header and the key id, and returns the
// buffer with the offset at which the additional authenticated data ends.
func appendHeader(dst []byte, suite Suite, keyID []byte) ([]byte, int) {
	dst = append(dst, Magic...)
	dst = append(dst, byte(suite), 0, byte(len(keyID)))
	dst = append(dst, keyID...)
	return dst, len(dst)
}
