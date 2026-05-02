package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"sync"
)

// Suite identifies an AEAD construction. It is one byte in the envelope.
type Suite uint8

// The suites an envelope can name.
//
// The two AES-GCM suites are the standard library's and always available. The
// ChaCha20-Poly1305 suite is DEFINED here -- its id, its sizes, its name -- so
// that an envelope carrying it parses with no key, but its primitive lives in
// golang.org/x/crypto, which ADR-002's stdlib-only root may not import. The
// out-of-tree module supplies it through
// [RegisterSuite], the way crypto/sha256 supplies crypto.SHA256 through
// crypto.RegisterHash: the wire id is fixed centrally so two modules cannot
// disagree about what byte 3 means, and the implementation is linked only by a
// program that asks for it. The specification's "no custom
// cryptography" holds for all three: each is a published construction from
// the Go project's own packages.
const (
	// SuiteUnknown is the zero value and names no construction. An envelope
	// carrying it is [ErrUnknownSuite], which makes a zeroed byte fail
	// rather than select something.
	SuiteUnknown Suite = 0
	// SuiteAES128GCM is AES-128-GCM: a 16-byte key, a 12-byte nonce, a
	// 16-byte tag.
	SuiteAES128GCM Suite = 1
	// SuiteAES256GCM is AES-256-GCM: a 32-byte key, a 12-byte nonce, a
	// 16-byte tag. It is the default for [NewStaticKeyring] when given a
	// 32-byte key.
	SuiteAES256GCM Suite = 2
	// SuiteChaCha20Poly1305 is ChaCha20-Poly1305 as RFC 8439 defines it: a
	// 32-byte key, a 12-byte nonce, a 16-byte tag. Its nonce is the same
	// size as GCM, so the birthday bound the specification records applies
	// to it unchanged.
	//
	// It is the suite for hardware without AES instructions -- a small ARM
	// consumer fleet -- where it is markedly faster than a software AES.
	// It is available only once an out-of-tree module has
	// registered it: until then [NewAEAD] reports [ErrUnknownSuite] for it
	// and [Suites] does not list it, while [ParseEnvelope] still parses an
	// envelope that names it (the layout needs no primitive), so a consumer
	// without the module reports [ErrNoKey] rather than a corrupt message.
	// [SuiteOf] never selects it: a 32-byte key is AES-256 unless the caller
	// names this suite.
	SuiteChaCha20Poly1305 Suite = 3
)

// String names the suite. The strings appear in errors and log lines, so they
// are spelled out rather than derived.
func (s Suite) String() string {
	switch s {
	case SuiteAES128GCM:
		return "AES-128-GCM"
	case SuiteAES256GCM:
		return "AES-256-GCM"
	case SuiteChaCha20Poly1305:
		return "ChaCha20-Poly1305"
	case SuiteUnknown:
		return "unknown"
	}
	return "invalid"
}

// KeyLen is the key length in bytes this suite requires, or 0 for a suite
// this version does not define. A defined suite whose primitive has not been
// registered still reports its key length.
func (s Suite) KeyLen() int {
	if d, ok := suiteOf(s); ok {
		return d.keyLen
	}
	return 0
}

// suiteDef is one suite's fixed sizes, and the constructor when the root
// module can supply one.
//
// The sizes are recorded here so that [ParseEnvelope] can bound an envelope's
// layout WITHOUT a key — a parser that had to construct an AEAD to learn its
// nonce size could not be fuzzed on arbitrary input, and would make key
// lookup a prerequisite of rejecting a malformed length. They are asserted
// against what the primitive reports in TestSuiteTableMatchesTheStdlib and in
// every [NewAEAD], so the duplication cannot drift.
type suiteDef struct {
	keyLen    int
	nonceSize int
	overhead  int
	// builtin constructs the AEAD from the standard library. Nil for a
	// suite whose primitive another module registers.
	builtin func(key []byte) (cipher.AEAD, error)
}

var suites = map[Suite]suiteDef{
	SuiteAES128GCM:        {keyLen: 16, nonceSize: 12, overhead: 16, builtin: newGCM},
	SuiteAES256GCM:        {keyLen: 32, nonceSize: 12, overhead: 16, builtin: newGCM},
	SuiteChaCha20Poly1305: {keyLen: 32, nonceSize: 12, overhead: 16},
}

func suiteOf(s Suite) (suiteDef, bool) {
	d, ok := suites[s]
	return d, ok
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// registered holds the constructors [RegisterSuite] has been given, keyed by
// suite. It is read by NewAEAD, which runs when a keyring is built or rotated
// and never per message, so a mutex is the whole of its concurrency story.
var registered struct {
	mu   sync.RWMutex
	ctor map[Suite]func(key []byte) (cipher.AEAD, error)
}

// RegisterSuite supplies the primitive for a suite this package defines but
// cannot construct itself -- today only [SuiteChaCha20Poly1305], whose
// implementation is in golang.org/x/crypto.
//
// Call it at start-up, before a keyring names the suite; the module that owns
// the primitive wraps it (that module's Register).
// It refuses, with [ErrInvalidConfig], a nil constructor, a suite this version
// does not define or constructs itself, and a second registration for one
// suite: which primitive a suite byte means must not depend on which of two
// modules ran first. Every AEAD the constructor returns is checked against the
// suite's key length, nonce size and tag overhead by [NewAEAD], so a
// constructor for the wrong construction is refused rather than used to seal
// envelopes no reader could parse.
func RegisterSuite(s Suite, newAEAD func(key []byte) (cipher.AEAD, error)) error {
	d, ok := suiteOf(s)
	if !ok || d.builtin != nil || newAEAD == nil {
		return ErrInvalidConfig
	}
	registered.mu.Lock()
	defer registered.mu.Unlock()
	if _, taken := registered.ctor[s]; taken {
		return ErrInvalidConfig
	}
	if registered.ctor == nil {
		registered.ctor = map[Suite]func(key []byte) (cipher.AEAD, error){}
	}
	registered.ctor[s] = newAEAD
	return nil
}

// constructor returns the function that builds s's AEAD, or nil when the
// suite is undefined or its primitive has not been registered.
func constructor(s Suite) func(key []byte) (cipher.AEAD, error) {
	d, ok := suiteOf(s)
	if !ok {
		return nil
	}
	if d.builtin != nil {
		return d.builtin
	}
	registered.mu.RLock()
	defer registered.mu.RUnlock()
	return registered.ctor[s]
}

// Suites returns the suite ids this process can seal and open with, in id
// order: the two AES-GCM suites always, and [SuiteChaCha20Poly1305] once it
// has been registered. It is for a start-up log line and for a test that wants
// to cover all of them.
func Suites() []Suite {
	out := []Suite{SuiteAES128GCM, SuiteAES256GCM}
	if constructor(SuiteChaCha20Poly1305) != nil {
		out = append(out, SuiteChaCha20Poly1305)
	}
	return out
}

// SuiteOf returns the suite a key length selects: AES-128-GCM for 16 bytes
// and AES-256-GCM for 32.
//
// It exists so that a caller holding a key from a KMS does not have to name
// the algorithm twice, and it refuses every other length rather than padding
// or truncating — a silently truncated key is a weaker cipher nobody chose.
// It never selects [SuiteChaCha20Poly1305], which is also a 32-byte key: a
// caller who wants it names it.
func SuiteOf(key []byte) (Suite, error) {
	switch len(key) {
	case 16:
		return SuiteAES128GCM, nil
	case 32:
		return SuiteAES256GCM, nil
	}
	return SuiteUnknown, ErrInvalidConfig
}

// NewAEAD returns the AEAD for suite and key.
//
// It checks the key length against the suite rather than letting the
// primitive decide, so that a 32-byte key handed to SuiteAES128GCM is an
// error instead of quietly becoming AES-256 under a label that says 128. A
// suite whose primitive has not been registered is [ErrUnknownSuite].
func NewAEAD(suite Suite, key []byte) (cipher.AEAD, error) {
	d, ok := suiteOf(suite)
	if !ok {
		return nil, ErrUnknownSuite
	}
	ctor := constructor(suite)
	if ctor == nil {
		return nil, ErrUnknownSuite
	}
	if len(key) != d.keyLen {
		return nil, ErrInvalidConfig
	}
	aead, err := ctor(key)
	if err != nil {
		return nil, err
	}
	if aead == nil || aead.NonceSize() != d.nonceSize || aead.Overhead() != d.overhead {
		// The table and the primitive disagree, which would make
		// ParseEnvelope's bounds wrong. Refuse rather than seal something
		// no reader can parse.
		return nil, ErrInvalidConfig
	}
	return aead, nil
}
