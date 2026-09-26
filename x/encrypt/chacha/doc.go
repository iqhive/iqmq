// Package chacha supplies ChaCha20-Poly1305 to
// [github.com/iqhive/iqmq/x/encrypt] as
// [github.com/iqhive/iqmq/x/encrypt.SuiteChaCha20Poly1305].
//
// It is the suite for hardware without AES instructions -- a small ARM
// consumer fleet, an older embedded core -- where AES-GCM runs in software and
// ChaCha20-Poly1305, which needs no table lookups and no special instructions,
// is markedly faster. On hardware WITH AES-NI or the ARMv8 crypto extensions
// AES-GCM is the faster of the two, so this is a deployment's choice and not a
// better default; nothing in this repository has measured either on this
// project's own paths, and this package makes no speed claim of its own.
//
// # Why a module of its own
//
// The primitive is golang.org/x/crypto/chacha20poly1305, and ADR-002 keeps the
// root module stdlib-only. So the ROOT defines the suite -- its envelope id,
// its key, nonce and tag sizes, its name -- and this module supplies the
// construction through [github.com/iqhive/iqmq/x/encrypt.RegisterSuite], the
// way crypto/sha256 supplies crypto.SHA256 through crypto.RegisterHash.
// An envelope that names the suite therefore parses in a
// program that has not linked this module, and fails to open there with
// ErrNoKey -- a deployment problem -- rather than as a corrupt message.
//
// # Use
//
//	if err := chacha.Register(); err != nil { … }
//	ring, err := encrypt.NewRotatingKeyring(encrypt.SuiteChaCha20Poly1305, key32, "2026-09")
//
// [Register] is explicit rather than an init side effect. A program states in
// its own source which primitives it links for its envelopes, and the
// contract's self-test (x/encrypt's TestTheSuiteContractCanFail) depends on
// being able to register a deliberately broken AEAD for this suite in a
// process that has not registered the real one.
//
// # What it is held to
//
// Exactly what the two AES-GCM suites are held to:
// [github.com/iqhive/iqmq/x/encrypt/encrypttest] runs the same envelope,
// tamper, key-id, additional-data, rotation and header-binding cases against
// all three, and this module's tests run it against this suite -- including
// rotation ONTO it from AES-GCM, which is how a deployment adopts it. A
// known-answer test pins the construction to RFC 8439 §2.8.2's vector, so the
// suite byte means the IETF construction and not merely something
// ChaCha-shaped.
//
// Nonces are 12 bytes and random, as for GCM, so the birthday bound the
// x/encrypt documentation states applies unchanged: rotate a key well before
// it has sealed 2^32 messages. XChaCha20-Poly1305's 24-byte nonce would lift
// that bound; it is not offered, because it is not an RFC and an envelope
// suite id is a promise to every other implementation that reads one.
package chacha
