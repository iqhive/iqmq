// Package encrypt seals a message body in a self-describing AEAD envelope
//
// It is the in-band counterpart to [github.com/iqhive/iqmq/middleware].Encrypt,
// and the difference is not packaging. That middleware records the key id in
// an `encryption-key-id` HEADER, which means the id is
//
//   - droppable — a dead-letter hop or a driver with no portable headers
//     loses it, and the body then cannot be opened at all; and
//   - unauthenticated — a header is outside the AEAD's additional data, so
//     anybody who can publish can change which key a consumer is told to use.
//
// This package puts the suite and the key id INSIDE the envelope and passes
// the whole header as additional authenticated data, so both travel with the
// body and neither can be altered without the open failing.
//
// The second half of that sentence deserves a precise statement, because the
// obvious test for it does not actually test it. No tampered envelope opens —
// [TestHeaderTamperingIsDetected] flips every bit of every byte and truncates
// at every length — but that is true WITHOUT the additional-data binding as
// well, and the tamper test was found passing with the binding removed. The
// reason is structural: every field in this header either selects the key
// (suite, key id) or moves an offset (the key-id length), and GCM's tag is
// computed under the key over the nonce and the ciphertext, so a tampered
// header yields the wrong key or a shifted nonce and the tag catches it
// either way.
//
// So the binding is defence in depth today and load-bearing tomorrow. An
// envelope's additional data is frozen the moment the format ships, and the
// first future header field that neither selects the key nor shifts an
// offset — a flag saying how to interpret the plaintext, an algorithm
// parameter — would be forgeable for ever if the header were not already
// covered. [TestTheHeaderIsInTheAuthenticatedData] asserts the binding
// directly, and is the check that fails when it is removed; the specification records the reasoning so that nobody deletes the binding after running the
// tamper test and concluding it does nothing.
//
// # The envelope
//
//	magic       4 bytes  "IQE1"
//	suite       1 byte   the AEAD suite id
//	flags       1 byte   reserved; a non-zero value is refused, not ignored
//	keyIDLen    1 byte   length of the key id, 0-255
//	keyID       keyIDLen bytes
//	nonce       the suite's nonce size (12 for all three suites)
//	ciphertext  the sealed body, with the AEAD tag appended
//
// The additional authenticated data is the seven-byte fixed header plus the
// key id, followed by whatever [Config.AdditionalData] adds. Nothing in the
// envelope is secret — a key id is not a secret — and everything in it is
// authenticated.
//
// # No custom cryptography
//
// The specification forbids it and there is none here. The two AES suites are `crypto/aes`
// plus `crypto/cipher`'s GCM from the standard library;
// [SuiteChaCha20Poly1305] is golang.org/x/crypto's RFC 8439 construction,
// supplied from the module github.com/iqhive/iqmq/x/encrypt/chacha through
// [Suites.Register] because the root module may import only the standard
// library. Every nonce comes from `crypto/rand`, and this package writes no
// primitive of its own: what it contributes is a framing and a key-selection
// rule. The suite table is the only place an algorithm is named, and its
// nonce and tag sizes are asserted against what the primitive reports on every
// [Suites.NewAEAD] rather than trusted. Every suite runs the one contract in
// [github.com/iqhive/iqmq/x/encrypt/encrypttest].
//
// Nonces are random and never counters. Two messages sealed under one key
// with one nonce leak the exclusive-or of their plaintexts and let an
// attacker forge tags, and a counter is unsafe the moment two processes or
// two restarts share a key — which is the normal case for a message queue.
// The cost is the birthday bound: at a 12-byte random nonce, one key should
// seal fewer than about 2^32 messages before it is rotated, which
// [Keyring] exists to make possible and which
// The specification records with the arithmetic.
//
// # What this does NOT protect
//
// The specification requires this list, and it is long enough to be worth reading before
// choosing this package as an answer to a compliance question.
//
//   - **Headers and properties.** Only the BODY is sealed. Topic, partition
//     key, message id, correlation id, reply-to, content type, timestamps
//     and every application header travel in clear. A schema id or a
//     `user-email` header is as public as it was.
//   - **Size.** The ciphertext is the plaintext's length plus a constant, so
//     the length is disclosed exactly. Where the length is the secret — "did
//     this customer send the long form or the short form" — padding is
//     needed and this package does none.
//   - **Timing, rate and volume.** When a message was published, how many
//     there were, and to which topic, are all visible to anyone who can see
//     the broker.
//   - **Replay.** An envelope has no sequence number and no expiry: a sealed
//     body captured and re-published opens perfectly, because it is
//     authentic. Replay is a delivery-semantics problem and
//     [github.com/iqhive/iqmq/x/inbox] and
//     [github.com/iqhive/iqmq/x/idempotency] are where it is handled.
//   - **Ordering and deletion.** An attacker who can write to the broker can
//     drop, delay or reorder sealed messages; each one that does arrive is
//     authentic, and nothing here says any arrived.
//   - **Forward secrecy.** A key that leaks opens every message it ever
//     sealed, including the ones already in the broker's retention. Rotation
//     bounds the blast radius going forward and does nothing for the past.
//   - **The key itself.** [Keyring] is an interface precisely so that the
//     key can live in a KMS or an HSM; [StaticKeyring] holds it in process
//     memory, where a core dump or a debugger finds it.
//
// # Ordering with compression
//
// Compress first, then encrypt: encrypted bytes do not compress. That order
// leaks the plaintext's COMPRESSIBILITY through the ciphertext length, which
// is a real attack where a secret is mixed with attacker-chosen text.
// The specification records the choice and names the alternative
// (encrypt-then-compress, which simply does not compress).
package encrypt
