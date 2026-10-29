# `x/encrypt`

An AEAD envelope carried in the body, with the suite and key id **inside it
and authenticated**.

```go
ring, err := encrypt.NewStaticKeyring(key32, "2026-09")  // 16 or 32 bytes
pm, err := encrypt.PublishEncrypt(encrypt.Config{Keys: ring})
mw, err := encrypt.Decrypt(encrypt.Config{Keys: ring, RequireSealed: true})
```

## Which one do I use?

| | `middleware.Encrypt` | `x/encrypt` |
|---|---|---|
| Where the key id is recorded | an `encryption-key-id` header | **inside the envelope** |
| Suite recorded on the message | no | yes |
| Survives a header-stripping hop | no — the body becomes unopenable | yes |
| Two suites on one topic | no | yes |
| Key rotation while running | restart | `RotatingKeyring` |

## The envelope

```
magic       4 bytes  "IQE1"
suite       1 byte   1 = AES-128-GCM, 2 = AES-256-GCM, 3 = ChaCha20-Poly1305
flags       1 byte   reserved; a non-zero value is REFUSED
keyIDLen    1 byte
keyID       keyIDLen bytes
nonce       12 bytes
ciphertext  the sealed body, with the tag appended
```

The additional authenticated data is the seven-byte header plus the key id,
followed by whatever `Config.AdditionalData` adds. Nothing in the envelope is
secret — a key id is not a secret — and everything in it is authenticated.

The two AES suites are the standard library's `crypto/aes` plus
`crypto/cipher` GCM, and ChaCha20-Poly1305 is `golang.org/x/crypto`'s RFC 8439
construction. The specification forbids custom cryptography and there is none: what this
package contributes is a framing and a key-selection rule.

## ChaCha20-Poly1305

For hardware without AES instructions — a small ARM consumer fleet, an older
embedded core — where AES-GCM runs in software and ChaCha20-Poly1305 is
markedly faster. On hardware *with* AES-NI or the ARMv8 crypto extensions
AES-GCM is the faster of the two, so this is a deployment's choice rather than
a better default. Nothing in this repository has measured either on this
project's own paths.

```go
import "github.com/iqhive/iqmq/x/encrypt/chacha"

suites := encrypt.NewSuites()
if err := chacha.Register(suites); err != nil { … }
ring, err := encrypt.NewRotatingKeyring(suites, encrypt.SuiteChaCha20Poly1305, key32, "2026-09")
```

The primitive is in `golang.org/x/crypto`, which the root module may not
import, so it lives in its own module, `x/encrypt/chacha`. The **root**
defines the suite — its envelope byte, its 32-byte key, 12-byte nonce and
16-byte tag, its name — and the module supplies the construction by
registering it in an `encrypt.Suites` you build and pass to the keyrings that
name the suite. Nothing is registered process-wide, so the code that builds a
keyring decides which primitive a suite byte means. Two consequences worth
knowing:

- **A consumer whose keyrings were built without `chacha.Register` still parses** an
  envelope naming the suite, and reports `ErrNoKey` for it — a deployment
  problem, and the same answer a consumer missing a rotated key gets — rather
  than calling it a corrupt message.
- **`SuiteOf` never picks it.** A 32-byte key is AES-256-GCM unless you name
  `SuiteChaCha20Poly1305`.

It is held to exactly what the AES suites are held to: the package
`x/encrypt/encrypttest` holds the envelope, tamper, key-id, wrong-key,
additional-data, rotation and header-binding cases, and all three suites run
that one function — including rotation *onto* ChaCha20-Poly1305 from AES-GCM,
which is how a deployment adopts it. The contract itself has been watched
failing: `x/encrypt` runs it against deliberately broken AEADs (one that skips
the tag check, one that ignores the additional data, one that ignores the
nonce) and requires the right case to fail for each. A known-answer test pins
the suite to RFC 8439 §2.8.2's vector.

Its nonce is 12 bytes, like GCM's, so the birthday bound below applies
unchanged. XChaCha20-Poly1305's 24-byte nonce would lift it and is not
offered: it is not an RFC, and an envelope suite byte is a promise to every
other implementation that reads one.

## Nonces are random, never counters

Two messages sealed under one key with one nonce leak the exclusive-or of
their plaintexts and let an attacker forge tags. A counter is unsafe the
moment two processes or two restarts share a key, which is the normal case for
a message queue, so every nonce comes from `crypto/rand`.

The cost is the birthday bound: at a 12-byte random nonce a single key should
seal fewer than roughly 2³² messages before rotation. `RotatingKeyring` is
what makes that possible without a restart — a key's safe message count is a
property of throughput, not of your deploy cadence — and it keeps every
previous key for *opening*, because a keyring that forgets an old key makes
every message still inside the topic's retention permanently unopenable.

## What this does **not** protect

The specification requires this list and it is the most useful section on this page.

- **Headers and properties.** Only the body is sealed. Topic, partition key,
  message id, correlation id, reply-to, content type, timestamps and every
  application header travel in clear. A `schema-id` or a `user-email` header
  is as public as it was.
- **Size.** The ciphertext is the plaintext's length plus a constant, so the
  length is disclosed exactly. Where the length *is* the secret, padding is
  needed and this package does none.
- **Timing, rate and volume.** When a message was published, how many there
  were, and to which topic, are visible to anyone who can see the broker.
- **Replay.** An envelope has no sequence number and no expiry. A sealed body
  captured and re-published opens perfectly, because it is authentic. Replay
  is a delivery-semantics problem: see [`x/inbox`](inbox.md).
- **Ordering and deletion.** An attacker who can write to the broker can
  drop, delay or reorder sealed messages. Each one that arrives is authentic,
  and nothing here says any arrived.
- **Forward secrecy.** A leaked key opens every message it ever sealed,
  including those already in retention. Rotation bounds the blast radius
  going forward and does nothing for the past.
- **The key itself.** `Keyring` is an interface so the key can live in a KMS
  or an HSM. `StaticKeyring` holds it in process memory, where a core dump or
  a debugger finds it.

## A precise note on tamper detection

No tampered envelope opens — the test flips every bit of every byte and
truncates at every length. But that is true *without* the header-in-AAD
binding as well, and the tamper test was found passing with the binding
removed.

The reason is structural: every field in this header either selects the key
(suite, key id) or moves an offset (the key-id length), and GCM's tag is
computed under the key over the nonce and ciphertext. A tampered header
therefore yields the wrong key or a shifted nonce, and the tag catches it
either way.

So the binding is defence in depth today and load-bearing tomorrow. An
envelope's additional data is frozen the moment the format ships, and the
first future header field that neither selects the key nor shifts an offset —
a flag describing how to interpret the plaintext, an algorithm parameter —
would be forgeable for ever if the header were not already covered.
`TestTheHeaderIsInTheAuthenticatedData` asserts the binding directly and is
the check that fails when it is removed.

## Migration

`RequireSealed` is **off** by default so a topic can be migrated without
dropping the messages already in it. Switch it on once the migration is done:
a consumer that accepts unsealed bodies accepts anything a publisher without
the key can write, which makes the encryption an integrity control an
attacker can opt out of.

`Config.AdditionalData` is empty by default. Binding it to the topic would be
stronger and is a footgun: a dead-lettered or forwarded message arrives on a
*different* topic, and the dead-letter consumer — the reader that most needs
to see it — would then be unable to open anything.

## Publishing seals a copy

`PublishEncrypt` seals a copy of each message and leaves yours with its
plaintext body and its own headers, so publishing it again seals it again.
The ID and `CreatedAt` the publisher stamps on the copy
(`PublisherConfig.AutoID` and `Timestamp`, or `middleware.ID` and
`middleware.Timestamp` further down the chain) come back to your message
where it had none. A republication after an ambiguous failure then keeps its
ID, and a consumer that deduplicates on it can match the two.
`middleware.PublishEncrypt` does the same.

## Dead letters and retries stay sealed

`Decrypt` puts the plaintext in the delivery for the rest of the chain, and
the runner keeps the sealed body it received. A message the handler fails is
dead-lettered sealed, so the dead-letter topic holds no plaintext, and a
strictly ordered retry (`mq.WithOrderingByKey()`) opens the envelope again
rather than meeting its own plaintext and refusing it as `ErrNotSealed`.
`middleware.Encrypt` does the same and keeps its `encryption-key-id` header on
the copy. Each attempt of a `middleware.Retry` placed before either of them
starts from the sealed body too.

## Order with compression

Compress first, then encrypt. Encrypted bytes do not compress, so the other
order simply does not compress. See [`x/compress`](compress.md) for what that
leaks.
