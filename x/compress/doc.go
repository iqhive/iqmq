// Package compress frames a compressed message body so that it is
// self-describing, and bounds what decompressing one can cost.
//
// It is the pluggable half of the compression story. The other half is
// [github.com/iqhive/iqmq/middleware].Compress, which speaks gzip and marks
// the body with a `content-encoding` header, and the two answer different
// questions:
//
//   - Use `middleware.Compress` when the broker has real headers and the
//     readers include something that is not this library. `content-encoding:
//     gzip` is an interoperable statement that any HTTP-shaped consumer
//     understands, and the body it describes is a plain gzip stream.
//   - Use this package when the coding has to travel IN THE BODY. Six of the
//     twelve shipped drivers cannot carry a portable header without the wire
//     envelope, a dead-letter hop through some brokers drops headers, and a
//     topic that is being migrated from one coding to another needs each
//     message to say which it is rather than a consumer to be configured for
//     one. A frame is also what lets a coding this module cannot import —
//     zstd, s2 — be registered from its own module (the follow-up
//     `middleware.Compress speaks gzip and nothing else` names this package
//     as the owner).
//
// # The frame
//
// A framed body is
//
//	magic    4 bytes  "IQZ1"
//	codec    1 byte   the registered codec id
//	flags    1 byte   reserved; a non-zero value is refused, not ignored
//	size     4 bytes  big-endian uint32, the ORIGINAL length
//	payload  the codec's own bytes
//
// ten bytes of header. [Parse] reads it without allocating.
//
// # The declared size is a check, never an allocation
//
// The `size` field is the single most dangerous number in this package, and
// the rule it is held to is the one learned from two codecs in which a
// five-byte input asked for 64 GiB: **nothing is ever allocated because the
// frame said so.**
//
//   - `size` greater than [Config.MaxDecompressed] is [ErrTooLarge], refused
//     before the payload is touched and before anything is allocated.
//   - The output buffer starts from the COMPRESSED length (clamped to a small
//     floor and ceiling) and grows as bytes actually arrive, bounded by the
//     limit. An attacker who declares exactly the limit therefore causes the
//     allocation their own input justifies and not the one they asked for.
//   - Decompression runs through a reader limited to the limit plus one byte,
//     so a stream that would exceed the limit is stopped there rather than
//     after it has been materialised.
//   - The declared size is then checked against what actually arrived, and a
//     frame whose payload decompresses to a different length is
//     [ErrSizeMismatch]. That catches a truncated stream and a lying header
//     with one comparison.
//
// [TestDecompressionBombIsRefusedBeforeAnythingIsAllocated] and
// [TestABombThatLiesAboutItsSizeIsStillBounded] are the two tests that hold
// it -- one for a frame that declares its true enormous size and one for a
// frame that lies low -- with [TestTheNaiveDecoderIsWhatTheBudgetsWouldCatch]
// as their negative control. [FuzzParse], [FuzzDecode],
// [FuzzEncodeDecodeRoundTrip] and [FuzzDeclaredSizeIsNeverAllocated] are the
// fuzz targets.
//
// # What this package does not do
//
// It does not authenticate anything. A framed body can be re-framed by
// anybody who can publish to the topic, and the `size` field is attacker
// controlled — which is why it is treated as a claim rather than as a fact.
// Confidentiality and integrity are [github.com/iqhive/iqmq/x/encrypt]'s job,
// and compressing before encrypting leaks the plaintext's compressibility
// (records that ordering and its cost).
package compress
