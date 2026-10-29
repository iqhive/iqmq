# `x/compress`

Framed, pluggable compression. The coding travels **in the body**, so it
survives a driver with no portable headers and a dead-letter hop, and a
coding this repository cannot import — zstd, s2 — can be registered from its
own module.

```go
enc, err := compress.NewEncoder(compress.Config{
    Codec:           compress.CodecGzip,
    MinSize:         512,
    MaxDecompressed: 16 << 20,
})
pm, err := compress.PublishCompress(cfg)   // publish side
mw, err := compress.Decompress(cfg, compress.UnframedPass) // consume side
```

## Which one do I use?

There are two compression paths in this repository and they are not
interchangeable.

| | `middleware.Compress` | `x/compress` |
|---|---|---|
| Where the coding is recorded | a `content-encoding` header | **inside the body** |
| Codings | gzip only | gzip, flate, zlib, stored, plus yours |
| Interoperable with non-iqmq readers | yes — it is a plain gzip stream | no |
| Survives a header-stripping hop | no | yes |
| Body is self-describing | no | yes |

Use `middleware.Compress` when something that is not this library reads the
topic. Use `x/compress` otherwise.

## The frame

```
magic    4 bytes  "IQZ1"
codec    1 byte   the registered codec id
flags    1 byte   reserved; a non-zero value is REFUSED, not ignored
size     4 bytes  big-endian uint32, the original length
payload  the codec's own bytes
```

Ten bytes. `Parse` reads it without allocating.

A body that does not shrink — one under `MinSize`, or one compression makes
bigger — is framed with `CodecStored`, so **every** message on a framed topic
carries a frame and a consumer never has to guess whether an unframed body is
an old publisher or a corruption.

## The size cap is the point

The specification requires that compression middleware caps decompressed output, and the
reason is that a gzip stream is a program: a few hundred broker-delivered
bytes can expand to gigabytes, and a decompressor reading to EOF into a
growing buffer allocates every one of them before anything notices.

Five things happen in this order, and the order is the security property:

1. the header is parsed — no allocation;
2. the **declared** size is compared with `MaxDecompressed`, and a frame over
   it is `ErrTooLarge` before a codec is even looked up;
3. the codec is looked up;
4. the payload is decompressed into a buffer sized from the **compressed**
   length and bounded by the limit;
5. the result's actual length is compared with the declared one, and a
   mismatch is `ErrSizeMismatch`.

Step 2 stops a bomb cheaply. Step 4 stops one that *lies about its size*,
which step 2 cannot catch — a frame may declare 1 KiB and decompress to 64
MiB. Step 5 turns a truncated stream into an error rather than a short body
the handler would treat as real.

There is no way to ask for "no limit". `MaxDecompressed` zero selects 16 MiB;
a negative value is refused. `math.MaxInt64` is accepted and is the largest
limit there is.

## Dead letters and retries stay framed

`Decompress` puts the decompressed body in the delivery for the rest of the
chain, and the runner keeps the frame it received. A message the handler fails
is dead-lettered as that frame, which is the size the broker already accepted,
and a strictly ordered retry (`mq.WithOrderingByKey()`) decompresses it again
rather than meeting the decompressed body and refusing it as `ErrNotFramed`
under `UnframedFail`. Each attempt of a `middleware.Retry` placed before
`Decompress` starts from the frame in the same way.

## What it does not do

**It does not authenticate anything.** Anybody who can publish to the topic
can write a frame, and the `size` field is attacker-controlled — which is
precisely why it is treated as a claim. For confidentiality and integrity,
see [`x/encrypt`](encrypt.md).

**Compressing before encrypting leaks compressibility.** That is the right
order — encrypted bytes do not compress — and the cost is that the
ciphertext's length reveals how compressible the plaintext was, which matters
where a secret is mixed with attacker-chosen text. `x/encrypt`'s page lists
it among the things that package does not protect.

**A plaintext that begins with `IQZ1` is passed through unframed.** `Encode`
refuses to frame a body that already looks framed, because framing it twice
would produce something that decodes to a framed body. A caller who cannot
accept either behaviour should use the wire envelope, which is
length-prefixed and needs no sniff.

## Errors

| Error | Code | Meaning |
|---|---|---|
| `ErrNotFramed` | `CodeCorruptMessage` | not a frame: short, or wrong magic |
| `ErrBadFlags` | `CodeCorruptMessage` | a reserved flag this version does not define |
| `ErrUnknownCodec` | `CodeUnsupported` | the reader has not registered this coding |
| `ErrTooLarge` | `CodeMessageTooLarge` | over `MaxDecompressed` |
| `ErrSizeMismatch` | `CodeCorruptMessage` | the frame lied, or the stream was truncated |

`ErrUnknownCodec` is `CodeUnsupported` and not `CodeCorruptMessage` on
purpose: the frame is well formed and the **reader** is what is missing. A
consumer that has not been given zstd is a deployment problem, not a bad
message, and the two want different alerts.

## Registering a coding

```go
r := compress.NewRegistry()
if err := r.Register(myZstd{}); err != nil { … }   // id must be >= 32
```

Ids below `FirstUserCodec` (32) are reserved, and a duplicate id is
`ErrDuplicateCodec` rather than a silent overwrite — two modules that both
claimed id 4 would give one process a decoder for the other's frames, and the
symptom would be a corrupt body rather than a configuration error.
