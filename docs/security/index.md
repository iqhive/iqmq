# Security

Specification. This page is what the library does, what it refuses to do,
and what is not in place yet.

Vulnerability reports go through
[GitHub's private vulnerability reporting](https://github.com/iqhive/iqmq/security/advisories/new)
— never a public issue.

## Broker-delivered payloads are untrusted

A message that arrives from a broker is input from the network, and this
library treats it as such:

* **limits are enforced before allocation.** A decoder reads the declared size
  and count and refuses, rather than allocating what a malicious header claims
  it needs. That is an allocation bound, and the review found that it is not
  by itself a *cost* bound — see
  [what the review found](#what-the-review-found). `mq.ErrMessageTooLarge` is the refusal a size or count limit gives,
  and `mq.CodeCorruptMessage` is the code a malformed payload's error carries.
  `wire`'s decoder has its own refusals for a bad magic, a bad version, a
  truncated frame, a varint that does not terminate and trailing bytes — five
  named errors rather than one, because a corrupt frame's *shape* is
  diagnostic.
* **the wire envelope is never auto-detected unless you configured it.**
  `mq.MetadataNativeStrict` is the default precisely so that a payload cannot
  become a header set because it happened to begin with the right bytes. That
  is a parsing decision with a security consequence, which is why it is a mode
  rather than a heuristic — and the one time adaptive mode got that sniff
  wrong, the regression input is committed and runs under `-race` in every CI
  build.
* **asynchronous and retry storage is bounded.** Every buffer has a default
  bound; there is no value of `MaxPendingMessages` that turns it off. An
  unbounded retry queue is a remote memory-exhaustion primitive.
* **compression middleware caps decompressed output.** A decompression bomb is
  a size limit's problem, not the decompressor's.
* **a reserved `iqmq-` header on an INBOUND delivery is producer-controlled
  data.** `Headers.Validate` refuses that prefix to a caller of this library and
  that is the whole of the enforcement: a producer using the broker's own client
  can set any header it likes. So an arriving `iqmq-attempts` or
  `iqmq-origin-topic` is a claim by whoever published, not provenance from the
  core — and nothing in the core reads one back as authority.
  `Delivery.Attempt` comes from the driver's own delivery count, and the five
  provenance headers are written on the dead-letter **copy** for an operator to
  read. The prefix reads as an authenticity claim; it is not one.

## Secrets never get rendered

## Transport

TLS is on by default for remote and cloud profiles. Any option that disables
transport security has **`Insecure` in its name** — that is a naming rule, so
`grep -r Insecure` over your own configuration finds every place you turned it
off.

Contradictory TLS settings are rejected **at construction**, not ignored with
the safer one winning. A configuration that says both things is a
configuration whose author is wrong about something, and guessing which half
they meant is how a deployment ends up in plaintext.

There is **no custom cryptography** anywhere in this repository, and that is an
inventory rather than an assurance. The only primitives any module uses are the
standard library's: `crypto/tls` (36 files), `crypto/rand` (9),
`crypto/aes` + `crypto/cipher` for the AES-GCM in `middleware.Encrypt`,
`crypto/sha256` for deriving a queue name in the two SQS drivers, and
`crypto/sha1` in `internal/streams` — which is Redis's own `EVALSHA` script
digest, a protocol identifier and not a security primitive.

## `unsafe`

None in the core package. Avoided in the memory engine. Any use requires: an
ADR, a safe pure-Go fallback behind a build tag, a material measured benefit,
race and multi-architecture testing, a security review, and isolation in its
own package.

The bake-off scoring adds **1000 lines of notional complexity per use of
`unsafe`** when breaking a tie, so an `unsafe` implementation has to win on
merit by more than a margin to be selected at all.

Two packages import it, and the review corrected this paragraph, which used
to name `internal/queue` and `wire` — `wire` has never imported it:

* `internal/queue/slot_unsafe.go`, behind the `iqmq_unsafe` build tag, with a
  pure-Go file taking its place by default. That is the shape exactly.
* `internal/payload/alias.go`, **unconditionally and with no fallback**, for
  `unsafe.SliceData` in `Overlaps`: it compares two slices' addresses to decide
  whether writing through one can be observed through the other, and it never
  dereferences either. There is no safe way to ask that question in Go, and a
  build-tagged fallback would have to answer it wrongly in one direction —
  always-true costs a copy on every borrowed payload, always-false misses real
  aliasing, which is a correctness bug in the memory-ownership contract.
  So it is a deliberate deviation from "isolated package or build tag" rather
  than an oversight, and it is recorded here because a policy with an unrecorded
  exception is a policy nobody can check.

## Supply chain

| | |
|---|---|
| licence | Apache-2.0, with `NOTICE`. `make license` scans dependencies. |
| vulnerabilities | `govulncheck` in every module per PR, on `main` and daily (`vuln.yml` → `make vuln`), on the latest Go patch release, judged against owned waivers — see [below](#dependency-advisories-as-govulncheck-reports-them) |
| static analysis | CodeQL weekly (`codeql.yml`) |
| Actions | pinned by full commit SHA, least-privilege `permissions:`, held by `make workflows-lint` |
| Scorecard | `scorecard.yml` weekly; the target is ≥ 8, and ≥ 7 at v1.0 |
| dependency hygiene | the root module depends on the standard library **only**, held by `make stdlib-check`. Each driver brings one client library and only its own. |

That last row is a security property as much as a performance one: your
program's attack surface is one broker SDK, not eleven.

## What the review found

The review that asks for has happened. It read every decoder for the
allocation rule, every `Config` for both renderings, every place a credential
can reach an output, every TLS refusal and every `crypto/` import, and ran
`govulncheck` over all 48 modules. Each finding below has a test that failed
before the fix.

### A decoder whose cost was the square of its input

`internal/wire`'s own documentation said "a hostile envelope can cost at most
its own size" (it now says the cost grows with the envelope's size, and names
the multiple). The TLV layout broke it in *time*: the commit pass asked for the
i-th header by restarting the walk from the first field of the metadata section,
so it was quadratic in a header count bounded only by the envelope's length. A
legal one-mebibyte envelope carrying 262,144 empty headers took **3m1s** to
decode; the fixed-width layout, which carries its cursor forward, took 4.8ms for
the same byte budget.

The reachability is the part that matters. The default limit is 1024 headers, so
a consumer's own limit **refuses** that envelope in 16.85µs — and then adaptive
metadata mode calls `wire.Detector.DetectFull` to tell a refused envelope from a native
body, and `DetectFull` removes every limit deliberately, so that "is this an
envelope" cannot depend on one consumer's configuration. The escalation happened
precisely on the input the limit exists to refuse, whatever was configured, and
the attacker picks the layout by writing it into the envelope's version byte.
Fixed; the test states the claim as a five-second budget for a one-mebibyte
body, which is a five-hundredfold margin over the linear cost.

### Two renderings that were not safe

`dsn.Config.String` is documented as "safe to put in a log line or an error" and
wrote every query parameter verbatim. Four shipped drivers read a credential
from one — `token` (NATS, JetStream, Pulsar), `authsecret` (NSQ), `authparams`
(Pulsar), `sessiontoken` (SQS, SNS+SQS) — so the one rendering documented as
safe printed `?token=…` in full. Worse, it had a pointer receiver, so a `Config`
**value** implemented neither `fmt.Stringer` nor `slog.LogValuer` and `%v` of
one printed the struct with the parameter slices in it. Both are fixed, the
predicate that decides which parameter is a credential now has one home, and
`memory.Config` — which implemented neither rendering — has both.

`String` still did not cover every verb. Under one that is not a string verb
(`%d`, `%t`, `%c`) fmt never asks `String` and prints the value beside its
bad-verb marker, so `fmt.Sprintf("%d", cfg.Password)` gave
`%!d(dsn.Secret=s3cr3t)`, and `%#v` of a `Config` printed its parameter values.
`dsn.Secret` and `dsn.Config` implement `fmt.Formatter` now, which fmt asks
first under every verb. Two verbs are beyond any method: fmt refuses `%p` for a
value that is not a pointer, and `%w` for a value that is not an error, before
it looks for one, and prints the value beside the refusal with every method
switched off. `dsn.Params` keeps its values behind a pointer, which fmt prints
in that report as an address, so neither verb prints a parameter. A `Secret`,
and so a `Config`'s password, still prints raw under both. `go vet` reports
both uses.

`Config.String` also wrote a decoded control character, or a byte that is not
UTF-8, as it was, so a username given as `%0A` put a newline into a log line.
It percent-encodes those now, along with any rune that does not print. The
`iqmq` CLI renders a target itself and had the same hole in every report that
names one, and in `doctor`'s parameter names. It follows the same rule now.

### A static analysis that covered one module of 48

`codeql.yml` ran `go build ./...` in the repository root. A Go pattern does not
cross into a nested module, and the root module is stdlib-only by policy, so
CodeQL extracted the one package set with no dependencies and none of the code
that links a broker client. It runs `make build` now. (It has still never run at
all — see below.)

### Three defects in the gate itself

Found while wiring `make security-check` in, and all three live on `main`: a
merge had put `docs-check` after the `##` in the Makefile's `ci:` line, where
make does not see it; the same line's trailing backslash continued the *comment*
and swallowed `work-sync`'s rule, so `make work-sync` exited 0 having done
nothing — a check in this repository's own gate that could not fail; and the
line's indentation made `hack/ci-parity.sh`, whose job is to notice exactly that
drift, exit 2 instead. All three are fixed and `ci-parity` now owns the shape of
that line.

## Dependency advisories, as `govulncheck` reports them

`make vuln` runs `govulncheck` in every module and judges what it finds against
a table of waivers in `tools/securitycheck/vuln.go`, each with an owner. It
**exits 0** on `go1.27.1` with the database of 2026-09-16: 48 modules scanned,
six affected, sixteen findings, every one waived.

| Advisory | Module | Fixed in | Reaches | State |
|---|---|---|---|---|
| GO-2026-5046 (CPU exhaustion, Avro decoder) | `hamba/avro/v2` | **none** | `codec/avro`, `cmd/iqmq`, `drivers/pulsar`, `x/pulsartx` | waived: no fix exists; `codec/avro` refuses the input before the decoder sees it |
| GO-2026-5047 (integer overflow, Avro decoder) | `hamba/avro/v2` | **none** | as above | as above |
| GO-2026-5048 (unbounded map allocations) | `hamba/avro/v2` | **none** | as above | as above |
| GO-2026-6061 (gRPC xDS RBAC / HTTP-2) | `google.golang.org/grpc` | `v1.82.1` | `benchmarks`, `benchmarks/competitors` | **fixed** at `v1.84.0` |
| GO-2026-6348 (gRPC HTTP-2 DATA fragmentation OOM) | `google.golang.org/grpc` | `v1.83.1` | as above | **fixed** at `v1.84.0` |
| GO-2026-4945 (go-jose JWE panic) | `go-jose/v4` | `v4.1.4` | `drivers/pulsar`, `x/pulsartx` | **fixed** in both |
| GO-2026-6253 (tar path traversal) | `moby/go-archive` | `v0.3.0` | `conformance` (testcontainers) | **fixed** |
| GO-2026-6354, GO-2026-6355 (SSH channel DoS) | `golang.org/x/crypto` | `v0.56.0` | `conformance` (testcontainers) | **fixed** |

Four things a reader should take from that table:

`make vuln` is deliberately **not** part of `make ci`: its verdict depends on a
database published after the commit, and a `make ci` that turned red overnight
with no change would stop meaning "this commit is green".

## Release verification

Local release tags span v0.5.0 through v1.0.0 and are lightweight references to commits. They carry no tag messages, tagger identities, dates or signatures. The commits are unsigned. GitHub publication and release-artefact signatures have not been verified.

Tags before v0.9.0 use the `bitbucket.org/iqhive/iqmq` module path. The v0.9.0 and later release trains use `github.com/iqhive/iqmq`.
