package claimcheck

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Magic is the four bytes every reference begins with.
const Magic = "IQK1"

// HeaderLen is the reference's fixed prefix: magic, flags, digest algorithm,
// size, store length and key length. The store, the key and the digest
// follow.
const HeaderLen = len(Magic) + 1 + 1 + 8 + 2 + 2

// MaxStoreLen and MaxKeyLen are what the two uint16 length fields hold. The
// key bound is generous on purpose: an S3 key is up to 1024 bytes and a GCS
// object name up to 1024 characters, both of which fit.
const (
	MaxStoreLen = 1<<16 - 1
	MaxKeyLen   = 1<<16 - 1
)

// Digest identifies the digest algorithm a reference carries.
type Digest uint8

const (
	// DigestNone records no digest.
	//
	// It exists for a store the caller genuinely trusts to be immutable and
	// content-addressed already, where a second digest is redundant. It is
	// not the default and [Config.RequireDigest] refuses it, because the
	// payload is remote input and a package that verified nothing by default
	// would be handing every handler an unauthenticated body.
	DigestNone Digest = 0
	// DigestSHA256 is SHA-256 over the payload. It is the default.
	//
	// SHA-256 and not a faster non-cryptographic hash: the threat is not
	// only a corrupted transfer but a store somebody else can write to, and
	// a CRC is trivial to collide on purpose. It is not a MAC either, so it
	// does not authenticate the publisher -- an attacker who can write BOTH
	// the bucket and the topic can produce a consistent pair, and
	// x/encrypt is what closes that.
	DigestSHA256 Digest = 1
)

// Len is the digest's length in bytes, or 0 for [DigestNone].
func (d Digest) Len() int {
	if d == DigestSHA256 {
		return sha256.Size
	}
	return 0
}

// String names the algorithm.
func (d Digest) String() string {
	switch d {
	case DigestSHA256:
		return "sha-256"
	case DigestNone:
		return "none"
	}
	return "invalid"
}

// Ref is one parsed reference. Store, Key and Sum ALIAS the input: parsing
// allocates nothing.
type Ref struct {
	// Flags is the reserved byte; a parsed Ref always carries zero.
	Flags uint8
	// Digest is the algorithm [Ref.Sum] is under.
	Digest Digest
	// Size is the payload's length as recorded at publish time. It is a
	// claim, checked against the limit before the store is called and
	// against the fetched payload afterwards.
	Size uint64
	// Store names the store that holds the payload, so that one consumer can
	// read references written by publishers using different buckets. A
	// consumer whose [Store.Name] does not match refuses with
	// [ErrWrongStore] rather than looking for the key in the wrong place.
	Store []byte
	// Key locates the payload within the store.
	Key []byte
	// Sum is the digest of the payload, [Digest.Len] bytes long.
	Sum []byte
}

// The errors this package reports.
var (
	// ErrNotRef (CodeCorruptMessage) reports a body that is not a
	// reference: too short, wrong magic, or length fields that do not fit
	// the bytes after them.
	ErrNotRef error = errcode.New(errcode.CorruptMessage, "iqmq/x/claimcheck: body is not a claim-check reference")
	// ErrBadFlags (CodeCorruptMessage) reports a reserved flag this version
	// does not define, refused rather than ignored.
	ErrBadFlags error = errcode.New(errcode.CorruptMessage, "iqmq/x/claimcheck: reference sets a reserved flag this version does not define")
	// ErrUnknownDigest (CodeUnsupported) reports a digest algorithm this
	// version does not implement.
	ErrUnknownDigest error = errcode.New(errcode.Unsupported, "iqmq/x/claimcheck: no digest algorithm is defined for this reference")
	// ErrDigestMismatch (CodeCorruptMessage) reports a payload whose digest
	// is not the one the reference recorded: the store returned a different
	// object than the publisher put there.
	ErrDigestMismatch error = errcode.New(errcode.CorruptMessage, "iqmq/x/claimcheck: the fetched payload does not match the reference's digest")
	// ErrSizeMismatch (CodeCorruptMessage) reports a payload whose length is
	// not the one the reference recorded.
	ErrSizeMismatch error = errcode.New(errcode.CorruptMessage, "iqmq/x/claimcheck: the fetched payload is a different length than the reference recorded")
	// ErrTooLarge (CodeMessageTooLarge) reports a payload over
	// [Config.MaxPayload], refused before it is fetched when the reference
	// says so and while it is being read otherwise.
	ErrTooLarge error = errcode.New(errcode.MessageTooLarge, "iqmq/x/claimcheck: payload exceeds the limit")
	// ErrPayloadGone (CodePoisonMessage) reports a store that answered and
	// said there is no such object.
	//
	// It is PERMANENT and that is the whole point: retrying it pins a lane
	// on a message that will never succeed. The runner's failure policy
	// decides (ADR-011), and the dead-lettered message still carries the
	// reference. See the package documentation on why the consumer never
	// deletes a payload.
	ErrPayloadGone error = errcode.New(errcode.PoisonMessage, "iqmq/x/claimcheck: the store has no payload for this reference")
	// ErrStoreUnavailable (CodeUnavailable) reports a store that could not
	// be reached or did not answer.
	//
	// It is RETRYABLE, and it is the error a store implementation must
	// return for a timeout, a refused connection or a 5xx -- anything that
	// might succeed later. Reporting ErrPayloadGone for one of those would
	// dead-letter a perfectly good message because a bucket was briefly
	// unreachable.
	ErrStoreUnavailable error = errcode.New(errcode.Unavailable, "iqmq/x/claimcheck: the payload store is unavailable")
	// ErrWrongStore (CodeInvalidConfig) reports a reference naming a store
	// this consumer is not configured for.
	ErrWrongStore error = errcode.New(errcode.InvalidConfig, "iqmq/x/claimcheck: the reference names a different store than this consumer reads")
	// ErrDigestRequired (CodeInvalidConfig) reports a DigestNone reference
	// where [Config.RequireDigest] is set.
	ErrDigestRequired error = errcode.New(errcode.InvalidConfig, "iqmq/x/claimcheck: the reference carries no digest and this consumer requires one")
	// ErrInvalidConfig (CodeInvalidConfig) reports a configuration that
	// cannot work.
	ErrInvalidConfig error = errcode.New(errcode.InvalidConfig, "iqmq/x/claimcheck: invalid configuration")
	// ErrClosed (CodeClosed) reports a call on a closed store.
	ErrClosed error = errcode.New(errcode.Closed, "iqmq/x/claimcheck: the store is closed")
)

// IsRef reports whether b begins with a reference header this package could
// parse. It is a sniff; [ParseRef] checks the rest.
func IsRef(b []byte) bool {
	return len(b) >= HeaderLen && string(b[:len(Magic)]) == Magic
}

// ParseRef reads a reference.
//
// It allocates nothing: Store, Key and Sum alias b. Every length in the
// layout is decided here, and the three variable-length regions are bounded
// by one comparison against the total so that no partial arithmetic can
// produce an offset past the end.
func ParseRef(b []byte) (Ref, error) {
	if !IsRef(b) {
		return Ref{}, ErrNotRef
	}
	r := Ref{
		Flags:  b[4],
		Digest: Digest(b[5]),
		Size:   binary.BigEndian.Uint64(b[6:14]),
	}
	if r.Flags != 0 {
		return Ref{}, ErrBadFlags
	}
	if r.Digest != DigestNone && r.Digest != DigestSHA256 {
		return Ref{}, ErrUnknownDigest
	}
	storeLen := int(binary.BigEndian.Uint16(b[14:16]))
	keyLen := int(binary.BigEndian.Uint16(b[16:HeaderLen]))
	sumLen := r.Digest.Len()
	if len(b) != HeaderLen+storeLen+keyLen+sumLen {
		// EXACTLY, not "at least": a reference with trailing bytes is a
		// reference this version does not understand, and accepting it
		// would make the trailing bytes unusable for a later version.
		return Ref{}, ErrNotRef
	}
	off := HeaderLen
	r.Store = b[off : off+storeLen]
	off += storeLen
	r.Key = b[off : off+keyLen]
	off += keyLen
	r.Sum = b[off : off+sumLen]
	return r, nil
}

// AppendRef appends the reference for a payload in store under key.
func AppendRef(dst []byte, store, key string, size uint64, digest Digest, sum []byte) ([]byte, error) {
	if store == "" || key == "" {
		return nil, ErrInvalidConfig
	}
	if len(store) > MaxStoreLen || len(key) > MaxKeyLen {
		return nil, ErrInvalidConfig
	}
	if len(sum) != digest.Len() {
		return nil, ErrInvalidConfig
	}
	dst = append(dst, Magic...)
	dst = append(dst, 0, byte(digest))
	dst = binary.BigEndian.AppendUint64(dst, size)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(store)))
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(key)))
	dst = append(dst, store...)
	dst = append(dst, key...)
	return append(dst, sum...), nil
}

// String renders a reference for a log line or a dead-letter header. It is
// not a parseable form and does not include the digest's bytes.
func (r Ref) String() string {
	return r.Digest.String() + " " + string(r.Store) + "/" + string(r.Key)
}

// KeyString is the reference's key as a string, for handing to a store.
func (r Ref) KeyString() string { return string(r.Key) }

// verify checks a fetched payload against the reference.
//
// The size is compared first because it is the cheap half, and the digest
// with subtle.ConstantTimeCompare because this is a comparison against a
// value an attacker may be trying to hit. The timing here almost certainly
// does not matter -- an attacker who can iterate on a bucket has better
// options -- and using the constant-time form costs nothing and removes the
// question.
func (r Ref) verify(payload []byte) error {
	if uint64(len(payload)) != r.Size {
		return ErrSizeMismatch
	}
	if r.Digest == DigestNone {
		return nil
	}
	sum := sha256.Sum256(payload)
	if subtle.ConstantTimeCompare(sum[:], r.Sum) != 1 {
		return ErrDigestMismatch
	}
	return nil
}

// SumHex is the reference's digest in hex, for a log line.
func (r Ref) SumHex() string { return hex.EncodeToString(r.Sum) }
