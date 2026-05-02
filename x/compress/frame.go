package compress

import (
	"encoding/binary"
	"errors"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Magic is the four bytes every framed body begins with.
//
// It is a version too: a second, incompatible layout would be "IQZ2" rather
// than a flag inside this one, so that an old reader rejects a new frame at
// the first four bytes instead of misreading its header. The bytes are chosen
// to be improbable in a plain payload without being impossible, which is why
// [Framed] is documented as a sniff and not a proof.
const Magic = "IQZ1"

// HeaderLen is the size of a frame header in bytes: magic, codec, flags, size.
const HeaderLen = len(Magic) + 1 + 1 + 4

// MaxDeclaredSize is the largest original length a frame can state, which is
// what a big-endian uint32 holds.
//
// The width is a deliberate mitigation and not an oversight. A varint or a
// uint64 would let a five-byte input ask for 64 GiB, which is exactly the
// defect the specification found in two codecs; a uint32 bounds the question to 4 GiB
// before [Config.MaxDecompressed] narrows it to something sane. The limit is
// what protects the process — this only bounds the arithmetic.
const MaxDeclaredSize = 1<<32 - 1

// Header is one parsed frame header. It holds no payload and no allocation.
type Header struct {
	// Codec is the registered codec id the payload was produced by.
	Codec CodecID
	// Flags is the reserved byte. [Parse] refuses a non-zero value, so a
	// parsed Header always carries zero; the field exists so that a future
	// reader can say what it saw.
	Flags uint8
	// Size is the ORIGINAL length the frame declares. It is a claim made by
	// whoever wrote the frame and is never used to size an allocation; see
	// the package documentation.
	Size uint32
}

// The errors this package reports. Each carries the code that says how
// the runner should treat it, so a caller branches on the code and a test
// branches on the sentinel.
var (
	// ErrNotFramed (CodeCorruptMessage) reports a body that does not begin
	// with a frame header: too short, or the wrong magic.
	ErrNotFramed error = errcode.New(errcode.CorruptMessage, "iqmq/x/compress: body is not framed")
	// ErrBadFlags (CodeCorruptMessage) reports a frame whose reserved flags
	// byte is not zero.
	//
	// It is refused rather than ignored on purpose. A reader that ignores a
	// flag it does not understand accepts a frame whose meaning it has not
	// got, and every such flag is then unusable for ever because the old
	// readers in the field silently mis-handle it.
	ErrBadFlags error = errcode.New(errcode.CorruptMessage, "iqmq/x/compress: frame sets a reserved flag this version does not define")
	// ErrUnknownCodec (CodeUnsupported) reports a codec id the registry does
	// not hold. It is Unsupported and not CorruptMessage because the frame
	// is well formed and the READER is what is missing — a consumer that has
	// not registered zstd is a deployment problem, not a bad message, and
	// the two want different alerts.
	ErrUnknownCodec error = errcode.New(errcode.Unsupported, "iqmq/x/compress: no codec is registered for this frame's codec id")
	// ErrTooLarge (CodeMessageTooLarge) reports a frame whose declared or
	// actual decompressed size exceeds the configured limit. Nothing was
	// allocated for it.
	ErrTooLarge error = errcode.New(errcode.MessageTooLarge, "iqmq/x/compress: decompressed size exceeds the limit")
	// ErrSizeMismatch (CodeCorruptMessage) reports a frame whose payload
	// decompressed to a length other than the one it declared: a truncated
	// stream, or a header that lied.
	ErrSizeMismatch error = errcode.New(errcode.CorruptMessage, "iqmq/x/compress: payload decompressed to a different length than the frame declared")
	// ErrInvalidConfig (CodeInvalidConfig) reports a configuration that
	// cannot work.
	ErrInvalidConfig error = errcode.New(errcode.InvalidConfig, "iqmq/x/compress: invalid configuration")
	// ErrDuplicateCodec (CodeInvalidConfig) reports two codecs registered
	// under one id.
	ErrDuplicateCodec error = errcode.New(errcode.InvalidConfig, "iqmq/x/compress: a codec is already registered for this id")
	// ErrReservedCodec (CodeInvalidConfig) reports a third-party
	// registration inside this package's reserved id range.
	ErrReservedCodec error = errcode.New(errcode.InvalidConfig, "iqmq/x/compress: codec ids below FirstUserCodec are reserved for this package")
)

// Framed reports whether b begins with a frame header this package could
// parse. It is a SNIFF and not a proof: a payload that happens to start with
// the four magic bytes answers true, which is why [Parse] checks the rest and
// why a decoder that must not guess is configured rather than sniffing (the
// same trap the specification's `MetadataAdaptive` fell into, where a native
// body beginning with the envelope's magic was mis-decoded).
func Framed(b []byte) bool {
	return len(b) >= HeaderLen && string(b[:len(Magic)]) == Magic
}

// Parse reads a frame header and returns it with the payload that follows.
//
// The returned payload aliases b; nothing is copied and nothing is allocated,
// which is what [TestAllocsParse] pins. A header that is short, mis-magicked
// or sets a reserved flag is refused here, before any codec sees the bytes.
func Parse(b []byte) (Header, []byte, error) {
	if !Framed(b) {
		return Header{}, nil, ErrNotFramed
	}
	h := Header{
		Codec: CodecID(b[4]),
		Flags: b[5],
		Size:  binary.BigEndian.Uint32(b[6:HeaderLen]),
	}
	if h.Flags != 0 {
		return Header{}, nil, ErrBadFlags
	}
	return h, b[HeaderLen:], nil
}

// AppendHeader appends a frame header for codec and size to dst.
func AppendHeader(dst []byte, codec CodecID, size uint32) []byte {
	dst = append(dst, Magic...)
	dst = append(dst, byte(codec), 0)
	return binary.BigEndian.AppendUint32(dst, size)
}

// errTooLong is the internal refusal of a body whose length does not fit the
// frame's uint32 size field. It is ErrTooLarge to a caller: from the outside
// "this body cannot be framed" and "this body exceeds the limit" are the same
// answer, and no caller can act differently on the difference.
var errTooLong = errors.Join(ErrTooLarge, errors.New("iqmq/x/compress: body is longer than a frame's size field can state"))
