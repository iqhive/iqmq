package iqmq

import (
	"bytes"
	"iter"
	"strings"
	"unicode/utf8"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/payload"
)

// ReservedHeaderPrefix is the header-key prefix the specification reserves for iqmq's own
// metadata. A caller-supplied key under it is rejected rather than silently
// renamed.
//
// A DRIVER's key under it is not a caller's, and a driver that treats the two
// alike breaks the dead-letter route: see the provenance headers in dlq.go
// for the six shipped drivers on which mq.WithDeadLetter therefore cannot work.
const ReservedHeaderPrefix = "iqmq-"

// Headers is a message's header set: ordered, duplicate-preserving,
// binary-capable and case-sensitive, with the lowercase-kebab convention.
//
// The representation is the winner (ADR-006, BinarySlice): an ordered
// slice of key/value pairs whose value is a []byte. It is byte-for-byte the
// memory engine's own storage form, so crossing the engine boundary is a slice
// copy and every []byte accessor is free; the map representation lost on more
// than speed, because a map cannot keep the duplicates or the insertion order
// The specification requires at all.
//
// The zero value is an empty, usable header set. Headers is a value type, and
// a copy shares the underlying storage with the original: use Clone for an
// independent copy.
//
// Ownership follows. Add and Set store the caller's slice rather than
// copying it, because a publication does not retain caller-owned data past
// the call that copies it; Clone is the deep copy, and received header values
// are read-only and surfaced without a copy.
type Headers struct {
	pairs []payload.KV
}

// Len returns the number of pairs, counting duplicates.
func (h *Headers) Len() int { return len(h.pairs) }

// Get returns the value of the first pair with the key and reports whether the
// key is present. A present key with a nil value returns nil, true, which is
// not the same answer as an absent key.
func (h *Headers) Get(key string) ([]byte, bool) {
	for i := range h.pairs {
		if h.pairs[i].Key == key {
			return h.pairs[i].Value, true
		}
	}
	return nil, false
}

// Values returns every value stored under the key, in insertion order, or nil
// when the key is absent. The values are the stored slices, not copies.
func (h *Headers) Values(key string) [][]byte {
	n := 0
	for i := range h.pairs {
		if h.pairs[i].Key == key {
			n++
		}
	}
	if n == 0 {
		return nil
	}
	out := make([][]byte, 0, n)
	for i := range h.pairs {
		if h.pairs[i].Key == key {
			out = append(out, h.pairs[i].Value)
		}
	}
	return out
}

// Add appends a pair, keeping any existing pair with the same key and the
// order in which the pairs were added. It is the duplicate-preserving
// half, behind FeatureDuplicateHeaders: a driver that cannot represent
// duplicates rejects them at publish rather than collapsing them here.
func (h *Headers) Add(key string, val []byte) {
	h.pairs = append(h.pairs, payload.KV{Key: key, Value: val})
}

// Set replaces every value stored under the key with val, in the position the
// first of them held, and appends the pair when the key is absent. It is the portable last-write-wins contract.
//
// Set at eight headers or fewer performs no allocation unless it appends a new
// pair and the storage has to grow.
func (h *Headers) Set(key string, val []byte) {
	first := -1
	for i := range h.pairs {
		if h.pairs[i].Key != key {
			continue
		}
		if first < 0 {
			first = i
			h.pairs[i].Value = val
			continue
		}
		// A later duplicate: the set has to be compacted, which only
		// happens for a key that was added more than once.
		h.deleteAfter(key, i)
		return
	}
	if first < 0 {
		h.pairs = append(h.pairs, payload.KV{Key: key, Value: val})
	}
}

// deleteAfter removes every pair with the key from index from onwards. It is
// the compaction half of Set, kept out of line so that the common Set — a key
// present at most once — is a scan and a store.
func (h *Headers) deleteAfter(key string, from int) {
	out := h.pairs[:from]
	for _, p := range h.pairs[from:] {
		if p.Key != key {
			out = append(out, p)
		}
	}
	clearTail(h.pairs, len(out))
	h.pairs = out
}

// Delete removes every pair with the key, keeping the order of the rest.
func (h *Headers) Delete(key string) {
	for i := range h.pairs {
		if h.pairs[i].Key == key {
			h.deleteAfter(key, i)
			return
		}
	}
}

// All returns an iterator over every pair in insertion order, duplicates
// included. The values are the stored slices, not copies.
func (h *Headers) All() iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for i := range h.pairs {
			if !yield(h.pairs[i].Key, h.pairs[i].Value) {
				return
			}
		}
	}
}

// Clone returns an independent deep copy: the pair slice and every value are
// copied, so neither set can be observed through the other.
func (h *Headers) Clone() Headers {
	if len(h.pairs) == 0 {
		return Headers{}
	}
	out := Headers{pairs: make([]payload.KV, len(h.pairs))}
	for i := range h.pairs {
		out.pairs[i] = payload.KV{Key: h.pairs[i].Key, Value: bytes.Clone(h.pairs[i].Value)}
	}
	return out
}

// Reset empties the header set while keeping its storage for reuse. It clears
// the vacated pairs so that a reused set holds no reference to the keys and
// values of the message before it (the buffer-reuse mode).
func (h *Headers) Reset() {
	clearTail(h.pairs, 0)
	h.pairs = h.pairs[:0]
}

// Validate enforces the rules before any I/O: the header count and byte
// limits the driver advertises, the reserved iqmq- prefix, and — in portable
// mode — valid UTF-8 keys.
//
// Only lim.MaxHeaderCount and lim.MaxHeaderBytes are read, and each is
// enforced only when it is Known. That is the rule that an unknown limit is
// never an overloaded zero: a Limit whose Known is false means the driver does
// not advertise the limit and nothing is enforced, while a Limit whose Known is
// true and whose Value is zero means the driver permits no headers at all.
//
// The rules are applied in one fixed order — count, then bytes, then the
// per-key rules in insertion order, with the reserved prefix checked before
// key validity — so that one header set always produces one verdict.
func (h *Headers) Validate(lim Limits, portable bool) error {
	if lim.MaxHeaderCount.Known && len(h.pairs) > lim.MaxHeaderCount.Value {
		return errTooManyHeaders
	}
	if lim.MaxHeaderBytes.Known {
		var total int64
		for i := range h.pairs {
			total += int64(len(h.pairs[i].Key)) + int64(len(h.pairs[i].Value))
			if total > lim.MaxHeaderBytes.Value {
				return errHeadersTooLarge
			}
		}
	}
	for i := range h.pairs {
		key := h.pairs[i].Key
		if strings.HasPrefix(key, ReservedHeaderPrefix) {
			return errReservedHeader
		}
		if portable && !utf8.ValidString(key) {
			return errInvalidHeaderKey
		}
	}
	return nil
}

// The header-validation failures. They are distinct values with distinct
// messages and no exported name of their own: the sentinel list does not
// name one for a header rule, and errors.Is matches by code, so a caller
// compares CodeOf(err) against CodeInvalidArgument or CodeMessageTooLarge —
// or against any exported sentinel carrying that code.
var (
	errReservedHeader   = errcode.New(errcode.InvalidArgument, "iqmq: header key uses the reserved "+ReservedHeaderPrefix+" prefix")
	errInvalidHeaderKey = errcode.New(errcode.InvalidArgument, "iqmq: header key is not valid UTF-8")
	errTooManyHeaders   = errcode.New(errcode.MessageTooLarge, "iqmq: too many headers")
	errHeadersTooLarge  = errcode.New(errcode.MessageTooLarge, "iqmq: headers too large")
)

// appendPayload writes the headers into the engine's storage form. It is the
// boundary the specification chose its winner for: the representation is already the
// storage form, so the crossing is one slice copy.
func (h *Headers) appendPayload(dst []payload.KV) []payload.KV {
	return append(dst, h.pairs...)
}

// loadPayload replaces the headers with the engine's storage form, as a
// receive does. Received header values are read-only, so they are borrowed
// rather than copied.
func (h *Headers) loadPayload(src []payload.KV) {
	h.pairs = append(h.pairs[:0], src...)
}

// clearTail zeroes s from n onwards so that a shortened or reset header set
// holds no reference to the keys and values it dropped.
func clearTail(s []payload.KV, n int) {
	clear(s[n:])
}
