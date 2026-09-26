// Package fuzzkit generates message-shaped test data from fuzz input.
//
// Every generator reads its decisions from a fuzzutil.Reader, so the fuzzer
// steers the shape of the data (nil versus empty bodies, duplicate header
// keys, hot keys, hierarchical topic names, maximum-length values) while the
// generators guarantee the structural properties a system under test relies
// on: GenTopic always yields a valid portable topic name, GenPayload never
// aliases the fuzz input, sizes respect Limits. The Put functions are the
// exact mirrors of the generators so that seed corpora can be authored in
// code and remain readable, in the same way fuzzutil.Writer mirrors
// fuzzutil.Reader.
//
// The package depends on the standard library and the internal packages
// only; it never imports the root package, so Layer-0 and Layer-1 fuzz
// targets can use it.
package fuzzkit

import (
	"slices"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/payload"
	"github.com/iqhive/iqmq/internal/topicname"
)

// Limits bounds what the generators produce. A zero field means the
// corresponding Default value.
type Limits struct {
	// MaxKey is the maximum key length in bytes.
	MaxKey int
	// MaxBody is the maximum body length in bytes.
	MaxBody int
	// MaxHeaders is the maximum number of headers.
	MaxHeaders int
	// MaxHeaderKey is the maximum header key length in bytes.
	MaxHeaderKey int
	// MaxHeaderValue is the maximum header value length in bytes.
	MaxHeaderValue int
	// MaxID is the maximum message ID length in bytes.
	MaxID int
	// MaxSegments is the maximum number of topic-name segments.
	MaxSegments int
	// MaxSegment is the maximum length of one topic-name segment.
	MaxSegment int
}

// Default holds the limits used for zero Limits fields.
var Default = Limits{
	MaxKey:         32,
	MaxBody:        4096,
	MaxHeaders:     8,
	MaxHeaderKey:   16,
	MaxHeaderValue: 64,
	MaxID:          32,
	MaxSegments:    4,
	MaxSegment:     16,
}

// normalized fills zero fields from Default and clamps the topic limits to
// the portable grammar.
func (l Limits) normalized() Limits {
	def := func(v, d int) int {
		if v <= 0 {
			return d
		}
		return v
	}
	l.MaxKey = def(l.MaxKey, Default.MaxKey)
	l.MaxBody = def(l.MaxBody, Default.MaxBody)
	l.MaxHeaders = def(l.MaxHeaders, Default.MaxHeaders)
	l.MaxHeaderKey = def(l.MaxHeaderKey, Default.MaxHeaderKey)
	l.MaxHeaderValue = def(l.MaxHeaderValue, Default.MaxHeaderValue)
	l.MaxID = def(l.MaxID, Default.MaxID)
	l.MaxSegments = def(l.MaxSegments, Default.MaxSegments)
	l.MaxSegment = min(def(l.MaxSegment, Default.MaxSegment), topicname.MaxLen)
	return l
}

// Size classes shared by the byte generators. The class is the first value
// a generator reads, so a seed author can pick the shape explicitly.
const (
	classNil   = iota // nil slice
	classEmpty        // empty, non-nil slice
	classRaw          // length and bytes taken from the input
	classPool         // a value from a small fixed pool (collisions)
	classSynth        // length from the input, bytes synthesised
	classMax          // exactly the maximum length, synthesised
	classCount
)

// keyPool holds the hot keys GenKey draws from to provoke per-key ordering
// and collision paths.
var keyPool = []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "order-42", "tenant/acme"}

// headerKeyPool holds header keys that recur so that duplicates and
// reserved-looking names appear.
var headerKeyPool = []string{"content-type", "x-trace-id", "iqmq-attempt", "a", "b", "Content-Type", "bin"}

// idPool holds message identifiers in common formats.
var idPool = []string{"id-1", "id-2", "01J8X5K7ZQ4M9N2P6R8T0V3W5Y", "550e8400-e29b-41d4-a716-446655440000"}

// topicPool holds topic names that recur so that many messages land on the
// same few topics.
var topicPool = []string{"orders", "orders/created", "events", "t0", "t1", "t2", "t3", "a.b.c", "tenant/ns/topic"}

// alphabet is the topic-name segment alphabet, letters first so that a
// zero-heavy input yields readable names.
const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"

// fill writes n deterministic bytes derived from seed into a fresh slice.
func fill(n int, seed uint32) []byte {
	b := make([]byte, n)
	x := seed | 1
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

// cloneBytes copies b into fresh storage, preserving nil versus empty.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// genBytes reads a class and produces a byte slice of at most max bytes
// that never aliases the input.
func genBytes(r *fuzzutil.Reader, max int, pool []string) []byte {
	if max < 0 {
		max = 0
	}
	switch r.Choice(classCount) {
	case classNil:
		return nil
	case classEmpty:
		return []byte{}
	case classRaw:
		return cloneBytes(r.Bytes(max))
	case classPool:
		s := pool[r.Choice(len(pool))]
		if len(s) > max {
			s = s[:max]
		}
		return []byte(s)
	case classSynth:
		if max == 0 {
			return []byte{}
		}
		return fill(r.Int(1, max), r.Uint32())
	default:
		return fill(max, r.Uint32())
	}
}

// putBytes mirrors genBytes: nil and empty use their classes, anything else
// is written raw (truncated to max).
func putBytes(w *fuzzutil.Writer, max int, b []byte) {
	switch {
	case b == nil:
		w.Choice(classCount, classNil)
	case len(b) == 0:
		w.Choice(classCount, classEmpty)
	default:
		w.Choice(classCount, classRaw)
		w.Bytes(max, b)
	}
}

// GenKey generates a message key of at most max bytes. It yields nil, empty,
// input-derived, pooled (hot), synthesised and maximum-length keys, and the
// result never aliases the input.
func GenKey(r *fuzzutil.Reader, max int) []byte { return genBytes(r, max, keyPool) }

// PutKey mirrors GenKey.
func PutKey(w *fuzzutil.Writer, max int, key []byte) { putBytes(w, max, key) }

// bodyPool holds recognisable bodies.
var bodyPool = []string{"{}", `{"id":1}`, "hello", "\x00\xff\x00\xff", "x"}

// GenBody generates a message body of at most max bytes, with the same
// shape classes as GenKey.
func GenBody(r *fuzzutil.Reader, max int) []byte { return genBytes(r, max, bodyPool) }

// PutBody mirrors GenBody.
func PutBody(w *fuzzutil.Writer, max int, body []byte) { putBytes(w, max, body) }

// genString generates a string of at most max bytes; it uses the pool for
// classPool and otherwise draws from the alphabet or the input.
func genString(r *fuzzutil.Reader, max int, pool []string) string {
	if max < 0 {
		max = 0
	}
	switch r.Choice(4) {
	case 0:
		return ""
	case 1:
		s := pool[r.Choice(len(pool))]
		if len(s) > max {
			s = s[:max]
		}
		return s
	case 2:
		if max == 0 {
			return ""
		}
		n := r.Int(1, max)
		b := make([]byte, n)
		for i := range b {
			b[i] = alphabet[r.Choice(len(alphabet))]
		}
		return string(b)
	default:
		return r.String(max)
	}
}

// putString mirrors genString: the empty string uses class 0, anything else
// is written raw (class 3, truncated to max).
func putString(w *fuzzutil.Writer, max int, s string) {
	if s == "" {
		w.Choice(4, 0)
		return
	}
	w.Choice(4, 3)
	w.String(max, s)
}

// GenHeaders generates a header table within l: nil, empty, or 1..MaxHeaders
// entries whose keys recur (duplicates) and whose values may be nil, empty,
// binary or maximum-length. Values never alias the input.
func GenHeaders(r *fuzzutil.Reader, l Limits) []payload.KV {
	l = l.normalized()
	switch r.Choice(4) {
	case 0:
		return nil
	case 1:
		return []payload.KV{}
	}
	n := r.Int(1, l.MaxHeaders)
	hs := make([]payload.KV, n)
	for i := range hs {
		hs[i].Key = genString(r, l.MaxHeaderKey, headerKeyPool)
		hs[i].Value = genBytes(r, l.MaxHeaderValue, bodyPool)
	}
	return hs
}

// PutHeaders mirrors GenHeaders. Tables longer than l.MaxHeaders are
// truncated.
func PutHeaders(w *fuzzutil.Writer, l Limits, hs []payload.KV) {
	l = l.normalized()
	switch {
	case hs == nil:
		w.Choice(4, 0)
		return
	case len(hs) == 0:
		w.Choice(4, 1)
		return
	}
	w.Choice(4, 2)
	if len(hs) > l.MaxHeaders {
		hs = hs[:l.MaxHeaders]
	}
	w.Int(1, l.MaxHeaders, len(hs))
	for i := range hs {
		putString(w, l.MaxHeaderKey, hs[i].Key)
		putBytes(w, l.MaxHeaderValue, hs[i].Value)
	}
}

// GenID generates a message identifier of at most max bytes; it is often
// empty.
func GenID(r *fuzzutil.Reader, max int) string { return genString(r, max, idPool) }

// PutID mirrors GenID.
func PutID(w *fuzzutil.Writer, max int, id string) { putString(w, max, id) }

// propPool holds values for the string properties.
var propPool = []string{"application/json", "gzip", "corr-1", "cause-1", "replies/inbox", "text/plain"}

// maxNanos is the largest nanosecond component of a time.
const maxNanos = 999_999_999

// GenProps generates message properties: mostly zero, otherwise a mix of
// set and unset fields with CreatedAt in UTC between 1970 and 2106.
func GenProps(r *fuzzutil.Reader, max int) payload.Props {
	if !r.Bool() {
		return payload.Props{}
	}
	var p payload.Props
	p.ContentType = genString(r, max, propPool)
	p.ContentEncoding = genString(r, max, propPool)
	p.CorrelationID = genString(r, max, propPool)
	p.CausationID = genString(r, max, propPool)
	p.ReplyTo = genString(r, max, propPool)
	if r.Bool() {
		p.CreatedAt = time.Unix(int64(r.Uint32()), int64(r.Int(0, maxNanos))).UTC()
	}
	return p
}

// PutProps mirrors GenProps. CreatedAt must be zero or a UTC time whose Unix
// seconds fit in 32 bits for the round trip to be exact.
func PutProps(w *fuzzutil.Writer, max int, p payload.Props) {
	if p.IsZero() {
		w.Bool(false)
		return
	}
	w.Bool(true)
	putString(w, max, p.ContentType)
	putString(w, max, p.ContentEncoding)
	putString(w, max, p.CorrelationID)
	putString(w, max, p.CausationID)
	putString(w, max, p.ReplyTo)
	if p.CreatedAt.IsZero() {
		w.Bool(false)
		return
	}
	w.Bool(true)
	w.Uint32(uint32(p.CreatedAt.Unix()))
	w.Int(0, maxNanos, p.CreatedAt.Nanosecond())
}

// GenPayload generates a complete payload within l. The result owns all of
// its storage: nothing aliases the fuzz input, and nil versus empty is
// meaningful for Key, Body and Headers.
func GenPayload(r *fuzzutil.Reader, l Limits) *payload.Payload {
	l = l.normalized()
	return &payload.Payload{
		Key:     GenKey(r, l.MaxKey),
		Body:    GenBody(r, l.MaxBody),
		Headers: GenHeaders(r, l),
		ID:      GenID(r, l.MaxID),
		Props:   GenProps(r, l.MaxID),
	}
}

// PutPayload mirrors GenPayload.
func PutPayload(w *fuzzutil.Writer, l Limits, p *payload.Payload) {
	l = l.normalized()
	PutKey(w, l.MaxKey, p.Key)
	PutBody(w, l.MaxBody, p.Body)
	PutHeaders(w, l, p.Headers)
	PutID(w, l.MaxID, p.ID)
	PutProps(w, l.MaxID, p.Props)
}

// GenTopic generates a valid portable topic name within l: either a pooled
// name or 1..MaxSegments segments of 1..MaxSegment alphabet bytes, never
// longer than topicname.MaxLen. A segment the input makes dot-only has its
// first byte replaced so the name stays valid.
func GenTopic(r *fuzzutil.Reader, l Limits) string {
	l = l.normalized()
	if r.Choice(4) == 0 {
		if name := topicPool[r.Choice(len(topicPool))]; fitsLimits(name, l) {
			return name
		}
	}
	var buf [topicname.MaxLen]byte
	n := 0
	nseg := r.Int(1, l.MaxSegments)
	for s := range nseg {
		segLen := r.Int(1, l.MaxSegment)
		if s > 0 {
			if n+1+segLen > topicname.MaxLen {
				break
			}
			buf[n] = topicname.Separator
			n++
		}
		allDots := true
		for range segLen {
			c := alphabet[r.Choice(len(alphabet))]
			if c != '.' {
				allDots = false
			}
			buf[n] = c
			n++
		}
		if allDots {
			buf[n-segLen] = alphabet[0]
		}
	}
	return string(buf[:n])
}

// fitsLimits reports whether a valid name respects the topic limits of l.
func fitsLimits(name string, l Limits) bool {
	if topicname.Depth(name) > l.MaxSegments {
		return false
	}
	for seg := range topicname.Segments(name) {
		if len(seg) > l.MaxSegment {
			return false
		}
	}
	return true
}

// PutTopic mirrors GenTopic for a valid name whose segments fit l. It panics
// on an invalid name or one that exceeds the limits, which surfaces seed
// authoring mistakes at test time.
func PutTopic(w *fuzzutil.Writer, l Limits, name string) {
	l = l.normalized()
	if err := topicname.Validate(name); err != nil {
		panic("fuzzkit: PutTopic of invalid name " + name)
	}
	depth := topicname.Depth(name)
	if depth > l.MaxSegments {
		panic("fuzzkit: PutTopic of name with too many segments " + name)
	}
	w.Choice(4, 1)
	w.Int(1, l.MaxSegments, depth)
	for seg := range topicname.Segments(name) {
		if len(seg) > l.MaxSegment {
			panic("fuzzkit: PutTopic of name with an over-long segment " + name)
		}
		w.Int(1, l.MaxSegment, len(seg))
		for i := range len(seg) {
			w.Choice(len(alphabet), alphabetIndex(seg[i]))
		}
	}
}

// alphabetIndex returns the index of c in alphabet; c must be a segment
// byte.
func alphabetIndex(c byte) int {
	for i := range len(alphabet) {
		if alphabet[i] == c {
			return i
		}
	}
	panic("fuzzkit: byte outside the topic alphabet")
}

// GenTopicUnchecked generates a candidate topic name that may be invalid:
// a valid name, raw input bytes, a valid name with one byte corrupted, or a
// structurally broken name (leading, trailing or doubled separators,
// dot-only segments, over-long names). It is the input generator for
// validation fuzzing; GenTopic is the one for systems that need valid names.
func GenTopicUnchecked(r *fuzzutil.Reader, l Limits) string {
	l = l.normalized()
	switch r.Choice(4) {
	case 0:
		return GenTopic(r, l)
	case 1:
		return r.String(topicname.MaxLen + 8)
	case 2:
		name := []byte(GenTopic(r, l))
		pos := r.Choice(len(name))
		switch r.Choice(3) {
		case 0:
			name[pos] = r.Byte()
		case 1:
			name[pos] = topicname.Separator
		default:
			name = slices.Insert(name, pos, r.Byte())
		}
		return string(name)
	default:
		base := GenTopic(r, l)
		switch r.Choice(6) {
		case 0:
			return string(topicname.Separator) + base
		case 1:
			return base + string(topicname.Separator)
		case 2:
			return base + string([]byte{topicname.Separator, topicname.Separator}) + base
		case 3:
			return base + string(topicname.Separator) + dots(r.Int(1, 3))
		case 4:
			return dots(r.Int(1, 3))
		default:
			long := make([]byte, topicname.MaxLen+r.Int(1, 8))
			for i := range long {
				long[i] = alphabet[i%len(alphabet)]
			}
			return string(long)
		}
	}
}

// dots returns n dots.
func dots(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '.'
	}
	return string(b)
}
