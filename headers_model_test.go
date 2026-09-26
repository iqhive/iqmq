package iqmq

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/payload"
)

// This file carries the reference model and op script across from
// x/apibench/b4, reduced to the one representation the bake-off chose. The
// model is an independent restatement of the semantics — it is not the
// implementation with the names changed — so a Headers that reorders,
// collapses, truncates or mis-counts a header fails against it.

// headerPair is one header as the model holds it.
type headerPair struct {
	key   string
	value []byte
}

// headerModel is the reference model of the header semantics: an ordered,
// duplicate-preserving list of pairs. It fixes the three things the specification leaves
// to prose:
//
//   - Add appends. A second Add of a key keeps both pairs, in the order they
//     were added.
//   - Set is last-write-wins: it replaces the value of the FIRST pair with
//     the key in place, drops every later pair with that key, and appends
//     when the key is absent. Replacing in place is what keeps the order of a
//     Set-heavy history stable, which "deterministically encoded" needs.
//   - Get returns the value of the first pair with the key; Values returns
//     every one, in insertion order.
//
// It stores copies of every value, so a Headers that handed the model a slice
// it later mutated would diverge rather than agree by accident.
type headerModel struct {
	pairs []headerPair
}

func (m *headerModel) add(key string, value []byte) {
	m.pairs = append(m.pairs, headerPair{key: key, value: bytes.Clone(value)})
}

func (m *headerModel) set(key string, value []byte) {
	first := -1
	out := m.pairs[:0]
	for _, p := range m.pairs {
		if p.key != key {
			out = append(out, p)
			continue
		}
		if first < 0 {
			first = len(out)
			out = append(out, headerPair{key: key, value: bytes.Clone(value)})
		}
	}
	m.pairs = out
	if first < 0 {
		m.pairs = append(m.pairs, headerPair{key: key, value: bytes.Clone(value)})
	}
}

func (m *headerModel) delete(key string) {
	out := m.pairs[:0]
	for _, p := range m.pairs {
		if p.key != key {
			out = append(out, p)
		}
	}
	m.pairs = out
}

func (m *headerModel) len() int { return len(m.pairs) }

func (m *headerModel) clone() *headerModel {
	out := &headerModel{pairs: make([]headerPair, len(m.pairs))}
	for i, p := range m.pairs {
		out.pairs[i] = headerPair{key: p.key, value: bytes.Clone(p.value)}
	}
	return out
}

func (m *headerModel) reset() { m.pairs = m.pairs[:0] }

func (m *headerModel) get(key string) ([]byte, bool) {
	for _, p := range m.pairs {
		if p.key == key {
			return p.value, true
		}
	}
	return nil, false
}

func (m *headerModel) values(key string) [][]byte {
	var out [][]byte
	for _, p := range m.pairs {
		if p.key == key {
			out = append(out, p.value)
		}
	}
	return out
}

// encoded is the deterministic encoding of the model: for every pair a
// uvarint key length, the key, a uvarint value length and the value. It is
// what "deterministically encoded" (the specification) is compared on, because it is the
// one observable that depends on the order of the whole set at once.
func (m *headerModel) encoded() []byte {
	var dst []byte
	for _, p := range m.pairs {
		dst = binary.AppendUvarint(dst, uint64(len(p.key)))
		dst = append(dst, p.key...)
		dst = binary.AppendUvarint(dst, uint64(len(p.value)))
		dst = append(dst, p.value...)
	}
	return dst
}

// storage is the engine storage form of the model.
func (m *headerModel) storage() []payload.KV {
	out := make([]payload.KV, 0, len(m.pairs))
	for _, p := range m.pairs {
		out = append(out, payload.KV{Key: p.key, Value: p.value})
	}
	return out
}

// validate restates the limit rules independently of Headers.Validate, in
// the one fixed order the specification's prose implies: count, then bytes,
// then the per-key rules in insertion order, with the reserved prefix checked
// before key validity.
//
// A limit is enforced only when it is Known. That is the rule that an
// unknown limit is never an overloaded zero, and it is the half no
// benchmark measured.
func (m *headerModel) validate(lim Limits, portable bool) error {
	if lim.MaxHeaderCount.Known && len(m.pairs) > lim.MaxHeaderCount.Value {
		return errTooManyHeaders
	}
	if lim.MaxHeaderBytes.Known {
		var total int64
		for _, p := range m.pairs {
			total += int64(len(p.key)) + int64(len(p.value))
			if total > lim.MaxHeaderBytes.Value {
				return errHeadersTooLarge
			}
		}
	}
	for _, p := range m.pairs {
		if strings.HasPrefix(p.key, ReservedHeaderPrefix) {
			return errReservedHeader
		}
		if portable && !utf8.ValidString(p.key) {
			return errInvalidHeaderKey
		}
	}
	return nil
}

// headerOpKind is one scripted header operation. The script is the whole
// Headers contract, so the model cannot be satisfied on a subset of it.
type headerOpKind uint8

const (
	opAdd headerOpKind = iota
	opSet
	opGet
	opValues
	opDelete
	opClone
	opReset
	opStore
	opLoad
	opValidate

	headerOpCount
)

var headerOpNames = [headerOpCount]string{
	"add", "set", "get", "values", "delete", "clone", "reset", "store", "load", "validate",
}

func (k headerOpKind) String() string {
	if k < headerOpCount {
		return headerOpNames[k]
	}
	return "op(?)"
}

// headerOp is one scripted operation.
type headerOp struct {
	kind     headerOpKind
	key      string
	value    []byte
	lim      Limits
	portable bool
}

// headerKeys is the key corpus every script draws from: an ordinary key, a
// second one so that duplicates and deletions interleave, the empty key, a
// key under the reserved prefix, a key that is not valid UTF-8, and one more
// so that a script can grow past the budget's eight headers.
var headerKeys = []string{"ct", "trace", "", ReservedHeaderPrefix + "id", "bad\xffkey", "zz"}

// headerValues is the value corpus: text, empty, nil, arbitrary bytes
// including a NUL, and a long value.
var headerValues = [][]byte{
	[]byte("application/json"),
	{},
	nil,
	{0x00, 0xff, 0x80, 0x7f},
	bytes.Repeat([]byte("v"), 100),
}

// runHeaderOps applies ops to a fresh Headers and to the model at the same
// time and fails tb on the first divergence. After every step the whole
// observable state is compared: length, iteration order, every corpus key's
// first value and all of its values, the deterministic encoding and the
// engine storage form.
func runHeaderOps(tb testing.TB, ops []headerOp) {
	tb.Helper()
	var h Headers
	m := &headerModel{}
	for i := range ops {
		op := &ops[i]
		switch op.kind {
		case opAdd:
			// The value is copied at the call site because Add stores the
			// caller's slice: opClone mutates the stored bytes to
			// prove Clone is a deep copy, and mutating the shared corpus
			// would corrupt every later step instead.
			v := bytes.Clone(op.value)
			h.Add(op.key, v)
			m.add(op.key, v)
		case opSet:
			v := bytes.Clone(op.value)
			h.Set(op.key, v)
			m.set(op.key, v)
		case opGet:
			got, ok := h.Get(op.key)
			want, wantOK := m.get(op.key)
			if ok != wantOK || !bytes.Equal(got, want) {
				tb.Fatalf("op %d: Get(%q) = (%q, %v), model (%q, %v)", i, op.key, got, ok, want, wantOK)
			}
		case opValues:
			got, want := h.Values(op.key), m.values(op.key)
			if !equalByteSlices(got, want) {
				tb.Fatalf("op %d: Values(%q) = %q, model %q", i, op.key, got, want)
			}
		case opDelete:
			h.Delete(op.key)
			m.delete(op.key)
		case opClone:
			snap := m.clone()
			c := h.Clone()
			// the specification's "independent deep copy" is three separate claims, and
			// a Clone that copied the pair slice but shared the value bytes
			// would satisfy only the first two. All three are checked:
			//
			//  1. a later write to the original is invisible to the clone;
			//  2. a write to the clone is invisible to the original;
			//  3. mutating the original's stored value BYTES in place is
			//     invisible to the clone, and the reverse.
			flipValues(&h)
			compareHeaderState(tb, i, &c, snap)
			flipValues(&h)
			flipValues(&c)
			compareHeaderState(tb, i, &h, m)
			flipValues(&c)
			c.Set("clone-only", []byte("x"))
			snap.set("clone-only", []byte("x"))
			compareHeaderState(tb, i, &h, m)
			h.Set("clone-probe", []byte("after"))
			m.set("clone-probe", []byte("after"))
			compareHeaderState(tb, i, &c, snap)
		case opReset:
			h.Reset()
			m.reset()
		case opStore:
			got, want := h.appendPayload(nil), m.storage()
			if !equalKVs(got, want) {
				tb.Fatalf("op %d: appendPayload = %v, model %v", i, got, want)
			}
		case opLoad:
			// A receive replaces the whole set with what the engine holds.
			src := m.storage()
			h.loadPayload(src)
			m.reset()
			for _, kv := range src {
				m.add(kv.Key, kv.Value)
			}
		case opValidate:
			got := h.Validate(op.lim, op.portable)
			want := m.validate(op.lim, op.portable)
			// Compared by identity: two of the four validation failures
			// carry one portable code, so errors.Is cannot tell them apart.
			// That errors.Is cannot is the contract, tested in
			// TestErrorsMatchByCode; here the question is which rule fired.
			if got != want { //nolint:errorlint // identity is the comparison this step needs
				tb.Fatalf("op %d: Validate(%+v, portable=%v) = %v, model %v", i, op.lim, op.portable, got, want)
			}
		case headerOpCount:
			tb.Fatalf("op %d: headerOpCount is the sentinel, not an operation", i)
		}
		if tb.Failed() {
			return
		}
		compareHeaderState(tb, i, &h, m)
	}
}

// compareHeaderState compares every observable of h against the model.
func compareHeaderState(tb testing.TB, op int, h *Headers, m *headerModel) {
	tb.Helper()
	if got, want := h.Len(), m.len(); got != want {
		tb.Fatalf("op %d: Len = %d, model %d", op, got, want)
		return
	}
	i := 0
	for key, val := range h.All() {
		if i >= len(m.pairs) {
			tb.Fatalf("op %d: All yielded more than the model's %d pairs", op, len(m.pairs))
			return
		}
		if key != m.pairs[i].key || !bytes.Equal(val, m.pairs[i].value) {
			tb.Fatalf("op %d: All pair %d = (%q, %q), model (%q, %q)",
				op, i, key, val, m.pairs[i].key, m.pairs[i].value)
			return
		}
		i++
	}
	if i != len(m.pairs) {
		tb.Fatalf("op %d: All yielded %d pairs, model %d", op, i, len(m.pairs))
		return
	}
	for _, key := range headerKeys {
		got, ok := h.Get(key)
		want, wantOK := m.get(key)
		if ok != wantOK || !bytes.Equal(got, want) {
			tb.Fatalf("op %d: Get(%q) = (%q, %v), model (%q, %v)", op, key, got, ok, want, wantOK)
			return
		}
		if !equalByteSlices(h.Values(key), m.values(key)) {
			tb.Fatalf("op %d: Values(%q) = %q, model %q", op, key, h.Values(key), m.values(key))
			return
		}
	}
	if got, want := appendEncodedHeaders(nil, h), m.encoded(); !bytes.Equal(got, want) {
		tb.Fatalf("op %d: encoding = %x, model %x", op, got, want)
	}
}

// appendEncodedHeaders writes the deterministic encoding of h through the
// public iteration order, so the encoding under test is the order All
// presents and not a second traversal of the storage.
func appendEncodedHeaders(dst []byte, h *Headers) []byte {
	for key, val := range h.All() {
		dst = binary.AppendUvarint(dst, uint64(len(key)))
		dst = append(dst, key...)
		dst = binary.AppendUvarint(dst, uint64(len(val)))
		dst = append(dst, val...)
	}
	return dst
}

// flipValues inverts every byte of every stored header value in place. It is
// how the model script asks whether Clone copied the value bytes or shared
// them: a shared byte is visible in both sets, and the comparison that
// follows names the header it saw change.
func flipValues(h *Headers) {
	for i := range h.pairs {
		for j := range h.pairs[i].Value {
			h.pairs[i].Value[j] ^= 0xff
		}
	}
}

func equalByteSlices(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func equalKVs(a, b []payload.KV) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

// known wraps v as an advertised limit.
func known[T any](v T) Limit[T] { return Limit[T]{Value: v, Known: true} }

// headerScripts are the deterministic op scripts Headers is checked against.
var headerScripts = []struct {
	name string
	ops  []headerOp
}{
	{
		name: "straight",
		ops: []headerOp{
			{kind: opAdd, key: "ct", value: headerValues[0]},
			{kind: opAdd, key: "trace", value: headerValues[3]},
			{kind: opGet, key: "ct"},
			{kind: opGet, key: "missing"},
			{kind: opStore},
		},
	},
	{
		// Duplicates: the requirement that Add keeps both pairs and Set
		// collapses them.
		name: "duplicates",
		ops: []headerOp{
			{kind: opAdd, key: "trace", value: []byte("one")},
			{kind: opAdd, key: "trace", value: []byte("two")},
			{kind: opAdd, key: "trace", value: []byte("three")},
			{kind: opValues, key: "trace"},
			{kind: opGet, key: "trace"},
			{kind: opSet, key: "trace", value: []byte("only")},
			{kind: opValues, key: "trace"},
			{kind: opDelete, key: "trace"},
			{kind: opValues, key: "trace"},
		},
	},
	{
		// Set in the middle of an ordered set must not move the pair.
		name: "setinplace",
		ops: []headerOp{
			{kind: opAdd, key: "a", value: []byte("1")},
			{kind: opAdd, key: "b", value: []byte("2")},
			{kind: opAdd, key: "c", value: []byte("3")},
			{kind: opSet, key: "b", value: []byte("9")},
			{kind: opSet, key: "d", value: []byte("4")},
			{kind: opDelete, key: "a"},
		},
	},
	{
		// A set that grows past the eight headers the specification budgets, is thinned
		// from the middle and both ends, cloned and reset.
		name: "grow",
		ops: []headerOp{
			{kind: opAdd, key: "h0", value: []byte("0")},
			{kind: opAdd, key: "h1", value: []byte("1")},
			{kind: opAdd, key: "h2", value: []byte("2")},
			{kind: opAdd, key: "h3", value: []byte("3")},
			{kind: opAdd, key: "h4", value: []byte("4")},
			{kind: opAdd, key: "h5", value: []byte("5")},
			{kind: opAdd, key: "h6", value: []byte("6")},
			{kind: opAdd, key: "h7", value: []byte("7")},
			{kind: opAdd, key: "h8", value: []byte("8")},
			{kind: opDelete, key: "h1"},
			{kind: opDelete, key: "h8"},
			{kind: opAdd, key: "h9", value: []byte("9")},
			{kind: opClone},
			{kind: opReset},
			{kind: opAdd, key: "h10", value: []byte("10")},
		},
	},
	{
		// Binary and empty values, the empty key, and the engine boundary in
		// both directions.
		name: "binary",
		ops: []headerOp{
			{kind: opAdd, key: "", value: headerValues[1]},
			{kind: opAdd, key: "bin", value: headerValues[3]},
			{kind: opAdd, key: "nil", value: headerValues[2]},
			{kind: opAdd, key: "long", value: headerValues[4]},
			{kind: opStore},
			{kind: opLoad},
			{kind: opStore},
		},
	},
	{
		// The limit rules, including the unknown-versus-zero distinction: an
		// unadvertised MaxHeaderCount enforces nothing, and an advertised
		// MaxHeaderCount of zero rejects the first header.
		name: "limits",
		ops: []headerOp{
			{kind: opValidate, lim: Limits{MaxHeaderCount: known(0)}},
			{kind: opAdd, key: "ct", value: headerValues[0]},
			{kind: opValidate, lim: Limits{}},
			{kind: opValidate, lim: Limits{MaxHeaderCount: known(0)}},
			{kind: opValidate, lim: Limits{MaxHeaderBytes: known(int64(0))}},
			{kind: opValidate, lim: Limits{MaxHeaderCount: known(4), MaxHeaderBytes: known(int64(64))}, portable: true},
			{kind: opAdd, key: "bad\xffkey", value: []byte("x")},
			{kind: opValidate, lim: Limits{MaxHeaderCount: known(4)}, portable: true},
			{kind: opValidate, lim: Limits{MaxHeaderCount: known(4)}},
			{kind: opAdd, key: ReservedHeaderPrefix + "id", value: []byte("y")},
			{kind: opValidate, lim: Limits{MaxHeaderCount: known(4)}},
			{kind: opValidate, lim: Limits{MaxHeaderCount: known(2)}},
			{kind: opValidate, lim: Limits{MaxHeaderBytes: known(int64(4))}},
			{kind: opDelete, key: ReservedHeaderPrefix + "id"},
			{kind: opDelete, key: "bad\xffkey"},
			{kind: opValidate, lim: Limits{MaxHeaderCount: known(4), MaxHeaderBytes: known(int64(64))}, portable: true},
		},
	},
	{
		// A clone taken while the set is empty, and a reset that has to leave
		// the storage usable.
		name: "emptyclone",
		ops: []headerOp{
			{kind: opClone},
			{kind: opReset},
			{kind: opClone},
			{kind: opAdd, key: "ct", value: headerValues[0]},
			{kind: opClone},
		},
	},
}

// TestHeadersModel runs every script against Headers and the model.
func TestHeadersModel(t *testing.T) {
	for _, sc := range headerScripts {
		t.Run(sc.name, func(t *testing.T) { runHeaderOps(t, sc.ops) })
	}
}

// Script limits keep a decoded script inside a fuzzing budget.
const (
	fuzzMaxOps    = 64
	fuzzMaxCount  = 8
	fuzzMaxBytes  = 64
	fuzzMaxSetLen = 8
	fuzzMaxKeyLen = 6
	fuzzMaxValLen = 6
)

// decodeHeaderScript turns fuzz input into an op script. It never fails:
// fuzzutil.Reader returns zeroes once the input is exhausted, so every input
// decodes to some script.
func decodeHeaderScript(data []byte) []headerOp {
	r := fuzzutil.NewReader(data)
	var ops []headerOp
	for len(ops) < fuzzMaxOps && !r.Exhausted() {
		op := headerOp{kind: headerOpKind(r.Choice(int(headerOpCount)))}
		switch op.kind {
		case opAdd, opSet:
			op.key = headerKeys[r.Choice(len(headerKeys))]
			op.value = headerValues[r.Choice(len(headerValues))]
		case opGet, opValues, opDelete:
			op.key = headerKeys[r.Choice(len(headerKeys))]
		case opValidate:
			op.lim = decodeLimits(r)
			op.portable = r.Bool()
		case opClone, opReset, opStore, opLoad, headerOpCount:
			// These take no argument.
		}
		ops = append(ops, op)
	}
	return ops
}

// encodeHeaderScript is decodeHeaderScript's mirror, so the hand-written
// scripts can seed the corpus.
func encodeHeaderScript(ops []headerOp) []byte {
	w := fuzzutil.NewWriter()
	for i := range ops {
		op := &ops[i]
		w.Choice(int(headerOpCount), int(op.kind))
		switch op.kind {
		case opAdd, opSet:
			w.Choice(len(headerKeys), indexOfHeaderKey(op.key))
			w.Choice(len(headerValues), indexOfHeaderValue(op.value))
		case opGet, opValues, opDelete:
			w.Choice(len(headerKeys), indexOfHeaderKey(op.key))
		case opValidate:
			encodeLimits(w, op.lim)
			w.Bool(op.portable)
		case opClone, opReset, opStore, opLoad, headerOpCount:
			// These take no argument.
		}
	}
	return w.Data()
}

// decodeLimits reads the two header limits, each with its own Known bit, so
// that an unadvertised limit and an advertised limit of zero are both
// reachable from fuzz input.
func decodeLimits(r *fuzzutil.Reader) Limits {
	var lim Limits
	if r.Bool() {
		lim.MaxHeaderCount = known(r.Int(0, fuzzMaxCount))
	}
	if r.Bool() {
		lim.MaxHeaderBytes = known(int64(r.Int(0, fuzzMaxBytes)))
	}
	return lim
}

func encodeLimits(w *fuzzutil.Writer, lim Limits) {
	w.Bool(lim.MaxHeaderCount.Known)
	if lim.MaxHeaderCount.Known {
		w.Int(0, fuzzMaxCount, lim.MaxHeaderCount.Value)
	}
	w.Bool(lim.MaxHeaderBytes.Known)
	if lim.MaxHeaderBytes.Known {
		w.Int(0, fuzzMaxBytes, int(lim.MaxHeaderBytes.Value))
	}
}

// indexOfHeaderKey maps a scripted key onto the corpus, so that a
// hand-written script seeds an input the decoder reproduces. A key outside
// the corpus seeds the first entry; the script itself still runs with the key
// it names, because runHeaderOps takes the script and not the encoding.
func indexOfHeaderKey(key string) int {
	for i, k := range headerKeys {
		if k == key {
			return i
		}
	}
	return 0
}

func indexOfHeaderValue(v []byte) int {
	for i, c := range headerValues {
		if bytes.Equal(c, v) {
			return i
		}
	}
	return 0
}

// FuzzHeadersOps is target 2's op-script half, re-homed to the root
// from x/apibench/b4: an arbitrary history of the whole Headers contract is
// applied to Headers and to the reference model at the same time, and length,
// iteration order, every key's values, the deterministic encoding and the
// engine storage form have to agree after every step.
//
// It is the same oracle TestHeadersModel uses, driven by arbitrary histories
// rather than by the seven scripted ones, so a Headers that is right only for
// the histories somebody thought of fails here.
func FuzzHeadersOps(f *testing.F) {
	for _, sc := range headerScripts {
		f.Add(encodeHeaderScript(sc.ops))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		ops := decodeHeaderScript(data)
		if len(ops) == 0 {
			return
		}
		runHeaderOps(t, ops)
	})
}

// limitCase is one decoded limits case: a header set and the limits to apply
// to it.
type limitCase struct {
	pairs    []headerPair
	lim      Limits
	portable bool
}

func decodeLimitCase(data []byte) limitCase {
	r := fuzzutil.NewReader(data)
	c := limitCase{lim: decodeLimits(r)}
	c.portable = r.Bool()
	n := r.Int(0, fuzzMaxSetLen)
	for range n {
		key := r.String(fuzzMaxKeyLen)
		if r.Bool() {
			// One case in two names a reserved key, which is the rule a
			// random string would almost never reach.
			key = ReservedHeaderPrefix + key
		}
		c.pairs = append(c.pairs, headerPair{key: key, value: bytes.Clone(r.Bytes(fuzzMaxValLen))})
	}
	return c
}

func encodeLimitCase(c limitCase) []byte {
	w := fuzzutil.NewWriter()
	encodeLimits(w, c.lim)
	w.Bool(c.portable)
	w.Int(0, fuzzMaxSetLen, len(c.pairs))
	for _, p := range c.pairs {
		key := p.key
		reserved := strings.HasPrefix(key, ReservedHeaderPrefix)
		if reserved {
			key = key[len(ReservedHeaderPrefix):]
		}
		if len(key) > fuzzMaxKeyLen {
			key = key[:fuzzMaxKeyLen]
		}
		w.String(fuzzMaxKeyLen, key)
		w.Bool(reserved)
		v := p.value
		if len(v) > fuzzMaxValLen {
			v = v[:fuzzMaxValLen]
		}
		w.Bytes(fuzzMaxValLen, v)
	}
	return w.Data()
}

// FuzzHeadersLimits is target 2's limits half, re-homed to the root
// from x/apibench/b4: MaxHeaderCount, MaxHeaderBytes, reserved prefix
// and portable-mode key validity, over arbitrary header sets and arbitrary
// limits — including the unadvertised limits the specification forbids reading as zero.
//
// Headers.Validate is compared with the model's restatement of the rules, and
// then the verdict is checked against a third, independent restatement, so a
// validator that agreed with a model carrying the same bug still fails.
func FuzzHeadersLimits(f *testing.F) {
	seeds := []limitCase{
		{lim: Limits{MaxHeaderCount: known(2), MaxHeaderBytes: known(int64(16))}, portable: true, pairs: []headerPair{
			{key: "ct", value: []byte("json")},
		}},
		{lim: Limits{MaxHeaderCount: known(1)}, pairs: []headerPair{
			{key: "a", value: []byte("1")}, {key: "b", value: []byte("2")},
		}},
		{lim: Limits{MaxHeaderBytes: known(int64(2))}, pairs: []headerPair{{key: "long", value: []byte("value")}}},
		// An advertised limit of zero rejects; the unadvertised limit beside
		// it enforces nothing. This pair of seeds is the unknown-versus-zero
		// rule in the corpus.
		{lim: Limits{MaxHeaderCount: known(0)}, pairs: []headerPair{{key: "a", value: nil}}},
		{lim: Limits{}, pairs: []headerPair{{key: "a", value: nil}}},
		{lim: Limits{}, pairs: []headerPair{{key: ReservedHeaderPrefix + "id", value: []byte("x")}}},
		{lim: Limits{}, portable: true, pairs: []headerPair{{key: "bad\xffkey", value: nil}}},
		{lim: Limits{}, portable: true},
	}
	for i := range seeds {
		f.Add(encodeLimitCase(seeds[i]))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		c := decodeLimitCase(data)
		var h Headers
		m := &headerModel{}
		for _, p := range c.pairs {
			h.Add(p.key, p.value)
			m.add(p.key, p.value)
		}
		got := h.Validate(c.lim, c.portable)
		want := m.validate(c.lim, c.portable)
		if got != want { //nolint:errorlint // two of the failures share a code; identity is the comparison
			t.Fatalf("Validate(%+v, portable=%v) over %d pairs = %v, model %v",
				c.lim, c.portable, len(c.pairs), got, want)
		}
		checkLimitVerdict(t, &h, c.lim, c.portable, got)
	})
}

// checkLimitVerdict restates the rules independently of both Headers and
// the model, and holds the verdict to them.
//
// The per-key rules are first-violation-wins in insertion order, and the
// reserved-prefix rule is checked before the key-validity rule for the same
// key: a set can hold both, and which one is reported depends on which comes
// first.
func checkLimitVerdict(t *testing.T, h *Headers, lim Limits, portable bool, got error) {
	t.Helper()
	var count int
	var bytesTotal int64
	var firstKeyErr error
	for key, val := range h.All() {
		count++
		bytesTotal += int64(len(key)) + int64(len(val))
		if firstKeyErr != nil {
			continue
		}
		switch {
		case strings.HasPrefix(key, ReservedHeaderPrefix):
			firstKeyErr = errReservedHeader
		case portable && !utf8.ValidString(key):
			firstKeyErr = errInvalidHeaderKey
		}
	}
	tooMany := lim.MaxHeaderCount.Known && count > lim.MaxHeaderCount.Value
	tooLarge := lim.MaxHeaderBytes.Known && bytesTotal > lim.MaxHeaderBytes.Value
	var want error
	switch {
	case tooMany:
		want = errTooManyHeaders
	case tooLarge:
		want = errHeadersTooLarge
	default:
		want = firstKeyErr
	}
	if got != want { //nolint:errorlint // identity: two of the failures share a portable code
		t.Fatalf("Validate over %d headers of %d bytes (limits %+v, portable=%v) = %v, the rules say %v",
			count, bytesTotal, lim, portable, got, want)
	}
}

// TestHeaderScriptRoundTrip pins that the seed encoder is the decoder's
// mirror: a seed that decoded to a different script than the one it was
// written for would seed the corpus with something nobody chose.
func TestHeaderScriptRoundTrip(t *testing.T) {
	for _, sc := range headerScripts {
		got := decodeHeaderScript(encodeHeaderScript(sc.ops))
		if len(got) != len(sc.ops) {
			t.Fatalf("%s: %d ops decoded as %d", sc.name, len(sc.ops), len(got))
		}
		for i := range got {
			if got[i].kind != sc.ops[i].kind {
				t.Fatalf("%s: op %d decoded as %v, want %v", sc.name, i, got[i].kind, sc.ops[i].kind)
			}
			if got[i].kind == opValidate && (got[i].lim != sc.ops[i].lim || got[i].portable != sc.ops[i].portable) {
				t.Fatalf("%s: op %d limits decoded as %+v/%v, want %+v/%v",
					sc.name, i, got[i].lim, got[i].portable, sc.ops[i].lim, sc.ops[i].portable)
			}
		}
	}
	for _, c := range []limitCase{
		{lim: Limits{MaxHeaderCount: known(3), MaxHeaderBytes: known(int64(9))}, portable: true, pairs: []headerPair{{key: "ab", value: []byte("cd")}}},
		{lim: Limits{}, pairs: []headerPair{{key: ReservedHeaderPrefix + "x", value: nil}}},
	} {
		got := decodeLimitCase(encodeLimitCase(c))
		if got.lim != c.lim || got.portable != c.portable || len(got.pairs) != len(c.pairs) {
			t.Fatalf("limit case %+v decoded as %+v", c, got)
		}
		for i := range c.pairs {
			if got.pairs[i].key != c.pairs[i].key || !bytes.Equal(got.pairs[i].value, c.pairs[i].value) {
				t.Fatalf("limit case pair %d decoded as %+v, want %+v", i, got.pairs[i], c.pairs[i])
			}
		}
	}
}
