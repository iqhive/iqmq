package codectest_test

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/codec/codectest"
)

type order struct {
	ID  string `json:"id"`
	Qty int    `json:"qty"`
}

// jsonSuite is the contract applied to the root package's own JSON codec. It
// is here so that codectest is anchored to an implementation that is known to
// satisfy the pinned contract: the suite agreeing with codec's own model
// test is what makes it evidence about the four modules outside the root.
func jsonSuite(c codec.Codec) codectest.Suite {
	return codectest.Suite{
		Codec:       c,
		ContentType: codec.ContentTypeJSON,
		Cases: []codectest.Case{{
			Name:      "order",
			Value:     &order{ID: "a", Qty: 2},
			NewTarget: func() any { return new(order) },
			Check: func(target any) error {
				got := target.(*order)
				if *got != (order{ID: "a", Qty: 2}) {
					return errors.New("got " + got.ID + "/" + strconv.Itoa(got.Qty))
				}
				return nil
			},
		}},
		Unencodable: []any{make(chan int), func() {}},
		Corrupt:     [][]byte{{}, []byte("{"), []byte(`{"qty":"no"}`), []byte("[1,2")},
	}
}

func TestJSONv1(t *testing.T) { jsonSuite(mustNew(t, codec.JSONv1)).Run(t) }

func TestJSONv2(t *testing.T) {
	if !codec.JSONv2.Available() {
		t.Skip("build has no encoding/json/v2")
	}
	jsonSuite(mustNew(t, codec.JSONv2)).Run(t)
}

// ---------------------------------------------------------------- the negative

// defects are codecs that each break exactly one clause of the contract, with
// the substring the suite's failure must contain.
//
// A contract suite nobody has watched reject a violation is assumed not to
// work, and six checks in this repository turned out not to.
// Each of these was watched failing before the clause it
// breaks was relied on by a codec module.
var defects = []struct {
	name  string
	want  string
	codec func() codec.Codec
}{
	{"partial-append", "back untouched", func() codec.Codec { return partialAppend{} }},
	{"encode-decode-error", "is not a *codec.DecodeError", func() codec.Codec { return encodeDecodeError{} }},
	{"plain-corrupt-error", "want a *codec.DecodeError", func() codec.Codec { return plainCorruptError{} }},
	{"non-deterministic", "is not deterministic", func() codec.Codec { return &nonDeterministic{} }},
	{"empty-content-type", "ContentType() is empty", func() codec.Codec { return emptyContentType{} }},
	{"accepts-bad-target", "want an InvalidArgument error", func() codec.Codec { return acceptsBadTarget{} }},
	{"no-appender", "does not implement codec.Appender", func() codec.Codec { return noAppender{} }},
	{"encode-succeeds", "so it is not unencodable", func() codec.Codec { return encodesAnything{} }},
}

// TestDefects is the body TestSuiteRejectsEachDefect runs in a subprocess: in
// this process it is skipped, because every subtest is meant to fail.
func TestDefects(t *testing.T) {
	want := os.Getenv("CODECTEST_DEFECT")
	if want == "" {
		t.Skip("run by TestSuiteRejectsEachDefect in a subprocess")
	}
	for _, d := range defects {
		if d.name != want {
			continue
		}
		s := jsonSuite(d.codec())
		s.Run(t)
		return
	}
	t.Fatalf("no defect named %q", want)
}

// TestSuiteRejectsEachDefect runs the suite against each broken codec in a
// subprocess and requires it to fail with the clause's own message. It is a
// subprocess because a failing subtest fails its parent: there is no
// in-process way to assert that a *testing.T-based check failed.
func TestSuiteRejectsEachDefect(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess per defect")
	}
	for _, d := range defects {
		t.Run(d.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestDefects$", "-test.v")
			cmd.Env = append(os.Environ(), "CODECTEST_DEFECT="+d.name)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("the suite PASSED a codec that breaks %q; it cannot fail\n%s", d.name, out)
			}
			var ee *exec.ExitError
			if !errors.As(err, &ee) {
				t.Fatalf("running the subprocess: %v\n%s", err, out)
			}
			if !strings.Contains(string(out), d.want) {
				t.Fatalf("the suite rejected %q but not for the stated reason: want %q in\n%s", d.name, d.want, out)
			}
		})
	}
}

// good is a correct JSON codec the defects derive from by overriding one
// method each.
type good struct{}

func (good) ContentType() string { return codec.ContentTypeJSON }

func (good) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (good) Unmarshal(data []byte, v any) error {
	if _, terr := codec.Target(v); terr != nil {
		return terr
	}
	if err := json.Unmarshal(data, v); err != nil {
		return &codec.DecodeError{ContentType: codec.ContentTypeJSON, Err: err}
	}
	return nil
}

func (g good) AppendMarshal(dst []byte, v any) ([]byte, error) {
	b, err := g.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

type partialAppend struct{ good }

func (p partialAppend) AppendMarshal(dst []byte, v any) ([]byte, error) {
	// The defect protobuf's own MarshalAppend has: the partially encoded
	// buffer is returned alongside the error.
	if _, err := p.Marshal(v); err != nil {
		return append(dst, "partial"...), err
	}
	return p.good.AppendMarshal(dst, v)
}

type encodeDecodeError struct{ good }

func (e encodeDecodeError) Marshal(v any) ([]byte, error) {
	b, err := e.good.Marshal(v)
	if err != nil {
		return nil, &codec.DecodeError{ContentType: codec.ContentTypeJSON, Err: err}
	}
	return b, nil
}

func (e encodeDecodeError) AppendMarshal(dst []byte, v any) ([]byte, error) {
	b, err := e.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

type plainCorruptError struct{ good }

func (plainCorruptError) Unmarshal(data []byte, v any) error {
	if _, err := codec.Target(v); err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

type nonDeterministic struct {
	good
	n atomic.Uint64
}

func (d *nonDeterministic) Marshal(v any) ([]byte, error) {
	b, err := d.good.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(b, byte('0'+d.n.Add(1)%10)), nil
}

func (d *nonDeterministic) AppendMarshal(dst []byte, v any) ([]byte, error) {
	b, err := d.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

type emptyContentType struct{ good }

func (emptyContentType) ContentType() string { return "" }

type acceptsBadTarget struct{ good }

func (a acceptsBadTarget) Unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return &codec.DecodeError{ContentType: codec.ContentTypeJSON, Err: err}
	}
	return nil
}

// noAppender is a Codec that is not an Appender.
type noAppender struct{ good }

func (noAppender) AppendMarshal() {} // shadows nothing; the method set no longer matches Appender

type encodesAnything struct{ good }

func (encodesAnything) Marshal(any) ([]byte, error) { return []byte("{}"), nil }

func (encodesAnything) AppendMarshal(dst []byte, _ any) ([]byte, error) {
	return append(dst, "{}"...), nil
}

func mustNew(tb testing.TB, k codec.Kind) codec.Codec {
	tb.Helper()
	c, err := codec.New(k)
	if err != nil {
		tb.Fatalf("codec.New(%v): %v", k, err)
	}
	return c
}
