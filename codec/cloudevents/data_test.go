package cloudevents_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"

	"github.com/iqhive/iqmq/codec"
	ce "github.com/iqhive/iqmq/codec/cloudevents"
	"github.com/iqhive/iqmq/internal/errcode"
)

// TestStructuredMarshalRefusesDataThatIsNotJSON is the reported defect: the
// SDK writes data under a JSON media type, or under none, after `"data":` as
// it stands. So an event from a binary-mode delivery whose body was not JSON,
// or a hand-built one, re-encoded to a body that was not JSON with no error,
// and one whose data was `1,"type":"evil.type"` re-encoded to valid JSON whose
// extra members overrode the event's type and source once it was decoded: a
// producer that controls only the body of a binary event rewrote the
// attributes that routing reads.
func TestStructuredMarshalRefusesDataThatIsNotJSON(t *testing.T) {
	c := ce.New()
	app := c.(codec.Appender)
	for _, tc := range []struct {
		name, dct, data string
	}{
		{"not JSON", "application/json", "abc"},
		{"no datacontenttype", "", "abc"},
		{"a JSON type in another case", "Application/JSON; charset=utf-8", "abc"},
		{"a JSON type once Unicode lower-cases it", "applİcation/json", "abc"},
		{"members that override the attributes", "application/json", `1,"type":"evil.type","source":"/evil"`},
		{"two values", "text/json", `1 2`},
		{"an unterminated object", "", `{"a":1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := binaryHeaders("ce-")
			if tc.dct != "" {
				headers = append(headers, ce.Header{Name: "content-type", Value: tc.dct})
			}
			var fromBinary event.Event
			if err := ce.NewBinding().Decode([]byte(tc.data), headers, &fromBinary); err != nil {
				t.Fatalf("Binding.Decode: %v", err)
			}
			byHand := event.New(ce.SpecVersion)
			byHand.SetID("1")
			byHand.SetSource("/s")
			byHand.SetType("t")
			if tc.dct != "" {
				byHand.SetDataContentType(tc.dct)
			}
			byHand.DataEncoded = []byte(tc.data)
			for name, e := range map[string]*event.Event{"from a binding": &fromBinary, "by hand": &byHand} {
				body, err := c.Marshal(e)
				if err == nil {
					var back event.Event
					uerr := c.Unmarshal(body, &back)
					t.Fatalf("%s: Marshal = %s, no error (valid JSON %v; decodes as type %q source %q, %v); want "+
						"an InvalidArgument error", name, body, json.Valid(body), back.Type(), back.Source(), uerr)
				}
				if code := errcode.CodeOf(err); code != errcode.InvalidArgument {
					t.Fatalf("%s: Marshal error %v carries %v, want InvalidArgument", name, err, code)
				}
				dst := []byte("P")
				if out, aerr := app.AppendMarshal(dst, e); aerr == nil || !bytes.Equal(out, dst) {
					t.Fatalf("%s: AppendMarshal = %q, %v; want the caller's buffer and an error", name, out, aerr)
				}
			}
		})
	}
	// An empty data member is no JSON value either.
	e := sample(t)
	e.DataEncoded = []byte{}
	if body, err := c.Marshal(e); err == nil {
		t.Fatalf("empty data under application/json: Marshal = %s, no error", body)
	}
}

// TestStructuredMarshalWritesEachJSONValue holds the refusal above to data
// that is not JSON: every kind of JSON value is written, space around it
// included, and data under a media type that is not JSON is written as a
// string whatever it holds.
func TestStructuredMarshalWritesEachJSONValue(t *testing.T) {
	c := ce.New()
	for _, tc := range []struct {
		dct, data string
	}{
		{"application/json", `{"a":[1,2]}`},
		{"", `"abc"`},
		{"text/json", `12.5e3`},
		{"application/json", ` true `},
		{"application/json", `null`},
		{"Application/JSON", `[]`},
		{"text/plain", `abc`},
		{"text/plain", `1,"type":"evil.type"`},
	} {
		e := event.New(ce.SpecVersion)
		e.SetID("1")
		e.SetSource("/s")
		e.SetType("t")
		if tc.dct != "" {
			e.SetDataContentType(tc.dct)
		}
		e.DataEncoded = []byte(tc.data)
		body, err := c.Marshal(&e)
		if err != nil {
			t.Fatalf("Marshal of %q data under %q: %v", tc.data, tc.dct, err)
		}
		if !json.Valid(body) {
			t.Fatalf("Marshal of %q data under %q = %s, which is not JSON", tc.data, tc.dct, body)
		}
		var back event.Event
		if err := c.Unmarshal(body, &back); err != nil {
			t.Fatalf("Unmarshal(%s): %v", body, err)
		}
		if back.Type() != "t" || back.Source() != "/s" {
			t.Fatalf("Unmarshal(%s) = type %q source %q", body, back.Type(), back.Source())
		}
	}
}

// TestStructuredUnmarshalRefusesDataThatIsNotJSON is the same defect on the
// way in: the SDK's decoder does not check a number it skips, so a body whose
// data was `-` or `1e` under a JSON media type was accepted, and its data was
// then written back as it stood, into a body that is not JSON.
func TestStructuredUnmarshalRefusesDataThatIsNotJSON(t *testing.T) {
	c := ce.New()
	for _, dct := range []string{"", "application/json", "Application/JSON", "application/vnd.acme+json"} {
		for _, data := range []string{`-`, `1e`, `-e`} {
			member := ""
			if dct != "" {
				member = `,"datacontenttype":` + strconv.Quote(dct)
			}
			body := `{"specversion":"1.0","id":"a","source":"/s","type":"t"` + member + `,"data":` + data + `}`
			var e event.Event
			err := c.Unmarshal([]byte(body), &e)
			if err == nil {
				out, merr := c.Marshal(&e)
				t.Fatalf("Unmarshal(%s) accepted data %q; it re-encodes as %s, %v", body, e.Data(), out, merr)
			}
			var de *codec.DecodeError
			if !errors.As(err, &de) || errcode.CodeOf(err) != errcode.CorruptMessage {
				t.Fatalf("Unmarshal(%s) = %v, want a CorruptMessage *codec.DecodeError", body, err)
			}
		}
	}
}

// TestPlusJSONDataIsReadAsJSON is the reported defect: a media type with the
// +json structured syntax suffix (RFC 6839) is JSON, and the CloudEvents JSON
// format writes JSON data as a JSON value, as the Java, Python and JS SDKs do.
// sdk-go reads data under such a type only as a JSON string, so a structured
// event from another SDK with object data was refused as corrupt and
// dead-lettered. The string form sdk-go writes is still read as before, and
// Marshal still writes that form.
func TestPlusJSONDataIsReadAsJSON(t *testing.T) {
	const attrs = `"specversion":"1.0","id":"0","source":"/s","type":"t"`
	c := ce.New()
	for _, dct := range []string{"application/vnd.acme+json", "application/merge-patch+json; charset=utf-8", "Application/Problem+JSON"} {
		for _, data := range []string{`{"a":[1,2]}`, `[1,"x"]`, `12`, `false`} {
			for _, order := range []string{"type first", "data first"} {
				t.Run(dct+"/"+data+"/"+order, func(t *testing.T) {
					q := strconv.Quote(dct)
					body := `{` + attrs + `,"datacontenttype":` + q + `,"data":` + data + `}`
					if order == "data first" {
						body = `{"data":` + data + `,"datacontenttype":` + q + `,` + attrs + `}`
					}
					var e event.Event
					if err := c.Unmarshal([]byte(body), &e); err != nil {
						t.Fatalf("Unmarshal(%s): %v", body, err)
					}
					if string(e.Data()) != data || e.DataContentType() != dct {
						t.Fatalf("Unmarshal(%s) = data %s under %q; want %s under %q", body, e.Data(), e.DataContentType(), data, dct)
					}
					out, err := c.Marshal(&e)
					if err != nil {
						t.Fatal(err)
					}
					if want := `"data":` + strconv.Quote(data); !bytes.Contains(out, []byte(want)) {
						t.Fatalf("Marshal = %s; want the data as sdk-go writes it, %s", out, want)
					}
					var again event.Event
					if err := c.Unmarshal(out, &again); err != nil {
						t.Fatalf("the codec cannot decode its own output %s: %v", out, err)
					}
					if !bytes.Equal(again.Data(), e.Data()) {
						t.Fatalf("the second decode gave data %s, want %s", again.Data(), e.Data())
					}
					if second, err := c.Marshal(&again); err != nil || !bytes.Equal(second, out) {
						t.Fatalf("the second round trip = %s, %v; want %s", second, err, out)
					}
				})
			}
		}
	}
	// DataAs, the SDK's accessor, reads the data under a lower-case type.
	body := `{` + attrs + `,"datacontenttype":"application/vnd.acme+json","data":{"a":7}}`
	var e event.Event
	if err := c.Unmarshal([]byte(body), &e); err != nil {
		t.Fatal(err)
	}
	var v struct {
		A int `json:"a"`
	}
	if err := e.DataAs(&v); err != nil || v.A != 7 {
		t.Fatalf("DataAs = %+v, %v; want a 7", v, err)
	}
	// The string form sdk-go writes keeps its reading: the string's contents.
	body = `{` + attrs + `,"datacontenttype":"application/vnd.acme+json","data":"{\"a\":7}"}`
	e = event.Event{}
	if err := c.Unmarshal([]byte(body), &e); err != nil || string(e.Data()) != `{"a":7}` {
		t.Fatalf("Unmarshal(%s) = data %s, %v; want {\"a\":7}", body, e.Data(), err)
	}
}
