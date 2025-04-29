package wire

import (
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/internal/payload"
)

// wildPayload is fuzzPayload without the sanitising, and with the field sizes
// drawn AROUND the default limits rather than below them.
//
// fuzzPayload deliberately produces only payloads the encoder and decoder both
// accept — it sanitises invalid UTF-8 and keeps every field inside the defaults
// — which is why FuzzWireRoundTrip could never have found the and the specification's
// hole. This one is the other half: the payloads a driver is handed by an
// application and hands to Encode without looking.
func wildPayload(r *fuzzutil.Reader) *payload.Payload {
	p := &payload.Payload{}
	switch r.Choice(3) {
	case 1:
		p.Key = []byte{}
	case 2:
		p.Key = r.Bytes(300)
	}
	switch r.Choice(3) {
	case 1:
		p.Body = []byte{}
	case 2:
		p.Body = r.Bytes(512)
	}
	switch r.Choice(3) {
	case 1:
		p.Headers = []payload.KV{}
	case 2:
		n := r.Int(0, 6)
		p.Headers = make([]payload.KV, n)
		for i := range p.Headers {
			p.Headers[i].Key = r.String(300)
			if r.Bool() {
				p.Headers[i].Value = r.Bytes(300)
			}
		}
	}
	p.ID = r.String(300)
	p.Props.ContentType = r.String(64)
	p.Props.ContentEncoding = r.String(64)
	p.Props.CorrelationID = r.String(64)
	p.Props.CausationID = r.String(64)
	p.Props.ReplyTo = r.String(64)
	if r.Bool() {
		p.Props.CreatedAt = time.Unix(int64(r.Uint64()>>2)-(1<<40), int64(r.Uint32()%1e9)).UTC()
	}
	return p
}

// wildLimits draws a limit set from the fuzzer: the defaults, or a tightened
// set that brings the bounds within reach of the payload sizes above.
//
// A zero field means the default and a negative field means unbounded, so the
// draw covers both spellings as well as the ordinary case.
func wildLimits(r *fuzzutil.Reader) *Limits {
	switch r.Choice(3) {
	case 0:
		return nil
	case 1:
		return &Limits{}
	default:
		return &Limits{
			MaxKey:         r.Int(-1, 64),
			MaxBody:        r.Int(-1, 64),
			MaxID:          r.Int(-1, 64),
			MaxHeaders:     r.Int(-1, 4),
			MaxHeaderKey:   r.Int(-1, 64),
			MaxHeaderValue: r.Int(-1, 64),
			MaxProp:        r.Int(-1, 8),
		}
	}
}

// FuzzWireCheck is what holds Check and Decode to one answer.
//
// Check exists so that a caller can refuse a message before any I/O rather
// than storing an envelope nothing can read back, and it is only worth having
// if it is exactly as strict as the pair it predicts. Two failure modes matter
// and both are silent: a Check that is too lenient hands the caller the poison
// record it was called to prevent, and one that is too strict refuses
// publications the broker would have taken.
//
// So the property is an equivalence rather than an implication, over both
// layouts, over payloads that reach past the bounds and past UTF-8 validity,
// and over limit sets that include the default, the zero and the unbounded
// spellings. A validation rule added to the decoder and not to Check fails
// here, which is the drift the specification found twice in one increment.
func FuzzWireCheck(f *testing.F) {
	seedWireCheck(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		l := New(k)
		lim := wildLimits(r)
		p := wildPayload(r)

		predicted := l.Check(p, lim)
		enc, eerr := l.Encode(nil, p)
		var derr error
		if eerr == nil {
			var got payload.Payload
			derr = l.Decode(enc, &got, lim)
			if derr == nil && !payload.Equal(p, &got) {
				t.Fatalf("%s: the round trip differs for a payload Check accepted", k)
			}
		}
		switch {
		case predicted == nil && eerr != nil:
			t.Fatalf("%s: Check accepted a payload Encode refused with %v", k, eerr)
		case predicted == nil && derr != nil:
			t.Fatalf("%s: Check accepted a payload Decode refused with %v — "+
				"an envelope the encoder wrote and the decoder will not read is a record "+
				"that can be neither delivered nor settled", k, derr)
		case predicted != nil && eerr == nil && derr == nil:
			t.Fatalf("%s: Check refused (%v) a payload that encoded and decoded perfectly "+
				"well: a driver that trusts it declines publications the broker would "+
				"have taken", k, predicted)
		}
	})
}

// seedWireCheck commits the inputs that found the hole, one per skin, for both
// layouts: a header key and an identifier one byte over their bounds, and
// invalid UTF-8 in each of the three kinds of string field the decoder
// validates.
func seedWireCheck(f *testing.F) {
	over := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = 'x'
		}
		return string(b)
	}
	// write lays out one payload in wildPayload's own field order.
	write := func(w *fuzzutil.Writer, id, hkey string, hval []byte, prop string) {
		w.Choice(3, 0) // no key
		w.Choice(3, 2) // a body
		w.Bytes(512, []byte("x"))
		if hkey == "" && hval == nil {
			w.Choice(3, 0) // no headers
		} else {
			w.Choice(3, 2)
			w.Int(0, 6, 1)
			w.String(300, hkey)
			w.Bool(hval != nil)
			if hval != nil {
				w.Bytes(300, hval)
			}
		}
		w.String(300, id)
		w.String(64, prop)
		w.String(64, "")
		w.String(64, "")
		w.String(64, "")
		w.String(64, "")
		w.Bool(false)
	}
	cases := []func(*fuzzutil.Writer){
		func(w *fuzzutil.Writer) { write(w, "", "trace", []byte("abc"), "application/json") },
		func(w *fuzzutil.Writer) { write(w, over(DefaultLimits.MaxID+1), "", nil, "") },
		func(w *fuzzutil.Writer) { write(w, "", over(DefaultLimits.MaxHeaderKey+1), []byte("v"), "") },
		func(w *fuzzutil.Writer) { write(w, "\xff", "", nil, "") },
		func(w *fuzzutil.Writer) { write(w, "", "k\x80", []byte{0, 0xff}, "") },
		func(w *fuzzutil.Writer) { write(w, "", "", nil, "text/\xc3") },
	}
	for ki := range Kinds() {
		for _, limChoice := range []int{0, 2} {
			for _, c := range cases {
				w := fuzzutil.NewWriter()
				w.Choice(len(Kinds()), ki)
				w.Choice(3, limChoice)
				if limChoice == 2 {
					w.Int(-1, 64, 64)
					w.Int(-1, 64, 64)
					w.Int(-1, 64, 8)
					w.Int(-1, 4, 2)
					w.Int(-1, 64, 8)
					w.Int(-1, 64, 16)
					w.Int(-1, 8, 4)
				}
				c(w)
				f.Add(w.Data())
			}
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
}
