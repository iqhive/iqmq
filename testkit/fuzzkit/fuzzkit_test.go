package fuzzkit

import (
	"bytes"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/internal/payload"
	"bitbucket.org/iqhive/iqmq/internal/testguard"
	"bitbucket.org/iqhive/iqmq/internal/topicname"
)

func TestNoRootImport(t *testing.T) {
	testguard.NoRootImport(t, "bitbucket.org/iqhive/iqmq/testkit/fuzzkit")
	testguard.StdlibOnly(t, "bitbucket.org/iqhive/iqmq/testkit/fuzzkit")
}

// randomInputs returns deterministic pseudo-random fuzz inputs of varied
// lengths, including very short ones that exhaust the reader early.
func randomInputs(n int) [][]byte {
	rng := rand.New(rand.NewPCG(11, 7))
	inputs := make([][]byte, n)
	for i := range inputs {
		b := make([]byte, rng.IntN(256))
		for j := range b {
			b[j] = byte(rng.IntN(256))
		}
		inputs[i] = b
	}
	return inputs
}

func TestGenTopicAlwaysValid(t *testing.T) {
	limits := []Limits{{}, {MaxSegments: 1}, {MaxSegments: 8, MaxSegment: 40}, {MaxSegments: 2, MaxSegment: 255}, {MaxSegments: 300, MaxSegment: 3}}
	hier, flat := 0, 0
	for _, data := range randomInputs(3000) {
		for _, l := range limits {
			name := GenTopic(fuzzutil.NewReader(data), l)
			if err := topicname.Validate(name); err != nil {
				t.Fatalf("GenTopic(%x, %+v) = %q: %v", data, l, name, err)
			}
			if !fitsLimits(name, l.normalized()) {
				t.Fatalf("GenTopic(%x, %+v) = %q exceeds its limits", data, l, name)
			}
			if topicname.Hierarchical(name) {
				hier++
			} else {
				flat++
			}
		}
	}
	if hier == 0 || flat == 0 {
		t.Errorf("topic shapes not covered: hierarchical=%d flat=%d", hier, flat)
	}
	// An exhausted reader still yields a valid name.
	if name := GenTopic(fuzzutil.NewReader(nil), Limits{}); topicname.Validate(name) != nil {
		t.Errorf("GenTopic on empty input = %q", name)
	}
}

func TestGenTopicUncheckedCoversBothOutcomes(t *testing.T) {
	valid, invalid := 0, 0
	for _, data := range randomInputs(2000) {
		name := GenTopicUnchecked(fuzzutil.NewReader(data), Limits{})
		if topicname.Validate(name) == nil {
			valid++
		} else {
			invalid++
		}
	}
	if valid == 0 || invalid == 0 {
		t.Errorf("GenTopicUnchecked outcomes: valid=%d invalid=%d", valid, invalid)
	}
}

func TestDeterministic(t *testing.T) {
	for _, data := range randomInputs(500) {
		a := GenPayload(fuzzutil.NewReader(data), Limits{})
		b := GenPayload(fuzzutil.NewReader(data), Limits{})
		if !payload.Equal(a, b) || !reflect.DeepEqual(a, b) {
			t.Fatalf("GenPayload not deterministic for %x:\n%+v\n%+v", data, *a, *b)
		}
		first := GenTopic(fuzzutil.NewReader(data), Limits{})
		if again := GenTopic(fuzzutil.NewReader(data), Limits{}); again != first {
			t.Fatalf("GenTopic not deterministic for %x: %q vs %q", data, first, again)
		}
	}
}

func TestNoInputAliasing(t *testing.T) {
	for _, data := range randomInputs(500) {
		orig := append([]byte{}, data...)
		p := GenPayload(fuzzutil.NewReader(data), Limits{})
		for i := range p.Key {
			p.Key[i] ^= 0xff
		}
		for i := range p.Body {
			p.Body[i] ^= 0xff
		}
		for i := range p.Headers {
			for j := range p.Headers[i].Value {
				p.Headers[i].Value[j] ^= 0xff
			}
		}
		if !bytes.Equal(orig, data) {
			t.Fatalf("mutating a generated payload changed the fuzz input")
		}
		if payload.Overlaps(p.Key, data) || payload.Overlaps(p.Body, data) {
			t.Fatal("generated slices alias the input")
		}
	}
}

func TestLimitsRespected(t *testing.T) {
	l := Limits{MaxKey: 3, MaxBody: 10, MaxHeaders: 2, MaxHeaderKey: 4, MaxHeaderValue: 5, MaxID: 6}
	for _, data := range randomInputs(1000) {
		p := GenPayload(fuzzutil.NewReader(data), l)
		if len(p.Key) > 3 || len(p.Body) > 10 || len(p.Headers) > 2 || len(p.ID) > 6 {
			t.Fatalf("limits exceeded: %+v", *p)
		}
		for _, kv := range p.Headers {
			if len(kv.Key) > 4 || len(kv.Value) > 5 {
				t.Fatalf("header limits exceeded: %+v", kv)
			}
		}
		for _, s := range []string{p.Props.ContentType, p.Props.ContentEncoding, p.Props.CorrelationID, p.Props.CausationID, p.Props.ReplyTo} {
			if len(s) > 6 {
				t.Fatalf("property limit exceeded: %q", s)
			}
		}
		if !p.Props.CreatedAt.IsZero() && p.Props.CreatedAt.Location() != time.UTC {
			t.Fatal("CreatedAt not in UTC")
		}
	}
}

func TestShapeCoverage(t *testing.T) {
	var nilKey, emptyKey, poolKey, maxKey, nilBody, emptyBody, maxBody, nilHdr, emptyHdr, dupHdr, nilVal, emptyVal, id, props, createdAt int
	for _, data := range randomInputs(4000) {
		p := GenPayload(fuzzutil.NewReader(data), Limits{})
		switch {
		case p.Key == nil:
			nilKey++
		case len(p.Key) == 0:
			emptyKey++
		case len(p.Key) == Default.MaxKey:
			maxKey++
		}
		for _, k := range keyPool {
			if string(p.Key) == k {
				poolKey++
				break
			}
		}
		switch {
		case p.Body == nil:
			nilBody++
		case len(p.Body) == 0:
			emptyBody++
		case len(p.Body) == Default.MaxBody:
			maxBody++
		}
		switch {
		case p.Headers == nil:
			nilHdr++
		case len(p.Headers) == 0:
			emptyHdr++
		}
		seen := map[string]bool{}
		for _, kv := range p.Headers {
			if seen[kv.Key] {
				dupHdr++
			}
			seen[kv.Key] = true
			switch {
			case kv.Value == nil:
				nilVal++
			case len(kv.Value) == 0:
				emptyVal++
			}
		}
		if p.ID != "" {
			id++
		}
		if !p.Props.IsZero() {
			props++
		}
		if !p.Props.CreatedAt.IsZero() {
			createdAt++
		}
	}
	for name, n := range map[string]int{
		"nil key": nilKey, "empty key": emptyKey, "pool key": poolKey, "max key": maxKey,
		"nil body": nilBody, "empty body": emptyBody, "max body": maxBody,
		"nil headers": nilHdr, "empty headers": emptyHdr, "duplicate header": dupHdr,
		"nil value": nilVal, "empty value": emptyVal, "id": id, "props": props, "created at": createdAt,
	} {
		if n == 0 {
			t.Errorf("shape %q never generated", name)
		}
	}
}

func TestPutGenRoundTrip(t *testing.T) {
	l := Limits{}
	fixed := []*payload.Payload{
		{},
		{Key: []byte{}, Body: []byte{}, Headers: []payload.KV{}},
		{Key: []byte("k"), Body: []byte("body"), Headers: []payload.KV{{Key: "a", Value: []byte("1")}, {Key: "a", Value: []byte("2")}, {Key: "n", Value: nil}, {Key: "e", Value: []byte{}}, {Key: "", Value: []byte("empty key")}}, ID: "id-1"},
		{Body: []byte{0, 255}, Props: payload.Props{ContentType: "application/json", ReplyTo: "replies", CreatedAt: time.Unix(1_700_000_000, 123_456_789).UTC()}},
		{Props: payload.Props{CorrelationID: "c"}},
	}
	// Generated payloads round-trip too: their times are already UTC.
	inputs := randomInputs(200)
	payloads := make([]*payload.Payload, 0, len(fixed)+len(inputs))
	payloads = append(payloads, fixed...)
	for _, data := range inputs {
		payloads = append(payloads, GenPayload(fuzzutil.NewReader(data), l))
	}
	for i, p := range payloads {
		w := fuzzutil.NewWriter()
		PutPayload(w, l, p)
		got := GenPayload(fuzzutil.NewReader(w.Data()), l)
		if !payload.Equal(p, got) || !reflect.DeepEqual(p, got) {
			t.Errorf("payload %d did not round-trip:\n%+v\n%+v", i, *p, *got)
		}
	}
	for _, name := range []string{"a", "orders/created", "tenant/ns/topic", "a.b-c_d", strings.Repeat("x", 16)} {
		w := fuzzutil.NewWriter()
		PutTopic(w, l, name)
		if got := GenTopic(fuzzutil.NewReader(w.Data()), l); got != name {
			t.Errorf("topic %q round-tripped as %q", name, got)
		}
	}
	for _, key := range [][]byte{nil, {}, []byte("k1"), []byte("order-42")} {
		w := fuzzutil.NewWriter()
		PutKey(w, Default.MaxKey, key)
		if got := GenKey(fuzzutil.NewReader(w.Data()), Default.MaxKey); !reflect.DeepEqual(key, got) {
			t.Errorf("key %q round-tripped as %q", key, got)
		}
	}
	w := fuzzutil.NewWriter()
	PutHeaders(w, l, nil)
	if GenHeaders(fuzzutil.NewReader(w.Data()), l) != nil {
		t.Error("nil headers did not round-trip")
	}
}

func TestPutTopicRejectsInvalid(t *testing.T) {
	for _, name := range []string{"", "a//b", "..", "a/b/c/d/e", strings.Repeat("x", 17)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("PutTopic(%q) did not panic", name)
				}
			}()
			PutTopic(fuzzutil.NewWriter(), Limits{}, name)
		}()
	}
}

func TestExhaustedReader(t *testing.T) {
	r := fuzzutil.NewReader(nil)
	p := GenPayload(r, Limits{})
	if p == nil || GenTopic(r, Limits{}) == "" || GenKey(r, 4) != nil || GenHeaders(r, Limits{}) != nil {
		t.Error("exhausted reader produced unexpected shapes")
	}
}
