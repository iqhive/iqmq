package avro_test

import (
	"math/rand/v2"
	"reflect"
	"testing"

	iqavro "github.com/iqhive/iqmq/codec/avro"
)

// The targets and bodies of TestSkippingTheRepairChangesNothing. propRec has
// a nullable []byte (b) and []int64 (l), a plain []byte (x), a list of records
// with one of each (items), an any (p) whose schema differs per arm, and a
// pointer to its own type under a nullable union (q). propWriter is the same
// record as the codec writes it, with p's type a parameter.
type (
	propItem struct {
		Opt []int64 `avro:"opt"`
		Raw []byte  `avro:"raw"`
	}
	propRec struct {
		P     any        `avro:"p"`
		Q     *propRec   `avro:"q"`
		B     []byte     `avro:"b"`
		L     []int64    `avro:"l"`
		X     []byte     `avro:"x"`
		Items []propItem `avro:"items"`
	}
	propInner struct {
		B []byte  `avro:"b"`
		L []int64 `avro:"l"`
	}
	innerBytesOnly struct {
		B []byte `avro:"b"`
	}
	propWriter[P any] struct {
		P     P              `avro:"p"`
		Q     *propWriter[P] `avro:"q"`
		B     []byte         `avro:"b"`
		L     []int64        `avro:"l"`
		X     []byte         `avro:"x"`
		Items []propItem     `avro:"items"`
	}
	// propShared decodes two schema fields each into b and l: x and y name
	// them as aliases.
	propShared struct {
		B []byte  `avro:"b"`
		L []int64 `avro:"l"`
	}
	propSharedWriter struct {
		X []byte  `avro:"x"`
		B []byte  `avro:"b"`
		Y []int64 `avro:"y"`
		L []int64 `avro:"l"`
	}
)

// propSchema is the record with the schema of p given.
func propSchema(p string) string {
	return `{"type":"record","name":"Prop","namespace":"iqmq.test","fields":[
  {"name":"p","type":` + p + `},
  {"name":"q","type":["null","Prop"]},
  {"name":"b","type":["null","bytes"]},
  {"name":"l","type":["null",{"type":"array","items":"long"}]},
  {"name":"x","type":"bytes"},
  {"name":"items","type":{"type":"array","items":{"type":"record","name":"Item","fields":[
    {"name":"opt","type":["null",{"type":"array","items":"long"}]},{"name":"raw","type":"bytes"}]}}}]}`
}

// propBytes, propLongs and propItems are random field values: nil, empty or
// up to a few hundred elements, so that a list outgrows what an earlier one
// left.
func propBytes(r *rand.Rand) []byte {
	switch r.IntN(4) {
	case 0:
		return nil
	case 1:
		return []byte{}
	default:
		return make([]byte, 1+r.IntN(300))
	}
}

func propLongs(r *rand.Rand) []int64 {
	switch r.IntN(4) {
	case 0:
		return nil
	case 1:
		return []int64{}
	default:
		return make([]int64, 1+r.IntN(300))
	}
}

func propItems(r *rand.Rand) []propItem {
	items := make([]propItem, r.IntN(4))
	for i := range items {
		items[i] = propItem{Opt: propLongs(r), Raw: propBytes(r)}
	}
	return items
}

// propBody is a random body for the arm whose p a random value of type P
// fills, with up to depth records nested through q.
func propBody[P any](r *rand.Rand, p func(*rand.Rand) P, depth int) *propWriter[P] {
	w := &propWriter[P]{P: p(r), B: propBytes(r), L: propLongs(r), X: propBytes(r), Items: propItems(r)}
	if depth > 0 && r.IntN(2) == 0 {
		w.Q = propBody(r, p, depth-1)
	}
	return w
}

// TestSkippingTheRepairChangesNothing holds the rule that skips the repair
// after a decode (prepare) to what always repairing would leave. Each case
// builds two identical targets from one seed: zero, or reused after an earlier
// body, and then pointed into themselves in one of the ways that let one
// decode give a nullable slice capacity before a null meets it (an any holding
// one of the target's own slices or the target itself, q pointing at the
// target, a schema field whose alias names another field). Both decode the
// same random body; the second is then repaired as if the rule had said so.
// They must be equal: where the rule skipped, nothing needed repair.
func TestSkippingTheRepairChangesNothing(t *testing.T) {
	type arm struct {
		name   string
		schema string
		body   func(r *rand.Rand) any
		// alias points the target into itself, as the arm's p allows.
		alias func(r *rand.Rand, v *propRec)
	}
	ownSlice := func(r *rand.Rand, v *propRec) any {
		switch r.IntN(4) {
		case 0:
			return &v.B
		case 1:
			return &v.X
		case 2:
			if len(v.Items) > 0 {
				return &v.Items[r.IntN(len(v.Items))].Raw
			}
		}
		return nil
	}
	arms := []arm{
		{
			"p bytes", propSchema(`"bytes"`),
			func(r *rand.Rand) any { return propBody(r, propBytes, 2) },
			func(r *rand.Rand, v *propRec) {
				if s, ok := ownSlice(r, v).(*[]byte); ok {
					v.P = s
				}
			},
		},
		{
			"p array", propSchema(`{"type":"array","items":"long"}`),
			func(r *rand.Rand) any { return propBody(r, propLongs, 2) },
			func(r *rand.Rand, v *propRec) {
				switch {
				case r.IntN(2) == 0:
					v.P = &v.L
				case len(v.Items) > 0:
					v.P = &v.Items[r.IntN(len(v.Items))].Opt
				}
			},
		},
		{
			"p record", propSchema(`{"type":"record","name":"Inner","fields":[{"name":"b","type":"bytes"},{"name":"l","type":{"type":"array","items":"long"}}]}`),
			func(r *rand.Rand) any {
				return propBody(r, func(r *rand.Rand) propInner { return propInner{B: propBytes(r), L: propLongs(r)} }, 2)
			},
			func(r *rand.Rand, v *propRec) {
				switch r.IntN(3) {
				case 0:
					v.P = v
				case 1:
					v.P = (*propRec)(nil)
				}
			},
		},
		{
			"p record of bytes", propSchema(`{"type":"record","name":"Inner","fields":[{"name":"b","type":"bytes"}]}`),
			func(r *rand.Rand) any {
				return propBody(r, func(r *rand.Rand) innerBytesOnly { return innerBytesOnly{B: propBytes(r)} }, 2)
			},
			func(r *rand.Rand, v *propRec) {
				if r.IntN(2) == 0 {
					v.P = v
				}
			},
		},
		{
			"p under a union", propSchema(`["null","bytes"]`),
			func(r *rand.Rand) any { return propBody(r, propBytes, 2) },
			func(r *rand.Rand, v *propRec) { v.P = &v.B },
		},
	}
	const seeds = 600
	for _, a := range arms {
		t.Run(a.name, func(t *testing.T) {
			c := newCodec(t, iqavro.Config{Schema: a.schema})
			encode := func(v any) []byte {
				b, err := c.Marshal(v)
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			decoded := 0
			for seed := range uint64(seeds) {
				// build makes the target from the seed: the same one each time.
				build := func() *propRec {
					r := rand.New(rand.NewPCG(seed, 1))
					v := &propRec{}
					if r.IntN(3) != 0 {
						if err := c.Unmarshal(encode(a.body(r)), v); err != nil {
							t.Fatal(err)
						}
					}
					if r.IntN(4) != 0 {
						a.alias(r, v)
					}
					if r.IntN(3) == 0 {
						v.Q = v
					}
					return v
				}
				body := encode(a.body(rand.New(rand.NewPCG(seed, 2))))
				got, want := build(), build()
				gotErr, wantErr := c.Unmarshal(body, got), c.Unmarshal(body, want)
				iqavro.RepairForTest(c, want)
				if (gotErr == nil) != (wantErr == nil) {
					t.Fatalf("seed %d: Unmarshal = %v and %v", seed, gotErr, wantErr)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("seed %d: the repair was skipped where it changes the result (body %d bytes)", seed, len(body))
				}
				if gotErr == nil {
					decoded++
				}
			}
			t.Logf("%d of %d seeds decoded", decoded, seeds)
		})
	}
	t.Run("schema fields sharing a Go field", func(t *testing.T) {
		c := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Shared","namespace":"iqmq.test","fields":[
  {"name":"x","aliases":["b"],"type":"bytes"},{"name":"b","type":["null","bytes"]},
  {"name":"y","aliases":["l"],"type":{"type":"array","items":"long"}},{"name":"l","type":["null",{"type":"array","items":"long"}]}]}`})
		for seed := range uint64(seeds) {
			r := rand.New(rand.NewPCG(seed, 3))
			body, err := c.Marshal(&propSharedWriter{X: propBytes(r), B: propBytes(r), Y: propLongs(r), L: propLongs(r)})
			if err != nil {
				t.Fatal(err)
			}
			var got, want propShared
			if seed%2 == 1 {
				got, want = propShared{B: []byte("earlier"), L: []int64{1}}, propShared{B: []byte("earlier"), L: []int64{1}}
			}
			gotErr, wantErr := c.Unmarshal(body, &got), c.Unmarshal(body, &want)
			iqavro.RepairForTest(c, &want)
			if (gotErr == nil) != (wantErr == nil) || !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d: %v / %v; got B len %d cap %d, L len %d cap %d; want B len %d cap %d, L len %d cap %d", seed, gotErr, wantErr,
					len(got.B), cap(got.B), len(got.L), cap(got.L), len(want.B), cap(want.B), len(want.L), cap(want.L))
			}
		}
	})
}
