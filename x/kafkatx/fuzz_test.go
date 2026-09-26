package kafkatx_test

import (
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/x/kafkatx"
)

func FuzzKafkaTxOffsets(f *testing.F) {
	seedOffsets(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		n := r.Int(0, 24)
		ds := make([]mq.DeliveryInfo, 0, n)
		topics := [...]string{"orders", "orders.retry.1", "acme.orders", ""}
		for range n {
			d := mq.DeliveryInfo{
				Topic:        topics[r.Int(0, len(topics)-1)],
				Partition:    int32(r.Int(0, 7)),
				HasPartition: r.Int(0, 4) != 0, // mostly true
				Offset:       int64(r.Int(0, 1<<20)),
				HasOffset:    r.Int(0, 4) != 0,
			}
			if r.Bool() {
				d.Source = topics[r.Int(0, len(topics)-1)]
			}
			ds = append(ds, d)
		}
		got := kafkatx.OffsetsOf(ds...)

		// Rule 3 first: what the result must NOT contain.
		want := map[string]map[int32]int64{}
		for i := range ds {
			d := &ds[i]
			if !d.HasOffset || !d.HasPartition {
				continue
			}
			topic := d.Source
			if topic == "" {
				topic = d.Topic
			}
			if topic == "" {
				continue
			}
			if want[topic] == nil {
				want[topic] = map[int32]int64{}
			}
			if next := d.Offset + 1; next > want[topic][d.Partition] {
				want[topic][d.Partition] = next
			}
		}
		if len(want) == 0 {
			if got != nil {
				t.Fatalf("OffsetsOf produced %v for deliveries that name no committable offset", got)
			}
			return
		}
		if len(got) != len(want) {
			t.Fatalf("OffsetsOf produced %d topic(s), want %d", len(got), len(want))
		}
		for topic, parts := range want {
			gotParts, ok := got[topic]
			if !ok {
				t.Fatalf("OffsetsOf lost topic %q", topic)
			}
			if len(gotParts) != len(parts) {
				t.Fatalf("topic %q has %d partition(s), want %d", topic, len(gotParts), len(parts))
			}
			for p, off := range parts {
				if gotParts[p] != off {
					t.Fatalf("%q/%d committed as %d, want %d (the offset AFTER the highest delivery)", topic, p, gotParts[p], off)
				}
				if gotParts[p] <= 0 {
					t.Fatalf("%q/%d committed as %d: a committable offset is at least one", topic, p, gotParts[p])
				}
			}
		}
	})
}

// seedOffsets commits the shapes that matter: nothing at all, one delivery,
// an out-of-order pair on one partition, a delivery with no offset, and one
// whose Source overrides its Topic.
func seedOffsets(f *testing.F) {
	f.Helper()
	add := func(build func(*fuzzutil.Writer)) {
		w := fuzzutil.NewWriter()
		build(w)
		f.Add(w.Data())
	}
	// A delivery, written in the order the fuzz function reads it.
	delivery := func(w *fuzzutil.Writer, topic, part, hasPart, off, hasOff, src int) {
		w.Int(0, 3, topic)
		w.Int(0, 7, part)
		w.Int(0, 4, hasPart)
		w.Int(0, 1<<20, off)
		w.Int(0, 4, hasOff)
		if src >= 0 {
			w.Bool(true)
			w.Int(0, 3, src)
		} else {
			w.Bool(false)
		}
	}
	add(func(w *fuzzutil.Writer) { w.Int(0, 24, 0) })
	add(func(w *fuzzutil.Writer) {
		w.Int(0, 24, 1)
		delivery(w, 0, 0, 1, 7, 1, -1)
	})
	add(func(w *fuzzutil.Writer) {
		w.Int(0, 24, 2)
		delivery(w, 0, 0, 1, 7, 1, -1)
		delivery(w, 0, 0, 1, 3, 1, -1)
	})
	add(func(w *fuzzutil.Writer) {
		w.Int(0, 24, 1)
		delivery(w, 0, 5, 1, 0, 0, -1) // no offset: nothing to commit
	})
	add(func(w *fuzzutil.Writer) {
		w.Int(0, 24, 1)
		delivery(w, 0, 0, 0, 9, 1, -1) // no partition
	})
	add(func(w *fuzzutil.Writer) {
		w.Int(0, 24, 1)
		delivery(w, 3, 2, 1, 9, 1, 2) // an empty Topic with a Source
	})
	add(func(w *fuzzutil.Writer) {
		w.Int(0, 24, 3)
		delivery(w, 0, 0, 1, 1, 1, -1)
		delivery(w, 1, 1, 1, 2, 1, -1)
		delivery(w, 2, 2, 1, 3, 1, -1)
	})
}
