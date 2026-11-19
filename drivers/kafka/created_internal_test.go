package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
)

// TestTheCreationTimeSurvivesEveryMove: a move stamps the copy with its own
// time, so the creation time the original's timestamp carried has to travel
// in the marker, from the first move to the last, and a record whose
// publisher set none must not gain one.
func TestTheCreationTimeSurvivesEveryMove(t *testing.T) {
	created := time.Date(2001, 2, 3, 4, 5, 6, 789e6, time.UTC)
	original := &kgo.Record{Timestamp: created, Headers: []kgo.RecordHeader{{Key: HeaderCreated}}}

	move := func(from *kgo.Record, at time.Time) *kgo.Record {
		to := &kgo.Record{Timestamp: at, Headers: append([]kgo.RecordHeader(nil), from.Headers...)}
		keepCreated(to, from.Timestamp)
		return to
	}
	decoded := func(rec *kgo.Record) time.Time {
		var m mq.Message
		Decode(&m, rec, "")
		return m.Properties.CreatedAt
	}

	first := move(original, created.Add(time.Hour))
	if got := decoded(first); !got.Equal(created) {
		t.Fatalf("the first copy's CreatedAt = %v, want %v", got, created)
	}
	if len(original.Headers[0].Value) != 0 {
		t.Fatalf("the move wrote %q into the original's marker", original.Headers[0].Value)
	}
	second := move(first, created.Add(2*time.Hour))
	if got := decoded(second); !got.Equal(created) {
		t.Fatalf("the second copy's CreatedAt = %v, want %v: the first move's time replaced it", got, created)
	}

	unset := move(&kgo.Record{Timestamp: created}, created.Add(time.Hour))
	if got := decoded(unset); !got.IsZero() {
		t.Fatalf("the copy of a record with no creation time has CreatedAt %v", got)
	}

	garbled := &kgo.Record{Timestamp: created, Headers: []kgo.RecordHeader{{Key: HeaderCreated, Value: []byte("not a time")}}}
	if got := decoded(garbled); !got.Equal(created) {
		t.Fatalf("a marker whose value does not parse gave CreatedAt %v, want the record's timestamp %v", got, created)
	}
}
