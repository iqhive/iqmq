package kafkago

import (
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
)

// TestTheCreationTimeSurvivesEveryMove: a move stamps the copy with its own
// time, so the creation time the original's timestamp carried has to travel
// in the marker, from the first move to the last, and a message whose
// publisher set none must not gain one.
func TestTheCreationTimeSurvivesEveryMove(t *testing.T) {
	created := time.Date(2001, 2, 3, 4, 5, 6, 789e6, time.UTC)
	original := &kgo.Message{Time: created, Headers: []kgo.Header{{Key: HeaderCreated}}}

	move := func(from *kgo.Message, at time.Time) *kgo.Message {
		to := &kgo.Message{Time: at, Headers: append([]kgo.Header(nil), from.Headers...)}
		keepCreated(to, from.Time)
		return to
	}
	decoded := func(msg *kgo.Message) time.Time {
		var m mq.Message
		Decode(&m, msg, "")
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

	unset := move(&kgo.Message{Time: created}, created.Add(time.Hour))
	if got := decoded(unset); !got.IsZero() {
		t.Fatalf("the copy of a message with no creation time has CreatedAt %v", got)
	}

	garbled := &kgo.Message{Time: created, Headers: []kgo.Header{{Key: HeaderCreated, Value: []byte("not a time")}}}
	if got := decoded(garbled); !got.Equal(created) {
		t.Fatalf("a marker whose value does not parse gave CreatedAt %v, want the message's timestamp %v", got, created)
	}
}
