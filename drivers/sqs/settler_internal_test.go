package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/sqs/internal/sqsapi"
)

// TestAnExpiredDeliveryLeavesTheInflightMap holds that a delivery the local
// lease check finds lost is forgotten as well: a lost delivery is terminal, so
// the in-flight map keeping it is a leak of one entry per expired message.
func TestAnExpiredDeliveryLeavesTheInflightMap(t *testing.T) {
	src := &source{b: &Backend{}, inflight: map[string]*delivery{}}
	set := &settler{src: src}
	ref := &delivery{id: "m", src: src}
	ref.expires.Store(1)
	src.supersede(ref)
	d := mq.DriverDelivery{Ref: ref}
	if err := set.Extend(t.Context(), &d, time.Second); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend of an expired delivery returned %v, want ErrLeaseLost", err)
	}
	src.mu.Lock()
	n := len(src.inflight)
	src.mu.Unlock()
	if n != 0 {
		t.Fatalf("the in-flight map holds %d lost deliveries, want 0", n)
	}
}

// visibilityRecorder is an API that counts ChangeMessageVisibility calls and
// answers nothing else, which is all an extension may reach.
type visibilityRecorder struct {
	sqsapi.API
	calls int
}

func (r *visibilityRecorder) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	r.calls++
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// TestExtendRefusesADeliveryBeingSettled holds that an extension does not
// reach the service while a settlement owns the visibility window: a requeue
// Nack's ChangeMessageVisibilityBatch sets the window to the redelivery delay,
// and an extension sent beside it would undo that delay.
func TestExtendRefusesADeliveryBeingSettled(t *testing.T) {
	for _, tc := range []struct {
		state uint32
		want  error
	}{
		{stateSettling, mq.ErrSettlementInProgress},
		{stateSettled, mq.ErrAlreadySettled},
		{stateUnknown, mq.ErrSettlementUnknown},
	} {
		api := &visibilityRecorder{}
		src := &source{b: &Backend{api: api}, inflight: map[string]*delivery{}}
		set := &settler{src: src}
		ref := &delivery{id: "m", src: src}
		ref.extend(time.Now().Add(time.Minute))
		ref.state.Store(tc.state)
		d := mq.DriverDelivery{Ref: ref}
		if err := set.Extend(t.Context(), &d, time.Second); !errors.Is(err, tc.want) {
			t.Errorf("Extend of a delivery in state %d returned %v, want %v", tc.state, err, tc.want)
		}
		if api.calls != 0 {
			t.Errorf("Extend of a delivery in state %d sent %d ChangeMessageVisibility calls, want 0: the settlement owns the window", tc.state, api.calls)
		}
	}
}
