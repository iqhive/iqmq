package jetstream

import (
	"testing"

	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
)

// TestAServerRefusalOfATTLIsInvalidArgument holds the server's answer to a TTL
// it cannot store, Nats-TTL or Nats-Schedule-TTL, to the caller's argument: the
// server stored nothing, so it is definite, and the same TTL fails again. The
// driver refuses a TTL below one second before any I/O, so the server's
// refusal (JSMessageTTLInvalid, 10165) reaches a caller only from a server
// that reads the header differently. It read as CodeUnknown.
func TestAServerRefusalOfATTLIsInvalidArgument(t *testing.T) {
	m := newErrorMapper()
	for _, refusal := range []*js.APIError{
		{Code: 400, ErrorCode: 10165, Description: "invalid per-message TTL"},
		{Code: 400, ErrorCode: js.JSErrCodeScheduleTTLInvalid, Description: "message schedules invalid per-message TTL"},
	} {
		err := m.MapNet(mq.OpPublish, "orders", "", refusal, true)
		if mq.CodeOf(err) != mq.CodeInvalidArgument || mq.IsAmbiguous(err) || mq.IsRetryable(err) {
			t.Errorf("the server's refusal %v mapped to %v (code %v, ambiguous %v, retryable %v), want a definite InvalidArgument",
				refusal, err, mq.CodeOf(err), mq.IsAmbiguous(err), mq.IsRetryable(err))
		}
	}
}
