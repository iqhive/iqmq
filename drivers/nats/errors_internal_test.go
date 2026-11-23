package nats

import (
	"fmt"
	"testing"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
)

// TestATooManyTokensRefusalIsInvalidArgument holds the server's refusal of a
// subscription for its tokens to InvalidArgument through the operation rules
// that can meet it and through the fallback, which read every permissions
// violation as Forbidden before the shape rules. A refusal for permissions
// stays Forbidden.
func TestATooManyTokensRefusalIsInvalidArgument(t *testing.T) {
	m := newErrorMapper()
	tokens := fmt.Errorf("%w: Permissions Violation for Subscription to %q, too many tokens", natsgo.ErrPermissionViolation, "a.b.c.d")
	denied := fmt.Errorf("%w: Permissions Violation for Subscription to %q using queue %q", natsgo.ErrPermissionViolation, "a.b", "q")
	for _, op := range []string{mq.OpOpen, mq.OpFetch, mq.OpPublish, mq.OpAck} {
		if got := m.Map(op, "t", "q", tokens); mq.CodeOf(got) != mq.CodeInvalidArgument || mq.IsRetryable(got) {
			t.Errorf("%s: a refusal for tokens maps to %v (retryable %v), want a definite InvalidArgument", op, mq.CodeOf(got), mq.IsRetryable(got))
		}
		if got := m.Map(op, "t", "q", denied); mq.CodeOf(got) != mq.CodeForbidden {
			t.Errorf("%s: a refusal for permissions maps to %v, want Forbidden", op, mq.CodeOf(got))
		}
	}
}
