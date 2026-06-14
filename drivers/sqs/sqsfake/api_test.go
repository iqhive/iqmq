package sqsfake

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"bitbucket.org/iqhive/iqmq/drivers/sqs/internal/sqsapi"
)

// _ is the compile-time proof that the fake and the real client are
// interchangeable.
//
// It is here rather than in the driver because the fake lives here: an
// assertion in the driver's package would be satisfied by an interface the
// driver happens to use rather than by the whole slice the driver declares.
// It is a declaration rather than a test because a test asserting a concrete
// type is non-nil asserts nothing — the compiler has already decided.
var _ sqsapi.API = (*Server)(nil)

// TestServerIsUsable is the runtime half: a server with no queues answers
// ListQueues rather than panicking, which is what a driver's Ping does first.
func TestServerIsUsable(t *testing.T) {
	out, err := New(Options{}).ListQueues(t.Context(), &sqs.ListQueuesInput{})
	if err != nil {
		t.Fatalf("ListQueues on an empty server: %v", err)
	}
	if len(out.QueueUrls) != 0 {
		t.Fatalf("an empty server reported %v", out.QueueUrls)
	}
}
