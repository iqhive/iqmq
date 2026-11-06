package gcppubsub

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
)

// answering is an invoker that answers each attempt with the next of errs.
func answering(errs ...error) grpc.UnaryInvoker {
	return func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		err := errs[0]
		errs = errs[1:]
		return err
	}
}

// TestTheRecordOfFailedAttemptsKeepsNothingItNoLongerNeeds holds the record
// retriedPublishes keeps to the life of the requests it names. A call whose
// attempt succeeds is forgotten at once, the failure after a failed attempt is
// marked, and a call whose last attempt failed and was never repeated is
// forgotten once its request is unreachable, so an outage that ends every
// publication with a failure leaves nothing behind.
func TestTheRecordOfFailedAttemptsKeepsNothingItNoLongerNeeds(t *testing.T) {
	const method = "/google.pubsub.v1.Publisher/Publish"
	unavailable := status.Error(codes.Unavailable, "injected")
	refused := status.Error(codes.PermissionDenied, "injected")
	r := newRetriedPublishes()
	count := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.reqs)
	}

	req := &pubsubpb.PublishRequest{Topic: "projects/p/topics/t"}
	call := answering(unavailable, nil)
	if err := r.intercept(t.Context(), method, req, nil, nil, call); err != unavailable { //nolint:errorlint // the first failure must come back as it was
		t.Fatalf("the first failure came back as %v", err)
	}
	if err := r.intercept(t.Context(), method, req, nil, nil, call); err != nil {
		t.Fatalf("the retry that succeeded came back as %v", err)
	}
	if n := count(); n != 0 {
		t.Fatalf("%d request(s) recorded after the call succeeded", n)
	}

	call = answering(unavailable, refused)
	if err := r.intercept(t.Context(), method, req, nil, nil, call); mq.IsAmbiguous(err) {
		t.Fatalf("the failure of a first attempt reads as ambiguous: %v", err)
	}
	err := r.intercept(t.Context(), method, req, nil, nil, call)
	if !mq.IsAmbiguous(err) || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("the refusal after a failed attempt = %v (ambiguous %v, code %v), want an ambiguous PermissionDenied",
			err, mq.IsAmbiguous(err), status.Code(err))
	}

	failOnce(t, r, method)
	deadline := time.Now().Add(5 * time.Second)
	for count() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("%d request(s) recorded, want the one still reachable", count())
		}
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	runtime.KeepAlive(req)
}

// TestAMarkedFailureReadsAsItsAttemptsOwnStatus: status.FromError, which the
// library's retry decision reads, sees a retriedFailure as it sees the
// attempt's own error, a plain error that carries no status included.
func TestAMarkedFailureReadsAsItsAttemptsOwnStatus(t *testing.T) {
	for _, err := range []error{status.Error(codes.PermissionDenied, "refused"), errors.New("not a status")} {
		want, wantOK := status.FromError(err)
		if got, gotOK := status.FromError(&retriedFailure{err: err}); got.Code() != want.Code() || gotOK != wantOK {
			t.Errorf("%v marked reads as %v, %v; unmarked as %v, %v", err, got.Code(), gotOK, want.Code(), wantOK)
		}
	}
}

// failOnce records one failed attempt of a request nothing else refers to.
func failOnce(t *testing.T, r *retriedPublishes, method string) {
	t.Helper()
	req := &pubsubpb.PublishRequest{Topic: "projects/p/topics/gone"}
	_ = r.intercept(t.Context(), method, req, nil, nil, answering(status.Error(codes.DeadlineExceeded, "injected")))
}
