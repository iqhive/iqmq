package sqs_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// The SDK's default retryer repeats a request after a connection reset, a
// throttle or a server error, so the attempt whose failure a call returns is
// not always the one the service stored. These tests put the SDK's own retry
// loop between the driver and the fake, so a publication can be stored by one
// attempt and answered by another.

// sdkRetried is the fake with the SDK's retry loop in front of its sends. Once
// armed, each SendMessage and SendMessageBatch goes through a real
// *sqs.Client, with the options the driver passed, and answer decides what
// each attempt meets. An attempt that calls store has the fake take the send
// as it would have taken it from the driver.
type sdkRetried struct {
	*sqsfake.Server
	answer   func(attempt int, store func()) (*http.Response, error)
	armed    atomic.Bool
	attempts atomic.Int64
}

func (r *sdkRetried) SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if !r.armed.Load() {
		return r.Server.SendMessage(ctx, in, opts...)
	}
	store := func() { _, _ = r.Server.SendMessage(ctx, in) }
	return r.client(store).SendMessage(ctx, in, opts...)
}

func (r *sdkRetried) SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if !r.armed.Load() {
		return r.Server.SendMessageBatch(ctx, in, opts...)
	}
	store := func() { _, _ = r.Server.SendMessageBatch(ctx, in) }
	return r.client(store).SendMessageBatch(ctx, in, opts...)
}

// client is an SDK client with the default retryer's attempts and retryable
// errors, as the driver builds one, but no wait between attempts. Its HTTP
// transport answers each attempt with r.answer.
func (r *sdkRetried) client(store func()) *sqs.Client {
	attempt := 0
	return sqs.New(sqs.Options{
		Region:      "fake-region-1",
		Credentials: aws.AnonymousCredentials{},
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		}),
		HTTPClient: &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
			if req.Body != nil {
				_ = req.Body.Close()
			}
			attempt++
			r.attempts.Add(1)
			return r.answer(attempt, store)
		})},
	})
}

// transport is an http.RoundTripper made of a function.
type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// What an attempt can meet.
var (
	noSuchHost   = &net.DNSError{Err: "no such host", Name: "sqs.fake-region-1.amazonaws.com", IsNotFound: true}
	lookupFailed = &net.DNSError{Err: "server misbehaving", Name: "sqs.fake-region-1.amazonaws.com", IsTemporary: true}
)

// storedThenReset is an attempt the service stores before the connection is
// reset, so its answer never arrives.
func storedThenReset(store func()) (*http.Response, error) {
	store()
	return nil, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
}

// unresolved is an attempt whose dial could not look the endpoint up.
func unresolved(dns *net.DNSError) (*http.Response, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: dns}
}

// refused is the service refusing a request it read, with an error code and
// no generated type.
func refused(code string) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(`{"__type":"` + code + `","message":"synthetic"}`)),
	}, nil
}

// retriedSends is every way a send's attempts can go that these tests cover.
// stored says whether one of them left the publication with the service.
var retriedSends = []struct {
	name   string
	answer func(attempt int, store func()) (*http.Response, error)
	stored bool
}{
	{"stored, then the retry found no such host", func(n int, store func()) (*http.Response, error) {
		if n == 1 {
			return storedThenReset(store)
		}
		return unresolved(noSuchHost)
	}, true},
	{"stored, then every retry's lookup failed", func(n int, store func()) (*http.Response, error) {
		if n == 1 {
			return storedThenReset(store)
		}
		return unresolved(lookupFailed)
	}, true},
	{"stored, then the retry was refused", func(n int, store func()) (*http.Response, error) {
		if n == 1 {
			return storedThenReset(store)
		}
		return refused("AccessDenied")
	}, true},
	{"no such host on the only attempt", func(int, func()) (*http.Response, error) {
		return unresolved(noSuchHost)
	}, false},
	{"every attempt's lookup failed", func(int, func()) (*http.Response, error) {
		return unresolved(lookupFailed)
	}, false},
}

// checkRetriedSend holds one failed send to what its attempts did: ambiguous
// when one of them may have been stored, and otherwise definite. An attempt
// that failed to resolve the endpoint wrote nothing, and ClassifyNet calls it
// definite, but that says nothing about the attempts before it.
func checkRetriedSend(t *testing.T, what string, err error, stored bool) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded although every attempt failed", what)
	}
	if mq.IsAmbiguous(err) != stored {
		t.Fatalf("%s = %v (ambiguous %v, retryable %v); want ambiguous %v: an attempt the SDK made before the last one "+
			"may have been stored, and a caller that repeats a definite failure duplicates it",
			what, err, mq.IsAmbiguous(err), mq.IsRetryable(err), stored)
	}
}

// TestAPublicationTheSDKRetriedIsClassifiedByEveryAttempt: the service stored
// the first attempt and its answer was lost, and the SDK's retry then failed
// on its own. When that retry's dial could not resolve the endpoint, the
// publication was reported as a definite, retryable failure, because a name
// that did not resolve is definite whatever sentAlready says, and a caller
// that repeated it put the message on the queue twice. A refusal the last
// attempt was given was definite the same way.
func TestAPublicationTheSDKRetriedIsClassifiedByEveryAttempt(t *testing.T) {
	for _, tc := range retriedSends {
		t.Run(tc.name, func(t *testing.T) {
			api := &sdkRetried{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), answer: tc.answer}
			h := openOver(t, api, api.Server)
			name := topic(t, "sdk-retried")
			h.publish(name, &mq.Message{Body: []byte("seed")})
			want := 1
			if tc.stored {
				want = 2
			}

			api.armed.Store(true)
			err := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("once")})
			t.Logf("%d attempt(s): %v", api.attempts.Load(), err)
			if got := h.depth(name); got != want {
				t.Fatalf("setup: the queue holds %d message(s), want %d", got, want)
			}
			checkRetriedSend(t, "Publish", err, tc.stored)
			var e *mq.OpError
			if !errors.As(err, &e) {
				t.Fatalf("Publish = %v, want an *mq.OpError", err)
			}
			// The driver's judgement of the last attempt is still on the
			// error: a lookup that failed is transient, a refusal is not.
			if transient := !strings.Contains(tc.name, "refused"); e.Retryable != transient {
				t.Fatalf("Publish = %v with Retryable %v, want %v", err, e.Retryable, transient)
			}
			if !tc.stored && !mq.IsRetryable(err) {
				t.Fatalf("Publish = %v is not safe to repeat, although no attempt reached the service", err)
			}
		})
	}
}

// TestABatchTheSDKRetriedIsClassifiedByEveryAttempt is the same rule for a
// SendMessageBatch that failed as a whole: every entry of the chunk may be on
// the queue.
func TestABatchTheSDKRetriedIsClassifiedByEveryAttempt(t *testing.T) {
	for _, tc := range retriedSends {
		t.Run(tc.name, func(t *testing.T) {
			api := &sdkRetried{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), answer: tc.answer}
			h := openOver(t, api, api.Server)
			name := topic(t, "sdk-retried-batch")
			h.publish(name, &mq.Message{Body: []byte("seed")})
			pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = pub.Close() }()
			want := 1
			if tc.stored {
				want = 3
			}

			api.armed.Store(true)
			err = pub.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
			t.Logf("%d attempt(s): %v", api.attempts.Load(), err)
			if got := h.depth(name); got != want {
				t.Fatalf("setup: the queue holds %d message(s), want %d", got, want)
			}
			checkRetriedSend(t, "PublishBatch", err, tc.stored)
		})
	}
}

// TestADeadLetterSendTheSDKRetriedIsClassifiedByEveryAttempt is the
// dead-letter copy a Reject sends. Reported as definite, the Reject read as
// one that had not happened, and a retried Reject sent a second copy while
// the first was on the dead-letter queue.
func TestADeadLetterSendTheSDKRetriedIsClassifiedByEveryAttempt(t *testing.T) {
	for _, tc := range retriedSends {
		t.Run(tc.name, func(t *testing.T) {
			api := &sdkRetried{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), answer: tc.answer}
			h := openOver(t, api, api.Server, func(c *sq.Config) {
				c.AckDeadline = time.Hour
				c.DeadLetterQueue = "dead"
				c.MaxReceiveCount = 5
			})
			name := topic(t, "sdk-retried-dlq")
			h.publish(name, &mq.Message{Body: []byte("seed")})
			d := h.receiveOne(h.consumer(name, 4), 5*time.Second)
			want := 0
			if tc.stored {
				want = 1
			}

			api.armed.Store(true)
			err := d.Reject()
			attempts := api.attempts.Load()
			t.Logf("%d attempt(s): %v", attempts, err)
			if got := len(h.server.Bodies("dead")); got != want {
				t.Fatalf("setup: the dead-letter queue holds %d message(s), want %d", got, want)
			}
			checkRetriedSend(t, "Reject", err, tc.stored)
			if got := h.depth(name); got != 1 {
				t.Fatalf("the source queue holds %d message(s) after a failed dead-letter send; the original must stay", got)
			}
			if !tc.stored {
				return
			}
			again := d.Reject()
			if mq.CodeOf(again) != mq.CodeSettlementUnknown && !errors.Is(again, mq.ErrSettlementUnknown) {
				t.Fatalf("a second Reject after an ambiguous one = %v, want the sticky unknown", again)
			}
			if got := api.attempts.Load() - attempts; got != 0 {
				t.Fatalf("the retried Reject made %d more attempt(s) at a dead-letter copy that may be stored", got)
			}
		})
	}
}

// partlyFailed is the service answering a two-entry SendMessageBatch with the
// first entry taken and the second in the Failed list, as a server fault.
func partlyFailed() (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body: io.NopCloser(strings.NewReader(`{"Successful":[{"Id":"0","MessageId":"m-0"}],` +
			`"Failed":[{"Id":"1","SenderFault":false,"Code":"InternalError","Message":"synthetic"}]}`)),
	}, nil
}

// TestABatchEntryTheSDKRetriedIsClassifiedByEveryAttempt: the service stored
// a whole SendMessageBatch on the first attempt and its answer was lost, and
// the SDK's retry was answered with one entry in the Failed list. That entry
// was reported as a definite, retryable failure, though the first attempt may
// have put it on the queue, and a caller that repeated it put it there twice.
// When the first attempt failed its name lookup, nothing was stored, and the
// entry stays definite.
func TestABatchEntryTheSDKRetriedIsClassifiedByEveryAttempt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		first  func(store func()) (*http.Response, error)
		stored bool
	}{
		{"stored, then the retry failed an entry", storedThenReset, true},
		{"the lookup failed, then the retry failed an entry", func(func()) (*http.Response, error) {
			return unresolved(lookupFailed)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			answer := func(n int, store func()) (*http.Response, error) {
				if n == 1 {
					return tc.first(store)
				}
				return partlyFailed()
			}
			api := &sdkRetried{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), answer: answer}
			h := openOver(t, api, api.Server)
			name := topic(t, "sdk-retried-entry")
			h.publish(name, &mq.Message{Body: []byte("seed")})
			pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = pub.Close() }()
			want := 1
			if tc.stored {
				want = 3
			}

			api.armed.Store(true)
			err = pub.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
			t.Logf("%d attempt(s): %v", api.attempts.Load(), err)
			if got := h.depth(name); got != want {
				t.Fatalf("setup: the queue holds %d message(s), want %d", got, want)
			}
			var be *mq.BatchError
			if !errors.As(err, &be) || len(be.Errs) != 2 {
				t.Fatalf("PublishBatch = %v, want a *BatchError with two entries", err)
			}
			if be.Errs[0] != nil {
				t.Fatalf("entry 0, which the retry's answer lists as taken, = %v", be.Errs[0])
			}
			checkRetriedSend(t, "entry 1", be.Errs[1], tc.stored)
			var e *mq.OpError
			if !errors.As(be.Errs[1], &e) || !e.Retryable {
				t.Fatalf("entry 1 = %v, want the driver's judgement of a server fault, retryable", be.Errs[1])
			}
		})
	}
}
