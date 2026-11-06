package snssqs_test

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
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/snssqs/internal/snsfake"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// The SDK's default retryer repeats a request after a connection reset, a
// throttle or a server error, so the attempt whose failure a call returns is
// not always the one SNS accepted. These tests put the SDK's own retry loop
// between the driver and the fake, so a publication can be accepted by one
// attempt and answered by another.

// sdkRetried is the SNS fake with the SDK's retry loop in front of its
// publications. Once armed, each Publish and PublishBatch goes through a real
// *sns.Client, with the options the driver passed, and answer decides what
// each attempt meets. An attempt that calls accept has the fake take the
// publication as it would have taken it from the driver, and fan it out.
type sdkRetried struct {
	*snsfake.Server
	answer   func(attempt int, accept func()) (*http.Response, error)
	armed    atomic.Bool
	attempts atomic.Int64
}

func (r *sdkRetried) Publish(ctx context.Context, in *awssns.PublishInput, opts ...func(*awssns.Options)) (*awssns.PublishOutput, error) {
	if !r.armed.Load() {
		return r.Server.Publish(ctx, in, opts...)
	}
	accept := func() { _, _ = r.Server.Publish(ctx, in) }
	return r.client(accept).Publish(ctx, in, opts...)
}

func (r *sdkRetried) PublishBatch(ctx context.Context, in *awssns.PublishBatchInput, opts ...func(*awssns.Options)) (*awssns.PublishBatchOutput, error) {
	if !r.armed.Load() {
		return r.Server.PublishBatch(ctx, in, opts...)
	}
	accept := func() { _, _ = r.Server.PublishBatch(ctx, in) }
	return r.client(accept).PublishBatch(ctx, in, opts...)
}

// client is an SDK client with the default retryer's attempts and retryable
// errors, as the driver builds one, but no wait between attempts. Its HTTP
// transport answers each attempt with r.answer.
func (r *sdkRetried) client(accept func()) *awssns.Client {
	attempt := 0
	return awssns.New(awssns.Options{
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
			return r.answer(attempt, accept)
		})},
	})
}

// transport is an http.RoundTripper made of a function.
type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// What an attempt can meet.
var (
	noSuchHost   = &net.DNSError{Err: "no such host", Name: "sns.fake-region-1.amazonaws.com", IsNotFound: true}
	lookupFailed = &net.DNSError{Err: "server misbehaving", Name: "sns.fake-region-1.amazonaws.com", IsTemporary: true}
)

// acceptedThenReset is an attempt SNS accepts before the connection is reset,
// so its answer never arrives.
func acceptedThenReset(accept func()) (*http.Response, error) {
	accept()
	return nil, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
}

// unresolved is an attempt whose dial could not look the endpoint up.
func unresolved(dns *net.DNSError) (*http.Response, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: dns}
}

// refused is SNS refusing a request it read: the query protocol's error
// document, with the code SNS gives a caller that may not publish.
func refused() (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Header:     http.Header{"Content-Type": []string{"text/xml"}},
		Body: io.NopCloser(strings.NewReader(`<ErrorResponse><Error><Type>Sender</Type>` +
			`<Code>AuthorizationError</Code><Message>synthetic</Message></Error>` +
			`<RequestId>r</RequestId></ErrorResponse>`)),
	}, nil
}

// retriedPublications is every way a publication's attempts can go that these
// tests cover. accepted says whether one of them left it with SNS.
var retriedPublications = []struct {
	name     string
	answer   func(attempt int, accept func()) (*http.Response, error)
	accepted bool
}{
	{"accepted, then the retry found no such host", func(n int, accept func()) (*http.Response, error) {
		if n == 1 {
			return acceptedThenReset(accept)
		}
		return unresolved(noSuchHost)
	}, true},
	{"accepted, then every retry's lookup failed", func(n int, accept func()) (*http.Response, error) {
		if n == 1 {
			return acceptedThenReset(accept)
		}
		return unresolved(lookupFailed)
	}, true},
	{"accepted, then the retry was refused", func(n int, accept func()) (*http.Response, error) {
		if n == 1 {
			return acceptedThenReset(accept)
		}
		return refused()
	}, true},
	// An answer no code rule reads: a gateway in front of SNS, or a success
	// cut off on the way back. Each was a definite CodeUnknown, and when the
	// retry after one then failed its lookup, a definite and retryable
	// failure.
	{"accepted, then a gateway's 502 page on every attempt", acceptedThenUndecoded(gatewayPage(http.StatusBadGateway)), true},
	{"accepted, then a gateway's 504 page on every attempt", acceptedThenUndecoded(gatewayPage(http.StatusGatewayTimeout)), true},
	{"accepted, then a 503 with no body on every attempt", acceptedThenUndecoded(gatewayEmpty(http.StatusServiceUnavailable)), true},
	{"accepted, then a 500 with no body on every attempt", acceptedThenUndecoded(gatewayEmpty(http.StatusInternalServerError)), true},
	{"accepted, then a gateway's 502 page, then the retry found no such host", func(n int, accept func()) (*http.Response, error) {
		if n == 1 {
			accept()
			return gatewayError(http.StatusBadGateway, true)
		}
		return unresolved(noSuchHost)
	}, true},
	{"no such host on the only attempt", func(int, func()) (*http.Response, error) {
		return unresolved(noSuchHost)
	}, false},
	{"every attempt's lookup failed", func(int, func()) (*http.Response, error) {
		return unresolved(lookupFailed)
	}, false},
}

// TestAPublicationTheSDKRetriedIsClassifiedByEveryAttempt: SNS accepted the
// first attempt and its answer was lost, and the SDK's retry then failed on
// its own. When that retry's dial could not resolve the endpoint, the
// publication was reported as a definite, retryable failure, because a name
// that did not resolve is definite whatever sentAlready says, and a caller
// that repeated it sent every subscribed queue a second copy. A refusal the
// last attempt was given was definite the same way.
func TestAPublicationTheSDKRetriedIsClassifiedByEveryAttempt(t *testing.T) {
	for _, tc := range retriedPublications {
		t.Run(tc.name, func(t *testing.T) {
			queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			server := snsfake.New(snsfake.Options{Region: "fake-region-1", AccountID: "000000000000", Queues: queues})
			api := &sdkRetried{Server: server, answer: tc.answer}
			h := openOver(t, api, server, queues)
			tp := topic(t, "sdk-retried")
			h.provision(tp, "a")
			want := 0
			if tc.accepted {
				want = 1
			}

			api.armed.Store(true)
			err := h.client.Publish(t.Context(), tp, &mq.Message{Body: []byte("once")})
			t.Logf("%d attempt(s): %v", api.attempts.Load(), err)
			if got := h.depth(tp, "a"); got != want {
				t.Fatalf("setup: the subscription's queue holds %d message(s), want %d", got, want)
			}
			if err == nil {
				t.Fatal("Publish succeeded although every attempt failed")
			}
			if mq.IsAmbiguous(err) != tc.accepted {
				t.Fatalf("Publish = %v (ambiguous %v, retryable %v); want ambiguous %v: an attempt the SDK made before "+
					"the last one may have been accepted, and a caller that repeats a definite failure duplicates it",
					err, mq.IsAmbiguous(err), mq.IsRetryable(err), tc.accepted)
			}
			var e *mq.OpError
			if !errors.As(err, &e) {
				t.Fatalf("Publish = %v, want an *mq.OpError", err)
			}
			// The driver's judgement of the last attempt is still on the
			// error: a lookup that failed is transient, a refusal is not.
			if transient := !strings.Contains(tc.name, "refused"); e.Retryable != transient {
				t.Fatalf("Publish = %v with Retryable %v, want %v", err, e.Retryable, transient)
			}
			if !tc.accepted && !mq.IsRetryable(err) {
				t.Fatalf("Publish = %v is not safe to repeat, although no attempt reached SNS", err)
			}
		})
	}
}

// TestABatchTheSDKRetriedIsClassifiedByEveryAttempt is the same rule for a
// PublishBatch that failed as a whole: every entry of the chunk may be on its
// way to every queue.
func TestABatchTheSDKRetriedIsClassifiedByEveryAttempt(t *testing.T) {
	for _, tc := range retriedPublications {
		t.Run(tc.name, func(t *testing.T) {
			queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			server := snsfake.New(snsfake.Options{Region: "fake-region-1", AccountID: "000000000000", Queues: queues})
			api := &sdkRetried{Server: server, answer: tc.answer}
			h := openOver(t, api, server, queues)
			tp := topic(t, "sdk-retried-batch")
			h.provision(tp, "a")
			p, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: tp})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = p.Close() }()
			want := 0
			if tc.accepted {
				want = 2
			}

			api.armed.Store(true)
			err = p.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
			t.Logf("%d attempt(s): %v", api.attempts.Load(), err)
			if got := h.depth(tp, "a"); got != want {
				t.Fatalf("setup: the subscription's queue holds %d message(s), want %d", got, want)
			}
			if err == nil {
				t.Fatal("PublishBatch succeeded although every attempt failed")
			}
			if mq.IsAmbiguous(err) != tc.accepted {
				t.Fatalf("PublishBatch = %v (ambiguous %v, retryable %v); want ambiguous %v",
					err, mq.IsAmbiguous(err), mq.IsRetryable(err), tc.accepted)
			}
		})
	}
}

// partlyFailed is SNS answering a two-entry PublishBatch with the first entry
// taken and the second in the Failed list, as a server fault.
func partlyFailed() (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/xml"}},
		Body: io.NopCloser(strings.NewReader(`<PublishBatchResponse><PublishBatchResult>` +
			`<Successful><member><Id>0</Id><MessageId>m-0</MessageId></member></Successful>` +
			`<Failed><member><Id>1</Id><Code>InternalError</Code><Message>synthetic</Message>` +
			`<SenderFault>false</SenderFault></member></Failed>` +
			`</PublishBatchResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></PublishBatchResponse>`)),
	}, nil
}

// TestABatchEntryTheSDKRetriedIsClassifiedByEveryAttempt: SNS accepted a
// whole PublishBatch on the first attempt and its answer was lost, and the
// SDK's retry was answered with one entry in the Failed list. That entry was
// reported as a definite, retryable failure, though the first attempt may have
// sent it to every subscribed queue, and a caller that repeated it sent each
// queue a second copy. When the first attempt failed its name lookup, nothing
// was accepted, and the entry stays definite.
func TestABatchEntryTheSDKRetriedIsClassifiedByEveryAttempt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		first    func(accept func()) (*http.Response, error)
		accepted bool
	}{
		{"accepted, then the retry failed an entry", acceptedThenReset, true},
		{"the lookup failed, then the retry failed an entry", func(func()) (*http.Response, error) {
			return unresolved(lookupFailed)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			answer := func(n int, accept func()) (*http.Response, error) {
				if n == 1 {
					return tc.first(accept)
				}
				return partlyFailed()
			}
			queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			server := snsfake.New(snsfake.Options{Region: "fake-region-1", AccountID: "000000000000", Queues: queues})
			api := &sdkRetried{Server: server, answer: answer}
			h := openOver(t, api, server, queues)
			tp := topic(t, "sdk-retried-entry")
			h.provision(tp, "a")
			p, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: tp})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = p.Close() }()
			want := 0
			if tc.accepted {
				want = 2
			}

			api.armed.Store(true)
			err = p.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
			t.Logf("%d attempt(s): %v", api.attempts.Load(), err)
			if got := h.depth(tp, "a"); got != want {
				t.Fatalf("setup: the subscription's queue holds %d message(s), want %d", got, want)
			}
			var be *mq.BatchError
			if !errors.As(err, &be) || len(be.Errs) != 2 {
				t.Fatalf("PublishBatch = %v, want a *BatchError with two entries", err)
			}
			if be.Errs[0] != nil {
				t.Fatalf("entry 0, which the retry's answer lists as taken, = %v", be.Errs[0])
			}
			if mq.IsAmbiguous(be.Errs[1]) != tc.accepted {
				t.Fatalf("entry 1 = %v (ambiguous %v, retryable %v); want ambiguous %v: an attempt the SDK made "+
					"before the last one may have been accepted, and a caller that repeats a definite failure duplicates it",
					be.Errs[1], mq.IsAmbiguous(be.Errs[1]), mq.IsRetryable(be.Errs[1]), tc.accepted)
			}
			var e *mq.OpError
			if !errors.As(be.Errs[1], &e) || !e.Retryable {
				t.Fatalf("entry 1 = %v, want the driver's judgement of a server fault, retryable", be.Errs[1])
			}
		})
	}
}

// gatewayError is something between the client and SNS, a load balancer, an
// API gateway or a reverse proxy, answering with status and a page of its
// own, or with no body at all. The SDK cannot decode either as SNS's error
// document, and its standard retryer retries 500, 502, 503 and 504 all the
// same.
func gatewayError(status int, page bool) (*http.Response, error) {
	header, body := http.Header{}, ""
	if page {
		header.Set("Content-Type", "text/html")
		body = "<html><head><title>" + http.StatusText(status) + "</title></head><body><h1>" +
			http.StatusText(status) + "</h1></body></html>"
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
}

// cutOff is a success whose body was cut off on the way back, which the SDK
// cannot decode: SNS took the publication and its answer did not arrive whole.
func cutOff() (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/xml"}},
		Body:       io.NopCloser(strings.NewReader(`<PublishResponse><PublishResult><MessageId>m-`)),
	}, nil
}

// acceptedThenUndecoded is a publication SNS accepts on its first attempt,
// with every attempt answered by answer, which the SDK cannot decode.
func acceptedThenUndecoded(answer func() (*http.Response, error)) func(int, func()) (*http.Response, error) {
	return func(n int, accept func()) (*http.Response, error) {
		if n == 1 {
			accept()
		}
		return answer()
	}
}

// gatewayPage and gatewayEmpty are gatewayError with a page and with no body.
func gatewayPage(status int) func() (*http.Response, error) {
	return func() (*http.Response, error) { return gatewayError(status, true) }
}

func gatewayEmpty(status int) func() (*http.Response, error) {
	return func() (*http.Response, error) { return gatewayError(status, false) }
}

// TestAPublicationWhoseAnswerWasCutOffIsAmbiguous: SNS accepted a Publish or
// a PublishBatch and fanned it out, and its answer came back cut off, which
// the SDK cannot decode. No rule claimed it, so the publication was reported
// as a definite, non-retryable CodeUnknown, and a caller that repeated it sent
// every subscribed queue a second copy. It is kept apart from
// retriedPublications because the XML decoder's text for it reads like a
// compiler's to hack/pinsweep.py; the Retryable flag it shares with the
// gateway cases is pinned there.
func TestAPublicationWhoseAnswerWasCutOffIsAmbiguous(t *testing.T) {
	queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	server := snsfake.New(snsfake.Options{Region: "fake-region-1", AccountID: "000000000000", Queues: queues})
	api := &sdkRetried{Server: server, answer: acceptedThenUndecoded(cutOff)} //nolint:bodyclose // the SDK reads and closes each answer's body
	h := openOver(t, api, server, queues)
	tp := topic(t, "cut-off")
	h.provision(tp, "a")
	p, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: tp})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = p.Close() }()

	api.armed.Store(true)
	err = h.client.Publish(t.Context(), tp, &mq.Message{Body: []byte("once")})
	if got := h.depth(tp, "a"); got != 1 {
		t.Fatalf("setup: the subscription's queue holds %d message(s), want 1", got)
	}
	if !mq.IsAmbiguous(err) || mq.CodeOf(err) != mq.CodeUnavailable {
		t.Fatalf("Publish answered with a cut-off success = %v (code %v, ambiguous %v), want an ambiguous "+
			"CodeUnavailable: SNS accepted it", err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
	err = p.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
	if got := h.depth(tp, "a"); got != 3 {
		t.Fatalf("setup: the subscription's queue holds %d message(s), want 3", got)
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("PublishBatch answered with a cut-off success = %v (ambiguous false), though SNS accepted it", err)
	}
}
