package sqs_test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// The service answers every send and every receive with the MD5 digest of
// each body, and the SDK refuses the whole call when one digest is not the
// digest of the body: a send the service has stored, every entry of it, or a
// receive whose messages it has already made invisible. These tests put a
// real *sqs.Client in front of the fake and give one body's answer a digest
// of something else, as a proxy that alters a body or an emulator that
// computes the digest another way would.

// skewedDigests is the fake behind a real *sqs.Client. Its answers carry the
// MD5 digest of each body, except for the bodies named in sent (in a send's
// answer) and received (in a receive's), whose digest is of something else.
// unchecked builds the client with the SDK's own check turned off, as a
// deployment whose emulator computes digests its own way would. Once mended
// is set, receives carry every digest right.
type skewedDigests struct {
	*sqsfake.Server
	sent, received map[string]bool
	unchecked      bool
	mended         atomic.Bool
}

func (s *skewedDigests) SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return s.client(func() (any, error) {
		out, err := s.Server.SendMessage(ctx, in)
		if err != nil {
			return nil, err
		}
		out.MD5OfMessageBody = digestOf(aws.ToString(in.MessageBody), s.sent)
		return out, nil
	}).SendMessage(ctx, in, opts...)
}

func (s *skewedDigests) SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return s.client(func() (any, error) {
		out, err := s.Server.SendMessageBatch(ctx, in)
		if err != nil {
			return nil, err
		}
		for i := range out.Successful {
			for _, e := range in.Entries {
				if aws.ToString(e.Id) == aws.ToString(out.Successful[i].Id) {
					out.Successful[i].MD5OfMessageBody = digestOf(aws.ToString(e.MessageBody), s.sent)
				}
			}
		}
		return out, nil
	}).SendMessageBatch(ctx, in, opts...)
}

func (s *skewedDigests) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return s.client(func() (any, error) {
		out, err := s.Server.ReceiveMessage(ctx, in)
		if err != nil {
			return nil, err
		}
		skew := s.received
		if s.mended.Load() {
			skew = nil
		}
		for i := range out.Messages {
			out.Messages[i].MD5OfBody = digestOf(aws.ToString(out.Messages[i].Body), skew)
		}
		return out, nil
	}).ReceiveMessage(ctx, in, opts...)
}

// client is an SDK client whose transport answers each request with what
// call returns, in the service's JSON protocol.
func (s *skewedDigests) client(call func() (any, error)) *sqs.Client {
	return sqs.New(sqs.Options{
		Region:                           "fake-region-1",
		Credentials:                      aws.AnonymousCredentials{},
		DisableMessageChecksumValidation: s.unchecked,
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		}),
		HTTPClient: &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
			if req.Body != nil {
				_ = req.Body.Close()
			}
			out, err := call()
			if err != nil {
				return nil, err
			}
			body, err := json.Marshal(out)
			if err != nil {
				return nil, err
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
				Body:       io.NopCloser(strings.NewReader(string(body))),
			}, nil
		})},
	})
}

// digestOf is the digest an answer carries for body: its MD5, or, for a body
// in skew, the MD5 of something else.
func digestOf(body string, skew map[string]bool) *string {
	if skew[body] {
		body += " as the service stored it"
	}
	sum := md5.Sum([]byte(body))
	return aws.String(hex.EncodeToString(sum[:]))
}

// TestASendWhoseDigestIsWrongIsAmbiguousForThatMessageAlone: the service
// stored every entry of a SendMessageBatch and listed each one as taken, but
// the digest it gave one entry was not the digest of that entry's body. The
// SDK failed the whole call, and the driver reported every entry of the chunk
// as a definite, non-retryable CodeUnknown: a caller that repeated them put
// all three on the queue twice, two of which the service had confirmed as
// sent. That one entry is on the queue, perhaps not as it was sent, which is
// an unknown outcome; the others are confirmed. A single SendMessage and a
// Reject's dead-letter send are the same with one entry.
func TestASendWhoseDigestIsWrongIsAmbiguousForThatMessageAlone(t *testing.T) {
	checkAltered := func(t *testing.T, what string, err error) {
		t.Helper()
		e, ok := errors.AsType[*mq.OpError](err)
		if !ok || !mq.IsAmbiguous(err) || e.Code != mq.CodeCorruptMessage || e.Retryable {
			t.Fatalf("%s = %v (ambiguous %v), want an ambiguous CodeCorruptMessage, not retryable: the service "+
				"stored the message, but under a digest that is not the body's", what, err, mq.IsAmbiguous(err))
		}
	}

	t.Run("a batch", func(t *testing.T) {
		api := &skewedDigests{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), sent: map[string]bool{"b": true}}
		h := openOver(t, api, api.Server)
		name := topic(t, "digest-batch")
		pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		defer func() { _ = pub.Close() }()

		err = pub.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}, {Body: []byte("c")}})
		t.Logf("PublishBatch: %v", err)
		if got := h.depth(name); got != 3 {
			t.Fatalf("setup: the queue holds %d message(s), want 3", got)
		}
		be, ok := errors.AsType[*mq.BatchError](err)
		if !ok || len(be.Errs) != 3 {
			t.Fatalf("PublishBatch = %v, want a *BatchError with three entries", err)
		}
		if be.Errs[0] != nil || be.Errs[2] != nil {
			t.Fatalf("entries 0 and 2, stored under their own digests, = %v and %v", be.Errs[0], be.Errs[2])
		}
		checkAltered(t, "entry 1", be.Errs[1])
	})

	t.Run("a single send", func(t *testing.T) {
		api := &skewedDigests{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), sent: map[string]bool{"b": true}}
		h := openOver(t, api, api.Server)
		name := topic(t, "digest-one")
		err := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("b")})
		if got := h.depth(name); got != 1 {
			t.Fatalf("setup: the queue holds %d message(s), want 1", got)
		}
		checkAltered(t, "Publish", err)
		if err := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("a")}); err != nil {
			t.Fatalf("Publish of a body whose digest is right = %v", err)
		}
	})

	t.Run("a dead-letter send", func(t *testing.T) {
		api := &skewedDigests{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), sent: map[string]bool{"b": true}}
		h := openOver(t, api, api.Server, func(c *sq.Config) {
			c.AckDeadline = time.Hour
			c.DeadLetterQueue = "dead"
			c.MaxReceiveCount = 5
		})
		name := topic(t, "digest-dlq")
		h.publish(name, &mq.Message{Body: []byte("a")})
		api.sent = map[string]bool{"a": true}
		d := h.receiveOne(h.consumer(name, 4), 5*time.Second)
		err := d.Reject()
		if got := len(h.server.Bodies("dead")); got != 1 {
			t.Fatalf("setup: the dead-letter queue holds %d message(s), want 1", got)
		}
		checkAltered(t, "Reject", err)
		if got := h.depth(name); got != 1 {
			t.Fatalf("the source queue holds %d message(s); the original must stay when its copy may be altered", got)
		}
	})

	t.Run("a client that does not check digests", func(t *testing.T) {
		api := &skewedDigests{
			Server:    sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}),
			sent:      map[string]bool{"b": true},
			unchecked: true,
		}
		h := openOver(t, api, api.Server)
		name := topic(t, "digest-unchecked")
		if err := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("b")}); err != nil {
			t.Fatalf("Publish over a client whose own options turn the check off = %v; the driver must not "+
				"check what the client was told not to", err)
		}
	})
}

// TestAReceivedMessageWhoseDigestIsWrongHoldsUpNoOther: one message of a
// receive's answer came with a digest that is not the digest of its body. The
// SDK failed the whole receive, and every message of the answer, which the
// service had already made invisible, came back only when its window passed,
// to be held up again by the same message on every receive that returned it.
// The other messages are handed out; the one whose body may not be what was
// stored is left on the queue, and comes back when its window passes, as an
// undecodable message does with no dead-letter queue. Its stored copy is
// untouched, so the queue's redrive policy can still move it.
func TestAReceivedMessageWhoseDigestIsWrongHoldsUpNoOther(t *testing.T) {
	api := &skewedDigests{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), received: map[string]bool{"b": true}}
	h := openOver(t, api, api.Server, func(c *sq.Config) { c.Pollers = 1 })
	name := topic(t, "digest-receive")
	for _, body := range []string{"a", "b", "c"} {
		h.publish(name, &mq.Message{Body: []byte(body)})
	}
	cons := h.consumer(name, 4)
	ackBodies(t, h.receive(cons, 2, 5*time.Second), "a", "c")
	h.expectNothing(cons, 200*time.Millisecond, "message whose digest is wrong handed out")
	if got := h.depth(name); got != 1 {
		t.Fatalf("the queue holds %d message(s), want the one whose digest is wrong: it must not be destroyed", got)
	}
}
