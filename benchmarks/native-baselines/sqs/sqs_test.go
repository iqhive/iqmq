// Package sqs measures github.com/aws/aws-sdk-go-v2/service/sqs against the
// ElasticMQ container (SQS-compatible). ElasticMQ numbers are not evidence
// for managed SQS (layer 4); the baseline exists so the iqmq SQS
// driver can be compared with the raw client on the same emulator.
//
// Confirmation boundary: the SendMessage / SendMessageBatch response.
// Settlement: DeleteMessage per receipt handle (DeleteMessageBatch is the
// driver's batch settler, measured by BenchmarkSQSSettlement, not here).
//
// Two limits shape the batching leaf, both verified in the pinned SDK's
// generated documentation for SendMessageBatch: at most 10 entries per call,
// and at most 1 MiB (1,048,576 bytes) for one message and for the sum of a
// batch. A message body may contain only XML, JSON or unformatted text
// (#x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF), so
// binary payloads must be encoded; the baseline base64-encodes them, which
// is what the driver will do, and charges itself the 4/3 inflation.
package sqs

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

const (
	// maxBatchEntries is the SendMessageBatch/DeleteMessageBatch entry limit.
	maxBatchEntries = 10
	// maxBatchBytes is the total payload limit for one batch.
	maxBatchBytes = 1 << 20
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "sqs", opener(harness.ElasticMQ(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.ElasticMQ(t)))
}

// opener binds the running broker to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	return func(tb testing.TB, _ nativebench.Params) nativebench.Session {
		return open(tb, broker.Addr)
	}
}

type session struct {
	client   *sqs.Client
	queueURL string
	buf      []types.Message
}

func open(tb testing.TB, addr string) *session {
	tb.Helper()
	ctx := nativebench.Context(tb)
	client := sqs.New(sqs.Options{
		Region:       "elasticmq",
		Credentials:  credentials.NewStaticCredentialsProvider("x", "x", ""),
		BaseEndpoint: aws.String("http://" + addr),
	})
	name := nativebench.Name("iqmqbench")
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			"VisibilityTimeout":             "600",
			"ReceiveMessageWaitTimeSeconds": "0",
		},
	})
	if err != nil {
		tb.Fatalf("sqs: create queue %s: %v", name, err)
	}
	return &session{client: client, queueURL: aws.ToString(out.QueueUrl)}
}

func encode(payload []byte) string { return base64.StdEncoding.EncodeToString(payload) }

func (s *session) Publish(ctx context.Context, payload []byte) error {
	_, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(s.queueURL),
		MessageBody: aws.String(encode(payload)),
	})
	return err
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	entries := make([]types.SendMessageBatchRequestEntry, 0, maxBatchEntries)
	bytes := 0
	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		out, err := s.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(s.queueURL),
			Entries:  entries,
		})
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			return errors.New("sqs: " + aws.ToString(out.Failed[0].Message))
		}
		entries, bytes = entries[:0], 0
		return nil
	}
	for i, p := range payloads {
		body := encode(p)
		if len(entries) == maxBatchEntries || bytes+len(body) > maxBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.Itoa(i)),
			MessageBody: aws.String(body),
		})
		bytes += len(body)
	}
	return flush()
}

func (s *session) Receive(ctx context.Context, n int) error {
	for len(s.buf) < n {
		out, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(s.queueURL),
			MaxNumberOfMessages: maxBatchEntries,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			return err
		}
		s.buf = append(s.buf, out.Messages...)
		if len(out.Messages) == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	for _, m := range s.buf[:n] {
		if _, err := s.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(s.queueURL),
			ReceiptHandle: m.ReceiptHandle,
		}); err != nil {
			return err
		}
	}
	s.buf = s.buf[:copy(s.buf, s.buf[n:])]
	return nil
}

func (s *session) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := s.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(s.queueURL)})
	return err
}
