package s3store_test

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/claimcheck"
)

// pinnedMinIO is the image conformance/harness/images.lock pins for the MinIO
// arm. It is the digest and not a tag, and TestThePinnedImageIsTheLockedOne
// holds the two together.
//
// It is quay.io's and not Docker Hub's: minio/minio no longer resolves on
// Docker Hub ("pull access denied ... repository does not exist"), and this
// digest is the RELEASE.2025-09-07T16-13-09Z image quay.io still serves.
const pinnedMinIO = "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"

func TestThePinnedImageIsTheLockedOne(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "..", "conformance", "harness", "images.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 2 && fields[0] == "minio" {
			if fields[1] != pinnedMinIO {
				t.Fatalf("images.lock pins minio as %s, this arm documents %s", fields[1], pinnedMinIO)
			}
			return
		}
	}
	t.Fatal("images.lock pins no minio image")
}

// goneThroughARunner is the x/claimcheck decision  proved
// against an S3 answer: a payload whose object has been deleted -- the bucket's
// lifecycle rule, ordinarily -- is dead-lettered on its first attempt, with the
// reference as its body, and the handler never runs.
func goneThroughARunner(t *testing.T, client *s3.Client, remove func(key string)) {
	t.Helper()
	ctx := context.Background()
	store := newStore(t, client)
	cfg := claimcheck.Config{Store: store, Threshold: 16, RequireDigest: true}
	pm, err := claimcheck.PublishCheckIn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	checkOut, err := claimcheck.CheckOut(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mc, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mc.Shutdown(ctx) }()
	consumer, err := mc.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	dead, err := mc.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	var wire []byte
	publish := pm(func(ctx context.Context, topic string, ms []*mq.Message, _ []mq.PublishOption) error {
		wire = bytes.Clone(ms[0].Body)
		return mc.Publish(ctx, topic, ms[0])
	})
	if err := publish(ctx, "orders", []*mq.Message{{ID: "o-1", Body: bytes.Repeat([]byte("a large order "), 64)}}, nil); err != nil {
		t.Fatal(err)
	}
	ref, err := claimcheck.ParseRef(wire)
	if err != nil {
		t.Fatal(err)
	}
	remove(ref.KeyString())

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	attempts := 0
	handled := make(chan struct{}, 1)
	go func() {
		_ = consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			handled <- struct{}{}
			return nil
		}),
			mq.WithMiddleware(func(next mq.Handler) mq.Handler {
				return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
					attempts++
					return next.Handle(ctx, d)
				})
			}, checkOut),
			mq.WithMaxAttempts(5),
			mq.WithRetryBackoff(mq.ConstantBackoff(time.Minute)),
			mq.WithDeadLetter("orders.dlq"))
	}()
	drainCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ms, err := memory.Drain(drainCtx, dead, 1)
	if err != nil {
		t.Fatalf("nothing reached the dead-letter topic: %v", err)
	}
	stop()
	select {
	case <-handled:
		t.Fatal("the handler ran for a message whose payload is gone")
	default:
	}
	if !bytes.Equal(ms[0].Body, wire) {
		t.Fatal("the dead letter's body is not the reference that was published")
	}
	if failure, _ := ms[0].Headers.Get(mq.HeaderFailure); !strings.Contains(string(failure), "no payload") {
		t.Errorf("the failure header is %q, want ErrPayloadGone's text", failure)
	}
	if n, _ := ms[0].Headers.Get(mq.HeaderAttempts); string(n) != "1" {
		t.Errorf("the dead letter records attempt %q, want 1", n)
	}
}

func TestAGonePayloadThroughARunner(t *testing.T) {
	t.Run("in-process S3", func(t *testing.T) {
		f := newFakeS3(t)
		goneThroughARunner(t, f.client(), func(key string) { f.remove(bucket, key) })
	})
	t.Run("MinIO", func(t *testing.T) {
		c := minioClient(t)
		goneThroughARunner(t, c, func(key string) {
			if _, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
				t.Fatal(err)
			}
		})
	})
}
