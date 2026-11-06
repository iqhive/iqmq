package v1compat_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/v1compat"
)

// TestAPublicationTheV1LibraryRetriedAfterItWasStoredIsAmbiguous is
// drivers/gcppubsub's test of the same name on this arm, whose Open dials a v1
// client: the first Publish attempt is stored and its answer lost, and the
// library's retry is refused. The publication was a definite refusal while the
// message was on the topic; it is ambiguous. A refusal of a first attempt
// stays definite.
func TestAPublicationTheV1LibraryRetriedAfterItWasStoredIsAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		name       string
		storeFirst bool
	}{
		{"after a stored attempt", true},
		{"of the first attempt", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := pstest.NewServer()
			var topic string
			var armed atomic.Bool
			var attempts atomic.Int32
			intercept := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				in, ok := req.(*pubsubpb.PublishRequest)
				if !ok || !armed.Load() || !strings.HasSuffix(in.GetTopic(), "/"+topic) {
					return handler(ctx, req)
				}
				if attempts.Add(1) > 1 || !tc.storeFirst {
					return nil, status.Error(codes.PermissionDenied, "injected: the attempt is refused")
				}
				if _, err := handler(ctx, req); err != nil {
					return nil, err
				}
				return nil, status.Error(codes.DeadlineExceeded, "injected: the answer to a stored publication was lost")
			}
			gs := grpc.NewServer(grpc.UnaryInterceptor(intercept))
			pubsubpb.RegisterPublisherServer(gs, &srv.GServer)
			pubsubpb.RegisterSubscriberServer(gs, &srv.GServer)
			lis, err := net.Listen("tcp", "localhost:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			var wg sync.WaitGroup
			wg.Go(func() { _ = gs.Serve(lis) })
			t.Cleanup(func() {
				gs.Stop()
				wg.Wait()
				_ = srv.Close()
			})
			c, err := v1compat.Open(context.Background(), gps.Config{
				ProjectID: "iqmq-test", Endpoint: lis.Addr().String(), InsecureTransport: true, AutoCreate: true,
				PublishCountThreshold: 1, PublishDelayThreshold: time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			topic = newName("libretry")
			provision(t, c, topic, "s")

			armed.Store(true)
			err = c.Publish(t.Context(), topic, &mq.Message{Body: []byte("once")})
			stored := len(srv.Messages())
			t.Logf("%d attempt(s), %d stored: %v (code %v, ambiguous %v)", attempts.Load(), stored, err, mq.CodeOf(err), mq.IsAmbiguous(err))
			if mq.CodeOf(err) != mq.CodeForbidden {
				t.Fatalf("Publish = %v (code %v), want the refusal's code, Forbidden", err, mq.CodeOf(err))
			}
			if tc.storeFirst && (stored != 1 || !mq.IsAmbiguous(err)) {
				t.Fatalf("Publish = %v (ambiguous %v) with %d stored, want ambiguous: the library's first attempt "+
					"is on the topic", err, mq.IsAmbiguous(err), stored)
			}
			if !tc.storeFirst && (stored != 0 || mq.IsAmbiguous(err)) {
				t.Fatalf("Publish = %v (ambiguous %v) with %d stored, want a definite refusal", err, mq.IsAmbiguous(err), stored)
			}
		})
	}
}
