package pulsar_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// refusingClient is the in-process model whose producer and consumer creations
// the broker refuses with err, in the form pulsar-client-go v0.21 hands a
// refusal to its caller.
type refusingClient struct {
	pulsarapi.Client
	err error
}

func (c *refusingClient) CreateProducer(pulsar.ProducerOptions) (pulsar.Producer, error) {
	return nil, c.err
}

func (c *refusingClient) Subscribe(pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	return nil, c.err
}

// TestBrokerRefusalsAtOpenKeepTheirCodes is what a sink's and a source's open
// report when the broker refuses the producer or the consumer.
//
// pulsar-client-go v0.21 gives such a refusal no result code: a broker's
// CommandError reaches the caller as errors.New("server error: <Name>:
// <message>") (internal/connection.go, handleResponseError), a lookup's as the
// bare name (internal/lookup_service.go), and a subscription's ERROR response
// as "<Name>: <message>" (consumer_partition.go). The error mapper classified
// only errors carrying a result code, so against a real broker a second
// Exclusive consumer, a terminated topic and a missing one all read as
// CodeUnknown, where the rules and the driver page promise Conflict and
// NotFound. The in-process model answered with result codes, which is why no
// test saw it.
func TestBrokerRefusalsAtOpenKeepTheirCodes(t *testing.T) {
	cases := []struct {
		text      string
		code      mq.ErrorCode
		retryable bool
		// result is the code ResultOf reads from the text, when
		// pulsar-client-go has one for the refusal.
		result pulsar.Result
	}{
		{
			"server error: TopicTerminatedError: org.apache.pulsar.broker.service.BrokerServiceException$TopicTerminatedException: Topic was already terminated",
			mq.CodeConflict, false, pulsar.TopicTerminated,
		},
		{"server error: ConsumerBusy: Exclusive consumer is already connected", mq.CodeConflict, false, pulsar.ConsumerBusy},
		{"server error: ProducerFenced: Producer was fenced", mq.CodeConflict, false, pulsar.ProducerFenced},
		{"server error: ProducerBusy: Producer with name 'orders' is already connected to topic", mq.CodeConflict, false, pulsar.Ok},
		{"TopicNotFound", mq.CodeNotFound, false, pulsar.TopicNotFound},
		{"server error: TopicNotFound: Topic does not exist", mq.CodeNotFound, false, pulsar.TopicNotFound},
		{"SubscriptionNotFound: Subscription does not exist", mq.CodeNotFound, false, pulsar.SubscriptionNotFound},
		{"server error: AuthorizationError: Client is not authorized to Produce", mq.CodeForbidden, false, pulsar.AuthorizationError},
		{"server error: AuthenticationError: Failed to authenticate", mq.CodeUnauthorized, false, pulsar.AuthenticationError},
		{"server error: ServiceNotReady: Namespace bundle is being unloaded", mq.CodeUnavailable, true, pulsar.ServiceUnitNotReady},
		{"server error: MetadataError: Failed to read the metadata store", mq.CodeUnavailable, true, pulsar.BrokerMetadataError},
		{"server error: PersistenceError: Failed to open the managed ledger", mq.CodeUnavailable, true, pulsar.BrokerPersistenceError},
		{"server error: TooManyRequests: Failed due to too many pending lookup requests", mq.CodeThrottled, true, pulsar.TooManyLookupRequestException},
		{
			"server error: ProducerBlockedQuotaExceededException: Cannot create producer on topic with backlog quota exceeded",
			mq.CodeResourceExhausted, false, pulsar.ProducerBlockedQuotaExceededException,
		},
		{"server error: IncompatibleSchema: Trying to create a producer with an incompatible schema", mq.CodeInvalidConfig, false, pulsar.SchemaFailure},
		{"server error: InvalidTopicName: Invalid topic name", mq.CodeInvalidArgument, false, pulsar.InvalidTopicName},
		{"server error: UnknownError: something the broker did not name", mq.CodeUnknown, false, pulsar.Ok},
		{"a failure that names no refusal", mq.CodeUnknown, false, pulsar.Ok},
	}
	for _, tc := range cases {
		t.Run(tc.text, func(t *testing.T) {
			native := errors.New(tc.text)
			got, ok := pd.ResultOf(fmt.Errorf("creating the producer: %w", native))
			if want := tc.result != pulsar.Ok; ok != want || got != tc.result {
				t.Errorf("ResultOf read %v (%v) from %q, want %v (%v)", got, ok, tc.text, tc.result, want)
			}
			server := pulsarfake.New()
			b, err := pd.BackendWithClient(context.Background(), &refusingClient{Client: server.Client(), err: native},
				driverkit.ClientOwned, rigConfig(server))
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			_, serr := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "refused", Metadata: mq.MetadataNativeStrict})
			_, cerr := b.OpenSource(context.Background(), mq.SourceConfig{
				Topic: "refused", Subscription: "workers", Prefetch: 4, Metadata: mq.MetadataNativeStrict,
			})
			for what, err := range map[string]error{"OpenSink": serr, "OpenSource": cerr} {
				if mq.CodeOf(err) != tc.code || mq.IsRetryable(err) != tc.retryable || mq.IsAmbiguous(err) {
					t.Errorf("%s refused with %q reported %v (code %v, retryable %v, ambiguous %v), want code %v, "+
						"retryable %v, definite", what, tc.text, err, mq.CodeOf(err), mq.IsRetryable(err),
						mq.IsAmbiguous(err), tc.code, tc.retryable)
				}
				if !errors.Is(err, native) {
					t.Errorf("%s's error %v does not wrap the client's own", what, err)
				}
			}
		})
	}
}
