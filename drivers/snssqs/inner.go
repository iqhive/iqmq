package snssqs

import (
	"context"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/sqs"
)

// innerBackend is the queue half of this driver: a whole `drivers/sqs`
// backend.
//
// The alias exists so that every reference to the composition reads as one
// idea rather than as "a second driver happens to be embedded here", and so
// that the one place that decides what the inner driver is built with is
// [Config.sqsConfig].
type innerBackend = sqs.Backend

// openInner builds the queue half over a real SQS client.
//
// The inner driver always BORROWS the client. There is one client, one owner
// and one place that closes it — this backend's own ownership — and a driver
// composed of two halves that both closed it would close it twice.
func openInner(ctx context.Context, client *awssqs.Client, own driverkit.Ownership, cfg Config) (*innerBackend, error) {
	return sqs.BackendWithClient(ctx, client, own, cfg.sqsConfig())
}

// openInnerAPI builds the queue half over the API slice, which is what this
// driver's own tests drive.
func openInnerAPI(ctx context.Context, api sqs.API, native any, own driverkit.Ownership, cfg Config) (*innerBackend, error) {
	return sqs.BackendWithAPI(ctx, api, native, own, cfg.sqsConfig())
}
