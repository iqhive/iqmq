// Package snssqs maps messages to its native broker client.
package snssqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	mq "bitbucket.org/iqhive/iqmq/legacy"
)

// New publishes to SNS and receives from a caller-provisioned raw-delivery SQS backend.
func New(client *sns.Client, subscription mq.Backend, topic string) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			_, err := client.Publish(ctx, encode(topic, m))
			return err
		},
		ReceiveFunc: subscription.Receive, CloseFunc: subscription.Close,
	}
}
