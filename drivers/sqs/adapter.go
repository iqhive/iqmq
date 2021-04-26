// Package sqs maps messages to its native broker client.
package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	native "github.com/aws/aws-sdk-go-v2/service/sqs"
	mq "bitbucket.org/iqhive/iqmq"
)

// New uses a caller-owned SDK client and an existing queue URL. Bodies must be valid SQS text.
func New(client *native.Client, queue string) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			_, err := client.SendMessage(ctx, encode(queue, m))
			return err
		},
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			for {
				out, err := client.ReceiveMessage(ctx, &native.ReceiveMessageInput{QueueUrl: aws.String(queue), MaxNumberOfMessages: int32(1), WaitTimeSeconds: int32(20)})
				if err != nil {
					return nil, err
				}
				if len(out.Messages) == 0 {
					continue
				}
				m := out.Messages[0]
				return mq.NewDelivery(mq.Message{Body: []byte(aws.ToString(m.Body))}, func(c context.Context) error {
					_, err := client.DeleteMessage(c, &native.DeleteMessageInput{QueueUrl: aws.String(queue), ReceiptHandle: m.ReceiptHandle})
					return err
				}, nil), nil
			}
		},
	}
}
