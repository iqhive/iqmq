// Package sqs maps messages to its native broker client.
package sqs

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	native "github.com/aws/aws-sdk-go-v2/service/sqs"
	mq "bitbucket.org/iqhive/iqmq/legacy"
)

func encode(queue string, m mq.Message) *native.SendMessageInput {
	return &native.SendMessageInput{QueueUrl: aws.String(queue), MessageBody: aws.String(string(m.Body))}
}
