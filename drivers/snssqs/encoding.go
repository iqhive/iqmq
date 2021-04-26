// Package snssqs maps messages to its native broker client.
package snssqs

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	mq "bitbucket.org/iqhive/iqmq"
)

func encode(topic string, m mq.Message) *sns.PublishInput {
	return &sns.PublishInput{TopicArn: aws.String(topic), Message: aws.String(string(m.Body))}
}
