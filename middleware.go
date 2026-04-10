package iqmq

import "context"

// PublishFunc is the publish half of iqmq's middleware surface: one call that
// publishes a batch of messages to a topic.
//
// A single-message publish is a batch of one, so one chain covers both. A
// chain is composed once, at construction, and never per message (
// rule 6).
type PublishFunc func(ctx context.Context, topic string, ms []*Message) error

// PublishMiddleware wraps a PublishFunc. Applied outermost-first, so the
// outermost middleware of a chain sees the call before the ones inside it and
// the result after them.
type PublishMiddleware func(PublishFunc) PublishFunc
