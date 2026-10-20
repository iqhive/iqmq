package iqmq

import "context"

// PublishFunc is the publish half of iqmq's middleware surface: one call that
// publishes a batch of messages to a topic.
//
// A single-message publish is a batch of one, so one chain covers both. A
// chain is composed once, at construction, and never per message (
// rule 6), so the per-call options travel as an argument: a chain built once
// has nowhere else to receive them from.
type PublishFunc func(ctx context.Context, topic string, ms []*Message, opts []PublishOption) error

// PublishMiddleware wraps a PublishFunc. Applied outermost-first, so the
// outermost middleware of a chain sees the call before the ones inside it and
// the result after them.
type PublishMiddleware func(PublishFunc) PublishFunc
