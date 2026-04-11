package nack

import mq "bitbucket.org/iqhive/iqmq"

// retryTopic republishes the record to `<topic>.retry.<n>` and then completes
// the original offset, so the partition keeps moving while the record waits
// its turn somewhere else.
//
// The throughput is the point and the reordering is the price: the record is
// now behind everything that was already in the partition, and a consumer
// that cares about a key's order cannot use this. The specification therefore forbids
// selecting it by default, and [Auto] does not.
//
// The retry level comes from the topic the record was delivered from rather
// than from its attempt count, which is the only source that survives a
// rebalance: a record redelivered on `orders.retry.2` goes to
// `orders.retry.3` whatever the broker says its attempt is, because the
// attempt count of a freshly produced record is 1 again.
type retryTopic struct{ cfg Config }

// Plan republishes to the next retry level, or takes ADR-011's failure route
// once the levels or the attempts are exhausted.
func (s retryTopic) Plan(d Delivery, o mq.NackOptions) (Plan, error) {
	if p, done, err := terminal(s.cfg, o); done {
		return p, err
	}
	if s.cfg.exhausted(d) {
		return failureRoute(s.cfg, o, "attempts exhausted"), nil
	}
	if d.Topic == "" {
		// FuzzNackPlan found this on its first run: with no topic to build
		// on, RetryTopicName produces ".retry.1", which is a topic nobody
		// created and which ParseRetryTopic then refuses to read back — so
		// the record would be published to a dead end and its original
		// offset committed. A delivery with no topic is a driver bug, and
		// refusing the settlement is what makes it visible.
		return Plan{}, ErrNoTopic
	}
	base, level, _ := ParseRetryTopic(d.Topic)
	next := level + 1
	if next > s.cfg.retries() {
		return failureRoute(s.cfg, o, "retry topics exhausted"), nil
	}
	return Plan{
		Op:      OpRepublish,
		Topic:   RetryTopicName(base, next),
		Attempt: d.attempt() + 1,
		Delay:   s.cfg.delay(o),
		Reason:  o.Reason,
	}, nil
}

// Kind returns RetryTopic.
func (s retryTopic) Kind() Kind { return RetryTopic }
