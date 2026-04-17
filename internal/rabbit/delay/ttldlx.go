package delay

import "time"

// ttldlx parks the message in a queue whose `x-message-ttl` is the delay and
// whose `x-dead-letter-exchange` routes back to where it came from.
//
// It is the reference kind and the one that works on every
// RabbitMQ there has ever been: `x-message-ttl` and `x-dead-letter-exchange`
// are core AMQP 0-9-1 queue arguments, so nothing has to be installed and the
// version matrix's oldest image supports it exactly as well as the newest.
//
// It pays for that in the two terms the criterion names.
//
// **Queue count.** A queue's `x-message-ttl` is fixed when the queue is
// declared, so one queue serves one delay. Every distinct (target, delay) pair
// is therefore a queue, which for arbitrary delays is unbounded — hence the
// bucket ladder, which trades the count against the accuracy.
//
// **Delay accuracy.** A requested delay is rounded UP to the first bucket at
// or above it, because the specification makes a delay a minimum and a message delivered
// early breaks the caller's contract. With the default doubling ladder the
// worst case is one tick short of twice the requested delay, and
// [Plan.Granted] says what was granted so that nobody has to infer it.
//
// **Two hops.** The message is published into the delay queue and then
// dead-lettered out of it, so the broker moves it twice where the plugin moves
// it once. That is this package's closest available proxy for the specification's "broker
// CPU", which needs a broker to measure properly.
type ttldlx struct{ strategy }

var _ Strategy = ttldlx{}

// Plan parks the message in the bucket at or above the requested delay.
//
// A delay of zero is not a delay: the caller asked for an immediate
// redelivery, and parking it in a queue with a TTL of zero would cost two
// broker hops to achieve nothing. The plan then publishes straight to the
// target, with one hop and no queue.
func (t ttldlx) Plan(req Request) (Plan, error) {
	if err := checkRequest(req, t.MaxDelay()); err != nil {
		return Plan{}, err
	}
	if req.Delay == 0 {
		return Plan{
			Exchange:   req.Target.Exchange,
			RoutingKey: req.Target.RoutingKey,
			Granted:    0,
			Hops:       1,
		}, nil
	}
	bucket := t.bucket(req.Delay)
	ms := millis(bucket)
	name, err := delayQueueName(req.Target, ms)
	if err != nil {
		return Plan{}, err
	}
	return Plan{
		// The publication goes to the DEFAULT exchange with the delay
		// queue's own name as the routing key, which is AMQP's direct route
		// to one queue. A dedicated exchange per delay queue would double
		// the topology for nothing.
		Exchange:   "",
		RoutingKey: name,
		Declare: &QueueDecl{
			Name:      name,
			TTLMillis: ms,
			// The route back. See the package comment: the default
			// exchange rather than the topic's, because a delayed
			// redelivery belongs to the one subscription that asked for it
			// and the topic exchange would fan it out to all of them.
			DeadLetterExchange:   req.Target.Exchange,
			DeadLetterRoutingKey: req.Target.RoutingKey,
			ExpiresMillis:        millis(t.cfg.QueueTTL),
		},
		Granted: bucket,
		Hops:    2,
	}, nil
}

// bucket is the first ladder entry at or above d, and the last entry when d is
// above every one of them — which cannot happen, because [ttldlx.MaxDelay] is
// that last entry and checkRequest has already refused anything longer.
func (t ttldlx) bucket(d time.Duration) time.Duration {
	for _, b := range t.cfg.Ladder {
		if b >= d {
			return b
		}
	}
	return t.cfg.Ladder[len(t.cfg.Ladder)-1]
}

// Requires nothing: the TTL and dead-letter arguments are core AMQP 0-9-1.
func (ttldlx) Requires() Requirement { return RequiresNothing }

// MaxDelay is the ladder's largest bucket.
func (t ttldlx) MaxDelay() time.Duration { return t.cfg.Ladder[len(t.cfg.Ladder)-1] }

// Resolution is the ladder's smallest bucket: a shorter delay is rounded up to
// it, so this is the finest delay the strategy can really express.
func (t ttldlx) Resolution() time.Duration { return t.cfg.Ladder[0] }

// Queues counts the distinct (target, bucket) pairs the requests need, which
// is the specification's "queue count growth".
//
// A request for no delay needs no queue, which is why the count of a workload
// of immediate redeliveries is zero rather than one.
func (t ttldlx) Queues(reqs []Request) int {
	seen := make(map[string]struct{}, len(reqs))
	for _, req := range reqs {
		if req.Delay <= 0 {
			continue
		}
		name, err := delayQueueName(req.Target, millis(t.bucket(req.Delay)))
		if err != nil {
			continue
		}
		seen[name] = struct{}{}
	}
	return len(seen)
}
