package delay

import "time"

// plugin publishes to an `x-delayed-message` exchange with an `x-delay` header
// and lets the broker hold the message.
//
// It is the better arm on every term the criterion names — the delay is
// exact to the millisecond, one exchange serves every delay so the queue count
// does not grow at all, and the broker moves the message once rather than
// twice — and it is not the default, which is the whole point of the family.
// `rabbitmq_delayed_message_exchange` is a community plugin and is **not** in
// the official image: `hack/images/rabbitmq-delayed/Dockerfile` builds one that
// has it, and the plugin only supports the RabbitMQ minor it was built for. A
// driver that assumed it would fail at open against a stock broker, so
// [AutoKind] is [TTLDLX] and the driver's own detection is what selects this.
//
// What it does not do is survive a broker restart cleanly in every
// configuration: the plugin holds a delayed message in a Mnesia table and its
// own documentation is explicit that a message whose delay elapses while the
// node is down is delivered when the node comes back, not at the moment it was
// due. That is a property of the plugin rather than of this plan, and
// docs/drivers/rabbitmq.md says so where a caller will read it.
type plugin struct{ strategy }

var _ Strategy = plugin{}

// Plan publishes to the delayed-message exchange with the delay in the
// `x-delay` header.
//
// The routing key is the TARGET's, not the delay queue's, because there is no
// delay queue: the plugin holds the message inside the exchange and routes it
// when the delay elapses, exactly as its `x-delayed-type` would have routed it
// immediately. So the exchange has to be bound to the target the same way the
// target is bound to its own exchange, which is the driver's declaration and
// not this plan's.
//
// A delay of zero is published straight to the target, for [ttldlx.Plan]'s
// reason: a plugin hop that holds a message for no time at all is a hop for
// nothing, and it would put this driver's own exchange in the path of every
// immediate redelivery.
func (p plugin) Plan(req Request) (Plan, error) {
	if err := checkRequest(req, p.MaxDelay()); err != nil {
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
	if req.DelayExchange == "" {
		return Plan{}, ErrNoDelayExchange
	}
	ms := millis(req.Delay)
	return Plan{
		Exchange:     req.DelayExchange,
		RoutingKey:   req.Target.RoutingKey,
		Header:       DelayHeader,
		HeaderMillis: ms,
		// The granted delay is the requested one rounded up to the whole
		// millisecond the header can carry, which is the only rounding this
		// kind does.
		Granted: time.Duration(ms) * time.Millisecond,
		Hops:    1,
	}, nil
}

// Requires the delayed-message exchange plugin, which is what a driver detects
// before it binds this kind.
func (plugin) Requires() Requirement { return RequiresDelayedExchange }

// MaxDelay is the plugin's own ceiling: the delay travels in a 32-bit signed
// integer of milliseconds.
func (plugin) MaxDelay() time.Duration { return MaxPluginDelay }

// Resolution is one millisecond, which is the unit of the x-delay header.
func (plugin) Resolution() time.Duration { return time.Millisecond }

// Queues is zero for any workload: the exchange holds the messages, so no
// delay queue exists at all. It is the term the criterion separates the two
// arms most sharply on.
func (plugin) Queues([]Request) int { return 0 }
