package amqp10

import (
	amqp "github.com/Azure/go-amqp"

	"github.com/iqhive/iqmq/internal/errcode"
)

// The typed per-handle options what a caller can say about ONE
// publisher or ONE consumer that the portable
// [github.com/iqhive/iqmq.PublisherConfig] and
// [github.com/iqhive/iqmq.ConsumerConfig] have no field for.
//
// # Where they are carried, and where they are not yet
//
// The specification asks for these to be carried in `PublisherConfig.Driver` and
// `ConsumerConfig.Driver`. Those fields do not exist in the root module at the
// time this increment landed: `PublisherConfig` and `ConsumerConfig` carry no
// `Driver any`, and [github.com/iqhive/iqmq.SinkConfig] and
// [github.com/iqhive/iqmq.SourceConfig] — which are what a driver actually
// sees — carry nothing either, so there is no path by which a per-publisher
// option could reach this driver even if a caller set one.
//
// So the types are declared here and are reachable through [Config.Publisher]
// and [Config.Consumer], which makes them per-CLIENT rather than per-handle:
// a program that needs two consumers with different selectors opens two
// clients. [PublisherOptionsFrom] and [ConsumerOptionsFrom] are the decoders
// the driver will read `Driver` with on the day the SPI carries it, and they
// are written and tested now so that the wiring is one line rather than a
// design decision. The gap is recorded in this increment's report.

// PublisherOptions are the AMQP 1.0 controls of one publisher.
type PublisherOptions struct {
	// LinkName is the name of the sending link, which has to be unique per
	// connection and direction. Empty means go-amqp generates one.
	//
	// It is worth setting for a broker whose management view lists links by
	// name: an operator looking at forty links wants to know which publisher
	// each belongs to, and a generated name tells them nothing.
	LinkName string
	// Properties are link properties sent in the ATTACH frame, which is how a
	// broker-specific capability is asked for on a link rather than on the
	// connection.
	Properties map[string]any
	// Capabilities are the link's own `capabilities` field, and
	// DesiredCapabilities its `desired-capabilities`. Both are extension
	// points the protocol defines and this core has no opinion about: a
	// profile whose broker needs one names it.
	Capabilities        []string
	DesiredCapabilities []string
	// TargetCapabilities are the capabilities the sender desires of the
	// target node, which is how several brokers are told what KIND of node to
	// attach to — "queue" or "topic" on Artemis.
	TargetCapabilities []string
}

// ConsumerOptions are the AMQP 1.0 controls of one consumer.
type ConsumerOptions struct {
	// LinkName is the name of the receiving link. Empty means go-amqp
	// generates one. On a broker that identifies a shared durable
	// subscription by its link name rather than by its address, this is the
	// subscription's identity — which is why a profile that has one sets it
	// from the subscription rather than leaving it generated.
	LinkName string
	// Properties are link properties sent in the ATTACH frame.
	Properties map[string]any
	// Capabilities and DesiredCapabilities are the link's own extension
	// points.
	Capabilities        []string
	DesiredCapabilities []string
	// SourceCapabilities are the capabilities the receiver desires of the
	// source node.
	SourceCapabilities []string
	// Filters are the source filters the attach asks for, which is the only
	// way a server-side SELECTOR can reach this driver: the portable
	// ConsumerConfig can name a topic and a subscription, and a selector is
	// the broker's own expression language.
	//
	// [github.com/Azure/go-amqp.NewSelectorFilter] builds the standard
	// `apache.org:selector-filter:string`, which both Artemis and RabbitMQ 4
	// understand. A peer that cannot honour a filter DETACHES the link, so a
	// filter this broker does not have is a refusal at the first fetch rather
	// than a filter silently ignored — which is the behaviour the specification wants and
	// the reason this is not emulated in the client.
	Filters []amqp.LinkFilter
	// Copy asks for the `copy` distribution mode: messages are browsed rather
	// than consumed, so nothing is removed from the node.
	//
	// It is a real AMQP 1.0 feature and it changes the semantics completely —
	// a browsing consumer settles deliveries that stay where they are — so it
	// is off by default and a caller that wants it says so.
	Copy bool
}

// PublisherOptionsFrom reads the publisher options out of the value the SPI
// carries in a publisher's driver-specific field.
//
// It accepts the options by value or by pointer, and a nil or absent value is
// the zero options rather than an error: a caller that set nothing wants the
// defaults, and refusing would make every publisher on every other driver's
// configuration fail here.
//
// What it does refuse is a value of some OTHER type, because that is a caller
// who has mixed up two drivers' options and whose setting would otherwise be
// silently ignored — the failure mode and ADR-010 both exist to
// prevent.
func PublisherOptionsFrom(v any) (PublisherOptions, error) {
	switch o := v.(type) {
	case nil:
		return PublisherOptions{}, nil
	case PublisherOptions:
		return o, nil
	case *PublisherOptions:
		if o == nil {
			return PublisherOptions{}, nil
		}
		return *o, nil
	default:
		return PublisherOptions{}, errWrongPublisherOptions
	}
}

// ConsumerOptionsFrom reads the consumer options out of the value the SPI
// carries in a consumer's driver-specific field. It follows
// [PublisherOptionsFrom]'s rules.
func ConsumerOptionsFrom(v any) (ConsumerOptions, error) {
	switch o := v.(type) {
	case nil:
		return ConsumerOptions{}, nil
	case ConsumerOptions:
		return o, nil
	case *ConsumerOptions:
		if o == nil {
			return ConsumerOptions{}, nil
		}
		return *o, nil
	default:
		return ConsumerOptions{}, errWrongConsumerOptions
	}
}

// apply folds the publisher options into a sender attach.
func (o PublisherOptions) apply(opts *amqp.SenderOptions) {
	if o.LinkName != "" {
		opts.Name = o.LinkName
	}
	if len(o.Properties) > 0 {
		opts.Properties = o.Properties
	}
	if len(o.Capabilities) > 0 {
		opts.Capabilities = o.Capabilities
	}
	if len(o.DesiredCapabilities) > 0 {
		opts.DesiredCapabilities = o.DesiredCapabilities
	}
	if len(o.TargetCapabilities) > 0 {
		opts.TargetCapabilities = o.TargetCapabilities
	}
}

// apply folds the consumer options into a receiver attach.
//
// The credit is NOT touched: it is -1 for every receiver this driver attaches,
// because the credit window owns the prefetch (credit.go). An option that could
// change it would be an option that breaks the one bound the specification gives a meaning
// to.
func (o ConsumerOptions) apply(opts *amqp.ReceiverOptions) {
	if o.LinkName != "" {
		opts.Name = o.LinkName
	}
	if len(o.Properties) > 0 {
		opts.Properties = o.Properties
	}
	if len(o.Capabilities) > 0 {
		opts.Capabilities = o.Capabilities
	}
	if len(o.DesiredCapabilities) > 0 {
		opts.DesiredCapabilities = o.DesiredCapabilities
	}
	if len(o.SourceCapabilities) > 0 {
		opts.SourceCapabilities = o.SourceCapabilities
	}
	if len(o.Filters) > 0 {
		opts.Filters = o.Filters
	}
	if o.Copy {
		opts.SourceDistributionMode = amqp.SourceDistributionModeCopy
	}
}

// The options' refusals.
var (
	errWrongPublisherOptions = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the publisher's driver options are not amqp10.PublisherOptions")
	errWrongConsumerOptions = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the consumer's driver options are not amqp10.ConsumerOptions")
)
