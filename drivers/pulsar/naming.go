package pulsar

import (
	"strings"

	"github.com/iqhive/iqmq/internal/errcode"
)

// The topic-name vocabulary of Apache Pulsar.
const (
	// DomainPersistent is the durable topic domain: entries are written to
	// BookKeeper before the broker acknowledges them.
	DomainPersistent = "persistent"
	// DomainNonPersistent is the in-memory topic domain: the broker
	// acknowledges a publication it has not stored and never will.
	DomainNonPersistent = "non-persistent"
	// DefaultTenant is Pulsar's own default tenant.
	DefaultTenant = "public"
	// DefaultNamespace is Pulsar's own default namespace.
	DefaultNamespace = "default"
	// Separator is what a fully qualified topic name puts between the
	// domain and the tenant.
	Separator = "://"
	// PartitionSuffix is how Pulsar names one partition of a partitioned
	// topic. A caller must not name it: consuming a partition directly
	// bypasses the partitioned-topic consumer and sees a third of the
	// topic.
	PartitionSuffix = "-partition-"
	// MaxNameLen bounds one segment of a topic name. Pulsar's own bound is
	// the whole path a broker uses in a ZooKeeper znode and a REST path, so
	// this is the conservative per-segment figure the driver refuses above
	// rather than a number Pulsar publishes.
	MaxNameLen = 255
)

// TopicName is one resolved Pulsar topic: the parts, and the fully qualified
// name a producer or consumer is opened with.
//
// It is a value and every field is set: a TopicName that came out of [ParseTopic]
// or [TopicOf] is complete, so nothing below it has to ask what an empty field
// meant.
type TopicName struct {
	// Domain is DomainPersistent or DomainNonPersistent.
	Domain string
	// Tenant is the Pulsar tenant.
	Tenant string
	// Namespace is the namespace inside the tenant, without the tenant.
	Namespace string
	// Local is the topic's own name inside the namespace.
	Local string
}

// String is the fully qualified name: `<domain>://<tenant>/<namespace>/<local>`.
func (t TopicName) String() string {
	return t.Domain + Separator + t.Tenant + "/" + t.Namespace + "/" + t.Local
}

// Persistent reports whether this topic's publications are stored before they
// are acknowledged, which is what makes ConfirmDurable answerable.
func (t TopicName) Persistent() bool { return t.Domain == DomainPersistent }

// Namespaced is `<tenant>/<namespace>`, which is the form Pulsar's
// administrative API takes.
func (t TopicName) Namespaced() string { return t.Tenant + "/" + t.Namespace }

// TopicOf maps an iqmq topic onto a Pulsar topic name.
//
// Two forms are accepted and the difference is deliberate:
//
//   - a name carrying `://` is taken as a FULLY QUALIFIED Pulsar topic and is
//     validated as one. It is how a caller reaches another tenant or another
//     namespace, and how a non-persistent topic is named at all.
//   - anything else is the LOCAL name inside the deployment's configured
//     tenant, namespace and domain, with [Config.TopicPrefix] prepended.
//
// A short name containing a slash is REFUSED rather than read as
// `tenant/namespace/topic`, which Pulsar itself would accept. The reason is
// containment: a deployment that configured a tenant and a namespace has said
// where its topics live, and a topic name that silently escaped that would put
// one tenant's messages in another's — the same class of mistake as a mapper
// that rewrites an illegal character and makes two topics one. A caller who
// means another namespace says so with the full form.
//
// There is no escaping: an iqmq topic that is not a legal Pulsar name is
// refused, never transformed, because a transform is what makes two iqmq topics
// share one Pulsar topic.
func TopicOf(cfg Config, topic string) (TopicName, error) {
	if topic == "" {
		// Refused here and not left to the grammar. With a configured
		// TopicPrefix an empty portable topic composes to the prefix
		// alone, which IS a legal Pulsar name — so the grammar accepts
		// it and every caller that forgot to name a topic publishes to
		// the same one. FuzzPulsarNaming found it.
		return TopicName{}, errEmptyTopic
	}
	if HasPattern(topic) {
		// The pattern check comes FIRST, before the grammar, because a
		// regex metacharacter is outside Pulsar's name alphabet and the
		// grammar would refuse it as an invalid name. A publication to a
		// pattern is a mistake worth naming as one, and a subscription to
		// a pattern goes through PatternOf instead.
		return TopicName{}, errTopicIsPattern
	}
	if strings.Contains(topic, Separator) {
		return ParseTopic(topic)
	}
	if strings.ContainsRune(topic, '/') {
		return TopicName{}, errShortNameWithSlash
	}
	t := TopicName{
		Domain:    orDefault(cfg.Domain, DomainPersistent),
		Tenant:    orDefault(cfg.Tenant, DefaultTenant),
		Namespace: orDefault(cfg.Namespace, DefaultNamespace),
		Local:     cfg.TopicPrefix + topic,
	}
	if err := t.validate(); err != nil {
		return TopicName{}, err
	}
	return t, nil
}

// ParseTopic parses a fully qualified Pulsar topic name.
//
// It is the driver's own parser rather than pulsar-client-go's, which lives in
// that module's `internal` tree and cannot be imported. The grammar is the same
// one the broker enforces, and FuzzPulsarNaming holds the round trip: every
// name this function accepts renders back to itself, and every name it produces
// it accepts.
func ParseTopic(name string) (TopicName, error) {
	domain, rest, ok := strings.Cut(name, Separator)
	if !ok {
		return TopicName{}, errNoDomain
	}
	if domain != DomainPersistent && domain != DomainNonPersistent {
		return TopicName{}, errBadDomain
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 {
		// Pulsar also accepts the legacy four-part form
		// `tenant/cluster/namespace/topic`. It is refused here: the form
		// was deprecated in Pulsar 2.0, a broker that still serves it
		// maps it onto a two-part namespace, and accepting it would give
		// this driver two spellings of one topic — which is exactly the
		// non-injectivity the mapping must not have.
		return TopicName{}, errBadTopicShape
	}
	t := TopicName{Domain: domain, Tenant: parts[0], Namespace: parts[1], Local: parts[2]}
	if err := t.validate(); err != nil {
		return TopicName{}, err
	}
	return t, nil
}

// validate holds every segment to Pulsar's named-entity grammar.
func (t TopicName) validate() error {
	for _, seg := range [...]string{t.Tenant, t.Namespace, t.Local} {
		if err := CheckName(seg); err != nil {
			return err
		}
	}
	if strings.Contains(t.Local, PartitionSuffix) {
		// `orders-partition-3` is one partition of `orders`. A producer
		// opened on it publishes to that partition alone and a consumer
		// opened on it receives a fraction of the topic with no
		// indication that it has — so the name is refused rather than
		// honoured.
		return errPartitionName
	}
	return nil
}

// CheckName reports whether one segment of a Pulsar topic name — a tenant, a
// namespace or a local topic name — is legal.
//
// Pulsar's own grammar is its `NamedEntity`: one or more of letters, digits and
// `-`, `_`, `=`, `:`, `.`. It is checked here rather than left to the broker
// because an illegal name is answered with a lookup failure after a round trip,
// and the specification wants the refusal before any I/O.
func CheckName(seg string) error {
	switch {
	case seg == "":
		return errEmptyName
	case len(seg) > MaxNameLen:
		return errNameTooLong
	case seg == "." || seg == "..":
		return errBadNameChar
	}
	for i := range len(seg) {
		c := seg[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-' || c == '_' || c == '=' || c == ':' || c == '.':
		default:
			return errBadNameChar
		}
	}
	return nil
}

// SubscriptionOf maps an iqmq subscription onto a Pulsar subscription name.
//
// A subscription is a Pulsar subscription: a named cursor on the topic that
// every consumer of that name shares, which is the consumer group, and
// which outlives its consumers, which is DurableSubscriptions. The name
// takes the topic grammar because Pulsar builds the retry and dead-letter topic
// names out of it (`<topic>-<subscription>-RETRY`), so a subscription name that
// is not a legal topic segment is a subscription whose retry topic cannot be
// created.
func SubscriptionOf(subscription string) (string, error) {
	if subscription == "" {
		return "", errNoSubscription
	}
	if err := CheckName(subscription); err != nil {
		return "", err
	}
	return subscription, nil
}

// PatternOf maps an iqmq topic pattern onto the fully qualified regular
// expression a Pulsar regex subscription takes.
//
// Pulsar matches the pattern against the WHOLE topic name, so the pattern has
// to carry the domain, tenant and namespace: a bare `orders.*` would match
// nothing. The namespace parts are taken from the configuration and the pattern
// itself is the caller's, untouched — a driver that escaped or rewrote it would
// change which topics a subscription receives.
func PatternOf(cfg Config, pattern string) (string, string, error) {
	if pattern == "" {
		return "", "", errEmptyName
	}
	if strings.Contains(pattern, Separator) {
		domain, rest, _ := strings.Cut(pattern, Separator)
		if domain != DomainPersistent && domain != DomainNonPersistent {
			return "", "", errBadDomain
		}
		parts := strings.SplitN(rest, "/", 3)
		if len(parts) != 3 {
			return "", "", errBadTopicShape
		}
		if err := CheckName(parts[0]); err != nil {
			return "", "", err
		}
		if err := CheckName(parts[1]); err != nil {
			return "", "", err
		}
		return pattern, parts[0] + "/" + parts[1], nil
	}
	if strings.ContainsRune(pattern, '/') {
		return "", "", errShortNameWithSlash
	}
	t := TopicName{
		Domain:    orDefault(cfg.Domain, DomainPersistent),
		Tenant:    orDefault(cfg.Tenant, DefaultTenant),
		Namespace: orDefault(cfg.Namespace, DefaultNamespace),
	}
	return t.Domain + Separator + t.Namespaced() + "/" + cfg.TopicPrefix + pattern, t.Namespaced(), nil
}

// patternChars are the characters that make a topic a PATTERN rather than a
// name.
//
// The set is the union of the grammars a portable caller might write, and not
// Pulsar's alone, because the question is "did the caller mean a pattern?" and
// the answer must not depend on which broker they had in mind. Every character
// in it is outside Pulsar's named-entity alphabet, so a legal Pulsar topic name
// can never be read as a pattern by accident.
//
// `.` is deliberately NOT in it. It is a regular-expression metacharacter and
// also a perfectly ordinary character in a Pulsar topic name — `orders.eu` is a
// name, not a pattern — so including it would make every dotted topic a regex
// subscription. `.*` is caught by the `*`, which is how a caller writes a
// pattern in practice.
const patternChars = `*?[]()|^$+\{}#>`

// HasPattern reports whether a topic carries a pattern metacharacter.
func HasPattern(topic string) bool {
	return strings.ContainsAny(topic, patternChars)
}

// orDefault is the configured value or the default.
func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// The naming refusals.
var (
	errEmptyName   = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a Pulsar tenant, namespace or topic name cannot be empty")
	errNameTooLong = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a Pulsar name segment is at most 255 bytes")
	errBadNameChar = errcode.New(errcode.InvalidArgument,
		"iqmq/pulsar: a Pulsar name segment may hold only letters, digits, '-', '_', '=', ':' and '.'")
	errNoDomain           = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a fully qualified topic name needs a domain, as in persistent://tenant/namespace/topic")
	errBadDomain          = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a topic's domain is either persistent or non-persistent")
	errBadTopicShape      = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a fully qualified topic name is <domain>://<tenant>/<namespace>/<topic>; the legacy four-part form is refused because it gives one topic two spellings")
	errPartitionName      = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a topic name may not carry Pulsar's '-partition-' suffix; that names one partition of another topic, and a handle opened on it sees a fraction of it")
	errShortNameWithSlash = errcode.New(errcode.InvalidArgument,
		"iqmq/pulsar: a topic that is not fully qualified may not contain '/': the tenant and namespace are Config.Tenant and Config.Namespace, and a name that escaped them would publish into another tenant. Write persistent://tenant/namespace/topic to reach another one")
	errTopicIsPattern = errcode.New(errcode.InvalidArgument,
		"iqmq/pulsar: the topic carries a regular-expression metacharacter, so it is a pattern; a pattern can be subscribed to and not published to")
	errNoSubscription = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a subscription is a Pulsar subscription name and cannot be empty")
	errEmptyTopic     = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: the topic is empty; with a configured TopicPrefix it would compose to the prefix alone, which is a legal Pulsar topic every caller that named none would share")
)
