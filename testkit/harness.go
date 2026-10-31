package testkit

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Need is the topology one scenario needs before it can run.
//
// It is a parameter of [Harness.Open] rather than a separate Provision hook
// because the two brokers that need it most cannot be told after the fact: a
// memory broker's topology is fixed when the broker is built, and a Kafka topic's
// partition count is fixed when the topic is created. A driver that can
// provision through Client.Admin ignores everything here except the names and
// calls EnsureTopic; one that cannot builds what the scenario asked for.
//
// A driver that cannot build what a Need asks for returns an error wrapping
// [github.com/iqhive/iqmq.ErrUnsupported], and the scenario is skipped with that
// reason rather than failed: a broker with no dead-letter destination is not a
// broker that failed the dead-letter scenario.
//
// Refuse a Need ONLY when accepting it would make the scenario prove something
// FALSE. It is not a declaration of what the broker supports, and refusing
// every field the broker cannot honour throws away the evidence the suite
// exists to collect: one driver refused a Need naming a priority and a
// de-duplication window and skipped eleven scenarios, TEN of them the negative
// direction of a claim that driver itself makes -- exactly what the specification asks a
// conformance run to demonstrate.
//
// A field the broker ignores is usually harmless, because the scenario that
// cares about it asks for the capability separately and is skipped on that.
// The question to ask is not "can this broker do this?" but "would running
// with what I can build make a passing scenario a lie?".
type Need struct {
	// Topic is the topic the scenario publishes to.
	Topic string
	// Subscriptions are the subscriptions the scenario consumes through. It
	// is never empty; the first is the scenario's main one.
	Subscriptions []string
	// DeadLetterTopic is the destination a rejected or exhausted message
	// goes to. It is empty when the scenario does not need one, and when it
	// is set the topic has to exist too.
	DeadLetterTopic string
	// MaxAttempts is how many deliveries a message may have before the
	// dead-letter route. Zero means the driver's own default.
	MaxAttempts uint32
	// AckDeadline is the lease the scenario needs. Zero means the driver's
	// default; a negative value asks for leases that never expire.
	AckDeadline time.Duration
	// Partitions is how many lanes the subscription needs. Zero means the
	// driver's default, which the specification expects to be the ordered one.
	Partitions int
	// Retain asks for a topic that retains delivered messages, which replay
	// and the seek positions need.
	Retain bool
	// Priority asks for a topic that serves the most urgent message first.
	Priority bool
	// DedupWindow asks for broker-side de-duplication over this window.
	DedupWindow time.Duration
	// Capacity bounds the messages one lane holds, which the backpressure
	// scenario needs in order to fill one.
	Capacity int
	// MaxMessageSize bounds a publication, which the oversized-payload
	// scenario needs in order to exceed it.
	MaxMessageSize int
	// Wildcards asks for a topic whose subscriptions match a pattern.
	Wildcards bool
}

// Sub returns the nth subscription, or the first when there are fewer.
func (n *Need) Sub(i int) string {
	if i < len(n.Subscriptions) {
		return n.Subscriptions[i]
	}
	if len(n.Subscriptions) > 0 {
		return n.Subscriptions[0]
	}
	return "conformance"
}

// Harness is what a driver hands the conformance kit.
//
// Open, TopicName and Timeout are the three names; everything else has a
// working default and exists so that a driver can say what only it knows — which
// broker it ran against, whether the evidence is a container or an emulator, and
// which scenarios it documents as inapplicable.
type Harness struct {
	// Driver is the driver's registered name. It is required: a report with
	// no driver names nothing.
	Driver string
	// Profile is the broker variant, such as "fifo" or "jetstream".
	Profile string
	// Open opens a client with the topology the scenario needs. It is called
	// once per scenario, and the client is closed for the scenario when it
	// ends, so scenarios cannot interfere with one another.
	Open func(ctx context.Context, n Need) (*mq.Client, error)
	// TopicName maps a scenario's purpose to a topic name the broker
	// accepts. It must return a name that is unique per run — a conformance
	// run against a live broker shares it with everything else — and the
	// same name for the same purpose within one run. Nil means a default
	// that prefixes the purpose with "iqmq-conformance" and a run nonce.
	TopicName func(purpose string) string
	// Timeout bounds every scenario. Zero means DefaultTimeout.
	Timeout time.Duration
	// Evidence classifies the run. The zero value is refused: a report that
	// does not say whether it was made against a broker or an emulator
	// cannot be read honestly.
	Evidence Evidence
	// Broker identifies the broker image the run was made against.
	Broker Broker
	// Client identifies the native client library.
	Client ClientInfo
	// Commit is the revision under test.
	Commit string
	// Fidelity is how many messages the byte-for-byte fidelity scenario
	// sends. Zero means DefaultFidelity, which is the 100,000; a driver
	// whose broker is slow enough for that to dominate the run lowers it and
	// says so on its page.
	Fidelity int
	// Secrets are credentials that must appear nowhere the kit can see: not
	// in an error string, not in Info, not in a rendered configuration
	// A harness with none skips the redaction scenario rather
	// than passing it, because a check with nothing to find proves nothing.
	Secrets []string
	// OpenWrongCredential opens a client whose credential is deliberately
	// wrong and returns the error, discarding any client it somehow got.
	// Nil means this harness cannot produce one, and the scenario skips.
	//
	// It exists because the error a native client is most likely to render
	// its connection string into is the DIAL or AUTHENTICATION failure, and
	// that happens inside Open -- before there is an [mq.Client] for a
	// scenario to hold. Every probe `secrets/redaction` has runs on an
	// already-open client, so the one error that matters most was the one
	// error the kit could not read. `dsn.Scrub` exists for exactly this and
	// no shipped driver calls it; they pass the other scenario because they
	// never interpolate their own configuration into an error, which is the
	// better answer structurally, and what went unprobed is the message the
	// CLIENT LIBRARY wrote.
	//
	// It is a field on the harness rather than a parameter of [Harness.Open]
	// because the harness builds the configuration: a scenario has no way to
	// corrupt a credential it never sees.
	OpenWrongCredential func(ctx context.Context) error
	// Skip names scenarios this driver documents as inapplicable, with the
	// reason, which is recorded in the report. It is not a way to make a
	// failing scenario go away: the reason is published in the driver's own
	// matrix entry.
	Skip map[string]string
	// Advance moves the driver's clock, for a driver whose time is
	// deterministic. It is given the scenario's own client, because a driver
	// whose clock is per broker has one clock per client. Nil means the
	// scenarios that need time to pass really wait for it, which is what a
	// network broker requires and why a conformance run against one is
	// slower than a run in process.
	//
	// A movable clock has to read within an hour of the wall clock when the
	// scenario starts. capability/SeekByTime/works names its position from
	// the wall clock (time.Now() plus half of a two-hour Advance, made
	// between its first publication and the rest), so a broker clock more
	// than an hour away from the wall clock puts every message on one side
	// of the position. A clock started at time.Now(), as the memory
	// driver's TestClock is, meets it.
	Advance func(ctx context.Context, c *mq.Client, d time.Duration) error
	// Wildcard returns a subscription pattern that matches topic and at
	// least one other subject under the same root, for a driver that
	// advertises FeatureWildcardSubscribe.
	//
	// It exists because a wildcard subscription IS reachable through the
	// portable API — ConsumerConfig.Topic documents that "a driver that
	// supports FeatureWildcardSubscribe accepts its pattern syntax here" —
	// and the only thing the kit is missing is the syntax, which is the
	// driver's. Nil means the kit cannot write one, and every wildcard
	// scenario skips rather than taking the claim on trust. It partly
	// retires what is still unreachable is a server-side
	// FILTER that is not expressible as a topic pattern.
	Wildcard func(topic string) string
	// Quiesce waits until the broker has finished what it was doing, for a
	// driver that can say. Nil means the scenarios do not wait.
	Quiesce func(ctx context.Context, c *mq.Client) error
	// Now is the clock the report's date is taken from. Nil means the wall
	// clock.
	Now func() time.Time
}

// The harness defaults.
const (
	// DefaultTimeout bounds one scenario.
	DefaultTimeout = 30 * time.Second
	// DefaultFidelity is the specification's "byte-for-byte fidelity of 100,000
	// interleaved messages".
	DefaultFidelity = 100_000
)

// validate checks the harness before anything is opened.
func (h *Harness) validate() error {
	switch {
	case h.Driver == "":
		return errNoDriver
	case h.Open == nil:
		return errNoOpen
	case !h.Evidence.Valid():
		return errNoEvidence
	case h.Timeout < 0 || h.Fidelity < 0:
		return errBadBounds
	}
	// A Skip key that names no scenario is an excuse outliving its subject:
	// the scenario was renamed or removed, the driver's matrix entry still
	// carries the reason, and nothing has ever said the exemption stopped
	// applying. This repository has had that shape twice in its allowlists,
	// and a conformance skip is the version a reader is most likely to
	// believe.
	if len(h.Skip) > 0 {
		known := make(map[string]struct{}, len(Scenarios()))
		for _, s := range Scenarios() {
			known[s.Name] = struct{}{}
		}
		dead := make([]string, 0, len(h.Skip))
		for name := range h.Skip {
			if _, ok := known[name]; !ok {
				dead = append(dead, name)
			}
		}
		if len(dead) > 0 {
			sort.Strings(dead)
			return fmt.Errorf("%w: %s", errDeadSkip, strings.Join(dead, ", "))
		}
	}
	return nil
}

// timeout is the per-scenario bound.
func (h *Harness) timeout() time.Duration {
	if h.Timeout == 0 {
		return DefaultTimeout
	}
	return h.Timeout
}

// fidelity is how many messages the fidelity scenario sends.
func (h *Harness) fidelity() int {
	if h.Fidelity == 0 {
		return DefaultFidelity
	}
	return h.Fidelity
}

// topicName is TopicName with the default.
func (h *Harness) topicName(purpose string) string {
	if h.TopicName != nil {
		return h.TopicName(purpose)
	}
	// Forty random bits distinguish one run's topics from another's on a
	// shared broker, and from each other, without a counter shared by every
	// harness in the process; base 36 keeps the name within the shortest
	// broker limit.
	return "iqmq-conformance-" + strconv.FormatUint(rand.Uint64()>>24, 36) + "-" + purpose //nolint:gosec // uniqueness, not secrecy
}

// Env is what a scenario is given: the client, the capabilities and the topology
// it asked for.
//
// Everything a scenario does goes through these — the portable API and nothing
// else. There is no accessor here for a driver's own types, and that is
// deliberate: a conformance kit that could reach into a driver would pass one
// whose public behaviour was wrong.
type Env struct {
	// Harness is the harness the run was started with.
	Harness Harness
	// Client is the client opened for this scenario. It is closed when the
	// scenario returns.
	Client *mq.Client
	// Caps is the client's capability table.
	Caps mq.Capabilities
	// Need is the topology the scenario asked for and the driver built.
	Need Need
	// Topic is Need.Topic, which is what nearly every scenario publishes to.
	Topic string
	// Sub is the scenario's main subscription.
	Sub string

	// notes are what the scenario recorded about HOW it reached its
	// outcome, which the report carries alongside the outcome.
	notes []string
	// oneSided records that this run found the claim to be one a single run
	// cannot settle, which makes the result's evidence one-sided whatever
	// the scenario's static Kind said. Only the capability scenarios set
	// it, and they set it for a driver that ACCEPTED the feature's call and
	// did something else: there was no refusal to observe, so the denial is
	// unrefuted rather than refuted or false.
	oneSided bool
}

// Note records a line about how the scenario reached its outcome, which lands
// in the report's note whether the scenario passes, fails or skips.
//
// It exists because of the one defect class the specification's "a skip is never a pass"
// does not cover: a PASS with nothing recorded is indistinguishable from a
// pass that checked nothing. The specification's `message.max.bytes` lookup returned nothing
// against Apache Kafka for four months and no scenario could notice, because
// publish/oversized-payload skips when the limit is unknown; the answer is a
// scenario that says what it saw, on the way past, in the file a reviewer
// reads.
func (e *Env) Note(format string, args ...any) {
	e.notes = append(e.notes, failf(format, args...).Error())
}

// Timeout is the scenario's bound.
func (e *Env) Timeout() time.Duration { return e.Harness.timeout() }

// Publisher opens a publisher on the scenario's topic with the given
// confirmation and metadata modes.
func (e *Env) Publisher(ctx context.Context, cfg mq.PublisherConfig) (*mq.PublisherHandle, error) {
	if cfg.Topic == "" {
		cfg.Topic = e.Topic
	}
	return e.Client.Publisher(ctx, cfg)
}

// Consumer opens a consumer on the scenario's topic and main subscription.
func (e *Env) Consumer(ctx context.Context, cfg mq.ConsumerConfig) (*mq.ConsumerHandle, error) {
	if cfg.Topic == "" {
		cfg.Topic = e.Topic
	}
	if cfg.Subscription == "" {
		cfg.Subscription = e.Sub
	}
	return e.Client.Consumer(ctx, cfg)
}

// Advance moves time forward, on the driver's clock when it has one it can move
// and by waiting when it does not. A driver with no movable clock makes the
// scenarios that need a lease to expire genuinely wait, which is why a
// conformance run against a real broker is slower than one in process.
func (e *Env) Advance(ctx context.Context, d time.Duration) error {
	if e.Harness.Advance != nil {
		return e.Harness.Advance(ctx, e.Client, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Quiesce waits for the broker to finish what it was doing, for a driver that
// can say so, and does nothing for one that cannot.
func (e *Env) Quiesce(ctx context.Context) error {
	if e.Harness.Quiesce != nil {
		return e.Harness.Quiesce(ctx, e.Client)
	}
	return nil
}

// skipError is a scenario reporting that it could not run.
type skipError struct{ reason string }

// Error implements error.
func (s *skipError) Error() string { return "skipped: " + s.reason }

// Skip returns the error a scenario returns when it cannot run, with the reason
// recorded in the report.
//
// A skip is not a pass. The claim a skipped scenario would have proved stays
// unverified, and the generated matrix renders it as unsupported, which is the
// only honest answer for something nobody checked.
func Skip(reason string) error { return &skipError{reason: reason} }

// IsSkip reports whether err is a skip, and returns the reason.
func IsSkip(err error) (string, bool) {
	var s *skipError
	if errors.As(err, &s) {
		return s.reason, true
	}
	return "", false
}

// SkipUnsupported turns a driver's ErrUnsupported into a skip, which is what a
// scenario does with a topology the driver said it cannot build.
func SkipUnsupported(err error, what string) error {
	if errors.Is(err, mq.ErrUnsupported) {
		return Skip(what + ": the driver reports ErrUnsupported")
	}
	return err
}

// The harness's construction failures.
var (
	errNoDriver = errors.New("testkit: the harness names no driver")
	// errDeadSkip is a Harness.Skip key naming a scenario that does not
	// exist: the exemption outlived its subject and nothing said so.
	errDeadSkip   = errors.New("testkit: Harness.Skip names scenario(s) the kit does not have; a skip whose scenario was renamed or removed keeps excusing a driver for something nobody runs")
	errNoOpen     = errors.New("testkit: the harness has no Open")
	errNoEvidence = errors.New("testkit: the harness does not say whether its evidence is a container, an emulator, a managed service or in process")
	errBadBounds  = errors.New("testkit: the harness's Timeout and Fidelity cannot be negative")
)
