// Package pulsarfake is an in-process model of Apache Pulsar AND of
// pulsar-client-go's own bookkeeping, for the driver's unit tests.
//
// # Why the seam is above the library
//
// The cost is stated plainly: this fake cannot catch a defect in
// pulsar-client-go. The conformance suite against the pinned container is what
// proves the fake did not lie.
//
// # What it models
//
//   - the four subscription types, with their dispatch rules: Shared's round
//     robin, Key_Shared's hash ranges (and the BLOCKING of a reassigned range
//     while its previous owner still holds messages from it), Exclusive's
//     refusal of a second consumer, and Failover's standby.
//   - a partitioned topic as independent logs, and partition EXPANSION: a
//     producer that discovers the new count puts a key in a different
//     partition, after which the key's older and newer messages are in two
//     logs with no order between them.
//   - batched entries. One entry holds several messages, an entry is dispatched
//     to ONE consumer, and an acknowledgement names an entry — with an
//     acknowledgement set when the client sends one and the broker honours it.
//   - redelivery under the SAME identifier, which is what makes a stale
//     settlement able to land on a later delivery.
//   - delayed delivery, and its absence on an Exclusive or Failover
//     subscription.
//   - the retry topic and the dead-letter topic, including the router's rule
//     that the original is acknowledged only after the republication succeeded.
//
// # The clock
//
// [Server] is an [bitbucket.org/iqhive/iqmq.Clock]: nothing here reads the wall
// clock, and [Server.Advance] is what makes a lease expire, a delayed message
// become deliverable and the driver's own sweeper run. A test that waited for
// real seconds would be a test nobody runs.
package pulsarfake

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
)

// HashRanges is how many hash ranges a Key_Shared subscription is divided into.
//
// Pulsar uses 65,536; this model uses a smaller number so that a test with two
// or three consumers can name a key that lands in a particular range without
// searching for one. The MECHANISM is the same: a range has one owner, a
// consumer set change moves ranges, and a moved range is blocked while its
// previous owner still holds messages from it.
const HashRanges = 16

// Stats are what one subscription did, which is what a test asserts on.
type Stats struct {
	// Delivered is how many messages were handed to a consumer, counting
	// redeliveries.
	Delivered int
	// Acked is how many messages the broker marked consumed.
	Acked int
	// Nacked is how many negative acknowledgements arrived.
	Nacked int
	// CrossSettled is how many messages were marked consumed WITHOUT
	// anybody acknowledging them, because an acknowledgement settled the
	// whole entry they were in.
	//
	// It is the number the batch-index critical test exists for: it is the specification's "can lose an accepted message outside a documented failure
	// boundary", counted.
	CrossSettled int
	// StaleSettled is how many settlements arrived for a delivery a later
	// one had superseded and were applied anyway.
	StaleSettled int
	// Retried is how many messages went to the retry topic.
	Retried int
	// DeadLettered is how many messages went to the dead-letter topic.
	DeadLettered int
	// Deferred is how many messages are waiting for a delivery time.
	Deferred int
	// Redelivered is how many deliveries were second or later ones.
	Redelivered int
}

// Option configures a [Server].
type Option func(*options)

type options struct {
	batchSize      int
	brokerBatchAck bool
	untracked      bool
	nackDelay      time.Duration
	start          time.Time
}

// WithBatchSize makes the producer put n messages in one entry, which is what
// gives a test a BATCH to acknowledge one message of. One — the default —
// models a producer with batching disabled.
func WithBatchSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

// WithBrokerBatchIndexAck says whether the broker honours an acknowledgement
// set, which is Pulsar's own `acknowledgmentAtBatchIndexLevelEnabled`. It is
// FALSE by default, which is Apache Pulsar's default.
func WithBrokerBatchIndexAck(v bool) Option {
	return func(o *options) { o.brokerBatchAck = v }
}

// WithUntrackedIDs makes the consumer hand out message identifiers that have
// LOST the client's per-entry batch bookkeeping, which is what an identifier
// put through Serialize and DeserializeMessageID looks like.
//
// It is the seam [bitbucket.org/iqhive/iqmq/drivers/pulsar/internal/pulsarapi.BatchTracker]
// exists for: through the MessageID interface such an identifier is
// indistinguishable from a tracked one, and acknowledging one message of a
// batch with it settles every sibling.
func WithUntrackedIDs() Option {
	return func(o *options) { o.untracked = true }
}

// WithNackRedeliveryDelay sets how long a negative acknowledgement holds a
// message back, which is the consumer's NackRedeliveryDelay.
func WithNackRedeliveryDelay(d time.Duration) Option {
	return func(o *options) { o.nackDelay = d }
}

// Server is the in-process broker and client model.
type Server struct {
	opt options

	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	topics map[string]*ftopic
}

// New builds a server. Its clock starts at a fixed instant so that a failure
// message is the same on every run.
func New(opts ...Option) *Server {
	o := options{batchSize: 1, nackDelay: time.Second, start: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	for _, fn := range opts {
		fn(&o)
	}
	return &Server{opt: o, now: o.start, topics: map[string]*ftopic{}}
}

// Client returns the client seam the driver is opened with.
func (s *Server) Client() pulsarapi.Client { return &fclient{s: s} }

// Now is the model's clock (the injected clock).
func (s *Server) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

// NewTimer is a timer on the model's clock.
func (s *Server) NewTimer(d time.Duration) mq.Timer {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &fakeTimer{s: s, ch: make(chan time.Time, 1), due: s.now.Add(d), live: true}
	s.timers = append(s.timers, t)
	return t
}

// Flush closes every producer's open batch, which is what
// BatchingMaxPublishDelay elapsing does.
//
// A test that publishes a number of messages that is not a whole number of
// batches calls it; one that fills its batches exactly does not need to.
func (s *Server) Flush() {
	s.mu.Lock()
	for _, t := range s.topics {
		for _, p := range t.parts {
			for _, e := range p.entries {
				e.closed = true
			}
		}
	}
	s.mu.Unlock()
	s.pumpAll()
}

// Advance moves the clock, fires every timer that came due and re-dispatches
// whatever became deliverable.
//
// It closes every open batch as well, because BatchingMaxPublishDelay is one of
// the things time passing does.
func (s *Server) Advance(d time.Duration) {
	s.mu.Lock()
	s.now = s.now.Add(d)
	for _, t := range s.topics {
		for _, p := range t.parts {
			for _, e := range p.entries {
				e.closed = true
			}
		}
	}
	now := s.now
	var fire []*fakeTimer
	for _, t := range s.timers {
		if t.live && !t.due.After(now) {
			t.live = false
			fire = append(fire, t)
		}
	}
	s.mu.Unlock()
	for _, t := range fire {
		select {
		case t.ch <- now:
		default:
		}
	}
	s.pumpAll()
}

// fakeTimer is one timer on the model's clock.
type fakeTimer struct {
	s   *Server
	ch  chan time.Time
	due time.Time

	live bool
}

// C is the timer's channel.
func (t *fakeTimer) C() <-chan time.Time { return t.ch }

// Stop stops the timer.
func (t *fakeTimer) Stop() bool {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	was := t.live
	t.live = false
	return was
}

// Reset restarts the timer.
func (t *fakeTimer) Reset(d time.Duration) bool {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	was := t.live
	t.due = t.s.now.Add(d)
	t.live = true
	return was
}

// slot is one message's position: the partition's log, the entry in it, and the
// message's index inside the entry.
type slot struct {
	part  int
	entry int
	idx   int
}

// fmsg is one stored message.
type fmsg struct {
	payload     []byte
	nullValue   bool
	key         string
	orderingKey string
	props       map[string]string
	eventTime   time.Time
	publishTime time.Time
	producer    string
}

// fentry is one broker entry, which holds a batch of one or more messages.
type fentry struct {
	msgs []*fmsg
	// due is when the entry becomes deliverable, which is Pulsar's delayed
	// delivery. A zero time means at once.
	due time.Time
	// closed reports whether the producer has finished filling the batch.
	//
	// It is what makes a batch a batch. A producer's open batch has not been
	// SENT, so the broker does not have it and no consumer can receive it;
	// pulsar-client-go closes one when it is full, when
	// BatchingMaxPublishDelay elapses, on a Flush and on a Close. Modelling
	// that is not pedantry: without it the model delivers the first message
	// of a batch on its own and the rest never, which is the first thing the
	// batch-index critical test found.
	closed bool
}

// plog is one partition's append-only log.
type plog struct{ entries []*fentry }

// ftopic is one topic: a partition count and one log per partition.
type ftopic struct {
	name  string
	count int // 0 for a non-partitioned topic
	parts []*plog
	subs  map[string]*fsub
	// batcher is the batcher the last producer on this topic asked for,
	// which is what the Key_Shared ordering test reads: the key-based
	// batcher is what keeps one key in one entry.
	batcher pulsar.BatcherBuilderType
	// rr is the round-robin cursor for a keyless publication.
	rr int
	// queueFull makes the next publication answer ProducerQueueIsFull.
	queueFull bool
}

// logOf is the partition's log, creating it if the count grew.
func (t *ftopic) logOf(i int) *plog {
	for len(t.parts) <= i {
		t.parts = append(t.parts, &plog{})
	}
	return t.parts[i]
}

// partitionCount is how many logs the topic has, at least one.
func (t *ftopic) partitionCount() int {
	if t.count <= 0 {
		return 1
	}
	return t.count
}

// topic returns the topic, creating it on first use the way a Pulsar namespace
// with automatic creation does.
func (s *Server) topic(name string) *ftopic {
	t := s.topics[name]
	if t == nil {
		t = &ftopic{name: name, subs: map[string]*fsub{}}
		t.parts = []*plog{{}}
		s.topics[name] = t
	}
	return t
}

// AddPartitions raises a topic's partition count, which is Pulsar's partition
// expansion.
func (s *Server) AddPartitions(name string, count int) {
	s.mu.Lock()
	t := s.topic(name)
	if count > t.count {
		t.count = count
		t.logOf(count - 1)
	}
	s.mu.Unlock()
	s.pumpAll()
}

// SetQueueFull makes every producer on a topic answer the next publication with
// ProducerQueueIsFull, which is what a producer whose send queue has filled
// does when DisableBlockIfQueueFull is set.
//
// It exists because the backpressure is a distinct answer and not a
// failure, and filling a real producer's queue takes a broker that has stopped
// accepting — which is not something a unit test can arrange.
func (s *Server) SetQueueFull(topic string, v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.topic(topic)
	t.queueFull = v
}

// Partitions is a topic's partition count, zero for a non-partitioned topic.
func (s *Server) Partitions(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.topics[name]; t != nil {
		return t.count
	}
	return 0
}

// Batcher is the batcher the last producer on a topic asked for.
func (s *Server) Batcher(name string) pulsar.BatcherBuilderType {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.topics[name]; t != nil {
		return t.batcher
	}
	return pulsar.DefaultBatchBuilder
}

// Entries is how many entries a topic holds across every partition, which is
// what says whether the producer batched.
func (s *Server) Entries(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.topics[name]
	if t == nil {
		return 0
	}
	n := 0
	for _, p := range t.parts {
		n += len(p.entries)
	}
	return n
}

// Messages is how many messages a topic holds across every partition.
func (s *Server) Messages(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.topics[name]
	if t == nil {
		return 0
	}
	n := 0
	for _, p := range t.parts {
		for _, e := range p.entries {
			n += len(e.msgs)
		}
	}
	return n
}

// Stats is what one subscription did.
func (s *Server) Stats(topic, sub string) Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.topics[topic]
	if t == nil {
		return Stats{}
	}
	fs := t.subs[sub]
	if fs == nil {
		return Stats{}
	}
	out := fs.stats
	out.Deferred = 0
	for i, p := range t.parts {
		for j, e := range p.entries {
			if e.due.After(s.now) && !fs.entryDone(t, slot{part: i, entry: j}) {
				out.Deferred += len(e.msgs)
			}
		}
	}
	return out
}

// Redeliver makes every message the subscription has handed out and not
// acknowledged available again, which is what Pulsar does when a consumer
// disconnects.
//
// The messages come back under the SAME identifiers, which is what makes a
// stale settlement able to land on a later delivery.
func (s *Server) Redeliver(topic, sub string) {
	s.mu.Lock()
	if t := s.topics[topic]; t != nil {
		if fs := t.subs[sub]; fs != nil {
			fs.releaseAll()
		}
	}
	s.mu.Unlock()
	s.pumpAll()
}

// Backlog is how many messages the subscription has neither acknowledged nor
// handed out.
func (s *Server) Backlog(topic, sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.topics[topic]
	if t == nil {
		return 0
	}
	fs := t.subs[sub]
	if fs == nil {
		return 0
	}
	n := 0
	for i, p := range t.parts {
		for j, e := range p.entries {
			for k := range e.msgs {
				sl := slot{part: i, entry: j, idx: k}
				if !fs.acked[sl] && !fs.inflight[sl] {
					n++
				}
			}
		}
	}
	return n
}

// InFlight is how many of the subscription's messages are out with a consumer
// and not yet settled.
func (s *Server) InFlight(topic, sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.topics[topic]
	if t == nil {
		return 0
	}
	fs := t.subs[sub]
	if fs == nil {
		return 0
	}
	return len(fs.inflight)
}

// pumpAll re-dispatches every subscription of every topic.
func (s *Server) pumpAll() {
	s.mu.Lock()
	type pair struct {
		t  *ftopic
		fs *fsub
	}
	var all []pair
	names := make([]string, 0, len(s.topics))
	for name := range s.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t := s.topics[name]
		subs := make([]string, 0, len(t.subs))
		for sn := range t.subs {
			subs = append(subs, sn)
		}
		sort.Strings(subs)
		for _, sn := range subs {
			all = append(all, pair{t: t, fs: t.subs[sn]})
		}
	}
	for _, p := range all {
		p.fs.dispatch(p.t, s.now)
	}
	s.mu.Unlock()
}

// JavaStringHash is Pulsar's default hashing scheme, exported so that a test
// can find a key that lands where the test needs it rather than hard-coding one
// and asserting arithmetic.
func JavaStringHash(s string) uint32 { return javaStringHash(s) }

// RangeOf is the hash range a key belongs to, out of [HashRanges].
func RangeOf(key string) int { return rangeOf(key) }

// PartitionOf is the partition a key is routed to on a topic with count
// partitions, which is the producer's router.
func PartitionOf(key string, count int) int {
	if count <= 1 {
		return 0
	}
	return int(javaStringHash(key) % uint32(count))
}

// javaStringHash is Pulsar's default hashing scheme, which is Java's
// String.hashCode with the sign cleared. It is reproduced rather than
// approximated so that a key lands in the same range here as it would at a
// broker.
func javaStringHash(s string) uint32 {
	var h int32
	for i := range len(s) {
		h = 31*h + int32(s[i])
	}
	return uint32(h) & 0x7fffffff
}

// rangeOf is the hash range a key belongs to.
func rangeOf(key string) int { return int(javaStringHash(key) % HashRanges) }

// Error is a failure carrying one of pulsar-client-go's result codes.
//
// It exists because the library exports `pulsar.Error` and no constructor for
// it, so a model cannot produce the library's own error value. The driver
// classifies on the RESULT — through the
// [bitbucket.org/iqhive/iqmq/drivers/pulsar.Resulter] interface — rather than on
// the concrete type, which is what lets this model exercise the driver's error
// mapper at all.
type Error struct {
	// Code is the Pulsar result.
	Code pulsar.Result
	// Reason is what happened, for a person reading a test failure.
	Reason string
}

// Result is the Pulsar result code.
func (e *Error) Result() pulsar.Result { return e.Code }

// Error implements error.
func (e *Error) Error() string {
	return "pulsarfake: " + e.Reason + " (result " + itoa(int(e.Code)) + ")"
}

// resultErr builds a failure the driver's error mapper can classify.
//
// Where the library has a package-level sentinel with the same result, that
// value is used instead — so a test can also assert with errors.Is against the
// library's own sentinel.
func resultErr(r pulsar.Result, msg string) error {
	switch r {
	case pulsar.ProducerQueueIsFull:
		return pulsar.ErrSendQueueIsFull
	case pulsar.MessageTooBig:
		return pulsar.ErrMessageTooLarge
	case pulsar.ProducerClosed:
		return pulsar.ErrProducerClosed
	case pulsar.TopicNotFound:
		return pulsar.ErrTopicNotfound
	case pulsar.TimeoutError:
		return pulsar.ErrSendTimeout
	case pulsar.ProducerFenced:
		return pulsar.ErrProducerFenced
	default:
		return &Error{Code: r, Reason: msg}
	}
}

// itoa is strconv.Itoa under a shorter name, for the property values the retry
// router writes.
func itoa(n int) string { return strconv.Itoa(n) }

// atoi is strconv.Atoi under a shorter name, for the reconsume count the retry
// router reads.
func atoi(s string) (int, error) { return strconv.Atoi(s) }

// ctxErr is the context's error or nil, so that a fake call honours a
// cancelled context the way the library does.
func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
