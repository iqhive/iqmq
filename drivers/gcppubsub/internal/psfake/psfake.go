// Package psfake is a scripted in-memory [psapi.Client] for the driver's L1
// tests and fuzz targets.
//
// It is not a second implementation of Pub/Sub and it does not try to be. What
// it is for is the set of behaviours the real service and the pinned emulator
// cannot be made to produce on demand: an acknowledgement the service refuses,
// a streaming pull that dies, an ordering key the publisher has paused, a
// delivery whose deadline has just expired. Every one of those decides a
// settlement rule, and a test that cannot produce it is a test that cannot
// fail.
//
// The behaviours it DOES model faithfully are the three the driver's
// correctness turns on:
//
//   - The ordering-key pause. A publisher that fails a publication carrying an
//     ordering key refuses every later publication of that key until
//     ResumePublish is called, exactly as the client library does. That is the
//     contract [github.com/iqhive/iqmq/drivers/gcppubsub]'s completion path
//     exists to keep, and this is what can prove it is kept.
//   - Redelivery. An outstanding message whose deadline is expired is
//     delivered again with a NEW settlement handle, which is what makes a
//     stale settlement detectable and is the shape the local lease check
//     defends against.
//   - A publication the library still holds ([Server.GatePublishResults]). It
//     is held by the caller's own bytes, read only when it is sent, and its
//     outcome, the ordering-key pause included, is decided then, one
//     publication at a time when a test asks for it. Flush waits for it, and
//     a Stop beside a running Flush is counted, which the library does not
//     survive.
package psfake

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// Module and Version identify this fake through DriverInfo, so that a report
// or a log line made against it can never be mistaken for one made against a
// real client.
const (
	Module     = "github.com/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
	Version    = "v0.0.0-fake"
	Generation = "fake"
)

// Server is the fake service. It is safe for concurrent use.
type Server struct {
	mu      sync.Mutex
	project string
	topics  map[string]*topic
	subs    map[string]*subscription
	nextID  int
	closed  bool
	// pubs is every publisher this server built, which is what
	// [Server.Paused] reads.
	pubs []*publisher

	// deadline is how long a delivery is outstanding before [Server.Expire]
	// will redeliver it. It is not a timer: nothing in this fake expires on
	// its own, because a test that waits for a deadline is a slow test that
	// fails on a loaded host.
	deadline time.Duration

	// The injection points. Each is consulted under the server's lock and
	// may be changed between operations.
	//
	// PublishErr fails a publication. AckErr and NackErr fail a settlement
	// with a RESULT, which is what an exactly-once subscription reports.
	// ReceiveErr ends a streaming pull.
	PublishErr func(topic string, out *psapi.Outgoing) error
	AckErr     func(ackID string) error
	NackErr    func(ackID string) error
	ReceiveErr func(sub string) error
	AdminErr   func(op, name string) error

	// gated, when true, holds every publication made since
	// [Server.GatePublishResults] in held, oldest first, until it is
	// released. It is what lets a test hold 1,000 publications
	// OUTSTANDING at once, which is the only way to tell a fixed completion
	// pool from a goroutine per message: with results that resolve
	// immediately, both shapes finish before the next publication starts and
	// the goroutine count never grows.
	//
	// A held publication is one the client library still holds, and it is
	// modelled the way the library holds one: by the caller's own Outgoing,
	// whose Data is the caller's bytes, read only when the publication is
	// released. That is what lets a test see a driver that handed the library
	// a buffer its caller reuses. Its outcome is decided then too, as the
	// library decides a bundle's when the bundle comes up: a publication of an
	// ordering key that was paused meanwhile is refused with the paused-key
	// error, and one PublishErr failed pauses its key only then.
	gated bool
	held  []*heldPublication

	// stopsInFlush counts the publishers stopped while a Flush of theirs was
	// still running. The client library does not survive that: its Flush
	// reads the publisher's stopped flag without the lock Stop writes it
	// under, a data race only -race sees. This count is what a test without
	// -race asserts on.
	stopsInFlush int
}

var (
	_ psapi.Client        = (*Server)(nil)
	_ psapi.TopicRetainer = (*fakeAdmin)(nil)
)

// topic is one topic and the subscriptions attached to it.
//
// retention is the topic's messageRetentionDuration, zero for none. A topic
// with retention keeps every publication in history, whatever its
// subscriptions acknowledge, and that history is what a seek on any attached
// subscription reads, including one created after the publication. A new
// subscription starts with an empty backlog either way: the managed service
// begins a subscription at its own creation whatever the topic retains, and
// only a seek reaches further back. The pinned emulator differs here (a new
// subscription on a topic with retention receives the retained messages
// without any seek), so this fake is the oracle for the seek and the emulator
// is not.
type topic struct {
	name      string
	subs      map[string]*subscription
	retention time.Duration
	history   []*record
}

// subscription is one subscription: its configuration, its backlog and what it
// has outstanding.
type subscription struct {
	spec    psapi.SubscriptionSpec
	backlog []*record
	out     map[string]*record
	// waiters are the receive loops parked on an empty backlog.
	waiters []chan struct{}
}

// record is one message in a subscription's backlog.
type record struct {
	id      string
	ackID   string
	data    []byte
	attrs   map[string]string
	order   string
	created time.Time
	// attempt counts deliveries, which the service reports only when the
	// subscription has a dead-letter policy — and so does this fake.
	attempt int
}

// New returns an empty server for one project.
func New(project string) *Server {
	return &Server{
		project:  project,
		topics:   map[string]*topic{},
		subs:     map[string]*subscription{},
		deadline: 10 * time.Second,
	}
}

// Project is the project the server serves.
func (s *Server) Project() string { return s.project }

// Generation identifies this implementation.
func (s *Server) Generation() string { return Generation }

// ClientModule identifies this implementation's module.
func (s *Server) ClientModule() string { return Module }

// ClientVersion identifies this implementation's version.
func (s *Server) ClientVersion() string { return Version }

// Native is the server itself, which is what a test asserting on
// Backend.Native reads.
func (s *Server) Native() any { return s }

// Close marks the server closed and wakes every parked receive loop.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, sub := range s.subs {
		s.wakeLocked(sub)
	}
	return nil
}

// Admin is the administration surface.
func (s *Server) Admin() psapi.Admin { return (*fakeAdmin)(s) }

// Publisher returns a publisher for one topic.
func (s *Server) Publisher(cfg psapi.PublisherConfig) (psapi.Publisher, error) {
	if cfg.Topic == "" {
		return nil, errors.New("psfake: a publisher needs a topic")
	}
	p := &publisher{s: s, topic: cfg.Topic, ordering: cfg.Ordering, paused: map[string]bool{}}
	s.mu.Lock()
	s.pubs = append(s.pubs, p)
	s.mu.Unlock()
	return p, nil
}

// Paused reports whether a publisher of topic has paused an ordering key,
// which is how a test asserts on the publisher a driver built for itself.
func (s *Server) Paused(topic, key string) bool {
	s.mu.Lock()
	pubs := append([]*publisher(nil), s.pubs...)
	s.mu.Unlock()
	for _, p := range pubs {
		if p.topic == topic && p.Paused(key) {
			return true
		}
	}
	return false
}

// Subscription returns a subscription handle.
func (s *Server) Subscription(cfg psapi.SubscriptionConfig) (psapi.Subscription, error) {
	if cfg.Subscription == "" {
		return nil, errors.New("psfake: a subscription handle needs a subscription")
	}
	return &receiver{s: s, name: cfg.Subscription}, nil
}

// CreateTopic makes a topic, for a test that provisions without the driver.
func (s *Server) CreateTopic(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topicLocked(name)
}

// CreateRetainedTopic makes a topic with message retention, for a test that
// provisions without the driver: a topic somebody else created with retention
// is one this driver has to find and read.
func (s *Server) CreateRetainedTopic(name string, retention time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topicLocked(name).retention = retention
}

// CreateSubscription makes a subscription, for a test that provisions without
// the driver.
func (s *Server) CreateSubscription(spec psapi.SubscriptionSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptionLocked(spec)
}

// GatePublishResults holds every publication made after this call, unsent,
// until it is released: its Result.Get blocks, and its body is read only at
// the release.
func (s *Server) GatePublishResults() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gated = true
}

// ReleasePublishResults sends every held publication, oldest first, and ends
// the gate.
func (s *Server) ReleasePublishResults() {
	for {
		s.mu.Lock()
		batch := s.held
		s.held = nil
		if len(batch) == 0 {
			s.gated = false
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		for _, h := range batch {
			s.send(h)
		}
	}
}

// ReleaseOldestPublishResult sends the oldest held publication and keeps the
// gate, reporting false when none is held. It is how a test lets the driver
// read one publication's outcome before the library takes up the next, which
// is the order the library answers the publications of one ordering key in.
func (s *Server) ReleaseOldestPublishResult() bool {
	s.mu.Lock()
	if len(s.held) == 0 {
		s.mu.Unlock()
		return false
	}
	h := s.held[0]
	s.held = s.held[1:]
	s.mu.Unlock()
	s.send(h)
	return true
}

// HeldPublications is how many publications the gate is holding.
func (s *Server) HeldPublications() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.held)
}

// StopsInFlush is how many publishers were stopped while a Flush of theirs
// was still running.
func (s *Server) StopsInFlush() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopsInFlush
}

// SetDeadline sets how long a delivery is outstanding before [Server.Expire]
// redelivers it.
func (s *Server) SetDeadline(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadline = d
}

// Expire redelivers every outstanding message of every subscription, which is
// what the service does when an acknowledgement deadline passes.
//
// It is explicit rather than time-driven: a test that waits out a real
// deadline is a slow test that fails on a loaded host, and the behaviour under
// test is what the DRIVER does with a redelivery rather than when the service
// produces one.
func (s *Server) Expire() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sub := range s.subs {
		for ackID, rec := range sub.out {
			delete(sub.out, ackID)
			sub.backlog = append(sub.backlog, rec)
			n++
		}
		if n > 0 {
			s.wakeLocked(sub)
		}
	}
	return n
}

// Backlog is how many messages a subscription has undelivered.
func (s *Server) Backlog(sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if got, ok := s.subs[sub]; ok {
		return len(got.backlog)
	}
	return 0
}

// Outstanding is how many messages a subscription has delivered and not
// settled.
func (s *Server) Outstanding(sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if got, ok := s.subs[sub]; ok {
		return len(got.out)
	}
	return 0
}

// topicLocked returns the topic, creating it if necessary.
func (s *Server) topicLocked(name string) *topic {
	t, ok := s.topics[name]
	if !ok {
		t = &topic{name: name, subs: map[string]*subscription{}}
		s.topics[name] = t
	}
	return t
}

// subscriptionLocked creates the subscription if it is not already there.
func (s *Server) subscriptionLocked(spec psapi.SubscriptionSpec) {
	if _, ok := s.subs[spec.Subscription]; ok {
		return
	}
	sub := &subscription{spec: spec, out: map[string]*record{}}
	s.subs[spec.Subscription] = sub
	s.topicLocked(spec.Topic).subs[spec.Subscription] = sub
}

// wakeLocked releases every receive loop parked on this subscription.
func (s *Server) wakeLocked(sub *subscription) {
	for _, ch := range sub.waiters {
		close(ch)
	}
	sub.waiters = nil
}

// nextIDLocked mints a message identifier.
func (s *Server) nextIDLocked() string {
	s.nextID++
	return "m" + strconv.Itoa(s.nextID)
}

// publisher is one topic's publication handle.
type publisher struct {
	s        *Server
	topic    string
	ordering bool

	mu     sync.Mutex
	paused map[string]bool
	closed bool
	// flushing is how many Flush calls are running.
	flushing int
}

// errServerClosed is a publication's answer from a closed server.
var errServerClosed = errors.New("psfake: the server is closed")

// Publish delivers the message to every subscription of the topic, or holds
// it while the server's gate is up.
//
// The ordering-key pause is modelled here, and it is the reason this fake
// exists: a publication carrying a key the publisher has paused is refused
// without reaching the topic, and a publication that FAILS pauses its key. The
// client library does exactly this, and a driver that does not resume the key
// silently stops carrying it.
func (p *publisher) Publish(_ context.Context, m *psapi.Outgoing) psapi.Result {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return failed(errors.New("psfake: the publisher is stopped"))
	}
	s := p.s
	s.mu.Lock()
	if s.gated {
		// PublishErr is asked now, which is what lets a test see that a
		// publication is in flight; its answer is applied when the
		// publication is sent.
		h := &heldPublication{pub: p, out: *m, ready: make(chan struct{})}
		if s.PublishErr != nil {
			h.err = s.PublishErr(p.topic, m)
		}
		s.held = append(s.held, h)
		s.mu.Unlock()
		p.mu.Unlock()
		return h
	}
	s.mu.Unlock()
	if m.OrderingKey != "" && p.paused[m.OrderingKey] {
		p.mu.Unlock()
		return failed(psapi.ErrOrderingPaused)
	}
	p.mu.Unlock()
	if m.OrderingKey != "" && !p.ordering {
		return failed(errors.New("psfake: the publisher was built without ordering"))
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return p.fail(m.OrderingKey, errServerClosed)
	}
	if s.PublishErr != nil {
		if err := s.PublishErr(p.topic, m); err != nil {
			s.mu.Unlock()
			return p.fail(m.OrderingKey, err)
		}
	}
	id := s.deliverLocked(p.topic, m)
	s.mu.Unlock()
	return result{id: id}
}

// deliverLocked stores one publication on its topic and every subscription
// whose filter it passes, copying its bytes, and returns its identifier.
func (s *Server) deliverLocked(topic string, m *psapi.Outgoing) string {
	id := s.nextIDLocked()
	t := s.topicLocked(topic)
	if t.retention > 0 {
		t.history = append(t.history, &record{
			id: id, data: clone(m.Data), attrs: cloneMap(m.Attributes),
			order: m.OrderingKey, created: time.Now(),
		})
	}
	for _, sub := range t.subs {
		if sub.spec.Filter != "" && !matches(sub.spec.Filter, m.Attributes) {
			continue
		}
		sub.backlog = append(sub.backlog, &record{
			id: id, data: clone(m.Data), attrs: cloneMap(m.Attributes),
			order: m.OrderingKey, created: time.Now(),
		})
		s.wakeLocked(sub)
	}
	return id
}

// send sends one held publication and readies its result: refused when
// PublishErr failed it or its ordering key is paused by now, delivered
// otherwise, with its body read only now.
func (s *Server) send(h *heldPublication) {
	p, key := h.pub, h.out.OrderingKey
	switch {
	case h.err != nil:
		p.pause(key)
		h.fail = h.err
	case key != "" && p.Paused(key):
		h.fail = psapi.ErrOrderingPaused
	case key != "" && !p.ordering:
		h.fail = errors.New("psfake: the publisher was built without ordering")
	default:
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			p.pause(key)
			h.fail = errServerClosed
			break
		}
		h.id = s.deliverLocked(p.topic, &h.out)
		s.mu.Unlock()
	}
	close(h.ready)
}

// heldBy takes the publications the gate holds for one publisher, oldest
// first. take removes them from the gate.
func (s *Server) heldBy(p *publisher, take bool) []*heldPublication {
	s.mu.Lock()
	defer s.mu.Unlock()
	var mine []*heldPublication
	kept := s.held[:0]
	for _, h := range s.held {
		if h.pub == p {
			mine = append(mine, h)
			if take {
				continue
			}
		}
		kept = append(kept, h)
	}
	s.held = kept
	return mine
}

// fail pauses the ordering key, as the client library does, and returns the
// failure.
func (p *publisher) fail(key string, err error) psapi.Result {
	p.pause(key)
	return failed(err)
}

// pause pauses an ordering key, as the client library does when a
// publication of it fails.
func (p *publisher) pause(key string) {
	if key == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused[key] = true
}

// ResumePublish clears an ordering key's pause.
func (p *publisher) ResumePublish(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.paused, key)
}

// Paused reports whether an ordering key is paused, which is what a test
// asserts on.
func (p *publisher) Paused(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused[key]
}

// Flush waits for this publisher's held publications to be sent, as the
// library's Flush waits for what its bundlers hold. Without a gate there is
// nothing to wait for: this fake publishes synchronously.
func (p *publisher) Flush() {
	p.mu.Lock()
	p.flushing++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.flushing--
		p.mu.Unlock()
	}()
	for _, h := range p.s.heldBy(p, false) {
		<-h.ready
	}
}

// Stop sends what the gate holds for this publisher, as the library's Stop
// flushes, and marks the publisher unusable. A Stop while a Flush of this
// publisher is still running is counted ([Server.StopsInFlush]).
func (p *publisher) Stop() {
	p.mu.Lock()
	inFlush := p.flushing > 0
	p.closed = true
	p.mu.Unlock()
	s := p.s
	if inFlush {
		s.mu.Lock()
		s.stopsInFlush++
		s.mu.Unlock()
	}
	for _, h := range s.heldBy(p, true) {
		s.send(h)
	}
}

// result is a [psapi.Result] whose outcome this fake decided synchronously.
type result struct {
	id  string
	err error
}

// Get returns the outcome.
func (r result) Get(context.Context) (string, error) { return r.id, r.err }

// heldPublication is a publication the gate holds: the [psapi.Result] of one
// the client library has not sent yet.
type heldPublication struct {
	pub *publisher
	// out is the caller's publication as the library keeps it: Data is the
	// caller's own bytes, not a copy.
	out psapi.Outgoing
	// err is PublishErr's answer, applied when the publication is sent.
	err error
	// ready is closed once id or fail holds the outcome.
	ready chan struct{}
	id    string
	fail  error
}

// Get waits for the publication to be sent, or for ctx.
func (h *heldPublication) Get(ctx context.Context) (string, error) {
	select {
	case <-h.ready:
		return h.id, h.fail
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// failed is a failed result.
func failed(err error) psapi.Result { return result{err: err} }

// receiver is one subscription's consumption handle.
type receiver struct {
	s    *Server
	name string
}

// Receive delivers the subscription's backlog by callback until ctx is done.
//
// Delivery is SEQUENTIAL and on one goroutine, which is what the driver asks
// the real library for (NumGoroutines = 1) and what makes a test deterministic.
// The callback blocking is the backpressure, exactly as it is in the library.
func (r *receiver) Receive(ctx context.Context, fn func(context.Context, *psapi.Incoming)) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		s := r.s
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil
		}
		if s.ReceiveErr != nil {
			if err := s.ReceiveErr(r.name); err != nil {
				s.mu.Unlock()
				return err
			}
		}
		sub, exists := s.subs[r.name]
		if !exists {
			s.mu.Unlock()
			return psapi.ErrNotFound
		}
		if len(sub.backlog) == 0 {
			ch := make(chan struct{})
			sub.waiters = append(sub.waiters, ch)
			s.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return nil
			}
			continue
		}
		rec := sub.backlog[0]
		sub.backlog = sub.backlog[1:]
		rec.attempt++
		rec.ackID = rec.id + "-" + strconv.Itoa(rec.attempt)
		sub.out[rec.ackID] = rec
		in := &psapi.Incoming{
			ID:          rec.id,
			Data:        clone(rec.data),
			Attributes:  cloneMap(rec.attrs),
			PublishTime: rec.created,
			OrderingKey: rec.order,
			Native:      rec,
			Settle:      settlement{s: s, sub: r.name, ackID: rec.ackID},
		}
		// The attempt count is reported only when the subscription has a
		// dead-letter policy, which is the service's rule and is what makes
		// DeliveryInfo.Attempt zero without one.
		if sub.spec.DeadLetterTopic != "" {
			in.Attempt, in.HasAttempt = rec.attempt, true
		}
		s.mu.Unlock()
		fn(ctx, in)
	}
}

// settlement settles one delivery.
type settlement struct {
	s     *Server
	sub   string
	ackID string
}

// Ack acknowledges without waiting for an outcome.
func (st settlement) Ack() { _ = st.settle(true) }

// Nack declines without waiting for an outcome.
func (st settlement) Nack() { _ = st.settle(false) }

// AckWithResult acknowledges and returns the outcome.
func (st settlement) AckWithResult(context.Context) error { return st.settle(true) }

// NackWithResult declines and returns the outcome.
func (st settlement) NackWithResult(context.Context) error { return st.settle(false) }

// settle applies one settlement.
//
// An acknowledgement identifier the subscription no longer holds is
// [psapi.ErrAckIDNotReachable] — the service's own answer for a settlement
// whose lease is gone — which is what the driver maps to ErrLeaseLost.
func (st settlement) settle(ack bool) error {
	s := st.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if inject := s.AckErr; ack && inject != nil {
		if err := inject(st.ackID); err != nil {
			return err
		}
	}
	if inject := s.NackErr; !ack && inject != nil {
		if err := inject(st.ackID); err != nil {
			return err
		}
	}
	sub, exists := s.subs[st.sub]
	if !exists {
		return psapi.ErrNotFound
	}
	rec, held := sub.out[st.ackID]
	if !held {
		return psapi.ErrAckIDNotReachable
	}
	delete(sub.out, st.ackID)
	if !ack {
		sub.backlog = append(sub.backlog, rec)
		s.wakeLocked(sub)
	}
	return nil
}

// fakeAdmin is the administration surface, over the server itself.
type fakeAdmin Server

// server is the fake this admin belongs to.
func (a *fakeAdmin) server() *Server { return (*Server)(a) }

// CreateTopic creates one topic.
func (a *fakeAdmin) CreateTopic(_ context.Context, name string) error {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("CreateTopic", name); err != nil {
		return err
	}
	if _, exists := s.topics[name]; exists {
		return psapi.ErrAlreadyExists
	}
	s.topicLocked(name)
	return nil
}

// CreateRetainedTopic creates one topic with message retention.
func (a *fakeAdmin) CreateRetainedTopic(_ context.Context, name string, retention time.Duration) error {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("CreateRetainedTopic", name); err != nil {
		return err
	}
	if _, exists := s.topics[name]; exists {
		return psapi.ErrAlreadyExists
	}
	s.topicLocked(name).retention = retention
	return nil
}

// TopicRetention reads a topic's message retention back.
func (a *fakeAdmin) TopicRetention(_ context.Context, name string) (time.Duration, error) {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("TopicRetention", name); err != nil {
		return 0, err
	}
	t, exists := s.topics[name]
	if !exists {
		return 0, psapi.ErrNotFound
	}
	return t.retention, nil
}

// DeleteTopic removes one topic, leaving its subscriptions in place as the
// service does.
func (a *fakeAdmin) DeleteTopic(_ context.Context, name string) error {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("DeleteTopic", name); err != nil {
		return err
	}
	if _, exists := s.topics[name]; !exists {
		return psapi.ErrNotFound
	}
	delete(s.topics, name)
	return nil
}

// TopicExists reports whether the topic is there.
func (a *fakeAdmin) TopicExists(_ context.Context, name string) (bool, error) {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("TopicExists", name); err != nil {
		return false, err
	}
	_, exists := s.topics[name]
	return exists, nil
}

// ListTopics iterates the project's topics.
func (a *fakeAdmin) ListTopics(_ context.Context, fn func(string) bool) error {
	s := a.server()
	s.mu.Lock()
	names := make([]string, 0, len(s.topics))
	for name := range s.topics {
		names = append(names, name)
	}
	err := s.injectLocked("ListTopics", "")
	s.mu.Unlock()
	if err != nil {
		return err
	}
	for _, name := range names {
		if !fn(name) {
			return nil
		}
	}
	return nil
}

// CreateSubscription creates one subscription.
func (a *fakeAdmin) CreateSubscription(_ context.Context, spec psapi.SubscriptionSpec) error {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("CreateSubscription", spec.Subscription); err != nil {
		return err
	}
	if _, exists := s.subs[spec.Subscription]; exists {
		return psapi.ErrAlreadyExists
	}
	if _, exists := s.topics[spec.Topic]; !exists {
		return psapi.ErrNotFound
	}
	s.subscriptionLocked(spec)
	return nil
}

// DeleteSubscription removes one subscription.
func (a *fakeAdmin) DeleteSubscription(_ context.Context, name string) error {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("DeleteSubscription", name); err != nil {
		return err
	}
	sub, exists := s.subs[name]
	if !exists {
		return psapi.ErrNotFound
	}
	delete(s.subs, name)
	delete(s.topicLocked(sub.spec.Topic).subs, name)
	s.wakeLocked(sub)
	return nil
}

// GetSubscription reads a subscription's configuration back.
func (a *fakeAdmin) GetSubscription(_ context.Context, name string) (psapi.SubscriptionInfo, error) {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("GetSubscription", name); err != nil {
		return psapi.SubscriptionInfo{}, err
	}
	sub, exists := s.subs[name]
	if !exists {
		return psapi.SubscriptionInfo{}, nil
	}
	spec := sub.spec
	return psapi.SubscriptionInfo{
		Exists:              true,
		Topic:               spec.Topic,
		AckDeadline:         spec.AckDeadline,
		Filter:              spec.Filter,
		ExactlyOnce:         spec.ExactlyOnce,
		Ordering:            spec.Ordering,
		RetainAcked:         spec.RetainAcked,
		Retention:           spec.Retention,
		DeadLetterTopic:     spec.DeadLetterTopic,
		MaxDeliveryAttempts: spec.MaxDeliveryAttempts,
	}, nil
}

// Seek moves a subscription's acknowledgement state.
//
// On a topic with message retention a seek backwards rebuilds the backlog from
// the topic's history, as the service does, which is what lets a subscription
// created after a publication read it. Anywhere else this fake keeps no
// history, so a seek backwards returns the outstanding messages to the backlog
// and a seek forwards discards them, which is the observable half of what the
// service does and is what the driver's own code path needs.
func (a *fakeAdmin) Seek(_ context.Context, name string, to time.Time) error {
	s := a.server()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injectLocked("Seek", name); err != nil {
		return err
	}
	sub, exists := s.subs[name]
	if !exists {
		return psapi.ErrNotFound
	}
	forward := to.After(time.Now())
	if t, ok := s.topics[sub.spec.Topic]; ok && t.retention > 0 && !forward {
		s.replayLocked(sub, t, to)
		return nil
	}
	for ackID, rec := range sub.out {
		delete(sub.out, ackID)
		if !forward {
			sub.backlog = append(sub.backlog, rec)
		}
	}
	if forward {
		sub.backlog = nil
	}
	s.wakeLocked(sub)
	return nil
}

// replayLocked makes a subscription's backlog what the topic retains from `to`
// on: every publication inside the topic's retention window and at or after
// `to` that the subscription's filter lets through. Everything before `to` is
// acknowledged by the seek and everything after it is not, so what the
// subscription had outstanding or queued is replaced rather than kept.
func (s *Server) replayLocked(sub *subscription, t *topic, to time.Time) {
	from := time.Now().Add(-t.retention)
	if to.After(from) {
		from = to
	}
	clear(sub.out)
	sub.backlog = nil
	for _, rec := range t.history {
		if rec.created.Before(from) {
			continue
		}
		if sub.spec.Filter != "" && !matches(sub.spec.Filter, rec.attrs) {
			continue
		}
		sub.backlog = append(sub.backlog, &record{
			id: rec.id, data: clone(rec.data), attrs: cloneMap(rec.attrs),
			order: rec.order, created: rec.created,
		})
	}
	s.wakeLocked(sub)
}

// Purge discards a subscription's backlog, which is a seek to now.
func (a *fakeAdmin) Purge(ctx context.Context, name string) error {
	return a.Seek(ctx, name, time.Now().Add(time.Second))
}

// injectLocked consults the administration injection point.
func (s *Server) injectLocked(op, name string) error {
	if s.AdminErr == nil {
		return nil
	}
	return s.AdminErr(op, name)
}

// matches is the fake's filter evaluation: `attributes.<key> = "<value>"` and
// nothing else.
//
// It is deliberately the smallest grammar that lets the driver's filter path
// be exercised. The real grammar is the service's and this fake does not
// pretend to implement it; a test that needs the real thing runs against the
// emulator.
func matches(filter string, attrs map[string]string) bool {
	const prefix = "attributes."
	if len(filter) <= len(prefix) || filter[:len(prefix)] != prefix {
		return false
	}
	rest := filter[len(prefix):]
	eq := -1
	for i := range len(rest) {
		if rest[i] == '=' {
			eq = i
			break
		}
	}
	if eq < 0 {
		return false
	}
	key := trimSpace(rest[:eq])
	want := trimQuotes(trimSpace(rest[eq+1:]))
	got, ok := attrs[key]
	return ok && got == want
}

// trimSpace removes leading and trailing spaces.
func trimSpace(s string) string {
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == ' ' {
		s = s[:len(s)-1]
	}
	return s
}

// trimQuotes removes one layer of double quotes.
func trimQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// clone copies a byte slice, preserving the nil-versus-empty distinction
// The specification requires.
func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// cloneMap copies an attribute map.
func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
