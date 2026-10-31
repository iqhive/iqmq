// Package sqsfake is an in-process SQS server for tests that need to choose
// what the broker decides.
//
// It exists because every one of those scenarios is the BROKER's decision. A
// visibility window that has already passed, a delete batch in which three
// entries of ten fail, a receipt handle another consumer has superseded, and a
// FIFO group that holds its second message back until the first is settled are
// not states a driver can reach through the portable API against a real
// endpoint — and ElasticMQ can be made to produce three of them only by
// sleeping for real seconds. This server produces all four in microseconds,
// against an injected clock, and the same tests then run against ElasticMQ to
// prove the fake did not lie.
//
// What it models, and only this:
//
//   - queues by name, with the attributes this driver reads and writes;
//   - per-message visibility windows on an injected clock, so
//     [Server.Advance] expires a lease rather than a sleep;
//   - a fresh receipt handle per receive, with every earlier handle for the
//     message remembered, so a stale handle is a state the tests can reach;
//   - ApproximateReceiveCount, which is this driver's attempt count;
//   - FIFO queues: one in-flight message per group, ordered within a group
//     and unordered across groups, with MessageDeduplicationId honoured;
//   - the redrive policy: a message received more than maxReceiveCount times
//     moves to the dead-letter queue;
//   - the size limit: a message, its body plus every message attribute's
//     name, data type and value, is at most the queue's MaximumMessageSize
//     (1 MiB unless the queue sets less), and a SendMessageBatch at most
//     1 MiB in all;
//   - per-operation and per-entry faults, which is how a partial batch is
//     produced on purpose.
//
// It is NOT an SQS emulator. It speaks no HTTP, validates far less than the
// service does, and its numbers mean nothing. Anything a test asserts about
// the service's own behaviour belongs against ElasticMQ or LocalStack.
//
// # Why this is not an internal package
package sqsfake

import (
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Options configures a [Server].
type Options struct {
	// Region is the region the queue URLs and ARNs are built with.
	Region string
	// AccountID is the account the queue URLs and ARNs are built with.
	AccountID string
	// Start is the fake clock's initial time. The zero value is a fixed,
	// arbitrary instant: nothing here reads the wall clock, so a test that
	// does not call Advance sees time stand still.
	Start time.Time
	// MaxWait bounds how long a long poll really waits, whatever
	// WaitTimeSeconds asks for. Zero means DefaultMaxWait. It is what keeps
	// a driver's poller from either spinning or parking for twenty seconds
	// in a unit test.
	MaxWait time.Duration
	// StaleHandleDeletes says what a delete carrying a superseded receipt
	// handle does. True — the default, and the pessimistic reading of the
	// service — deletes the message anyway, which is exactly why this
	// driver refuses a stale settlement locally BEFORE the API call. False
	// answers ReceiptHandleIsInvalid, which is what ElasticMQ does.
	StaleHandleDeletes bool
}

// DefaultMaxWait is how long a long poll really waits when Options.MaxWait is
// zero.
const DefaultMaxWait = 25 * time.Millisecond

// maxMessageBytes is the service's message size limit: the largest
// MaximumMessageSize a queue takes, the one a queue has when it sets none, and
// the most one SendMessageBatch may carry in all.
const maxMessageBytes = 1 << 20

// defaultStart is the fake clock's zero: a fixed instant, so that a test that
// never advances the clock sees no time pass at all.
var defaultStart = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// Server is the in-process server. Every method is safe for concurrent use.
type Server struct {
	opts Options

	mu      sync.Mutex
	now     time.Time
	queues  map[string]*queue
	names   []string
	seq     uint64
	faults  []*Fault
	arrived chan struct{}
	calls   map[string]int
}

// queue is one queue.
//
// A FIFO group's hold is NOT a field here. It is derived from the messages the
// queue holds — see Server.groupHeld — because a counter is a second copy of a
// fact the deadlines and the clock already decide, and the two disagreed the
// moment the clock moved.
type queue struct {
	name  string
	attrs map[string]string
	msgs  []*message
}

// message is one message in a queue.
type message struct {
	id        string
	body      string
	attrs     map[string]types.MessageAttributeValue
	group     string
	dedup     string
	seqNum    uint64
	sentAt    time.Time
	visibleAt time.Time
	received  int32
	firstRecv time.Time
	handle    string
	stale     map[string]struct{}
}

// New returns a server with no queues.
func New(opts Options) *Server {
	if opts.Region == "" {
		opts.Region = "fake-region-1"
	}
	if opts.AccountID == "" {
		opts.AccountID = "000000000000"
	}
	if opts.MaxWait == 0 {
		opts.MaxWait = DefaultMaxWait
	}
	start := opts.Start
	if start.IsZero() {
		start = defaultStart
	}
	return &Server{
		opts:   opts,
		now:    start,
		queues: map[string]*queue{},
		calls:  map[string]int{},
	}
}

// Now is the fake clock's current time. It is what a driver under test should
// read too, so that a lease this server considers expired is one the driver
// considers expired.
func (s *Server) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

// Advance moves the fake clock forward, which is how a visibility window
// expires without a sleep. It wakes every parked long poll, because a message
// whose window has just passed is available now — and on a FIFO queue that
// includes the message's GROUP, which is released by the same move: a group is
// held by a message in flight, and a message whose window has passed is not in
// flight.
func (s *Server) Advance(d time.Duration) {
	s.mu.Lock()
	s.now = s.now.Add(d)
	s.wake()
	s.mu.Unlock()
}

// Calls is how many times an operation was invoked, which is the round-trip
// count the specification is measured on.
func (s *Server) Calls(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

// QueueURL is the URL of a queue that exists, and "" for one that does not.
func (s *Server) QueueURL(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.queues[name]; !ok {
		return ""
	}
	return s.url(name)
}

// Depth is how many messages a queue holds, visible or not. It is the
// assertion a settlement test makes: a message that was deleted is gone, and
// one whose delete failed is still there.
func (s *Server) Depth(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	if !ok {
		return -1
	}
	return len(q.msgs)
}

// Bodies is every message a queue holds, in queue order, whether visible or
// not.
func (s *Server) Bodies(name string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(q.msgs))
	for _, m := range q.msgs {
		out = append(out, m.body)
	}
	return out
}

// Invisible is how many of a queue's messages are currently invisible, which
// is how a test knows a poller has taken them.
func (s *Server) Invisible(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	if !ok {
		return -1
	}
	n := 0
	for _, m := range q.msgs {
		if m.visibleAt.After(s.now) {
			n++
		}
	}
	return n
}

// ExpireVisibility makes every invisible message in a queue visible again,
// which is a lease expiry without moving the clock. It returns how many it
// released.
//
// [Server.Advance] is the other way to the same state and the two now agree:
// this method exists for a test that must expire a window WITHOUT moving the
// clock, and not — as it was for one increment — because moving the clock left
// a FIFO group stranded.
func (s *Server) ExpireVisibility(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	if !ok {
		return 0
	}
	n := 0
	for _, m := range q.msgs {
		if m.visibleAt.After(s.now) {
			m.visibleAt = s.now
			n++
		}
	}
	s.wake()
	return n
}

// Deliverable is how many messages the next unbounded receive could produce:
// visible, and on a FIFO queue in a group nothing holds.
//
// It is the observable half of the FIFO rule, and it is what a test asserts on
// when the thing it is checking is an ABSENCE. A receive that produces nothing
// says nothing about why, so "the queue holds messages and none of them can be
// delivered" was a state nothing could see and one this server could reach.
// Counting is not a receive: nothing is taken, no handle
// is minted and no window moves.
func (s *Server) Deliverable(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	if !ok {
		return -1
	}
	fifo := q.attrs[string(types.QueueAttributeNameFifoQueue)] == "true"
	// taken is the receive's own rule as well: one message per group per
	// call, so a group holding three visible messages contributes one.
	taken, n := map[string]bool{}, 0
	for _, m := range q.msgs {
		if s.deliverable(q, m, fifo, taken) {
			taken[m.group] = true
			n++
		}
	}
	return n
}

// url is the URL of a queue name.
func (s *Server) url(name string) string {
	return "http://sqs." + s.opts.Region + ".fake/" + s.opts.AccountID + "/" + name
}

// arn is the ARN of a queue name.
func (s *Server) arn(name string) string {
	return "arn:aws:sqs:" + s.opts.Region + ":" + s.opts.AccountID + ":" + name
}

// nameOf recovers a queue name from a URL or an ARN, so that the server
// accepts whatever the driver happens to hold.
func nameOf(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// wake releases every parked long poll. It is called with the lock held.
func (s *Server) wake() {
	if s.arrived != nil {
		close(s.arrived)
		s.arrived = nil
	}
}

// listen returns the channel a parked long poll waits on. It is called with
// the lock held.
func (s *Server) listen() chan struct{} {
	if s.arrived == nil {
		s.arrived = make(chan struct{})
	}
	return s.arrived
}

// Fault is one installed failure.
type Fault struct {
	// Op is the operation name it applies to: "SendMessage",
	// "DeleteMessageBatch", and so on. Empty applies to every operation.
	Op string
	// Queue is the queue name it applies to. Empty applies to every queue.
	Queue string
	// Err is the error the operation returns. Nil with Entries set makes
	// the whole call succeed with per-entry failures.
	Err error
	// Entries names the batch entry Ids that fail, mapping each to the
	// error code the result entry carries. An empty code is
	// "InternalError".
	//
	// This is how a partial batch is produced on purpose, and it is the one
	// thing no real endpoint will do on request.
	Entries map[string]string
	// EntryIndexes names the batch entry POSITIONS that fail, for a test
	// that does not know the driver's entry ids. It is applied after
	// Entries.
	//
	// A position is only usable when the test controls the ORDER of the
	// entries, which it does not when several goroutines settle
	// concurrently: the driver queues them in whatever order they arrive.
	// Bodies is the selector for that case.
	EntryIndexes map[int]string
	// Bodies names the failing entries by the BODY of the message they
	// settle, mapping each to the error code the result entry carries.
	//
	// It is the selector a settlement test wants, and it exists because
	// nothing else correlates: the entry identifier is the driver's, the
	// position is the order the settlements happened to be queued in, and
	// the receipt handle is minted by this server. The body is the only
	// thing the TEST chose.
	Bodies map[string]string
	// Count is how many times the fault fires. A negative count fires for
	// ever; zero is read as one.
	Count int

	fired int
}

// Fault installs a failure and returns it, so that a test can remove it.
func (s *Server) Fault(f Fault) *Fault {
	if f.Count == 0 {
		f.Count = 1
	}
	p := &f
	s.mu.Lock()
	s.faults = append(s.faults, p)
	s.mu.Unlock()
	return p
}

// Remove uninstalls the fault. It is safe to call twice.
func (s *Server) Remove(f *Fault) {
	if f == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, p := range s.faults {
		if p == f {
			s.faults = append(s.faults[:i], s.faults[i+1:]...)
			return
		}
	}
}

// match finds the first fault that applies, consuming one of its firings. It
// is called with the lock held.
func (s *Server) match(op, queue string) *Fault {
	for i, f := range s.faults {
		if f.Op != "" && f.Op != op {
			continue
		}
		if f.Queue != "" && f.Queue != queue {
			continue
		}
		f.fired++
		if f.Count > 0 && f.fired >= f.Count {
			s.faults = append(s.faults[:i], s.faults[i+1:]...)
		}
		return f
	}
	return nil
}

// enter records the call and returns the fault that applies to it, if any. It
// is called with the lock held.
func (s *Server) enter(op, queue string) *Fault {
	s.calls[op]++
	return s.match(op, queue)
}
