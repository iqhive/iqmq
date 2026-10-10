// Package snsfake is an in-process SNS server that fans out into an in-process
// SQS server: enough of the service for this driver's tests to make the
// BROKER behave the way a fan-out driver has to survive.
//
// It exists because the states this driver must get right are the service's
// to decide, and the most important of them cannot be asked for:
//
//   - a subscription that exists with RawMessageDelivery OFF. Every body a
//     consumer received would then be SNS's JSON notification wrapped around
//     the published body, so a driver that accepted it would silently change
//     every message. No real endpoint will produce that state on request —
//     you have to create a subscription wrongly on purpose — and this server
//     produces it in microseconds, in both shapes: the attribute absent, and
//     the attribute present and false.
//   - a publication reaching N queues, so that one subscriber's settlement
//     can be shown not to touch another's copy. Against a container that
//     needs three queues, three subscriptions and real polls.
//   - a filter policy that keeps a message out of one queue and not another,
//     which is the conditional claim.
//
// What it models, and only this:
//
//   - topics by name, with ARNs built from the configured region and account;
//   - subscriptions with the four attributes this driver reads and writes —
//     Protocol, Endpoint, RawMessageDelivery, FilterPolicy/FilterPolicyScope;
//   - Publish and PublishBatch, delivering into the SQS server's queues by
//     calling its own SendMessage, which is what raw delivery IS;
//   - the SNS JSON notification, for a subscription without raw delivery;
//   - a SUBSET of the filter-policy grammar — an exact-value list, `exists`
//     and `prefix`, over message attributes or over a JSON body;
//   - per-operation and per-entry faults, which is how a partial batch is
//     produced on purpose.
//
// It is NOT an SNS emulator. It speaks no HTTP, validates far less than the
// service does, applies no delivery retry or subscription dead-letter policy,
// and its filter grammar is a subset. Anything a test asserts about the
// service's own behaviour belongs against LocalStack — which
// `conformance/suites/snssqs_test.go` runs the same harness against, so a
// claim the two disagree about is a test failure rather than a surprise in
// production.
package snsfake

import (
	b64 "encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// Options configures a [Server].
type Options struct {
	// Region is the region the topic and subscription ARNs are built with.
	Region string
	// AccountID is the account those ARNs are built with.
	AccountID string
	// Queues is the SQS server this one delivers into. It is required: a
	// fan-out with nowhere to fan out to models nothing.
	Queues *sqsfake.Server
	// RawByDefault makes a Subscribe that names no RawMessageDelivery
	// attribute behave as though it had asked for raw delivery.
	//
	// It is FALSE by default, which is the service's behaviour and the one
	// that matters: raw delivery is opt-in at SNS, so a subscription made
	// without it wraps every body. A test that wants the wrong state asks
	// for a subscription without the attribute; a test that wants to prove
	// this server is not simply refusing everything sets this.
	RawByDefault bool
}

// Server is the in-process server. Every method is safe for concurrent use.
type Server struct {
	opts Options

	mu     sync.Mutex
	topics map[string]*topic
	names  []string
	subs   map[string]*subscription
	seq    uint64
	faults []*Fault
	calls  map[string]int
	// dropped counts publications a subscription's filter policy excluded,
	// which is the number a filter test asserts on: a filter that excluded
	// nothing is indistinguishable from no filter at all.
	dropped int
	// undeliverable counts deliveries whose endpoint queue does not exist.
	// The real service retries and then dead-letters; this server counts,
	// so that a test can tell "the message went nowhere" from "the message
	// went somewhere else".
	undeliverable int
}

// topic is one SNS topic.
type topic struct {
	name  string
	attrs map[string]string
}

// subscription is one subscription on a topic.
type subscription struct {
	arn      string
	topicARN string
	protocol string
	endpoint string
	attrs    map[string]string
}

// New returns a server with no topics.
func New(opts Options) *Server {
	if opts.Region == "" {
		opts.Region = "fake-region-1"
	}
	if opts.AccountID == "" {
		opts.AccountID = "000000000000"
	}
	return &Server{
		opts:   opts,
		topics: map[string]*topic{},
		subs:   map[string]*subscription{},
		calls:  map[string]int{},
	}
}

// Calls is how many times an operation was invoked, which is what a test that
// cares about round trips asserts on.
func (s *Server) Calls(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

// Dropped is how many deliveries a filter policy excluded.
func (s *Server) Dropped() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

// Undeliverable is how many deliveries had nowhere to go because the endpoint
// queue does not exist.
func (s *Server) Undeliverable() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.undeliverable
}

// TopicARN is the ARN of a topic that exists, and "" for one that does not.
func (s *Server) TopicARN(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[name]; !ok {
		return ""
	}
	return s.topicARN(name)
}

// Subscriptions is how many subscriptions a topic has.
func (s *Server) Subscriptions(topicARN string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sub := range s.subs {
		if sub.topicARN == topicARN {
			n++
		}
	}
	return n
}

// SubscriptionFor is the subscription ARN delivering topicARN into queueARN,
// and "" for none.
func (s *Server) SubscriptionFor(topicARN, queueARN string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.subs {
		if sub.topicARN == topicARN && sub.endpoint == queueARN {
			return sub.arn
		}
	}
	return ""
}

// SetRaw turns a subscription's RawMessageDelivery on or off directly, which
// is how a test reaches the state no API call would produce on purpose.
func (s *Server) SetRaw(subARN string, raw bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub, ok := s.subs[subARN]; ok {
		sub.attrs[attrRaw] = strconv.FormatBool(raw)
	}
}

// UnsetRaw removes a subscription's RawMessageDelivery attribute altogether,
// which is the OTHER wrong state: the service's own default, and the one a
// subscription made through the console without ticking the box is in.
func (s *Server) UnsetRaw(subARN string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub, ok := s.subs[subARN]; ok {
		delete(sub.attrs, attrRaw)
	}
}

// The subscription attributes this server models.
const (
	attrRaw         = "RawMessageDelivery"
	attrFilter      = "FilterPolicy"
	attrFilterScope = "FilterPolicyScope"
	attrProtocol    = "Protocol"
	attrEndpoint    = "Endpoint"
	attrTopicARN    = "TopicArn"
	attrSubARN      = "SubscriptionArn"
)

// topicARN builds a topic's ARN. It is called with the lock held.
func (s *Server) topicARN(name string) string {
	return "arn:aws:sns:" + s.opts.Region + ":" + s.opts.AccountID + ":" + name
}

// nameOfARN is the last colon-separated element of an ARN.
func nameOfARN(arn string) string {
	if i := strings.LastIndexByte(arn, ':'); i >= 0 {
		return arn[i+1:]
	}
	return arn
}

// topicNameOfARN is the topic an ARN names, whether it is a topic ARN or a
// subscription ARN (which is a topic ARN with one more element).
func topicNameOfARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return arn
	}
	return parts[5]
}

// Fault is one installed failure.
type Fault struct {
	// Op is the operation name it applies to: "Publish", "Subscribe", and so
	// on. Empty applies to every operation.
	Op string
	// Topic is the topic NAME it applies to. Empty applies to every topic.
	Topic string
	// Err is the error the operation returns. Nil with Entries or
	// EntryIndexes set makes the whole call succeed with per-entry failures.
	Err error
	// Entries names the batch entry Ids that fail, mapping each to the error
	// code the result entry carries. An empty code is "InternalError".
	//
	// This is how a partial batch is produced on purpose, and it is the one
	// thing no real endpoint will do on request.
	Entries map[string]string
	// EntryIndexes names the batch entry POSITIONS that fail, for a test
	// that does not know the driver's entry ids. It is applied after
	// Entries.
	EntryIndexes map[int]string
	// SenderFault is what the failed entries report, which decides whether
	// the driver reads them as retryable.
	SenderFault bool
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

// enter records the call and returns the fault that applies to it, if any. It
// is called with the lock held.
func (s *Server) enter(op, topicName string) *Fault {
	s.calls[op]++
	for i, f := range s.faults {
		if f.Op != "" && f.Op != op {
			continue
		}
		if f.Topic != "" && f.Topic != topicName {
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

// notFound is the service's answer for an ARN that names nothing.
func notFound(what string) error {
	return &snstypes.NotFoundException{Message: aws.String(what)}
}

// invalid is the service's answer for a parameter it refuses.
func invalid(what string) error {
	return &snstypes.InvalidParameterException{Message: aws.String(what)}
}

// deliver fans one publication out to every subscription of a topic. It is
// called with the lock held.
//
// This is the whole of what the fake exists to model, and the raw/non-raw
// branch is the line that matters: with raw delivery the queue's message body
// IS the published body and the SNS attributes ARE the SQS attributes, and
// without it the body is a JSON notification with the payload inside it and no
// attributes at all. A driver that consumed the second shape would hand every
// application a body that is not the one that was published.
// It returns the deliveries rather than performing them, so that the SQS
// server is called with this server's lock RELEASED. The two servers take two
// locks and nothing orders them; a delivery made under both would deadlock the
// first test that published from one goroutine while reading the queue from
// another, and that is the kind of defect a fake introduces into tests it was
// written to make possible.
func (s *Server) deliver(topicARN, body string, attrs map[string]snstypes.MessageAttributeValue, messageID string) []sqsInput {
	var out []sqsInput
	for _, sub := range s.subs {
		if sub.topicARN != topicARN || !strings.EqualFold(sub.protocol, "sqs") {
			continue
		}
		if !s.matches(sub, body, attrs) {
			s.dropped++
			continue
		}
		name := nameOfARN(sub.endpoint)
		url := s.opts.Queues.QueueURL(name)
		if url == "" {
			s.undeliverable++
			continue
		}
		in := sqsInput{QueueURL: url}
		if s.raw(sub) {
			in.Body = body
			in.Attrs = rawAttributes(attrs)
		} else {
			in.Body = notification(topicARN, messageID, body, attrs)
		}
		out = append(out, in)
	}
	return out
}

// raw reports whether a subscription delivers the published body unchanged.
//
// An ABSENT attribute is not raw, which is the service's own default and the
// state a subscription made without ticking the box is in.
// [Options.RawByDefault] flips it, for a test that wants the fake to behave
// the way a correctly configured deployment does without saying so every time.
func (s *Server) raw(sub *subscription) bool {
	v, ok := sub.attrs[attrRaw]
	if !ok {
		return s.opts.RawByDefault
	}
	return strings.EqualFold(v, "true")
}

// sqsInput is one delivery into a queue.
type sqsInput struct {
	QueueURL string
	Body     string
	Attrs    map[string]sqstypes.MessageAttributeValue
}

// notification is the JSON envelope SNS delivers when raw message delivery is
// off, in the shape the service documents.
//
// It is why a non-raw subscription is refused rather than decoded: the
// published body is a STRING FIELD inside this object, so a consumer that read
// the SQS body would read this, and the message's headers, key and identifier
// would all be inside MessageAttributes rather than on the message.
func notification(topicARN, messageID, body string, attrs map[string]snstypes.MessageAttributeValue) string {
	type attr struct {
		Type  string `json:"Type"`
		Value string `json:"Value"`
	}
	out := map[string]any{
		"Type":      "Notification",
		"MessageId": messageID,
		"TopicArn":  topicARN,
		"Message":   body,
	}
	if len(attrs) > 0 {
		ma := make(map[string]attr, len(attrs))
		for k, v := range attrs {
			a := attr{Type: aws.ToString(v.DataType)}
			switch {
			case len(v.BinaryValue) > 0:
				a.Value = b64.StdEncoding.EncodeToString(v.BinaryValue)
			default:
				a.Value = aws.ToString(v.StringValue)
			}
			ma[k] = a
		}
		out["MessageAttributes"] = ma
	}
	buf, err := json.Marshal(out)
	if err != nil {
		// Unreachable: every value above is a string or a map of strings.
		return `{"Type":"Notification","Message":""}`
	}
	return string(buf)
}

// rawAttributes is what raw message delivery does to the attribute map: the
// SNS attributes become the SQS attributes, field for field.
//
// It is exported through [RawAttributes] so that a test can model the delivery
// without a server.
func rawAttributes(in map[string]snstypes.MessageAttributeValue) map[string]sqstypes.MessageAttributeValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]sqstypes.MessageAttributeValue, len(in))
	for k, v := range in {
		out[k] = sqstypes.MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}
	return out
}

// RawAttributes is [rawAttributes] for a test that models raw delivery without
// running a server.
func RawAttributes(in map[string]snstypes.MessageAttributeValue) map[string]sqstypes.MessageAttributeValue {
	return rawAttributes(in)
}

// Notification is [notification] for a test that needs the exact shape a
// non-raw subscription delivers.
func Notification(topicARN, messageID, body string, attrs map[string]snstypes.MessageAttributeValue) string {
	return notification(topicARN, messageID, body, attrs)
}

// matches applies a subscription's filter policy. It is called with the lock
// held.
//
// The grammar is a SUBSET of the service's: a list of exact string values, an
// `exists` test and a `prefix` test, over message attributes or over a JSON
// body. That is enough to prove a filter keeps a message out of one queue and
// not another, which is what the conditional claim needs; anything finer is
// the service's own behaviour and belongs against LocalStack.
func (s *Server) matches(sub *subscription, body string, attrs map[string]snstypes.MessageAttributeValue) bool {
	policy := sub.attrs[attrFilter]
	if policy == "" {
		return true
	}
	var rules map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policy), &rules); err != nil {
		// The service would have refused the policy at Subscribe. A policy
		// that got this far and does not parse matches nothing, which is
		// the conservative direction: a test would see a message that never
		// arrived rather than one that arrived despite a filter.
		return false
	}
	values := map[string]string{}
	present := map[string]bool{}
	if strings.EqualFold(sub.attrs[attrFilterScope], "MessageBody") {
		var obj map[string]any
		if err := json.Unmarshal([]byte(body), &obj); err != nil {
			return false
		}
		for k, v := range obj {
			present[k] = true
			if s, ok := v.(string); ok {
				values[k] = s
			}
		}
	} else {
		for k, v := range attrs {
			present[k] = true
			if len(v.BinaryValue) == 0 {
				values[k] = aws.ToString(v.StringValue)
			}
		}
	}
	for name, raw := range rules {
		if !matchOne(raw, values[name], present[name]) {
			return false
		}
	}
	return true
}

// matchOne applies one attribute's list of matchers.
func matchOne(raw json.RawMessage, value string, present bool) bool {
	var list []json.RawMessage
	if err := json.Unmarshal(raw, &list); err != nil {
		return false
	}
	for _, item := range list {
		var str string
		if err := json.Unmarshal(item, &str); err == nil {
			if present && value == str {
				return true
			}
			continue
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(item, &obj); err != nil {
			continue
		}
		if v, ok := obj["exists"]; ok {
			var want bool
			if err := json.Unmarshal(v, &want); err == nil && want == present {
				return true
			}
		}
		if v, ok := obj["prefix"]; ok {
			var want string
			if err := json.Unmarshal(v, &want); err == nil && present && strings.HasPrefix(value, want) {
				return true
			}
		}
	}
	return false
}
