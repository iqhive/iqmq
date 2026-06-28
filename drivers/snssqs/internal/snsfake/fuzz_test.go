package snsfake

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// FuzzFakeTopic holds this server to the rules `drivers/snssqs`'s tests
// believe it follows.
//
//   - FAN-OUT CONSERVATION: a publication reaches every subscribed queue whose
//     filter matched it, and no other queue, exactly once. A publication that
//     reached one queue twice, or a queue it was filtered out of, would make
//     the per-queue settlement test pass for the wrong reason.
//   - RAW versus NON-RAW: a raw subscription's queue holds the published body
//     UNCHANGED and the published attributes; a non-raw one holds SNS's JSON
//     notification with the body inside it and no attributes at all. That
//     difference is the whole reason the driver refuses a non-raw
//     subscription, so a fake that blurred it would make the refusal
//     untestable.
//   - SUBSCRIPTION IDENTITY: a subscription is its protocol and its endpoint,
//     so a second Subscribe for the same pair returns the SAME ARN. The
//     driver's `findSubscription` matches on the endpoint for that reason, and
//     a fake that minted a second subscription would hide a driver that
//     created duplicates.
//   - ISOLATION: nothing done to one subscription changes another's queue.
func FuzzFakeTopic(f *testing.F) {
	for _, seed := range topicSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		queues := sqsfake.New(sqsfake.Options{})
		s := New(Options{Queues: queues, RawByDefault: r.Bool()})
		ctx := context.Background()
		out, err := s.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("t")})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		st := &topicRun{
			sns: s, queues: queues, ctx: ctx,
			arn: aws.ToString(out.TopicArn),
			raw: map[string]bool{}, filter: map[string]string{},
			subARN: map[string]string{}, want: map[string][]string{},
		}
		for _, q := range st.names() {
			if _, err := queues.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(q)}); err != nil {
				t.Fatalf("CreateQueue(%s): %v", q, err)
			}
		}
		topicScript.Run(t, data[len(data)-r.Remaining():], st)
	})
}

// topicRun is the state one script drives, and the model it is checked
// against.
type topicRun struct {
	sns    *Server
	queues *sqsfake.Server
	ctx    context.Context
	arn    string

	// raw, filter and subARN are the subscriptions, keyed by queue name. A
	// queue with no entry in subARN is not subscribed.
	raw    map[string]bool
	filter map[string]string
	subARN map[string]string

	// want is what each queue should hold, in order: the PUBLISHED body for
	// a raw subscription and the empty string for a non-raw one, whose body
	// is a notification the model does not reproduce (the driver's own fuzz
	// target checks its shape).
	want map[string][]string
	// rawAt records, per queue and position, whether the delivery was made
	// raw, so the check can tell which shape to expect.
	rawAt map[string][]bool

	sent int
}

// names is the fixed set of queues, which is three because the interesting
// states need two subscriptions plus one queue that is not subscribed.
func (s *topicRun) names() []string { return []string{"q0", "q1", "q2"} }

// queueOf picks one of them from the input.
func (s *topicRun) queueOf(r *fuzzutil.Reader) string { return s.names()[r.Choice(3)] }

// subscribe subscribes a queue, with or without raw delivery and with or
// without a filter policy.
func (s *topicRun) subscribe(t testing.TB, r *fuzzutil.Reader) {
	q := s.queueOf(r)
	attrs := map[string]string{}
	// One byte is consumed and discarded, so that the three attribute states
	// below stay at the same offset in an input as the script grows. The
	// seeds write it too.
	_ = r.Bool()
	var wantRaw bool
	switch r.Choice(3) {
	case 0:
		// The attribute absent, which is the service's own default.
		wantRaw = s.sns.opts.RawByDefault
	case 1:
		attrs[attrRaw] = "true"
		wantRaw = true
	default:
		attrs[attrRaw] = "false"
		wantRaw = false
	}
	filter := ""
	if r.Bool() {
		filter = []string{"eu", "us"}[r.Choice(2)]
		attrs[attrFilter] = `{"region":["` + filter + `"]}`
	}
	endpoint := "arn:aws:sqs:fake-region-1:000000000000:" + q
	out, err := s.sns.Subscribe(s.ctx, &sns.SubscribeInput{
		TopicArn: aws.String(s.arn), Protocol: aws.String("sqs"),
		Endpoint: aws.String(endpoint), Attributes: attrs,
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		t.Fatalf("Subscribe(%s): %v", q, err)
	}
	got := aws.ToString(out.SubscriptionArn)
	if prev, ok := s.subARN[q]; ok {
		// SUBSCRIPTION IDENTITY: the same protocol and endpoint is the same
		// subscription, so the attributes of the first one stand.
		if got != prev {
			t.Fatalf("a second Subscribe for %s returned %q, want the existing %q: a subscription IS its protocol and endpoint", q, got, prev)
		}
		return
	}
	s.subARN[q] = got
	s.raw[q] = wantRaw
	s.filter[q] = filter
}

// unsubscribe removes a subscription, which must stop deliveries and leave the
// queue's contents alone.
func (s *topicRun) unsubscribe(t testing.TB, r *fuzzutil.Reader) {
	q := s.queueOf(r)
	arn, ok := s.subARN[q]
	if !ok {
		return
	}
	if _, err := s.sns.Unsubscribe(s.ctx, &sns.UnsubscribeInput{SubscriptionArn: aws.String(arn)}); err != nil {
		t.Fatalf("Unsubscribe(%s): %v", q, err)
	}
	delete(s.subARN, q)
	delete(s.raw, q)
	delete(s.filter, q)
}

// setRaw flips a subscription's raw delivery, which is the state no real
// endpoint will produce on request.
func (s *topicRun) setRaw(r *fuzzutil.Reader) {
	q := s.queueOf(r)
	arn, ok := s.subARN[q]
	if !ok {
		return
	}
	if r.Bool() {
		s.sns.UnsetRaw(arn)
		s.raw[q] = s.sns.opts.RawByDefault
		return
	}
	on := r.Bool()
	s.sns.SetRaw(arn, on)
	s.raw[q] = on
}

// publish publishes one message and records what each queue should now hold.
func (s *topicRun) publish(t testing.TB, r *fuzzutil.Reader) {
	body, region := s.next(r)
	if _, err := s.sns.Publish(s.ctx, &sns.PublishInput{
		TopicArn: aws.String(s.arn),
		Message:  aws.String(body),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"region": {DataType: aws.String("String"), StringValue: aws.String(region)},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	s.expect(body, region)
}

// publishBatch publishes two messages in one call.
func (s *topicRun) publishBatch(t testing.TB, r *fuzzutil.Reader) {
	first, firstRegion := s.next(r)
	second, secondRegion := s.next(r)
	if _, err := s.sns.PublishBatch(s.ctx, &sns.PublishBatchInput{
		TopicArn: aws.String(s.arn),
		PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{
			{Id: aws.String("0"), Message: aws.String(first), MessageAttributes: regionAttr(firstRegion)},
			{Id: aws.String("1"), Message: aws.String(second), MessageAttributes: regionAttr(secondRegion)},
		},
	}); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	s.expect(first, firstRegion)
	s.expect(second, secondRegion)
}

// regionAttr is the one attribute the filter policies in this script match on.
func regionAttr(region string) map[string]snstypes.MessageAttributeValue {
	return map[string]snstypes.MessageAttributeValue{
		"region": {DataType: aws.String("String"), StringValue: aws.String(region)},
	}
}

// next is the next body and its region.
func (s *topicRun) next(r *fuzzutil.Reader) (string, string) {
	s.sent++
	return "b" + itoa(s.sent), []string{"eu", "us"}[r.Choice(2)]
}

// expect records the deliveries one publication should have produced.
func (s *topicRun) expect(body, region string) {
	if s.rawAt == nil {
		s.rawAt = map[string][]bool{}
	}
	for q := range s.subARN {
		if f := s.filter[q]; f != "" && f != region {
			continue
		}
		s.want[q] = append(s.want[q], body)
		s.rawAt[q] = append(s.rawAt[q], s.raw[q])
	}
}

// check is the invariant set.
func (s *topicRun) check(t testing.TB) {
	for _, q := range s.names() {
		bodies := s.queues.Bodies(q)
		want := s.want[q]
		if len(bodies) != len(want) {
			t.Fatalf("queue %s holds %d message(s) %v, want %d %v: a publication reached a queue it was filtered out of, or reached one twice, or reached none",
				q, len(bodies), bodies, len(want), want)
		}
		for i, got := range bodies {
			raw := s.rawAt[q][i]
			switch {
			case raw && got != want[i]:
				t.Fatalf("queue %s message %d is %q, want the published %q: raw delivery means the queue's body IS the published body",
					q, i, got, want[i])
			case !raw && got == want[i]:
				t.Fatalf("queue %s message %d is the published body %q on a subscription WITHOUT raw delivery; it should be SNS's notification with the body inside it",
					q, i, got)
			case !raw && !strings.Contains(got, `"Message":"`+want[i]+`"`):
				t.Fatalf("queue %s message %d is %q, which is not a notification carrying %q", q, i, got, want[i])
			case !raw && !strings.Contains(got, `"Type":"Notification"`):
				t.Fatalf("queue %s message %d is %q, which is not an SNS notification at all", q, i, got)
			}
		}
	}
}

// topicScript is the op script.
var topicScript = fuzzutil.Script[*topicRun]{
	MaxSteps: 96,
	Steps: []fuzzutil.Step[*topicRun]{
		{Name: "subscribe", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, s *topicRun) { s.subscribe(t, r) }},
		{Name: "publish", Weight: 4, Run: func(t testing.TB, r *fuzzutil.Reader, s *topicRun) { s.publish(t, r) }},
		{Name: "batch", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *topicRun) { s.publishBatch(t, r) }},
		{Name: "unsubscribe", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *topicRun) { s.unsubscribe(t, r) }},
		{Name: "setraw", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, s *topicRun) { s.setRaw(r) }},
	},
	Check: func(t testing.TB, s *topicRun) { s.check(t) },
}

// topicSeeds are the committed inputs, and each of them exists because it is
// the one that catches a specific way this server could lie. They were each
// watched failing under the corresponding mutation before being committed: a
// seed corpus that cannot fail is the same defect as a test that cannot.
//
//  0. two queues subscribed WITH raw delivery, then a publication and a batch:
//     catches a fan-out that reaches the wrong number of queues, and a server
//     that wraps a raw delivery.
//  1. the same without raw delivery: catches a server that delivers the
//     published body where SNS would deliver its notification — the state the
//     driver's whole refusal is about.
//  2. one filtered subscription and one unfiltered one, with a publication
//     that matches and one that does not: catches a filter that excludes
//     nothing (which is what no policy already means) and one that excludes
//     everything.
func topicSeeds() [][]byte {
	out := make([][]byte, 0, 3)
	for _, raw := range []bool{true, false} {
		w := fuzzutil.NewWriter()
		w.Bool(false) // RawByDefault off: raw delivery is opt-in at the service
		for i := range 2 {
			topicScript.WriteStep(w, "subscribe")
			w.Choice(3, i) // the queue
			w.Bool(raw)    // the discarded byte
			w.Choice(3, map[bool]int{true: 1, false: 2}[raw])
			w.Bool(false) // no filter policy
		}
		topicScript.WriteStep(w, "publish")
		w.Choice(2, 0)
		topicScript.WriteStep(w, "batch")
		w.Choice(2, 0)
		w.Choice(2, 1)
		out = append(out, w.Data())
	}

	w := fuzzutil.NewWriter()
	w.Bool(false)
	// q0: raw, filtered on "eu".
	topicScript.WriteStep(w, "subscribe")
	w.Choice(3, 0)
	w.Bool(true)
	w.Choice(3, 1)
	w.Bool(true)
	w.Choice(2, 0)
	// q1: raw, unfiltered, so the exclusion below is the filter's doing.
	topicScript.WriteStep(w, "subscribe")
	w.Choice(3, 1)
	w.Bool(true)
	w.Choice(3, 1)
	w.Bool(false)
	// One publication the filter matches and one it does not.
	topicScript.WriteStep(w, "publish")
	w.Choice(2, 0)
	topicScript.WriteStep(w, "publish")
	w.Choice(2, 1)
	out = append(out, w.Data())
	return out
}

// itoa is strconv.Itoa for a small non-negative number, spelled here so the
// target's imports stay to the packages it is about.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
