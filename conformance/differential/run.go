package differential

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/testkit"
)

// Arm is one side of the differential.
//
// The topology comes from [testkit.Harness], which every driver in this
// repository already ships, so a new broker arm is a name and a harness rather
// than a new copy of the run loop. That is the whole reason the arm is a
// harness and not a client: a differential that opened clients itself would
// have to know how each broker provisions a topic, and the four that cannot
// provision through Client.Admin would each need a special case here.
type Arm struct {
	// Name identifies the arm in failure messages.
	Name string
	// Harness opens the client and builds the topology.
	Harness testkit.Harness
	// Perm is what the arm is permitted to do. The zero value is the
	// strictest reading; [Pair.Run] replaces a zero Perm with
	// [PermissionsFor] over the arm's own capability table, which is the
	// normal case — a Perm set by hand is for a test that wants a stricter
	// reading than the driver claims.
	Perm Permissions
	// Caps is the arm's capability table once [Probe] has read it. Nil
	// means it has not been probed yet.
	Caps *mq.Capabilities
}

// RunOptions bounds one run of one script against one arm.
type RunOptions struct {
	// Topic overrides the harness's topic name. Both arms must use their
	// own name for the same purpose, which is what the default gives.
	Topic string
	// Subscription is the subscription name. Empty means "differential".
	Subscription string
	// ReceiveTimeout bounds one OpReceive. It has to be long enough that a
	// broker that is merely slow is not read as a broker that lost the
	// message, and short enough that a script of MaxOps receives fits in a
	// test. A receive that times out is recorded as "nothing available",
	// which is a legal answer.
	ReceiveTimeout time.Duration
	// DrainBudget bounds the drain phase at the end of the script: the
	// phase that receives and acks everything still outstanding so that
	// both arms end with a settled multiset rather than with whatever the
	// script happened to leave in flight.
	DrainBudget time.Duration
	// MaxDrainDeliveries bounds the drain by count as well as by time, so
	// that a broker in a redelivery loop fails the run rather than hanging
	// it.
	MaxDrainDeliveries int
	// AckDeadline is the lease the topology is created with. The drain will
	// not converge before twice this has elapsed, because a driver whose
	// nack leaves the entry pending needs the lease to expire before the
	// redelivery can arrive.
	AckDeadline time.Duration
}

// resolve fills in the defaults.
func (o RunOptions) resolve() RunOptions {
	if o.Subscription == "" {
		o.Subscription = "differential"
	}
	if o.ReceiveTimeout <= 0 {
		o.ReceiveTimeout = 2 * time.Second
	}
	if o.DrainBudget <= 0 {
		o.DrainBudget = 15 * time.Second
	}
	if o.MaxDrainDeliveries <= 0 {
		o.MaxDrainDeliveries = 8 * MaxOps
	}
	if o.AckDeadline <= 0 {
		o.AckDeadline = 5 * time.Second
	}
	if o.DrainBudget < 4*o.AckDeadline {
		o.DrainBudget = 4 * o.AckDeadline
	}
	return o
}

// TopicFor is the topic name an arm uses for a purpose.
//
// testkit.Harness keeps its own topicName unexported, so this repeats the
// contract rather than reaching for it: a harness that names its own topics
// (every containerised driver does, because a live broker is shared with
// everything else on it) is asked, and one that does not gets a name with a
// run nonce in it. Two arms must NOT share a name — they are different
// brokers — and each must be unique per run.
func TopicFor(h testkit.Harness, purpose string) string {
	if h.TopicName != nil {
		return h.TopicName(purpose)
	}
	return "iqmq-differential-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + purpose
}

// Key is the key the script's nth key index maps to. It is a fixed string per
// index so that the two arms publish under identical keys, and so that a
// failure message names the key a reader can grep the broker for.
func Key(i int) string { return "k" + strconv.Itoa(i) }

// ID is the script's serial number for the nth publish.
func ID(n int) string { return "m" + strconv.Itoa(n) }

// bodyFor is the payload: the id, so that a delivery is matched to its
// publication by what the SCRIPT wrote and not by anything the broker
// assigned. A broker that rewrites ids, loses headers or reorders would
// otherwise be compared against its own bookkeeping.
func bodyFor(id string) []byte { return []byte("iqmq-differential:" + id) }

// idFrom reads the id back out of a body, or "" when the body is not one of
// ours — which is itself a finding, and is what [KindUnknown] reports.
func idFrom(body []byte) string {
	const prefix = "iqmq-differential:"
	s := string(body)
	if !strings.HasPrefix(s, prefix) {
		return ""
	}
	return s[len(prefix):]
}

// Run replays script against arm and returns what it observed.
//
// An error is a failure of the RUN rather than a divergence: the client would
// not open, the topology could not be built, a receive returned a transport
// error the driver did not recover from. The caller reports it as such, because
// a run that did not happen must not be compared.
func Run(ctx context.Context, arm Arm, script Script, opts RunOptions) (Observation, error) {
	opts = opts.resolve()
	obs := Observation{Arm: arm.Name}

	topic := opts.Topic
	if topic == "" {
		topic = TopicFor(arm.Harness, "differential")
	}
	need := testkit.Need{
		Topic:         topic,
		Subscriptions: []string{opts.Subscription},
		Partitions:    1,
		// The lease is SHORT, and the first version's two minutes was
		// wrong. Redis Streams has no native nack: the driver leaves the
		// entry pending and it becomes reclaimable only once its idle
		// time exceeds the lease, so with a two-minute lease a nacked
		// message could not come back inside any drain a test can
		// afford — "nacked for redelivery on redis and never came back",
		// which is the harness's lease and not the driver's redelivery.
		// Five seconds is long enough that the
		// script's own pace never expires one and short enough that the
		// drain outlasts it.
		AckDeadline: opts.AckDeadline,
	}
	client, err := arm.Harness.Open(ctx, need)
	if err != nil {
		return obs, fmt.Errorf("%s: opening a client: %w", arm.Name, err)
	}
	defer func() { _ = client.Close() }()

	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataAdaptive})
	if err != nil {
		return obs, fmt.Errorf("%s: publisher: %w", arm.Name, err)
	}
	defer func() { _ = pub.Close() }()

	// The start position is left at its default (StartLatest), and the
	// consumer is opened BEFORE the first publish, which is what makes that
	// correct: StartEarliest needs a retained log, so a memory topic or a
	// plain queue refuses it outright with ErrUnsupported — the first run of
	// this runner failed with "memory: consumer: iqmq: unsupported" for
	// exactly that reason.
	con, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        topic,
		Subscription: opts.Subscription,
		Prefetch:     16,
		Metadata:     mq.MetadataAdaptive,
	})
	if err != nil {
		return obs, fmt.Errorf("%s: consumer: %w", arm.Name, err)
	}
	defer func() { _ = con.Close() }()

	r := &runner{arm: arm, opts: opts, topic: topic, pub: pub, con: con, obs: &obs}
	if err := r.play(ctx, script); err != nil {
		return obs, err
	}
	if err := r.drain(ctx); err != nil {
		return obs, err
	}
	return obs, nil
}

// runner holds the state one arm's replay needs.
type runner struct {
	arm   Arm
	opts  RunOptions
	topic string
	pub   *mq.PublisherHandle
	con   *mq.ConsumerHandle
	obs   *Observation
	// serial is the next publish's id number.
	serial int
	// action is the settlement action the script assigned each published
	// message, by id. It is what makes the two arms settle the SAME message
	// the same way whatever order their deliveries arrived in.
	action map[string]Action
	// seen counts deliveries of each id, so that ActionNackThenAck nacks
	// the first and acks the second.
	seen map[string]int
	// buf is the receive destination, reused so that the run's own
	// allocations do not dominate a driver whose receive path is measured
	// elsewhere.
	buf []mq.Delivery
}

// play runs the script's ops in order.
func (r *runner) play(ctx context.Context, s Script) error {
	r.action = make(map[string]Action, len(s.Ops))
	r.seen = make(map[string]int, len(s.Ops))
	for i, op := range s.Ops {
		var err error
		switch op.Kind {
		case OpPublish:
			err = r.publish(ctx, Key(op.Key), op.Action)
		case OpReceive:
			err = r.receive(ctx, op.N)
		}
		if err != nil {
			return fmt.Errorf("%s: op %d (%s): %w", r.arm.Name, i, op, err)
		}
	}
	return nil
}

// publish sends one message and records the outcome.
func (r *runner) publish(ctx context.Context, key string, a Action) error {
	r.serial++
	id := ID(r.serial)
	r.action[id] = a
	m := &mq.Message{Topic: r.topic, Key: []byte(key), Body: bodyFor(id), ID: id}
	err := r.pub.Publish(ctx, m)
	switch {
	case err == nil:
		r.obs.Pubs = append(r.obs.Pubs, Pub{Key: key, ID: id, Outcome: Accepted})
		return nil
	case isAmbiguous(err):
		// The specification: the publication's fate is unknown and stays unknown. The
		// relation permits both readings and counts them.
		r.obs.Pubs = append(r.obs.Pubs, Pub{Key: key, ID: id, Outcome: Ambiguous})
		return nil
	case isDefinite(err):
		r.obs.Pubs = append(r.obs.Pubs, Pub{Key: key, ID: id, Outcome: Rejected})
		return nil
	default:
		// A transport error the driver did not classify is a failure of
		// the RUN: recording it as either outcome would put a guess into
		// the comparison.
		return fmt.Errorf("publishing %s under key %q: %w", id, key, err)
	}
}

// receive asks for up to n deliveries and records each one.
func (r *runner) receive(ctx context.Context, n int) error {
	if n > cap(r.buf) {
		r.buf = make([]mq.Delivery, n)
	}
	dst := r.buf[:n]
	rctx, cancel := context.WithTimeout(ctx, r.opts.ReceiveTimeout)
	defer cancel()
	got, err := r.con.Receive(rctx, dst)
	switch {
	case err == nil:
	case errors.Is(err, context.DeadlineExceeded):
		// A deadline with nothing to show is "nothing available", which
		// is a legal answer and not an error. A deadline WITH deliveries
		// keeps them: Receive fills what it got before it returns.
		//
		// That reading only holds while it was OUR ReceiveTimeout that
		// elapsed. rctx also carries the RUN's deadline, so once the run
		// is out of budget every remaining receive returns instantly
		// with nothing, the script finishes early and a TRUNCATED
		// observation reaches Compare — a divergence about the harness
		// reported as one about the broker , which is
		// what the drain's two bounds already refuse in the other phase.
		// Issue #58 is that failure seen from the fuzzer's side.
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("the run on %s spent its whole budget with %d deliver(ies) taken; the observation would be truncated: %w",
				r.arm.Name, len(r.obs.Dels), cerr)
		}
	case errors.Is(err, mq.ErrClosed):
		return err
	default:
		if got == 0 {
			return fmt.Errorf("receive: %w", err)
		}
	}
	for i := range got {
		d := dst[i]
		id := idFrom(d.Body)
		if id == "" {
			// A body we did not write. Recorded with the raw body's
			// length as the id so that the relation reports
			// KindUnknown rather than the run silently dropping it.
			id = "foreign:" + strconv.Itoa(len(d.Body))
		}
		r.obs.Dels = append(r.obs.Dels, Del{
			Key:         string(d.Key),
			ID:          id,
			Settlement:  Unsettled,
			Redelivered: d.Info().Redelivered,
		})
		rec := len(r.obs.Dels) - 1
		r.seen[id]++
		if err := r.settle(&d, id, rec, r.seen[id]); err != nil {
			return err
		}
	}
	return nil
}

// settle applies the delivery's own message's action.
//
// attempt is how many times this run has seen the id, so
// [ActionNackThenAck] nacks the first delivery and acks every later one. An id
// the run never published — which a broker can produce and the relation
// reports as [KindUnknown] — is acked, because leaving it leased would make it
// come back for ever.
func (r *runner) settle(d *mq.Delivery, id string, rec, attempt int) error {
	want := Acked
	switch r.action[id] {
	case ActionAck:
		// Acked already, and named so that a new Action is a lint
		// failure here rather than a silent ack.
	case ActionNackThenAck:
		if attempt == 1 {
			want = Nacked
		}
	case ActionDrop:
		if attempt == 1 {
			want = Dropped
		}
	}
	var err error
	switch want {
	case Unsettled:
		// Unreachable: want starts at Acked and the switch above only
		// ever moves it to Nacked or Dropped. Named because a
		// settlement nobody performed must not look like an ack.
		return fmt.Errorf("settling %s: the run chose no settlement at all", id)
	case Acked:
		err = d.Ack()
	case Nacked:
		err = d.Nack(mq.NackRequeue())
	case Dropped:
		err = d.Nack(mq.NackDrop())
	}
	switch {
	case err == nil:
		r.obs.Dels[rec].Settlement = want
		return nil
	case errors.Is(err, mq.ErrSettlementUnknown):
		// An ambiguous settlement leaves the delivery Unsettled, which is
		// exactly what it is: the broker may or may not have taken it, so
		// a redelivery is permitted and so is none. This is the
		// "latency > timeout during ack" case the chaos scenarios inject
		// deliberately.
		return nil
	case errors.Is(err, mq.ErrLeaseLost), errors.Is(err, mq.ErrAlreadySettled):
		// The lease expired or something else settled it first. Neither
		// is a run failure and neither is our settlement.
		return nil
	default:
		return fmt.Errorf("settling %s as %s: %w", id, want, err)
	}
}

// drain receives and acks until nothing comes back, so that both arms end with
// a settled multiset rather than with whatever the script left in flight.
//
// The two bounds are not belt and braces. Time alone would let a broker in a
// redelivery loop spend the whole budget and still report success on a
// truncated observation; count alone would let a slow broker's last message
// arrive after the loop gave up. A drain that hits either bound returns an
// error, because a truncated observation compared against a complete one is a
// divergence about the harness.
func (r *runner) drain(ctx context.Context) error {
	deadline := time.Now().Add(r.opts.DrainBudget)
	// The drain may not converge before twice the lease has elapsed: a
	// driver whose nack leaves the entry pending (Redis Streams has no
	// native nack) can only redeliver once the lease has expired, and a
	// drain that stopped at the first empty receive reported "nacked for
	// redelivery on redis and never came back".
	earliest := time.Now().Add(2 * r.opts.AckDeadline)
	seen, empties := 0, 0
	for empties < 2 || time.Now().Before(earliest) {
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: the drain spent its %v budget with %d deliver(ies) taken and %d consecutive empty receive(s); the observation is truncated",
				r.arm.Name, r.opts.DrainBudget, seen, empties)
		}
		if seen > r.opts.MaxDrainDeliveries {
			return fmt.Errorf("%s: the drain took %d deliver(ies), over its %d ceiling: the broker is redelivering faster than the drain settles",
				r.arm.Name, seen, r.opts.MaxDrainDeliveries)
		}
		before := len(r.obs.Dels)
		if err := r.receive(ctx, 8); err != nil {
			return err
		}
		got := len(r.obs.Dels) - before
		seen += got
		if got == 0 {
			empties++
			continue
		}
		empties = 0
	}
	return nil
}

// isAmbiguous reports whether a publish error leaves the outcome unknown.
func isAmbiguous(err error) bool {
	// ErrPartial is here because a batch whose parts disagree leaves the
	// publication's fate per message unknown, which is the same reading
	// The specification gives CodeSettlementUnknown. The script publishes one message at
	// a time today, so it is the belt for a batching script rather than a
	// live branch.
	return errors.Is(err, mq.ErrSettlementUnknown) || errors.Is(err, mq.ErrPartial)
}

// isDefinite reports whether a publish error means the broker definitely did
// not take the message.
func isDefinite(err error) bool {
	return errors.Is(err, mq.ErrUnsupported) ||
		errors.Is(err, mq.ErrMessageTooLarge) ||
		errors.Is(err, mq.ErrInvalidTopic) ||
		errors.Is(err, mq.ErrTopicNotFound)
}

// Probe opens a client on the arm, reads its capability table and closes it.
//
// The relation's permissions and the script's restriction both come from the
// table, and both have to be known BEFORE either arm runs: a script restricted
// per arm would let the two arms run different scripts, which is the one thing
// the comparison cannot survive. So the table is read first, in its own client,
// and [Pair] caches it so that a fuzz campaign against a container pays for one
// probe rather than one per input.
func Probe(ctx context.Context, arm *Arm) (mq.Capabilities, error) {
	if arm.Caps != nil {
		return *arm.Caps, nil
	}
	client, err := arm.Harness.Open(ctx, testkit.Need{
		Topic:         TopicFor(arm.Harness, "probe"),
		Subscriptions: []string{"probe"},
		Partitions:    1,
	})
	if err != nil {
		return mq.Capabilities{}, fmt.Errorf("%s: probing capabilities: %w", arm.Name, err)
	}
	caps := client.Capabilities()
	if err := client.Close(); err != nil {
		return caps, fmt.Errorf("%s: closing the probe client: %w", arm.Name, err)
	}
	arm.Caps = &caps
	return caps, nil
}

// Pair is one differential: an oracle, a subject and what they agreed to run.
type Pair struct {
	// Oracle is the reference arm, which must be the memory engine with
	// nothing injected.
	Oracle Arm
	// Subject is the arm under test.
	Subject Arm
	// Opts bounds both runs.
	Opts RunOptions
}

// PairResult is one comparison, with everything a failure message needs to be
// reproducible.
type PairResult struct {
	// Script is the script both arms ran, AFTER restriction.
	Script Script
	// Rewritten is how many ops the restriction turned into an ack because
	// one of the arms could not perform them.
	Rewritten int
	// Perm is what the subject was held to.
	Perm Permissions
	// Oracle and Subject are the two observations.
	Oracle, Subject Observation
	// Result is the verdict.
	Result Result
}

// String is the one-line report a run logs whether or not it diverged.
func (p PairResult) String() string {
	ops, actions := p.Script.Counts()
	return fmt.Sprintf("%s vs %s: %s; %s; script(keys=%d publishes=%d receives=%d acked=%d nacked=%d dropped=%d, %d rewritten): %s",
		p.Oracle.Arm, p.Subject.Arm, p.Result.Summary, p.Perm.Explain(),
		p.Script.Keys, ops[OpPublish], ops[OpReceive],
		actions[ActionAck], actions[ActionNackThenAck], actions[ActionDrop],
		p.Rewritten, p.Script)
}

// Run probes both arms, restricts the script to what both can do, replays it on
// each and compares.
//
// The subject runs SECOND and the oracle first, deliberately: the oracle is
// in-process and cannot be affected by the subject, while a container arm that
// leaves a connection or a consumer group behind could perturb a later oracle
// run. It also means a run that fails to open the container has already proved
// the oracle works.
func (p *Pair) Run(ctx context.Context, script Script) (PairResult, error) {
	out := PairResult{}
	oc, err := Probe(ctx, &p.Oracle)
	if err != nil {
		return out, err
	}
	sc, err := Probe(ctx, &p.Subject)
	if err != nil {
		return out, err
	}
	out.Script, out.Rewritten = script.Restrict(Intersect(FeaturesOf(oc), FeaturesOf(sc)))
	out.Perm = p.Subject.Perm
	if out.Perm == (Permissions{}) {
		out.Perm = PermissionsFor(sc)
	}
	if out.Oracle, err = Run(ctx, p.Oracle, out.Script, p.Opts); err != nil {
		return out, err
	}
	if out.Subject, err = Run(ctx, p.Subject, out.Script, p.Opts); err != nil {
		return out, err
	}
	out.Result = Compare(out.Oracle, out.Subject, out.Perm)
	return out, nil
}
