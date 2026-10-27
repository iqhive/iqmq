package testkit

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"time"

	mq "github.com/iqhive/iqmq"
)

// exercise is a behavioural probe of one feature through the portable API.
//
// The same function proves both directions, which is the point. A driver that
// claims the feature must have it work; a driver that does not must refuse it
// with ErrUnsupported. Writing one probe and running it against both gates makes
// the two answers impossible to drift apart, and it is what turns "verify
// negative claims" from a slogan into a check: a driver that quietly does what
// it says it cannot fails the refusal, and a driver that quietly does not do
// what it says it can fails the positive.
type exercise struct {
	// shape is the topology the probe needs.
	shape func(n *Need)
	// refusal names the ONE portable call a driver that does not have the
	// feature has to refuse, in the API's own spelling, and the probe marks
	// that call's answer with refusedAt.
	//
	// It is what makes the negative direction evidence about the feature
	// rather than about whatever failed last. Exactly one of refusal and
	// noRefusal is set, and a probe that sets neither is a probe nobody
	// decided about -- which is why the table is held to it by
	// TestEveryProbeSaysWhereARefusalWouldCome.
	refusal string
	// noRefusal says the feature has NO call a driver could refuse, with
	// the reason, and is set instead of refusal. A guarantee has none by
	// definition; so does a feature whose absence shows up only in what
	// arrives, which is the class this increment exists for.
	noRefusal string
	// run exercises the feature. It returns nil when the feature worked, a
	// refusedAt-marked error when the feature's own call was refused, a
	// Diverged when the call was accepted and something else happened, a
	// violation when it observed a guarantee break, a Skip when it could
	// not run, and any other error for a driver that is simply broken.
	run func(ctx context.Context, e *Env) error
}

// capabilityScenarios builds the three scenarios every feature of the specification's
// inventory gets: the feature works, the claim is well formed, and the
// unsupported path is refused.
//
// Exactly two of the three run for any one driver. A claimed feature gets
// "works" (positive) and "claim" (negative); an unclaimed one gets "refused"
// (negative). That is what the report's verification rule needs: an unsupported
// claim is proved by its refusal alone, and a supported one by both directions
func capabilityScenarios() []Scenario {
	probes := exercises()
	promises := guarantees()
	out := make([]Scenario, 0, mq.FeatureCount*3)
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		name := f.String()
		ex, probed := probes[f]
		kind := KindBehaviour
		if promises[f] {
			kind = KindGuarantee
		}
		works := Scenario{
			Name: "capability/" + name + "/works", Feature: f, Has: true, Positive: true,
			Gate: claims(f), Kind: kind,
			Run: func(ctx context.Context, e *Env) error {
				if !probed {
					return Skip("no portable probe for " + name + " yet, so the claim stays unverified rather than being taken on trust")
				}
				err := ex.run(ctx, e)
				if err == nil {
					return nil
				}
				if reason, skipped := IsSkip(err); skipped {
					return Skip(reason)
				}
				if what, broke := isViolation(err); broke {
					// The one direction a guarantee CAN be
					// settled in by a single run: a
					// counter-example refutes the promise
					// outright.
					return failf("the driver advertises %v at %v and this run observed the guarantee break: %v",
						f, e.Caps.Support(f).Level, what)
				}
				if what, diverged := isDivergence(err); diverged {
					// The same observation the negative direction
					// leaves unrefuted is a FAILURE here: the driver
					// advertised the feature, refused nothing, and
					// did something else.
					return failf("the driver advertises %v at %v and did something else instead: %v",
						f, e.Caps.Support(f).Level, what)
				}
				if ref, attributed := isRefusal(err); attributed {
					return failf("the driver advertises %v at %v and refused %v: %v",
						f, e.Caps.Support(f).Level, ref.at, ref.err)
				}
				return failf("the driver advertises %v at %v and the feature did not work: %v",
					f, e.Caps.Support(f).Level, err)
			},
		}
		refused := Scenario{
			Name: "capability/" + name + "/refused", Feature: f, Has: true, Positive: false,
			Gate: lacks(f), Kind: kind,
			Run: func(ctx context.Context, e *Env) error {
				if !probed {
					return Skip("no portable probe for " + name + " yet, so the negative claim stays unverified")
				}
				err := ex.run(ctx, e)
				if kind == KindGuarantee {
					return refusedGuarantee(e, f, err)
				}
				return refusedBehaviour(e, f, ex, err)
			},
		}
		claim := Scenario{
			Name: "capability/" + name + "/claim", Feature: f, Has: true, Positive: false,
			Gate: claims(f), Kind: kind,
			Run: func(_ context.Context, e *Env) error { return checkClaim(e.Caps, f) },
		}
		if ex.shape != nil {
			// The CLAIM scenario gets the probe's shape too, and
			// that is a repair rather than tidiness.
			//
			// A feature whose availability depends on the topology is
			// configured per scenario: RabbitMQ's priority ceiling
			// forces a classic queue, and a Pulsar dead-letter route
			// needs a destination the Need names. Without the shape,
			// `claim` ran against a client built from an UNSHAPED
			// Need — which does not advertise the feature — so its
			// own gate skipped it and the claim was reported
			// "positive true, negative false" for ever. Two drivers
			// lost a claim to it in exactly that way: the specification's
			// FeaturePriority and FeatureDeadLetter
			// (the maintenance notes, "capability/<F>/claim does not get
			// the feature's Shape").
			works.Shape, refused.Shape, claim.Shape = ex.shape, ex.shape, ex.shape
		}
		out = append(out, works, claim, refused)
	}
	return out
}

// guarantees are the features whose claim is a PROMISE ABOUT EVERY RUN rather
// than a call with something to refuse.
//
// AtLeastOnce is deliberately NOT here. It is a guarantee in the same sense,
// but its probe is a call — Nack, and a redelivery afterwards — so a driver
// that lacks it has something to refuse and a driver that has it something to
// demonstrate. What remains open for it is the neighbouring class recorded in
// the maintenance notes: a driver that does something DIFFERENT rather than
// refusing, which FanOut and Replay reach through the SPI accepting a
// subscription name it cannot honour.
func guarantees() map[mq.Feature]bool {
	return map[mq.Feature]bool{
		mq.FeatureDurableStorage:      true,
		mq.FeatureAtMostOnce:          true,
		mq.FeatureExactlyOnceDelivery: true,
		mq.FeatureOrderedByKey:        true,
		mq.FeatureOrderedByPartition:  true,
		mq.FeatureOrderedGlobally:     true,
	}
}

// refusedGuarantee is the negative direction of a guarantee claim, where a
// single run cannot establish an absence.
//
// Three answers, and only one of them is a pass:
//
//   - the probe observed a COUNTER-EXAMPLE. The denial is confirmed by
//     evidence, which is the only way it can be.
//   - the probe saw nothing wrong. That is neither a pass nor a failure, and
//     calling it either is what this function exists to stop. It is a skip
//     whose note says so, and the claim stays unproven — which is the specification's
//     honest rendering for something nobody could check.
//   - the probe could not run. Also a skip, INCLUDING when the reason is
//     ErrUnsupported. That is the one answer worth spelling out: for a
//     behaviour a refusal IS the negative proof, and for a guarantee there is
//     no call to refuse — that is what makes it a guarantee — so a refusal can
//     only have come from something the probe needed on the way past, and
//     reading it as evidence about the promise is how a check comes to pass
//     for the wrong reason. This function's first version did read it that
//     way, and because the exactly-once probe then still sent a DedupKey,
//     four of the nine driver suites recorded their exactly-once DENIAL as
//     proven by a refusal of DE-DUPLICATION.
func refusedGuarantee(e *Env, f mq.Feature, err error) error {
	if what, broke := isViolation(err); broke {
		e.Note("the denial is confirmed by observation: %s", what)
		return nil
	}
	if reason, skipped := IsSkip(err); skipped {
		return Skip(reason)
	}
	if what, diverged := isDivergence(err); diverged {
		// A guarantee's probe reached the same dead end a behaviour's
		// can: the call was accepted and something else happened. The
		// kind is already one-sided, so nothing is promoted; what the
		// report gains is the observation instead of a bare error.
		e.Note("the driver refused nothing and did something else: %s", what)
		return Skip("the driver advertises no " + f.String() + " and the probe found no counter-example because the driver did something else: " + what)
	}
	if err == nil {
		return Skip("the driver advertises no " + f.String() + " and this run observed no counter-example; a guarantee's ABSENCE is not observable in one run, so the denial stays unrefuted rather than being read as false ")
	}
	return Skip("the probe for " + f.String() + " could not run, so the denial stays unrefuted: " + err.Error())
}

// refusedBehaviour is the negative direction of a BEHAVIOUR claim: the driver
// says it cannot do this, and the question is whether it REFUSES.
//
// Five answers, and only one of them is a pass. The shape is the whole of this
// increment, because the version it replaces had three -- nil failed, a skip
// skipped, and every other error went to wantUnsupported, which accepted an
// ErrUnsupported from ANY of the probe's five or six calls and failed
// everything else. Both halves of that were wrong in the same way, and the way
// is that the outcome did not depend on WHICH call answered:
//
//   - the feature WORKED and the driver does not claim it. A failure, and the
//     one answer the old shape got right.
//   - the driver ACCEPTED the feature's own call and did something else. Not a
//     refusal and not the feature: there is nothing for a refusal probe to
//     observe, so the denial is left UNREFUTED the way a guarantee's is, and
//     the result's kind is promoted to say the evidence is one-sided. This is
//     the answer that was a FAILURE before, and it failed drivers whose denials
//     were true -- capability/FanOut/refused and capability/Replay/refused on
//     both SQS profiles, plus the at-least-once denial of any driver that has
//     settlement and correctly does not redeliver.
//   - the feature's own call was REFUSED, with ErrUnsupported or the specification's
//     ErrNotSettleable. The denial is proven, and the note names the call, so a
//     reader of the report can see what the evidence is about.
//   - the feature's own call answered something that is neither. The specification gives a
//     driver that does not support a call exactly one answer; a driver that
//     reports an unrelated failure has done something other than refuse, and
//     that is a failure NAMED against the call rather than a generic "want
//     ErrUnsupported" about whichever error surfaced last.
//   - nothing reached the feature's own call at all. A refusal from a step on
//     the way past is evidence about that step: it used to be a PASS, and a
//     driver whose consumer open answered ErrUnsupported had every negative
//     claim in the suite recorded as refuted by observation. It is a skip.
func refusedBehaviour(e *Env, f mq.Feature, ex exercise, err error) error {
	if err == nil {
		return failf("the driver advertises no %v and the feature worked anyway: an unverified negative claim is how a caller ends up relying on behaviour the driver may remove", f)
	}
	if what, diverged := isDivergence(err); diverged {
		e.oneSided = true
		e.Note("the driver refused nothing and did something else: %s", what)
		return Skip("the driver advertises no " + f.String() +
			" and did not refuse: it ACCEPTED the call and did something DIFFERENT, so there is no refusal to observe and one run cannot show the absence; the denial stays unrefuted rather than being read as refuted or as false ")
	}
	if reason, skipped := IsSkip(err); skipped {
		return Skip(reason)
	}
	ref, attributed := isRefusal(err)
	if !attributed {
		return unattributed(f, ex, err)
	}
	if errors.Is(ref.err, mq.ErrUnsupported) || errors.Is(ref.err, mq.ErrNotSettleable) {
		e.Note("the denial is proven by a refusal of %v: %v", ref.at, ref.err)
		return nil
	}
	return failf("the driver advertises no %v and answered %v with %v (code %v): the specification gives a driver that does not support a call exactly one answer, ErrUnsupported (or ErrNotSettleable for a settlement on a source with no settler), and this is neither the feature working nor a refusal ",
		f, ref.at, ref.err, mq.CodeOf(ref.err))
}

// unattributed is what the negative direction reports for an error that did not
// come from the feature's own call.
//
// The two cases are different answers, and the distinction IS the defect: an
// error that is itself a refusal is a refusal of something ELSE, and reading it
// as evidence is how a claim nobody probed came to render as the strongest
// thing a negative claim can be. Anything else is a driver that neither refused
// nor behaved, which stays a failure -- a probe that cannot tell that apart
// from the broker's own shape says so with Diverged instead.
func unattributed(f mq.Feature, ex exercise, err error) error {
	if ex.refusal == "" {
		// The feature has no call to refuse, so there was never an
		// attribution to make and the error is not one either: the
		// probe neither observed the feature nor recognised what the
		// driver did instead, which is a probe that could not run.
		if errors.Is(err, mq.ErrUnsupported) || errors.Is(err, mq.ErrNotSettleable) {
			return Skip("the probe for " + f.String() + " was refused on the way past and " + f.String() +
				" has no call a driver could refuse (" + ex.noRefusal +
				"), so the refusal is evidence about that step and the denial stays unrefuted: " + err.Error() +
				" ")
		}
		return failf("the driver advertises no %v and the probe neither observed the feature nor recognised what the driver did instead: %v (code %v). %v has no call a driver could refuse (%v), so reaching here with an unclassified error means the probe has to name what it saw before the claim can be judged at all",
			f, err, mq.CodeOf(err), f, ex.noRefusal)
	}
	if errors.Is(err, mq.ErrUnsupported) || errors.Is(err, mq.ErrNotSettleable) {
		return Skip("the probe did not reach the call a driver without " + f.String() + " has to refuse (" +
			ex.refusal + "), so the denial stays unrefuted: " + err.Error() +
			". The refusal it did produce came from a step on the way past, which is evidence about that step and not about " +
			f.String() + " ")
	}
	return failf("the driver advertises no %v and neither refused %v nor let the probe observe the feature: %v (code %v)",
		f, ex.refusal, err, mq.CodeOf(err))
}

// checkClaim holds a claim to the rules about claims themselves: a level that
// exists, a condition where the specification requires one, and a requirement machinery that
// answers about the claim the way the specification says it must.
func checkClaim(caps mq.Capabilities, f mq.Feature) error {
	s := caps.Support(f)
	if !s.Level.Valid() {
		return failf("%v is advertised at a level outside the five", f)
	}
	switch s.Level {
	case mq.Conditional:
		if s.Note == "" {
			return failf("%v is advertised as conditional with no condition: the specification requires a conditional support to state its condition, and one that does not tells a caller nothing they can act on", f)
		}
	case mq.EmulatedVolatile, mq.EmulatedDurable:
		if s.Note == "" {
			return failf("%v is advertised as %v with no note saying what the emulation is", f, s.Level)
		}
	case mq.Native, mq.Unsupported:
	}
	if err := caps.Require(mq.RequireAtLeast(f, s.Level)); err != nil {
		return failf("the driver advertises %v at %v and a requirement for exactly that was refused: %v", f, s.Level, err)
	}
	if err := caps.Require(mq.RequireAny(f, s.Level)); err != nil {
		return failf("RequireAny(%v, %v) was refused by a driver advertising it: %v", f, s.Level, err)
	}
	if s.Level != mq.Native {
		if err := caps.Require(mq.RequireAtLeast(f, mq.Native)); err == nil {
			return failf("the driver advertises %v at %v and accepted a requirement for native support", f, s.Level)
		} else if !errors.Is(err, mq.ErrUnsupported) {
			return failf("an unmet requirement for %v reported %v (code %v), want ErrUnsupported", f, err, mq.CodeOf(err))
		}
	}
	if s.Level == mq.EmulatedVolatile {
		// The specification: volatile emulation NEVER satisfies a durability
		// requirement, whatever the ranks say. It is the one rule in the
		// capability model that a plain ordinal comparison gets wrong.
		if err := caps.Require(mq.RequireAtLeast(f, mq.EmulatedDurable)); err == nil {
			return failf("%v is emulated volatilely and satisfied a durable requirement: the specification forbids it, because the emulation is lost on a crash", f)
		}
	}
	return nil
}

// exercises are the portable probes, one per feature that has one.
//
// A feature with no entry is honestly unverified: its scenarios skip, and the
// generated matrix renders it as unsupported. That is the right answer — a claim
// nobody checked is not a claim a caller should rely on — and the list grows as
// the portable API does.
//
//nolint:gocognit,maintidx // the probes are a table; splitting it hides which feature each line is about
func exercises() map[mq.Feature]exercise {
	return map[mq.Feature]exercise{
		mq.FeatureAtLeastOnce: {
			refusal: "Delivery.Nack",
			shape:   func(n *Need) { n.AckDeadline = time.Second },
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("alo")}); err != nil {
					return err
				}
				if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					return refusedAt("Delivery.Nack", d.Nack())
				}); err != nil {
					return err
				}
				// A nacked message is not lost: at-least-once means it
				// comes back. A driver that ACCEPTED the nack and does
				// not redeliver is the third answer: it refused
				// nothing, and not redelivering is exactly what
				// denying at-least-once means, so the denial is
				// unrefuted rather than failed.
				if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "alo" {
						return failf("the redelivery carries %q", d.Body)
					}
					return d.Ack()
				}); err != nil {
					if isNoDelivery(err) {
						return Diverged("the nack was accepted and nothing came back: the driver does not redeliver a negatively settled message, which is what having no at-least-once delivery looks like (%v)", err)
					}
					return err
				}
				return nil
			},
		},
		mq.FeatureExplicitAck: {
			refusal: "Delivery.Ack",
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("ack")}); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					return refusedAt("Delivery.Ack", d.Ack())
				})
			},
		},
		mq.FeatureNack: {
			refusal: "Delivery.Nack",
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("nack")}); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					return refusedAt("Delivery.Nack", d.Nack())
				})
			},
		},
		mq.FeatureDelayedRedelivery: {
			refusal: "Delivery.Nack with a NackDelay",
			shape:   func(n *Need) { n.AckDeadline = 30 * time.Second },
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("later")}); err != nil {
					return err
				}
				const delay = 2 * time.Second
				if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					return refusedAt("Delivery.Nack with a NackDelay", d.Nack(mq.NackDelay(delay)))
				}); err != nil {
					return err
				}
				if err := expectNothing(ctx, c, 300*time.Millisecond, "a delayed nack redelivered the message immediately"); err != nil {
					return err
				}
				if err := e.Advance(ctx, delay+time.Second); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if d.Attempt != 0 && d.Attempt < 2 {
						return failf("the redelivery reports attempt %d, want at least 2", d.Attempt)
					}
					return d.Ack()
				})
			},
		},
		mq.FeatureScheduledPublish: {
			refusal: "Client.Publish with a Delay option",
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				const delay = 2 * time.Second
				if err := refusedAt("Client.Publish with a Delay option",
					e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("scheduled")}, mq.Delay(delay))); err != nil {
					return err
				}
				if err := expectNothing(ctx, c, 300*time.Millisecond, "a scheduled publication was delivered early"); err != nil {
					return err
				}
				if err := e.Advance(ctx, delay+time.Second); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error { return d.Ack() })
			},
		},
		mq.FeatureMessageTTL: {
			refusal: "Client.Publish with a TTL option",
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				const ttl = time.Second
				if err := refusedAt("Client.Publish with a TTL option",
					e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("expires")}, mq.TTL(ttl))); err != nil {
					return err
				}
				if err := e.Advance(ctx, ttl+2*time.Second); err != nil {
					return err
				}
				if err := e.Quiesce(ctx); err != nil {
					return err
				}
				return expectNothing(ctx, c, time.Second, "a message past its TTL was still delivered")
			},
		},
		mq.FeaturePriority: {
			refusal: "Client.Publish with a Priority option",
			shape:   func(n *Need) { n.Priority = true },
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				if err := refusedAt("Client.Publish with a Priority option",
					e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("low")}, mq.Priority(0))); err != nil {
					return err
				}
				if err := refusedAt("Client.Publish with a Priority option",
					e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("high")}, mq.Priority(9))); err != nil {
					return err
				}
				if err := e.Quiesce(ctx); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "high" {
						// The option was ACCEPTED and the
						// order is the publication order,
						// which is what a broker with no
						// priority ceiling does. It refused
						// nothing, so a refusal probe has
						// nothing to see.
						_ = d.Ack()
						return Diverged("both priorities were accepted and the first delivery is %s rather than the higher-priority message: the driver took the option and served the queue in publication order", strconv.Quote(string(d.Body)))
					}
					return d.Ack()
				})
			},
		},
		mq.FeatureDeadLetter: {
			refusal: "Delivery.Reject with a dead-letter destination configured",
			shape:   func(n *Need) { n.DeadLetterTopic = n.Topic + "-dlq"; n.MaxAttempts = 2 },
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				// The start position is the one thing about
				// this probe that no single answer serves, and
				// both answers were measured before this one
				// was written.
				//
				// StartEarliest is what it asked for, and a
				// driver with no retained log must refuse that
				// — which is the honest answer and the one
				// capability/Replay/refused requires of it. So
				// the probe skipped for every such driver and
				// the one scenario the toy driver claims a
				// feature for and could not have checked was
				// the one about the feature it has.
				//
				// StartLatest alone is worse. It is enough in
				// principle, because this consumer is opened
				// BEFORE the poison message is published, and
				// it is not enough in practice: a Kafka
				// consumer resolves "latest" when it first
				// fetches rather than when the handle opens,
				// and drivers/kafka/kafkago resolves it late
				// enough that the dead-lettered record is
				// already behind the offset. That is a
				// forty-five-second timeout on a driver whose
				// dead-letter route works, and it is the one
				// direction of breakage a kit change must not
				// have.
				//
				// So: ask for the position that is exactly
				// right, and accept the position a broker
				// without retention can honour when it
				// refuses. Which one ran is recorded, because
				// a fallback nobody can see is a second code
				// path pretending to be one.
				dcfg := mq.ConsumerConfig{
					Topic: e.Need.DeadLetterTopic, Subscription: e.Sub, Start: mq.StartEarliest(),
				}
				dlq, err := e.Client.Consumer(ctx, dcfg)
				if errors.Is(err, mq.ErrUnsupported) {
					e.Note("the dead-letter destination is consumed from the END: this driver refuses StartEarliest, which a broker with no retained log has to, and the consumer exists before the poison publish ")
					dcfg.Start = mq.StartLatest()
					dlq, err = e.Client.Consumer(ctx, dcfg)
				}
				if err != nil {
					return Skip("the dead-letter destination cannot be consumed through the portable API: " + err.Error())
				}
				defer func() { _ = dlq.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("poison")}); err != nil {
					return err
				}
				if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					return refusedAt("Delivery.Reject with a dead-letter destination configured", d.Reject())
				}); err != nil {
					return err
				}
				if err := oneDelivery(ctx, dlq, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "poison" {
						return failf("the dead-letter destination holds %q", d.Body)
					}
					return d.Ack()
				}); err != nil {
					if isNoDelivery(err) {
						return Diverged("the reject was accepted and the dead-letter destination stayed empty: the driver dropped the rejected message rather than routing it, which is what having no dead-letter route looks like (%v)", err)
					}
					return err
				}
				return nil
			},
		},
		mq.FeatureConsumerGroups: {
			noRefusal: "two consumers of one subscription are both accepted at open and the difference shows up only in what arrives",
			run: func(ctx context.Context, e *Env) error {
				a, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = a.Close() }()
				b, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = b.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("shared")}); err != nil {
					return err
				}
				// Exactly one of the two sees it: that is what sharing a
				// subscription means.
				seen := 0
				for _, c := range []*mq.ConsumerHandle{a, b} {
					dst := make([]mq.Delivery, 1)
					bctx, cancel := context.WithTimeout(ctx, e.Timeout()/4)
					n, _ := c.Receive(bctx, dst)
					cancel()
					if n == 1 {
						seen++
						if err := ack(&dst[0]); err != nil {
							return err
						}
					}
				}
				switch seen {
				case 1:
					return nil
				case 2:
					// Both opens were accepted and both readers
					// got a copy: the driver FANNED OUT where a
					// shared subscription was asked for. It
					// refused nothing, so the denial is
					// unrefuted.
					return Diverged("both consumers of one subscription received the message: the driver accepted two readers of one name and gave each a copy rather than sharing the work")
				default:
					return failf("%d of two consumers sharing one subscription received the message, want exactly one", seen)
				}
			},
		},
		mq.FeatureDurableSubscriptions: {
			noRefusal: "reopening a subscription by name is accepted by every driver and the difference shows up only in whether what was published while nobody read is still there",
			run: func(ctx context.Context, e *Env) error {
				first, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("kept")}); err != nil {
					_ = first.Close()
					return err
				}
				if err := first.Close(); err != nil {
					return err
				}
				// The subscription outlived its consumer, so the message
				// published while nobody was reading is still there.
				second, err := e.Consumer(ctx, mq.ConsumerConfig{Start: mq.StartLatest()})
				if err != nil {
					return err
				}
				defer func() { _ = second.Close() }()
				if err := oneDelivery(ctx, second, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "kept" {
						return failf("the reopened subscription holds %q", d.Body)
					}
					return d.Ack()
				}); err != nil {
					if isNoDelivery(err) {
						return Diverged("the subscription reopened without complaint and the message published while nobody was reading is gone: the driver's subscription does not outlive its consumer, which is what having no durable subscription looks like (%v)", err)
					}
					return err
				}
				return nil
			},
		},
		mq.FeatureFanOut: {
			// The SPI accepts two subscription NAMES and has no way
			// to say they are one subscription, so a queue broker
			// refuses nothing here and the difference shows up only
			// in what arrives. That is why the probe counts the
			// readers that got a copy instead of failing on the
			// first empty receive: one reader of two is a queue
			// broker competing, which is what having no fan-out
			// looks like, and it used to be reported as this
			// driver failing its own denial.
			noRefusal: "two subscription names are accepted at open and the SPI cannot say they are one subscription, so a queue broker refuses nothing and its two readers simply compete",
			shape:     func(n *Need) { n.Subscriptions = []string{"fan-1", "fan-2"} },
			run: func(ctx context.Context, e *Env) error {
				a, err := e.Client.Consumer(ctx, mq.ConsumerConfig{Topic: e.Topic, Subscription: e.Need.Sub(0)})
				if err != nil {
					return err
				}
				defer func() { _ = a.Close() }()
				b, err := e.Client.Consumer(ctx, mq.ConsumerConfig{Topic: e.Topic, Subscription: e.Need.Sub(1)})
				if err != nil {
					return err
				}
				defer func() { _ = b.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("both")}); err != nil {
					return err
				}
				seen := 0
				for _, c := range []*mq.ConsumerHandle{a, b} {
					if err := oneDelivery(ctx, c, e.Timeout(), ack); err != nil {
						if isNoDelivery(err) {
							continue
						}
						return err
					}
					seen++
				}
				switch seen {
				case 2:
					return nil
				case 1:
					return Diverged("one of the two subscriptions received the publication and the other received nothing: both names were accepted and they are ONE subscription, so the two readers COMPETE rather than each receiving a copy")
				default:
					return failf("neither of two subscriptions of one topic received the publication")
				}
			},
		},
		mq.FeatureReplay: {
			refusal: "Client.Consumer with Start at the earliest retained position",
			shape:   func(n *Need) { n.Retain = true; n.Subscriptions = []string{"replay-a", "replay-b"} },
			run: func(ctx context.Context, e *Env) error {
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("history")}); err != nil {
					return err
				}
				a, err := e.Client.Consumer(ctx, mq.ConsumerConfig{Topic: e.Topic, Subscription: e.Need.Sub(0), Start: mq.StartEarliest()})
				if err != nil {
					return refusedAt("Client.Consumer with Start at the earliest retained position", err)
				}
				defer func() { _ = a.Close() }()
				if err := oneDelivery(ctx, a, e.Timeout(), func(d *mq.Delivery) error { return d.Ack() }); err != nil {
					return err
				}
				// A second subscription created afterwards reads the same
				// history, which is what a retained log is for.
				b, err := e.Client.Consumer(ctx, mq.ConsumerConfig{Topic: e.Topic, Subscription: e.Need.Sub(1), Start: mq.StartEarliest()})
				if err != nil {
					return refusedAt("Client.Consumer with Start at the earliest retained position", err)
				}
				defer func() { _ = b.Close() }()
				if err := oneDelivery(ctx, b, e.Timeout(), func(d *mq.Delivery) error {
					if string(d.Body) != "history" {
						return failf("the replayed message is %q", d.Body)
					}
					return d.Ack()
				}); err != nil {
					if isNoDelivery(err) {
						// The second reader was accepted at
						// the earliest position and found
						// nothing, because the first reader's
						// settlement REMOVED the message.
						// That is a queue rather than a log,
						// and it is what having no replay
						// looks like.
						return Diverged("a second reader was accepted at the earliest retained position and received nothing after the first had settled the message: the broker consumed the history rather than retaining it, so there is nothing to replay (%v)", err)
					}
					return err
				}
				return nil
			},
		},
		mq.FeatureSeekByTime: {
			refusal: "Client.Consumer with Start at a timestamp",
			shape:   func(n *Need) { n.Retain = true },
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Start: mq.StartAt(time.Now().Add(-time.Hour))})
				if err != nil {
					return refusedAt("Client.Consumer with Start at a timestamp", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("sought")}); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error { return d.Ack() })
			},
		},
		mq.FeatureSeekBySequence: {
			refusal: "Client.Consumer with Start at a sequence",
			shape:   func(n *Need) { n.Retain = true },
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Start: mq.StartAtSequence(1)})
				if err != nil {
					return refusedAt("Client.Consumer with Start at a sequence", err)
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("seq")}); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error { return d.Ack() })
			},
		},
		mq.FeatureWildcardSubscribe: {
			refusal: "Client.Consumer on a subscription pattern",
			shape:   func(n *Need) { n.Wildcards = true },
			run:     wildcardProbe,
		},
		mq.FeatureOrderedByKey:       {noRefusal: orderingHasNoCall, shape: orderedShape, run: orderedProbe},
		mq.FeatureOrderedByPartition: {noRefusal: orderingHasNoCall, shape: orderedShape, run: orderedProbe},
		mq.FeatureOrderedGlobally:    {noRefusal: orderingHasNoCall, shape: orderedShape, run: orderedProbe},
		mq.FeatureNativeHeaders: {
			refusal: "Client.Publish with a header set",
			run: func(ctx context.Context, e *Env) error {
				return headerProbe(ctx, e, "Client.Publish with a header set", func(m *mq.Message) {
					m.Headers.Set("content-kind", []byte("order"))
				}, func(d *mq.Delivery) error {
					v, ok := d.Headers.Get("content-kind")
					if !ok || string(v) != "order" {
						return failf("the header came back as %q (present %v)", v, ok)
					}
					return nil
				})
			},
		},
		mq.FeatureBinaryHeaders: {
			refusal: "Client.Publish with a binary header value",
			run: func(ctx context.Context, e *Env) error {
				binary := []byte{0x00, 0xff, 0xfe, 0x80}
				return headerProbe(ctx, e, "Client.Publish with a binary header value", func(m *mq.Message) {
					m.Headers.Set("trace-state", binary)
				}, func(d *mq.Delivery) error {
					v, ok := d.Headers.Get("trace-state")
					if !ok || !bytes.Equal(v, binary) {
						return failf("the binary header came back as %q (present %v)", v, ok)
					}
					return nil
				})
			},
		},
		mq.FeatureDuplicateHeaders: {
			refusal: "Client.Publish with two values under one header key",
			run: func(ctx context.Context, e *Env) error {
				return headerProbe(ctx, e, "Client.Publish with two values under one header key", func(m *mq.Message) {
					m.Headers.Add("tag", []byte("one"))
					m.Headers.Add("tag", []byte("two"))
				}, func(d *mq.Delivery) error {
					vals := d.Headers.Values("tag")
					if len(vals) != 2 || string(vals[0]) != "one" || string(vals[1]) != "two" {
						return failf("the duplicate header came back as %d values: the specification keeps the duplicates and the insertion order", len(vals))
					}
					return nil
				})
			},
		},
		mq.FeatureBatchPublish: {
			refusal: "PublisherHandle.PublishBatch",
			run: func(ctx context.Context, e *Env) error {
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = p.Close() }()
				return refusedAt("PublisherHandle.PublishBatch",
					p.PublishBatch(ctx, []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}))
			},
		},
		mq.FeatureBatchReceive: {
			noRefusal: "a Receive with room for several is accepted by every driver -- lets one return fewer -- so the difference shows up only in how many arrive",
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 32})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = p.Close() }()
				if err := publishN(ctx, p, 8, "batch-recv-"); err != nil {
					return err
				}
				if err := e.Quiesce(ctx); err != nil {
					return err
				}
				dst := make([]mq.Delivery, 8)
				bctx, cancel := context.WithTimeout(ctx, e.Timeout())
				defer cancel()
				n, rerr := c.Receive(bctx, dst)
				for i := range n {
					_ = dst[i].Ack()
				}
				switch {
				case n >= 2:
					return nil
				case n == 0:
					// Nothing at all is a probe that could not
					// run, not a broker without batching: the
					// eight publications should have produced
					// at least one delivery.
					return failf("one Receive with room for 8 filled none of them, so the eight publications never arrived: %v", rerr)
				default:
					return Diverged("one Receive with room for 8 was accepted and filled %v of them: the driver takes the batch request and hands deliveries out one at a time, which is what having no batch receive looks like (%v)", n, rerr)
				}
			},
		},
		mq.FeatureExtendLease: {
			refusal: "Delivery.Extend",
			shape:   func(n *Need) { n.AckDeadline = 10 * time.Second },
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("slow")}); err != nil {
					return err
				}
				return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
					before := d.Info().LeaseExpires
					// By the lease the shape asks for, which is what the
					// lease scheduler asks for. Asking for more than the
					// lease required a grant a broker that can only restart
					// its deadline (JetStream's +WPI, Redis's XCLAIM) cannot
					// give, so a driver that honestly refused it, as the
					// MaxLease rule below demands, failed here instead.
					if err := refusedAt("Delivery.Extend", d.Extend(10*time.Second)); err != nil {
						return err
					}
					if !before.IsZero() && d.Settled() {
						return failf("Extend settled the delivery: the specification says an extension is not a settlement")
					}
					// An extension by nothing is a mistake, not a request.
					if err := d.Extend(0); !hasCode(err, mq.CodeInvalidArgument) {
						return failf("Extend(0) reported %v (code %v), want InvalidArgument", err, mq.CodeOf(err))
					}
					// An extension past the advertised ceiling has to be
					// REFUSED and not quietly shortened.
					//
					// MaxLease is what the scheduler reads, and
					// `DeadlineExtender.Extend` returns only an error: a
					// driver that clamped silently would hand back a
					// shorter lease than the caller asked for, the
					// scheduler would go on assuming the documented
					// minimum, and the first thing anyone would learn
					// about it is a redelivery of a message still being
					// handled. All three capped drivers refuse today and
					// nothing required it.
					//
					// Only when the limit is KNOWN: an unknown ceiling is
					// not a claim, and asking a driver that advertises
					// none to refuse an arbitrary duration would be
					// inventing a limit for it.
					if lim := e.Caps.Limits().MaxLease; lim.Known && lim.Value > 0 {
						if err := d.Extend(lim.Value + time.Second); err == nil {
							return failf("Extend(%v) was accepted, which is longer than the advertised "+
								"MaxLease of %v. Extend returns only an error, so a shorter grant is "+
								"invisible: the lease scheduler goes on assuming the documented minimum "+
								"and the caller learns of the gap when the message is redelivered "+
								"underneath it", lim.Value+time.Second, lim.Value)
						}
					}
					return d.Ack()
				})
			},
		},
		mq.FeatureDeduplication: {
			refusal: "Client.Publish with a DedupKey option",
			shape:   dedupShape, run: dedupProbe,
		},
		mq.FeatureExactlyOnceDelivery: {
			noRefusal: "a delivery multiplicity is a property of what arrives, not a call: there is nothing for a driver that redelivers to refuse",
			shape:     exactlyOnceShape, run: exactlyOnceProbe,
		},
		mq.FeatureAdmin: {
			refusal: "Client.Admin",
			run: func(ctx context.Context, e *Env) error {
				admin, err := e.Client.Admin()
				if err != nil {
					return refusedAt("Client.Admin", err)
				}
				if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: e.Topic}); err != nil {
					return failf("EnsureTopic is not idempotent: %v", err)
				}
				if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: e.Topic, Name: e.Sub}); err != nil {
					return failf("EnsureSubscription: %v", err)
				}
				if _, err := admin.Stats(ctx, e.Topic, e.Sub); err != nil && !errors.Is(err, mq.ErrUnsupported) {
					return failf("Stats: %v", err)
				}
				for name, err := range admin.ListTopics(ctx) {
					if err != nil {
						return failf("ListTopics: %v", err)
					}
					_ = name
				}
				// The destructive operations are explicit and may be
				// unsupported, but they may not silently do nothing.
				if err := admin.Purge(ctx, e.Topic, e.Sub); err != nil && !errors.Is(err, mq.ErrUnsupported) {
					return failf("Purge: %v", err)
				}
				if err := admin.DeleteTopic(ctx, e.Topic); err != nil && !errors.Is(err, mq.ErrUnsupported) {
					return failf("DeleteTopic: %v", err)
				}
				return nil
			},
		},
		mq.FeaturePublisherConfirm: {
			refusal: "Client.Publisher with Confirmation set to ConfirmAccepted",
			run: func(ctx context.Context, e *Env) error {
				c, err := e.Consumer(ctx, mq.ConsumerConfig{})
				if err != nil {
					return err
				}
				defer func() { _ = c.Close() }()
				p, err := e.Publisher(ctx, mq.PublisherConfig{Confirmation: mq.ConfirmAccepted})
				if err != nil {
					return refusedAt("Client.Publisher with Confirmation set to ConfirmAccepted", err)
				}
				defer func() { _ = p.Close() }()
				if err := p.Publish(ctx, &mq.Message{Body: []byte("confirmed")}); err != nil {
					return err
				}
				// A confirmed publication is at the broker when Publish
				// returns, so it is already there to be consumed.
				return oneDelivery(ctx, c, e.Timeout(), ack)
			},
		},
		mq.FeatureReceipts: {
			refusal: "Client.Publisher with Receipts",
			run: func(ctx context.Context, e *Env) error {
				// the receipt, and it IS reachable through the
				// portable API: the publisher asks for one and the
				// storage is the caller's, pointed at by
				// Message.Receipt. A driver that does not
				// advertise the feature is refused at
				// construction, which is the negative direction.
				//
				// The probe asks only for what the specification promises:
				// something the broker SAID. A receipt with every
				// field empty is a receipt the driver did not
				// fill, and a caller correlating a publication
				// with a broker-side identifier would have
				// nothing.
				p, err := e.Publisher(ctx, mq.PublisherConfig{Receipts: true})
				if err != nil {
					return refusedAt("Client.Publisher with Receipts", err)
				}
				defer func() { _ = p.Close() }()
				// TWO publications under one key, and the SECOND is the
				// one asserted on.
				//
				// "Every field is zero" is the only portable way to ask
				// whether a driver filled the receipt at all, and on a
				// log-shaped broker the first message of a fresh topic
				// legitimately answers offset 0 on partition 0 with no
				// timestamp the client library exposes. That is a
				// correct receipt which this probe read as an empty one,
				// and it failed `drivers/kafka/kafkago` for answering
				// truthfully. One key so that both publications land on
				// one partition: the second is then at a non-zero offset
				// wherever offsets are what the broker says.
				// The key is used only where a key MEANS something. A
				// driver that does not order by key may refuse one or
				// ignore it, and its receipt is an identifier or a
				// sequence rather than an offset anyway -- so the
				// keyed form is for exactly the log-shaped brokers
				// whose first offset is zero.
				var key []byte
				if e.Caps.Support(mq.FeatureOrderedByKey).Level != mq.Unsupported {
					key = []byte("receipt-probe")
				}
				if err := p.Publish(ctx, &mq.Message{Key: key, Body: []byte("receipted-first")}); err != nil {
					return err
				}
				var receipt mq.PublishReceipt
				m := &mq.Message{Key: key, Body: []byte("receipted"), Receipt: &receipt}
				if err := p.Publish(ctx, m); err != nil {
					return err
				}
				if receipt.MessageID == "" && receipt.Sequence == 0 &&
					receipt.Offset == 0 && receipt.ConfirmedAt.IsZero() {
					return failf("a publisher opened with Receipts filled nothing into the SECOND publication's receipt: the receipt is what the BROKER said about the publication, and a caller correlating one with a broker-side identifier has nothing. The second is asserted on because the first can legitimately be all zeros")
				}
				return nil
			},
		},
		mq.FeatureDurableConfirm: {
			refusal: "Client.Publisher with Confirmation set to ConfirmDurable",
			run: func(ctx context.Context, e *Env) error {
				const call = "Client.Publisher with Confirmation set to ConfirmDurable"
				p, err := e.Client.Publisher(ctx, mq.PublisherConfig{Topic: e.Topic, Confirmation: mq.ConfirmDurable})
				if err != nil {
					return refusedAt(call, err)
				}
				defer func() { _ = p.Close() }()
				return refusedAt(call, p.Publish(ctx, &mq.Message{Body: []byte("durable")}))
			},
		},
	}
}

// wildcardProbe opens a subscription on the pattern the harness writes for the
// scenario topic and checks that a publication to the concrete topic arrives
// there, reporting the concrete topic rather than the pattern.
//
// A driver with no Wildcard function skips: a claim nobody could check stays
// unverified, which is the only honest answer and is what the kit does with
// every feature it has no probe for.
func wildcardProbe(ctx context.Context, e *Env) error {
	if e.Harness.Wildcard == nil {
		return Skip("the harness does not say how to write a subscription pattern for this broker, so the claim stays unverified rather than being taken on trust")
	}
	pattern := e.Harness.Wildcard(e.Topic)
	if pattern == "" || pattern == e.Topic {
		return Skip("the harness produced no pattern for this topic")
	}
	c, err := e.Client.Consumer(ctx, mq.ConsumerConfig{Topic: pattern, Subscription: e.Sub})
	if err != nil {
		return refusedAt("Client.Consumer on a subscription pattern", err)
	}
	defer func() { _ = c.Close() }()
	if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("wild")}); err != nil {
		return err
	}
	err = oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
		if string(d.Body) != "wild" {
			return failf("the pattern subscription received %q", d.Body)
		}
		// The specification: a delivery reports the concrete topic it arrived on, not
		// the pattern the subscription named. A driver that reported the
		// pattern would make a fan-in subscriber unable to tell where a
		// message came from, which is the whole point of a wildcard.
		if d.Info().Topic != e.Topic {
			return failf("the delivery metadata reports topic %v, want the resolved %v", d.Info().Topic, e.Topic)
		}
		return ack(d)
	})
	if isNoDelivery(err) {
		return Diverged("the pattern %s was accepted as a subscription topic and nothing published to %s arrived on it: the driver took the pattern as a literal name rather than matching it (%v)",
			strconv.Quote(pattern), strconv.Quote(e.Topic), err)
	}
	return err
}

// orderingHasNoCall is why the three ordering claims have no refusal point:
// an ordering is not requested, it is either kept or not, so there is nothing
// for a driver that lacks one to refuse. It is the same reason they are
// [KindGuarantee].
const orderingHasNoCall = "an ordering is never requested through a call: it is either kept or not, so a driver that lacks one has nothing to refuse"

// orderedShape asks for the one-lane topology an ordering claim is about.
func orderedShape(n *Need) { n.Partitions = 1 }

// orderedProbe publishes a run of keyed messages and checks that they arrive in
// the order they were published, which is what every ordering claim means within
// its own scope.
func orderedProbe(ctx context.Context, e *Env) error {
	c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 32})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	p, err := e.Publisher(ctx, mq.PublisherConfig{})
	if err != nil {
		return err
	}
	defer func() { _ = p.Close() }()
	const n = 32
	key := []byte("one-key")
	for i := range n {
		if err := p.Publish(ctx, &mq.Message{Key: key, Body: []byte(strconv.Itoa(i))}); err != nil {
			return err
		}
	}
	want := 0
	_, err = collectAck(ctx, c, n, e.Timeout(), func(d *mq.Delivery) error {
		got, cerr := strconv.Atoi(string(d.Body))
		if cerr != nil {
			return failf("a delivery's body is not one this probe published: %q", d.Body)
		}
		if got != want {
			// A VIOLATION rather than a failure: the same
			// observation refutes a claim of ordering and confirms
			// a denial of it, and only the scenario knows which of
			// the two it is asking.
			return violation("message %d arrived where %d was expected, so the delivery order is not the publication order", got, want)
		}
		want++
		return nil
	})
	return err
}

// dedupShape asks for a de-duplication window.
func dedupShape(n *Need) { n.DedupWindow = time.Minute }

// dedupProbe publishes the same dedup key twice and checks that one copy is
// discarded, which is what both the de-duplication and the exactly-once-delivery
// claims rest on.
func dedupProbe(ctx context.Context, e *Env) error {
	c, err := e.Consumer(ctx, mq.ConsumerConfig{Prefetch: 8})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	for range 2 {
		if err := refusedAt("Client.Publish with a DedupKey option",
			e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("dup")}, mq.DedupKey("k-1"))); err != nil {
			return err
		}
	}
	if err := e.Quiesce(ctx); err != nil {
		return err
	}
	if err := oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error { return d.Ack() }); err != nil {
		return err
	}
	if err := expectNothing(ctx, c, time.Second, "both publications of one dedup key were delivered"); err != nil {
		if isUnexpectedArrival(err) {
			return Diverged("the DedupKey was ACCEPTED on both publications and both were delivered: the driver takes the key and does not discard the duplicate, which is what having no broker-side de-duplication looks like (%v)", err)
		}
		return err
	}
	return nil
}

// exactlyOnceShape asks for a lease short enough to expire inside a scenario.
//
// It deliberately asks for NOTHING else. In particular it does not ask for a
// de-duplication window: this probe publishes once, so there is no duplicate
// publication for a window to discard, and a Need a scenario does not need is
// a Need a driver might refuse for no reason.
func exactlyOnceShape(n *Need) { n.AckDeadline = 500 * time.Millisecond }

// exactlyOnceProbe is the DELIVERY half of exactly-once, which is not what a
// de-duplication key proves.
//
// What this does instead is take one delivery and NOT settle it, let the lease
// run out, and look for the message again. A second delivery of one
// publication is a counter-example to exactly-once delivery and nothing else;
// no second delivery is not a proof, because a guarantee's absence is not
// observable in one run, which is why this returns a violation rather than an
// error and why the feature is a [KindGuarantee].
//
// It publishes WITHOUT a DedupKey, and that is the whole point restated. The
// core refuses a DedupKey from a driver that does not advertise
// FeatureDeduplication, so a probe that sent one got ErrUnsupported from four
// of the nine suites and read it as proof of the exactly-once denial -- which
// is evidence about de-duplication and is the very conflation this probe
// exists to end. The first version of this function did exactly that; the
// reports it wrote all said "the denial is confirmed the strong way: there is
// a call and the driver refused it", and the call they had refused was the
// other feature's.
func exactlyOnceProbe(ctx context.Context, e *Env) error {
	c, err := e.Consumer(ctx, mq.ConsumerConfig{})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	if err := e.Client.Publish(ctx, e.Topic, &mq.Message{Body: []byte("eod")}); err != nil {
		return err
	}
	first := make([]mq.Delivery, 1)
	bctx, cancel := context.WithTimeout(ctx, e.Timeout())
	defer cancel()
	n, rerr := c.Receive(bctx, first)
	if n != 1 {
		return Skip("the probe's one publication never arrived, so there is no delivery to see twice: " + render(rerr))
	}
	// Deliberately unsettled: the whole question is what the broker does
	// with a delivery nobody completed.
	if err := e.Advance(ctx, 3*time.Second); err != nil {
		return err
	}
	if err := e.Quiesce(ctx); err != nil {
		return err
	}
	second := make([]mq.Delivery, 1)
	sctx, scancel := context.WithTimeout(ctx, 2*time.Second)
	defer scancel()
	got, _ := c.Receive(sctx, second)
	if got == 0 {
		_ = ack(&first[0])
		return nil
	}
	body := string(second[0].Body)
	_ = ack(&second[0])
	_ = ack(&first[0])
	if body != "eod" {
		return Skip("a second delivery arrived carrying " + strconv.Quote(body) + ", which this probe did not publish")
	}
	return violation("the one publication %s was delivered a second time once its lease expired, so a consumer can see it twice", strconv.Quote(body))
}

// headerProbe publishes a message the setter decorates and checks the delivery
// with the verifier, which is the shape of all three header claims.
func headerProbe(ctx context.Context, e *Env, call string, set func(*mq.Message), check func(*mq.Delivery) error) error {
	c, err := e.Consumer(ctx, mq.ConsumerConfig{})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	m := &mq.Message{Body: []byte("headers")}
	set(m)
	if err := refusedAt(call, e.Client.Publish(ctx, e.Topic, m)); err != nil {
		return err
	}
	return oneDelivery(ctx, c, e.Timeout(), func(d *mq.Delivery) error {
		if err := check(d); err != nil {
			// The publication was ACCEPTED and the header did not
			// survive it. A driver that cannot carry the header is
			// required to refuse the publication, and one
			// that takes it and loses it has done neither -- but
			// from outside there is no way to tell that from a
			// broker that has no such header at all, so the
			// negative direction leaves the denial unrefuted and
			// the positive one fails.
			_ = ack(d)
			return Diverged("the publication carrying the header was accepted and %v", err)
		}
		return ack(d)
	})
}
