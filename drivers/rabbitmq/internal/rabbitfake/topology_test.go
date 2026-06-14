package rabbitfake_test

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// The topology half of the fake: the refusals, the routing rules and the
// overflow policies.
//
// These are the parts of the model a driver test never reaches, because a
// driver that is working never asks for any of them -- and they are exactly
// the parts a driver test RELIES on being right. A fake that accepted a
// declaration the broker refuses certifies a driver against its own mistake.

// codeOf is the AMQP code of a channel exception, or 0.
func codeOf(err error) int {
	var ae *amqp.Error
	if errors.As(err, &ae) {
		return ae.Code
	}
	return 0
}

// TestTheDefaultExchangeCannotBeDeclaredOrDeleted.
//
// It is not a real exchange: it is the implicit binding of every queue to its
// own name. RabbitMQ answers ACCESS_REFUSED, and a fake that allowed it would
// let a driver declare it and pass here while failing everywhere else.
func TestTheDefaultExchangeCannotBeDeclaredOrDeleted(t *testing.T) {
	t.Parallel()
	_, ch := setup(t, rabbitfake.Config{})

	if err := ch.ExchangeDeclare("", amqp.ExchangeDirect, true, false, false, false, nil); codeOf(err) != amqp.AccessRefused {
		t.Errorf("declaring the default exchange is %v (code %d), want ACCESS_REFUSED", err, codeOf(err))
	}
	_, ch2 := setup(t, rabbitfake.Config{})
	if err := ch2.ExchangeDelete("", false, false); codeOf(err) != amqp.AccessRefused {
		t.Errorf("deleting the default exchange is %v (code %d), want ACCESS_REFUSED", err, codeOf(err))
	}
}

// TestTheReservedPrefixIsRefusedForAnExchangeToo.
//
// `amq.` is the broker's own namespace. The driver's naming layer refuses to
// derive a name in it, and this is the refusal that makes that rule worth
// having rather than merely cautious.
func TestTheReservedPrefixIsRefusedForAnExchangeToo(t *testing.T) {
	t.Parallel()
	_, ch := setup(t, rabbitfake.Config{})

	err := ch.ExchangeDeclare("amq.mine", amqp.ExchangeDirect, true, false, false, false, nil)
	if codeOf(err) != amqp.AccessRefused {
		t.Errorf("declaring an exchange under the reserved prefix is %v (code %d), want ACCESS_REFUSED",
			err, codeOf(err))
	}
}

// TestAnUnknownExchangeTypeIsAPreconditionFailure, and the delayed-message
// type is unknown unless the plugin is configured -- which is what makes
// [TestDelayedMessageExchangeNeedsThePlugin]'s subject a real deployment
// question rather than a flag.
func TestAnUnknownExchangeTypeIsAPreconditionFailure(t *testing.T) {
	t.Parallel()
	_, ch := setup(t, rabbitfake.Config{})

	err := ch.ExchangeDeclare("weird", "x-not-a-type", true, false, false, false, nil)
	if codeOf(err) != amqp.PreconditionFailed {
		t.Errorf("declaring an exchange of an unknown type is %v (code %d), want PRECONDITION_FAILED",
			err, codeOf(err))
	}
}

// TestPassiveExchangeDeclareIsANotFoundForOneThatIsNotThere.
//
// A passive declare asks "is it there", and the answer for one that is not is
// a 404 rather than a creation -- which is the whole difference between
// `ExchangeDeclarePassive` and `ExchangeDeclare`.
func TestPassiveExchangeDeclareIsANotFoundForOneThatIsNotThere(t *testing.T) {
	t.Parallel()
	_, ch := setup(t, rabbitfake.Config{})

	if err := ch.ExchangeDeclarePassive("never-declared", amqp.ExchangeDirect, true, false, false, false, nil); codeOf(err) != amqp.NotFound {
		t.Errorf("a passive declare of a missing exchange is %v (code %d), want NOT_FOUND", err, codeOf(err))
	}

	_, ch2 := setup(t, rabbitfake.Config{})
	if err := ch2.ExchangeDeclare("here", amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Fatalf("declaring: %v", err)
	}
	if err := ch2.ExchangeDeclarePassive("here", amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Errorf("a passive declare of an exchange that IS there is %v, want nil", err)
	}
}

// TestDeletingAnExchangeInUseIsRefusedWhenIfUnusedIsSet.
//
// `ifUnused` is the caller saying "only if nothing is bound", and a fake that
// ignored it would let a teardown pass that a real broker refuses.
func TestDeletingAnExchangeInUseIsRefusedWhenIfUnusedIsSet(t *testing.T) {
	t.Parallel()
	_, ch := setup(t, rabbitfake.Config{})
	if err := ch.ExchangeDeclare("x", amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Fatalf("declaring the exchange: %v", err)
	}
	if err := ch.QueueBind("q", "k", "x", false, nil); err != nil {
		t.Fatalf("binding: %v", err)
	}

	if err := ch.ExchangeDelete("x", true, false); codeOf(err) != amqp.PreconditionFailed {
		t.Errorf("deleting a bound exchange with ifUnused is %v (code %d), want PRECONDITION_FAILED",
			err, codeOf(err))
	}
	_, ch2 := setup(t, rabbitfake.Config{})
	if err := ch2.ExchangeDeclare("y", amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Fatalf("declaring: %v", err)
	}
	if err := ch2.ExchangeDelete("y", true, false); err != nil {
		t.Errorf("deleting an UNbound exchange with ifUnused is %v, want nil", err)
	}
}

// TestDeletingAQueueIsRefusedWhenItIsInUseOrNotEmpty.
//
// Both flags exist so a teardown can say "only if nobody is reading" and
// "only if nothing is waiting". A fake that ignored either would let a
// destructive test pass that a real broker would have stopped.
func TestDeletingAQueueIsRefusedWhenItIsInUseOrNotEmpty(t *testing.T) {
	t.Parallel()
	b, ch := setup(t, rabbitfake.Config{})

	if err := ch.PublishWithContext(t.Context(), "", "q", false, false, amqp.Publishing{Body: []byte("x")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	if _, err := ch.QueueDelete("q", false, true, false); codeOf(err) != amqp.PreconditionFailed {
		t.Errorf("deleting a non-empty queue with ifEmpty is %v (code %d), want PRECONDITION_FAILED",
			err, codeOf(err))
	}

	// A channel exception CLOSES the channel, exactly as it does on the
	// wire, so the second half needs one of its own.
	_, ch2 := setup(t, rabbitfake.Config{})
	if _, err := ch2.ConsumeWithContext(t.Context(), "q", "", true, false, false, false, nil); err != nil {
		t.Fatalf("consuming: %v", err)
	}
	if _, err := ch2.QueueDelete("q", true, false, false); codeOf(err) != amqp.PreconditionFailed {
		t.Errorf("deleting a queue with a consumer and ifUnused is %v (code %d), want PRECONDITION_FAILED",
			err, codeOf(err))
	}
	_ = b
}

// TestAServerNamedQueueGetsAGeneratedName, and a PASSIVE declare of the empty
// name is a 404 rather than a generated one.
//
// The empty name is the client asking the broker to choose, which is how
// every exclusive reply queue in the AMQP world is made. A passive declare
// cannot mean that -- there is nothing to look up -- so it is refused.
func TestAServerNamedQueueGetsAGeneratedName(t *testing.T) {
	t.Parallel()
	_, ch := setup(t, rabbitfake.Config{})

	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declaring a server-named queue: %v", err)
	}
	if q.Name == "" {
		t.Error("the broker generated no name for a server-named queue")
	}

	if _, err := ch.QueueDeclarePassive("", false, true, true, false, nil); codeOf(err) != amqp.NotFound {
		t.Errorf("a passive declare of the empty name is %v (code %d), want NOT_FOUND: there is nothing to "+
			"look up, so it cannot mean \"choose one for me\"", err, codeOf(err))
	}
}

// TestAnExclusiveQueueIsLockedToItsConnection.
//
// It is what a reply queue relies on: another connection must not be able to
// consume from it, and the refusal is RESOURCE_LOCKED rather than a silent
// share.
func TestAnExclusiveQueueIsLockedToItsConnection(t *testing.T) {
	t.Parallel()
	b := newBroker(t, rabbitfake.Config{})
	first := openChannel(t, dial(t, b))
	if _, err := first.QueueDeclare("mine", false, false, true, false, nil); err != nil {
		t.Fatalf("declaring the exclusive queue: %v", err)
	}

	second := openChannel(t, dial(t, b))
	_, err := second.ConsumeWithContext(t.Context(), "mine", "", true, false, false, false, nil)
	if codeOf(err) != amqp.ResourceLocked {
		t.Errorf("a second connection consuming an exclusive queue is %v (code %d), want RESOURCE_LOCKED",
			err, codeOf(err))
	}
}

// TestBindingToAnExchangeThatIsNotThereIsANotFound, and the default exchange
// cannot be bound to at all.
//
// Binding to the default exchange is refused because the binding is implicit:
// allowing an explicit one would create a second, contradictory route.
func TestBindingToAnExchangeThatIsNotThereIsANotFound(t *testing.T) {
	t.Parallel()
	_, ch := setup(t, rabbitfake.Config{})

	if err := ch.QueueBind("q", "k", "never-declared", false, nil); codeOf(err) != amqp.NotFound {
		t.Errorf("binding to a missing exchange is %v (code %d), want NOT_FOUND", err, codeOf(err))
	}
	_, ch2 := setup(t, rabbitfake.Config{})
	if err := ch2.QueueBind("q", "k", "", false, nil); codeOf(err) != amqp.AccessRefused {
		t.Errorf("binding to the default exchange is %v (code %d), want ACCESS_REFUSED: the binding is "+
			"implicit, and an explicit one would be a second contradictory route", err, codeOf(err))
	}
}

// TestAQueueArgumentOutOfRangeIsAPreconditionFailure.
//
// Each of these is a number the broker reads out of the declaration, and one
// it cannot USE is a declaration it refuses rather than a default it
// substitutes -- the difference between a queue with the bound you asked for
// and one with no bound at all.
//
// A value of the wrong TYPE is a different case and is NOT asserted here: the
// fake ignores it silently where a real broker answers PRECONDITION_FAILED
// with `unacceptable_type`. That divergence is recorded in the maintenance notes
// rather than guessed at, because settling it needs a container and no driver
// can currently reach it -- every one of these arguments is built from typed
// configuration.
func TestAQueueArgumentOutOfRangeIsAPreconditionFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		arg   string
		value any
	}{
		{"x-expires", int32(-1)},
		{"x-max-length", int32(-1)},
		{"x-max-priority", int32(-1)},
		{"x-max-priority", int32(256)},
	} {
		_, ch := setup(t, rabbitfake.Config{})
		_, err := ch.QueueDeclare("bad", false, false, false, false, amqp.Table{tc.arg: tc.value})
		if codeOf(err) != amqp.PreconditionFailed {
			t.Errorf("declaring a queue with %s=%v is %v (code %d), want PRECONDITION_FAILED",
				tc.arg, tc.value, err, codeOf(err))
		}
	}
}

// TestAnUnknownOverflowPolicyIsRefused, and the three known ones are not.
func TestAnUnknownOverflowPolicyIsRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		policy string
		ok     bool
	}{
		{amqp.QueueOverflowDropHead, true},
		{amqp.QueueOverflowRejectPublish, true},
		{amqp.QueueOverflowRejectPublishDLX, true},
		{"discard-at-random", false},
	} {
		_, ch := setup(t, rabbitfake.Config{})
		_, err := ch.QueueDeclare("ovf", false, false, false, false, amqp.Table{"x-overflow": tc.policy})
		if tc.ok && err != nil {
			t.Errorf("declaring a queue with x-overflow=%q is %v, want nil", tc.policy, err)
		}
		if !tc.ok && codeOf(err) != amqp.PreconditionFailed {
			t.Errorf("declaring a queue with x-overflow=%q is %v (code %d), want PRECONDITION_FAILED",
				tc.policy, err, codeOf(err))
		}
	}
}

// TestRejectPublishKeepsTheOldestAndDropHeadKeepsTheNewest.
//
// These are opposite answers to the same question and a driver cannot tell
// them apart from the publish side: `reject-publish` refuses the new message
// and `drop-head` accepts it by discarding the oldest. A fake that
// implemented one for both would make a max-length test prove the wrong
// thing.
func TestRejectPublishKeepsTheOldestAndDropHeadKeepsTheNewest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		policy string
		want   string
	}{
		{amqp.QueueOverflowRejectPublish, "first"},
		{amqp.QueueOverflowDropHead, "second"},
	} {
		b, ch := setup(t, rabbitfake.Config{})
		if _, err := ch.QueueDeclare("bounded", false, false, false, false,
			amqp.Table{"x-max-length": int32(1), "x-overflow": tc.policy}); err != nil {
			t.Fatalf("declaring the bounded queue: %v", err)
		}
		for _, body := range []string{"first", "second"} {
			if err := ch.PublishWithContext(t.Context(), "", "bounded", false, false,
				amqp.Publishing{Body: []byte(body)}); err != nil {
				t.Fatalf("publishing %s: %v", body, err)
			}
		}

		deliveries, err := ch.ConsumeWithContext(t.Context(), "bounded", "", true, false, false, false, nil)
		if err != nil {
			t.Fatalf("consuming: %v", err)
		}
		select {
		case d := <-deliveries:
			if got := string(d.Body); got != tc.want {
				t.Errorf("with x-overflow=%q the queue kept %q, want %q", tc.policy, got, tc.want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("with x-overflow=%q nothing was delivered", tc.policy)
		}
		_ = b
	}
}

// TestAHeadersExchangeMatchesOnAllOrAnyAsAsked.
//
// `x-match` is the whole of a headers exchange, and the default is `all`. A
// binding whose arguments are all `x-`-prefixed matches NOTHING, because
// there is nothing left to compare -- which is the case that reads like a
// bug and is the broker's rule.
func TestAHeadersExchangeMatchesOnAllOrAnyAsAsked(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		bind    amqp.Table
		headers amqp.Table
		want    bool
	}{
		{"all matches when both do", amqp.Table{"a": "1", "b": "2"}, amqp.Table{"a": "1", "b": "2"}, true},
		{"all fails when one does not", amqp.Table{"a": "1", "b": "2"}, amqp.Table{"a": "1", "b": "x"}, false},
		{"all fails when one is absent", amqp.Table{"a": "1", "b": "2"}, amqp.Table{"a": "1"}, false},
		{"any matches on one", amqp.Table{"x-match": "any", "a": "1", "b": "2"}, amqp.Table{"b": "2"}, true},
		{"any fails on none", amqp.Table{"x-match": "any", "a": "1"}, amqp.Table{"c": "3"}, false},
		{"only x- arguments match nothing", amqp.Table{"x-match": "all"}, amqp.Table{"a": "1"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, ch := setup(t, rabbitfake.Config{})
			if err := ch.ExchangeDeclare("h", amqp.ExchangeHeaders, true, false, false, false, nil); err != nil {
				t.Fatalf("declaring the headers exchange: %v", err)
			}
			if err := ch.QueueBind("q", "", "h", false, tc.bind); err != nil {
				t.Fatalf("binding: %v", err)
			}
			if err := ch.PublishWithContext(t.Context(), "h", "", false, false,
				amqp.Publishing{Headers: tc.headers, Body: []byte("x")}); err != nil {
				t.Fatalf("publishing: %v", err)
			}

			q, err := ch.QueueDeclarePassive("q", true, false, false, false, nil)
			if err != nil {
				t.Fatalf("reading the queue back: %v", err)
			}
			if got := q.Messages > 0; got != tc.want {
				t.Errorf("the publication routed=%t, want %t", got, tc.want)
			}
		})
	}
}

// TestAQueueThatNobodyUsesExpires.
//
// `x-expires` is how a reply queue cleans itself up, and time here moves only
// when [Broker.Advance] says so: a fake that expired on a wall clock would
// make every test that declares one flaky.
func TestAQueueThatNobodyUsesExpires(t *testing.T) {
	t.Parallel()
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	if _, err := ch.QueueDeclare("ephemeral", false, false, false, false,
		amqp.Table{"x-expires": int32(1000)}); err != nil {
		t.Fatalf("declaring: %v", err)
	}
	if _, err := ch.QueueDeclarePassive("ephemeral", false, false, false, false, nil); err != nil {
		t.Fatalf("the queue is not there before the clock moved: %v", err)
	}

	b.Advance(2 * time.Second)
	// Any operation makes the broker sweep; a passive declare is the
	// smallest one that reports what it found.
	if _, err := ch.QueueDeclarePassive("ephemeral", false, false, false, false, nil); codeOf(err) != amqp.NotFound {
		t.Errorf("a queue with x-expires=1s is still there two seconds later: %v (code %d), want NOT_FOUND",
			err, codeOf(err))
	}
}
