package redis_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// TestNackStrategiesRedeliverThroughTheSPI drives every arm through the SPI, which is the only place their differences become the
// caller's problem.
//
// The command layer's own model test (internal/streams) holds each strategy
// to the contract; this holds the DRIVER to it — the plan reaching the server,
// the redelivery reaching Fetch, and the delay being refused by the arms that
// cannot honour it.
func TestNackStrategiesRedeliverThroughTheSPI(t *testing.T) {
	for _, kind := range streams.Kinds() {
		t.Run(kind.String(), func(t *testing.T) {
			r := newRig(t, func(c *rd.Config) {
				c.Nack = kind
				c.ClaimInterval = 10 * time.Millisecond
				c.MoverInterval = 10 * time.Millisecond
			})
			sink, src := r.openPair(t)
			if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("payload")}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, r.ctx, src)
			if err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{}); err != nil {
				t.Fatalf("Nack: %v", err)
			}
			// The pending kinds need the lease to run out before the sweep
			// will take the entry back; the re-adding kinds are deliverable
			// at once.
			if !strategyReorders(kind) {
				r.srv.Advance(testLease + time.Second)
			}
			again := fetchOne(t, r.ctx, src)
			if string(again.Message.Body) != "payload" {
				t.Fatalf("the redelivered body is %q", again.Message.Body)
			}
			if again.Info.Attempt < 2 {
				t.Fatalf("the redelivered message reports attempt %d, want at least 2", again.Info.Attempt)
			}
			if err := src.Settler().Ack(r.ctx, &again); err != nil {
				t.Fatalf("Ack of the redelivered message: %v", err)
			}
		})
	}
}

// strategyReorders mirrors the driver's own pairing, so this test knows which
// arms re-add.
func strategyReorders(k streams.Kind) bool {
	switch k.Resolve() {
	case streams.ZSetMover, streams.ReAdd:
		return true
	default:
		return false
	}
}

// TestNackDelayIsHonouredOrRefused is the rule at the SPI: a driver that
// cannot honour a nack's action returns ErrUnsupported and settles nothing.
//
// The "settles nothing" half is what makes it matter. A refusal that had
// already acknowledged the entry would leave the caller with an error and the
// message gone.
func TestNackDelayIsHonouredOrRefused(t *testing.T) {
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	for _, kind := range streams.Kinds() {
		t.Run(kind.String(), func(t *testing.T) {
			r := newRig(t, func(c *rd.Config) {
				c.Nack = kind
				c.MoverInterval = 10 * time.Millisecond
			})
			sink, src := r.openPair(t)
			if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, r.ctx, src)
			err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{Delay: time.Hour})
			if kind.Resolve() == streams.ZSetMover {
				if err != nil {
					t.Fatalf("the zset strategy refused a one-hour delay: %v", err)
				}
				if n := r.srv.ZLen(keys.Delayed(0)); n != 1 {
					t.Fatalf("the delayed set holds %d members, want 1", n)
				}
				return
			}
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("%v accepted a one-hour delay it cannot honour: %v", kind, err)
			}
			// Nothing was settled: the entry is still pending, so the
			// delivery is still settleable and the message is not lost.
			pending := r.srv.Pending(keys.Stream(0), testSub)
			if len(pending) != 1 {
				t.Fatalf("the refused nack settled the entry anyway (%d pending)", len(pending))
			}
			if err := src.Settler().Ack(r.ctx, &d); err != nil {
				t.Fatalf("the delivery is not settleable after a refused nack: %v", err)
			}
		})
	}
}

// TestDeadLetterMoveIsAtomic is the Lua move's whole reason: the copy and the
// acknowledgement are one step.
//
// The assertion that makes it mean something is the pair: the message is in
// the dead-letter stream AND gone from the source's pending list, after ONE
// command. A two-command implementation would satisfy both of those and fail
// the command count, which is what a crash between them would expose in
// production.
func TestDeadLetterMoveIsAtomic(t *testing.T) {
	// The destination is CONFIGURATION: there is no dead-letter stream unless
	// the operator asked for one, which is what the next test is about.
	r := newRig(t, func(c *rd.Config) { c.DeadLetterSuffix = rd.DefaultDeadLetterSuffix })
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("poison")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	if err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Nack(deadletter): %v", err)
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	if n := len(r.srv.Pending(keys.Stream(0), testSub)); n != 0 {
		t.Fatalf("the dead-lettered entry is still pending (%d)", n)
	}
	dlq := r.srv.Entries(keys.Stream(0) + ":dlq")
	if len(dlq) != 1 {
		t.Fatalf("the dead-letter stream holds %d entries, want 1", len(dlq))
	}
	var moved mq.Message
	if err := streams.Decode(&moved, dlq[0].Fields); err != nil {
		t.Fatalf("the dead-lettered entry does not decode: %v", err)
	}
	if string(moved.Body) != "poison" {
		t.Fatalf("the dead-lettered body is %q", moved.Body)
	}
	// The entry is still in the SOURCE stream, which is right: a dead-letter
	// is a decision about this subscription, and deleting the entry would
	// remove it for every other consumer group on the topic.
	if n := len(r.srv.Entries(keys.Stream(0))); n != 1 {
		t.Fatalf("the source stream holds %d entries: the dead-letter deleted the entry for every "+
			"other subscription as well", n)
	}
}

// TestDeadLetterWithNoDestinationIsRefused is the rule, and it is the
// check a driver is most likely to get wrong by being helpful.
//
// The destination is the SUBSCRIPTION's, and a driver that invented one out of
// the topic's name would make a Reject succeed on a subscription nobody gave a
// dead-letter destination to — the message moved somewhere the operator has no
// consumer for, which is indistinguishable from destroying it. This driver did
// exactly that until the suite's dlq/failed-publish-never-acks scenario
// ran against a real server.
//
// The other half is what keeps the message: a refused Reject settles nothing,
// so the delivery is still settleable.
func TestDeadLetterWithNoDestinationIsRefused(t *testing.T) {
	r := newRig(t) // no DeadLetterTopic, no DeadLetterSuffix
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("nowhere")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Reject on a subscription with no dead-letter destination returned %v, want "+
			"ErrUnsupported: a driver that invented a destination moves the message somewhere "+
			"the operator has no consumer for", err)
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	if n := len(r.srv.Pending(keys.Stream(0), testSub)); n != 1 {
		t.Fatalf("the refused Reject settled the entry anyway (%d pending): the specification leaves a refused "+
			"settlement settleable", n)
	}
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Fatalf("the delivery is not settleable after a refused Reject: %v", err)
	}
}

// TestDeadLetterTopicIsUsedAsGiven: a destination named as a TOPIC gets its
// own key naming, which means its own hash tag — so the Lua move cannot run
// and the plan is two commands with the documented duplicate window.
func TestDeadLetterTopicIsUsedAsGiven(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.DeadLetterTopic = "acme.orders-dlq" })
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("poison")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	if err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Nack(deadletter): %v", err)
	}
	dlqKeys, err := streams.NewKeys("acme.orders-dlq", 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	entries := r.srv.Entries(dlqKeys.Stream(0))
	if len(entries) != 1 {
		t.Fatalf("the named dead-letter topic holds %d entries, want 1", len(entries))
	}
	var moved mq.Message
	if err := streams.Decode(&moved, entries[0].Fields); err != nil {
		t.Fatalf("the dead-lettered entry does not decode: %v", err)
	}
	if string(moved.Body) != "poison" {
		t.Fatalf("the dead-lettered body is %q", moved.Body)
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	if n := len(r.srv.Pending(keys.Stream(0), testSub)); n != 0 {
		t.Fatalf("the dead-lettered entry is still pending (%d)", n)
	}
}

// TestReplayMovesTheGroup is the SETID replay: a subscription can be moved
// back to the beginning of the stream.
//
// The two limits are asserted as well as the behaviour, because they are the
// part a caller has to know: SETID moves the GROUP, so every consumer of the
// subscription moves together, and it does not touch the pending list, so
// entries already handed out stay pending.
func TestReplayMovesTheGroup(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	for _, body := range []string{"one", "two", "three"} {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte(body)}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	dst := make([]mq.DriverDelivery, 3)
	fctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	n, err := src.Fetch(fctx, dst)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 3 {
		t.Fatalf("Fetch returned %d of 3", n)
	}
	bs, _ := src.Settler().(mq.BatchSettler)
	if err := bs.AckBatch(r.ctx, []*mq.DriverDelivery{&dst[0], &dst[1], &dst[2]}); err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	// Replay: move the group back to the beginning.
	keys, _ := streams.NewKeys(testTopic, 1)
	if _, err := r.srv.Do(r.ctx, streams.XGroupSetID(keys.Stream(0), testSub, streams.IDZero)); err != nil {
		t.Fatalf("XGROUP SETID: %v", err)
	}
	again := make([]mq.DriverDelivery, 3)
	fctx2, cancel2 := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel2()
	got, err := src.Fetch(fctx2, again)
	if err != nil {
		t.Fatalf("Fetch after replay: %v", err)
	}
	if got != 3 {
		t.Fatalf("the replay delivered %d of 3 messages", got)
	}
	if string(again[0].Message.Body) != "one" {
		t.Fatalf("the replay started at %q, want the beginning", again[0].Message.Body)
	}
}

// TestStartPositionsMapToStreamIdentifiers covers the five positions.
//
// StartAt(time) is the one worth reading: Redis has no seek-by-time, and a
// stream identifier's high half IS the publication time in milliseconds, so
// "the first entry at or after t" is a real position rather than an
// approximation. StartAtSequence is exact because the sequence is the PACKED
// identifier.
func TestStartPositionsMapToStreamIdentifiers(t *testing.T) {
	r := newRig(t)
	sink, _ := r.openPair(t)
	for _, body := range []string{"one", "two", "three"} {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte(body)}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	// Earliest sees everything the stream still holds.
	earliest := r.openSourceAt(t, "sub-earliest", mq.StartEarliest())
	if n := countFetch(t, r.ctx, earliest, 3); n != 3 {
		t.Fatalf("StartEarliest delivered %d of 3", n)
	}
	// Latest sees nothing published before it.
	latest := r.openSourceAt(t, "sub-latest", mq.StartLatest())
	if n := countFetch(t, r.ctx, latest, 1); n != 0 {
		t.Fatalf("StartLatest delivered %d messages published before the subscription existed", n)
	}
	// A sequence resumes exactly at the ENTRY, not at the start of its
	// millisecond, and the three messages above were all published within one
	// (the fake's clock is frozen, and a real Redis under load does the same).
	// So the resume is taken from the SECOND entry's sequence: that entry and
	// the one after it are delivered, and the first is not.
	//
	// The distinction is the whole. A sequence carrying
	// only the millisecond decodes back to "<ms>-0", so a resume from the
	// second entry of a millisecond re-delivers the first entry as well: a
	// duplicate for every message published in the same millisecond before
	// the resume point, which on a busy stream is thousands.
	dst := make([]mq.DriverDelivery, 3)
	fctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	probe := r.openSourceAt(t, "sub-probe", mq.StartEarliest())
	if n, err := probe.Fetch(fctx, dst); err != nil || n != 3 {
		t.Fatalf("Fetch: %d, %v", n, err)
	}
	firstSeq := streams.Unpack(dst[0].Info.Sequence)
	secondSeq := streams.Unpack(dst[1].Info.Sequence)
	if firstSeq.Ms != secondSeq.Ms {
		t.Skipf("the three publications landed in different milliseconds (%v, %v), so this "+
			"assertion cannot distinguish an exact resume from a millisecond one", firstSeq, secondSeq)
	}
	atSeq := r.openSourceAt(t, "sub-atseq", mq.StartAtSequence(dst[1].Info.Sequence))
	if n := countFetch(t, r.ctx, atSeq, 3); n != 2 {
		t.Fatalf("StartAtSequence(the second of three entries in one millisecond) delivered %d "+
			"messages, want 2, that entry and the one after it: a sequence that carries only the "+
			"millisecond resumes at <ms>-0 and re-delivers everything published in that millisecond", n)
	}
	// A time resumes at the millisecond, which is the resolution a stream
	// identifier has.
	atTime := r.openSourceAt(t, "sub-attime", mq.StartAt(r.srv.Now().Add(time.Hour)))
	if n := countFetch(t, r.ctx, atTime, 1); n != 0 {
		t.Fatalf("StartAt(an hour from now) delivered %d messages", n)
	}
}

// TestStartAtDeliversTheEntryPublishedAtThatInstant is StartAt(t) for a
// message published at exactly t: "the first message at or after t" is that
// message.
//
// A consumer group is created at the identifier of the LAST entry it has
// seen, so a group created at "<t-ms>-0" treats the entry "<t-ms>-0" as
// delivered already, and that is the identifier Redis gives the first entry
// published in t's millisecond. The start is the last identifier before the
// millisecond instead. An entry from the millisecond before stays out.
func TestStartAtDeliversTheEntryPublishedAtThatInstant(t *testing.T) {
	r := newRig(t)
	sink, _ := r.openPair(t)
	publish := func(body string) {
		t.Helper()
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte(body)}, nil); err != nil {
			t.Fatalf("Publish(%q): %v", body, err)
		}
	}
	publish("a millisecond before t")
	r.srv.Advance(time.Millisecond)
	at := r.srv.Now()
	publish("at t")
	publish("after t, in the same millisecond")
	r.srv.Advance(time.Millisecond)
	publish("a millisecond after t")

	src := r.openSourceAt(t, "sub-at-t", mq.StartAt(at))
	dst := make([]mq.DriverDelivery, 8)
	fctx, cancel := context.WithTimeout(r.ctx, 300*time.Millisecond)
	defer cancel()
	n, err := src.Fetch(fctx, dst)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch: %v", err)
	}
	got := make([]string, n)
	for i := range n {
		got[i] = string(dst[i].Message.Body)
	}
	want := []string{"at t", "after t, in the same millisecond", "a millisecond after t"}
	if !slices.Equal(got, want) {
		t.Fatalf("StartAt(t) delivered %q, want %q", got, want)
	}
}

// TestStartAtSequenceDeliversTheEntryAtThatSequence is StartAtSequence(n) for
// the n a delivery reported: the group starts AT the entry, as the memory
// engine, Kafka and JetStream do, so a caller resuming from a recorded
// DeliveryInfo.Sequence gets that entry again instead of losing it.
//
// XGROUP CREATE takes the last entry the group has seen, so a group created at
// n's own identifier started after it. The start is the identifier immediately
// before n's: the one before it in the same millisecond, or the last one of the
// millisecond before when n is the first entry of its own. Both cases are
// here, and an entry from before n stays out.
func TestStartAtSequenceDeliversTheEntryAtThatSequence(t *testing.T) {
	r := newRig(t)
	sink, _ := r.openPair(t)
	bodies := []string{"a millisecond before", "first of its millisecond", "second of its millisecond", "a millisecond after"}
	publish := func(body string) {
		t.Helper()
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte(body)}, nil); err != nil {
			t.Fatalf("Publish(%q): %v", body, err)
		}
	}
	publish(bodies[0])
	r.srv.Advance(time.Millisecond)
	publish(bodies[1])
	publish(bodies[2])
	r.srv.Advance(time.Millisecond)
	publish(bodies[3])

	probe := r.openSourceAt(t, "sub-probe", mq.StartEarliest())
	seq := make(map[string]uint64, len(bodies))
	for range bodies {
		d := fetchOne(t, r.ctx, probe)
		seq[string(d.Message.Body)] = d.Info.Sequence
	}
	if first, second := streams.Unpack(seq[bodies[1]]), streams.Unpack(seq[bodies[2]]); first.Seq != 0 || second.Ms != first.Ms {
		t.Fatalf("the instrument is broken: %q is %v and %q is %v, want the first two entries of one millisecond",
			bodies[1], first, bodies[2], second)
	}

	for i, sub := range []string{"sub-at-first", "sub-at-second"} {
		from := bodies[1+i]
		src := r.openSourceAt(t, sub, mq.StartAtSequence(seq[from]))
		dst := make([]mq.DriverDelivery, 8)
		fctx, cancel := context.WithTimeout(r.ctx, 300*time.Millisecond)
		n, err := src.Fetch(fctx, dst)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Fetch: %v", err)
		}
		got := make([]string, n)
		for j := range n {
			got[j] = string(dst[j].Message.Body)
		}
		if want := bodies[1+i:]; !slices.Equal(got, want) {
			t.Errorf("StartAtSequence(%v, the sequence of %q) delivered %q, want %q",
				streams.Unpack(seq[from]), from, got, want)
		}
	}
}

// openSourceAt opens a second subscription at a given start position.
func (r *rig) openSourceAt(t testing.TB, sub string, pos mq.StartPosition) mq.Source {
	t.Helper()
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic:        testTopic,
		Subscription: sub,
		Prefetch:     16,
		AckDeadline:  testLease,
		Start:        pos,
	})
	if err != nil {
		t.Fatalf("OpenSource(%v): %v", pos, err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// countFetch fetches once with a short bound and reports how many arrived.
func countFetch(t testing.TB, ctx context.Context, src mq.Source, want int) int {
	t.Helper()
	dst := make([]mq.DriverDelivery, want)
	fctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	n, err := src.Fetch(fctx, dst)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch: %v", err)
	}
	return n
}

// TestAdminSurface covers the portable intersection.
func TestAdminSurface(t *testing.T) {
	r := newRig(t)
	adm, err := r.b.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := adm.EnsureTopic(r.ctx, mq.TopicSpec{Name: testTopic}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	// Idempotent, which the specification requires.
	if err := adm.EnsureTopic(r.ctx, mq.TopicSpec{Name: testTopic}); err != nil {
		t.Fatalf("EnsureTopic twice: %v", err)
	}
	if err := adm.EnsureSubscription(r.ctx, mq.SubscriptionSpec{Topic: testTopic, Name: testSub}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	if err := adm.EnsureSubscription(r.ctx, mq.SubscriptionSpec{Topic: testTopic, Name: testSub}); err != nil {
		t.Fatalf("EnsureSubscription twice (BUSYGROUP is not a failure): %v", err)
	}
	// The throwaway group EnsureTopic used must be gone: an operator finding
	// it would wonder what subscription it is.
	keys, _ := streams.NewKeys(testTopic, 1)
	v, err := r.srv.Do(r.ctx, streams.Cmd{
		Tokens:   []string{"XINFO", "GROUPS", keys.Stream(0)},
		KeyStart: 2, KeyCount: 1,
	})
	if err != nil {
		t.Fatalf("XINFO GROUPS: %v", err)
	}
	rows, _ := v.Arr()
	for _, row := range rows {
		fields, _ := row.Arr()
		for i := 0; i+1 < len(fields); i += 2 {
			if name, _ := fields[i].Str(); name == "name" {
				if got, _ := fields[i+1].Str(); got != testSub {
					t.Fatalf("the group %q was left behind by EnsureTopic", got)
				}
			}
		}
	}
	// Stats over a subscription that has undelivered entries.
	sink, _ := r.openPair(t)
	for range 3 {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	stats, err := adm.Stats(r.ctx, testTopic, testSub)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Lag.Known {
		t.Fatal("Stats reports an unknown lag for a stream nothing has trimmed")
	}
	// ListTopics reports the topic once, not once per shard.
	seen := 0
	for topic, err := range adm.ListTopics(r.ctx) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		if topic == testTopic {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("ListTopics reported %q %d times, want once", testTopic, seen)
	}
	// Purge moves the group past everything undelivered.
	if err := adm.Purge(r.ctx, testTopic, testSub); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	after, err := adm.Stats(r.ctx, testTopic, testSub)
	if err != nil {
		t.Fatalf("Stats after Purge: %v", err)
	}
	if after.Lag.Known && after.Lag.Value != 0 {
		t.Fatalf("the subscription still has a lag of %d after Purge", after.Lag.Value)
	}
	// DeleteTopic removes the streams and the delayed sorted sets.
	if err := adm.DeleteTopic(r.ctx, testTopic); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if n := len(r.srv.Entries(keys.Stream(0))); n != 0 {
		t.Fatalf("the stream still holds %d entries after DeleteTopic", n)
	}
}

// TestListTopicsReportsATopicOnce: a topic with four shards is four Redis
// keys, and a listing that reported each of them would tell a caller it has
// four topics.
func TestListTopicsReportsATopicOnce(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.Partitions = 4 })
	adm, err := r.b.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := adm.EnsureTopic(r.ctx, mq.TopicSpec{Name: testTopic}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	names := []string{}
	for topic, err := range adm.ListTopics(r.ctx) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		names = append(names, topic)
	}
	if len(names) != 1 || names[0] != testTopic {
		t.Fatalf("ListTopics reported %v for one four-shard topic; a listing that reports each "+
			"shard tells a caller it has four topics", names)
	}
}

// TestListTopicsIsPrefixScoped: two deployments sharing one Redis must not see
// each other's topics.
func TestListTopicsIsPrefixScoped(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	open := func(prefix string) *rd.Backend {
		b, err := rd.BackendWithCommander(context.Background(), srv, driverkit.ClientBorrowed, rd.Config{
			AutoCreate: true, KeyPrefix: prefix, Clock: fakeClock{srv},
		})
		if err != nil {
			t.Fatalf("BackendWithCommander: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	a, b := open("tenant-a."), open("tenant-b.")
	for _, backend := range []*rd.Backend{a, b} {
		adm, err := backend.Admin()
		if err != nil {
			t.Fatalf("Admin: %v", err)
		}
		if err := adm.EnsureTopic(context.Background(), mq.TopicSpec{Name: "orders"}); err != nil {
			t.Fatalf("EnsureTopic: %v", err)
		}
	}
	admA, _ := a.Admin()
	names := []string{}
	for topic, err := range admA.ListTopics(context.Background()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		names = append(names, topic)
	}
	if len(names) != 1 || names[0] != "orders" {
		t.Fatalf("tenant-a sees %v; a prefixed deployment must see only its own topics", names)
	}
}

// TestScriptCacheRecoveryKeepsSettlementWorking is the NOSCRIPT recovery.
//
// It is the one server error this driver handles itself, and it has to be
// handled on every route: a server that restarted or ran SCRIPT FLUSH has an
// empty cache, and every settlement route is an EVALSHA. A driver that did
// not recover would stop being able to acknowledge anything after a failover
// while still being able to publish, which is an outage nobody can diagnose.
func TestScriptCacheRecoveryKeepsSettlementWorking(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	// The next EVALSHA meets an empty script cache.
	r.srv.Fail("EVALSHA", fakestreams.ErrNoScript)
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Fatalf("Ack did not recover from an empty script cache: %v", err)
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	if n := len(r.srv.Pending(keys.Stream(0), testSub)); n != 0 {
		t.Fatalf("the entry is still pending after a recovered acknowledgement (%d)", n)
	}
}

// TestSettlementFailureLeavesTheDeliverySettleable is the rule for a
// definite refusal: the core returns the error and the caller may try again.
//
// The half that matters is the second attempt succeeding. A driver that
// forgot to release its own settlement mark would leave the delivery
// permanently unsettleable, which is the same outcome as losing it.
func TestSettlementFailureLeavesTheDeliverySettleable(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	r.srv.Fail("EVALSHA", errors.New("LOADING Redis is loading the dataset in memory"))
	if err := src.Settler().Ack(r.ctx, &d); err == nil {
		t.Fatal("Ack reported success for a settlement the server refused")
	}
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Fatalf("the delivery is no longer settleable after a failed settlement: %v", err)
	}
}

// TestHealthDegradesWhenTheSweepFails is for the transition that
// matters most on this broker.
//
// The reclaim sweep is the only thing that redelivers, so a sweep that keeps
// failing is a subscription that has silently stopped being at-least-once.
// A driver that reported it only in a log would leave a caller polling Health
// with no way to find out.
func TestHealthDegradesWhenTheSweepFails(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.ClaimInterval = 10 * time.Millisecond })
	_, src := r.openPair(t)
	defer func() { _ = src.Close() }()
	r.srv.Fail("XAUTOCLAIM", errors.New("LOADING Redis is loading the dataset in memory"))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.b.Health().State == mq.HealthDegraded {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the sweep failed and health stayed %v: a subscription whose reclaim loop has "+
		"stopped is no longer at-least-once, and the specification requires that to surface", r.b.Health().State)
}
