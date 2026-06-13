package nsq_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
	"bitbucket.org/iqhive/iqmq/wire"
)

// TestTheEnvelopeIsTheOnlyMetadataPath is on a broker with no headers.
//
// Each mode has to do something different and none of them may do something
// quietly: strict refuses before any I/O, the two enveloping modes produce a
// round trip that reproduces the message, and the lenient mode publishes the
// body and says how much it discarded.
func TestTheEnvelopeIsTheOnlyMetadataPath(t *testing.T) {
	msg := func() *mq.Message {
		m := &mq.Message{Topic: "orders", Key: []byte("k"), Body: []byte("body"), ID: "id-1"}
		m.Headers.Add("trace", []byte{0x00, 0xff})
		m.Headers.Add("trace", []byte("second"))
		m.Properties.ContentType = "application/json"
		return m
	}

	t.Run("strict refuses before any I/O", func(t *testing.T) {
		b, server := newBackendRig(t)
		snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
		err := snk.Publish(context.Background(), msg(), nil)
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("Publish answered %v, want ErrUnsupported", err)
		}
		if n := server.Published(); n != 0 {
			t.Fatalf("%d message(s) reached the broker; the strict mode refuses BEFORE any I/O", n)
		}
	})

	for name, mode := range map[string]mq.MetadataMode{
		"portable": mq.MetadataPortableEnvelope,
		"adaptive": mq.MetadataAdaptive,
	} {
		t.Run(name+" round trip", func(t *testing.T) {
			b, _ := newBackendRig(t)
			src := sourceOf(t, b, "orders", "workers", mode, 4)
			snk := sinkOf(t, b, "orders", mode)
			want := msg()
			if err := snk.Publish(context.Background(), want, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, src)
			got := d.Message
			if !bytes.Equal(got.Body, want.Body) || !bytes.Equal(got.Key, want.Key) || got.ID != want.ID {
				t.Fatalf("the round trip gave key=%q body=%q id=%q", got.Key, got.Body, got.ID)
			}
			if got.Headers.Len() != 2 {
				t.Fatalf("the round trip kept %d headers, want the duplicate pair; the promise on this broker is the envelope's", got.Headers.Len())
			}
			if got.Properties.ContentType != "application/json" {
				t.Errorf("the content type came back %q", got.Properties.ContentType)
			}
			if got.Topic != "orders" {
				t.Errorf("the topic came back %q", got.Topic)
			}
		})
	}

	t.Run("lenient drops and counts", func(t *testing.T) {
		b, _ := newBackendRig(t)
		src := sourceOf(t, b, "orders", "workers", mq.MetadataLenientDrop, 4)
		snk := sinkOf(t, b, "orders", mq.MetadataLenientDrop)
		if err := snk.Publish(context.Background(), msg(), nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		d := fetchOne(t, src)
		if !bytes.Equal(d.Message.Body, []byte("body")) {
			t.Fatalf("the body came back %q", d.Message.Body)
		}
		if d.Message.Headers.Len() != 0 || d.Message.ID != "" {
			t.Fatal("the lenient mode carried metadata it cannot carry")
		}
		// The specification requires the drop to be counted rather than silent: two
		// headers, an identifier, a key and a property set.
		if n := b.DroppedMetadata(); n != 5 {
			t.Fatalf("DroppedMetadata is %d, want 5; the specification forbids dropping metadata silently", n)
		}
	})
}

// TestAMessageTheEnvelopeCannotCarryIsRefusedBeforeAnyIO is this increment's
// first trap.
//
// The envelope is the only metadata path NSQ has, so a message the envelope
// cannot carry cannot be published at all — and the refusal has to name what
// exceeded which limit, because "cannot be enveloped" on its own leaves the
// caller nothing to act on. Every case here would otherwise become a record
// that Unwrap refuses for ever: a delivery that cannot be decoded cannot be
// settled, which is the disqualifier reached through a publication the
// driver accepted.
func TestAMessageTheEnvelopeCannotCarryIsRefusedBeforeAnyIO(t *testing.T) {
	lim := wire.DefaultLimits
	over := func(n int) string { return strings.Repeat("x", n) }
	for name, tc := range map[string]struct {
		build func() *mq.Message
		code  mq.ErrorCode
		says  string
	}{
		"a header key one byte over": {
			build: func() *mq.Message {
				m := &mq.Message{Body: []byte("b")}
				m.Headers.Add(over(lim.MaxHeaderKey+1), []byte("v"))
				return m
			},
			code: mq.CodeMessageTooLarge, says: "header key",
		},
		"a header value one byte over": {
			build: func() *mq.Message {
				m := &mq.Message{Body: []byte("b")}
				m.Headers.Add("k", bytes.Repeat([]byte("v"), lim.MaxHeaderValue+1))
				return m
			},
			code: mq.CodeMessageTooLarge, says: "header value",
		},
		"an identifier one byte over": {
			build: func() *mq.Message { return &mq.Message{Body: []byte("b"), ID: over(lim.MaxID + 1)} },
			code:  mq.CodeMessageTooLarge, says: "identifier",
		},
		"a property one byte over": {
			build: func() *mq.Message {
				return &mq.Message{Body: []byte("b"), Properties: mq.Properties{ReplyTo: over(lim.MaxProp + 1)}}
			},
			code: mq.CodeMessageTooLarge, says: "typed property",
		},
		"an identifier that is not UTF-8": {
			build: func() *mq.Message { return &mq.Message{Body: []byte("b"), ID: "a\xff"} },
			code:  mq.CodeCorruptMessage, says: "identifier",
		},
		"a header key that is not UTF-8": {
			build: func() *mq.Message {
				m := &mq.Message{Body: []byte("b")}
				m.Headers.Add("a\xff", []byte("v"))
				return m
			},
			code: mq.CodeCorruptMessage, says: "header key",
		},
	} {
		t.Run(name, func(t *testing.T) {
			b, server := newBackendRig(t)
			snk := sinkOf(t, b, "orders", mq.MetadataPortableEnvelope)
			err := snk.Publish(context.Background(), tc.build(), nil)
			if err == nil {
				t.Fatal("the publication was accepted; the record it made cannot be decoded, " +
					"so it can never be delivered and never be settled")
			}
			if n := server.Published(); n != 0 {
				t.Fatalf("%d message(s) reached the broker; the refusal must come BEFORE any I/O", n)
			}
			if mq.CodeOf(err) != tc.code {
				t.Errorf("the refusal carries %v, want %v: %v", mq.CodeOf(err), tc.code, err)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the refusal does not name the field at fault (%q is absent): %v", tc.says, err)
			}
			if !strings.Contains(err.Error(), "envelope") {
				t.Errorf("the refusal does not say the envelope is what refused: %v", err)
			}
		})
	}
}

// TestAnAdaptiveNativeBodyThatLooksLikeAnEnvelopeSurvives is the recorded
// MetadataAdaptive defect, reached through the driver rather than through the
// kit.
//
// On a broker with headers the discrimination is avoidable — drivers/sqs sets a
// message attribute and never sniffs — and on this one it is not: nsqd carries
// a body and nothing else. So a caller who publishes a body beginning with the
// envelope's magic is exactly the case adaptive mode has to get right, and
// getting it wrong is either a corrupt delivery or a subscription that can
// never make progress.
func TestAnAdaptiveNativeBodyThatLooksLikeAnEnvelopeSurvives(t *testing.T) {
	// A byte string that IS a whole envelope, which a caller may perfectly
	// well publish as a payload.
	inner, err := wire.New(wire.Auto).Encode(nil, &mq.Message{Body: []byte("inner"), ID: "inner-1"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for name, body := range map[string][]byte{
		"a magic prefix and nothing else": []byte("IQMQ\x11\x00"),
		"a magic prefix and a payload":    []byte("IQMQ\x01\x00 and then some ordinary bytes"),
		"a whole envelope as a payload":   inner,
	} {
		t.Run(name, func(t *testing.T) {
			b, _ := newBackendRig(t)
			src := sourceOf(t, b, "orders", "workers", mq.MetadataAdaptive, 4)
			snk := sinkOf(t, b, "orders", mq.MetadataAdaptive)
			if err := snk.Publish(context.Background(), &mq.Message{Topic: "orders", Body: body}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, src)
			if !bytes.Equal(d.Message.Body, body) {
				t.Fatalf("the body came back as %q, want %q: a native publication was decoded as metadata",
					d.Message.Body, body)
			}
			if d.Message.ID != "" || d.Message.Headers.Len() != 0 {
				t.Fatalf("the delivery carries metadata the publication did not: id=%q headers=%d",
					d.Message.ID, d.Message.Headers.Len())
			}
		})
	}
}

// TestPublishRefusesWhatNSQCannotDo holds the specification's "enforced before I/O" for
// every control this broker has no verb for.
func TestPublishRefusesWhatNSQCannotDo(t *testing.T) {
	b, server := newBackendRig(t)
	snk := sinkOf(t, b, "orders", mq.MetadataPortableEnvelope)
	for name, o := range map[string]*mq.PublishOptions{
		"a time to live": {TTL: time.Minute},
		"a priority":     {PrioritySet: true},
		"a dedup key":    {DedupKey: "k"},
		"a partition":    {PartitionSet: true},
	} {
		t.Run(name, func(t *testing.T) {
			err := snk.Publish(context.Background(), &mq.Message{Body: []byte("b")}, o)
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("Publish answered %v, want ErrUnsupported", err)
			}
		})
	}
	t.Run("a delay past nsqd's maximum", func(t *testing.T) {
		err := snk.Publish(context.Background(), &mq.Message{Body: []byte("b")},
			&mq.PublishOptions{Delay: 2 * nq.DefaultMaxRequeueDelay})
		if mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Fatalf("Publish answered %v, want CodeInvalidArgument: %v", mq.CodeOf(err), err)
		}
	})
	t.Run("an empty body in a mode that does not envelope", func(t *testing.T) {
		// nsqd refuses a PUB with no body, so in the two modes that put the
		// caller's bytes on the wire unchanged an empty message cannot be
		// published at all — and the refusal belongs before the round trip
		// rather than as an E_BAD_MESSAGE afterwards.
		strict := sinkOf(t, b, "orders-strict", mq.MetadataNativeStrict)
		for _, body := range [][]byte{nil, {}} {
			err := strict.Publish(context.Background(), &mq.Message{Body: body}, nil)
			if mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Fatalf("Publish answered %v, want CodeInvalidArgument: %v", mq.CodeOf(err), err)
			}
		}
	})
	t.Run("a body over nsqd's --max-msg-size", func(t *testing.T) {
		err := snk.Publish(context.Background(), &mq.Message{Body: bytes.Repeat([]byte("x"), nq.DefaultMaxMessageBytes+1)}, nil)
		if !errors.Is(err, mq.ErrMessageTooLarge) {
			t.Fatalf("Publish answered %v, want ErrMessageTooLarge", err)
		}
	})
	if n := server.Published(); n != 0 {
		t.Fatalf("%d message(s) reached the broker; every refusal above is a pre-I/O one", n)
	}
}

// TestOpenSinkRefusesADurableBoundary pins for this broker: nsqd answers
// OK once the message is in the topic's queue, which is in memory up to
// --mem-queue-size, so there is no durable boundary to wait for and a publisher
// asking for one fails at construction rather than being handed a weaker one.
func TestOpenSinkRefusesADurableBoundary(t *testing.T) {
	b, _ := newBackendRig(t)
	_, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmDurable})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("OpenSink answered %v, want ErrUnsupported", err)
	}
}

// TestOpenSourceRefusesAStartPositionItCannotHonour is what makes the
// conformance kit's replay denial answerable in both directions.
//
// An NSQ channel has no cursor: it holds what has not been finished and keeps
// nothing it has. The specification forbids emulating a start position, so StartEarliest is
// refused with ErrUnsupported rather than quietly behaving like StartLatest.
func TestOpenSourceRefusesAStartPositionItCannotHonour(t *testing.T) {
	b, _ := newBackendRig(t)
	for name, start := range map[string]mq.StartPosition{
		"earliest": mq.StartEarliest(),
		"a time":   mq.StartAt(time.Unix(0, 0)),
		"sequence": mq.StartAtSequence(7),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := b.OpenSource(context.Background(), mq.SourceConfig{
				Topic: "orders", Subscription: "workers", Prefetch: 1, Start: start,
			})
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("OpenSource answered %v, want ErrUnsupported", err)
			}
		})
	}
}

// TestBatchPublishSplitsOnTheBodyLimitAndAlignsItsResults is the batch
// contract on a broker whose batch verb is all-or-nothing.
//
// MPUB carries many bodies and one answer, so a failure belongs at every index
// that command carried and nowhere else. A batch larger than nsqd's
// --max-body-size is split, so a batch CAN half succeed — and the alignment is
// the only thing that tells the caller which half.
func TestBatchPublishSplitsOnTheBodyLimitAndAlignsItsResults(t *testing.T) {
	b, server := newBackendRig(t, func(c *nq.Config) {
		c.MaxMessageBytes = 1 << 10
		c.MaxBodyBytes = 3 << 10
	})
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	bs, ok := snk.(mq.BatchSink)
	if !ok {
		t.Fatal("the sink does not implement BatchSink; MPUB is NSQ's native batch verb")
	}
	ms := make([]*mq.Message, 8)
	for i := range ms {
		ms[i] = &mq.Message{Body: bytes.Repeat([]byte("x"), 900)}
	}
	if err := bs.PublishBatch(context.Background(), ms, nil); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if n := server.Published(); n != len(ms) {
		t.Fatalf("the broker took %d of %d messages", n, len(ms))
	}

	t.Run("a refused message keeps its own index", func(t *testing.T) {
		ms := []*mq.Message{
			{Body: []byte("ok")},
			{Body: bytes.Repeat([]byte("x"), 2<<10)}, // over MaxMessageBytes
			{Body: []byte("ok")},
		}
		err := bs.PublishBatch(context.Background(), ms, nil)
		var be *mq.BatchError
		if !errors.As(err, &be) {
			t.Fatalf("PublishBatch answered %v, want a *BatchError", err)
		}
		if len(be.Errs) != len(ms) {
			t.Fatalf("the result has %d entries for %d messages; the alignment is the whole contract", len(be.Errs), len(ms))
		}
		if be.Errs[0] != nil || be.Errs[2] != nil {
			t.Errorf("a message the broker took is reported as failed: %v, %v", be.Errs[0], be.Errs[2])
		}
		if !errors.Is(be.Errs[1], mq.ErrMessageTooLarge) {
			t.Errorf("index 1 is %v, want ErrMessageTooLarge", be.Errs[1])
		}
	})

	t.Run("a whole batch refused is one error", func(t *testing.T) {
		server.FailPublishes(10, errors.New("nsqd is not there"))
		ms := []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}
		err := bs.PublishBatch(context.Background(), ms, nil)
		var be *mq.BatchError
		if errors.As(err, &be) {
			t.Fatalf("PublishBatch answered a *BatchError when nothing was accepted; the specification asks for a single error")
		}
		if err == nil {
			t.Fatal("PublishBatch accepted a batch the broker refused")
		}
	})
}

// TestDeferredPublishUsesDPUB is the scheduled publication: nsqd holds the
// message at the channel level until the delay has passed, which is a native
// verb and not an emulation.
func TestDeferredPublishUsesDPUB(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("later")}, &mq.PublishOptions{Delay: time.Minute}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if st := server.Stats("orders", "workers"); st.Deferred != 1 {
		t.Fatalf("the broker holds %d deferred message(s) and %d pending; a delayed publication must not be deliverable at once",
			st.Deferred, st.Depth)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a deferred message was delivered early: n=%d err=%v", n, err)
	}
	server.Advance(2 * time.Minute)
	d := fetchOne(t, src)
	if !bytes.Equal(d.Message.Body, []byte("later")) {
		t.Fatalf("the delivery is %q", d.Message.Body)
	}
}

// TestAttemptsBecomeTheAttemptCount holds Attempt to nsqd's own
// counter: 1 on the first delivery, and Redelivered only from the second.
func TestAttemptsBecomeTheAttemptCount(t *testing.T) {
	b, _ := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("b")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := fetchOne(t, src)
	if first.Info.Attempt != 1 || first.Info.Redelivered {
		t.Fatalf("the first delivery reports attempt %d redelivered=%v", first.Info.Attempt, first.Info.Redelivered)
	}
	if first.Info.LeaseExpires.IsZero() {
		t.Fatal("the delivery reports no lease; nsqd grants a per-message timeout and the specification makes the zero time 'no lease'")
	}
	if err := src.Settler().Nack(context.Background(), &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second := fetchOne(t, src)
	if second.Info.Attempt != 2 || !second.Info.Redelivered {
		t.Fatalf("the redelivery reports attempt %d redelivered=%v", second.Info.Attempt, second.Info.Redelivered)
	}
	if err := src.Settler().Ack(context.Background(), &second); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestAdaptiveEnvelopesAnEmptyBodyBecauseNSQCannotCarryOne is the specification's
// nil-versus-empty distinction on a broker that refuses both.
//
// nsqd answers a PUB with a zero-length body with E_BAD_MESSAGE, so the two
// modes that publish the caller's bytes unchanged cannot carry an empty message
// at all. Adaptive mode can, by enveloping it — which is a reason to envelope
// that driverkit.Envelope.Needed cannot express, because it reads the
// capability table and "this broker refuses a zero-length message" is not a
// feature.
func TestAdaptiveEnvelopesAnEmptyBodyBecauseNSQCannotCarryOne(t *testing.T) {
	b, _ := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataAdaptive, 4)
	snk := sinkOf(t, b, "orders", mq.MetadataAdaptive)
	for name, body := range map[string][]byte{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			if err := snk.Publish(context.Background(), &mq.Message{Topic: "orders", Body: body}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, src)
			if len(d.Message.Body) != 0 {
				t.Fatalf("the body came back as %q", d.Message.Body)
			}
			if (body == nil) != (d.Message.Body == nil) {
				t.Fatalf("the nil-versus-empty distinction was lost: sent nil=%v, got nil=%v; "+
					"The specification makes it observable", body == nil, d.Message.Body == nil)
			}
			if err := src.Settler().Ack(context.Background(), &d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestTheConfiguredModeIsTheFloorAndNeverTheCeiling pins Config.Metadata's
// exact reach: it replaces the zero value and nothing else.
//
// The dangerous mistake would be an override that also replaced an EXPLICIT
// MetadataPortableEnvelope or MetadataLenientDrop, because then a caller who
// asked for one mode would silently get another — which is the class of
// thing the last line forbids outright.
func TestTheConfiguredModeIsTheFloorAndNeverTheCeiling(t *testing.T) {
	b, _ := newBackendRig(t, func(c *nq.Config) { c.Metadata = mq.MetadataAdaptive })
	t.Run("a handle that named nothing is enveloped", func(t *testing.T) {
		src := sourceOf(t, b, "floor", "workers", mq.MetadataNativeStrict, 4)
		snk := sinkOf(t, b, "floor", mq.MetadataNativeStrict)
		m := &mq.Message{Topic: "floor", Body: []byte("b"), Key: []byte("k")}
		if err := snk.Publish(context.Background(), m, nil); err != nil {
			t.Fatalf("Publish: %v; the configured mode did not reach the handle", err)
		}
		d := fetchOne(t, src)
		if !bytes.Equal(d.Message.Key, []byte("k")) {
			t.Fatalf("the key came back %q", d.Message.Key)
		}
		if err := src.Settler().Ack(context.Background(), &d); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
	t.Run("a handle that named the lenient mode keeps it", func(t *testing.T) {
		src := sourceOf(t, b, "ceiling", "workers", mq.MetadataLenientDrop, 4)
		snk := sinkOf(t, b, "ceiling", mq.MetadataLenientDrop)
		m := &mq.Message{Topic: "ceiling", Body: []byte("b"), Key: []byte("k")}
		if err := snk.Publish(context.Background(), m, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		d := fetchOne(t, src)
		if len(d.Message.Key) != 0 {
			t.Fatalf("the key survived a mode that drops what it cannot carry: %q; "+
				"the configured mode overrode an explicit request", d.Message.Key)
		}
		if err := src.Settler().Ack(context.Background(), &d); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
}
