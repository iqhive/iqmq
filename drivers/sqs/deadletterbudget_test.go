package sqs_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
)

// The core's dead-letter route, mq.WithDeadLetter, publishes a copy of the
// failed message with five provenance headers, through a MetadataNativeStrict
// publisher, and on this driver each header is a message attribute. The
// service takes ten.

// budgetMessage is a message with a key, an identifier, properties typed
// properties and headers headers. With the one attribute this driver always
// keeps back for its body encoding, it spends 3 + properties + headers of the
// ten.
func budgetMessage(properties, headers int) *mq.Message {
	m := &mq.Message{Body: []byte(`{"id":42}`), Key: []byte("order-42"), ID: "id-42"}
	p := &m.Properties
	for i, set := range []*string{&p.ContentType, &p.CorrelationID, &p.CausationID, &p.ReplyTo, &p.ContentEncoding} {
		if i < properties {
			*set = "p" + strconv.Itoa(i)
		}
	}
	for i := range headers {
		m.Headers.Set("h"+strconv.Itoa(i), []byte("v"))
	}
	return m
}

// TestTheCoreDeadLetterRouteFitsTheAttributeBudget runs the core route over
// messages whose own metadata leaves room for all five provenance headers,
// for some, and for none. The copy carries every attribute the message was
// published with, and as much provenance as fits, the origin topic last to
// go. It used to be refused before any I/O whenever the five did not fit, for
// ever: the message stayed on its queue, redelivered and failed again on every
// delivery, counted only as DeadLetterFailures.
//
// The headers stay within the nine the driver advertises (MaxHeaderCount),
// copy and provenance included, because the core refuses a copy over that
// before the driver sees it.
func TestTheCoreDeadLetterRouteFitsTheAttributeBudget(t *testing.T) {
	all := []string{
		mq.HeaderOriginTopic, mq.HeaderOriginSubscription, mq.HeaderAttempts, mq.HeaderFailure,
		mq.HeaderFirstDeliveredAt,
	}
	for _, tc := range []struct {
		properties, headers int
		want                []string
	}{
		{0, 2, all},
		{0, 4, all[:3]},
		{4, 2, all[:1]},
		{5, 2, nil},
	} {
		attributes := 3 + tc.properties + tc.headers
		t.Run(strconv.Itoa(attributes)+"-attributes-"+strconv.Itoa(tc.headers)+"-headers", func(t *testing.T) {
			h := open(t)
			tp, dead := topic(t, "dl-budget"), topic(t, "dl-budget-dead")
			m := budgetMessage(tc.properties, tc.headers)
			h.publish(tp, m)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cons := h.consumer(tp, 1)
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() {
				done <- cons.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
					return errors.New("schema mismatch")
				}), mq.WithMaxAttempts(1), mq.WithRetryBackoff(mq.ConstantBackoff(0)), mq.WithDeadLetter(dead))
			}()
			reader := h.consumer(dead, 1)
			rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
			defer rcancel()
			dst := make([]mq.Delivery, 1)
			n, rerr := reader.Receive(rctx, dst)
			stop()
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run: %v", err)
			}
			if n == 0 {
				t.Fatalf("the copy of a message spending %d of the %d attributes never reached the dead-letter "+
					"queue (%v); DeadLetterFailures %d", attributes, sq.MaxAttributes, rerr,
					cons.Stats().DeadLetterFailures)
			}
			d := dst[0]
			defer func() { _ = d.Ack() }()
			if !bytes.Equal(d.Key, m.Key) || d.ID != m.ID || d.Properties != m.Properties {
				t.Errorf("the copy lost metadata it was published with: key %q, identifier %q, properties %+v",
					d.Key, d.ID, d.Properties)
			}
			for k, v := range m.Headers.All() {
				if got, ok := d.Headers.Get(k); !ok || !bytes.Equal(got, v) {
					t.Errorf("the copy's header %s is %q (present %v), want %q", k, got, ok, v)
				}
			}
			var got []string
			for _, k := range all {
				if _, ok := d.Headers.Get(k); ok {
					got = append(got, k)
				}
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("the copy carries the provenance %q, want %q", got, tc.want)
			}
		})
	}
}
