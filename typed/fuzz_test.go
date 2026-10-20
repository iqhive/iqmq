package typed_test

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/typed"
)

// FuzzTypedDispatch is the test suite's typed-dispatch target.
//
// It holds the decode failure path to its whole contract at once, over
// arbitrary bodies, arbitrary content types and every content-type policy:
//
//   - dispatch never panics, whatever the body is;
//   - the typed handler runs at most once per delivery, and never at all for
//     a delivery that did not decode — a handler that saw a zero value would
//     be processing a message that was never there;
//   - a delivery that did not decode is refused with a named disposition (the
//     default is DeadLetter) and the codec's own error is reachable through
//     it, so the specification's "errors.Is and errors.As reach the cause" holds across
//     the typed boundary;
//   - a body this codec itself produced round-trips: the handler sees the
//     value that was encoded, not a value that merely decodes.
//
// The last property is what stops the first three being satisfied by a
// handler that refuses everything.
func FuzzTypedDispatch(f *testing.F) {
	seeds := []struct {
		body   string
		ctype  string
		policy uint8
	}{
		{`{"id":1,"cust":"c"}`, "application/json", 0},
		{`{"id":1,"cust":"c"}`, "application/json; charset=utf-8", 1},
		{`{"id":1,"cust":"c"}`, "application/vnd.acme+json", 1},
		{`{"id":1,"cust":"c"}`, "application/xml", 1},
		{`{"id":1,"cust":"c"}`, "", 2},
		{`{"id":`, "application/json", 0},
		{``, "", 0},
		{`null`, "", 0},
		{`[]`, "", 0},
		{`{"id":"not a number"}`, "", 0},
		{`{"id":99999999999999999999999999}`, "", 0},
		{"\x00\x01\x02", "", 0},
		{`{"id":1}`, "application/json;;;", 1},
		{`{"id":1,"id":2}`, "", 0},
		{`{"cust":"\ud800"}`, "", 0},
	}
	for _, s := range seeds {
		f.Add([]byte(s.body), s.ctype, s.policy)
	}

	f.Fuzz(func(t *testing.T, body []byte, ctype string, policy uint8) {
		p := typed.ContentTypePolicy(policy % 3)
		ctx := context.Background()

		// Property 1-3: dispatch of whatever arrived.
		s := &seen{}
		h := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](s.handle),
			typed.HandlerConfig{ContentType: p})
		d := &mq.Delivery{Message: mq.Message{Topic: "orders", Body: body}}
		d.Properties.ContentType = ctype
		if err := h.Handle(ctx, d); err == nil {
			if s.n.Load() != 1 {
				t.Fatalf("Handle = nil but the typed handler ran %d times, want exactly 1: a delivery was acked without being processed", s.n.Load())
			}
		} else {
			if s.n.Load() != 0 {
				t.Fatalf("Handle = %v after running the typed handler %d times; a refused delivery must not reach the handler", err, s.n.Load())
			}
			if dir, _ := mq.DirectiveOf(err); dir != mq.DirectiveDeadLetter {
				t.Fatalf("directive = %v for %v, want DirectiveDeadLetter: the default policy names the disposition", dir, err)
			}
			var de *codec.DecodeError
			var cte *codec.ContentTypeError
			if !errors.As(err, &de) && !errors.As(err, &cte) {
				t.Fatalf("Handle = %v, want the codec's own *DecodeError or *ContentTypeError reachable through it", err)
			}
		}

		// Property 4: what this codec encodes, it decodes. The customer is
		// hex so that the round trip is about the dispatch and not about
		// JSON's treatment of invalid UTF-8.
		cust := hex.EncodeToString(body)
		if len(cust) > 64 {
			cust = cust[:64]
		}
		want := Order{ID: len(body), Cust: cust}
		r := &recorder{}
		pub := mustNewPublisher(t, r, codec.JSON[Order]())
		if err := pub.Publish(ctx, want); err != nil {
			t.Fatalf("Publish = %v, want nil", err)
		}
		back := &seen{}
		rh := mustNewHandler(t, codec.JSON[Order](), typed.HandlerFunc[Order](back.handle),
			typed.HandlerConfig{ContentType: p})
		rd := &mq.Delivery{Message: mq.Message{Topic: "orders", Body: r.sent[0].Body}}
		rd.Properties.ContentType = r.sent[0].Properties.ContentType
		if err := rh.Handle(ctx, rd); err != nil {
			t.Fatalf("a message this publisher produced did not dispatch under %s: %v", p, err)
		}
		if back.n.Load() != 1 {
			t.Fatalf("the round-tripped message reached the handler %d times, want 1", back.n.Load())
		}
		if back.values[0] != want {
			t.Fatalf("round trip = %+v, want %+v", back.values[0], want)
		}
	})
}
