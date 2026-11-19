package rabbitmq

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit"
)

// everyProperty is a publication with every property set and a header of
// every field type amqp091 encodes, for the frame arithmetic. userID is the
// connection's own user: RabbitMQ refuses a `user-id` that is not.
func everyProperty(userID string) amqp.Publishing {
	return amqp.Publishing{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		DeliveryMode:    deliveryPersistent,
		Priority:        5,
		CorrelationId:   "c1",
		ReplyTo:         "r",
		Expiration:      "60000",
		MessageId:       "m",
		Timestamp:       time.Unix(1_700_000_000, 0),
		Type:            "t",
		UserId:          userID,
		AppId:           "app",
		Headers: amqp.Table{
			"a": true, "b": byte(1), "c": int8(-1), "d": int16(-2), "e": uint16(2), "f": 3, "g": int32(-3),
			"h": uint32(3), "i": float32(1.5), "j": amqp.Decimal{Scale: 2, Value: 314}, "k": int64(-4),
			"l": 2.5, "m": time.Unix(1_700_000_000, 0), "n": "abc", "o": []byte{1, 2}, "p": nil,
			"q": amqp.Table{"x": int32(1)}, "r": []any{int32(1), "z"},
		},
		Body: []byte("body"),
	}
}

// TestHeaderFrameSizeIsTheEncodedFrame holds [headerFrameSize] to AMQP 0-9-1's
// encoding of the content header frame, worked out by hand for a publication
// with no property, one with every property, and one header of every field
// type. TestHeaderFrameSizeIsExactOnABroker holds it to RabbitMQ.
func TestHeaderFrameSizeIsTheEncodedFrame(t *testing.T) {
	if got := headerFrameSize(&amqp.Publishing{Body: []byte("b")}); got != 14 {
		t.Errorf("a publication with no property = %d, want 14: class, weight, body size and flags", got)
	}
	pub := everyProperty("guest")
	headers := pub.Headers
	pub.Headers = nil
	// The shortstrs, a length octet and the bytes: content-type 17,
	// content-encoding 5, correlation-id 3, reply-to 2, expiration 6,
	// message-id 2, type 2, user-id 6, app-id 4; delivery-mode and priority
	// one octet each; the timestamp 8.
	want := 14 + 17 + 5 + 3 + 2 + 6 + 2 + 2 + 6 + 4 + 1 + 1 + 8
	if got := headerFrameSize(&pub); got != want {
		t.Errorf("a publication with every property and no header = %d, want %d", got, want)
	}
	// The table is a longstr of its entries, 4 octets of length, and each
	// entry is the key as a shortstr, 2 octets here, the type octet and the
	// value: bool, byte and int8 1; int16 and uint16 2; int, int32, uint32 and
	// float32 4; the decimal 5; int64, float64 and the timestamp 8; the
	// string and the byte array 4 of length and their bytes, 3 and 2; void
	// nothing; the nested table its own longstr, 4 + (2 + 1 + 4); and the
	// array 4 of length and its values with their type octets, (1 + 4) and
	// (1 + 4 + 1).
	entries := 3*(2+1+1) + 2*(2+1+2) + 4*(2+1+4) + (2 + 1 + 5) + 3*(2+1+8) + (2 + 1 + 4 + 3) + (2 + 1 + 4 + 2) +
		(2 + 1) + (2 + 1 + 4 + (2 + 1 + 4)) + (2 + 1 + 4 + (1 + 4) + (1 + 4 + 1))
	pub.Headers = headers
	if got := headerFrameSize(&pub); got != want+4+entries {
		t.Errorf("a publication with every property and every field type = %d, want %d", got, want+4+entries)
	}
}

// TestHeaderFrameSizeAllocatesNothing holds the check on the publish path to
// no allocation: it walks the table the encoding fills, and the frame the
// client library builds is the library's own.
func TestHeaderFrameSizeAllocatesNothing(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cn, err := broker.Dialer().Dial(t.Context(), []string{"amqp://fake/"}, client.DialConfig{})
	if err != nil {
		t.Fatal(err)
	}
	pub := everyProperty("guest")
	if got := testing.AllocsPerRun(1000, func() {
		if err := fitsFrame(&pub, cn); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Errorf("checking one publication's header frame allocated %.1f times, want 0", got)
	}
}

// TestHeaderFrameSizeIsExactOnABroker is [headerFrameSize] against RabbitMQ
// and the client library on the wire, on one connection each:
//
//   - a publication whose header frame it works out to the negotiated frame
//     size less the frame's 8 octets of header and end is confirmed, and reads
//     back;
//   - one a byte larger is confirmed too, because RabbitMQ allows a client 8
//     octets over, and its delivery cannot be read: the client library refuses
//     a frame over that size from the broker, with 501, and so would any
//     consumer of the queue on the same frame size;
//   - one a byte larger than the frame size itself makes RabbitMQ close the
//     connection with 501 FRAME_ERROR.
//
// So the frame size less 8 is the bound, and the driver holds a publication to
// it. It runs against the broker IQMQ_RABBITMQ_ADDR names (host:port, with
// IQMQ_RABBITMQ_USER and IQMQ_RABBITMQ_PASSWORD, guest by default), and skips
// without one.
func TestHeaderFrameSizeIsExactOnABroker(t *testing.T) {
	addr := os.Getenv("IQMQ_RABBITMQ_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_RABBITMQ_ADDR (host:port) to run this against a RabbitMQ broker")
	}
	user, password := "guest", "guest"
	if u := os.Getenv("IQMQ_RABBITMQ_USER"); u != "" {
		user, password = u, os.Getenv("IQMQ_RABBITMQ_PASSWORD")
	}
	// publish sends a publication whose header frame is pad octets over the
	// frame size less 8, and reports whether the broker confirmed it, how the
	// connection closed if it did, and what reading it back returned.
	publish := func(pad int) (frame int, confirmed bool, closed *amqp.Error, read error) {
		t.Helper()
		conn, err := amqp.Dial("amqp://" + user + ":" + password + "@" + addr + "/")
		if err != nil {
			t.Fatalf("dialling the broker: %v", err)
		}
		defer func() { _ = conn.Close() }()
		closes := conn.NotifyClose(make(chan *amqp.Error, 1))
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := ch.Confirm(false); err != nil {
			t.Fatal(err)
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		pub := everyProperty(user)
		pub.Headers["pad"] = []byte{}
		pub.Headers["pad"] = make([]byte, conn.Config.FrameSize-frameOverhead-headerFrameSize(&pub)+pad)
		frame = headerFrameSize(&pub)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		if err := ch.PublishWithContext(ctx, "", q.Name, false, false, pub); err != nil {
			t.Fatalf("publishing a header frame of %d octets: %v", frame, err)
		}
		select {
		case c := <-confirms:
			if !c.Ack {
				t.Fatalf("the broker nacked a header frame of %d octets", frame)
			}
			_, ok, err := ch.Get(q.Name, true)
			if err == nil && !ok {
				t.Fatalf("the publication with a header frame of %d octets was confirmed and is not in its queue", frame)
			}
			return frame, true, nil, err
		case e := <-closes:
			return frame, false, e, nil
		case <-ctx.Done():
			t.Fatalf("no confirm and no close for a header frame of %d octets", frame)
			return frame, false, nil, nil
		}
	}
	if frame, confirmed, closed, read := publish(0); !confirmed || read != nil {
		t.Fatalf("a header frame of %d octets, the frame size less 8: confirmed %v, read back %v, closed %v; want it "+
			"stored and read back", frame, confirmed, read, closed)
	}
	if frame, confirmed, closed, read := publish(1); !confirmed || !isFrameError(read) {
		t.Fatalf("a header frame of %d octets, one over the frame size less 8: confirmed %v, read back %v, closed %v; "+
			"want it stored, and its delivery refused by the client library", frame, confirmed, read, closed)
	}
	frame, confirmed, closed, _ := publish(frameOverhead + 1)
	if confirmed || closed == nil || closed.Code != amqp.FrameError {
		t.Fatalf("a header frame of %d octets, one over the frame size: confirmed %v, closed %v; want 501 FRAME_ERROR",
			frame, confirmed, closed)
	}
	t.Logf("%d octets: %v", frame, closed)
}

// TestTheRequeueHeadroomIsWhatRabbitMQAddsOnABroker holds [requeueHeadroom] to
// RabbitMQ 4.3, with the client library on the wire: a quorum-queue
// publication whose header frame is the frame size less 8 less the headroom
// is read back after a requeueing nack and a requeueing reject, the second
// redelivery carrying `x-acquired-count` and `x-delivery-count`; one octet
// larger, that second redelivery is a frame the library refuses with 501. So
// the 52 octets are what the two headers add, and no more.
func TestTheRequeueHeadroomIsWhatRabbitMQAddsOnABroker(t *testing.T) {
	addr := os.Getenv("IQMQ_RABBITMQ_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_RABBITMQ_ADDR (host:port) to run this against a RabbitMQ broker")
	}
	user, password := "guest", "guest"
	if u := os.Getenv("IQMQ_RABBITMQ_USER"); u != "" {
		user, password = u, os.Getenv("IQMQ_RABBITMQ_PASSWORD")
	}
	// requeue publishes a publication pad octets over the bound into a
	// quorum queue of its own and reads it three times, returning it with a
	// nack and then a reject; it reports the count headers the second
	// redelivery carried and what the third read returned.
	requeue := func(pad int) (counts []string, third error) {
		t.Helper()
		conn, err := amqp.Dial("amqp://" + user + ":" + password + "@" + addr + "/")
		if err != nil {
			t.Fatalf("dialling the broker: %v", err)
		}
		defer func() { _ = conn.Close() }()
		if v, _ := rabbit.ParseVersion(stringProp(conn.Properties, "version")); !v.AtLeast(timeoutCancels) {
			t.Skipf("RabbitMQ %v adds only x-delivery-count to a redelivery; this measures 4.3 and later", v)
		}
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		q, err := ch.QueueDeclare("iqmq-test-headroom-"+time.Now().Format("150405.000000"), true, false, false, false,
			amqp.Table{"x-queue-type": "quorum"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if c, err := conn.Channel(); err == nil {
				_, _ = c.QueueDelete(q.Name, false, false, false)
			}
		}()
		pub := amqp.Publishing{DeliveryMode: deliveryPersistent, Headers: amqp.Table{"pad": []byte{}}, Body: []byte("b")}
		pub.Headers["pad"] = make([]byte, conn.Config.FrameSize-frameOverhead-requeueHeadroom-headerFrameSize(&pub)+pad)
		if err := ch.PublishWithContext(t.Context(), "", q.Name, false, false, pub); err != nil {
			t.Fatal(err)
		}
		get := func() (amqp.Delivery, error) {
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				d, ok, err := ch.Get(q.Name, false)
				if err != nil || ok {
					return d, err
				}
			}
			t.Fatal("the message did not come back within 5s")
			return amqp.Delivery{}, nil
		}
		d, err := get()
		if err != nil {
			t.Fatalf("the first delivery: %v", err)
		}
		if err := d.Nack(false, true); err != nil {
			t.Fatal(err)
		}
		if d, err = get(); err != nil {
			t.Fatalf("the redelivery after a requeueing nack: %v", err)
		}
		if err := d.Reject(true); err != nil {
			t.Fatal(err)
		}
		d, err = get()
		for _, k := range []string{"x-acquired-count", "x-delivery-count"} {
			if _, ok := d.Headers[k]; ok {
				counts = append(counts, k)
			}
		}
		return counts, err
	}
	counts, third := requeue(0)
	if third != nil || len(counts) != 2 {
		t.Fatalf("a publication at the bound, after a nack and a reject: read %v, count headers %v; want it read "+
			"with both", third, counts)
	}
	if _, third := requeue(1); !isFrameError(third) {
		t.Fatalf("a publication one octet over the bound, after a nack and a reject: read %v, want 501 from the "+
			"client library: the headroom is larger than what the two headers add", third)
	}
}

// TestDeadLetterGrowthIsTheMeasuredSize holds [deadLetterGrowth] to the
// review's measurement on RabbitMQ 4.3.5: a park into the delay queue
// `iqmq-rv10-dmy83sa8wcmo.orders.workers.delay.400` (47 octets) came back 509
// octets larger than the delivery it parked, of which 20 are `x-iqmq-attempt`
// and 18 `x-iqmq-pub` (a two-octet token), so the dead-lettering added 471.
// A copy that came back, parked again, grows as each version treats the death
// headers it carries, and by the larger of the two when the version is not
// known.
func TestDeadLetterGrowthIsTheMeasuredSize(t *testing.T) {
	q := "iqmq-rv10-dmy83sa8wcmo.orders.workers.delay.400"
	v313, v43 := rabbit.Version{Major: 3, Minor: 13, Patch: 7}, rabbit.Version{Major: 4, Minor: 3, Patch: 5}
	h := amqp.Table{"h": make([]byte, 100), AttemptHeader: int32(1), ConfirmHeader: "p1"}
	for _, v := range []rabbit.Version{v313, v43, {}} {
		if got := deadLetterGrowth(h, q, "", q, v); got != 471 {
			t.Errorf("RabbitMQ %v: the growth of a first park into a %d-octet queue = %d, want 471", v, len(q), got)
		}
	}
	back := amqp.Table{
		"h": make([]byte, 100), AttemptHeader: int32(1), ConfirmHeader: "p1",
		deathHeader: []any{amqp.Table{
			"count": int64(1), "reason": deathExpired, "queue": q, "time": time.Unix(1_700_000_000, 0),
			"exchange": "", "routing-keys": []any{q},
		}},
		firstDeathReason: deathExpired, firstDeathQueue: q, firstDeathExchange: "",
		lastDeathReason: deathExpired, lastDeathQueue: q, lastDeathExchange: "",
	}
	other := q[:len(q)-3] + "1600"
	shorter := q[:len(q)-1]
	entry := deathEntrySize(other, "", other)
	for _, c := range []struct {
		v     rabbit.Version
		queue string
		want  int
		why   string
	}{
		// 4.1 and later rebuild all seven headers.
		{v43, q, 0, "4.3, the same queue: the same headers"},
		{v43, other, 4, "4.3, another queue: one octet more in the entry's queue and key, and in both death queues"},
		{v43, shorter, -4, "4.3, a shorter name: one octet less in each"},
		// 3.13 keeps x-death and the first-death headers.
		{v313, q, 0, "3.13, the same queue: the entry's count raised in place"},
		{v313, other, entry + 1, "3.13, another queue: a second entry, and one octet more of the last-death queue"},
		{v313, shorter, deathEntrySize(shorter, "", shorter) - 1, "3.13, a shorter name: a second entry"},
		// Not known: the larger.
		{rabbit.Version{}, other, entry + 1, "an unknown version, another queue"},
		{rabbit.Version{Major: 4}, shorter, deathEntrySize(shorter, "", shorter) - 1, "4.0, a shorter name"},
	} {
		if got := deadLetterGrowth(back, c.queue, "", c.queue, c.v); got != c.want {
			t.Errorf("%s: growth = %d, want %d", c.why, got, c.want)
		}
	}
}

// TestDeadLetterGrowthIsWhatRabbitMQAddsOnABroker holds [deadLetterGrowth] to
// RabbitMQ with the client library on the wire: a publication into a queue of
// the delay queues' shape (classic, `x-message-ttl`, dead-lettering to the
// default exchange) comes back, from the queue it is dead-lettered to, in a
// frame exactly the published frame plus the growth for the broker's version,
// for a queue name of 20 and of 200 octets; published again, as a second park
// is, into the same queue, into one of a longer name and into one of a shorter
// name, it comes back exactly as predicted again; and the growth for a version
// not known is never less than what the broker added.
func TestDeadLetterGrowthIsWhatRabbitMQAddsOnABroker(t *testing.T) {
	addr := os.Getenv("IQMQ_RABBITMQ_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_RABBITMQ_ADDR (host:port) to run this against a RabbitMQ broker")
	}
	user, password := "guest", "guest"
	if u := os.Getenv("IQMQ_RABBITMQ_USER"); u != "" {
		user, password = u, os.Getenv("IQMQ_RABBITMQ_PASSWORD")
	}
	conn, err := amqp.Dial("amqp://" + user + ":" + password + "@" + addr + "/")
	if err != nil {
		t.Fatalf("dialling the broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	v, _ := rabbit.ParseVersion(stringProp(conn.Properties, "version"))
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
	target := "iqmq-test-growth-target-" + stamp
	if _, err := ch.QueueDeclare(target, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	var queues []string
	t.Cleanup(func() {
		if c, err := conn.Channel(); err == nil {
			for _, q := range append(queues, target) {
				_, _ = c.QueueDelete(q, false, false, false)
			}
		}
	})
	delayQueue := func(n int) string {
		name := "g" + stamp + "-"
		for len(name) < n {
			name += "d"
		}
		name = name[:n]
		args := amqp.Table{
			"x-queue-type": "classic", "x-message-ttl": int64(50),
			"x-dead-letter-exchange": "", "x-dead-letter-routing-key": target,
		}
		if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
			t.Fatal(err)
		}
		queues = append(queues, name)
		return name
	}
	// park publishes pub into queue q and reads what comes back in target,
	// reporting its frame against the published frame plus the growth.
	park := func(pub amqp.Publishing, q string) (amqp.Publishing, int, int) {
		t.Helper()
		want := headerFrameSize(&pub) + deadLetterGrowth(pub.Headers, q, "", q, v)
		bound := headerFrameSize(&pub) + deadLetterGrowth(pub.Headers, q, "", q, rabbit.Version{})
		defer func() {
			if bound < want {
				t.Errorf("into %s: the growth for a version not known is %d octets less than for %v",
					q, want-bound, v)
			}
		}()
		if err := ch.PublishWithContext(t.Context(), "", q, false, false, pub); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			d, ok, err := ch.Get(target, true)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				back := deliveredPublishing(&d)
				got := headerFrameSize(&back)
				return back, got, want
			}
		}
		t.Fatalf("nothing came back from %s", q)
		return amqp.Publishing{}, 0, 0
	}
	for _, n := range []int{20, 200} {
		q := delayQueue(n)
		pub := amqp.Publishing{
			DeliveryMode: deliveryPersistent, Body: []byte("b"),
			Headers: amqp.Table{"h": make([]byte, 100), AttemptHeader: int32(1), ConfirmHeader: "p12"},
		}
		back, got, want := park(pub, q)
		if got != want {
			t.Errorf("a park into a %d-octet queue came back in %d octets, want %d (published %d)",
				n, got, want, headerFrameSize(&pub))
		}
		again, got, want := park(back, q)
		if got != want {
			t.Errorf("a second park into the same %d-octet queue came back in %d octets, want %d", n, got, want)
		}
		third, got, want := park(again, delayQueue(n+7))
		if got != want {
			t.Errorf("a third park, into a queue of %d octets, came back in %d octets, want %d", n+7, got, want)
		}
		if _, got, want := park(third, delayQueue(n-3)); got != want {
			t.Errorf("a fourth park, into a queue of %d octets, came back in %d octets, want %d", n-3, got, want)
		}
	}
}

// deliveredPublishing is a delivery's properties as the publication that would
// carry them, for its frame.
func deliveredPublishing(d *amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		Headers: d.Headers, ContentType: d.ContentType, ContentEncoding: d.ContentEncoding,
		DeliveryMode: d.DeliveryMode, Priority: d.Priority, CorrelationId: d.CorrelationId, ReplyTo: d.ReplyTo,
		Expiration: d.Expiration, MessageId: d.MessageId, Timestamp: d.Timestamp, Type: d.Type, UserId: d.UserId,
		AppId: d.AppId, Body: d.Body,
	}
}

// isFrameError reports whether err is a 501 FRAME_ERROR, which the client
// library answers a frame too large to read with.
func isFrameError(err error) bool {
	aerr, ok := errors.AsType[*amqp.Error](err)
	return ok && aerr.Code == amqp.FrameError
}
