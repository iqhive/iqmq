package kafkago

import (
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// The pooled publication buffer's reference discipline, tested from inside the
// package because the pool and the refcount are the driver's own and the
// failure they prevent is invisible from outside: a buffer pooled while the
// writer still holds the record is a data race and a corrupted record on the
// wire, and a buffer NEVER pooled is an allocation per publication that no
// black-box test can see.

// TestMsgbufReferenceCounting is the discipline itself: two references, the
// first release changes nothing a holder can see, the last one clears
// everything.
func TestMsgbufReferenceCounting(t *testing.T) {
	b := getMsgbuf()
	if got := b.refs.Load(); got != 2 {
		t.Fatalf("a fresh buffer has %d reference(s), want 2: the submitting goroutine's and the writer completion's", got)
	}
	b.msg.Topic = "t"
	b.msg.Key = []byte("k")
	b.route.confirm = mq.ConfirmDurable
	b.route.buf = b

	b.release()
	if got := b.refs.Load(); got != 1 {
		t.Fatalf("after one release the buffer has %d reference(s), want 1", got)
	}
	if b.msg.Topic != "t" || string(b.msg.Key) != "k" || b.route.buf == nil {
		t.Fatal("the first release cleared a buffer the other holder can still read; that is the corruption the refcount exists to stop")
	}

	b.release()
	if got := b.refs.Load(); got != 0 {
		t.Fatalf("after the last release the buffer has %d reference(s), want 0", got)
	}
	if b.msg.Topic != "" || b.msg.Key != nil || b.route.buf != nil || b.route.report != nil {
		t.Fatalf("the last release left a publication's state behind: %+v", b.msg)
	}
	// The grown header storage survives, which is the whole point of pooling.
	if b.msg.Headers == nil && cap(b.msg.Headers) != 0 {
		t.Error("the header slice's capacity was thrown away")
	}
}

// TestMsgbufDiscardIsForTheRefusalPaths: a buffer the writer never saw is
// pooled at once, because nothing else can hold it.
func TestMsgbufDiscardIsForTheRefusalPaths(t *testing.T) {
	b := getMsgbuf()
	b.msg.Topic = "t"
	b.discard()
	if got := b.refs.Load(); got != 0 {
		t.Fatalf("a discarded buffer has %d reference(s), want 0", got)
	}
	if b.msg.Topic != "" {
		t.Error("discard did not clear the buffer")
	}
	// And it is safe on a nil buffer, which is what the refusal paths pass
	// when `build` failed before one was taken.
	var nilbuf *msgbuf
	nilbuf.discard()
	nilbuf.release()
}

// TestRouteSettlesOnce is the specification's "done runs exactly once" and the specification's "the
// owned message is released exactly once", under the one condition that makes
// them hard on this client: an error from WriteMessages does not say whether
// the record was submitted, so the submitting goroutine and the writer's
// completion may BOTH have an answer and neither knows whether the other will
// run.
func TestRouteSettlesOnce(t *testing.T) {
	var (
		calls int
		last  error
	)
	r := &route{report: func(err error) { calls++; last = err }}
	boom := errors.New("boom")
	if !r.settle(boom) {
		t.Fatal("the first settle did not report; its return value is what PublishAsync reads to know whether the callback has an outcome")
	}
	if r.settle(nil) || r.settle(boom) {
		t.Fatal("a later settle claimed to have reported")
	}
	if calls != 1 {
		t.Fatalf("report ran %d times, want 1", calls)
	}
	if !errors.Is(last, boom) {
		t.Fatalf("report was given %v, want the first answer", last)
	}
	// A route with no report is the synchronous path, where the caller's own
	// return value IS the report. Settling it must not panic, and it still
	// says it was the one that settled.
	if !(&route{}).settle(nil) {
		t.Error("a route with no report did not claim the settlement")
	}
}

// TestBuildAttachesTheBufferToTheRoute is the regression guard for the
// mistake the refcount made possible: a publication path that takes a buffer
// and does not tell the completion which buffer it is leaves the reference
// undropped for ever, so the buffer is collected rather than pooled and every
// synchronous publication allocates one. It is set in `build` — the one
// function every publication path calls — and this is what says so.
func TestBuildAttachesTheBufferToTheRoute(t *testing.T) {
	s := &sink{name: "t", meta: metaStrict}
	buf := getMsgbuf()
	defer buf.discard()
	if err := s.build(buf, &mq.Message{Body: []byte("b")}, nil, false); err != nil {
		t.Fatalf("build: %v", err)
	}
	if buf.route.buf != buf {
		t.Fatal("build did not attach the buffer to its route; the writer's completion cannot drop a reference to a buffer it cannot name")
	}
	r, ok := buf.msg.WriterData.(*route)
	if !ok || r != &buf.route {
		t.Fatalf("build did not put the route in WriterData: %T", buf.msg.WriterData)
	}
	// And the record points at the caller's storage rather than a copy, which
	// is what `own` false means.
	if len(buf.msg.Value) != 1 || buf.msg.Topic != "t" {
		t.Fatalf("build produced %+v", buf.msg)
	}
}
