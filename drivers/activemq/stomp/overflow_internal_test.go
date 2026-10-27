package stomp

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	"github.com/iqhive/iqmq/internal/activemq"
)

// closedStop is a stop channel that is already closed. A put given one returns
// false exactly when it would otherwise WAIT, because the room check comes
// before the wait, which makes "the queue is at its bound" an assertion with no
// clock in it.
func closedStop() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// message is a MESSAGE as go-stomp hands it over, with a body of n bytes.
func message(n int) *gostomp.Message {
	h := frame.NewHeader(frame.Destination, "/queue/orders", frame.Subscription, "1", frame.Ack, "a-"+strconv.Itoa(n))
	return &gostomp.Message{Header: h, Body: bytes.Repeat([]byte("x"), n)}
}

// TestOverflowStopsAtTheBrokersWindowAndNotBefore pins the bound in both of the
// brokers' units, and the side of it each unit stops on.
//
// The bound has to be one an honest broker cannot reach, or a full buffer
// stops the heart-beats again. Classic sends only while fewer than its
// prefetch are unacknowledged, so the queue takes up to the prefetch; Artemis
// sends while its byte credit is still positive, so the queue takes a frame
// whenever it holds less than the window -- which can leave it one frame past
// the window, exactly as the broker's own accounting does.
func TestOverflowStopsAtTheBrokersWindowAndNotBefore(t *testing.T) {
	t.Parallel()

	t.Run("classic counts messages", func(t *testing.T) {
		t.Parallel()
		q := newOverflow(3, 0)
		for i := range 3 {
			if !q.put(message(10_000), closedStop()) {
				t.Fatalf("put %d of a prefetch of 3 waited; Classic sends that many and a drain that waits "+
					"for room stops go-stomp's loop", i+1)
			}
		}
		if q.put(message(1), closedStop()) {
			t.Fatal("a fourth put into a prefetch of 3 was taken; a queue that outgrows the window it asked " +
				"the broker for is bounded by nothing")
		}
		if _, ok := q.take(nil); !ok {
			t.Fatal("take found nothing in a full queue")
		}
		if !q.put(message(1), closedStop()) {
			t.Fatal("a take freed no room")
		}
	})

	t.Run("artemis counts frame bytes", func(t *testing.T) {
		t.Parallel()
		size := frameSize(message(100))
		q := newOverflow(0, 2*size+1)
		for i := range 3 {
			if !q.put(message(100), closedStop()) {
				t.Fatalf("put %d waited with %d of %d bytes queued; Artemis sends while its credit is "+
					"positive, so the queue has to take a frame whenever it is below the window",
					i+1, int64(i)*size, 2*size+1)
			}
		}
		if q.put(message(100), closedStop()) {
			t.Fatalf("a put was taken with %d bytes queued against a window of %d", 3*size, 2*size+1)
		}
	})

	t.Run("one frame larger than the window", func(t *testing.T) {
		t.Parallel()
		q := newOverflow(0, 10)
		if !q.put(message(1_000), closedStop()) {
			t.Fatal("an empty queue refused a frame larger than the window; the broker sends one such frame " +
				"to a consumer with any credit at all, and a drain that waited for room would wait for ever")
		}
		if q.put(message(1), closedStop()) {
			t.Fatal("a second frame was taken past the window")
		}
	})
}

// TestOverflowHandsATerminalErrorOverFirst pins the one reordering the queue
// does. A terminal message on the live subscription means go-stomp has ended
// the connection, so every MESSAGE queued ahead of it is on a socket that is
// gone; handed out, each would cost a caller a lost lease before the error
// that explains them.
func TestOverflowHandsATerminalErrorOverFirst(t *testing.T) {
	t.Parallel()
	q := newOverflow(3, 0)
	for range 3 {
		q.put(message(10), nil)
	}
	terminal := &gostomp.Message{Err: errors.New("connection closed")}
	if !q.put(terminal, closedStop()) {
		t.Fatal("a full queue refused a terminal message; it carries no delivery and the broker counts nothing for it")
	}
	after := message(20)
	if !q.put(after, closedStop()) {
		t.Fatal("the queue was still full after the terminal message dropped what it held")
	}
	if m, _ := q.take(nil); m != terminal {
		t.Fatalf("take returned %+v first, want the terminal message", m)
	}
	if m, _ := q.take(nil); m != after {
		t.Fatalf("take returned %+v second, want the message put after the terminal one", m)
	}
	if q.count != 0 || q.bytes != 0 {
		t.Errorf("an empty queue counts %d messages and %d bytes", q.count, q.bytes)
	}
}

// TestOverflowEndsAndFinishes pins the three ways out, each of which a pump or
// a drain parked on the queue depends on to exit.
func TestOverflowEndsAndFinishes(t *testing.T) {
	t.Parallel()

	t.Run("an ended queue is taken to the end", func(t *testing.T) {
		t.Parallel()
		q := newOverflow(0, 0)
		q.put(message(1), nil)
		q.end()
		if _, ok := q.take(nil); !ok {
			t.Fatal("the drain's end dropped what it had queued; the pump takes what is left before it stops")
		}
		if _, ok := q.take(nil); ok {
			t.Fatal("take on an ended, empty queue reported a message")
		}
	})

	t.Run("a stopped take returns", func(t *testing.T) {
		t.Parallel()
		if _, ok := newOverflow(0, 0).take(closedStop()); ok {
			t.Fatal("take with stopPumps closed reported a message")
		}
	})

	t.Run("finish wakes a drain waiting for room", func(t *testing.T) {
		t.Parallel()
		q := newOverflow(1, 0)
		q.put(message(1), nil)
		got := make(chan bool, 1)
		go func() { got <- q.put(message(1), nil) }()
		q.finish()
		select {
		case ok := <-got:
			if ok {
				t.Fatal("a put into a queue whose pump had finished was taken")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("finish did not wake a put waiting for room; the drain would never stop, and Close's " +
				"wait for it would never return")
		}
		select {
		case <-q.done():
		default:
			t.Fatal("done is not closed after finish")
		}
	})

	t.Run("a take wakes a drain waiting for room", func(t *testing.T) {
		t.Parallel()
		q := newOverflow(1, 0)
		q.put(message(1), nil)
		got := make(chan bool, 1)
		go func() { got <- q.put(message(2), nil) }()
		if _, ok := q.take(nil); !ok {
			t.Fatal("take found nothing in a full queue")
		}
		select {
		case ok := <-got:
			if !ok {
				t.Fatal("the waiting put gave up")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a take did not wake the put waiting for the room it made")
		}
	})
}

// TestFrameSizeNeverOvercounts pins the direction of the estimate. Artemis
// charges a consumer's window the frame it WROTE, escapes and all, and the
// queue's byte bound is only unreachable for an honest broker while the
// estimate stays at or under that.
func TestFrameSizeNeverOvercounts(t *testing.T) {
	t.Parallel()
	// Seven characters in these headers are escaped on the wire, each one
	// written as two bytes and handed over by go-stomp as one: the two colons
	// and the newline and backslash of the message-id, the colon of the key,
	// and the newline and carriage return of its value.
	const escaped = 7
	f := frame.New(frame.MESSAGE,
		frame.Destination, "/queue/orders",
		frame.Subscription, "1",
		frame.MessageId, "ID:a:b\nc\\d",
		frame.Ack, "a-1",
		"colon:key", "multi\nline\rvalue",
	)
	// With its `content-length`, which Artemis writes on every MESSAGE with
	// a body and without which a body holding a NUL ends at the NUL.
	f.Body = []byte("body\x00with a null")
	f.Header.Set(frame.ContentLength, strconv.Itoa(len(f.Body)))
	var wire bytes.Buffer
	if err := frame.NewWriter(&wire).Write(f); err != nil {
		t.Fatalf("writing the frame: %v", err)
	}
	written := int64(wire.Len())
	read, err := frame.NewReader(bytes.NewReader(wire.Bytes())).Read()
	if err != nil {
		t.Fatalf("reading the frame back: %v", err)
	}
	got := frameSize(&gostomp.Message{Header: read.Header, Body: read.Body})
	if got > written {
		t.Fatalf("frameSize is %d for a frame of %d bytes on the wire; an estimate over the broker's charge "+
			"makes the queue's bound reachable by a broker that honours its window", got, written)
	}
	if got != written-escaped {
		t.Errorf("frameSize is %d for a frame of %d bytes on the wire, want %d: short by the escapes and "+
			"nothing else", got, written, written-escaped)
	}
}

// TestOverflowBoundsAreTheWindowEachBrokerWasAskedFor pins that the queue is
// bounded by the same numbers [Source.subscribeOptions] sends, in each
// broker's own unit.
func TestOverflowBoundsAreTheWindowEachBrokerWasAskedFor(t *testing.T) {
	t.Parallel()
	classic := &Source{b: &Backend{cfg: Config{Broker: activemq.Classic, ConsumerWindow: 4096, PrefetchBytes: 1}}, prefetch: 7}
	if n, b := classic.overflowBounds(); n != 7 || b != 0 {
		t.Errorf("Classic's overflow is bounded at %d messages and %d bytes, want 7 and none: its window is "+
			"`activemq.prefetchSize`, a count, and it has no byte window to bound by", n, b)
	}
	artemis := &Source{b: &Backend{cfg: Config{Broker: activemq.Artemis, ConsumerWindow: 4096, PrefetchBytes: 1}}, prefetch: 7}
	if n, b := artemis.overflowBounds(); n != 0 || b != 4096 {
		t.Errorf("Artemis's overflow is bounded at %d messages and %d bytes, want none and 4096: its window is "+
			"`consumer-window-size`, in bytes, and it sends any number of messages inside it", n, b)
	}
}
