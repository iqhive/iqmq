package stompsrv

import (
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
)

// TestASilentClientIsDroppedAndAHeartBeatingOneIsNot pins the two facilities a
// heart-beat test in the module above stands on: the server closes a client
// that has sent nothing for [Options.DropSilentClients], as a broker does, and
// counts the heart-beats of one that keeps sending them, per connection.
//
// Both halves are needed. A server that never dropped would let a driver whose
// heart-beats stopped pass, and one that dropped a client still heart-beating
// would fail a driver that is right -- and a count that was not per
// connection would let a backend's heart-beats stand in for a stalled
// source's.
func TestASilentClientIsDroppedAndAHeartBeatingOneIsNot(t *testing.T) {
	t.Parallel()
	const (
		silence = 200 * time.Millisecond
		beat    = 20 * time.Millisecond
		beats   = 30
	)
	srv, err := New(Options{WantHeartBeats: beat, DropSilentClients: silence})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		for _, v := range srv.Violations() {
			t.Errorf("violation: %v", v)
		}
	})
	connect := frame.New(frame.CONNECT, frame.AcceptVersion, "1.2", frame.Host, "localhost")
	silent := newScriptedClient(t, srv)
	t.Cleanup(silent.stop)
	silent.write(connect)
	// Each connection has its own reader, so the second client dials only
	// once the first one's CONNECT is in the log: that is what makes the
	// first CONNECT the silent client's.
	if err := srv.WaitFrames(frame.CONNECT, 1, testBudget); err != nil {
		t.Fatal(err)
	}
	beating := newScriptedClient(t, srv)
	t.Cleanup(beating.stop)
	beating.write(connect)
	if err := srv.WaitFrames(frame.CONNECT, 2, testBudget); err != nil {
		t.Fatal(err)
	}
	connects := srv.FramesOf(frame.CONNECT)

	// Three times the silence, a heart-beat at a time, from this goroutine:
	// the beating client is never quiet for longer than one beat, and the
	// silent one is quiet for all of it.
	for range beats {
		beating.raw([]byte("\n"))
		time.Sleep(beat)
	}

	select {
	case <-silent.done:
	case <-time.After(testBudget):
		t.Fatalf("a client that sent nothing for %v after its CONNECT was not dropped", beats*beat)
	}
	select {
	case <-beating.done:
		t.Fatal("a client that heart-beat every " + beat.String() + " was dropped")
	default:
	}
	if got := srv.SilentDrops(); got != 1 {
		t.Errorf("SilentDrops is %d, want 1", got)
	}
	if err := srv.WaitHeartBeats(connects[1].Conn, beats, testBudget); err != nil {
		t.Errorf("%v: the beating client's connection counted %d", err, srv.HeartBeats(connects[1].Conn))
	}
	if got := srv.HeartBeats(connects[0].Conn); got != 0 {
		t.Errorf("the silent client's connection counted %d heart-beats, want 0: they are the other "+
			"connection's", got)
	}
}
