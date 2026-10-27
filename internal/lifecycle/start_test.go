package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

// startWait bounds every wait below: StartClose exists so that a caller the
// run may be waiting for does not wait for the run, and a regression must fail
// rather than hang the package.
const startWait = 10 * time.Second

// TestStartCloseDoesNotWaitForTheRun pins StartClose's three promises: it
// returns while the run is still going, with nil because there is no outcome
// yet; a publication is refused from the moment it returns, whether or not
// the run's goroutine has been scheduled; and once the run has finished it
// answers what Close answers.
func TestStartCloseDoesNotWaitForTheRun(t *testing.T) {
	release := make(chan struct{})
	want := errors.New("native close failed")
	l := New(Config{Steps: Steps{CloseNative: func() error {
		<-release
		return want
	}}})
	if err := l.StartClose(); err != nil {
		t.Fatalf("StartClose returned %v while the run was still going, want nil", err)
	}
	if !l.Closing() {
		t.Fatal("Closing() is false after StartClose returned")
	}
	if tk := l.Enter(OpPublish); tk.OK() {
		l.Leave(tk)
		t.Fatal("a publication was admitted after StartClose returned")
	}
	select {
	case <-l.Done():
		t.Fatal("the run finished before its CloseNative step returned")
	default:
	}
	close(release)
	select {
	case <-l.Done():
	case <-time.After(startWait):
		t.Fatal("the run StartClose began never finished")
	}
	if err := l.StartClose(); !errors.Is(err, want) {
		t.Fatalf("StartClose after the run finished returned %v, want the run's %v", err, want)
	}
	if err := l.Close(); !errors.Is(err, want) {
		t.Fatalf("Close after StartClose returned %v, want the run's %v", err, want)
	}
}

// TestStartCloseFromInsideTheRun is the caller StartClose is for: code the
// run itself is executing — here a step, standing in for a callback a step
// delivers — that a Close would leave waiting for the run it is part of. It
// must not move the phase back either: the run is further along than
// StopAccept by the time Commit runs.
func TestStartCloseFromInsideTheRun(t *testing.T) {
	type inner struct {
		closeErr, shutdownErr error
		phase                 Phase
	}
	got := make(chan inner, 1)
	var l *Lifecycle
	l = New(Config{Steps: Steps{Commit: func() error {
		var in inner
		in.closeErr = l.StartClose()
		in.shutdownErr = l.StartShutdown(context.Background())
		in.phase = l.Phase()
		got <- in
		return nil
	}}})
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(startWait):
		t.Fatal("Close did not return: a step's StartClose waited for the run it is part of")
	}
	in := <-got
	if in.closeErr != nil || in.shutdownErr != nil {
		t.Fatalf("StartClose and StartShutdown inside the run returned %v and %v, want nil", in.closeErr, in.shutdownErr)
	}
	if in.phase != Committing {
		t.Fatalf("the phase inside Commit was %v after StartClose, want %v: it moved backwards", in.phase, Committing)
	}
}

// TestStartShutdownRunsTheGracefulRun pins that StartShutdown begins the run
// Shutdown performs and not Close's: the waiting steps run.
func TestStartShutdownRunsTheGracefulRun(t *testing.T) {
	s, r := steps()
	l := New(Config{Steps: s})
	if err := l.StartShutdown(context.Background()); err != nil {
		t.Fatalf("StartShutdown: %v", err)
	}
	select {
	case <-l.Done():
	case <-time.After(startWait):
		t.Fatal("the run StartShutdown began never finished")
	}
	const want = "stopaccept,stopfetch,flush,drain,settle,commit,closenative"
	if got := r.joined(); got != want {
		t.Fatalf("StartShutdown ran %q, want the graceful run %q", got, want)
	}
}
