package opbench

import (
	"testing"
)

// CheckSession is the correctness gate for one arm: at every canonical payload
// size it round-trips a single message and a full batch through a freshly
// opened session, exactly as the benchmark leaves do. It catches the things
// that would otherwise show up as a mysterious benchmark failure -- a batch
// above the broker's request limit, a payload the broker rejects, a consumer
// that never joins, a settle call that errors -- without measuring anything,
// so it needs no benchmark lock.
//
// Both arms of the overhead comparison run it, for the same reason they share
// the loop: a driver arm that could not round-trip and a raw-client arm that
// could would produce an overhead figure whose numerator is a different
// experiment.
func CheckSession(t *testing.T, open Open) {
	t.Helper()
	for _, size := range Sizes() {
		t.Run(size.Name, func(t *testing.T) {
			ctx := Context(t)
			for _, op := range []string{"EndToEnd", "PublishBatch"} {
				s := open(t, Params{Op: op, Size: size})
				switch op {
				case "EndToEnd":
					if err := s.Publish(ctx, Payload(size.Bytes)); err != nil {
						t.Fatalf("publish: %v", err)
					}
					if err := s.Receive(ctx, 1); err != nil {
						t.Fatalf("receive: %v", err)
					}
				case "PublishBatch":
					batch := Payloads(BatchSize, size.Bytes)
					if err := s.PublishBatch(ctx, batch); err != nil {
						t.Fatalf("publish batch of %d: %v", BatchSize, err)
					}
					if err := s.Receive(ctx, BatchSize); err != nil {
						t.Fatalf("receive batch of %d: %v", BatchSize, err)
					}
				}
				if err := s.Close(); err != nil {
					t.Fatalf("close after %s: %v", op, err)
				}
			}
		})
	}
}
