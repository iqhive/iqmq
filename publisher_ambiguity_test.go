package iqmq_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/middleware"
)

// Every batch surface of the publisher answers one question the same way:
// when none of the batch was accepted, is the single error it returns every
// message's outcome? The specification gives each message one of three outcomes, and a
// batch whose first message was refused and whose last may already be in the
// broker has two of them. PublishBatch, PublishItems and PublishBatchOwned
// each collapsed such a batch to its first error, so mq.IsAmbiguous was false
// and a caller who republished the batch duplicated the last message.
//
// FuzzBatchResults drives PublishBatch through the SPI fake with arbitrary
// patterns; this test holds the three surfaces, over both driver shapes, to
// the same four patterns, because the three are three separate
// implementations of the rule.

// batchSurfaces are the publisher's batch methods, each publishing one
// message per body.
var batchSurfaces = map[string]func(ctx context.Context, p *mq.PublisherHandle, bodies []string) error{
	"PublishBatch": func(ctx context.Context, p *mq.PublisherHandle, bodies []string) error {
		ms := make([]*mq.Message, len(bodies))
		for i, b := range bodies {
			ms[i] = &mq.Message{Body: []byte(b)}
		}
		return p.PublishBatch(ctx, ms)
	},
	"PublishItems": func(ctx context.Context, p *mq.PublisherHandle, bodies []string) error {
		return p.PublishItems(ctx, publishItems(bodies...))
	},
	"PublishBatchOwned": func(ctx context.Context, p *mq.PublisherHandle, bodies []string) error {
		oms := make([]mq.OwnedMessage, len(bodies))
		for i, b := range bodies {
			oms[i] = mq.OwnedMessage{Message: &mq.Message{Body: []byte(b)}, Release: func(*mq.Message) {}}
		}
		return p.PublishBatchOwned(ctx, oms)
	},
}

func TestEveryBatchSurfaceKeepsAnAmbiguousMessage(t *testing.T) {
	// Answers the way a driver gives them : a FRESH
	// *OpError per message from the real mapper, and a refusal with the SAME
	// code as the ambiguous answer -- MapNet(io.EOF) before and after the
	// write. With one shared errors.New refusal, "compare codes" and
	// "compare identity" passed this test, and so did an IsAmbiguous that
	// stopped at the first *OpError in the batch.
	mapper := &driverkit.ErrorMapper{Driver: "adv"}
	refused := errors.New("the broker refused this message")
	if a, r := mapper.MapNet(mq.OpPublish, "orders", "", io.EOF, true), mapper.MapNet(mq.OpPublish, "orders", "", io.EOF, false); !mq.IsAmbiguous(a) || mq.IsAmbiguous(r) || mq.CodeOf(a) != mq.CodeOf(r) {
		t.Fatalf("the instrument is broken: %v and %v are not one code, ambiguous and definite", a, r)
	}
	fail := func(_ int, m *mq.Message) error {
		switch string(m.Body) {
		case "refuse":
			return mapper.MapNet(mq.OpPublish, "orders", "", fmt.Errorf("%w: %w", refused, io.EOF), false)
		case "ambiguous":
			return mapper.MapNet(mq.OpPublish, "orders", "", io.EOF, true)
		case "codeonly":
			// Ambiguity in the code alone: what MapNet made of
			// drivers/redis's short WAITAOF before ClassifyNet read the
			// code, and a shape any driver can build by hand.
			return &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeAmbiguous}
		}
		return nil
	}
	patterns := []struct {
		bodies []string
		// mixed says the outcomes differ, so the answer is per message.
		mixed bool
	}{
		{[]string{"refuse", "refuse", "refuse", "ambiguous"}, true},
		{[]string{"ambiguous", "refuse"}, true},
		{[]string{"refuse", "refuse"}, false},
		{[]string{"ambiguous", "ambiguous", "ambiguous"}, false},
		{[]string{"refuse", "codeonly"}, true},
		{[]string{"codeonly", "refuse", "refuse"}, true},
		{[]string{"codeonly", "ambiguous"}, false},
	}
	shapes := map[string]func(*advBroker, mq.SinkConfig) mq.Sink{"plain": openPlain, "batch": openBatch}
	for surface, publish := range batchSurfaces {
		for shape, open := range shapes {
			for _, pat := range patterns {
				name := surface + "/" + shape + "/" + strings.Join(pat.bodies, ",")
				t.Run(name, func(t *testing.T) {
					b, p := advClient(t, open, mq.PublisherConfig{})
					b.setFail(fail)
					err := publish(context.Background(), p, pat.bodies)
					if err == nil {
						t.Fatal("the batch succeeded; every message was meant to fail")
					}
					anyAmbiguous, anyRefused := false, false
					for _, body := range pat.bodies {
						anyAmbiguous = anyAmbiguous || body == "ambiguous" || body == "codeonly"
						anyRefused = anyRefused || body == "refuse"
					}
					if got := mq.IsAmbiguous(err); got != anyAmbiguous {
						t.Fatalf("%v: IsAmbiguous = %v, want %v: a message that may be in the broker must never be "+
							"reported as refused ", err, got, anyAmbiguous)
					}
					if got := errors.Is(err, refused); got != anyRefused {
						t.Fatalf("%v: errors.Is(refused) = %v, want %v", err, got, anyRefused)
					}
					var be *mq.BatchError
					if !pat.mixed {
						if errors.As(err, &be) {
							t.Fatalf("%v: want the single error; every message has the same outcome", err)
						}
						return
					}
					if !errors.As(err, &be) {
						t.Fatalf("%v: want a *BatchError; the messages' outcomes differ and one error cannot say both", err)
					}
					if len(be.Errs) != len(pat.bodies) {
						t.Fatalf("%v carries %d results for %d messages", err, len(be.Errs), len(pat.bodies))
					}
					for i, itemErr := range be.Errs {
						if itemErr == nil {
							t.Fatalf("message %d is reported as accepted: %v", i, be.Errs)
						}
						if got, want := mq.IsAmbiguous(itemErr), pat.bodies[i] != "refuse"; got != want {
							t.Fatalf("message %d (%s) is reported as %v: IsAmbiguous = %v, want %v",
								i, pat.bodies[i], itemErr, got, want)
						}
					}
					if st := p.Stats(); st.Published != 0 || st.Failed != int64(len(pat.bodies)) {
						t.Fatalf("Stats = %+v, want 0 published and %d failed", st, len(pat.bodies))
					}
				})
			}
		}
	}
}

// blameNoneSink is a native batcher whose PublishBatch fails without blaming
// any message: it answers with a fixed error that is, or wraps, a *BatchError
// with no failed entry.
type blameNoneSink struct {
	advSink
	err error
}

func (s *blameNoneSink) PublishBatch(context.Context, []*mq.Message, *mq.PublishOptions) error {
	return s.err
}

// TestADriverFailureThatBlamesNoMessageIsNeverASuccess is the other end of
// the same rule: the driver reported a failure and attached it to no message
// -- a *BatchError none of whose entries failed, or one whose entries are not
// aligned with the piece it was given.
//
// PublishItems (and the batching flusher, through the same sendAligned)
// copied a blameless *BatchError's nil entries as per-item results and
// reported the batch as ACCEPTED. The first fix stopped that, and then handed
// the same blameless *BatchError back to the caller as every message's error
// (the review's correctness-2): errors.As reached a length-matched
// *BatchError whose entries all read "accepted", so a caller following the
// per-item contract -- cmd/iqmq's pub among them -- counted every message as
// confirmed. A *BatchError the core cannot use per item is now never handed
// on: each message gets the core's own answer, which is AMBIGUOUS, because
// the driver's answer contradicts itself or cannot be attributed and nobody
// can tell which messages it took.
func TestADriverFailureThatBlamesNoMessageIsNeverASuccess(t *testing.T) {
	refused := errors.New("refused")
	blameless := &mq.BatchError{Errs: []error{nil, nil}}
	for name, driverErr := range map[string]error{
		"a *BatchError blaming nothing":                blameless,
		"an ambiguous error around such a *BatchError": &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true, Err: blameless},
		"a definite error around such a *BatchError":   &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true, Err: blameless},
		"a *BatchError too short for the piece":        &mq.BatchError{Errs: []error{refused}},
		"a *BatchError joined to another error":        errors.Join(errors.New("flush failed"), blameless),
		"a deadline joined to such a *BatchError":      &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Code: mq.CodeDeadlineExceeded, Ambiguous: true, Err: errors.Join(context.DeadlineExceeded, blameless)},
	} {
		for _, surface := range []string{"PublishBatch", "PublishItems"} {
			t.Run(name+"/"+surface, func(t *testing.T) {
				open := func(b *advBroker, cfg mq.SinkConfig) mq.Sink {
					return &blameNoneSink{advSink: advSink{b: b, receipts: cfg.Receipts}, err: driverErr}
				}
				_, p := advClient(t, open, mq.PublisherConfig{})
				err := batchSurfaces[surface](context.Background(), p, []string{"a", "b"})
				if err == nil {
					t.Fatalf("%s = nil for a driver that reported %v: a failed call was reported as accepted", surface, driverErr)
				}
				var be *mq.BatchError
				if errors.As(err, &be) && (len(be.Errs) != 2 || be.Failed() != 2) {
					t.Fatalf("%s = %v: errors.As reaches a *BatchError with %d entries, %d of them failed -- a caller reading "+
						"it per item is told messages were accepted that nobody knows were", surface, err, len(be.Errs), be.Failed())
				}
				if !mq.IsAmbiguous(err) {
					t.Fatalf("%s = %v: IsAmbiguous = false; the driver's answer cannot say which messages it took", surface, err)
				}
				if st := p.Stats(); st.Published != 0 || st.Failed != 2 {
					t.Fatalf("Stats = %+v, want 0 published and 2 failed", st)
				}
				// The specification: context.Canceled and DeadlineExceeded stay
				// discoverable. The core's answer does not unwrap to the
				// driver's error -- errors.As would reach the unusable
				// *BatchError -- but errors.Is still finds what the driver's
				// error carried (the review's correctness-3).
				for _, target := range []error{context.Canceled, context.DeadlineExceeded} {
					if errors.Is(driverErr, target) && !errors.Is(err, target) {
						t.Fatalf("%s = %v: errors.Is(%v) is false, and the driver's error carried it", surface, err, target)
					}
				}
				if errors.Is(err, mq.ErrPartial) {
					t.Fatalf("%s = %v matches ErrPartial, and errors.As gives no *BatchError to read", surface, err)
				}
			})
		}
	}
}

// TestAnAmbiguousErrorAroundAPerItemAnswerKeepsItsAmbiguity: a driver wraps
// an aligned, partial *BatchError in an ambiguous *OpError. The entries that
// name a failure are the driver's own and stay as they are; an entry that
// says "accepted" under an error that says "nobody knows" is not accepted --
// it is ambiguous. The first fix read the *BatchError through the wrapper and
// dropped the wrapper's ambiguity (the review's concurrency-4).
func TestAnAmbiguousErrorAroundAPerItemAnswerKeepsItsAmbiguity(t *testing.T) {
	refused := &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Code: mq.CodeInvalidArgument, Err: errors.New("refused")}
	answer := func() error { return &mq.BatchError{Errs: []error{refused, nil}} }
	for name, tc := range map[string]struct {
		driverErr error
		// accepted says message 1 reads as accepted: the error around the
		// per-item answer is definite.
		accepted bool
	}{
		"an ambiguous *OpError around it": {&mq.OpError{
			Driver: "adv", Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true, Err: answer(),
		}, false},
		// The rules batchErrorIn applies, each pinned (the review's
		// rigour-4): the code counts as well as the flag; a code-carrying
		// error that is not an *OpError counts; and the OUTERMOST *OpError
		// decides, as it does for IsAmbiguous.
		"a flagless CodeAmbiguous *OpError around it":   {&mq.OpError{Driver: "adv", Operation: mq.OpPublish, Code: mq.CodeAmbiguous, Err: answer()}, false},
		"a CodeAmbiguous error that is not an *OpError": {codedWrapper{code: errcode.Ambiguous, err: answer()}, false},
		"a definite *OpError around an ambiguous one": {&mq.OpError{
			Driver: "adv", Operation: mq.OpPublish, Code: mq.CodeInvalidArgument,
			Err: &mq.OpError{Code: mq.CodeUnavailable, Ambiguous: true, Err: answer()},
		}, true},
		"a plain wrapper around it": {fmt.Errorf("adv: %w", answer()), true},
	} {
		for _, surface := range []string{"PublishBatch", "PublishItems"} {
			t.Run(name+"/"+surface, func(t *testing.T) {
				driverErr, accepted := tc.driverErr, tc.accepted
				open := func(b *advBroker, cfg mq.SinkConfig) mq.Sink {
					return &blameNoneSink{advSink: advSink{b: b, receipts: cfg.Receipts}, err: driverErr}
				}
				_, p := advClient(t, open, mq.PublisherConfig{})
				err := batchSurfaces[surface](context.Background(), p, []string{"a", "b"})
				var be *mq.BatchError
				if !errors.As(err, &be) || len(be.Errs) != 2 {
					t.Fatalf("%s = %v: want the per-message answer", surface, err)
				}
				if be.Errs[0] != error(refused) || mq.IsAmbiguous(be.Errs[0]) { //nolint:errorlint // identity: the driver's own entry
					t.Fatalf("%s: message 0 = %v, want the driver's own definite refusal", surface, be.Errs[0])
				}
				if accepted {
					if be.Errs[1] != nil {
						t.Fatalf("%s: message 1 = %v, want nil: nothing around the answer says its outcome is unknown", surface, be.Errs[1])
					}
					return
				}
				if be.Errs[1] == nil || !mq.IsAmbiguous(be.Errs[1]) {
					t.Fatalf("%s: message 1 = %v, want an ambiguous answer: the driver's error says nobody knows", surface, be.Errs[1])
				}
				if st := p.Stats(); st.Published != 0 || st.Failed != 2 {
					t.Fatalf("Stats = %+v, want 0 published and 2 failed", st)
				}
			})
		}
	}
}

// codedWrapper carries a portable code without being an *OpError, the way an
// internal/errcode error does, and wraps another error.
type codedWrapper struct {
	code errcode.Code
	err  error
}

func (c codedWrapper) Error() string      { return "coded: " + c.err.Error() }
func (c codedWrapper) Code() errcode.Code { return c.code }
func (c codedWrapper) Unwrap() error      { return c.err }

// TestAFatalFailureLeavesTheUnsentMessagesDefinite: when a fatal error --
// here a deadline after the write, which is ambiguous -- stops a batch, the
// messages after it never reached the driver. They used to be given that same
// error, so they read "may be in the broker", and a caller that republishes
// only definite refusals, as the confirmation notes advises, never
// published them (the review's correctness-4). They now get a DEFINITE error
// that keeps the fatal code, so errors.Is against context.DeadlineExceeded or
// ErrClosed still holds.
func TestAFatalFailureLeavesTheUnsentMessagesDefinite(t *testing.T) {
	mapper := &driverkit.ErrorMapper{Driver: "adv"}
	fatal := mapper.MapNet(mq.OpPublish, "orders", "", context.DeadlineExceeded, true)
	if !mq.IsAmbiguous(fatal) || mq.CodeOf(fatal) != mq.CodeDeadlineExceeded {
		t.Fatalf("the instrument is broken: %v is not an ambiguous DeadlineExceeded", fatal)
	}
	refused := errors.New("refused")
	for _, surface := range []string{"PublishBatch", "PublishItems", "PublishBatchOwned"} {
		t.Run(surface, func(t *testing.T) {
			b, p := advClient(t, openPlain, mq.PublisherConfig{})
			b.setFail(func(_ int, m *mq.Message) error {
				switch string(m.Body) {
				case "refuse":
					return mapper.MapNet(mq.OpPublish, "orders", "", fmt.Errorf("%w: %w", refused, io.EOF), false)
				case "fatal":
					return fatal
				}
				return nil
			})
			err := batchSurfaces[surface](context.Background(), p, []string{"refuse", "fatal", "x", "x"})
			var be *mq.BatchError
			if !errors.As(err, &be) || len(be.Errs) != 4 {
				t.Fatalf("%s = %v: want a *BatchError over four messages", surface, err)
			}
			if mq.IsAmbiguous(be.Errs[0]) || !errors.Is(be.Errs[0], refused) {
				t.Fatalf("message 0 = %v, want its own definite refusal", be.Errs[0])
			}
			if be.Errs[1] != fatal { //nolint:errorlint // identity: the driver's own answer
				t.Fatalf("message 1 = %v, want the driver's own ambiguous answer", be.Errs[1])
			}
			for i := 2; i < 4; i++ {
				e := be.Errs[i]
				if e == nil || mq.IsAmbiguous(e) {
					t.Fatalf("message %d = %v: IsAmbiguous = %v, want a definite failure -- it never reached the driver", i, e, mq.IsAmbiguous(e))
				}
				if mq.CodeOf(e) != mq.CodeDeadlineExceeded || !errors.Is(e, context.DeadlineExceeded) {
					t.Fatalf("message %d = %v (code %v): want the fatal code, DeadlineExceeded", i, e, mq.CodeOf(e))
				}
				if !mq.IsRetryable(e) {
					t.Fatalf("message %d = %v is not retryable; a message that never left is safe to send", i, e)
				}
			}
			if got := b.nativeCalls(); got != 2 {
				t.Fatalf("the driver was asked about %d messages, want 2", got)
			}
		})
	}
}

// TestBatchingReportsAmbiguityPerCallback is the flusher's side of the rule:
// PublishAsync with Batching hands each ready batch to the driver's native
// batch, and each callback must get its own message's outcome. A batch whose
// first message was refused and whose second may be in the broker is exactly
// the shape a collapse to the first error used to report as two refusals
// (the review's concurrency-3).
func TestBatchingReportsAmbiguityPerCallback(t *testing.T) {
	mapper := &driverkit.ErrorMapper{Driver: "adv"}
	refused := errors.New("refused")
	b, p := advClient(t, openBatch, mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 100}})
	b.setFail(func(_ int, m *mq.Message) error {
		switch string(m.Body) {
		case "refuse":
			return mapper.MapNet(mq.OpPublish, "orders", "", fmt.Errorf("%w: %w", refused, io.EOF), false)
		case "ambiguous":
			return mapper.MapNet(mq.OpPublish, "orders", "", io.EOF, true)
		}
		return nil
	})
	var mu sync.Mutex
	got := map[string]error{}
	for _, body := range []string{"refuse", "ambiguous"} {
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte(body)}, func(r mq.PublishResult) {
			mu.Lock()
			got[body] = r.Err
			mu.Unlock()
		}); err != nil {
			t.Fatalf("PublishAsync(%s): %v", body, err)
		}
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if e := got["refuse"]; e == nil || mq.IsAmbiguous(e) || !errors.Is(e, refused) {
		t.Fatalf("the refused message's callback got %v, want its definite refusal", e)
	}
	if e := got["ambiguous"]; e == nil || !mq.IsAmbiguous(e) {
		t.Fatalf("the ambiguous message's callback got %v, want an ambiguous answer: it may be in the broker", e)
	}
}

// TestADriversOwnMixedBatchErrorKeepsItsAmbiguousEntry is the redis shape the
// review of this branch found: a native batch in which one XADD was refused by
// a reply and the other was stored but WAITAOF came back short. The driver
// answers with its own all-failed *BatchError, and the short entry's
// ambiguity is in its CODE (errcode.Ambiguous, mapped by MapNet).
//
// main returned the driver's *BatchError as it was: CodePartial, Transient
// false. The first version of this branch read only the Ambiguous flag,
// found both entries "definite", collapsed the batch to its first entry and
// returned a retryable Backpressure -- so middleware.Retry would have
// republished a message the error itself said was in the stream. Probed on
// both: `cb8d4d2a ... isBatchError=true code=Partial Transient=false`,
// `bc3b8c05 ... isBatchError=false code=Backpressure Transient=true`.
func TestADriversOwnMixedBatchErrorKeepsItsAmbiguousEntry(t *testing.T) {
	m := &driverkit.ErrorMapper{Driver: "redis", Fallback: []driverkit.Rule{
		{Match: driverkit.MatchContains("OOM command not allowed"), Code: mq.CodeBackpressure, Retryable: true},
	}}
	refused := m.MapNet(mq.OpPublish, "orders", "", errors.New("OOM command not allowed when used memory > 'maxmemory'"), true)
	for name, short := range map[string]error{
		"a short WAITAOF through MapNet": m.MapNet(mq.OpPublish, "orders", "", errcode.New(errcode.Ambiguous, "WAITAOF came back short"), true),
		"a flagless CodeAmbiguous":       &mq.OpError{Driver: "redis", Operation: mq.OpPublish, Code: mq.CodeAmbiguous},
	} {
		for _, surface := range []string{"PublishBatch", "PublishItems"} {
			t.Run(name+"/"+surface, func(t *testing.T) {
				driverErr := &mq.BatchError{Errs: []error{refused, short}}
				open := func(b *advBroker, cfg mq.SinkConfig) mq.Sink {
					return &blameNoneSink{advSink: advSink{b: b, receipts: cfg.Receipts}, err: driverErr}
				}
				_, p := advClient(t, open, mq.PublisherConfig{})
				err := batchSurfaces[surface](context.Background(), p, []string{"a", "b"})
				var be *mq.BatchError
				if !errors.As(err, &be) || len(be.Errs) != 2 {
					t.Fatalf("%s = %v (code %v): want the per-message answer, the second message is in the stream", surface, err, mq.CodeOf(err))
				}
				if !mq.IsAmbiguous(err) || mq.IsAmbiguous(be.Errs[0]) || !mq.IsAmbiguous(be.Errs[1]) {
					t.Fatalf("%s = %v: IsAmbiguous(batch, 0, 1) = %v, %v, %v; want true, false, true", surface, err,
						mq.IsAmbiguous(err), mq.IsAmbiguous(be.Errs[0]), mq.IsAmbiguous(be.Errs[1]))
				}
				if middleware.Transient(err) {
					t.Fatalf("%s = %v: Transient = true, so Retry would republish a message that is in the stream", surface, err)
				}
			})
		}
	}
}

// TestAFatalNativePieceLeavesTheUnreachedPiecesDefinite is the native half of
// the same rule, on the path PublishItems and the batching flusher share
// (sendAligned): the driver takes the batch in pieces of two, the first piece
// comes back with a fatal, ambiguous answer, and the second piece is never
// offered. Its messages must read DEFINITE and retryable. Reverting the fill
// in sendAligned's native arm passed the whole root package (the review's
// rigour-2 and concurrency-2), because every other fatal test drives a loop.
func TestAFatalNativePieceLeavesTheUnreachedPiecesDefinite(t *testing.T) {
	mapper := &driverkit.ErrorMapper{Driver: "adv"}
	fatal := mapper.MapNet(mq.OpPublish, "orders", "", context.DeadlineExceeded, true)
	check := func(t *testing.T, name string, errs []error) {
		t.Helper()
		for i := range 2 {
			if !mq.IsAmbiguous(errs[i]) {
				t.Fatalf("%s: message %d = %v, want the driver's ambiguous answer", name, i, errs[i])
			}
		}
		for i := 2; i < 4; i++ {
			e := errs[i]
			if e == nil || mq.IsAmbiguous(e) || !mq.IsRetryable(e) || !errors.Is(e, context.DeadlineExceeded) {
				t.Fatalf("%s: message %d = %v (ambiguous %v, retryable %v): want a definite, retryable "+
					"DeadlineExceeded -- its piece was never offered to the driver", name, i, e, mq.IsAmbiguous(e), mq.IsRetryable(e))
			}
		}
	}
	open := func(t *testing.T, cfg mq.PublisherConfig) (*advBroker, *mq.PublisherHandle) {
		b := newAdvBroker(openBatch)
		b.caps = advCapsWithBatchLimit(2)
		b.setFail(func(_ int, m *mq.Message) error {
			if string(m.Body) == "fatal" {
				return fatal
			}
			return nil
		})
		return b, advPublisher(t, b, cfg)
	}
	bodies := []string{"fatal", "fatal", "x", "x"}

	t.Run("PublishItems", func(t *testing.T) {
		b, p := open(t, mq.PublisherConfig{})
		err := p.PublishItems(context.Background(), publishItems(bodies...))
		var be *mq.BatchError
		if !errors.As(err, &be) || len(be.Errs) != 4 {
			t.Fatalf("PublishItems = %v, want a *BatchError over four messages", err)
		}
		check(t, "PublishItems", be.Errs)
		if got := b.nativeCalls(); got != 2 {
			t.Fatalf("the driver was asked about %d messages, want 2", got)
		}
	})
	t.Run("PublishAsync with Batching", func(t *testing.T) {
		b, p := open(t, mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 100}})
		var mu sync.Mutex
		got := make([]error, len(bodies))
		for i, body := range bodies {
			if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte(body)}, func(r mq.PublishResult) {
				mu.Lock()
				got[i] = r.Err
				mu.Unlock()
			}); err != nil {
				t.Fatalf("PublishAsync(%d): %v", i, err)
			}
		}
		if err := p.Flush(context.Background()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		check(t, "the callbacks", got)
		if n := b.nativeCalls(); n != 2 {
			t.Fatalf("the driver was asked about %d messages, want 2", n)
		}
	})
}

// TestAFatalEntryInAPerItemPieceStopsTheBatch: a native piece whose messages
// failed with mixed outcomes is a *BatchError, CodePartial, which
// isFatalBatch cannot see into -- so a close or a deadline INSIDE it did not
// stop the batch, and the next piece was offered to a sink that could not
// take it (the review's concurrency-3). The first fatal entry of a piece now
// stops the batch like a fatal whole-piece answer does.
func TestAFatalEntryInAPerItemPieceStopsTheBatch(t *testing.T) {
	mapper := &driverkit.ErrorMapper{Driver: "adv"}
	fatal := mapper.MapNet(mq.OpPublish, "orders", "", context.DeadlineExceeded, true)
	for _, surface := range []string{"PublishBatch", "PublishItems"} {
		t.Run(surface, func(t *testing.T) {
			b := newAdvBroker(openBatch)
			b.caps = advCapsWithBatchLimit(2)
			b.setFail(func(_ int, m *mq.Message) error {
				switch string(m.Body) {
				case "fatal":
					return fatal
				case "closed":
					return mq.ErrClosed
				}
				return nil
			})
			p := advPublisher(t, b, mq.PublisherConfig{})
			err := batchSurfaces[surface](context.Background(), p, []string{"fatal", "closed", "x", "x"})
			var be *mq.BatchError
			if !errors.As(err, &be) || len(be.Errs) != 4 {
				t.Fatalf("%s = %v, want a *BatchError over four messages", surface, err)
			}
			if got := b.nativeCalls(); got != 2 {
				t.Fatalf("the driver was asked about %d messages, want 2: the first piece's answer said it cannot continue", got)
			}
			for i := 2; i < 4; i++ {
				e := be.Errs[i]
				if e == nil || mq.IsAmbiguous(e) || !errors.Is(e, context.DeadlineExceeded) {
					t.Fatalf("%s: message %d = %v, want a definite not-attempted DeadlineExceeded", surface, i, e)
				}
			}
		})
	}
}
