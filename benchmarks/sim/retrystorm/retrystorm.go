// Package retrystorm simulates a retry storm: many clients that fail
// together during an outage and retry together afterwards.
//
// It is the experiment that decides:
// "retry-storm simulation (10k clients, 30 s outage): peak concurrency,
// time-to-recover". Backoff without jitter keeps the clients in lockstep,
// so every retry wave is as tall as the failure that caused it; jitter
// spreads the wave out. The simulation reports how tall the wave gets
// (peak-concurrency), how long the system takes to drain it (recover-s)
// and how much extra load it costs (attempts).
//
// The same client model is executed two ways. RunEvents is a
// discrete-event simulation: deterministic, allocation-light and fast
// enough for a benchmark loop. RunBubble runs one goroutine per client
// inside a testing/synctest bubble, where time is virtual but everything
// else — the scheduler, the channels, the sleeps — is real. The two must
// agree; TestBubbleMatchesEvents holds them to it, which is what makes the
// fast simulation trustworthy.
package retrystorm

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/backoff"
)

// Params describes one experiment.
type Params struct {
	// Kind is the backoff policy under test.
	Kind backoff.Kind
	// Config parameterises the policy; the zero value means the defaults.
	Config backoff.Config
	// Clients is the number of clients that fail together.
	Clients int
	// Outage is how long every request fails.
	Outage time.Duration
	// FailLatency is how long a request takes to fail during the outage
	// (a broker that refuses a connection fails fast).
	FailLatency time.Duration
	// ServiceLatency is how long a successful request occupies the server.
	ServiceLatency time.Duration
	// MaxAttempts bounds a client's attempts; 0 means unlimited.
	MaxAttempts int
	// Seed makes the jitter reproducible.
	Seed uint64
}

// withDefaults fills the zero fields with the experiment.
func (p Params) withDefaults() Params {
	if p.Clients == 0 {
		p.Clients = 10000
	}
	if p.Outage == 0 {
		p.Outage = 30 * time.Second
	}
	if p.FailLatency == 0 {
		p.FailLatency = time.Millisecond
	}
	if p.ServiceLatency == 0 {
		p.ServiceLatency = 5 * time.Millisecond
	}
	if p.Seed == 0 {
		p.Seed = 0xA21A21A21A21A21
	}
	return p
}

// Result is what one experiment measured.
type Result struct {
	// PeakConcurrency is the largest number of requests in flight at one
	// instant, over the whole run.
	PeakConcurrency int
	// PeakAfterOutage is the largest number in flight after the outage
	// ended: the size of the recovery wave.
	PeakAfterOutage int
	// Recover is the time from the end of the outage until the last
	// client succeeded, or zero when no client ever succeeded.
	Recover time.Duration
	// Attempts is the total number of requests sent, successful or not:
	// the load amplification the policy caused.
	Attempts int
	// Failed is the number of clients that ran out of attempts.
	Failed int
	// LastSuccess is when the final client succeeded, measured from the
	// start of the outage.
	LastSuccess time.Duration
}

// rngFor derives the random word for one client's attempt. Both
// simulations draw from this function, so they see identical jitter.
func rngFor(seed uint64, client, attempt int) uint64 {
	z := seed + uint64(client)*0x9E3779B97F4A7C15 + uint64(attempt)*0xBF58476D1CE4E5B9
	z ^= z >> 30
	z *= 0xBF58476D1CE4E5B9
	z ^= z >> 27
	z *= 0x94D049BB133111EB
	return z ^ z>>31
}

// event kinds in the discrete-event simulation.
const (
	evSend = iota // a client issues a request
	evDone        // a request completes
)

type event struct {
	at     time.Duration
	kind   int
	client int
	// attempt is the attempt number of the request the event belongs to.
	attempt int
	// ok reports whether the completing request succeeded.
	ok bool
	// seq breaks ties so the simulation is deterministic.
	seq uint64
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	// Completions before sends at the same instant: a server that frees a
	// slot has freed it before the next request arrives.
	if q[i].kind != q[j].kind {
		return q[i].kind > q[j].kind
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(event)) }
func (q *eventQueue) Pop() any     { old := *q; n := len(old); e := old[n-1]; *q = old[:n-1]; return e }

// RunEvents runs the experiment as a discrete-event simulation and
// returns the metrics. It allocates one event queue and nothing per
// event, and is deterministic given Params. It fails on a Params.Kind that
// names no backoff policy.
func RunEvents(p Params) (Result, error) {
	p = p.withDefaults()
	pol, err := backoff.NewWith(p.Kind, p.Config)
	if err != nil {
		return Result{}, fmt.Errorf("retrystorm: %w", err)
	}
	q := make(eventQueue, 0, 2*p.Clients)
	var seq uint64
	push := func(e event) {
		e.seq = seq
		seq++
		heap.Push(&q, e)
	}
	for c := range p.Clients {
		push(event{at: 0, kind: evSend, client: c, attempt: 1})
	}
	prev := make([]time.Duration, p.Clients)
	var res Result
	inFlight := 0
	for len(q) > 0 {
		e := heap.Pop(&q).(event)
		switch e.kind {
		case evSend:
			res.Attempts++
			inFlight++
			if inFlight > res.PeakConcurrency {
				res.PeakConcurrency = inFlight
			}
			if e.at >= p.Outage && inFlight > res.PeakAfterOutage {
				res.PeakAfterOutage = inFlight
			}
			ok := e.at >= p.Outage
			d := p.FailLatency
			if ok {
				d = p.ServiceLatency
			}
			push(event{at: e.at + d, kind: evDone, client: e.client, attempt: e.attempt, ok: ok})
		case evDone:
			inFlight--
			if e.ok {
				if e.at > res.LastSuccess {
					res.LastSuccess = e.at
				}
				continue
			}
			if p.MaxAttempts > 0 && e.attempt >= p.MaxAttempts {
				res.Failed++
				continue
			}
			next := e.attempt + 1
			d := pol.Next(next, prev[e.client], rngFor(p.Seed, e.client, next))
			prev[e.client] = d
			push(event{at: e.at + d, kind: evSend, client: e.client, attempt: next})
		}
	}
	if res.LastSuccess > 0 {
		res.Recover = res.LastSuccess - p.Outage
	}
	return res, nil
}

// RunBubble runs the same experiment with one goroutine per client. It
// must be called inside a testing/synctest bubble, where time is virtual;
// outside one it would take as long as the outage it simulates.
func RunBubble(tb testing.TB, p Params) Result {
	tb.Helper()
	p = p.withDefaults()
	pol, err := backoff.NewWith(p.Kind, p.Config)
	if err != nil {
		tb.Fatalf("retrystorm: %v", err)
	}
	start := time.Now()
	var (
		inFlight  atomic.Int64
		peak      atomic.Int64
		peakAfter atomic.Int64
		attempts  atomic.Int64
		failed    atomic.Int64
		lastNanos atomic.Int64
	)
	observe := func(cur int64, after bool) {
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		if !after {
			return
		}
		for {
			old := peakAfter.Load()
			if cur <= old || peakAfter.CompareAndSwap(old, cur) {
				break
			}
		}
	}
	var wg sync.WaitGroup
	for c := range p.Clients {
		wg.Go(func() {
			var prev time.Duration
			for attempt := 1; ; attempt++ {
				sent := time.Since(start)
				attempts.Add(1)
				observe(inFlight.Add(1), sent >= p.Outage)
				ok := sent >= p.Outage
				switch {
				case ok:
					time.Sleep(p.ServiceLatency)
				default:
					time.Sleep(p.FailLatency)
				}
				inFlight.Add(-1)
				if ok {
					done := int64(time.Since(start))
					for {
						old := lastNanos.Load()
						if done <= old || lastNanos.CompareAndSwap(old, done) {
							break
						}
					}
					return
				}
				if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
					failed.Add(1)
					return
				}
				d := pol.Next(attempt+1, prev, rngFor(p.Seed, c, attempt+1))
				prev = d
				time.Sleep(d)
			}
		})
	}
	wg.Wait()
	last := time.Duration(lastNanos.Load())
	recover := time.Duration(0)
	if last > 0 {
		recover = last - p.Outage
	}
	return Result{
		PeakConcurrency: int(peak.Load()),
		PeakAfterOutage: int(peakAfter.Load()),
		Recover:         recover,
		Attempts:        int(attempts.Load()),
		Failed:          int(failed.Load()),
		LastSuccess:     last,
	}
}
