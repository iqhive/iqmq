package soak

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Config is one soak run.
//
// The workload is the list and not a throughput loop: a soak that only
// publishes and acks exercises the steady state, which is the one state a leak
// is least likely to hide in. Reconnect storms, leases left to expire and a
// counter that is read every cycle are what the specification names, and each of them has
// its own retained state in the core — a settlement record, a lease, a
// statistics shard — which is where the leaks this tier exists to find have
// been.
type Config struct {
	// Client is the client under soak.
	Client *mq.Client
	// Topic and Subscription are the topology the arm created.
	Topic, Subscription string
	// Duration is how long to run.
	Duration time.Duration
	// Batch is how many messages one cycle publishes.
	Batch int
	// ReconnectEvery is how often the publisher and consumer are closed
	// and reopened: the reconnect storm. Zero disables it.
	ReconnectEvery time.Duration
	// ExpireEvery is how often one cycle leaves a delivery unsettled so
	// that its lease expires: the lease expiry. Zero disables it.
	ExpireEvery time.Duration
	// Series carries the thresholds and the minimum duration.
	Series Series
	// Log records progress; nil is silent.
	Log func(format string, args ...any)
}

func (c Config) resolve() Config {
	if c.Batch <= 0 {
		c.Batch = 32
	}
	if c.Duration <= 0 {
		c.Duration = time.Hour
	}
	if c.Subscription == "" {
		c.Subscription = "soak"
	}
	if c.Log == nil {
		c.Log = func(string, ...any) {}
	}
	return c
}

// Stats is what the workload did, beside the profile.
type Stats struct {
	// Cycles is how many publish-and-drain cycles ran.
	Cycles int64
	// Published, Delivered and Settled count the workload's own tally.
	Published, Delivered, Settled int64
	// Expired is how many deliveries were deliberately left unsettled.
	Expired int64
	// Reconnects is how many times the handles were closed and reopened.
	Reconnects int64
	// Errors is how many operations failed, with the last one kept.
	Errors int64
	Last   error
}

// String implements fmt.Stringer.
func (s Stats) String() string {
	return fmt.Sprintf("cycles=%d published=%d delivered=%d settled=%d expired=%d reconnects=%d errors=%d",
		s.Cycles, s.Published, s.Delivered, s.Settled, s.Expired, s.Reconnects, s.Errors)
}

const bodyPrefix = "iqmq-soak:"

// Run soaks the client for the configured duration and returns the profile
// diff and the tally.
//
// It returns an error only when the soak could not run at all. A workload
// error is counted in [Stats] and the run carries on: a soak that stopped at
// the first transient would never reach a duration that could conclude
// anything, and the whole tier is about what happens after a long time.
func Run(ctx context.Context, cfg Config) (Diff, Stats, error) {
	cfg = cfg.resolve()
	var st Stats
	start := time.Now()
	rec := NewRecorder(cfg.Topic, start, cfg.Series)
	every := SampleEvery(cfg.Duration, rec.series.MinSamples)

	pub, con, err := open(ctx, cfg)
	if err != nil {
		return Diff{}, st, err
	}
	defer func() {
		if pub != nil {
			_ = pub.Close()
		}
		if con != nil {
			_ = con.Close()
		}
	}()

	var serial atomic.Int64
	deadline := start.Add(cfg.Duration)
	nextSample := start.Add(every)
	nextReconnect := start.Add(cfg.ReconnectEvery)
	nextExpire := start.Add(cfg.ExpireEvery)
	dst := make([]mq.Delivery, cfg.Batch)

	// The first sample is taken before the first cycle, so that the series
	// brackets the whole run rather than starting after warm-up.
	rec.Sample(start, 0)

	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			break
		}
		st.Cycles++
		expire := cfg.ExpireEvery > 0 && !time.Now().Before(nextExpire)
		if expire {
			nextExpire = time.Now().Add(cfg.ExpireEvery)
		}

		for range cfg.Batch {
			n := serial.Add(1)
			m := &mq.Message{
				Topic: cfg.Topic,
				Key:   []byte("k" + strconv.FormatInt(n%8, 10)),
				Body:  []byte(bodyPrefix + strconv.FormatInt(n, 10)),
			}
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := pub.Publish(pctx, m)
			cancel()
			if err != nil {
				st.Errors++
				st.Last = err
				continue
			}
			st.Published++
		}

		// Drain what is there, up to a budget. The budget is not
		// belt-and-braces: with a lease short enough for the expiry limb
		// to fire, the deliberately-unsettled delivery comes BACK inside
		// the same drain, and the loop as first written ran until the
		// whole run's context expired — a six-second soak took sixty
		// seconds and reported "pass over 1m0s" from three samples.
		budget := 8 * cfg.Batch
		drained := 0
		// The run's own deadline bounds the drain as well as the cycle
		// count. Without it a cycle that started just before the deadline
		// could drain for another budget's worth of redeliveries, and a
		// six-second soak with a 300 ms lease overran to nineteen
		// seconds — a run that does not stop when it says it will cannot
		// report the duration its verdict depends on.
		for drained < budget && time.Now().Before(deadline) {
			rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			got, rerr := con.Receive(rctx, dst)
			cancel()
			if rerr != nil && !errors.Is(rerr, context.DeadlineExceeded) && got == 0 {
				if errors.Is(rerr, mq.ErrClosed) {
					break
				}
				st.Errors++
				st.Last = rerr
				break
			}
			if got == 0 {
				break
			}
			st.Delivered += int64(got)
			drained += got
			for i := range got {
				if expire && i == 0 {
					// Left unsettled on purpose: the lease
					// expires and the record comes back.
					st.Expired++
					continue
				}
				if err := dst[i].Ack(); err != nil {
					if !errors.Is(err, mq.ErrAlreadySettled) {
						st.Errors++
						st.Last = err
					}
					continue
				}
				st.Settled++
			}
		}

		// the reconnect storm: close both handles and reopen them.
		// It is the path that has held a settlement record or a source
		// goroutine past its owner in three increments (the contributor notes's
		// recurring defect class), so a soak that never reconnects never
		// visits it.
		if cfg.ReconnectEvery > 0 && !time.Now().Before(nextReconnect) {
			nextReconnect = time.Now().Add(cfg.ReconnectEvery)
			_ = pub.Close()
			_ = con.Close()
			pub, con = nil, nil
			np, nc, err := open(ctx, cfg)
			if err != nil {
				st.Errors++
				st.Last = err
				// A reopen that fails ends the run: everything
				// after it would be measuring nothing.
				break
			}
			pub, con = np, nc
			st.Reconnects++
		}

		if !time.Now().Before(nextSample) {
			nextSample = time.Now().Add(every)
			s := rec.Sample(time.Now(), st.Settled)
			cfg.Log("soak %s t=%v heap=%s goroutines=%d %s",
				cfg.Topic, s.Elapsed.Round(time.Second), bytes(s.HeapAlloc), s.Goroutines, st)
		}
	}

	// The last sample is taken after the loop, so the series ends at the
	// run's real end rather than one interval before it.
	rec.Sample(time.Now(), st.Settled)
	return rec.Analyse(), st, nil
}

// open builds the publisher and the consumer.
func open(ctx context.Context, cfg Config) (*mq.PublisherHandle, *mq.ConsumerHandle, error) {
	pub, err := cfg.Client.Publisher(ctx, mq.PublisherConfig{Topic: cfg.Topic, Metadata: mq.MetadataAdaptive})
	if err != nil {
		return nil, nil, fmt.Errorf("soak publisher: %w", err)
	}
	con, err := cfg.Client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        cfg.Topic,
		Subscription: cfg.Subscription,
		Prefetch:     cfg.Batch,
		Metadata:     mq.MetadataAdaptive,
	})
	if err != nil {
		_ = pub.Close()
		return nil, nil, fmt.Errorf("soak consumer: %w", err)
	}
	return pub, con, nil
}
