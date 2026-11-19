package goredis_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// TestAConsumerClosedWithItsReceiveParkedRemovesItsRow is the go-redis arm of
// drivers/redis's test of that name: a consumer of a quiet topic, holding
// nothing, is closed while its Receive or its runner's fetch is parked in
// XREADGROUP, by ConsumerHandle.Close, by its Shutdown or by Client.Close, and
// its consumer row must go. go-redis cannot end a blocking read early, so the
// close waits for the read, at most its block, before it tidies up; it left
// the row for good, and a runner's row under Client.Close every time. With
// IQMQ_REDIS_ADDR set it also runs against that server.
//
// The wait is drivers/redis's after v1.1.2, so the test skips against v1.1.2
// and runs from the release whose go.mod requires the drivers/redis release
// that has it.
func TestAConsumerClosedWithItsReceiveParkedRemovesItsRow(t *testing.T) {
	if v, old := driversRedisBefore(t, "v1.1.3"); old {
		t.Skipf("this build links drivers/redis %s, whose close leaves the row of a consumer with a read in flight", v)
	}
	t.Run("the in-memory server", func(t *testing.T) {
		_, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
		closeParkedConsumers(t, srv.Addr())
	})
	t.Run("IQMQ_REDIS_ADDR", func(t *testing.T) {
		addr := os.Getenv("IQMQ_REDIS_ADDR")
		if addr == "" {
			t.Skip("set IQMQ_REDIS_ADDR to run this against a server")
		}
		closeParkedConsumers(t, addr)
	})
}

// closeParkedConsumers is TestAConsumerClosedWithItsReceiveParkedRemovesItsRow
// against the server at addr.
func closeParkedConsumers(t *testing.T, addr string) {
	const block = time.Second
	raw, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	t.Cleanup(raw.Close)
	for _, how := range []string{
		"ConsumerHandle.Close", "ConsumerHandle.Shutdown", "Client.Close",
		"a runner, ConsumerHandle.Close", "a runner, Client.Close",
	} {
		t.Run(how, func(t *testing.T) {
			ctx := t.Context()
			topic := "parked." + strconv.FormatInt(time.Now().UnixNano(), 36)
			keys, err := streams.NewKeys(topic, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = raw.Do(context.Background(), raw.B().Del().Key(keys.Stream(0)).Build()).Error() })
			cl, err := goredis.Open(ctx, rd.Config{Addrs: []string{addr}, AutoCreate: true, Block: block})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = cl.Close() }()
			c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: testSub, Start: mq.StartEarliest()})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			// One message handled first, so that the row exists on every
			// version (6.2 makes it only when a read hands something over)
			// and holds nothing.
			if err := p.Publish(ctx, &mq.Message{Body: []byte("one")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			done := make(chan error, 1)
			if strings.HasPrefix(how, "a runner") {
				handled := make(chan struct{}, 1)
				go func() {
					done <- c.Run(context.Background(), mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
						handled <- struct{}{}
						return nil
					}))
				}()
				select {
				case <-handled:
				case <-time.After(5 * time.Second):
					t.Fatal("setup: the runner handled nothing")
				}
			} else {
				first := make([]mq.Delivery, 1)
				if n, err := c.Receive(ctx, first); n != 1 || err != nil {
					t.Fatalf("Receive = %d, %v", n, err)
				}
				if err := first[0].Ack(); err != nil {
					t.Fatalf("Ack: %v", err)
				}
				go func() {
					_, err := c.Receive(context.Background(), make([]mq.Delivery, 4))
					done <- err
				}()
			}
			time.Sleep(300 * time.Millisecond) // the read is parked at the server, and a runner's acknowledgement has landed
			before := groupRows(t, raw, keys, testSub)
			start := time.Now()
			switch how {
			case "ConsumerHandle.Close", "a runner, ConsumerHandle.Close":
				err = c.Close()
			case "ConsumerHandle.Shutdown":
				sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err = c.Shutdown(sctx)
				cancel()
			default:
				err = cl.Close()
			}
			took := time.Since(start)
			var ended error
			select {
			case ended = <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("the parked Receive or Run had not returned 5s after %s", how)
			}
			after := groupRows(t, raw, keys, testSub)
			t.Logf("%s = %v after %v (the parked call returned %v); consumer rows before %d, after %d",
				how, err, took.Round(time.Millisecond), ended, before, after)
			if after != 0 {
				t.Errorf("%s of a consumer holding nothing, with its read parked, left %d consumer rows", how, after)
			}
			if took > block+time.Second {
				t.Errorf("%s took %v, more than the block (%v) and a second", how, took, block)
			}
		})
	}
}

// TestClosingAClientOfManyParkedConsumersTakesOneBound is the go-redis arm of
// drivers/redis's test of that name: Client.Close of 8 consumers holding
// nothing, each with a Receive loop whose reads are parked at its own point of
// the default 5s block, which go-redis cannot end. The close stays within the
// shared bound and the second after it and removes every row; it took the
// bound and about a second for each consumer after it, and left most rows
// (18s and 12 of 16 rows for 16 consumers against 7.4), because each close
// waited for a read the Fetches not yet closed had just started again. With
// IQMQ_REDIS_ADDR set it also runs against that server.
//
// The bound is drivers/redis's after v1.1.2, so the test skips against v1.1.2.
func TestClosingAClientOfManyParkedConsumersTakesOneBound(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a 5s block")
	}
	if v, old := driversRedisBefore(t, "v1.1.3"); old {
		t.Skipf("this build links drivers/redis %s, whose closes do not wait for a parked read", v)
	}
	t.Run("the in-memory server", func(t *testing.T) {
		_, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
		closeManyParked(t, srv.Addr())
	})
	t.Run("IQMQ_REDIS_ADDR", func(t *testing.T) {
		addr := os.Getenv("IQMQ_REDIS_ADDR")
		if addr == "" {
			t.Skip("set IQMQ_REDIS_ADDR to run this against a server")
		}
		closeManyParked(t, addr)
	})
}

// closeManyParked is TestClosingAClientOfManyParkedConsumersTakesOneBound
// against the server at addr.
func closeManyParked(t *testing.T, addr string) {
	const consumers = 8
	raw, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	t.Cleanup(raw.Close)
	ctx := t.Context()
	topic := "many." + strconv.FormatInt(time.Now().UnixNano(), 36)
	keys, err := streams.NewKeys(topic, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Do(context.Background(), raw.B().Del().Key(keys.Stream(0)).Build()).Error() })
	cl, err := goredis.Open(ctx, rd.Config{Addrs: []string{addr}, AutoCreate: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = cl.Close() }()
	cons := make([]*mq.ConsumerHandle, consumers)
	for i := range consumers {
		c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s" + strconv.Itoa(i), Start: mq.StartEarliest()})
		if err != nil {
			t.Fatalf("Consumer %d: %v", i, err)
		}
		cons[i] = c
	}
	p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	if err := p.Publish(ctx, &mq.Message{Body: []byte("one")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for i, c := range cons {
		d := make([]mq.Delivery, 1)
		if got, err := c.Receive(ctx, d); got != 1 || err != nil {
			t.Fatalf("Receive %d = %d, %v", i, got, err)
		}
		if err := d[0].Ack(); err != nil {
			t.Fatalf("Ack %d: %v", i, err)
		}
	}
	loops := make([]chan struct{}, consumers)
	for i, c := range cons {
		loops[i] = make(chan struct{})
		go func() {
			defer close(loops[i])
			time.Sleep(time.Duration(consumers-1-i) * rd.DefaultBlock / consumers)
			buf := make([]mq.Delivery, 4)
			for {
				_, err := c.Receive(context.Background(), buf)
				if errors.Is(err, mq.ErrClosed) {
					return
				}
				if err != nil {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}
	time.Sleep(rd.DefaultBlock + time.Second) // every loop's read is parked, each at its own point of the block
	start := time.Now()
	err = cl.Close()
	took := time.Since(start)
	for i, loop := range loops {
		select {
		case <-loop:
		case <-time.After(10 * time.Second):
			t.Fatalf("consumer %d's Receive loop had not returned 10s after Client.Close", i)
		}
	}
	left := 0
	for i := range consumers {
		left += groupRows(t, raw, keys, "s"+strconv.Itoa(i))
	}
	t.Logf("Client.Close of %d consumers = %v after %v; rows left %d", consumers, err, took.Round(time.Millisecond), left)
	if bound := 5*time.Second + time.Second + 500*time.Millisecond; took > bound {
		t.Errorf("Client.Close of %d consumers with parked reads took %v, more than the shared bound, "+
			"the second after it and slack (%v)", consumers, took, bound)
	}
	if left != 0 {
		t.Errorf("Client.Close of %d consumers holding nothing left %d consumer rows", consumers, left)
	}
}

// groupRows is how many consumers subscription sub's group has on the first
// shard of keys, read through raw.
func groupRows(t *testing.T, raw rueidis.Client, keys streams.Keys, sub string) int {
	t.Helper()
	group, err := streams.Group(sub)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := raw.Do(context.Background(), raw.B().XinfoConsumers().Key(keys.Stream(0)).Group(group).Build()).ToArray()
	if err != nil {
		t.Fatalf("XINFO CONSUMERS: %v", err)
	}
	return len(rows)
}
