package nsq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	"bitbucket.org/iqhive/iqmq/drivers/nsq/internal/nsqapi"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// The compile-time proof that the real client satisfies the narrow interface
// the driver and the fake share.
var (
	_ nsqapi.Producer = (*producer)(nil)
	_ nsqapi.Consumer = (*consumer)(nil)
)

// clientConfig turns a [Config] into go-nsq's, which is the one place this
// driver decides how go-nsq behaves.
//
// Three of the settings are not tuning and must not be read as tuning.
//
//	MaxAttempts        = 0  go-nsq's default is 5, and what it does when a
//	                        message's Attempts passes it is call FINISH and
//	                        log a warning. That is a message DISCARDED by the
//	                        client library while the core was still retrying
//	                        it — a silent loss of an accepted message, which
//	                        The specification makes a disqualifier. Zero means "never
//	                        give up here"; giving up is the core's decision
//	                        and it dead-letters rather than dropping.
//	MaxBackoffDuration = 0  go-nsq's default backoff sets RDY to zero for up
//	                        to two minutes after a requeue, so the whole
//	                        subscription stops receiving because ONE message
//	                        failed. The specification gives redelivery policy to one layer;
//	                        a client library pausing the consumer underneath
//	                        the core makes the core's retry schedule a
//	                        fiction.
//	MaxAttempts's partner   every delivery is marked DisableAutoResponse, so
//	                        the handler returning is not a settlement. See
//	                        source.handle.
//
// It returns go-nsq's own validation error unchanged when it has one: that
// error names the field, and rewriting it would lose the name.
func clientConfig(cfg Config, maxInFlight int, deadline time.Duration) (*gonsq.Config, error) {
	c := gonsq.NewConfig()
	c.MaxInFlight = maxInFlight
	// A zero deadline leaves nsqd's own --msg-timeout in place, which is what
	// a PRODUCER wants: msg_timeout is negotiated in the IDENTIFY handshake
	// by every client, receiving or not, and nsqd refuses one below its
	// --min-msg-timeout. So a producer that passed the consumer's ack
	// deadline through would have its CONNECTION refused for a value it has
	// no use for — which arrived as "publish failed: failed to IDENTIFY"
	// against a 500 ms scenario deadline and is the reason the two
	// configurations are built separately.
	c.MsgTimeout = deadline
	c.MaxAttempts = 0
	c.MaxBackoffDuration = 0
	c.BackoffMultiplier = 0
	c.DefaultRequeueDelay = 0
	c.MaxRequeueDelay = cfg.MaxRequeueDelay
	c.DialTimeout = cfg.DialTimeout
	c.ReadTimeout = cfg.ReadTimeout
	c.WriteTimeout = cfg.WriteTimeout
	c.HeartbeatInterval = cfg.HeartbeatInterval
	c.LookupdPollInterval = cfg.LookupdPollInterval
	c.OutputBufferTimeout = cfg.OutputBufferTimeout
	if cfg.ClientID != "" {
		c.ClientID = cfg.ClientID
	}
	if cfg.Hostname != "" {
		c.Hostname = cfg.Hostname
	}
	if cfg.UserAgent != "" {
		c.UserAgent = cfg.UserAgent
	}
	if cfg.AuthSecret != "" {
		c.AuthSecret = cfg.AuthSecret.Reveal()
		// AuthSecret is nsqd's AUTH secret, and go-nsq sends it to nsqLOOKUPD
		// as well unless told not to. `Config.LookupdAuthorization` defaults to
		// TRUE (`config.go`: `default:"true"`, applied by `NewConfig`), and
		// `queryLookupd` then sets `Authorization: Bearer <AuthSecret>` on every
		// discovery poll -- to an address that is cleartext unless it says
		// `https://`, because the scheme decides and go-nsq prepends `http://`
		// when there is none.
		//
		// MEASURED, not inferred. With `AuthSecret` set and an `http://`
		// lookupd address, the first bytes off a loopback listener were:
		//
		//	GET /lookup?topic=orders HTTP/1.1
		//	Host: 127.0.0.1:33247
		//	Authorization: Bearer s3cret-bearer-token
		//
		// `fix/dsn-dial` recorded the opposite -- "it carries no credential,
		// AuthSecret goes out as a bearer token only from Backend.do, which is
		// never given a lookupd address" -- and that claim was wrong: it is true
		// of this driver's OWN requests and not of go-nsq's. It was the whole
		// premise for leaving a cleartext lookupd address documented rather than
		// refused, so the premise had to be tested before the decision could
		// stand.
		//
		// The secret is therefore NOT sent to lookupd at all. That is the
		// narrow reading of what the field is: nsqlookupd is a discovery
		// service and this is nsqd's AUTH secret, so the credential goes where
		// the credential is for. Refusing an `http://` address beside an
		// AuthSecret was the alternative and is strictly worse: it breaks a
		// working deployment to protect a header that should not have been on
		// that request in the first place, and it leaves the secret on the wire
		// for anyone who writes `https://` to a lookupd that does not want it.
		// A deployment that really does put an authorizing proxy in front of
		// nsqlookupd needs an explicit opt-in, which is recorded in
		// the maintenance notes rather than guessed at here.
		c.LookupdAuthorization = false
	}
	if cfg.TLS != nil {
		c.TlsV1 = true
		c.TlsConfig = cfg.TLS
	}
	if cfg.Deflate {
		c.Deflate = true
		c.DeflateLevel = cfg.DeflateLevel
	}
	c.Snappy = cfg.Snappy
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// producer publishes to the configured nsqd endpoints.
//
// One `*nsq.Producer` per endpoint, chosen round-robin. There is no failover
// and no retry: the specification gives retry policy to the core, and a driver that tried
// the next endpoint on a failure would turn one publication into two attempts
// with the core's own attempt count blind to the second. A publication that
// fails fails, and the core's next attempt lands on the next endpoint anyway,
// because the cursor has moved.
type producer struct {
	cfg   Config
	nsqds []*gonsq.Producer
	next  atomic.Uint64

	stopOnce sync.Once
}

// newProducer builds one go-nsq producer per configured nsqd address.
func newProducer(cfg Config) (*producer, error) {
	if len(cfg.Addrs) == 0 {
		// Publication goes to an nsqd directly; nsqlookupd is a discovery
		// service for consumers and cannot be published through. A
		// configuration with only lookupd addresses can consume, and this
		// is where it finds out it cannot publish.
		return nil, errNoPublishAddress
	}
	gc, err := clientConfig(cfg, 1, 0)
	if err != nil {
		return nil, err
	}
	p := &producer{cfg: cfg}
	for _, addr := range cfg.Addrs {
		w, err := gonsq.NewProducer(addr, gc)
		if err != nil {
			p.Close()
			return nil, err
		}
		// go-nsq logs to stderr at Info level by default, which turns a
		// library into a program that prints. A nil logger silences it;
		// what matters operationally reaches the caller as an error or as
		// a health transition.
		w.SetLogger(nil, gonsq.LogLevelError)
		p.nsqds = append(p.nsqds, w)
	}
	return p, nil
}

// pick returns the next endpoint round-robin.
func (p *producer) pick() *gonsq.Producer {
	if len(p.nsqds) == 1 {
		return p.nsqds[0]
	}
	i := p.next.Add(1) - 1
	return p.nsqds[i%uint64(len(p.nsqds))]
}

// Addr is the endpoint the next publication would use.
func (p *producer) Addr() string {
	if len(p.nsqds) == 0 {
		return ""
	}
	i := p.next.Load()
	return p.nsqds[i%uint64(len(p.nsqds))].String()
}

// Publish sends one PUB and waits for nsqd's answer under ctx.
func (p *producer) Publish(ctx context.Context, topic string, body []byte) error {
	return p.await(ctx, func(w *gonsq.Producer, done chan *gonsq.ProducerTransaction) error {
		return w.PublishAsync(topic, body, done)
	})
}

// MultiPublish sends one MPUB and waits for nsqd's answer under ctx.
func (p *producer) MultiPublish(ctx context.Context, topic string, bodies [][]byte) error {
	return p.await(ctx, func(w *gonsq.Producer, done chan *gonsq.ProducerTransaction) error {
		return w.MultiPublishAsync(topic, bodies, done)
	})
}

// DeferredPublish sends one DPUB and waits for nsqd's answer under ctx.
func (p *producer) DeferredPublish(ctx context.Context, topic string, delay time.Duration, body []byte) error {
	return p.await(ctx, func(w *gonsq.Producer, done chan *gonsq.ProducerTransaction) error {
		return w.DeferredPublishAsync(topic, delay, body, done)
	})
}

// await runs one asynchronous publish and waits for it under ctx.
//
// go-nsq's synchronous Publish takes no context and blocks until nsqd answers,
// so it is unusable: the specification requires every blocking operation to accept one. The
// asynchronous form plus a select is the same round trip with a cancellation
// the caller controls.
//
// A context that is done while the frame is in flight is AMBIGUOUS and not a
// failure. The command has already been written, nsqd may well have taken it,
// and reporting "did not publish" would make a duplicate look like a loss
// The done channel is buffered so that the transaction's own goroutine
// never blocks writing to a channel this function has stopped reading.
func (p *producer) await(ctx context.Context, send func(*gonsq.Producer, chan *gonsq.ProducerTransaction) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan *gonsq.ProducerTransaction, 1)
	if err := send(p.pick(), done); err != nil {
		// Nothing was written: the producer is stopped, or the connection
		// could not be made. A definite refusal.
		return err
	}
	select {
	case t := <-done:
		return t.Error
	case <-ctx.Done():
		return &ambiguousError{cause: ctx.Err()}
	}
}

// Ping connects if necessary and sends a NOP.
func (p *producer) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.nsqds) == 0 {
		return errNoPublishAddress
	}
	// go-nsq's Ping takes no context, so it is run on a goroutine and the
	// caller's context bounds the wait rather than the work. The goroutine
	// finishes on its own: the producer's own dial and write deadlines bound
	// it, and Close stops it.
	res := make(chan error, 1)
	w := p.pick()
	go func() { res <- w.Ping() }()
	select {
	case err := <-res:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops every producer. It is idempotent.
func (p *producer) Close() {
	p.stopOnce.Do(func() {
		for _, w := range p.nsqds {
			w.Stop()
		}
	})
}

// consumer is one nsqd channel subscription.
type consumer struct {
	c *gonsq.Consumer
}

// newConsumer builds a go-nsq consumer for one topic and channel.
func newConsumer(cfg Config, topic, channel string, maxInFlight int, deadline time.Duration) (*consumer, error) {
	gc, err := clientConfig(cfg, maxInFlight, deadline)
	if err != nil {
		return nil, err
	}
	c, err := gonsq.NewConsumer(topic, channel, gc)
	if err != nil {
		return nil, err
	}
	c.SetLogger(nil, gonsq.LogLevelError)
	// nsqlookupd discovery is a THIRD HTTP surface and it ignored Config.TLS.
	//
	// `gc.TlsConfig` above reaches the nsqd TCP connection and nothing else.
	// go-nsq builds its own `*http.Client` for lookupd polling
	// (`consumer.go`'s `ConnectToNSQLookupd`) with a transport whose
	// `TLSClientConfig` it never sets and never reads from its Config -- so an
	// `https://` entry in LookupdAddrs verified against the SYSTEM roots,
	// which is not what the operator configured: an nsqlookupd whose
	// certificate came from a private CA in Config.TLS.RootCAs failed as
	// signed by an unknown authority, Config.TLS.InsecureSkipVerify did
	// nothing, and a client certificate was never presented. That is the defect -- the same one the admin surface had -- on
	// the one HTTP surface this driver did not own, and `newAdminClient` is
	// already the right shape for it: an owned transport, a cloned config and
	// a cleared ServerName so each request's own host is verified.
	//
	// The timeout is go-nsq's own for this poll, so a lookupd that hangs costs
	// what it cost before. The client is installed BEFORE any connect, which
	// is the only ordering that works: go-nsq builds its default one lazily
	// inside `ConnectToNSQLookupd` and one set afterwards would never be used.
	//
	// It does NOT make an `http://` or scheme-less entry secure and cannot:
	// the scheme decides, and go-nsq prepends `http://` when there is none.
	// Config.LookupdAddrs documents that, and `lookupd_wire_test.go` measures
	// both halves on the wire.
	if cfg.TLS != nil {
		c.SetLookupdHttpClient(newAdminClient(gc.LookupdPollTimeout, cfg.TLS))
	}
	return &consumer{c: c}, nil
}

func (c *consumer) AddConcurrentHandlers(h nsqapi.Handler, n int) { c.c.AddConcurrentHandlers(h, n) }

func (c *consumer) ConnectToNSQLookupds(addrs []string) error { return c.c.ConnectToNSQLookupds(addrs) }

func (c *consumer) ConnectToNSQDs(addrs []string) error { return c.c.ConnectToNSQDs(addrs) }

func (c *consumer) Stop() { c.c.Stop() }

func (c *consumer) StopChan() <-chan int { return c.c.StopChan }

// ambiguousError is a publish whose outcome nobody knows.
//
// The specification has a code for it and IsAmbiguous reads it, and the cause is kept so
// that errors.Is still finds context.Canceled or DeadlineExceeded — which the specification
// requires to stay discoverable through whatever a driver wraps them in.
type ambiguousError struct{ cause error }

func (e *ambiguousError) Error() string {
	return "iqmq/nsq: the publish command was written and nsqd's answer was not read: " + e.cause.Error()
}

func (e *ambiguousError) Unwrap() error { return e.cause }

var errNoPublishAddress = errcode.New(errcode.InvalidConfig,
	"iqmq/nsq: publication needs an nsqd address; nsqlookupd is a discovery service for consumers and cannot be published through")

// ambiguous reports whether err is one of this driver's ambiguous outcomes, so
// that the error mapper can set the flag without a type switch of its own.
func ambiguous(err error) bool {
	var a *ambiguousError
	return errors.As(err, &a)
}
