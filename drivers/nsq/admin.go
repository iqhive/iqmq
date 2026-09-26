package nsq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"iter"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of what this admin is.
var _ mq.Admin = (*admin)(nil)

// admin is nsqd's HTTP topology API.
//
// It is HTTP and not the TCP protocol because that is where nsqd puts it: the
// protocol has PUB, SUB and the settlement verbs and nothing administrative.
// The endpoints are stable across nsqd 1.x — /topic/create, /topic/delete,
// /channel/create, /channel/delete, /channel/empty, /stats — and every one of
// them is answered by ONE nsqd about ITSELF.
//
// That last point is the whole shape of this surface, and it is stated on the
// driver page: nsqlookupd knows which nsqd holds a topic but cannot create,
// delete or purge anything, so an administrative call has to be sent to every
// nsqd the deployment has. This driver sends each one to every address in
// [Config.AdminAddrs] and reports the first failure, and Stats SUMS what they
// answer. A deployment whose AdminAddrs name a subset of its nsqd instances
// therefore gets a partial answer, which is why the field is explicit rather
// than discovered.
type admin struct {
	b *Backend
}

// newAdminClient is the HTTP client the administrative surface talks through,
// over a transport this backend alone owns.
//
// A client with no Transport shares [http.DefaultTransport] with everything
// else in the process, and anything that calls CloseIdleConnections on that
// transport can fail a request of ours that is in flight. The route is inside
// net/http: a response with no body puts its connection back in the idle
// pool BEFORE the caller has the response, so a close that lands in between
// closes a connection the caller is still waiting on, and the caller gets
// "HTTP/1.x transport connection broken: http: CloseIdleConnections called".
// A POST is not retried, and every writing call here is a POST that nsqd
// answers with no body. httptest.Server.Close makes exactly that call on
// DefaultTransport, which is how a parallel test's cleanup failed
// TestEveryWritingAdminCallReachesEveryNSQD on GitHub; any library in an
// application that tidies the default pool can do the same to it.
//
// Owning the transport is also what lets [Backend.Close] release the
// connections it pooled, rather than leaving them in a process-wide pool
// until the idle timeout. It starts as a CLONE of
// DefaultTransport, so what an application set there before Open -- a proxy, a
// dialer, trusted roots -- still applies; only the pool is this backend's.
//
// And it is what carries [Config.TLS] to the https endpoint. DefaultTransport
// knows nothing of it, so an nsqd whose certificate came from a private CA
// verified on the protocol connection and failed every admin call as signed
// by an unknown authority, and a client certificate was never presented. The
// admin surface now verifies nsqd as the protocol connection does, with the
// same configuration -- a clone, because a transport keeps its own -- and
// against each nsqd's own name: go-nsq replaces ServerName with the host it
// is dialling, so the clone's is cleared and net/http takes each request's
// host instead.
func newAdminClient(timeout time.Duration, tlsCfg *tls.Config) *http.Client {
	tr := ownedTransport()
	if tlsCfg != nil {
		c := tlsCfg.Clone()
		c.ServerName = ""
		tr.TLSClientConfig = c
	}
	return &http.Client{Timeout: timeout, Transport: tr}
}

// ownedTransport is a transport with a pool of its own, configured as
// http.DefaultTransport is: a clone of it when it is an *http.Transport, and
// its documented settings when an application has replaced it with some other
// RoundTripper, which cannot be cloned and whose pool would be shared again.
func ownedTransport() *http.Transport {
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		return dt.Clone()
	}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// adminAddrs is the nsqd HTTP addresses to talk to.
//
// When AdminAddrs is empty they are derived from the TCP addresses by adding
// one to the port, which is nsqd's own default layout (4150 TCP and 4151
// HTTP). It is a guess and the driver page says so; a deployment that moved
// the HTTP port names it.
func (b *Backend) adminAddrs() []string {
	if len(b.cfg.AdminAddrs) > 0 {
		return b.cfg.AdminAddrs
	}
	out := make([]string, 0, len(b.cfg.Addrs))
	for _, a := range b.cfg.Addrs {
		host, port, ok := strings.Cut(a, ":")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(port)
		if err != nil {
			continue
		}
		out = append(out, host+":"+strconv.Itoa(n+1))
	}
	return out
}

// EnsureTopic creates the topic on every known nsqd.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	topic, err := TopicOf(a.b.cfg.TopicPrefix, spec.Name)
	if err != nil {
		return err
	}
	return a.b.ensureTopic(ctx, topic)
}

// DeleteTopic removes the topic and every channel on it.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	name, err := TopicOf(a.b.cfg.TopicPrefix, topic)
	if err != nil {
		return err
	}
	return a.b.post(ctx, "/topic/delete", url.Values{"topic": {name}})
}

// ListTopics iterates the topics nsqd reports.
//
// The iteration is over the UNION of what every address reports, de-duplicated,
// with the configured prefix stripped: a topic this client cannot name is not
// one it can do anything with, so a prefixed deployment does not list another
// deployment's topics.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		seen := map[string]struct{}{}
		for _, addr := range a.b.adminAddrs() {
			var s nsqdStats
			if err := a.b.get(ctx, addr, "/stats", url.Values{"format": {"json"}}, &s); err != nil {
				yield("", err)
				return
			}
			for _, t := range s.Topics {
				name, ok := strings.CutPrefix(t.Name, a.b.cfg.TopicPrefix)
				if !ok {
					continue
				}
				if _, dup := seen[name]; dup {
					continue
				}
				seen[name] = struct{}{}
				if !yield(name, nil) {
					return
				}
			}
		}
	}
}

// EnsureSubscription creates the channel, and the topic it hangs off.
//
// The topic comes first because nsqd's /channel/create fails for a topic it
// does not have, and a caller asking for a subscription on a topic that does
// not exist yet means both.
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	topic, err := TopicOf(a.b.cfg.TopicPrefix, spec.Topic)
	if err != nil {
		return err
	}
	channel, err := ChannelOf(spec.Name)
	if err != nil {
		return err
	}
	return a.b.ensureChannel(ctx, topic, channel)
}

// DeleteSubscription removes a channel.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	name, err := TopicOf(a.b.cfg.TopicPrefix, topic)
	if err != nil {
		return err
	}
	channel, err := ChannelOf(sub)
	if err != nil {
		return err
	}
	return a.b.post(ctx, "/channel/delete", url.Values{"topic": {name}, "channel": {channel}})
}

// Purge discards everything a channel has not consumed.
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	name, err := TopicOf(a.b.cfg.TopicPrefix, topic)
	if err != nil {
		return err
	}
	channel, err := ChannelOf(sub)
	if err != nil {
		return err
	}
	return a.b.post(ctx, "/channel/empty", url.Values{"topic": {name}, "channel": {channel}})
}

// Stats reports a channel's depth and in-flight count, summed over every nsqd
// the configuration names.
//
// Lag is never known. nsqd reports a channel's depth and its in-flight count
// and nothing about a position in a log, because there is no log — a channel
// holds what has not been finished and forgets the rest. The specification makes an unknown
// a Limit whose Known is false rather than a zero, which is the difference
// between a subscription that is up to date and one nobody can measure.
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	name, err := TopicOf(a.b.cfg.TopicPrefix, topic)
	if err != nil {
		return mq.QueueStats{}, err
	}
	channel, err := ChannelOf(sub)
	if err != nil {
		return mq.QueueStats{}, err
	}
	var depth, inflight int64
	found := false
	for _, addr := range a.b.adminAddrs() {
		var s nsqdStats
		if err := a.b.get(ctx, addr, "/stats", url.Values{"format": {"json"}, "topic": {name}, "channel": {channel}}, &s); err != nil {
			return mq.QueueStats{}, err
		}
		for _, t := range s.Topics {
			if t.Name != name {
				continue
			}
			for _, c := range t.Channels {
				if c.Name != channel {
					continue
				}
				found = true
				depth += c.Depth + c.DeferredCount
				inflight += c.InFlightCount
			}
		}
	}
	if !found {
		return mq.QueueStats{}, mq.ErrTopicNotFound
	}
	return mq.QueueStats{
		Depth:    mq.Limit[int64]{Value: depth, Known: true},
		InFlight: mq.Limit[int64]{Value: inflight, Known: true},
	}, nil
}

// ensureTopic creates a topic on every known nsqd.
func (b *Backend) ensureTopic(ctx context.Context, topic string) error {
	return b.post(ctx, "/topic/create", url.Values{"topic": {topic}})
}

// ensureChannel creates a topic and a channel on every known nsqd.
func (b *Backend) ensureChannel(ctx context.Context, topic, channel string) error {
	if err := b.ensureTopic(ctx, topic); err != nil {
		return err
	}
	return b.post(ctx, "/channel/create", url.Values{"topic": {topic}, "channel": {channel}})
}

// post sends one administrative command to every configured nsqd.
//
// Every one is tried even when one fails, because the alternative is a
// topology that exists on the first half of a cluster; the first failure is
// what is reported.
func (b *Backend) post(ctx context.Context, path string, q url.Values) error {
	addrs := b.adminAddrs()
	if len(addrs) == 0 {
		return errNoAdminAddress
	}
	var first error
	for _, addr := range addrs {
		if err := b.do(ctx, http.MethodPost, addr, path, q, nil); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// get reads one JSON document from one nsqd.
func (b *Backend) get(ctx context.Context, addr, path string, q url.Values, out any) error {
	return b.do(ctx, http.MethodGet, addr, path, q, out)
}

// do performs one nsqd HTTP request.
//
// The response body is read under a bound. nsqd's /stats over a busy
// deployment is large and it is the one response this driver parses, so an
// unbounded read would let a broker decide how much memory this process uses,
// which the specification's "every buffer bounded" forbids as much here as on a hot path.
//
// It refuses once the backend is closed, and a request that was IN FLIGHT when
// Close ran puts its connection back in the pool after teardown emptied it, so
// such a request empties it again on its way out. Without both, an admin
// surface held past Close repopulates a pool nothing will ever release.
func (b *Backend) do(ctx context.Context, method, addr, path string, q url.Values, out any) error {
	const maxResponse = 16 << 20
	if b.Closed() {
		return mq.ErrClosed
	}
	// Deferred first, so it runs after the body is closed and the
	// connection is back in the pool.
	defer b.releaseIfClosed()
	u := "http://" + addr + path
	if b.cfg.TLS != nil {
		u = "https://" + addr + path
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return b.errs.Map(mq.OpAdmin, "", "", err)
	}
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	if b.cfg.AuthSecret != "" {
		req.Header.Set("Authorization", "Bearer "+b.cfg.AuthSecret.Reveal())
	}
	resp, err := b.http.Do(req)
	if err != nil {
		// The URL is in net/http's error and the URL carries no credential
		// (the secret is a header), so the mapper's redaction is enough.
		return b.errs.Map(mq.OpAdmin, "", "", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return b.errs.Map(mq.OpAdmin, "", "", err)
	}
	if resp.StatusCode/100 != 2 {
		return httpStatusError(resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return errStatsUnparseable
	}
	return nil
}

// releaseIfClosed empties the admin pool when the backend has closed.
//
// Close sets the closed flag before teardown empties the pool, so a request
// that checks the flag after putting its connection back either sees it set,
// and empties the pool itself, or ran before teardown, which empties it.
func (b *Backend) releaseIfClosed() {
	if b.Closed() {
		b.http.CloseIdleConnections()
	}
}

// httpStatusError maps an nsqd HTTP status onto the codes.
//
// The body is deliberately NOT included. nsqd echoes the request's parameters
// into several of its error messages, and a topic name is the one thing a
// caller already knows — while the body of a proxied or authenticated
// deployment can carry rather more than that.
func httpStatusError(code int) error {
	switch code {
	case http.StatusNotFound:
		return mq.ErrTopicNotFound
	case http.StatusBadRequest:
		return errAdminBadRequest
	case http.StatusUnauthorized:
		return errAdminUnauthorized
	case http.StatusForbidden:
		return errAdminForbidden
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		return errAdminUnavailable
	default:
		return errAdminFailed
	}
}

// nsqdStats is the part of nsqd's /stats document this driver reads.
type nsqdStats struct {
	Version string `json:"version"`
	Topics  []struct {
		Name     string `json:"topic_name"`
		Depth    int64  `json:"depth"`
		Channels []struct {
			Name          string `json:"channel_name"`
			Depth         int64  `json:"depth"`
			InFlightCount int64  `json:"in_flight_count"`
			DeferredCount int64  `json:"deferred_count"`
		} `json:"channels"`
	} `json:"topics"`
}

// The admin surface's refusals.
var (
	errNoAdminAddress    = errcode.New(errcode.Unsupported, "iqmq/nsq: the administrative API is nsqd's HTTP endpoint and no address for it could be derived; set Config.AdminAddrs")
	errStatsUnparseable  = errcode.New(errcode.Unknown, "iqmq/nsq: nsqd's /stats response could not be parsed")
	errAdminBadRequest   = errcode.New(errcode.InvalidArgument, "iqmq/nsq: nsqd refused the administrative request as malformed")
	errAdminUnauthorized = errcode.New(errcode.Unauthorized, "iqmq/nsq: nsqd refused the administrative request as unauthenticated")
	errAdminForbidden    = errcode.New(errcode.Forbidden, "iqmq/nsq: nsqd refused the administrative request as forbidden")
	errAdminUnavailable  = errcode.New(errcode.Unavailable, "iqmq/nsq: nsqd's administrative endpoint is unavailable")
	errAdminFailed       = errcode.New(errcode.Unknown, "iqmq/nsq: nsqd's administrative endpoint answered with an unexpected status")
)
