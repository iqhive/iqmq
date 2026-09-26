package pulsar

import (
	"context"
	"encoding/json"
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

// The compile-time proof of what this administrative surface is.
var _ mq.Admin = (*admin)(nil)

// admin is Pulsar's HTTP administrative API behind Admin interface.
//
// It is HTTP and not the binary protocol because that is where Pulsar puts
// topology: pulsar-client-go's `pulsaradmin` package wraps the same endpoints,
// and this driver calls them directly rather than taking a second module for
// eight requests. Every call is idempotent where the specification says it must be — a 409
// from Pulsar means "it is already like that", which is success for an Ensure
// and nothing else.
type admin struct{ b *Backend }

// newAdminClient is the HTTP client the administrative surface talks through,
// over a transport this backend alone owns.
//
// A client with no Transport shares [http.DefaultTransport] with everything
// else in the process, and anything that calls CloseIdleConnections on that
// transport can fail a request of ours that is in flight: net/http puts a
// connection back in the idle pool BEFORE it hands the caller a response with
// no body -- every 204 the writing endpoints answer -- so a close landing in
// between closes a connection the caller is still waiting on, with
// "HTTP/1.x transport connection broken: http: CloseIdleConnections called".
// httptest.Server.Close makes that call, which is how drivers/nsq's identical
// client failed a GitHub run from a parallel test's cleanup; any library in an
// application that tidies the default pool can do the same.
//
// Owning the transport is also what lets [Backend.Close] -- and an Open that
// is refused after it has asked the broker for its facts -- release the
// connections it pooled. It starts as a CLONE of
// DefaultTransport, so what an application set there before Open still
// applies: until the admin surface carries Config.TLS,
// DefaultTransport's TLSClientConfig is the in-process way to trust a private
// CA on an https AdminURL, and a transport built from defaults would drop it.
func newAdminClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: ownedTransport()}
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

// EnsureTopic creates the topic if it does not exist. It is idempotent.
//
// Whether the topic is partitioned is [Config.Partitions]: zero creates a
// non-partitioned topic, which is the only kind on which pulsar-client-go
// honours a Seek, and a positive value creates a partitioned one. A topic that
// already exists is left as it is, partition count included: raising a
// partition count is a rehash of every key and the specification's "idempotent" does not
// stretch to it.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	topic, err := TopicOf(a.b.cfg, spec.Name)
	if err != nil {
		return err
	}
	if a.b.cfg.Partitions > 0 {
		return a.put(ctx, a.path(topic)+"/partitions", strconv.Itoa(a.b.cfg.Partitions), spec.Name)
	}
	return a.put(ctx, a.path(topic), "", spec.Name)
}

// DeleteTopic removes the topic and everything on it. It is destructive
// and explicit.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	t, err := TopicOf(a.b.cfg, topic)
	if err != nil {
		return err
	}
	// The partitioned form first, because deleting a partitioned topic
	// through the non-partitioned endpoint answers 409 and leaves the
	// partitions behind.
	//
	// A 404 or a 409 from THAT endpoint is not success and must not be
	// accepted here. Both mean the topic has no partitioned metadata --
	// which is what Pulsar answers for an ordinary non-partitioned topic,
	// the kind Partitions=0 creates and the only kind pulsar-client-go
	// honours a Seek on. Accepting them returned nil with the topic still
	// on the broker, and made the fallback below unreachable in exactly the
	// case it was written for. Falling through instead
	// costs one extra request on a partitioned topic that was already gone,
	// and the fallback accepts a 404 of its own, so a delete of something
	// that does not exist is still success.
	if err := a.do(ctx, http.MethodDelete, a.path(t)+"/partitions?force=true", "", topic); err == nil {
		return nil
	}
	return a.do(ctx, http.MethodDelete, a.path(t)+"?force=true", "", topic, http.StatusNotFound)
}

// ListTopics yields every topic in the configured namespace.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		ns := a.namespace()
		for _, suffix := range [...]string{"", "/partitioned"} {
			var names []string
			path := "/admin/v2/" + a.domain() + "/" + ns + suffix
			if err := a.get(ctx, path, &names, ""); err != nil {
				yield("", err)
				return
			}
			for _, n := range names {
				t, err := ParseTopic(n)
				if err != nil {
					// A topic the broker holds and this driver's
					// grammar refuses. It is reported rather
					// than hidden: a name nothing here can
					// address is worth knowing about.
					if !yield(n, nil) {
						return
					}
					continue
				}
				if !yield(a.strip(t.Local), nil) {
					return
				}
			}
		}
	}
}

// EnsureSubscription creates the subscription if it does not exist. It is
// idempotent.
//
// The position it is created at is the END of the topic, which is row
// the default and the same answer [mq.StartPosition]'s zero value resolves
// to: a subscription created by provisioning must not replay a topic's history
// by omission. A consumer that wants the beginning opens with StartEarliest,
// which creates the subscription itself when this one did not.
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	topic, err := TopicOf(a.b.cfg, spec.Topic)
	if err != nil {
		return err
	}
	sub, err := SubscriptionOf(spec.Name)
	if err != nil {
		return err
	}
	// Pulsar's REST API takes a message identifier as the cursor's starting
	// position; (-1, -1) is the latest.
	return a.put(ctx, a.path(topic)+"/subscription/"+url.PathEscape(sub),
		`{"ledgerId":-1,"entryId":-1}`, spec.Topic)
}

// DeleteSubscription removes the subscription.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	t, err := TopicOf(a.b.cfg, topic)
	if err != nil {
		return err
	}
	name, err := SubscriptionOf(sub)
	if err != nil {
		return err
	}
	return a.do(ctx, http.MethodDelete, a.path(t)+"/subscription/"+url.PathEscape(name)+"?force=true", "", topic, http.StatusNotFound)
}

// Purge discards the subscription's backlog.
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	t, err := TopicOf(a.b.cfg, topic)
	if err != nil {
		return err
	}
	name, err := SubscriptionOf(sub)
	if err != nil {
		return err
	}
	return a.do(ctx, http.MethodPost, a.path(t)+"/subscription/"+url.PathEscape(name)+"/skip_all", "", topic, http.StatusNotFound)
}

// Stats reports the subscription's depth and in-flight count.
//
// Every figure is an mq.Limit, so "Pulsar did not report this" is a different
// answer from "it is zero" — which matters here, because a topic with no
// subscription of that name reports nothing at all rather than an empty one.
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	t, err := TopicOf(a.b.cfg, topic)
	if err != nil {
		return mq.QueueStats{}, err
	}
	name, err := SubscriptionOf(sub)
	if err != nil {
		return mq.QueueStats{}, err
	}
	var body struct {
		Subscriptions map[string]struct {
			MsgBacklog       int64 `json:"msgBacklog"`
			UnackedMessages  int64 `json:"unackedMessages"`
			MsgDelayed       int64 `json:"msgDelayed"`
			BacklogSizeBytes int64 `json:"backlogSize"`
		} `json:"subscriptions"`
	}
	if err := a.get(ctx, a.path(t)+"/stats", &body, topic); err != nil {
		return mq.QueueStats{}, err
	}
	s, ok := body.Subscriptions[name]
	if !ok {
		return mq.QueueStats{}, nil
	}
	return mq.QueueStats{
		Depth:    mq.Limit[int64]{Value: s.MsgBacklog, Known: true},
		InFlight: mq.Limit[int64]{Value: s.UnackedMessages, Known: true},
		Lag:      mq.Limit[int64]{Value: s.MsgBacklog, Known: true},
	}, nil
}

// SetNamespaceRetention sets the configured namespace's retention policy.
//
// It is a TYPED administrative extension rather than part of the portable
// Admin, for the reason drivers/sqs's redrive configuration is one: retention
// is what decides whether this driver's FeatureReplay claim holds, and the
// portable interface has no word for it. Pulsar's default retention is zero,
// under which an entry every subscription has acknowledged may be deleted at
// once — so a deployment that wants to replay a topic sets a policy, and a
// conformance run that means to VERIFY the replay claim has to set one too.
//
// Minutes or megabytes of -1 mean "unbounded" in Pulsar's own vocabulary.
func (a *admin) SetNamespaceRetention(ctx context.Context, minutes, megabytes int) error {
	body := `{"retentionTimeInMinutes":` + strconv.Itoa(minutes) +
		`,"retentionSizeInMB":` + strconv.Itoa(megabytes) + `}`
	return a.do(ctx, http.MethodPost, "/admin/v2/namespaces/"+a.namespace()+"/retention", body, "")
}

// SetNamespaceRetention is [admin.SetNamespaceRetention] reached from the
// backend, for a caller that has the backend rather than the mq.Admin.
func (b *Backend) SetNamespaceRetention(ctx context.Context, minutes, megabytes int) error {
	if b.cfg.AdminURL == "" {
		return errNoAdminURL
	}
	return (&admin{b: b}).SetNamespaceRetention(ctx, minutes, megabytes)
}

// domain is the administrative path segment for the configured topic domain.
func (a *admin) domain() string {
	if a.b.cfg.Domain == DomainNonPersistent {
		return DomainNonPersistent
	}
	return DomainPersistent
}

// namespace is `<tenant>/<namespace>`.
func (a *admin) namespace() string { return a.b.cfg.Tenant + "/" + a.b.cfg.Namespace }

// path is the administrative path of one topic.
func (a *admin) path(t TopicName) string {
	return "/admin/v2/" + t.Domain + "/" + t.Tenant + "/" + t.Namespace + "/" + url.PathEscape(t.Local)
}

// strip removes the configured prefix from a topic's local name, which is the
// portable topic a caller named.
func (a *admin) strip(local string) string {
	if a.b.cfg.TopicPrefix == "" {
		return local
	}
	return strings.TrimPrefix(local, a.b.cfg.TopicPrefix)
}

// put is an idempotent creation: a 409 means it already exists, which is
// success.
func (a *admin) put(ctx context.Context, path, body, topic string) error {
	return a.do(ctx, http.MethodPut, path, body, topic, http.StatusConflict)
}

// get performs a GET and decodes the JSON body into dst.
func (a *admin) get(ctx context.Context, path string, dst any, topic string) error {
	if a.b.isClosed() {
		return mq.ErrClosed
	}
	// Deferred first, so it runs after the body is closed and the
	// connection is back in the pool (see do).
	defer a.b.releaseIfClosed()
	resp, err := a.request(ctx, http.MethodGet, path, "")
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return errcode.New(errcode.NotFound, "iqmq/pulsar: the broker has no such topic or namespace")
	}
	if resp.StatusCode >= 300 {
		return a.status(resp, topic)
	}
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	return nil
}

// do performs a request and accepts the given extra status codes beside the
// 2xx range.
//
// It refuses once the backend is closed, and a request that was IN FLIGHT when
// Close ran -- past the check, but asking the transport for a connection only
// after teardown emptied the pool -- puts that connection back when it
// finishes, so such a request empties the pool again on its way out. Without
// both, an admin surface held past Close repopulates a pool nothing will ever
// release.
func (a *admin) do(ctx context.Context, method, path, body, topic string, accept ...int) error {
	if a.b.isClosed() {
		return mq.ErrClosed
	}
	// Deferred first, so it runs after the body is closed and the
	// connection is back in the pool.
	defer a.b.releaseIfClosed()
	resp, err := a.request(ctx, method, path, body)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 300 {
		return nil
	}
	for _, code := range accept {
		if resp.StatusCode == code {
			return nil
		}
	}
	return a.status(resp, topic)
}

// request performs one administrative request.
func (a *admin) request(ctx context.Context, method, path, body string) (*http.Response, error) {
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	var req *http.Request
	var err error
	if rdr != nil {
		req, err = http.NewRequestWithContext(ctx, method, strings.TrimSuffix(a.b.cfg.AdminURL, "/")+path, rdr)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, strings.TrimSuffix(a.b.cfg.AdminURL, "/")+path, nil)
	}
	if err != nil {
		return nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if tok := a.b.cfg.Token.Reveal(); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	return a.b.http.Do(req)
}

// isClosed reports whether the backend has closed. The broker-facts lookup
// runs inside newBackend, before there is a base to ask, and a backend that
// does not exist yet has not closed.
func (b *Backend) isClosed() bool { return b.BaseBackend != nil && b.Closed() }

// releaseIfClosed empties the admin pool when the backend has closed.
//
// Close sets the closed flag before teardown empties the pool, so a request
// that checks the flag after putting its connection back either sees it set,
// and empties the pool itself, or ran before teardown, which empties it.
func (b *Backend) releaseIfClosed() {
	if b.isClosed() {
		b.http.CloseIdleConnections()
	}
}

// status turns an administrative HTTP status into the model.
//
// The response BODY is deliberately not read into the error. Pulsar echoes the
// request in some failures, and a request this driver made carries an
// Authorization header's topic and namespace but could in principle carry
// more; forbid a credential or an address in an error string, and
// the status code is what a caller acts on.
func (a *admin) status(resp *http.Response, topic string) error {
	code := errcode.Unknown
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		code = errcode.Unauthorized
	case http.StatusForbidden:
		code = errcode.Forbidden
	case http.StatusNotFound:
		code = errcode.NotFound
	case http.StatusConflict:
		code = errcode.Conflict
	case http.StatusPreconditionFailed, http.StatusBadRequest:
		code = errcode.InvalidArgument
	case http.StatusTooManyRequests:
		code = errcode.Throttled
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		code = errcode.Unavailable
	case http.StatusNotImplemented:
		code = errcode.Unsupported
	}
	return &mq.OpError{
		Driver: DriverName, Operation: mq.OpAdmin, Topic: topic,
		Code:      mq.ErrorCode(code),
		Retryable: code == errcode.Unavailable || code == errcode.Throttled,
		Err:       errcode.New(code, "iqmq/pulsar: the administrative API answered HTTP "+strconv.Itoa(resp.StatusCode)),
	}
}

// ensureTopic creates a topic before a handle is opened, for
// [Config.EnsureTopology].
func (b *Backend) ensureTopic(ctx context.Context, t TopicName) error {
	if b.cfg.AdminURL == "" {
		return errNoAdminURL
	}
	a := &admin{b: b}
	if b.cfg.Partitions > 0 {
		return a.put(ctx, a.path(t)+"/partitions", strconv.Itoa(b.cfg.Partitions), t.Local)
	}
	return a.put(ctx, a.path(t), "", t.Local)
}

// brokerHealth is Ping's probe when there is an administrative endpoint: the
// broker's own health check, which touches no topic.
func (b *Backend) brokerHealth(ctx context.Context) error {
	a := &admin{b: b}
	return a.do(ctx, http.MethodGet, "/admin/v2/brokers/health", "", "")
}

// brokerFacts is what the broker's own runtime configuration says, read once at
// open.
type brokerFacts struct {
	// version is the broker's version, for DriverInfo. An empty
	// string is "the driver did not say", which is the truth when there is
	// no administrative endpoint.
	version string
	// maxMessageSize is the broker's own maxMessageSize. It is the one
	// limit this driver can LEARN rather than assume: the figure is
	// negotiated per connection in Pulsar's binary protocol and
	// pulsar-client-go surfaces the negotiated value nowhere, so the
	// administrative API is the only route.
	maxMessageSize int64
	// batchIndexAck is the broker's acknowledgmentAtBatchIndexLevelEnabled,
	// which decides whether an acknowledgement set is honoured or ignored —
	// and a broker that ignores one settles the whole batched entry. It is
	// FALSE by default in Apache Pulsar, and false here when nothing asked.
	batchIndexAck bool
}

// brokerVersion is the broker's version, for DriverInfo, or an empty
// string when nothing asked.
func (b *Backend) brokerVersion() string { return b.broker.version }

// lookupBrokerFacts reads the broker's runtime configuration.
//
// It is best effort for the limit and load-bearing for the batch-index flag: a
// broker that does not answer leaves the documented default message size in
// place, and leaves batchIndexAck false — which makes Open refuse
// Config.EnableBatchIndexAck rather than risk the sibling settlement.
func (b *Backend) lookupBrokerFacts(ctx context.Context) (brokerFacts, bool) {
	if b.cfg.AdminURL == "" {
		return brokerFacts{}, false
	}
	ctx, cancel := context.WithTimeout(ctx, b.cfg.OperationTimeout)
	defer cancel()
	a := &admin{b: b}
	var runtime map[string]string
	if err := a.get(ctx, "/admin/v2/brokers/configuration/runtime", &runtime, ""); err != nil {
		return brokerFacts{}, false
	}
	facts := brokerFacts{
		version:       runtime["brokerVersion"],
		batchIndexAck: runtime["acknowledgmentAtBatchIndexLevelEnabled"] == "true",
	}
	if v, ok := runtime["maxMessageSize"]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			facts.maxMessageSize = n
		}
	}
	return facts, true
}

// The administrative refusals.
var (
	errNoAdminURL   = errcode.New(errcode.Unsupported, "iqmq/pulsar: Config.AdminURL is not set, so this client has no administrative surface; Pulsar's topology API is HTTP on a different port from the binary protocol and this driver does not guess it")
	errNoPingTarget = errcode.New(errcode.Unsupported, "iqmq/pulsar: Ping has nothing to probe: there is no Config.AdminURL and this backend has opened no handle, and a metadata lookup for a topic nobody created would be the hidden topology creation the specification forbids. Set Config.AdminURL or Config.PingTopic")
)
