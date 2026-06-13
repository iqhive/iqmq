package nsq_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// The Admin surface, driven against a fake nsqd HTTP endpoint.
//
// nsqd's topology API is HTTP -- /topic/create, /channel/empty, /stats -- so
// all of it is reachable from `httptest` and none of it needed the container
// arm it was only ever run in. The shape that matters is the FAN-OUT:
// nsqlookupd cannot create, delete or purge anything, so every administrative
// call goes to every address in Config.AdminAddrs and Stats sums what they
// answer. A test with one endpoint cannot see that, so
// most of these run two.

// nsqdStub is one fake nsqd. It records every request it is asked for and
// answers /stats from whatever the test put in `stats`.
type nsqdStub struct {
	srv *httptest.Server

	mu       sync.Mutex
	requests []string       // "METHOD path?query", in order
	headers  []http.Header  // the headers of each request, same order
	stats    string         // the /stats body
	status   map[string]int // path -> status to answer instead of 200
}

// newNSQD starts a fake nsqd and stops it when the test ends.
func newNSQD(t *testing.T) *nsqdStub {
	t.Helper()
	s := &nsqdStub{stats: `{"version":"1.2.1","topics":[]}`, status: map[string]int{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests = append(s.requests, r.Method+" "+r.URL.RequestURI())
		s.headers = append(s.headers, r.Header.Clone())
		code, body := http.StatusOK, s.stats
		if c, ok := s.status[r.URL.Path]; ok {
			code = c
		}
		s.mu.Unlock()
		w.WriteHeader(code)
		if code/100 == 2 && r.URL.Path == "/stats" {
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// addr is the host:port the driver should be pointed at.
func (s *nsqdStub) addr() string { return strings.TrimPrefix(s.srv.URL, "http://") }

// setStats replaces the /stats body.
func (s *nsqdStub) setStats(body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats = body
}

// fail makes one path answer a status instead of 200.
func (s *nsqdStub) fail(path string, code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[path] = code
}

// seen is the requests this nsqd was sent.
func (s *nsqdStub) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

// sawPath reports whether any request was for a path.
func (s *nsqdStub) sawPath(path string) bool {
	for _, r := range s.seen() {
		if strings.Contains(r, " "+path) {
			return true
		}
	}
	return false
}

// adminOver opens an admin surface pointed at the given fake nsqd addresses.
func adminOver(t *testing.T, addrs ...string) mq.Admin {
	t.Helper()
	b, _ := newBackendRig(t, func(c *nq.Config) { c.AdminAddrs = addrs })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	return admin
}

// statsBody is one nsqd's /stats answer for a single topic and channel.
func statsBody(topic, channel string, depth, inflight, deferredCount int64) string {
	b, err := json.Marshal(map[string]any{
		"version": "1.2.1",
		"topics": []map[string]any{{
			"topic_name": topic,
			"depth":      depth,
			"channels": []map[string]any{{
				"channel_name":    channel,
				"depth":           depth,
				"in_flight_count": inflight,
				"deferred_count":  deferredCount,
			}},
		}},
	})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// TestEveryWritingAdminCallReachesEveryNSQD is the fan-out, and it is the
// whole shape of this surface.
//
// One nsqd knows only about itself. A driver that sent /topic/create to the
// first address and stopped would create the topic on one instance of a
// cluster and report success, and the topic would then exist or not depending
// on which nsqd a producer happened to connect to.
func TestEveryWritingAdminCallReachesEveryNSQD(t *testing.T) {
	t.Parallel()
	a, b := newNSQD(t), newNSQD(t)
	admin := adminOver(t, a.addr(), b.addr())
	ctx := t.Context()

	for _, tc := range []struct {
		name string
		path string
		call func() error
	}{
		{"EnsureTopic", "/topic/create", func() error {
			return admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"})
		}},
		{"DeleteTopic", "/topic/delete", func() error { return admin.DeleteTopic(ctx, "orders") }},
		{"DeleteSubscription", "/channel/delete", func() error {
			return admin.DeleteSubscription(ctx, "orders", "workers")
		}},
		{"Purge", "/channel/empty", func() error { return admin.Purge(ctx, "orders", "workers") }},
	} {
		if err := tc.call(); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !a.sawPath(tc.path) || !b.sawPath(tc.path) {
			t.Errorf("%s sent %s to only some of the two nsqd instances (first saw it: %t, second: %t); "+
				"nsqlookupd cannot create or delete anything, so an administrative call has to reach every one",
				tc.name, tc.path, a.sawPath(tc.path), b.sawPath(tc.path))
		}
	}

	// EnsureSubscription creates the topic FIRST: nsqd's /channel/create
	// fails for a topic it does not have.
	c := newNSQD(t)
	single := adminOver(t, c.addr())
	if err := single.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	seen := c.seen()
	topicAt, channelAt := -1, -1
	for i, r := range seen {
		if strings.Contains(r, "/topic/create") && topicAt < 0 {
			topicAt = i
		}
		if strings.Contains(r, "/channel/create") && channelAt < 0 {
			channelAt = i
		}
	}
	if topicAt < 0 || channelAt < 0 {
		t.Fatalf("EnsureSubscription sent %v, want both /topic/create and /channel/create", seen)
	}
	if topicAt > channelAt {
		t.Errorf("EnsureSubscription created the channel before the topic (%v); nsqd's /channel/create fails "+
			"for a topic it does not have", seen)
	}
}

// TestAFailureOnOneNSQDIsReported is the other half of the fan-out: a call
// that reached two instances and failed on one has NOT succeeded.
//
// Reporting the first failure rather than the last is deliberate and it is
// the reason the healthy instance is second: a driver that overwrote the
// error with the next instance's nil would report success for a half-applied
// change.
func TestAFailureOnOneNSQDIsReported(t *testing.T) {
	t.Parallel()
	bad, good := newNSQD(t), newNSQD(t)
	bad.fail("/topic/create", http.StatusBadRequest)
	admin := adminOver(t, bad.addr(), good.addr())

	err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"})
	if err == nil {
		t.Fatal("EnsureTopic returned nil while one of the two nsqd instances refused it")
	}
	if got := mq.CodeOf(err); got != mq.CodeInvalidArgument {
		t.Errorf("a 400 from nsqd is code %v, want CodeInvalidArgument", got)
	}
	// The healthy instance is still asked: the call is a fan-out and not a
	// sequence that stops at the first error, or a cluster would be left
	// further out of step by every failure.
	if !good.sawPath("/topic/create") {
		t.Error("EnsureTopic stopped at the failing nsqd and never reached the healthy one; a fan-out that " +
			"stops at the first error leaves the cluster further out of step than it found it")
	}
}

// TestEveryNSQDStatusMapsOntoACode pins the status table, which is the part
// of the driver a caller writes a retry loop against.
//
// The body is deliberately not in the error: nsqd echoes request parameters
// into several of its messages and a proxied deployment can put rather more
// than that in one.
func TestEveryNSQDStatusMapsOntoACode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status int
		want   mq.ErrorCode
	}{
		{http.StatusNotFound, mq.CodeNotFound},
		{http.StatusBadRequest, mq.CodeInvalidArgument},
		{http.StatusUnauthorized, mq.CodeUnauthorized},
		{http.StatusForbidden, mq.CodeForbidden},
		{http.StatusServiceUnavailable, mq.CodeUnavailable},
		{http.StatusBadGateway, mq.CodeUnavailable},
		{http.StatusGatewayTimeout, mq.CodeUnavailable},
		{http.StatusTeapot, mq.CodeUnknown},
		{http.StatusInternalServerError, mq.CodeUnknown},
	} {
		d := newNSQD(t)
		d.fail("/topic/create", tc.status)
		admin := adminOver(t, d.addr())
		err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"})
		if got := mq.CodeOf(err); got != tc.want {
			t.Errorf("HTTP %d from nsqd is code %v, want %v", tc.status, got, tc.want)
		}
	}
}

// TestStatsSumsEveryNSQDAndNeverClaimsALag is Stats over a cluster.
//
// Depth is summed because a channel's messages are spread over the instances
// that hold them, and it includes the DEFERRED count because a deferred
// message is waiting, not gone -- a caller draining a queue before a shutdown
// that stopped at depth zero would leave them.
//
// Lag is never known: a channel holds what has not been finished and forgets
// the rest, so there is no position to be behind. The specification makes that Known=false
// rather than a zero that reads as "caught up".
func TestStatsSumsEveryNSQDAndNeverClaimsALag(t *testing.T) {
	t.Parallel()
	a, b := newNSQD(t), newNSQD(t)
	a.setStats(statsBody("orders", "workers", 3, 1, 2))
	b.setStats(statsBody("orders", "workers", 4, 5, 0))
	admin := adminOver(t, a.addr(), b.addr())

	stats, err := admin.Stats(t.Context(), "orders", "workers")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	// 3+2 deferred on the first, 4+0 on the second.
	if !stats.Depth.Known || stats.Depth.Value != 9 {
		t.Errorf("Stats reports depth %d (known=%t) over two nsqd instances holding 3+2 and 4+0, want 9: "+
			"a deferred message is waiting, not gone", stats.Depth.Value, stats.Depth.Known)
	}
	if !stats.InFlight.Known || stats.InFlight.Value != 6 {
		t.Errorf("Stats reports %d in flight over two instances holding 1 and 5, want 6", stats.InFlight.Value)
	}
	if stats.Lag.Known {
		t.Errorf("Stats claims to know the lag (%d); a channel has no position in a log to be behind, so "+
			"requires Known=false rather than a zero that reads as caught up", stats.Lag.Value)
	}
}

// TestStatsOnAChannelNoNSQDHasIsNotFound is the difference between an empty
// channel and one that does not exist.
//
// Both would sum to zero. Answering QueueStats{} for the second would tell a
// caller the subscription is empty and up to date, when in fact nothing is
// reading their messages.
func TestStatsOnAChannelNoNSQDHasIsNotFound(t *testing.T) {
	t.Parallel()
	d := newNSQD(t) // answers the empty topic list.
	admin := adminOver(t, d.addr())

	_, err := admin.Stats(t.Context(), "orders", "workers")
	if got := mq.CodeOf(err); got != mq.CodeNotFound {
		t.Errorf("Stats for a channel no nsqd reports is %v (code %v), want CodeNotFound: summing to zero would "+
			"tell a caller their subscription is empty and up to date", err, got)
	}
}

// TestListTopicsMergesTheInstancesAndStripsThePrefix.
//
// Two nsqd instances holding the same topic list it once, and a topic that
// does not carry the configured prefix is not this client's to report at all.
func TestListTopicsMergesTheInstancesAndStripsThePrefix(t *testing.T) {
	t.Parallel()
	a, b := newNSQD(t), newNSQD(t)
	a.setStats(`{"version":"1.2.1","topics":[{"topic_name":"app_orders"},{"topic_name":"other_thing"}]}`)
	b.setStats(`{"version":"1.2.1","topics":[{"topic_name":"app_orders"},{"topic_name":"app_payments"}]}`)

	backend, _ := newBackendRig(t, func(c *nq.Config) {
		c.AdminAddrs = []string{a.addr(), b.addr()}
		c.TopicPrefix = "app_"
	})
	admin, err := backend.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}

	var got []string
	for topic, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics yielded an error: %v", err)
		}
		got = append(got, topic)
	}
	if len(got) != 2 || got[0] != "orders" || got[1] != "payments" {
		t.Errorf("ListTopics yielded %v, want [orders payments]: the same topic on two instances is listed once, "+
			"and a topic without the configured prefix is not this client's to report", got)
	}

	// A caller that stops early stops the iteration, including across the
	// instance boundary -- the second nsqd must not be asked at all.
	n := 0
	for range admin.ListTopics(t.Context()) {
		n++
		break
	}
	if n != 1 {
		t.Errorf("a caller that broke out of ListTopics saw %d topics, want 1", n)
	}
}

// TestListTopicsReportsAFailureRatherThanEndingQuietly.
//
// An iterator that returned on an error without yielding it would read as
// "this deployment has no topics", which is the same thing a working call
// against an empty deployment says.
func TestListTopicsReportsAFailureRatherThanEndingQuietly(t *testing.T) {
	t.Parallel()
	d := newNSQD(t)
	d.fail("/stats", http.StatusServiceUnavailable)
	admin := adminOver(t, d.addr())

	var yielded int
	var last error
	for topic, err := range admin.ListTopics(t.Context()) {
		yielded++
		last = err
		_ = topic
	}
	if last == nil {
		t.Fatalf("ListTopics over an unavailable nsqd yielded %d topics and no error; an empty iteration reads "+
			"as \"this deployment has no topics\"", yielded)
	}
	if got := mq.CodeOf(last); got != mq.CodeUnavailable {
		t.Errorf("a 503 from nsqd's /stats is code %v, want CodeUnavailable", got)
	}
}

// TestStatsThatCannotBeParsedIsNotAnEmptyQueue.
//
// A JSON body this driver cannot read is an unknown, and the failure mode
// worth refusing is the one where `json.Unmarshal` leaves the struct zeroed
// and the caller is told the channel is empty.
func TestStatsThatCannotBeParsedIsNotAnEmptyQueue(t *testing.T) {
	t.Parallel()
	d := newNSQD(t)
	d.setStats(`{"topics": "this is not a list"}`)
	admin := adminOver(t, d.addr())

	_, err := admin.Stats(t.Context(), "orders", "workers")
	if err == nil {
		t.Fatal("Stats over an unparseable /stats returned nil; an unreadable answer is not an empty channel")
	}
	if !strings.Contains(err.Error(), "could not be parsed") {
		t.Errorf("Stats over an unparseable /stats failed with %v, want the parse refusal", err)
	}
}

// TestTheAdminRequestCarriesItsHeaders.
//
// The version header is what makes nsqd answer the 1.0 JSON shape rather than
// its older one, and the secret goes in a HEADER and never in the URL --
// which is what lets the error mapper's redaction be enough for a transport
// failure, because net/http puts the URL in its error and not the headers.
func TestTheAdminRequestCarriesItsHeaders(t *testing.T) {
	t.Parallel()
	d := newNSQD(t)
	b, _ := newBackendRig(t, func(c *nq.Config) {
		c.AdminAddrs = []string{d.addr()}
		c.AuthSecret = dsn.Secret("s3cr3t")
	})
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.headers) == 0 {
		t.Fatal("no request reached the fake nsqd")
	}
	h := d.headers[0]
	if got := h.Get("Accept"); !strings.Contains(got, "application/vnd.nsq") {
		t.Errorf("the request's Accept is %q, want nsqd's versioned media type", got)
	}
	if got := h.Get("Authorization"); got != "Bearer s3cr3t" {
		t.Errorf("the request's Authorization is %q, want the bearer secret", got)
	}
	for _, r := range d.requests {
		if strings.Contains(r, "s3cr3t") {
			t.Errorf("the secret reached the URL (%q); it goes in a header so that net/http's error, which "+
				"carries the URL, cannot leak it", r)
		}
	}
}

// TestAdminCallsRespectACancelledContext.
func TestAdminCallsRespectACancelledContext(t *testing.T) {
	t.Parallel()
	d := newNSQD(t)
	admin := adminOver(t, d.addr())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err == nil {
		t.Error("EnsureTopic with a cancelled context returned nil")
	}
	if _, err := admin.Stats(ctx, "orders", "workers"); err == nil {
		t.Error("Stats with a cancelled context returned nil")
	}
}

// TestTheHTTPAddressIsDerivedFromTheTCPOneWhenItIsNotGiven pins nsqd's own
// default layout: 4150 for the protocol and 4151 for HTTP.
//
// The fake nsqd is addressed through the DERIVED address rather than through
// AdminAddrs, so the test fails if the derivation is wrong in either
// direction: the request has to arrive at the port one above the one
// configured.
func TestTheHTTPAddressIsDerivedFromTheTCPOneWhenItIsNotGiven(t *testing.T) {
	t.Parallel()
	d := newNSQD(t)
	host, port, err := net.SplitHostPort(d.addr())
	if err != nil {
		t.Fatalf("splitting the fake nsqd's address %q: %v", d.addr(), err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("the fake nsqd's port %q is not a number: %v", port, err)
	}
	// One BELOW the fake's port, so that the derivation has to add one to
	// land on it.
	b, _ := newBackendRig(t, func(c *nq.Config) {
		c.Addrs = []string{net.JoinHostPort(host, strconv.Itoa(n-1))}
		c.AdminAddrs = nil
	})
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin() over a derivable address: %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic over the derived address: %v", err)
	}
	if !d.sawPath("/topic/create") {
		t.Errorf("the request never reached the nsqd one port above %d; the derivation is nsqd's own layout "+
			"(4150 protocol, 4151 HTTP) and a wrong one fails at request time with a connection error", n-1)
	}
}

// TestAnAddressTheHTTPPortCannotBeDerivedFromContributesNothing.
//
// The derivation is a GUESS, and the failure it must not have is a silent
// one: an address with no port, or a port that is not a number, must drop out
// rather than produce a nonsense address that fails later with a confusing
// connection error naming a host nobody configured.
func TestAnAddressTheHTTPPortCannotBeDerivedFromContributesNothing(t *testing.T) {
	t.Parallel()
	b, _ := newBackendRig(t, func(c *nq.Config) {
		c.Addrs = []string{"nsqd-without-a-port", "nsqd:not-a-number"}
		c.AdminAddrs = nil
	})
	_, err := b.Admin()
	if err == nil {
		t.Fatal("Admin() returned a surface over two addresses no HTTP port can be derived from; the calls " +
			"would fail later naming a host nobody configured")
	}
	if got := mq.CodeOf(err); got != mq.CodeUnsupported {
		t.Errorf("Admin() with no derivable address is code %v, want CodeUnsupported", got)
	}
}
