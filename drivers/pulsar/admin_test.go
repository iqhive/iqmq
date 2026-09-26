package pulsar_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
)

// The Admin surface, driven against a fake Pulsar administrative endpoint.
//
// Pulsar's topology API is REST over the web service port, so all of it is
// reachable from `httptest` and only the binary protocol is not. It was 21.9 %
// covered before these, and the first thing a test of it found was a
// DeleteTopic that deleted nothing -- see
// TestDeleteTopicRemovesANonPartitionedTopic.

// pulsarAdmin is a fake Pulsar web service. It records every request and
// answers each path from a table the test fills in.
type pulsarAdmin struct {
	srv *httptest.Server

	mu       sync.Mutex
	requests []string
	bodies   []string
	headers  []http.Header
	// answer maps a path SUFFIX to the status and body to answer with. The
	// first matching suffix wins, so a test can override one endpoint and
	// leave the rest at the default.
	answer []rule
}

type rule struct {
	suffix string
	method string // empty matches any
	status int
	body   string
}

func newPulsarAdmin(t *testing.T) *pulsarAdmin {
	t.Helper()
	p := &pulsarAdmin{}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		if r.Body != nil {
			body, _ = io.ReadAll(r.Body)
		}
		p.mu.Lock()
		p.requests = append(p.requests, r.Method+" "+r.URL.RequestURI())
		p.bodies = append(p.bodies, string(body))
		p.headers = append(p.headers, r.Header.Clone())
		status, out := http.StatusNoContent, ""
		for _, rl := range p.answer {
			if rl.method != "" && rl.method != r.Method {
				continue
			}
			if strings.Contains(r.URL.Path, rl.suffix) {
				status, out = rl.status, rl.body
				break
			}
		}
		p.mu.Unlock()
		if out != "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(status)
		if out != "" {
			_, _ = w.Write([]byte(out))
		}
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// on makes a path suffix answer a status and body.
func (p *pulsarAdmin) on(method, suffix string, status int, body string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.answer = append(p.answer, rule{suffix: suffix, method: method, status: status, body: body})
}

// seen is every request, as "METHOD path?query".
func (p *pulsarAdmin) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.requests...)
}

// adminSeen is `seen` without the broker's own runtime-configuration probe,
// which every backend makes at open time and no test here is about.
func (p *pulsarAdmin) adminSeen() []string {
	out := make([]string, 0, len(p.seen()))
	for _, r := range p.seen() {
		if strings.Contains(r, "/brokers/configuration") {
			continue
		}
		out = append(out, r)
	}
	return out
}

// newAdmin opens an admin surface against the fake web service.
func newAdmin(t *testing.T, mutate func(*pd.Config)) (*pulsarAdmin, mq.Admin) {
	t.Helper()
	p := newPulsarAdmin(t)
	b, _ := newRig(t, func(c *pd.Config) {
		c.AdminURL = p.srv.URL
		if mutate != nil {
			mutate(c)
		}
	})
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	return p, admin
}

// TestDeleteTopicRemovesANonPartitionedTopic is the regression for a
// DeleteTopic that reported success and deleted nothing.
//
// The delete goes to the partitioned endpoint first, because deleting a
// partitioned topic through the other one answers 409 and leaves the
// partitions behind. That call used to ACCEPT a 404 and a 409 -- and a 404 is
// exactly what Pulsar answers for an ordinary non-partitioned topic, the kind
// Partitions=0 creates and the only kind pulsar-client-go honours a Seek on.
// So the common case took the accept, returned nil, and never issued the
// delete that would have removed anything; the fallback below it was
// unreachable in precisely the case it was written for.
//
// `make integration` did not catch it because a DeleteTopic followed by an
// idempotent EnsureTopic leaves no trace of having done nothing.
func TestDeleteTopicRemovesANonPartitionedTopic(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil)
	// Pulsar's answer for a topic that has no partitioned metadata.
	p.on(http.MethodDelete, "/partitions", http.StatusNotFound, "")

	if err := admin.DeleteTopic(t.Context(), "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}

	seen := p.adminSeen()
	var deletedTheTopic bool
	for _, r := range seen {
		if strings.HasPrefix(r, "DELETE ") && !strings.Contains(r, "/partitions") {
			deletedTheTopic = true
		}
	}
	if !deletedTheTopic {
		t.Errorf("DeleteTopic returned nil having sent only %v; a 404 from the partitioned endpoint means the "+
			"topic has no partitioned metadata, not that it is gone, so the non-partitioned delete still has "+
			"to be issued or the topic stays on the broker", seen)
	}
}

// TestDeleteTopicOnAPartitionedTopicStopsAtTheFirstCall is the other side of
// the same decision: when the partitioned delete SUCCEEDS there is nothing
// left to delete, and issuing the second call would answer 404 on a topic
// this driver just removed.
func TestDeleteTopicOnAPartitionedTopicStopsAtTheFirstCall(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil) // every path answers 204.

	if err := admin.DeleteTopic(t.Context(), "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if got := p.adminSeen(); len(got) != 1 || !strings.Contains(got[0], "/partitions") {
		t.Errorf("DeleteTopic on a partitioned topic sent %v, want the partitioned delete alone", got)
	}
}

// TestDeleteTopicIsStillIdempotent: a topic that is not there at all answers
// 404 from both endpoints, and a delete of something already gone is success.
func TestDeleteTopicIsStillIdempotent(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil)
	p.on(http.MethodDelete, "/admin/v2/", http.StatusNotFound, "")

	if err := admin.DeleteTopic(t.Context(), "orders"); err != nil {
		t.Errorf("DeleteTopic for a topic the broker does not have is %v, want nil: the specification makes a delete "+
			"idempotent and the topic is, either way, gone", err)
	}
}

// TestEnsureTopicCreatesThePartitionedFormOnlyWhenAskedTo.
//
// Partitions=0 creates a non-partitioned topic, and it is the default because
// it is the only kind pulsar-client-go honours a Seek on. A positive count
// creates the partitioned form, and the COUNT is the request's body.
func TestEnsureTopicCreatesThePartitionedFormOnlyWhenAskedTo(t *testing.T) {
	t.Parallel()

	plain, admin := newAdmin(t, nil)
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	for _, r := range plain.adminSeen() {
		if strings.Contains(r, "/partitions") {
			t.Errorf("EnsureTopic with Partitions=0 sent %q; the non-partitioned form is the default and the "+
				"only one a Seek works on", r)
		}
	}

	part, partAdmin := newAdmin(t, func(c *pd.Config) { c.Partitions = 4 })
	if err := partAdmin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic with partitions: %v", err)
	}
	seen := part.adminSeen()
	if len(seen) == 0 || !strings.Contains(seen[0], "/partitions") {
		t.Fatalf("EnsureTopic with Partitions=4 sent %v, want the partitioned endpoint", seen)
	}
	part.mu.Lock()
	defer part.mu.Unlock()
	var body string
	for i, r := range part.requests {
		if strings.Contains(r, "/partitions") {
			body = part.bodies[i]
			break
		}
	}
	if body != "4" {
		t.Errorf("EnsureTopic with Partitions=4 sent the body %q, want \"4\"", body)
	}
}

// TestEnsureTopicTreatsAConflictAsSuccess is the idempotence: a 409 means
// the topic is already there, which is what the caller asked for.
//
// A topic that exists is left ALONE, partition count included: raising a
// partition count rehashes every key and "idempotent" does not stretch to
// that.
func TestEnsureTopicTreatsAConflictAsSuccess(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil)
	p.on("", "/admin/v2/", http.StatusConflict, "")

	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Errorf("EnsureTopic for a topic that already exists is %v, want nil", err)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: "orders", Name: "workers"}); err != nil {
		t.Errorf("EnsureSubscription for a subscription that already exists is %v, want nil", err)
	}
}

// TestEnsureSubscriptionStartsAtTheEndOfTheTopic is default,
// and the same answer mq.StartPosition's zero value resolves to.
//
// Pulsar takes a message identifier as the cursor's starting position and
// (-1, -1) is the latest. A subscription created by PROVISIONING that started
// at the beginning would replay a topic's entire history to whoever opened it
// next, by omission rather than by anyone's choice.
func TestEnsureSubscriptionStartsAtTheEndOfTheTopic(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil)
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: "orders", Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	var body, path string
	for i, r := range p.requests {
		if strings.Contains(r, "/subscription/") {
			body, path = p.bodies[i], r
			break
		}
	}
	if path == "" {
		t.Fatalf("EnsureSubscription sent %v, want a request to the subscription endpoint", p.requests)
	}
	if !strings.Contains(body, `"ledgerId":-1`) || !strings.Contains(body, `"entryId":-1`) {
		t.Errorf("EnsureSubscription created the cursor at %q, want the (-1,-1) latest position: a "+
			"subscription created by provisioning must not replay a topic's history by omission", body)
	}
}

// TestStatsReadsTheNamedSubscriptionAndNothingElse.
//
// Pulsar reports every subscription of a topic in one document, so the driver
// has to pick the one asked for -- and a topic with no subscription of that
// name reports nothing at all rather than an empty one, which is why the
// figures are Limits.
func TestStatsReadsTheNamedSubscriptionAndNothingElse(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil)
	p.on(http.MethodGet, "/stats", http.StatusOK, `{"subscriptions":{
		"workers":{"msgBacklog":7,"unackedMessages":2,"msgDelayed":1,"backlogSize":900},
		"audit":{"msgBacklog":99,"unackedMessages":99,"msgDelayed":0,"backlogSize":0}}}`)

	stats, err := admin.Stats(t.Context(), "orders", "workers")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Depth.Known || stats.Depth.Value != 7 {
		t.Errorf("Stats reports depth %d (known=%t), want 7 from the named subscription and not the other "+
			"one's 99", stats.Depth.Value, stats.Depth.Known)
	}
	if !stats.InFlight.Known || stats.InFlight.Value != 2 {
		t.Errorf("Stats reports %d in flight, want 2", stats.InFlight.Value)
	}
	if !stats.Lag.Known || stats.Lag.Value != 7 {
		t.Errorf("Stats reports lag %d (known=%t), want the backlog: Pulsar's backlog IS the distance from "+
			"the cursor to the end", stats.Lag.Value, stats.Lag.Known)
	}
}

// TestStatsForASubscriptionTheTopicDoesNotHaveKnowsNothing.
//
// The topic exists and the subscription does not. Every figure has to be
// UNKNOWN rather than zero: zero would say the subscription is empty and
// caught up, when what is true is that nothing is reading at all.
func TestStatsForASubscriptionTheTopicDoesNotHaveKnowsNothing(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil)
	p.on(http.MethodGet, "/stats", http.StatusOK, `{"subscriptions":{"audit":{"msgBacklog":3}}}`)

	stats, err := admin.Stats(t.Context(), "orders", "workers")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Depth.Known || stats.InFlight.Known || stats.Lag.Known {
		t.Errorf("Stats for a subscription the topic does not have claims to know depth=%t inflight=%t lag=%t; "+
			"zero would say it is empty and caught up when nothing is reading it at all",
			stats.Depth.Known, stats.InFlight.Known, stats.Lag.Known)
	}
}

// TestEveryAdministrativeStatusMapsOntoACode, including which of them a
// caller may retry.
func TestEveryAdministrativeStatusMapsOntoACode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status    int
		want      mq.ErrorCode
		retryable bool
	}{
		{http.StatusUnauthorized, mq.CodeUnauthorized, false},
		{http.StatusForbidden, mq.CodeForbidden, false},
		{http.StatusPreconditionFailed, mq.CodeInvalidArgument, false},
		{http.StatusBadRequest, mq.CodeInvalidArgument, false},
		{http.StatusTooManyRequests, mq.CodeThrottled, true},
		{http.StatusServiceUnavailable, mq.CodeUnavailable, true},
		{http.StatusBadGateway, mq.CodeUnavailable, true},
		{http.StatusGatewayTimeout, mq.CodeUnavailable, true},
		{http.StatusNotImplemented, mq.CodeUnsupported, false},
		{http.StatusInternalServerError, mq.CodeUnknown, false},
	} {
		p, admin := newAdmin(t, nil)
		p.on("", "/admin/v2/", tc.status, "")
		err := admin.Purge(t.Context(), "orders", "workers")
		if got := mq.CodeOf(err); got != tc.want {
			t.Errorf("HTTP %d from the administrative API is code %v, want %v", tc.status, got, tc.want)
		}
		if got := mq.IsRetryable(err); got != tc.retryable {
			t.Errorf("HTTP %d from the administrative API is retryable=%t, want %t", tc.status, got, tc.retryable)
		}
	}
}

// TestAnAdministrativeErrorCarriesNoBody is.
//
// Pulsar echoes the request into some of its failures. A caller acts on the
// status code, and the body of a proxied or authenticated deployment can
// carry rather more than a topic name.
func TestAnAdministrativeErrorCarriesNoBody(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, func(c *pd.Config) { c.Token = "s3cr3t-token" })
	p.on("", "/admin/v2/", http.StatusForbidden, `{"reason":"auth failed for token s3cr3t-token at broker-7.internal"}`)

	err := admin.Purge(t.Context(), "orders", "workers")
	if err == nil {
		t.Fatal("Purge against a 403 returned nil")
	}
	if strings.Contains(err.Error(), "s3cr3t-token") || strings.Contains(err.Error(), "broker-7.internal") {
		t.Errorf("the administrative error repeats the broker's body: %v", err)
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("the administrative error is %v, want the status code a caller acts on", err)
	}
}

// TestTheTokenTravelsInAHeader.
func TestTheTokenTravelsInAHeader(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, func(c *pd.Config) { c.Token = "s3cr3t-token" })
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	var found bool
	for i, h := range p.headers {
		if strings.Contains(p.requests[i], "/brokers/configuration") {
			continue
		}
		if h.Get("Authorization") == "Bearer s3cr3t-token" {
			found = true
		}
		if strings.Contains(p.requests[i], "s3cr3t-token") {
			t.Errorf("the token reached the URL (%q); net/http puts the URL in its transport errors and not "+
				"the headers, which is what makes the mapper's redaction sufficient", p.requests[i])
		}
	}
	if !found {
		t.Error("no administrative request carried the bearer token")
	}
}

// TestListTopicsAsksBothFormsAndStripsThePrefix.
//
// A namespace holds non-partitioned and partitioned topics under two separate
// endpoints, and a caller asking "what is here" means both. The configured
// prefix is removed because the portable name is what the caller passed in.
func TestListTopicsAsksBothFormsAndStripsThePrefix(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, func(c *pd.Config) { c.TopicPrefix = "app-" })
	p.on(http.MethodGet, "/partitioned", http.StatusOK,
		`["persistent://public/default/app-payments"]`)
	p.on(http.MethodGet, "/admin/v2/persistent/public/default", http.StatusOK,
		`["persistent://public/default/app-orders"]`)

	var got []string
	for topic, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics yielded an error: %v", err)
		}
		got = append(got, topic)
	}
	if len(got) != 2 || got[0] != "orders" || got[1] != "payments" {
		t.Errorf("ListTopics yielded %v, want [orders payments]: both the plain and the partitioned listing, "+
			"with the configured prefix removed", got)
	}

	// A caller that stops gets to stop, across the endpoint boundary too.
	n := 0
	for range admin.ListTopics(t.Context()) {
		n++
		break
	}
	if n != 1 {
		t.Errorf("a caller that broke out of ListTopics saw %d topics, want 1", n)
	}
}

// TestListTopicsReportsANameItCannotParseRatherThanHidingIt.
//
// A topic the broker holds and this driver's grammar refuses is worth knowing
// about: it is addressable by something, just not by this client, and
// dropping it silently makes an inventory lie.
func TestListTopicsReportsANameItCannotParseRatherThanHidingIt(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil)
	// The partitioned rule is registered FIRST because the first matching
	// suffix wins and the plain path is a prefix of the partitioned one.
	p.on(http.MethodGet, "/partitioned", http.StatusOK, `[]`)
	p.on(http.MethodGet, "/admin/v2/persistent/public/default", http.StatusOK,
		`["not-a-pulsar-url","persistent://public/default/orders"]`)

	var got []string
	for topic, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics yielded an error: %v", err)
		}
		got = append(got, topic)
	}
	if len(got) != 2 {
		t.Fatalf("ListTopics yielded %v, want both names including the unparseable one", got)
	}
	if got[0] != "not-a-pulsar-url" {
		t.Errorf("ListTopics rendered the unparseable name as %q, want it reported as the broker gave it", got[0])
	}
}

// TestListTopicsReportsAFailureRatherThanEndingQuietly.
func TestListTopicsReportsAFailureRatherThanEndingQuietly(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, nil)
	p.on(http.MethodGet, "/admin/v2/persistent", http.StatusServiceUnavailable, "")

	var last error
	var n int
	for _, err := range admin.ListTopics(t.Context()) {
		n++
		last = err
	}
	if last == nil {
		t.Fatalf("ListTopics over an unavailable broker yielded %d topics and no error; an empty iteration "+
			"reads as \"this namespace is empty\"", n)
	}
	if got := mq.CodeOf(last); got != mq.CodeUnavailable {
		t.Errorf("a 503 from the topic listing is code %v, want CodeUnavailable", got)
	}
}

// TestTheNonPersistentDomainChangesEveryPath.
//
// A non-persistent topic lives under a different administrative prefix, and a
// driver configured for one that addressed the other would create topics
// nobody reads from.
func TestTheNonPersistentDomainChangesEveryPath(t *testing.T) {
	t.Parallel()
	p, admin := newAdmin(t, func(c *pd.Config) { c.Domain = pd.DomainNonPersistent })
	// Both listings answer an empty array, so the iterator reaches the
	// second endpoint instead of stopping at a decode failure on the first.
	p.on(http.MethodGet, "/admin/v2/", http.StatusOK, `[]`)
	for range admin.ListTopics(t.Context()) {
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}

	for _, r := range p.adminSeen() {
		if !strings.Contains(r, "/non-persistent/") {
			t.Errorf("a non-persistent client asked %q; every administrative path carries the domain", r)
		}
	}
}

// TestAdminCallsRespectACancelledContext.
func TestAdminCallsRespectACancelledContext(t *testing.T) {
	t.Parallel()
	_, admin := newAdmin(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err == nil {
		t.Error("EnsureTopic with a cancelled context returned nil")
	}
	if _, err := admin.Stats(ctx, "orders", "workers"); err == nil {
		t.Error("Stats with a cancelled context returned nil")
	}
}

// TestSetNamespaceRetentionIsRefusedWithoutAnAdminURL.
//
// Retention is what decides whether this driver's FeatureReplay claim holds,
// so a conformance run that means to verify the claim has to set one -- and a
// deployment with no administrative URL has to be told it cannot, rather than
// having the call quietly do nothing.
func TestSetNamespaceRetentionIsRefusedWithoutAnAdminURL(t *testing.T) {
	t.Parallel()
	b, _ := newRig(t, func(c *pd.Config) { c.AdminURL = "" })
	err := b.SetNamespaceRetention(t.Context(), 60, 100)
	if err == nil {
		t.Fatal("SetNamespaceRetention with no AdminURL returned nil; nothing was configured")
	}
	if got := mq.CodeOf(err); got != mq.CodeUnsupported {
		t.Errorf("SetNamespaceRetention with no AdminURL is code %v, want CodeUnsupported", got)
	}
}

// TestSetNamespaceRetentionSendsWhatPulsarCallsUnbounded.
//
// Minutes or megabytes of -1 are Pulsar's own word for unbounded, and the
// default is ZERO -- under which an entry every subscription has acknowledged
// may be deleted at once, which is a topic that cannot be replayed.
func TestSetNamespaceRetentionSendsWhatPulsarCallsUnbounded(t *testing.T) {
	t.Parallel()
	p := newPulsarAdmin(t)
	b, _ := newRig(t, func(c *pd.Config) { c.AdminURL = p.srv.URL })
	if err := b.SetNamespaceRetention(t.Context(), -1, -1); err != nil {
		t.Fatalf("SetNamespaceRetention: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	var body, path string
	for i, r := range p.requests {
		if strings.Contains(r, "/retention") {
			body, path = p.bodies[i], r
			break
		}
	}
	if path == "" {
		t.Fatalf("SetNamespaceRetention sent %v, want a request to the retention endpoint", p.requests)
	}
	if !strings.Contains(body, `"retentionTimeInMinutes":-1`) || !strings.Contains(body, `"retentionSizeInMB":-1`) {
		t.Errorf("SetNamespaceRetention sent %q, want both figures as -1", body)
	}
}
