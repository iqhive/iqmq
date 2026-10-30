package sqs_test

import (
	"errors"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
	"github.com/iqhive/iqmq/internal/errcode"
)

// TestAFIFOBatchTheCoreSplitsStopsAGroupAcrossItsPieces is a FIFO batch of
// twelve, two more than the driver's MaxBatchMessages, so the core hands it
// to the driver in two pieces. m1 of group "b" is refused in the first
// SendMessageBatch call, and the driver holds group b back for the rest of
// that piece. The core sent m11 of group b in the second piece anyway, so it
// was on the queue ahead of the caller's retry of m1.
// TestAFIFOBatchStopsAGroupAtItsFirstFailure has ten messages and never
// reaches the split.
//
// The hold is the root module's, and CI tests this module against the
// published root module, which has it only from keyHoldRelease on. So the test
// skips against an earlier release, and only there: it reads the version the
// binary linked, and runs against the workspace's tree and every release from
// keyHoldRelease. It used to ask the linked root module whether it held the
// key, and skipped whenever it did not, so it skipped exactly when the defect
// it is about was present.
func TestAFIFOBatchTheCoreSplitsStopsAGroupAcrossItsPieces(t *testing.T) {
	if v, ok := linkedRootRelease(t); ok && releaseBefore(v, keyHoldRelease) {
		t.Skipf("this binary linked the root module %s, which predates the hold %s added; the version bump "+
			"that requires %s turns the test on", v, keyHoldRelease, keyHoldRelease)
	}
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) {
		c.FIFO = true
		c.MaxMessageBytes = 1024
	})
	name := topic(t, "fifo-split")
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	// Three 300-byte messages fit under the 1024-byte limit, so the first
	// piece goes in four calls, 0-2, 3-5, 6-8 and 9, and m1's refusal is in
	// the first of them.
	const n = 12
	ms := make([]*mq.Message, n)
	group := func(i int) string { return map[bool]string{true: "a", false: "b"}[i%2 == 0] }
	pad := strings.Repeat("x", 300)
	for i := range ms {
		ms[i] = &mq.Message{Key: []byte(group(i)), Body: []byte("m" + strconv.Itoa(i) + pad)}
	}
	server.Fault(sqsfake.Fault{
		Op:     "SendMessageBatch",
		Bodies: map[string]string{string(ms[1].Body): "InternalError"},
		Count:  -1,
	})
	err = pub.PublishBatch(t.Context(), ms)
	var be *mq.BatchError
	if !errors.As(err, &be) || len(be.Errs) != n {
		t.Fatalf("PublishBatch = %v, want a *BatchError with %d outcomes", err, n)
	}
	on := map[string]bool{}
	for _, body := range h.bodies(name) {
		on[body] = true
	}
	for i := 3; i < n; i++ {
		sent := on[string(ms[i].Body)]
		if group(i) == "a" {
			if be.Errs[i] != nil || !sent {
				t.Errorf("m%d of group a, which had no failure, reported %v (on the queue: %v)", i, be.Errs[i], sent)
			}
			continue
		}
		if sent {
			t.Errorf("m%d of group b was sent after m1 of its group was refused; the caller's retry of m1 lands behind it", i)
		}
		if !errors.Is(be.Errs[i], errcode.ErrNotAttempted) || !mq.IsRetryable(be.Errs[i]) {
			t.Errorf("m%d of group b reported %v, want a retryable not-attempted answer", i, be.Errs[i])
		}
	}
}

// keyHoldRelease is the first release of the root module whose publisher holds
// a key back from the later pieces of a batch it splits, once a message of the
// key has failed.
const keyHoldRelease = "v1.2.0"

// linkedRootRelease is the version of the root module this test binary linked,
// and false when it is not a published one: the workspace's tree, or a
// replacement.
func linkedRootRelease(t *testing.T) (string, bool) {
	t.Helper()
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("no build information in this test binary, so the linked root module cannot be read")
	}
	for _, d := range bi.Deps {
		if d.Path != "github.com/iqhive/iqmq" {
			continue
		}
		if d.Replace != nil || !strings.HasPrefix(d.Version, "v") {
			return "", false
		}
		return d.Version, true
	}
	t.Fatalf("the root module is not among the %d modules this binary linked", len(bi.Deps))
	return "", false
}

// releaseBefore reports whether version v comes before release r, by their
// major, minor and patch numbers alone. A pre-release or a pseudo-version
// counts as its numbers: one made on the way to r counts as r, and one made
// after an earlier release as before r. A version it cannot read is not before
// anything, so the test runs rather than skip on a guess.
func releaseBefore(v, r string) bool {
	a, okA := releaseNumbers(v)
	b, okB := releaseNumbers(r)
	if !okA || !okB {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// releaseNumbers is the major, minor and patch numbers of a module version.
func releaseNumbers(v string) ([3]int, bool) {
	var n [3]int
	core, _, _ := strings.Cut(strings.TrimPrefix(v, "v"), "-")
	core, _, _ = strings.Cut(core, "+")
	parts := strings.Split(core, ".")
	if len(parts) != len(n) {
		return n, false
	}
	for i, p := range parts {
		x, err := strconv.Atoi(p)
		if err != nil {
			return n, false
		}
		n[i] = x
	}
	return n, true
}
