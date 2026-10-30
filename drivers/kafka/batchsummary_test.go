package kafka_test

import (
	"errors"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestBatchSummaryKeepsAMixedOutcome is the rule for a batch none of whose
// records was accepted. Kafka answers per partition, so one partition can
// refuse definitely (MESSAGE_TOO_LARGE) while another times out after the
// append may have happened (REQUEST_TIMED_OUT, ambiguous). BatchSummary
// returned the first record's error whatever the others said, so such a batch
// read as a definite refusal and a caller who republished it duplicated the
// records that may be in the log. It now answers through
// driverkit.CollapseBatch: the single error only when every record has the
// same outcome, which for a refusal means one code and one retryability.
func TestBatchSummaryKeepsAMixedOutcome(t *testing.T) {
	mixed := kq.BatchOutcome([]error{kerr.MessageTooLarge, kerr.RequestTimedOut, kerr.RequestTimedOut})
	if mq.IsAmbiguous(mixed[0]) || !mq.IsAmbiguous(mixed[1]) {
		t.Fatalf("the instrument is broken: %v should be definite and %v ambiguous", mixed[0], mixed[1])
	}
	got := kq.BatchSummary(mixed)
	var be *mq.BatchError
	if !errors.As(got, &be) || len(be.Errs) != 3 {
		t.Fatalf("BatchSummary(refused, ambiguous, ambiguous) = %v, want the index-aligned *BatchError", got)
	}
	if !mq.IsAmbiguous(got) || mq.IsAmbiguous(be.Errs[0]) || !mq.IsAmbiguous(be.Errs[2]) {
		t.Fatalf("BatchSummary(refused, ambiguous, ambiguous) = %v: the per-record outcomes were lost", got)
	}
	for name, errs := range map[string][]error{
		"every record refused alike": kq.BatchOutcome([]error{kerr.MessageTooLarge, kerr.MessageTooLarge}),
		"every record ambiguous":     kq.BatchOutcome([]error{kerr.RequestTimedOut, kerr.RequestTimedOut}),
	} {
		got := kq.BatchSummary(errs)
		if errors.As(got, &be) || got != errs[0] { //nolint:errorlint // identity: the first record's own error
			t.Errorf("%s: BatchSummary = %v, want the first record's own error: every record has one outcome", name, got)
		}
	}

	// Every record refused, for different reasons: MESSAGE_TOO_LARGE and
	// INVALID_RECORD carry different codes, and one error cannot name both.
	// driverkit.CollapseBatch keeps them apart from root v1.2.0 on.
	if v, old := rootModuleBefore(t, "v1.2.0"); old {
		t.Logf("this build uses root module %s, whose CollapseBatch compares ambiguity alone", v)
		return
	}
	differ := kq.BatchOutcome([]error{kerr.MessageTooLarge, kerr.InvalidRecord})
	if got := kq.BatchSummary(differ); !errors.As(got, &be) || len(be.Errs) != 2 {
		t.Fatalf("BatchSummary(too large, invalid record) = %v, want the index-aligned *BatchError: the records "+
			"were refused with different codes", got)
	}
}

// oneOutcome reports whether every error of errs, none of them nil, has the
// code and the retryability of the first.
func oneOutcome(errs []error) bool {
	for _, err := range errs[1:] {
		if mq.CodeOf(err) != mq.CodeOf(errs[0]) || mq.IsRetryable(err) != mq.IsRetryable(errs[0]) {
			return false
		}
	}
	return true
}

// rootModuleBefore reports the version of the root module this test binary was
// built with, and whether it is a release older than want.
//
// CI also builds this module with GOWORK=off against the root release its
// go.mod requires, so a test of behaviour the root module gained after that
// release skips there until the release train moves the requirement on. A
// workspace build uses the root module in the tree, which the build
// information reports as (devel), and so does a replacement by a directory;
// neither is older than anything. drivers/redis has the same helper.
func rootModuleBefore(t testing.TB, want string) (string, bool) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("this test binary carries no build information")
	}
	for _, dep := range info.Deps {
		if dep.Path != "github.com/iqhive/iqmq" {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		have, okHave := releaseNumbers(dep.Version)
		limit, okLimit := releaseNumbers(want)
		return dep.Version, okHave && okLimit && slices.Compare(have, limit) < 0
	}
	t.Fatal("this test binary's build information does not list the root module")
	return "", false
}

// releaseNumbers is the major, minor and patch numbers of a module version
// such as v1.1.1 or v1.1.2-0.20261030010203-0123456789ab, and false for
// anything else, (devel) and the empty version of a directory among them.
func releaseNumbers(version string) ([]int, bool) {
	core, ok := strings.CutPrefix(version, "v")
	if !ok {
		return nil, false
	}
	if i := strings.IndexAny(core, "-+"); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return nil, false
	}
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}
