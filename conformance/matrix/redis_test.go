//go:build matrix

// The point is not coverage. A driver whose capability table is the same
// against every version either supports nothing version-dependent or is
// guessing, and the way to tell those apart is to run the same table-building
// code against each version and require the flips the driver says exist.
package matrix

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/conformance/harness"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
)

// redisMatrixImages are the servers this driver is held to.
//
// The default is the four the version the project ships
// against, the oldest one the driver opens against at all, the newest, and
// Valkey — which is a different SERVER rather than a different version, and
// is the one entry that tests whether the driver's version probe reads
// something Valkey reports.
func redisMatrixImages() []string {
	if v := os.Getenv("IQMQ_REDIS_IMAGES"); v != "" {
		var out []string
		for part := range strings.SplitSeq(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
		return out
	}
	return []string{
		"redis:7.4-alpine",
		"redis:6.2-alpine",
		"redis:8.2.9-alpine",
		"valkey/valkey:8.1.10-alpine",
	}
}

// redisResult is what one server reported.
type redisResult struct {
	image, version, software string
	aof                      bool
	opened                   bool
	openErr                  string
	durableConfirm           bool
	atLeastOnce              bool
	extendLease              bool
}

// TestMatrix_redis opens the driver against every server in the matrix and
// asserts the claims flip where the driver says they do.
func TestMatrix_redis(t *testing.T) {
	images := redisMatrixImages()
	if len(images) == 0 {
		t.Skip("IQMQ_REDIS_IMAGES is empty")
	}
	results := make([]redisResult, 0, len(images))
	for _, image := range images {
		// sanitise is the package's, from the first matrix test that needed it:
		// a package with one matrix test per driver shares the helpers whose
		// behaviour is not driver-specific and prefixes the ones that are.
		t.Run(sanitise(image), func(t *testing.T) {
			results = append(results, probeRedis(t, image))
		})
	}
	if len(results) < 2 {
		t.Skip("a matrix of one server cannot show a flip")
	}
	reportRedis(t, results)
	assertRedisFlips(t, results)
}

// probeRedis starts one server and reads what the driver claims against it.
//
// The server is started with `appendonly yes` deliberately: WAITAOF's two
// conditions are the version and the append-only file, and turning the file
// ON is what isolates the VERSION as the thing that differs between the rows.
// A matrix that left it off would find DurableConfirm unsupported everywhere
// and conclude the driver has no version dependency at all.
func probeRedis(t *testing.T, image string) redisResult {
	t.Helper()
	name := "redis"
	if strings.Contains(image, "valkey") {
		name = "valkey"
	}
	t.Setenv(redisEnvKey(name), image)
	var b *harness.Broker
	switch name {
	case "valkey":
		b = harness.Valkey(t, testcontainers.WithCmdArgs("--appendonly", "yes"))
	default:
		b = harness.Redis(t, testcontainers.WithCmdArgs("--appendonly", "yes"))
	}
	res := redisResult{image: image}
	backend, err := rd.BackendWithClientFromAddr(t.Context(), b.Addr, rd.Config{AutoCreate: true})
	if err != nil {
		// A server the driver REFUSES is a result and not a failure: the 6.2
		// row is expected to open (XAUTOCLAIM arrived there), and a hypothetical
		// 6.0 row is expected not to. What would be a failure is a refusal
		// that did not say why.
		res.openErr = err.Error()
		if res.openErr == "" {
			t.Fatal("the driver refused the server with an empty error")
		}
		t.Logf("%s: the driver refused to open: %v", image, err)
		return res
	}
	defer func() { _ = backend.Close() }()
	res.opened = true
	res.version, res.software, res.aof, _ = backend.Profile()
	caps := backend.Capabilities()
	res.durableConfirm = caps.Has(mq.FeatureDurableConfirm)
	res.atLeastOnce = caps.Has(mq.FeatureAtLeastOnce)
	res.extendLease = caps.Has(mq.FeatureExtendLease)
	if res.version == "" {
		t.Fatalf("%s: the driver read no version from the server, so its capability table is "+
			"guessing rather than branching", image)
	}
	if !res.aof {
		t.Fatalf("%s: the server was started with --appendonly yes and the driver reports no "+
			"append-only file; the matrix cannot isolate the version without it", image)
	}
	return res
}

// redisEnvKey is the harness's image override for a broker name.
func redisEnvKey(name string) string {
	return "IQMQ_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_IMAGE"
}

// reportRedis logs the whole matrix, which is what a reader of the CI log needs:
// the table is the evidence, and a single assertion failure without it says
// nothing about which server disagreed.
func reportRedis(t *testing.T, results []redisResult) {
	t.Helper()
	t.Logf("%-30s %-10s %-8s %-6s %-8s %-8s %s", "image", "version", "software", "open", "durable", "atleast", "extend")
	for _, r := range results {
		if !r.opened {
			t.Logf("%-30s %-10s %-8s %-6s refused: %s", r.image, "-", "-", "no", r.openErr)
			continue
		}
		t.Logf("%-30s %-10s %-8s %-6s %-8v %-8v %v",
			r.image, r.version, r.software, "yes", r.durableConfirm, r.atLeastOnce, r.extendLease)
	}
}

// assertRedisFlips is this driver's half of the matrix's actual assertion.
//
// Two things are required and each is the negation of a plausible defect:
//
//  1. Every server the driver OPENED claims at-least-once delivery and lease
//     extension. Those are what the driver is for, and they rest on
//     XAUTOCLAIM — which is why 6.2 is the floor. A row that opened and did
//     not claim them would be a driver that opened against a server it cannot
//     deliver reliably from.
//  2. Every server the driver OPENED claims FeatureDurableConfirm exactly when
//     it is 7.2 or later and has an append-only file. It is WAITAOF, which
//     arrived in 7.2, and every server here has an append-only file, so on a
//     matrix spanning 6.2 and 7.4 the claim has to flip at 7.2. A table that
//     is not reading the version at all fails on one side of it, which is the
//     defect this whole test exists to catch and is invisible to every
//     single-version run. Asking only that the claim DIFFER somewhere let a
//     reversed table pass, and one that claimed it from 7.0, where WAITAOF
//     does not exist.
func assertRedisFlips(t *testing.T, results []redisResult) {
	t.Helper()
	problems, opened, before, after := redisFlipProblems(results)
	for _, p := range problems {
		t.Error(p)
	}
	if opened < 2 {
		t.Skipf("only %d server(s) opened, so no flip can be shown", opened)
	}
	if !before || !after {
		t.Logf("the matrix spans only one side of WAITAOF's 7.2 boundary (before=%v, 7.2 and later=%v), "+
			"so the durable-confirmation flip cannot be shown by it", before, after)
	}
}

// redisFlipProblems is what assertRedisFlips reports, as data, so a test can
// hold the assertion to a wrong table without starting a server: the
// problems, how many servers opened, and whether they span each side of
// WAITAOF's 7.2 boundary.
func redisFlipProblems(results []redisResult) (problems []string, opened int, before, after bool) {
	for _, r := range results {
		if !r.opened {
			continue
		}
		opened++
		if !r.atLeastOnce {
			problems = append(problems, fmt.Sprintf("%s (%s): the driver opened and does not claim at-least-once delivery",
				r.image, r.version))
		}
		if !r.extendLease {
			problems = append(problems, fmt.Sprintf("%s (%s): the driver opened and does not claim lease extension, which is "+
				"XCLAIM ... JUSTID and is what a long handler needs", r.image, r.version))
		}
		major, minor, ok := redisMajorMinor(r.version)
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: the server version %q has no numeric major.minor, so no claim can be held to it",
				r.image, r.version))
			continue
		}
		waitaof := atLeast(major, minor, 7, 2)
		if waitaof {
			after = true
		} else {
			before = true
		}
		if want := waitaof && r.aof; r.durableConfirm != want {
			problems = append(problems, fmt.Sprintf("%s (%s): FeatureDurableConfirm is %v, want %v: it is WAITAOF, "+
				"which arrived in Redis 7.2 and waits on the append-only file (aof=%v), so it belongs to every "+
				"server from 7.2 with one and to none before 7.2",
				r.image, r.version, r.durableConfirm, want, r.aof))
		}
	}
	return problems, opened, before, after
}

// redisMajorMinor reads a server version's major and minor numbers.
func redisMajorMinor(version string) (major, minor int, ok bool) {
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}

// compile-time proof that the matrix uses the driver's public surface only.
var (
	_ = context.Background
	_ = driverkit.ClientOwned
)
