//go:build matrix

// The point is not coverage. A driver whose capability table is the same
// against every version either supports nothing version-dependent or is
// guessing, and the way to tell those apart is to run the same table-building
// code against each version and require the flips the driver says exist.
package matrix

import (
	"context"
	"os"
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
	if name == "valkey" {
		b = harness.Valkey(t, testcontainers.WithCmdArgs("--appendonly", "yes"))
	} else {
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
//  2. FeatureDurableConfirm DIFFERS across the matrix. It is WAITAOF, which
//     arrived in 7.0, and every server here has an append-only file — so a
//     matrix spanning 6.2 and 7.4 in which the claim did NOT differ is a
//     capability table that is not reading the version at all. That is the
//     defect this whole test exists to catch, and it is invisible to every
//     single-version run.
func assertRedisFlips(t *testing.T, results []redisResult) {
	t.Helper()
	opened := 0
	durable := map[bool]int{}
	for _, r := range results {
		if !r.opened {
			continue
		}
		opened++
		if !r.atLeastOnce {
			t.Errorf("%s (%s): the driver opened and does not claim at-least-once delivery",
				r.image, r.version)
		}
		if !r.extendLease {
			t.Errorf("%s (%s): the driver opened and does not claim lease extension, which is "+
				"XCLAIM ... JUSTID and is what a long handler needs", r.image, r.version)
		}
		durable[r.durableConfirm]++
	}
	if opened < 2 {
		t.Skipf("only %d server(s) opened, so no flip can be shown", opened)
	}
	spansSeven := false
	spansSix := false
	for _, r := range results {
		if !r.opened {
			continue
		}
		switch {
		case strings.HasPrefix(r.version, "6."):
			spansSix = true
		case strings.HasPrefix(r.version, "7."), strings.HasPrefix(r.version, "8."):
			spansSeven = true
		}
	}
	if !spansSix || !spansSeven {
		t.Logf("the matrix spans only one side of WAITAOF's 7.0 boundary (6.x=%v, 7.x/8.x=%v), "+
			"so the durable-confirmation flip cannot be shown by it", spansSix, spansSeven)
		return
	}
	if durable[true] == 0 || durable[false] == 0 {
		t.Errorf("FeatureDurableConfirm is %v on every server in a matrix spanning Redis 6.x and "+
			"7.x, each with an append-only file: WAITAOF arrived in 7.0, so a table that does not "+
			"differ here is not reading the version at all",
			durable[true] > 0)
	}
}

// compile-time proof that the matrix uses the driver's public surface only.
var (
	_ = context.Background
	_ = driverkit.ClientOwned
)
