package overhead_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/streams"

	"github.com/iqhive/iqmq/benchmarks/opbench"
	"github.com/iqhive/iqmq/benchmarks/overhead"
)

// baselineDir is benchmarks/native-baselines, read as a DIRECTORY rather than
// imported. The baselines module requires this one (it shares the timing
// loop), so importing it back would be a module cycle -- and the property
// being checked is structural anyway: each baseline is a package directory
// named after its client slug.
const baselineDir = "../native-baselines"

func TestEveryProfileNamesABaselineThatExists(t *testing.T) {
	entries, err := os.ReadDir(baselineDir)
	if err != nil {
		t.Fatalf("reading %s: %v", baselineDir, err)
	}
	clients := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() && e.Name() != "nativebench" && !strings.HasPrefix(e.Name(), ".") {
			clients[e.Name()] = true
		}
	}
	if len(clients) == 0 {
		t.Fatalf("no baseline package found under %s; this check would pass having read nothing", baselineDir)
	}
	profiles := overhead.Profiles()
	if len(profiles) == 0 {
		t.Fatal("no driver profile is declared, so BenchmarkDriver measures nothing")
	}
	seen := map[string]bool{}
	for _, p := range profiles {
		switch {
		case p.Arm == "":
			t.Errorf("a profile has no arm name: %+v", p)
		case seen[p.Arm]:
			t.Errorf("duplicate arm %q", p.Arm)
		case p.Harness == nil:
			t.Errorf("%s has no harness, so it cannot open a client", p.Arm)
		}
		seen[p.Arm] = true
		if p.Native == "" {
			// Allowed, but it has to SAY so: an arm with no denominator
			// produces no overhead figure, and a reader of the report needs
			// to know that rather than look for a missing row.
			if p.Note == "" {
				t.Errorf("%s names no BenchmarkNative baseline and no reason; an arm with no denominator has to say why", p.Arm)
			}
			continue
		}
		if !clients[p.Native] {
			t.Errorf("%s is compared with BenchmarkNative/%s, which is not a baseline package under %s",
				p.Arm, p.Native, baselineDir)
		}
	}
}

// redisBaselines are the native arms whose denominator is a Redis client, and
// the file each one names its stream in. The key's bytes are charged per
// command on these two arms and on nobody else's, which is why the check below
// is theirs.
var redisBaselines = map[string]string{
	"rueidis": "rueidis/rueidis_test.go",
	"goredis": "goredis/goredis_test.go",
}

// TestTheRedisArmsHashTheSameKeyBytes pins the thing nothing was testing when
// the redis rows were measured: that the two arms' stream keys are comparable.
//
// They were not. The driver arm published to
// `{iqmqconf.<nonce>.<n>.bench-PublishBatch}` and the native arm to
// `iqmqbench-<runid>-<n>` -- 42 hashed bytes against 20, and 150 RESP bytes
// against 126 -- so the overhead ratio included the conformance harness's
// nonce. A benchmark asymmetry nothing tests is how that survived a campaign.
//
// Two assertions make the hashed byte counts equal, and both are deterministic
// with no broker:
//
//   - the hash tag's contents are EXACTLY the topic, so the driver's braces add
//     no bytes to the CRC16 that rueidis runs per command, and
//   - the driver arm's topic has the same stem and run id as the native arm's
//     resource name, so for the same counter the two names are the same length.
//
// The key itself is longer by the two braces, and that is deliberate: the hash
// tag is real driver behaviour (internal/streams.Keys) and the driver keeps
// paying for it. See benchTopicNames for which side moves and why.
func TestTheRedisArmsHashTheSameKeyBytes(t *testing.T) {
	// The native arm's name, from the generator and stem both arms share.
	native := opbench.Name(opbench.ResourceStem)
	checked := 0
	for _, p := range overhead.Profiles() {
		if _, ok := redisBaselines[p.Native]; !ok {
			continue
		}
		checked++
		h := p.Harness("127.0.0.1:0")
		if h.TopicName == nil {
			t.Errorf("%s names its topic with the driver's default, which no benchmark controls", p.Arm)
			continue
		}
		topic := h.TopicName("bench-PublishBatch")
		keys, err := streams.NewKeys(topic, 1)
		if err != nil {
			t.Errorf("%s: the driver refuses its own benchmark topic %q: %v", p.Arm, topic, err)
			continue
		}
		key := keys.Stream(0)
		span, ok := streams.HashTag(key)
		switch {
		case !ok:
			t.Errorf("%s: key %q carries no hash tag, so rueidis hashes the whole key and this check is measuring the wrong span", p.Arm, key)
		case span != topic:
			t.Errorf("%s: the hashed span of %q is %q, want the topic %q: %d bytes are hashed per command that the native arm does not hash",
				p.Arm, key, span, topic, len(span)-len(topic))
		}
		if len(key) != len(topic)+2 {
			t.Errorf("%s: key %q is %d bytes for a %d-byte topic, want %d: the hash tag's two braces are the only bytes the driver arm may add",
				p.Arm, key, len(key), len(topic), len(topic)+2)
		}
		// Logged, not asserted: the figures a reader of the overhead table
		// wants when they ask what the key costs. They are deterministic, so
		// `go test -v -run TestTheRedisArmsHashTheSameKeyBytes` is a cheaper
		// answer than a campaign.
		t.Logf("%s: key %q is %d bytes, of which %d are hashed per command; the native arm's %q is %d bytes, all %d hashed",
			p.Arm, key, len(key), len(span), native, len(native), len(native))
		// The counter is the only part that may differ, so it is dropped from
		// both before they are compared: what is left is the stem and the run
		// id, and if those agree the two names are the same length.
		if got, want := withoutCounter(topic), withoutCounter(native); got != want {
			t.Errorf("%s names its topic %q and the BenchmarkNative/%s arm names its stream %q: %q against %q once the counter is dropped, so the two arms hash different numbers of bytes",
				p.Arm, topic, p.Native, native, got, want)
		}
	}
	if checked == 0 {
		t.Fatalf("no profile is compared with a Redis baseline (%v), so this check passed having asserted nothing", redisBaselines)
	}
}

// withoutCounter drops opbench.Name's trailing "-<n>", leaving the stem and the
// run id -- the parts that have to agree for two names to be the same length.
func withoutCounter(name string) string {
	if i := strings.LastIndexByte(name, '-'); i > 0 {
		return name[:i]
	}
	return name
}

// TestTheRedisBaselinesNameTheirStreamFromTheSharedStem is the other half of
// the pin, and it reads the baselines as SOURCE for the same reason
// TestEveryProfileNamesABaselineThatExists reads them as a directory: the
// baselines module requires this one, so importing it back would be a module
// cycle. What is checked is that neither redis baseline names its stream from a
// literal of its own -- a literal is how the two arms drift apart again without
// either side looking wrong on its own.
func TestTheRedisBaselinesNameTheirStreamFromTheSharedStem(t *testing.T) {
	for client, file := range redisBaselines {
		path := filepath.Join(baselineDir, file)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("reading the %s baseline at %s: %v", client, path, err)
			continue
		}
		src := string(data)
		if !strings.Contains(src, "nativebench.Name(nativebench.ResourceStem)") {
			t.Errorf("%s does not name its stream nativebench.Name(nativebench.ResourceStem); the driver arm's topic comes from that same stem, and a name of a different length puts itself into the overhead ratio", path)
		}
		if strings.Contains(src, `nativebench.Name("`) {
			t.Errorf("%s names a broker resource from a string literal; the stem is shared so that the two arms cannot drift to different key lengths", path)
		}
	}
}

// TestTheTwoArmsProduceTheSameBenchmarkShape is the structural half of
// "identical settings". The rows are compared leaf by leaf, so if one arm ever
// gained an operation or a payload size the other did not have, the overhead
// table would silently lose or invent rows.
func TestTheTwoArmsProduceTheSameBenchmarkShape(t *testing.T) {
	for _, p := range overhead.Profiles() {
		if p.Native == "" {
			continue
		}
		driver := opbench.Names("BenchmarkDriver", p.Arm)
		native := opbench.Names("BenchmarkNative", p.Native)
		if len(driver) != len(native) {
			t.Fatalf("%s: %d driver rows against %d native rows", p.Arm, len(driver), len(native))
		}
		for i := range driver {
			d := strings.TrimPrefix(driver[i], "BenchmarkDriver/"+p.Arm+"/")
			n := strings.TrimPrefix(native[i], "BenchmarkNative/"+p.Native+"/")
			if d != n {
				t.Errorf("%s: row %d is %q on the driver arm and %q on the native arm", p.Arm, i, d, n)
			}
		}
	}
}

// TestBenchmarkDriverNamesFollowTheSpecification pins the shape, because
// a budget pattern and an overhead table are both written against these names.
func TestBenchmarkDriverNamesFollowTheSpecification(t *testing.T) {
	names := opbench.Names("BenchmarkDriver", "redis/streams")
	if len(names) != 12 {
		t.Fatalf("names: %d, want 12", len(names))
	}
	for _, want := range []string{
		"BenchmarkDriver/redis/streams/Publish/64B",
		"BenchmarkDriver/redis/streams/PublishBatch/1KiB",
		"BenchmarkDriver/redis/streams/Receive/64KiB",
		"BenchmarkDriver/redis/streams/EndToEnd/64B",
	} {
		found := false
		for _, n := range names {
			if n == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s is not among the names BenchmarkDriver produces: %v", want, names)
		}
	}
}

// TestTheInProcessProfileRoundTrips is opbench's own correctness gate run on
// the driver arm, for the one profile that needs no container. It is the same
// gate every native baseline's TestNativeSession runs , so
// the session contract is held to one standard on both sides -- and it is why
// `make test-race` can fail a broken driver session without a broker.
func TestTheInProcessProfileRoundTrips(t *testing.T) {
	var mem *overhead.Profile
	for _, p := range overhead.Profiles() {
		if p.AddrEnv == "" {
			mem = &p
			break
		}
	}
	if mem == nil {
		t.Skip("no in-process profile is declared")
	}
	h := mem.Harness("")
	opbench.CheckSession(t, func(tb testing.TB, params opbench.Params) opbench.Session {
		return overhead.Open(tb, h, params)
	})
}

// TestTheProfileTableIsDocumented keeps the report honest about coverage: a
// partial matrix is the deliverable, but which profiles are in it and which
// are not has to be written down where a reader will find it.
func TestTheProfileTableIsDocumented(t *testing.T) {
	page := filepath.Join("..", "..", "docs", "performance", "overhead.md")
	data, err := os.ReadFile(page)
	if err != nil {
		t.Fatalf("reading %s: %v", page, err)
	}
	text := string(data)
	for _, p := range overhead.Profiles() {
		if !strings.Contains(text, p.Arm) {
			t.Errorf("%s does not name the measured profile %q", page, p.Arm)
		}
	}
}
