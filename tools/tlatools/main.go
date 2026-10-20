// Command tlatools fetches the pinned TLA+ tools jar and runs TLC.
//
// Usage:
//
//	tlatools [-lock models/tla/tools.lock] [-cache DIR] [-pin]            # ensure jar, print path
//	tlatools run [-workers N] [-xmx 8g] [-nodeadlock] SPEC [tlc args...]   # run TLC on models/tla/SPEC
//	tlatools run-all [flags]                                               # run every spec with a .cfg
//	tlatools mutate [-only NAME] [-record FILE] [-list]                    # negative control: every
//	                                                                       # invariant must be falsifiable
//
// The lock file records the release version, download URL and sha256. With
// -pin an empty sha256 is filled in from the downloaded file; otherwise the
// download must match the recorded digest.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tlatools:", err)
		os.Exit(1)
	}
}

type lock struct {
	order  []string
	values map[string]string
}

func readLock(path string) (*lock, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	l := &lock{values: map[string]string{}}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s: bad line %q", path, line)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		l.order = append(l.order, k)
		l.values[k] = v
	}
	for _, k := range []string{"version", "url"} {
		if l.values[k] == "" {
			return nil, fmt.Errorf("%s: missing %s", path, k)
		}
	}
	return l, nil
}

func (l *lock) write(path string) error {
	var b strings.Builder
	b.WriteString("# Pinned TLA+ tools release. Regenerate the digest with: go run ./tools/tlatools -pin\n")
	for _, k := range l.order {
		fmt.Fprintf(&b, "%s=%s\n", k, l.values[k])
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func defaultCache() string {
	if d, err := os.UserCacheDir(); err == nil {
		return filepath.Join(d, "iqmq")
	}
	return filepath.Join(os.TempDir(), "iqmq-cache")
}

func run(args []string) error {
	fs := flag.NewFlagSet("tlatools", flag.ContinueOnError)
	lockPath := fs.String("lock", "models/tla/tools.lock", "lock file path")
	cache := fs.String("cache", defaultCache(), "cache directory for the jar")
	pin := fs.Bool("pin", false, "record the downloaded sha256 into the lock file when it is empty")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	jar, err := ensureJar(*lockPath, *cache, *pin)
	if err != nil {
		return err
	}
	if len(rest) == 0 {
		fmt.Println(jar)
		return nil
	}
	switch rest[0] {
	case "run":
		return runTLC(jar, rest[1:], false)
	case "run-all":
		return runTLC(jar, rest[1:], true)
	case "mutate":
		return mutate(jar, rest[1:])
	default:
		return fmt.Errorf("unknown command %q", rest[0])
	}
}

func ensureJar(lockPath, cache string, pin bool) (string, error) {
	l, err := readLock(lockPath)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return "", err
	}
	jar := filepath.Join(cache, "tla2tools-"+l.values["version"]+".jar")
	if _, err := os.Stat(jar); err != nil {
		if err := download(l.values["url"], jar); err != nil {
			return "", err
		}
	}
	sum, err := fileSHA256(jar)
	if err != nil {
		return "", err
	}
	switch want := l.values["sha256"]; {
	case want == "" && pin:
		l.values["sha256"] = sum
		if !contains(l.order, "sha256") {
			l.order = append(l.order, "sha256")
		}
		if err := l.write(lockPath); err != nil {
			return "", err
		}
		fmt.Fprintf(os.Stderr, "tlatools: pinned sha256=%s\n", sum)
	case want == "":
		return "", errors.New("lock file has no sha256; run with -pin to record it")
	case want != sum:
		_ = os.Remove(jar)
		return "", fmt.Errorf("sha256 mismatch for %s: got %s want %s (file removed)", jar, sum, want)
	}
	return jar, nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func download(url, dst string) error {
	fmt.Fprintf(os.Stderr, "tlatools: downloading %s\n", url)
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: %s", url, resp.Status)
	}
	tmp := dst + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func runTLC(jar string, args []string, all bool) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	workers := fs.Int("workers", min(16, runtime.NumCPU()), "TLC worker threads")
	xmx := fs.String("xmx", "8g", "JVM heap")
	noDeadlock := fs.Bool("nodeadlock", false, "disable TLC deadlock checking (passes -deadlock)")
	dir := fs.String("dir", "models/tla", "directory holding the specs (run-all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var specs []string
	switch {
	case all:
		matches, err := filepath.Glob(filepath.Join(*dir, "*.cfg"))
		if err != nil {
			return err
		}
		for _, m := range matches {
			specs = append(specs, strings.TrimSuffix(m, ".cfg"))
		}
		if len(specs) == 0 {
			fmt.Fprintln(os.Stderr, "tlatools: no specs found")
			return nil
		}
	case fs.NArg() == 0:
		return errors.New("run: SPEC argument required")
	default:
		specs = []string{strings.TrimSuffix(fs.Arg(0), ".tla")}
	}
	for _, spec := range specs {
		specDir, name := filepath.Split(spec)
		if specDir == "" {
			specDir = "."
		}
		if _, err := os.Stat(filepath.Join(specDir, name+".tla")); err != nil {
			return err
		}
		states := filepath.Join(specDir, "states")
		_ = os.MkdirAll(states, 0o755)
		tlc := []string{
			"-XX:+UseParallelGC", activeProcessorCount(), "-Xmx" + *xmx, "-cp", jar, "tlc2.TLC",
			"-workers", strconv.Itoa(*workers), "-metadir", "states", "-config", name + ".cfg", "-cleanup",
		}
		if *noDeadlock {
			tlc = append(tlc, "-deadlock")
		}
		if !all {
			tlc = append(tlc, fs.Args()[1:]...)
		}
		tlc = append(tlc, name+".tla")
		cmd := exec.Command("java", tlc...)
		cmd.Dir = specDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		fmt.Fprintf(os.Stderr, "tlatools: TLC %s (workers=%d)\n", spec, *workers)
		start := time.Now()
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("TLC %s failed after %s: %w", spec, time.Since(start).Round(time.Millisecond), err)
		}
		fmt.Fprintf(os.Stderr, "tlatools: TLC %s ok in %s\n", spec, time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// activeProcessorCount is the JVM's half of the host CPU ceiling.
// The Go side of `make ci` is capped by GOMAXPROCS and -p,
// but TLC is a Java process and the JVM sizes its GC and compiler threads from
// the machine's core count, not from anything Go sets: -XX:+UseParallelGC on a
// host this size asks for tens of GC threads behind a run that
// `make tla` deliberately holds to two TLC workers. -XX:ActiveProcessorCount
// is what the JVM reads instead, and the ceiling is the same IQMQ_MAX_CPUS the
// rest of the repository uses, never more than the machine has.
func activeProcessorCount() string {
	n := 16
	if v := os.Getenv("IQMQ_MAX_CPUS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}
	if n > runtime.NumCPU() {
		n = runtime.NumCPU()
	}
	return "-XX:ActiveProcessorCount=" + strconv.Itoa(n)
}
