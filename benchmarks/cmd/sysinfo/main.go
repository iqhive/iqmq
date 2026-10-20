// Command sysinfo prints a JSON description of the benchmark host: CPU,
// cores, kernel, frequency governor, NUMA layout, load, memory, Go
// configuration and git commit. Every benchmark report and ADR references
// one of these files.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Info is the machine description.
type Info struct {
	Date        string  `json:"date"`
	Hostname    string  `json:"hostname"`
	OS          string  `json:"os"`
	Arch        string  `json:"arch"`
	CPUModel    string  `json:"cpu_model"`
	CPUs        int     `json:"cpus"`
	GOMAXPROCS  int     `json:"gomaxprocs"`
	Kernel      string  `json:"kernel"`
	Governor    string  `json:"governor"`
	NUMANodes   int     `json:"numa_nodes"`
	LoadAvg1    float64 `json:"loadavg1"`
	MemTotalKB  int64   `json:"mem_total_kb"`
	GoVersion   string  `json:"go_version"`
	GOAMD64     string  `json:"goamd64,omitempty"`
	GOARM64     string  `json:"goarm64,omitempty"`
	GOGC        string  `json:"gogc,omitempty"`
	GOEXPERMENT string  `json:"goexperiment,omitempty"`
	GitCommit   string  `json:"git_commit,omitempty"`
	// GitCommitStatus records when the measured source revision is unavailable.
	GitCommitStatus string `json:"git_commit_status,omitempty"`
	GitDirty        bool   `json:"git_dirty"`
	// GitDirtySource is GitDirty restricted to the code under measurement: it
	// ignores the campaign's own output. `bakeoff decide` writes a generated
	// binding and a variant report for every family it finishes, so from the
	// second family onward a campaign is always "dirty" -- with its own
	// product. A check that refused on that could never pass during a
	// campaign, which is the only time it is asked. GitDirtyPaths names what
	// was uncommitted either way, so an ADR can say so rather than imply a
	// clean tree.
	GitDirtySource bool     `json:"git_dirty_source"`
	GitDirtyPaths  []string `json:"git_dirty_paths,omitempty"`
	Provisional    bool     `json:"provisional"`
	Note           string   `json:"note,omitempty"`
}

func main() {
	out := flag.String("o", "", "write JSON to this file instead of stdout")
	authoritative := flag.Bool("authoritative", false, "mark this host as a pinned, authoritative benchmark host")
	flag.Parse()
	info := collect()
	info.Provisional = !*authoritative
	if info.Provisional {
		info.Note = "shared, unpinned host: results are provisional and not authoritative for cross-host comparison"
	}
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "sysinfo:", err)
		os.Exit(1)
	}
	data = append(data, '\n')
	if *out == "" {
		_, _ = os.Stdout.Write(data)
		return
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "sysinfo:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "sysinfo:", err)
		os.Exit(1)
	}
}

func collect() Info {
	info := Info{
		Date:        time.Now().UTC().Format(time.RFC3339),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		CPUs:        runtime.NumCPU(),
		GOMAXPROCS:  runtime.GOMAXPROCS(0),
		GoVersion:   runtime.Version(),
		GOAMD64:     os.Getenv("GOAMD64"),
		GOARM64:     os.Getenv("GOARM64"),
		GOGC:        os.Getenv("GOGC"),
		GOEXPERMENT: os.Getenv("GOEXPERIMENT"),
	}
	info.Hostname, _ = os.Hostname()
	info.CPUModel = cpuModel()
	info.Kernel = firstLine("/proc/version")
	info.Governor = firstLine("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
	if info.Governor == "" {
		info.Governor = "unknown"
	}
	info.NUMANodes = numaNodes()
	if f := strings.Fields(firstLine("/proc/loadavg")); len(f) > 0 {
		info.LoadAvg1, _ = strconv.ParseFloat(f[0], 64)
	}
	info.MemTotalKB = memTotal()
	switch out, err := exec.Command("git", "rev-parse", "HEAD").Output(); {
	case err == nil:
		info.GitCommit = strings.TrimSpace(string(out))
	default:
		info.GitCommitStatus = "unavailable"
	}
	// `status.relativePaths=false` and the repository root: `git status
	// --porcelain` reports paths relative to the *current directory* by
	// default, and this runs from inside the `benchmarks` module -- so
	// `benchmarks/reports/...` arrived as `reports/...` and every prefix test
	// below silently missed. The exclusion then matched nothing and a
	// campaign's own output read as dirt in the code under measurement.
	top := ""
	if out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		top = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "-c", "status.relativePaths=false", "status", "--porcelain").Output(); err == nil {
		info.GitDirty = strings.TrimSpace(string(out)) != ""
		info.GitDirtySource, info.GitDirtyPaths = sourceDirt(string(out), top)
	}
	return info
}

// sourceDirt splits `git status --porcelain` into "dirt in the code under
// measurement" and the list of everything uncommitted. The campaign's own
// output is excluded from the first: the results and variant reports it
// writes, and any file carrying `bakeoff decide`'s generated-code marker --
// checked by reading the file rather than by its name, so a hand-edited
// binding still counts as dirt.
func sourceDirt(porcelain, top string) (bool, []string) {
	var paths []string
	source := false
	for _, line := range strings.Split(strings.TrimSpace(porcelain), "\n") {
		if len(line) < 4 {
			continue
		}
		path := strings.TrimSpace(line[2:])
		if i := strings.Index(path, " -> "); i >= 0 { // a rename reports both
			path = path[i+4:]
		}
		path = strings.Trim(path, `"`)
		paths = append(paths, path)
		if strings.HasPrefix(path, "benchmarks/reports/") || strings.HasPrefix(path, "docs/benchmarks/variants/") {
			continue
		}
		if generatedByBakeoff(filepath.Join(top, path)) {
			continue
		}
		source = true
	}
	return source, paths
}

// generatedByBakeoff reports whether a path is a binding `bakeoff decide`
// wrote, by its marker and not by its name.
func generatedByBakeoff(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false // deleted, or outside this module: treat as source
	}
	head := data
	if len(head) > 256 {
		head = head[:256]
	}
	return strings.Contains(string(head), "Code generated by bakeoff decide")
}

func firstLine(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	line, _, _ := strings.Cut(string(data), "\n")
	return strings.TrimSpace(line)
}

func cpuModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return runtime.GOARCH
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "model name") {
			_, v, _ := strings.Cut(line, ":")
			return strings.TrimSpace(v)
		}
	}
	return runtime.GOARCH
}

func numaNodes() int {
	entries, err := os.ReadDir("/sys/devices/system/node")
	if err != nil {
		return 1
	}
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "node") {
			if _, err := strconv.Atoi(strings.TrimPrefix(e.Name(), "node")); err == nil {
				n++
			}
		}
	}
	if n == 0 {
		return 1
	}
	return n
}

func memTotal() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				v, _ := strconv.ParseInt(f[1], 10, 64)
				return v
			}
		}
	}
	return 0
}
