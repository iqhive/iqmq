package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// This file judges `make vuln`'s output: govulncheck's JSON stream for every
// module, written by hack/vuln.sh under one directory per module, against the
// advisories this repository has decided it cannot fix yet.
//
// `make vuln` used to be `govulncheck ./...` in every module with the text
// output, which exits 3 on any finding. Four modules carry Avro advisories that
// have NO fixed version, so the command could not pass on any day, and a job
// that is red every day says nothing on the day something new arrives -- the
// new finding and the six old ones look the same. The first time vuln.yml ran
// on GitHub it was red in 38 of 49 modules, and 31 of those were a statement
// about the toolchain the job installed rather than about this repository.
// This is hack/ci-parity.sh's and this tool's own waiver
// design applied to advisories: every waiver is printed on every run, names an
// owner, and fails when it matches nothing.

// vulnWaiver is an advisory this repository knows about and has not fixed.
type vulnWaiver struct {
	// osv is the advisory's identifier, GO-YYYY-NNNN.
	osv string
	// modules are the module directories, as hack/modules.sh prints them,
	// the waiver covers. Each is checked separately: a module that no longer
	// has the finding fails the waiver, so the list cannot outgrow the truth.
	modules []string
	// noFix says the advisory had no fixed version when the waiver was
	// written, and that this is the reason for it. The day the database
	// names one, the waiver FAILS: a fix to take is not a thing to waive.
	noFix bool
	// owner is the increment that will fix it.
	owner string
	// reason says why it is not fixed here.
	reason string
}

// avroModules are the modules that link github.com/hamba/avro/v2: codec/avro
// directly (v2.31.0), and three through the Pulsar client's schema support
// (v2.29.0). Two versions of one decoder, so a fix is two bumps.
var avroModules = []string{"cmd/iqmq", "codec/avro", "drivers/pulsar", "x/pulsartx"}

// The GO-2026-6061 and GO-2026-6348 waivers lived here, over a grpcModules list
// of "benchmarks" and "benchmarks/competitors". Both are GONE rather than
// trimmed: grpc v1.84.0 fixes both advisories and the measurement modules took
// that bump deliberately, with the re-measurement the waivers named as their
// condition. MEASURED before removing them -- `make vuln` reported "waiver for
// GO-2026-6061 in benchmarks matched no finding" for all four module-advisory
// pairs, which is this checker refusing a waiver that has outgrown the truth.
//
// That refusal is also what CAUGHT the bump. grpc is INDIRECT in those modules,
// so the pinned-client registry cannot see it and TestPinnedVersions passed
// while PR #78 lifted a deliberate hold under a title about x/crypto. Making the
// job green by deleting the waiver, which is the obvious reading of the error,
// would have accepted the refused change in silence. Keep the rule
// bidirectional.

// vulnWaivers are the known, owned advisories. Every one is printed on each run.
var vulnWaivers = []vulnWaiver{
	{
		osv: "GO-2026-5046", modules: avroModules, noFix: true, owner: "B18 (Pulsar schema path), C07 (release gate)",
		reason: "CPU exhaustion in the Avro decoder; no fixed version exists. codec/avro checks MaxBytes " +
			"before the decoder sees a body; the Pulsar client's schema support does not go through codec/avro",
	},
	{
		osv: "GO-2026-5047", modules: avroModules, noFix: true, owner: "B18 (Pulsar schema path), C07 (release gate)",
		reason: "integer overflow in the Avro decoder; no fixed version exists; same paths as GO-2026-5046",
	},
	{
		osv: "GO-2026-5048", modules: avroModules, noFix: true, owner: "B18 (Pulsar schema path), C07 (release gate)",
		reason: "unbounded map allocation in the Avro decoder; no fixed version exists; the class B20 found " +
			"in this decoder by fuzzing (a six-byte body asking for 8.8 TB)",
	},
}

// govulncheckFile is the name hack/vuln.sh gives each module's JSON stream.
const govulncheckFile = "govulncheck.json"

// vulnFinding is one advisory that reaches one module's own code.
type vulnFinding struct {
	module  string // the module directory scanned
	osv     string
	found   string // module@version the vulnerable symbol is in
	fixed   string // the fixed version, "" when the database names none
	summary string
	trace   string // one example call path, entry first
}

// vulnScan is one module's scan.
type vulnScan struct {
	module    string
	goVersion string
	db        string
	findings  map[string]*vulnFinding // by osv
}

// govulncheck's JSON protocol v1: a stream of objects, each with one key.
type govulnMessage struct {
	Config *struct {
		GoVersion      string `json:"go_version"`
		ScannerVersion string `json:"scanner_version"`
		DBLastModified string `json:"db_last_modified"`
	} `json:"config"`
	OSV *struct {
		ID      string `json:"id"`
		Summary string `json:"summary"`
	} `json:"osv"`
	Finding *struct {
		OSV          string `json:"osv"`
		FixedVersion string `json:"fixed_version"`
		Trace        []struct {
			Module   string `json:"module"`
			Version  string `json:"version"`
			Package  string `json:"package"`
			Function string `json:"function"`
			Receiver string `json:"receiver"`
			Position *struct {
				Filename string `json:"filename"`
				Line     int    `json:"line"`
			} `json:"position"`
		} `json:"trace"`
	} `json:"finding"`
}

// readVulnScan parses one module's govulncheck stream. Only SYMBOL-level
// findings count -- a trace whose first frame names a function -- because
// that is govulncheck's own "your code is affected" and what its text mode
// exits 3 on. Module- and package-level findings are advisories in code the
// module requires but never calls.
func readVulnScan(module string, r io.Reader) (*vulnScan, error) {
	s := &vulnScan{module: module, findings: map[string]*vulnFinding{}}
	summaries := map[string]string{}
	dec := json.NewDecoder(r)
	sawConfig := false
	for {
		var m govulnMessage
		err := dec.Decode(&m)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", module, err)
		}
		switch {
		case m.Config != nil:
			sawConfig = true
			s.goVersion, s.db = m.Config.GoVersion, m.Config.DBLastModified
		case m.OSV != nil:
			summaries[m.OSV.ID] = m.OSV.Summary
		case m.Finding != nil:
			tr := m.Finding.Trace
			if len(tr) == 0 || tr[0].Function == "" {
				continue
			}
			f := s.findings[m.Finding.OSV]
			if f == nil {
				f = &vulnFinding{module: module, osv: m.Finding.OSV, fixed: m.Finding.FixedVersion}
				f.found = tr[0].Module
				if tr[0].Version != "" {
					f.found += "@" + tr[0].Version
				}
				// Entry first: the last frame is this module's own code.
				last := tr[len(tr)-1]
				where := last.Package + "." + last.Function
				if last.Position != nil {
					where = fmt.Sprintf("%s:%d %s", last.Position.Filename, last.Position.Line, where)
				}
				f.trace = where + " -> ... -> " + tr[0].Package + "." + tr[0].Function
				s.findings[m.Finding.OSV] = f
			}
		}
	}
	if !sawConfig {
		// A stream with no config message is not a govulncheck result -- an
		// empty file from a scan that died, most likely -- and reading it as
		// "no findings" is the silent pass this whole file exists to avoid.
		return nil, fmt.Errorf("%s: no govulncheck config message; the scan did not run", module)
	}
	for id, f := range s.findings {
		f.summary = summaries[id]
	}
	return s, nil
}

// readVulnScans reads every <module>/govulncheck.json under dir.
func readVulnScans(dir string) ([]*vulnScan, error) {
	var scans []*vulnScan
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != govulncheckFile {
			return nil
		}
		rel, err := filepath.Rel(dir, filepath.Dir(p))
		if err != nil {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		s, err := readVulnScan(filepath.ToSlash(rel), f)
		if err != nil {
			return err
		}
		scans = append(scans, s)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(scans) == 0 {
		return nil, fmt.Errorf("no %s under %s; nothing was scanned", govulncheckFile, dir)
	}
	sort.Slice(scans, func(i, j int) bool { return scans[i].module < scans[j].module })
	return scans, nil
}

// judgeVulns holds the scans to the waiver table. It returns the report lines
// and the problems; any problem fails `make vuln`.
func judgeVulns(scans []*vulnScan, waivers []vulnWaiver) (report, problems []string) {
	waiverFor := func(module, osv string) *vulnWaiver {
		for i := range waivers {
			w := &waivers[i]
			if w.osv != osv {
				continue
			}
			for _, m := range w.modules {
				if m == module {
					return w
				}
			}
		}
		return nil
	}
	seen := map[string]bool{} // osv + "\x00" + module
	toolchains := map[string]bool{}
	dbs := map[string]bool{}
	affected := 0
	for _, s := range scans {
		toolchains[s.goVersion] = true
		dbs[s.db] = true
		ids := make([]string, 0, len(s.findings))
		for id := range s.findings {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		if len(ids) > 0 {
			affected++
		}
		for _, id := range ids {
			f := s.findings[id]
			seen[id+"\x00"+s.module] = true
			fixed := f.fixed
			if fixed == "" {
				fixed = "N/A"
			}
			w := waiverFor(s.module, id)
			switch {
			case w == nil:
				problems = append(problems, fmt.Sprintf(
					"%s: %s (%s) in %s, fixed in %s\n      %s", s.module, id, f.summary, f.found, fixed, f.trace,
				))
			case w.noFix && f.fixed != "":
				problems = append(problems, fmt.Sprintf(
					"%s: %s is waived as having no fixed version, and the database now names %s: take it and "+
						"delete the waiver (owner %s)", s.module, id, f.fixed, w.owner,
				))
			default:
				report = append(report, fmt.Sprintf("  WAIVED  %-14s %-24s %s, fixed in %s\n            owner %s: %s",
					id, s.module, f.found, fixed, w.owner, w.reason))
			}
		}
	}
	for i := range waivers {
		w := &waivers[i]
		for _, m := range w.modules {
			if !seen[w.osv+"\x00"+m] {
				problems = append(problems, fmt.Sprintf(
					"waiver for %s in %s (owner %s) matched no finding; remove the module from it, or the "+
						"next finding of that name there is waived too", w.osv, m, w.owner,
				))
			}
		}
	}
	report = append(report, fmt.Sprintf(
		"securitycheck: vuln: %d module(s) scanned with %s against the database of %s; %d affected",
		len(scans), joinSorted(toolchains), joinSorted(dbs), affected,
	))
	return report, problems
}

// runVuln is `securitycheck -vuln DIR`.
func runVuln(dir string) int {
	scans, err := readVulnScans(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "securitycheck: vuln: "+err.Error())
		return 2
	}
	report, problems := judgeVulns(scans, vulnWaivers)
	for _, line := range report {
		fmt.Println(line)
	}
	for _, p := range problems {
		fmt.Fprintln(os.Stderr, "securitycheck: vuln: "+p)
	}
	if len(problems) > 0 {
		fmt.Fprintf(os.Stderr, "securitycheck: vuln: %d problem(s)\n", len(problems))
		if stdlib := countStdlib(problems); stdlib > 0 {
			fmt.Fprintf(os.Stderr, "securitycheck: vuln: %d of them are in the Go standard library, which is a "+
				"statement about the toolchain that ran the scan (%s) rather than about this repository\n",
				stdlib, strings.TrimSpace(scans[0].goVersion))
		}
		return 1
	}
	return 0
}

func countStdlib(problems []string) int {
	n := 0
	for _, p := range problems {
		if strings.Contains(p, " in stdlib") {
			n++
		}
	}
	return n
}
