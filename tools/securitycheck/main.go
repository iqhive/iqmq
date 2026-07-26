// Command securitycheck holds this repository to the parts of the API contract
// §12 that can be decided by reading it, without a network and without a
// build.
//
// §12 is unusually specific for a security section -- "secrets use redacting
// wrapper types", "`Config` implements safe `String`/`LogValue`", "insecure
// transport requires an option whose name contains `Insecure`", "SBOM + SLSA
// provenance for CLI binaries" -- and every one of those is a property of the
// source tree rather than of a running system. Until C06 all of them were
// checked by reading, which is how three of them came to be false at once:
//
// The checks, each of which has been watched failing -- on this tree for the
// three above, and on a fixture for every check, which is what `-self-test`
// is for:
//
//	configs      every exported Config in a package a caller configures a
//	             connection with implements String() string AND
//	             LogValue() slog.Value
//	credentials  every exported struct field whose NAME says it holds a
//	             credential, in a connection configuration or a login, has a
//	             redacting type -- dsn.Secret, or a local Secret with both
//	             renderings -- and not a bare string
//	insecure     every exported field that weakens or disables transport
//	             security has "Insecure" in its name
//	supplychain  the security workflows cover every module, the release
//	             workflow's provenance subjects cover every archive format the
//	             goreleaser configuration produces, and the vulnerability scan
//	             and the CLI build run a patched Go rather than go.mod's floor
//
// With -vuln DIR it does something different: it judges the govulncheck JSON
// that hack/vuln.sh (`make vuln`) wrote under DIR against the advisory waivers
// in vuln.go. That half needs the network to produce its input, which is why
// it is `make vuln` and not part of `make security-check`.
//
// A finding the repository has decided not to fix yet is not silenced, it is
// WAIVED: waivers below name the subject, the owner and the reason, they are
// printed on every run, and a waiver whose finding has gone away fails the
// check -- dead text that would excuse the next defect to take that name is
// the failure mode a silenced check has. This is the CI parity check's design,
// which exists for the same reason.
package main

import (
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func main() {
	root := flag.String("root", ".", "repository root")
	selfTest := flag.Bool("self-test", false, "run every check against a fixture that must fail it")
	only := flag.String("check", "", "run one check only (configs, credentials, insecure, supplychain)")
	vulnDir := flag.String("vuln", "", "judge hack/vuln.sh's govulncheck output under this directory against the advisory waivers")
	flag.Parse()

	if *vulnDir != "" {
		os.Exit(runVuln(*vulnDir))
	}

	if *selfTest {
		if err := selfCheck(); err != nil {
			fmt.Fprintln(os.Stderr, "securitycheck: "+err.Error())
			os.Exit(1)
		}
		return
	}

	c := &checker{root: *root}
	if err := c.load(); err != nil {
		fmt.Fprintln(os.Stderr, "securitycheck: "+err.Error())
		os.Exit(2)
	}
	c.run(*only)
	for _, line := range c.report {
		fmt.Println(line)
	}
	if len(c.problems) > 0 {
		for _, p := range c.problems {
			fmt.Fprintln(os.Stderr, "securitycheck: "+p)
		}
		fmt.Fprintf(os.Stderr, "securitycheck: %d problem(s)\n", len(c.problems))
		os.Exit(1)
	}
}

// ---------------------------------------------------------------- waivers

// waiver is a finding this repository knows about and has not fixed.
type waiver struct {
	// check is which check produces the finding.
	check string
	// subject is the finding's subject, matched exactly against what the
	// check reports.
	subject string
	// owner is the increment that will fix it. A waiver with no owner is
	// not a waiver, it is a shrug.
	owner string
	// reason says why it is not fixed here.
	reason string
}

// waivers are the known, owned findings. Every one is printed on each run.
//
// The table is empty. C06 left three entries -- redisstore's Password, the
// custom-driver example's Credential, release.yml's missing zip provenance --
// and C06b closed all three; each was watched failing as dead once its finding
// had gone, which is the half of this mechanism that stops a waiver outliving
// its subject. The mechanism stays: the next finding that cannot be fixed where
// it is found goes here with an owner, not into a quieter check.
var waivers = []waiver{}

// ---------------------------------------------------------------- loading

// checker holds the parsed tree.
type checker struct {
	root     string
	fset     *token.FileSet
	files    []*srcFile
	modules  []string
	problems []string
	report   []string
	// used records which waiver matched something, so that a waiver whose
	// finding has gone away can be reported as dead.
	used map[string]bool
	// noWaivers turns the waiver table off. The self-test sets it, and it
	// has to: the first run of the self-test passed with the provenance
	// check reporting NOTHING, because the fixture's finding has the same
	// subject as this repository's real waiver and was silenced by it. A
	// waiver is a statement about this repository; letting it reach a
	// fixture makes the fixture prove the opposite of what it is for.
	noWaivers bool
}

// srcFile is one parsed non-test Go file.
type srcFile struct {
	rel  string
	pkg  string
	file *ast.File
}

// skipDirs are the directories no check reads. testdata holds deliberately
// malformed input; .git and dist hold no source.
var skipDirs = map[string]bool{
	".git": true, "testdata": true, "dist": true, "node_modules": true, ".archive": true,
}

func (c *checker) load() error {
	c.fset = token.NewFileSet()
	c.used = map[string]bool{}
	err := filepath.WalkDir(c.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(c.root, p)
		if rerr != nil {
			return rerr
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return fs.SkipDir
			}
			return nil
		}
		switch {
		case d.Name() == "go.mod":
			c.modules = append(c.modules, filepath.ToSlash(filepath.Dir(rel)))
			return nil
		case !strings.HasSuffix(d.Name(), ".go"), strings.HasSuffix(d.Name(), "_test.go"):
			return nil
		}
		f, perr := parser.ParseFile(c.fset, p, nil, parser.SkipObjectResolution)
		if perr != nil {
			// A file this tool cannot parse is not silently skipped: every
			// check below would then be vacuous for it.
			return fmt.Errorf("parsing %s: %w", rel, perr)
		}
		c.files = append(c.files, &srcFile{rel: filepath.ToSlash(rel), pkg: f.Name.Name, file: f})
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(c.modules)
	sort.Slice(c.files, func(i, j int) bool { return c.files[i].rel < c.files[j].rel })
	if len(c.files) == 0 {
		return errors.New("no Go files found; -root is probably wrong")
	}
	return nil
}

// fail records a finding unless a waiver names it.
func (c *checker) fail(check, subject, format string, args ...any) {
	for i := range waivers {
		if c.noWaivers {
			break
		}
		w := &waivers[i]
		if w.check == check && w.subject == subject {
			c.used[check+"\x00"+subject] = true
			c.report = append(c.report, fmt.Sprintf("  WAIVED  %-12s %s\n            owner %s: %s",
				check, subject, w.owner, w.reason))
			return
		}
	}
	c.problems = append(c.problems, check+": "+fmt.Sprintf(format, args...))
}

func (c *checker) okf(format string, args ...any) {
	c.report = append(c.report, "securitycheck: ok, "+fmt.Sprintf(format, args...))
}

func (c *checker) run(only string) {
	if only == "" || only == "configs" {
		c.checkConfigs()
	}
	if only == "" || only == "credentials" {
		c.checkCredentials()
	}
	if only == "" || only == "insecure" {
		c.checkInsecure()
	}
	if only == "" || only == "supplychain" {
		c.checkSupplyChain()
	}
	if only != "" || c.noWaivers {
		return
	}
	// A waiver that matched nothing is dead text, and dead text excuses the
	// next defect to take that name.
	for i := range waivers {
		w := &waivers[i]
		if !c.used[w.check+"\x00"+w.subject] {
			c.problems = append(c.problems, fmt.Sprintf(
				"waiver for %s/%q (owner %s) matched no finding; delete it, or the next defect of that name is waived too",
				w.check, w.subject, w.owner,
			))
		}
	}
}

// ---------------------------------------------------------------- configs

// connectionConfigDirs returns the packages whose exported Config is the thing
// a caller fills in to REACH A BROKER: an address, a credential, a TLS
// setting. That is the Config §12's rendering obligation is about.
//
// It is derived rather than listed, so a driver added tomorrow is covered
// without this file changing: every non-internal directory under drivers/ that
// holds a config.go, plus the two that hold a connection configuration and are
// not under drivers/ -- dsn, whose Config is a PARSED connection string and
// was the one that leaked , and memory, whose Config is the
// one every test and every quick start opens and which implemented neither
// rendering.
//
// What is deliberately NOT here, because the obligation is about a connection
// and a rendering nobody needs is a rendering nobody keeps right:
// codec/*.Config (a schema and four size limits), integrations/*.Config (a
// tracer and a histogram), internal fakes, and the examples' toy driver.
func (c *checker) connectionConfigDirs() map[string]bool {
	out := map[string]bool{"dsn": true, "memory": true}
	for _, f := range c.files {
		dir := filepath.ToSlash(filepath.Dir(f.rel))
		if !strings.HasPrefix(dir, "drivers/") || strings.Contains(dir, "/internal/") {
			continue
		}
		if filepath.Base(f.rel) == "config.go" {
			out[dir] = true
		}
	}
	return out
}

func (c *checker) checkConfigs() {
	dirs := c.connectionConfigDirs()
	type key struct{ dir, name string }
	declared := map[key]string{} // -> file
	methods := map[key]map[string]bool{}
	for _, f := range c.files {
		dir := filepath.ToSlash(filepath.Dir(f.rel))
		if !dirs[dir] {
			continue
		}
		for _, d := range f.file.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				if d.Tok != token.TYPE {
					continue
				}
				for _, sp := range d.Specs {
					ts, ok := sp.(*ast.TypeSpec)
					if !ok || !ts.Name.IsExported() || ts.Name.Name != "Config" {
						continue
					}
					if _, isStruct := ts.Type.(*ast.StructType); !isStruct {
						continue // a type alias renders through the type it names
					}
					declared[key{dir, ts.Name.Name}] = f.rel
				}
			case *ast.FuncDecl:
				if d.Recv == nil || len(d.Recv.List) != 1 {
					continue
				}
				recv := receiverName(d.Recv.List[0].Type)
				k := key{dir, recv}
				if methods[k] == nil {
					methods[k] = map[string]bool{}
				}
				methods[k][d.Name.Name] = true
			}
		}
	}
	ks := make([]key, 0, len(declared))
	for k := range declared {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i].dir < ks[j].dir })
	for _, k := range ks {
		for _, m := range [...]string{"String", "LogValue"} {
			if methods[k][m] {
				continue
			}
			c.fail("configs", k.dir+"."+k.name+"."+m,
				"%s declares %s with no %s method; §12 requires a safe String AND LogValue (%s)",
				k.dir, k.name, m, declared[k])
		}
	}
	c.okf("%d connection Config type(s) implement both §12 renderings", len(ks))
}

// ---------------------------------------------------------------- credentials

// credentialField matches a field name that holds a credential. It is the
// same substring rule as dsn.IsCredentialName, spelled as a pattern over
// Go identifiers.
var credentialField = regexp.MustCompile(`(?i)(password|passwd|secret|token|credential|apikey|privatekey|passphrase|authparams|nkey)`)

// pathField matches a field name that holds a PATH to a credential rather than
// the credential. `CredentialsFile` and `TLSTrustCertsFilePath` are filenames,
// and a filename in a log line is what an operator needs to see; redacting one
// would hide the setting and protect nothing.
var pathField = regexp.MustCompile(`(?i)(file|path|dir|filename)$`)

// addressField matches a field name that holds where to connect to. A struct
// with one is a CONNECTION configuration wherever it lives, which is the
// mechanical form of "the thing that holds an address and a credential" -- and
// it is what puts x/idempotency/redisstore.Options in scope even though it is
// neither a driver nor in the root module.
var addressField = regexp.MustCompile(`^(Addr|Addrs|Address|Addresses|Endpoint|Endpoints|URL|URLs|Brokers|Host|Hosts|DSN)$`)

// principalField matches a field that names WHO authenticates. A struct that
// carries one is a login -- a user name and, beside it, the credential that
// proves it -- wherever it lives and whatever it is called.
//
// It is what puts conformance/harness.Auth in scope. `Auth{User, Password
// string}` names no address and is called neither Config nor Options, so the
// two rules above passed it by, and `make security-check` said nothing about a
// plain-string password in the harness every broker suite opens its client
// through.
var principalField = regexp.MustCompile(`^(User|Username|UserName|Login|Principal)$`)

// isConnectionStruct reports whether a struct is one a credential belongs in:
// it names somewhere to connect to or someone to log in as, or it is the
// configuration type of a package by name. A struct that is none of these --
// a settlement Outcome that carries a broker's publish token, an idempotency
// Claim, a fault-injection knob called LeakSecret -- holds an identifier or a
// test fixture and not a credential, and reporting those would be the noise
// that gets a check switched off.
func isConnectionStruct(name string, st *ast.StructType) bool {
	if name == "Config" || name == "Options" {
		return true
	}
	for _, fld := range st.Fields.List {
		for _, nm := range fld.Names {
			if addressField.MatchString(nm.Name) || principalField.MatchString(nm.Name) {
				return true
			}
		}
	}
	return false
}

// redactingTypes are the qualified type expressions a credential field may
// have. A credential in a bare string is the defect; a credential in a type
// that renders itself as the redaction is the rule §12 states.
//
// An UNQUALIFIED `Secret` is accepted only when its own package declares it
// with both renderings -- see localSecretRenderings. It used to be accepted by
// name alone, so `type Secret string` with no methods at all passed, and the
// check's own passing fixture was exactly that: a type that prints the
// credential under a name that promises not to.
var redactingTypes = map[string]bool{
	"dsn.Secret": true, "mq.Secret": true,
}

// secretRenderings are the methods a local Secret must have: the two §12
// names for a Config, which are the two ways a program prints a value it was
// not written to print -- fmt's Stringer and slog's LogValuer.
var secretRenderings = [...]string{"String", "LogValue"}

// localSecretRenderings returns, per package directory, the methods declared on
// a package-local type named Secret, or no entry when the directory declares
// no such type.
func (c *checker) localSecretRenderings() map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, f := range c.files {
		dir := filepath.ToSlash(filepath.Dir(f.rel))
		for _, d := range f.file.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				if d.Tok != token.TYPE {
					continue
				}
				for _, sp := range d.Specs {
					if ts, ok := sp.(*ast.TypeSpec); ok && ts.Name.Name == "Secret" && out[dir] == nil {
						out[dir] = map[string]bool{}
					}
				}
			case *ast.FuncDecl:
				if d.Recv == nil || len(d.Recv.List) != 1 || receiverName(d.Recv.List[0].Type) != "Secret" {
					continue
				}
				if out[dir] == nil {
					out[dir] = map[string]bool{}
				}
				out[dir][d.Name.Name] = true
			}
		}
	}
	return out
}

func (c *checker) checkCredentials() {
	checked, redacted := 0, 0
	local := c.localSecretRenderings()
	for _, f := range c.files {
		dir := filepath.ToSlash(filepath.Dir(f.rel))
		for _, d := range f.file.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, sp := range gd.Specs {
				ts, ok := sp.(*ast.TypeSpec)
				if !ok || !ts.Name.IsExported() {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok || !isConnectionStruct(ts.Name.Name, st) {
					continue
				}
				for _, fld := range st.Fields.List {
					typ := exprString(fld.Type)
					for _, nm := range fld.Names {
						if !nm.IsExported() || !credentialField.MatchString(nm.Name) {
							continue
						}
						if pathField.MatchString(nm.Name) {
							continue
						}
						if redactingTypes[typ] {
							checked++
							redacted++
							continue
						}
						if typ == "Secret" {
							checked++
							methods, declared := local[dir]
							var missing []string
							for _, m := range secretRenderings {
								if !methods[m] {
									missing = append(missing, m)
								}
							}
							if declared && len(missing) == 0 {
								redacted++
								continue
							}
							c.fail("credentials", dir+"."+ts.Name.Name+"."+nm.Name,
								"%s.%s.%s is a local Secret with no %s method in %s, so it renders the credential it "+
									"holds; a local redacting type needs both of %v (%s)",
								dir, ts.Name.Name, nm.Name, strings.Join(missing, " or "), dir, secretRenderings, f.rel)
							continue
						}
						// A bool or a numeric field named for a credential is
						// a flag about one, not the credential: "TokenSet",
						// "MaxTokens". Only a string-shaped field can carry
						// the bytes.
						if typ != "string" && typ != "[]byte" {
							continue
						}
						checked++
						c.fail("credentials", dir+"."+ts.Name.Name+"."+nm.Name,
							"%s.%s.%s is a %s; §12 requires a redacting wrapper type for a credential (%s)",
							dir, ts.Name.Name, nm.Name, typ, f.rel)
					}
				}
			}
		}
	}
	// The two counts differ by exactly the findings and the waivers: a field
	// is counted when it can hold the bytes (a redacting type, a local
	// Secret, a string or a []byte) and not when it is a flag about one. This
	// line used to print every credential-NAMED field as one that "uses a
	// redacting type", flags and waived plain strings included, so it said 26
	// beside two plain strings it had just waived.
	c.okf("%d of %d credential-named exported field(s) use a redacting type", redacted, checked)
}

// ---------------------------------------------------------------- insecure

// weakensTransport matches a field name whose effect is to weaken or remove
// transport security.
var weakensTransport = regexp.MustCompile(
	`(?i)(skipverify|plaintext|plainsasl|plainlogin|notls|disabletls|allowplain|nossl|disableverify)`,
)

func (c *checker) checkInsecure() {
	checked := 0
	for _, f := range c.files {
		dir := filepath.ToSlash(filepath.Dir(f.rel))
		for _, d := range f.file.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, sp := range gd.Specs {
				ts, ok := sp.(*ast.TypeSpec)
				if !ok || !ts.Name.IsExported() {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, fld := range st.Fields.List {
					for _, nm := range fld.Names {
						if !nm.IsExported() || !weakensTransport.MatchString(nm.Name) {
							continue
						}
						checked++
						if strings.Contains(strings.ToLower(nm.Name), "insecure") {
							continue
						}
						c.fail("insecure", dir+"."+ts.Name.Name+"."+nm.Name,
							"%s.%s.%s weakens transport security and does not say Insecure in its name (%s)",
							dir, ts.Name.Name, nm.Name, f.rel)
					}
				}
			}
		}
	}
	c.okf("%d transport-weakening option(s) carry Insecure in their name", checked)
}

// ---------------------------------------------------------------- supply chain

func (c *checker) checkSupplyChain() {
	c.checkCodeQLCoversEveryModule()
	c.checkVulnCoversEveryModule()
	c.checkProvenanceCoversEveryArchive()
	c.checkPatchedToolchain()
}

// goDirectiveRe reads a go.mod's `go` line.
var goDirectiveRe = regexp.MustCompile(`(?m)^go\s+(\S+)\s*$`)

// checkPatchedToolchain holds the two workflows whose answer depends on WHICH
// Go they run -- the vulnerability scan and the job that builds the shipped
// binaries -- to a toolchain at its latest patch release rather than the
// go.mod directive's floor.
//
// `go-version-file: go.mod` installs exactly the `go` line, and this
// repository's is `go 1.27.0`: the minimum it supports, and a toolchain with
// every standard-library fix since 1.27.0 missing. The first run of vuln.yml on
// GitHub installed it and reported 38 of 49 modules affected, 31 of them only
// by the standard library -- while the same scan on go1.27.1 reports six
// modules and no standard-library advisory at all. The release job installed
// the same floor, so the CLI binaries would have linked every one of those
// fixes' absences. The other workflows keep the floor on
// purpose: testing against the oldest supported Go is what they are for.
func (c *checker) checkPatchedToolchain() {
	floor := "the go.mod directive"
	if b, err := os.ReadFile(filepath.Join(c.root, "go.mod")); err == nil {
		if m := goDirectiveRe.FindSubmatch(b); m != nil {
			floor = "go " + string(m[1])
		}
	}
	vuln, err := c.workflow("vuln.yml")
	if err != nil {
		c.problems = append(c.problems, err.Error())
		return
	}
	release, err := c.workflow("release.yml")
	if err != nil {
		c.problems = append(c.problems, err.Error())
		return
	}
	targets := []struct{ subject, what, body string }{
		{"vuln.yml scans with the go.mod floor", "vuln.yml's scan", vuln},
		{"release.yml builds the CLI with the go.mod floor", "release.yml's goreleaser job", jobUsing(release, "goreleaser/goreleaser-action")},
	}
	ok := true
	for _, t := range targets {
		if t.body == "" {
			ok = false
			c.fail("supplychain", t.subject, "%s was not found, so which Go it runs cannot be checked", t.what)
			continue
		}
		if !hasKey(t.body, "go-version-file:") && hasKey(t.body, "check-latest: true") {
			continue
		}
		ok = false
		c.fail("supplychain", t.subject,
			"%s installs Go with go-version-file (or without check-latest: true), which is %s, the oldest "+
				"supported toolchain and none of the standard-library fixes after it; use `go-version: stable` "+
				"with `check-latest: true`", t.what, floor)
	}
	if ok {
		c.okf("vuln.yml's scan and release.yml's CLI build run Go at its latest patch release, not %s", floor)
	}
}

// hasKey reports whether a non-comment line of body contains key.
func hasKey(body, key string) bool {
	for line := range strings.SplitSeq(body, "\n") {
		t := strings.TrimSpace(line)
		if !strings.HasPrefix(t, "#") && strings.Contains(t, key) {
			return true
		}
	}
	return false
}

// jobUsing returns the text of the workflow job that contains marker, or ""
// when none does. A job is a two-space-indented key under `jobs:`.
func jobUsing(body, marker string) string {
	var jobs []string
	var cur strings.Builder
	in := false
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, "jobs:") {
			in = true
			continue
		}
		if !in {
			continue
		}
		if len(line) > 2 && line[:2] == "  " && line[2] != ' ' && line[2] != '#' && strings.HasSuffix(strings.TrimSpace(line), ":") {
			jobs = append(jobs, cur.String())
			cur.Reset()
		}
		cur.WriteString(line + "\n")
	}
	jobs = append(jobs, cur.String())
	for _, j := range jobs {
		if hasKey(j, marker) {
			return j
		}
	}
	return ""
}

const everyModuleHelper = "for-each-module.sh"

func (c *checker) checkCodeQLCoversEveryModule() {
	wf := "codeql.yml"
	body, err := c.workflow(wf)
	if err != nil {
		c.problems = append(c.problems, err.Error())
		return
	}
	if strings.Contains(body, everyModuleHelper) || strings.Contains(body, "make build") {
		c.okf("%s builds every module, so CodeQL sees all %d", wf, len(c.modules))
		return
	}
	c.fail("supplychain", "codeql.yml builds one module",
		"%s does not build every module: it runs a build in the repository root only, and the root module is "+
			"stdlib-only, so CodeQL analyses 1 of %d modules and none of the code that links a third-party client. "+
			"Use %s or `make build`.", wf, len(c.modules), everyModuleHelper)
}

func (c *checker) checkVulnCoversEveryModule() {
	wf := "vuln.yml"
	body, err := c.workflow(wf)
	if err != nil {
		c.problems = append(c.problems, err.Error())
		return
	}
	if strings.Contains(body, "make vuln") {
		c.okf("%s runs `make vuln`, which is govulncheck in every module", wf)
		return
	}
	c.fail("supplychain", "vuln.yml does not run make vuln",
		"%s must run `make vuln`, which is govulncheck in every module; a scan of the root module alone "+
			"finds nothing, because the root module has no dependencies", wf)
}

// archiveFormatRe finds the formats cmd/iqmq/.goreleaser.yaml produces,
// including the per-OS overrides.
var archiveFormatRe = regexp.MustCompile(`(?m)^\s*-\s*(tar\.gz|zip|binary|tar\.xz|tar\.zst|gz)\s*$`)

func (c *checker) checkProvenanceCoversEveryArchive() {
	rel := filepath.Join("cmd", "iqmq", ".goreleaser.yaml")
	gr, err := os.ReadFile(filepath.Join(c.root, rel))
	if err != nil {
		// No goreleaser configuration is no CLI arm and nothing to attest.
		c.okf("no %s, so there is no CLI artifact to attest", filepath.ToSlash(rel))
		return
	}
	body, err := c.workflow("release.yml")
	if err != nil {
		c.problems = append(c.problems, err.Error())
		return
	}
	formats := map[string]bool{}
	for _, m := range archiveFormatRe.FindAllStringSubmatch(string(gr), -1) {
		formats[m[1]] = true
	}
	if len(formats) == 0 {
		c.problems = append(c.problems, "supplychain: no archive format found in "+filepath.ToSlash(rel)+
			"; the provenance check would be vacuous")
		return
	}
	subjects := provenanceSubjects(body)
	if len(subjects) == 0 {
		c.fail("supplychain", "release.yml has no provenance subjects",
			"release.yml has no attest-build-provenance step with a subject-path, so nothing .goreleaser.yaml "+
				"produces (%s) would ship with an SLSA attestation", joinSorted(formats))
		return
	}
	missing := make([]string, 0, len(formats))
	for f := range formats {
		if f == "binary" {
			continue // not an archive; nothing named after it is produced
		}
		named := false
		for _, s := range subjects {
			if strings.HasSuffix(s, "*."+f) {
				named = true
			}
		}
		if !named {
			missing = append(missing, f)
		}
	}
	sort.Strings(missing)
	if len(missing) == 0 {
		c.okf("release.yml attests every archive format .goreleaser.yaml produces (%s)", joinSorted(formats))
		return
	}
	c.fail("supplychain", "release.yml provenance misses "+strings.Join(missing, ","),
		"cmd/iqmq/.goreleaser.yaml produces %s and release.yml's provenance subject-path names no %s, "+
			"so those artifacts ship with no SLSA attestation", joinSorted(formats),
		strings.Join(missing, "/"))
}

// subjectPathRe finds an attest-build-provenance step's subject-path key and
// the indentation it sits at.
var subjectPathRe = regexp.MustCompile(`^(\s*)subject-path:\s*(.*)$`)

// provenanceSubjects returns the paths a workflow's subject-path keys name,
// comments and blank lines dropped: a block scalar's lines (`subject-path: |`
// and everything indented deeper) or an inline value.
//
// The check used to ask whether "*.zip" appeared ANYWHERE in release.yml, so a
// comment explaining that the zips were missing, or an `ls dist/*.zip` in an
// unrelated step, satisfied it -- the self-test's failing fixture now carries
// both and was watched passing the old check. The subject
// list is the only place the name attests anything.
func provenanceSubjects(body string) []string {
	var out []string
	lines := strings.Split(body, "\n")
	for i := 0; i < len(lines); i++ {
		m := subjectPathRe.FindStringSubmatch(lines[i])
		if m == nil || strings.HasPrefix(strings.TrimSpace(lines[i]), "#") {
			continue
		}
		indent, value := len(m[1]), strings.TrimSpace(m[2])
		if value != "|" && value != "|-" && value != ">" && value != "" {
			if v := strings.Trim(value, `"'`); !strings.HasPrefix(v, "#") {
				out = append(out, v)
			}
			continue
		}
		for i+1 < len(lines) {
			next := lines[i+1]
			trimmed := strings.TrimSpace(next)
			if trimmed != "" && len(next)-len(strings.TrimLeft(next, " ")) <= indent {
				break
			}
			i++
			if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
				out = append(out, trimmed)
			}
		}
	}
	return out
}

func (c *checker) workflow(name string) (string, error) {
	p := filepath.Join(c.root, ".github", "workflows", name)
	b, err := os.ReadFile(p)
	if err != nil {
		return "", fmt.Errorf(".github/workflows/%s is missing, so its half of §12's supply chain is unchecked", name)
	}
	return string(b), nil
}

// ---------------------------------------------------------------- helpers

func receiverName(e ast.Expr) string {
	if s, ok := e.(*ast.StarExpr); ok {
		return receiverName(s.X)
	}
	if i, ok := e.(*ast.Ident); ok {
		return i.Name
	}
	if ix, ok := e.(*ast.IndexExpr); ok { // a generic receiver
		return receiverName(ix.X)
	}
	return ""
}

func exprString(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return exprString(t.X) + "." + t.Sel.Name
	case *ast.StarExpr:
		return "*" + exprString(t.X)
	case *ast.ArrayType:
		if t.Len == nil {
			return "[]" + exprString(t.Elt)
		}
		return "[...]" + exprString(t.Elt)
	case *ast.MapType:
		return "map[" + exprString(t.Key) + "]" + exprString(t.Value)
	default:
		return ""
	}
}

func joinSorted(set map[string]bool) string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}
