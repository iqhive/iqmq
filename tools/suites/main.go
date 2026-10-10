package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	root := flag.String("root", ".", "repository root")
	check := flag.Bool("check", false, "hold every entry of the manifest to the repository")
	list := flag.Bool("list", false, "print the suites a mode and a tier select")
	asJSON := flag.Bool("json", false, "print the list as a JSON array, for a CI matrix")
	modes := flag.Bool("modes", false, "print the run-mode registry")
	resolve := flag.String("resolve", "", "print where one suite's run of -mode lives")
	mode := flag.String("mode", "", "run mode: integration, critical, matrix, inprocess")
	tier := flag.Int("tier", 0, "§2.7 maintenance tier, 0 for every tier")
	flag.Parse()

	if err := run(*root, *check, *list, *asJSON, *modes, *resolve, *mode, *tier); err != nil {
		fmt.Fprintln(os.Stderr, "suites:", err)
		os.Exit(1)
	}
}

// run is main with the flags read, so that an error is one return rather than
// an exit from the middle of a function.
func run(root string, check, list, asJSON, modes bool, resolve, mode string, tier int) error {
	if modes {
		printModes()
		return nil
	}
	m, err := Load(root)
	if err != nil {
		return err
	}
	if mode != "" {
		if _, ok := LookupMode(mode); !ok {
			return fmt.Errorf("%q is not a run mode; the modes are %s", mode, strings.Join(modeNames(), ", "))
		}
	}
	switch {
	case check:
		return runCheck(m, root)
	case resolve != "":
		if mode == "" {
			return errors.New("-resolve needs -mode")
		}
		res, err := m.Resolve(resolve, mode)
		if err != nil {
			return err
		}
		// One key=value per line, for `eval` in hack/conformance.sh.
		fmt.Printf("dir=%s\n", res.Dir)
		fmt.Printf("pattern=%s\n", res.Pattern)
		fmt.Printf("tags=%s\n", res.Tags)
		fmt.Printf("timeout=%s\n", res.Timeout)
		return nil
	case list:
		return runList(m, mode, tier, asJSON)
	}
	flag.Usage()
	return errors.New("nothing to do: pass -check, -list, -modes or -resolve")
}

// runCheck prints every problem and fails when there is one.
func runCheck(m *Manifest, root string) error {
	errs := m.Validate(root)
	for _, err := range errs {
		fmt.Fprintln(os.Stderr, "  "+err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("%d problem(s) in %s", len(errs), ManifestPath)
	}
	impl := 0
	for _, md := range Modes() {
		if md.Implemented {
			impl++
		}
	}
	fmt.Printf("suites: ok, %d suite(s), %d of %d run modes implemented\n", len(m.Suites), impl, len(Modes()))
	return nil
}

// runList prints the selected suites, one per line or as a JSON array.
func runList(m *Manifest, mode string, tier int, asJSON bool) error {
	sel := m.Select(mode, tier)
	names := make([]string, 0, len(sel))
	for i := range sel {
		names = append(names, sel[i].Name)
	}
	if !asJSON {
		for _, n := range names {
			fmt.Println(n)
		}
		return nil
	}
	// An empty selection has to render as [] rather than null: a CI matrix
	// built from fromJSON(null) is an error, and one built from [] is an
	// honestly empty matrix.
	if names == nil {
		names = []string{}
	}
	out, err := json.Marshal(names)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// printModes prints the registry, which is the answer to "why does `make
// chaos` do nothing?".
func printModes() {
	for _, md := range Modes() {
		state := "MISSING    "
		if md.Implemented {
			state = "implemented"
		}
		dir := md.Dir
		if dir == "" {
			dir = "<the suite's own package>"
		}
		fmt.Printf("%-13s %s  %s %s*\n", md.Name, state, dir, md.Prefix)
		fmt.Printf("%-13s %s\n", "", md.Note)
	}
}
