// Command bakeoff supports the variant bake-off programme of specification
// §0.3 and §9.4.
//
//	bakeoff lint [-root DIR] [packages...]
//	    Checks that every variant kind of each family package appears in the
//	    model test (TestModel/<kind>) and in at least one benchmark name.
//	    Family packages announce their kinds through a TestKindsList test
//	    that logs "kinds: slug=Ident,slug=Ident,...".
//
//	    It also holds every hack/bakeoff/<id>.env declaration to what the
//	    campaign needs of it: its BENCH root must select at least one
//	    benchmark in its PKG, and every kind its KINDS_TEST announces must
//	    have a benchmark the per-kind filter selects. Without a package list
//	    every declaration is checked; with one, only the families of the
//	    packages it names.
//
//	bakeoff gate-pattern NAME [-budgets FILE]
//	    Prints a -bench regular expression selecting the named gate set.
//
//	bakeoff modpkg PKG [-root DIR]
//	    Prints "<module dir>\t<package inside that module>" for a
//	    repository-relative package path, which is what a script that has to
//	    `cd` and `go test` a gated package in a nested module needs.
//
//	bakeoff decide -family ID -results FILE[,FILE] -pkg DIR -bench ROOT [flags]
//	    Applies the default-selection rule to benchmark results, writes the
//	    run's scorecard and the family's stable scorecard, writes the variant
//	    report and regenerates the family's auto.go binding. It renders no
//	    ADR: an ADR number is shared by many families.
//
//	bakeoff adr [-adr N] [-dry-run] [flags]
//	    Renders one ADR file per ADR number from every per-family scorecard
//	    belonging to it, attaching the generated material to a hand-written
//	    ADR between markers rather than writing a second file for the number.
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "lint":
		err = lint(os.Args[2:])
	case "decide":
		err = decide(os.Args[2:])
	case "adr":
		err = adrCmd(os.Args[2:])
	case "gate-pattern":
		err = gatePattern(os.Args[2:])
	case "modpkg":
		err = modPkg(os.Args[2:])
	case "apiscore":
		err = apiscore(os.Args[2:])
	default:
		usage()
	}
	os.Exit(report(os.Stderr, err))
}

// report writes a subcommand's outcome and returns the process's exit status.
//
// Two things it does that the first version did not. Every
// failure carries the `!!` marker, because "green" for this repository means a
// `make ci` log with no `FAIL` and no `!!` line in it, and a failure that
// carries neither is a red target that reads as a green log. And a subcommand
// that was ENDED rather than answered exits 2 -- hack/benchgate.sh's "nothing
// could be measured" status -- rather than 1, because it is not a finding
// about the tree.
//
// The status is for a direct caller. `make bench-lint` runs this through
// `go run`, which flattens any non-zero status to 1 and prints "exit status
// N", and `make` flattens that again to its own 2 -- so in a `make ci` log the
// `!!` marker is the whole signal, which is why every failure carries it.
func report(w io.Writer, err error) int {
	if err == nil {
		return 0
	}
	if aborted(err) {
		_, _ = fmt.Fprintln(w, "!! bakeoff:", err)
		_, _ = fmt.Fprintln(w, "!! bakeoff: this check DID NOT RUN and proved nothing; re-run it on a host where nothing kills its children")
		return 2
	}
	_, _ = fmt.Fprintln(w, "!! bakeoff:", err)
	return 1
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bakeoff (lint|decide|adr|gate-pattern|modpkg) [flags]")
	os.Exit(2)
}
