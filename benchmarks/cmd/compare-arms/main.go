// Command compare-arms prints the -bench pattern of every arm of one
// competitor family, in measurement order, one per line.
//
//	compare-arms nats
//
// It exists so that hack/bench/run-compare.sh does not carry a second copy of
// the arm list. The arms are declared once, in benchmarks/competitors, and the
// script that interleaves them asks. A shell array of patterns beside a Go
// slice of arms is the drift this repository has already paid for in the
// pairing between a benchmark and its baseline.
package main

import (
	"fmt"
	"os"

	"github.com/iqhive/iqmq/benchmarks/competitors"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: compare-arms <family>")
		os.Exit(2)
	}
	family := os.Args[1]
	arms := competitors.ArmsFor(family)
	if len(arms) == 0 {
		fmt.Fprintf(os.Stderr, "compare-arms: no arm declared for family %q\n", family)
		os.Exit(2)
	}
	for _, a := range arms {
		fmt.Printf("^BenchmarkCompare/%s/%s/\n", a.Library, a.Family)
	}
}
