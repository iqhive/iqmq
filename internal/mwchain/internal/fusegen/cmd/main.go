// Command fusegen writes mwchain's generated fused compositions.
//
// It is run by go:generate from internal/mwchain:
//
//	go run ./internal/fusegen/cmd -o fused_gen.go
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/iqhive/iqmq/internal/mwchain/internal/fusegen"
)

func main() {
	out := flag.String("o", "fused_gen.go", "output file")
	max := flag.Int("max", fusegen.Max, "longest generated chain")
	flag.Parse()
	src, err := fusegen.Source(*max)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, src, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "fusegen:", err)
		os.Exit(1)
	}
}
