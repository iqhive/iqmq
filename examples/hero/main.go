// Command hero supplies project demonstrations to the shared banner CLI.
package main

//go:generate go run . -svg ../../docs/hero.svg
import (
	"io"
	"os"

	"github.com/iqhive/banner"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
func run(args []string, stdout, stderr io.Writer) int {
	return banner.RunCLI(args, stdout, stderr, animation)
}
