package subject

import (
	"strconv"
	"strings"
	"testing"
)

// BenchmarkSubject/Match/tokens=<n>/wild=<none|one|tail>/compiled=<bool>
// measures a successful match; Compile measures pattern compilation.
func BenchmarkSubject(b *testing.B) {
	for _, n := range []int{1, 4, 8} {
		subject := makeSubject(n)
		for _, wild := range []string{"none", "one", "tail"} {
			pattern := makePattern(n, wild)
			p := MustCompile(pattern)
			if !p.Match(subject) {
				b.Fatalf("%q does not match %q", pattern, subject)
			}
			dims := "tokens=" + strconv.Itoa(n) + "/wild=" + wild
			b.Run("Match/"+dims+"/compiled=true", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if !p.Match(subject) {
						b.Fatal("no match")
					}
				}
			})
			b.Run("Match/"+dims+"/compiled=false", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if !Match(pattern, subject) {
						b.Fatal("no match")
					}
				}
			})
		}
		pattern := makePattern(n, "one")
		b.Run("Compile/tokens="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Compile(pattern); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	miss := MustCompile("orders.eu.*.created")
	b.Run("Match/tokens=4/wild=one/miss=true", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if miss.Match("orders.us.x.created") {
				b.Fatal("unexpected match")
			}
		}
	})
}

func makeSubject(n int) string {
	toks := make([]string, n)
	for i := range toks {
		toks[i] = "token" + strconv.Itoa(i)
	}
	return strings.Join(toks, ".")
}

func makePattern(n int, wild string) string {
	toks := strings.Split(makeSubject(n), ".")
	switch wild {
	case "one":
		toks[n/2] = "*"
	case "tail":
		toks[n-1] = ">"
	}
	return strings.Join(toks, ".")
}
