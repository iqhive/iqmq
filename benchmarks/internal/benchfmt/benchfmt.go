// Package benchfmt parses the output of go test -bench.
package benchfmt

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// Result is one benchmark line.
type Result struct {
	// Name is the full benchmark name including sub-benchmarks and the
	// -procs suffix, e.g. "BenchmarkMemory/Queue/mpmc/MPMC/P=4-8".
	Name string
	// Procs is the GOMAXPROCS suffix, or 1 when absent.
	Procs int
	// Iters is the iteration count.
	Iters int64
	// Values maps unit to value, e.g. "ns/op" -> 123.4, "allocs/op" -> 0.
	Values map[string]float64
	// Units lists the units in the order they appeared.
	Units []string
}

// Base returns the name without the -procs suffix.
//
// A measurement's identity is the base name AND the rung it was taken at:
// the full profile measures at -cpu=1,4,8,32 and the four rungs of one row
// are four different configurations. Anything that aggregates samples must
// therefore key on both (see Result.Row).
func (r Result) Base() string { return Base(r.Name) }

// Row returns the measurement's identity: its base name and the GOMAXPROCS
// rung it was taken at. Pooling the rungs of one row produces a median that
// describes no configuration, which is what the "GOMAXPROCS=1,2,4,8,max" sweep exists to tell apart.
func (r Result) Row() (base string, procs int) { return Base(r.Name), r.Procs }

// Base strips a trailing -N procs suffix from a benchmark name.
func Base(name string) string {
	i := strings.LastIndexByte(name, '-')
	if i < 0 {
		return name
	}
	if _, err := strconv.Atoi(name[i+1:]); err != nil {
		return name
	}
	return name[:i]
}

// Dims splits a base name on "/".
func Dims(name string) []string { return strings.Split(Base(name), "/") }

// Dim returns the value of a "key=value" segment of the name.
func Dim(name, key string) (string, bool) {
	for _, seg := range Dims(name) {
		if k, v, ok := strings.Cut(seg, "="); ok && k == key {
			return v, true
		}
	}
	return "", false
}

// Parse reads benchmark lines from r, ignoring anything else.
func Parse(r io.Reader) ([]Result, error) {
	var out []Result
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		res, ok := parseLine(line)
		if ok {
			out = append(out, res)
		}
	}
	return out, sc.Err()
}

// ParseBytes is Parse over a byte slice.
func ParseBytes(b []byte) ([]Result, error) { return Parse(bytes.NewReader(b)) }

func parseLine(line string) (Result, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return Result{}, false
	}
	iters, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return Result{}, false
	}
	res := Result{Name: fields[0], Iters: iters, Values: map[string]float64{}, Procs: 1}
	if i := strings.LastIndexByte(res.Name, '-'); i >= 0 {
		if p, err := strconv.Atoi(res.Name[i+1:]); err == nil {
			res.Procs = p
		}
	}
	for i := 2; i+1 < len(fields); i += 2 {
		v, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return Result{}, false
		}
		unit := fields[i+1]
		if _, dup := res.Values[unit]; !dup {
			res.Units = append(res.Units, unit)
		}
		res.Values[unit] = v
	}
	return res, true
}

// Group collects the samples of one benchmark name.
type Group struct {
	Name    string
	Samples map[string][]float64 // unit -> samples in input order
	Units   []string
}

// Median returns the median sample of unit and whether any sample exists.
func (g *Group) Median(unit string) (float64, bool) {
	s := g.Samples[unit]
	if len(s) == 0 {
		return 0, false
	}
	c := append([]float64(nil), s...)
	sort.Float64s(c)
	n := len(c)
	if n%2 == 1 {
		return c[n/2], true
	}
	return (c[n/2-1] + c[n/2]) / 2, true
}

// GroupByName groups results by full name, preserving first-seen order.
func GroupByName(rs []Result) []*Group {
	index := map[string]*Group{}
	var out []*Group
	for _, r := range rs {
		g, ok := index[r.Name]
		if !ok {
			g = &Group{Name: r.Name, Samples: map[string][]float64{}}
			index[r.Name] = g
			out = append(out, g)
		}
		for _, u := range r.Units {
			if _, seen := g.Samples[u]; !seen {
				g.Units = append(g.Units, u)
			}
			g.Samples[u] = append(g.Samples[u], r.Values[u])
		}
	}
	return out
}

// Format renders a Result as a benchmark line, the inverse of parseLine.
func Format(r Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\t%d", r.Name, r.Iters)
	for _, u := range r.Units {
		fmt.Fprintf(&b, "\t%s %s", strconv.FormatFloat(r.Values[u], 'f', -1, 64), u)
	}
	return b.String()
}

// ErrNoResults is returned by helpers that require at least one result.
var ErrNoResults = errors.New("benchfmt: no benchmark results")
