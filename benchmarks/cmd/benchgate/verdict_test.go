package main

import (
	"strings"
	"testing"
)

// The verdict's RENDERING had no test, which is how a counted regression came
// to be invisible in the table that reports it.
//
// #85's gate printed `Regressions: 1` over a table whose 51 rows all read `ok`
// or `waived`, and it took six attempts to work out which row it meant. Three
// things hid it at once: a `scope: time` waiver drops the ns/op and msgs/s
// halves and deliberately KEEPS the exact counts , so the
// surviving regression was real and counted; the waiver text REPLACED the
// row's verdict instead of joining it; and the metric was `B/op`, which the
// table has no column for. A gate that fails a pull request without naming the
// row is past crying wolf and into being ignored.
func TestWaivedRowStillNamesASurvivingRegression(t *testing.T) {
	for _, tc := range []struct {
		label     string
		row       row
		wantIn    []string
		wantNotIn []string
	}{
		{
			// The shape that failed #85: waived for its clock, regressed on a
			// metric the waiver does not cover and the table does not show.
			label: "a time-waived row with a surviving B/op regression",
			row: row{
				name:        "BenchmarkMemory/PubSubAck/safe/1P1C/bytes=64/reuse=on-16",
				inGate:      true,
				waived:      "ADR-017 assumption A25.16",
				waivedTime:  true,
				regressions: []string{"B/op +7.4% (p=0.002)"},
			},
			wantIn:    []string{"waived", "A25.16", "B/op +7.4%"},
			wantNotIn: nil,
		},
		{
			// Allocations survive a time waiver too, and are the half §10.2
			// relies on this gate to bind.
			label: "a time-waived row with a surviving allocation regression",
			row: row{
				name:        "BenchmarkMemory/PubSubAck/owned/1P1C/bytes=64/reuse=on-16",
				inGate:      true,
				waived:      "ADR-017 assumption A25.16",
				waivedTime:  true,
				regressions: []string{"allocs/op 0 -> 1"},
			},
			wantIn:    []string{"waived", "allocs/op 0 -> 1"},
			wantNotIn: nil,
		},
		{
			// A whole-scope waiver really does clear the comparison, so there
			// is nothing to add and the text must stay clean. Without this case
			// the fix above could have appended an empty "BUT STILL" to every
			// waived row in the table.
			label: "a wholly waived row with nothing surviving says only that",
			row: row{
				name:      "BenchmarkMemory/FanOut/16/arch=persubenvelopes/policy=atomic/slow=0-16",
				inGate:    true,
				waived:    "ADR-017 assumption A25.13",
				waivedAll: true,
			},
			wantIn:    []string{"waived", "A25.13"},
			wantNotIn: []string{"BUT STILL", "**"},
		},
	} {
		t.Run(tc.label, func(t *testing.T) {
			v := &verdict{gate: "pr", thresholdPct: 3, rows: []row{tc.row}}
			md := v.markdown("")
			line := rowLine(t, md, tc.row.name)
			for _, want := range tc.wantIn {
				if !strings.Contains(line, want) {
					t.Errorf("the row's verdict does not mention %q, so a reader of a red gate cannot act on it:\n  %s", want, line)
				}
			}
			for _, not := range tc.wantNotIn {
				if strings.Contains(line, not) {
					t.Errorf("the row's verdict contains %q and should not:\n  %s", not, line)
				}
			}
		})
	}
}

// A regression on an UNWAIVED row must still be stated plainly, without the
// waiver vocabulary leaking into it.
func TestUnwaivedRegressionIsStatedPlainly(t *testing.T) {
	v := &verdict{gate: "pr", thresholdPct: 3, rows: []row{{
		name:        "BenchmarkWire/fixed/Encode/1KiB/headers=0-16",
		inGate:      true,
		regressions: []string{"ns/op +5.2% (net of CI +3.1%, p=0.001)"},
	}}}
	line := rowLine(t, v.markdown(""), "BenchmarkWire/fixed/Encode/1KiB/headers=0-16")
	if !strings.Contains(line, "ns/op +5.2%") {
		t.Errorf("the regression is not named in the row:\n  %s", line)
	}
	if strings.Contains(line, "waived") {
		t.Errorf("an unwaived row's verdict mentions a waiver:\n  %s", line)
	}
}

// The summary's count and the table have to agree, which is the property #85
// violated: a reader who sees a non-zero count must be able to find the rows it
// counted. This asserts the pair rather than either half.
func TestRegressionCountIsVisibleInTheTable(t *testing.T) {
	v := &verdict{gate: "pr", thresholdPct: 3, rows: []row{
		{
			name: "BenchA-16", inGate: true,
			regressions: []string{"B/op +7.4% (p=0.002)"},
			waived:      "ADR-017 assumption A25.16", waivedTime: true,
		},
		{name: "BenchB-16", inGate: true},
	}}
	for i := range v.rows {
		v.regressions += len(v.rows[i].regressions)
	}
	md := v.markdown("")
	if !strings.Contains(md, "Regressions: 1") {
		t.Fatalf("the summary does not report the one regression:\n%s", md)
	}
	// Exactly as many rows must carry a bolded note as the summary counted.
	bolded := 0
	for _, line := range strings.Split(md, "\n") {
		if strings.HasPrefix(line, "| Bench") && strings.Contains(line, "**") {
			bolded++
		}
	}
	if bolded != v.regressions {
		t.Errorf("the summary counted %d regression(s) and the table bolds %d row(s); a count nobody can trace is what made this test necessary",
			v.regressions, bolded)
	}
}

// rowLine returns the verdict table's line for one benchmark.
func rowLine(t *testing.T, md, name string) string {
	t.Helper()
	for _, line := range strings.Split(md, "\n") {
		if strings.HasPrefix(line, "| "+name+" ") {
			return line
		}
	}
	t.Fatalf("no table row for %q in:\n%s", name, md)
	return ""
}
