package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// apiscoreTree writes a repository-shaped temp dir: a scorecard and the env
// files the rows are checked against.
func apiscoreTree(t *testing.T, mutate func(*storedScorecard)) (root, card string) {
	t.Helper()
	root = t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "hack", "bakeoff"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "reports"), 0o750); err != nil {
		t.Fatal(err)
	}
	var sc storedScorecard
	for _, r := range apiRequired {
		row := struct {
			ID      string `json:"id"`
			Title   string `json:"title"`
			Package string `json:"package"`
			Shapes  []struct {
				Shape   string `json:"shape"`
				Metrics []struct {
					Name  string  `json:"name"`
					Value float64 `json:"value"`
					Unit  string  `json:"unit"`
				} `json:"metrics"`
			} `json:"shapes"`
		}{ID: r.ID, Title: r.Title, Package: "x/apibench/" + strings.ToLower(r.ID)}
		type metric = struct {
			Name  string  `json:"name"`
			Value float64 `json:"value"`
			Unit  string  `json:"unit"`
		}
		one := struct {
			Shape   string   `json:"shape"`
			Metrics []metric `json:"metrics"`
		}{Shape: "first", Metrics: []metric{{Name: "delivery.safe.allocs", Value: 1, Unit: "allocs/msg"}}}
		for _, axis := range r.Axes {
			one.Metrics = append(one.Metrics, metric{Name: axis + ".detected", Value: 4, Unit: "cases"})
		}
		row.Shapes = append(row.Shapes, one)
		sc.Rows = append(sc.Rows, row)
		mustWrite(t, filepath.Join(root, "hack", "bakeoff", r.ID+".env"),
			"PKG=./x/apibench/"+strings.ToLower(r.ID)+"\nBENCH=BenchmarkAPIBench\nREF=first\nAUTO_KEY="+strings.ToLower(r.ID)+"\n")
	}
	if mutate != nil {
		mutate(&sc)
	}
	data, err := json.MarshalIndent(sc, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	card = filepath.Join("reports", "card.json")
	mustWrite(t, filepath.Join(root, card), string(data)+"\n")
	return root, card
}

// A check over a stored file is exactly the shape that passes for ever if
// nobody watches it fail, and the axis half is the one that matters: §0.3
// weights misuse resistance at 25 points, and the whole axis once went
// missing from this document while its predecessor stayed green on "at least
// one result per row".
func TestAPIScoreHoldsTheStoredScorecardToTheGateThatUsedToRunTheCode(t *testing.T) {
	root, card := apiscoreTree(t, nil)
	if err := apiscore([]string{"-root", root, "-card", card, "-check"}); err != nil {
		t.Fatalf("a complete scorecard was refused: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*storedScorecard)
		want   string
	}{
		{
			name: "a required row has no results",
			mutate: func(sc *storedScorecard) {
				sc.Rows = sc.Rows[1:]
			},
			want: "row B1 (Received-message shape) has no results at all",
		},
		{
			name: "a row that claims the misuse axis carries no misuse metric",
			mutate: func(sc *storedScorecard) {
				for i := range sc.Rows {
					if sc.Rows[i].ID != "B9" {
						continue
					}
					for j := range sc.Rows[i].Shapes {
						kept := sc.Rows[i].Shapes[j].Metrics[:0]
						for _, m := range sc.Rows[i].Shapes[j].Metrics {
							if !strings.HasPrefix(m.Name, "misuse.") {
								kept = append(kept, m)
							}
						}
						sc.Rows[i].Shapes[j].Metrics = kept
					}
				}
			},
			want: "row B9 claims the misuse axis and the scorecard carries no misuse.* metric",
		},
		{
			name: "a row has no shapes",
			mutate: func(sc *storedScorecard) {
				sc.Rows[0].Shapes = nil
			},
			want: "row B1 (Received-message shape) has no shapes",
		},
		{
			name: "a shape has no metrics",
			mutate: func(sc *storedScorecard) {
				sc.Rows[0].Shapes[0].Metrics = nil
			},
			want: "row B1 shape first has no metrics",
		},
		{
			name: "a row nothing declares",
			mutate: func(sc *storedScorecard) {
				sc.Rows[0].ID = "B99"
			},
			want: "row B99 is in the scorecard and hack/bakeoff/B99.env does not exist",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root, card := apiscoreTree(t, c.mutate)
			err := apiscore([]string{"-root", root, "-card", card, "-check"})
			if err == nil {
				t.Fatal("the doctored scorecard was accepted")
			}
			// The per-problem lines go to stderr; the returned error carries
			// the count, so the message is asserted by re-running with the
			// problem list captured.
			if !strings.Contains(captureStderr(t, func() {
				_ = apiscore([]string{"-root", root, "-card", card, "-check"})
			}), c.want) {
				t.Errorf("the refusal does not say %q", c.want)
			}
		})
	}

	// And an absent document is a refusal rather than an empty pass, which is
	// the failure mode a check over a committed file has and one over a test
	// run does not.
	root2, _ := apiscoreTree(t, nil)
	if err := apiscore([]string{"-root", root2, "-card", "reports/gone.json", "-check"}); err == nil {
		t.Error("a missing scorecard was accepted")
	}
}

// captureStderr runs fn with os.Stderr replaced and returns what it wrote.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	fn()
	_ = w.Close()
	os.Stderr = old
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, rerr := r.Read(buf)
		b.Write(buf[:n])
		if rerr != nil {
			break
		}
	}
	return b.String()
}
