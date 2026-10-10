package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The §9.2 scorecard check, re-homed.
//
//   - delete it with the code, which loses §0.3's misuse-resistance and DX
//     axes for all fourteen rows -- 25 and 25 of its 100 points -- from a
//     repository whose API is frozen on them;
//   - keep the command and let it find nothing, which is a check that passes
//     for ever;
//   - COMMIT the document the scorer produced and check THAT, which is what
//     this is.
//
// `benchmarks/reports/apibench-scorecard.json` was written by the last tree
// that could produce it (`scoring -root . -out`), 14 rows / 43 shapes / 189
// metrics, and is the evidence for six ADRs. The checks below are the old
// `Scorecard.Check`'s two halves over the stored document -- every required
// row has at least one result, and every row that claims a scoring axis
// carries a metric for it -- plus the two the old one could not ask: that the
// document is not empty of numbers, and that every row it covers is a row the
// repository still declares.
//
// The axes half is load-bearing and is why the old check had it: §0.3 weights
// misuse resistance at 25 points, the lines were emitted from TestMisuse, the
// collector ran only TestScore, and no misuse metric reached the document at
// all while the gate stayed green on "at least one result".

// apiRow is one §9.2 row the gate requires evidence for.
//
// The misuse axis is claimed by the six rows that ran a misuse suite: the
// three delivery-side rows whose subject is one settlement (B1, B3, B9) and
// the three surface-side rows whose subject is a call site a caller can get
// wrong (B5, B7, B12). The other rows deliberately have none; claiming the
// axis for them would fail the gate on evidence nobody intended to produce,
// and leaving it unclaimed everywhere is how the axis went missing.
type apiRow struct {
	ID    string
	Title string
	Axes  []string
}

var apiRequired = []apiRow{
	{ID: "B1", Title: "Received-message shape", Axes: []string{"misuse"}},
	{ID: "B2", Title: "Settlement representation"},
	{ID: "B3", Title: "Message key type", Axes: []string{"misuse"}},
	{ID: "B4", Title: "Headers representation"},
	{ID: "B5", Title: "Publish options", Axes: []string{"misuse"}},
	{ID: "B6", Title: "Top-level types"},
	{ID: "B7", Title: "Capability representation", Axes: []string{"misuse"}},
	{ID: "B8", Title: "Codec interface"},
	{ID: "B9", Title: "Receive storage and iteration", Axes: []string{"misuse"}},
	{ID: "B10", Title: "Error construction"},
	{ID: "B11", Title: "Lifecycle"},
	{ID: "B12", Title: "Consumer construction", Axes: []string{"misuse"}},
	{ID: "A36", Title: "Batch SPI contract"},
	{ID: "A37", Title: "Publisher routing"},
}

// storedScorecard is the committed document's shape, which is what
// x/apibench/scoring wrote.
type storedScorecard struct {
	Rows []struct {
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
	} `json:"rows"`
}

func apiscore(args []string) error {
	fs := flag.NewFlagSet("apiscore", flag.ContinueOnError)
	root := fs.String("root", "..", "repository root")
	path := fs.String("card", "benchmarks/reports/apibench-scorecard.json", "committed §9.2 scorecard, relative to -root")
	check := fs.Bool("check", false, "fail when a required row has no result or a claimed axis no metric")
	if err := fs.Parse(args); err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(*root, *path))
	if err != nil {
		return fmt.Errorf("apiscore: %w", err)
	}
	var card storedScorecard
	if err := json.Unmarshal(data, &card); err != nil {
		return fmt.Errorf("apiscore: %s: %w", *path, err)
	}

	byID := map[string]int{}
	metrics, shapes := 0, 0
	var problems []string
	for i, r := range card.Rows {
		byID[r.ID] = i
		shapes += len(r.Shapes)
		for _, sh := range r.Shapes {
			metrics += len(sh.Metrics)
		}
	}
	// A row in the document that the repository does not declare is a row
	// nobody can trace back to a family, which is the stale half of the
	// invariant assumption C01.1 established for the per-family scorecards.
	declared := map[string]bool{}
	fams, ferr := readBakeoffEnvs(*root)
	if ferr != nil {
		return ferr
	}
	for i := range fams {
		declared[fams[i].id] = true
	}
	for _, r := range card.Rows {
		if !declared[r.ID] {
			problems = append(problems, fmt.Sprintf("row %s is in the scorecard and hack/bakeoff/%s.env does not exist", r.ID, r.ID))
		}
	}
	for _, want := range apiRequired {
		i, ok := byID[want.ID]
		if !ok {
			problems = append(problems, fmt.Sprintf("row %s (%s) has no results at all", want.ID, want.Title))
			continue
		}
		r := card.Rows[i]
		if len(r.Shapes) == 0 {
			problems = append(problems, fmt.Sprintf("row %s (%s) has no shapes", want.ID, want.Title))
			continue
		}
		for _, sh := range r.Shapes {
			if len(sh.Metrics) == 0 {
				problems = append(problems, fmt.Sprintf("row %s shape %s has no metrics", want.ID, sh.Shape))
			}
		}
		for _, axis := range want.Axes {
			found := false
			for _, sh := range r.Shapes {
				for _, m := range sh.Metrics {
					if strings.HasPrefix(m.Name, axis+".") {
						found = true
					}
				}
			}
			if !found {
				problems = append(problems, fmt.Sprintf("row %s claims the %s axis and the scorecard carries no %s.* metric for it",
					want.ID, axis, axis))
			}
		}
	}
	if metrics == 0 {
		problems = append(problems, "the scorecard holds no numbers at all")
	}
	if len(problems) > 0 && *check {
		sort.Strings(problems)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, " -", p)
		}
		return fmt.Errorf("apiscore: %d problem(s) in %s", len(problems), *path)
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, " -", p)
		}
		return errors.New("apiscore: problems above")
	}
	fmt.Fprintf(os.Stderr, "apiscore: ok, %d row(s), %d shape(s), %d metric(s) in %s; %d required row(s), %d claimed axis/axes\n",
		len(card.Rows), shapes, metrics, *path, len(apiRequired), claimedAxes())
	return nil
}

// claimedAxes counts the axis claims the table makes, so the summary line
// says how much of §0.3's scoring the document is being held to.
func claimedAxes() int {
	n := 0
	for _, r := range apiRequired {
		n += len(r.Axes)
	}
	return n
}
