//go:build matrix

package matrix

import (
	"strings"
	"testing"
)

// TestRedisFlipsHoldEachServerToItsSideOf72 is tt6-9. The flip check asked
// only that FeatureDurableConfirm differ somewhere across the matrix, so a
// table that claimed it on the wrong side of WAITAOF's 7.2 boundary passed:
// reversed (on 6.2 and not on 7.4), or from 7.0 (hunt 5's rd-3, where WAITAOF
// does not exist) on a matrix with a 7.0 row. Each server is now held to the
// claim its version and append-only file give it, as the JetStream matrix
// holds each server to its own. None of it needs a server: the assertion is a
// function of the results.
func TestRedisFlipsHoldEachServerToItsSideOf72(t *testing.T) {
	row := func(image, version string, aof, durable bool) redisResult {
		return redisResult{
			image: image, version: version, software: "redis", aof: aof, opened: true,
			durableConfirm: durable, atLeastOnce: true, extendLease: true,
		}
	}
	for _, c := range []struct {
		name    string
		results []redisResult
		want    []string // a substring of each problem, in order; none for a correct table
	}{
		{
			name: "the claim where WAITAOF is",
			results: []redisResult{
				row("redis:7.4-alpine", "7.4.2", true, true),
				row("redis:6.2-alpine", "6.2.17", true, false),
				row("redis:8.2.9-alpine", "8.2.9", true, true),
				row("valkey/valkey:8.1.10-alpine", "8.1.10", true, true),
				row("redis:7.0-alpine", "7.0.15", true, false),
				row("redis:7.2-alpine", "7.2.7", true, true),
			},
		},
		{
			name: "reversed",
			results: []redisResult{
				row("redis:7.4-alpine", "7.4.2", true, false),
				row("redis:6.2-alpine", "6.2.17", true, true),
				row("redis:8.2.9-alpine", "8.2.9", true, false),
			},
			want: []string{
				"redis:7.4-alpine (7.4.2): FeatureDurableConfirm is false, want true",
				"redis:6.2-alpine (6.2.17): FeatureDurableConfirm is true, want false",
				"redis:8.2.9-alpine (8.2.9): FeatureDurableConfirm is false, want true",
			},
		},
		{
			name: "from 7.0, where WAITAOF does not exist",
			results: []redisResult{
				row("redis:6.2-alpine", "6.2.17", true, false),
				row("redis:7.0-alpine", "7.0.15", true, true),
				row("redis:7.4-alpine", "7.4.2", true, true),
			},
			want: []string{"redis:7.0-alpine (7.0.15): FeatureDurableConfirm is true, want false"},
		},
		{
			name: "everywhere, not reading the version at all",
			results: []redisResult{
				row("redis:6.2-alpine", "6.2.17", true, true),
				row("redis:7.4-alpine", "7.4.2", true, true),
			},
			want: []string{"redis:6.2-alpine (6.2.17): FeatureDurableConfirm is true, want false"},
		},
		{
			name: "without an append-only file to wait on",
			results: []redisResult{
				row("redis:7.4-alpine", "7.4.2", false, true),
				row("redis:8.2.9-alpine", "8.2.9", true, true),
			},
			want: []string{"redis:7.4-alpine (7.4.2): FeatureDurableConfirm is true, want false"},
		},
		{
			name: "one server, still held to its side",
			results: []redisResult{
				row("redis:6.2-alpine", "6.2.17", true, true),
			},
			want: []string{"redis:6.2-alpine (6.2.17): FeatureDurableConfirm is true, want false"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			problems, _, _, _ := redisFlipProblems(c.results)
			if len(problems) != len(c.want) {
				t.Fatalf("%d problem(s), want %d:\n%s", len(problems), len(c.want), strings.Join(problems, "\n"))
			}
			for i, w := range c.want {
				if !strings.Contains(problems[i], w) {
					t.Errorf("problem %d is %q, want it to say %q", i, problems[i], w)
				}
			}
		})
	}
}
