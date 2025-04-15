package testguard

import (
	"os"
	"path/filepath"
	"strings"
)

// ADRAutoBinding scans adr/ADR-*.md (relative to root) for a front-matter
// `auto:` mapping that binds family and returns the bound kind slug. It
// understands the flow form `auto: {queue: seqmpmc}` and the block form.
// bound is false when no ADR binds the family yet.
func ADRAutoBinding(root, family string) (slug string, bound bool, err error) {
	files, err := filepath.Glob(filepath.Join(root, "adr", "ADR-*.md"))
	if err != nil {
		return "", false, err
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return "", false, err
		}
		if s, ok := ParseADRAutoBinding(string(data), family); ok {
			return s, true, nil
		}
	}
	return "", false, nil
}

// ParseADRAutoBinding extracts auto.<family> from one ADR document.
func ParseADRAutoBinding(doc, family string) (string, bool) {
	if !strings.HasPrefix(doc, "---\n") {
		return "", false
	}
	end := strings.Index(doc[4:], "\n---")
	if end < 0 {
		return "", false
	}
	lines := strings.Split(doc[4:4+end], "\n")
	for i, line := range lines {
		rest, ok := strings.CutPrefix(line, "auto:")
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		if strings.HasPrefix(rest, "{") {
			for pair := range strings.SplitSeq(strings.Trim(rest, "{}"), ",") {
				k, v, ok := strings.Cut(pair, ":")
				if ok && strings.TrimSpace(k) == family {
					return strings.TrimSpace(v), true
				}
			}
			return "", false
		}
		for _, sub := range lines[i+1:] {
			if !strings.HasPrefix(sub, " ") {
				break
			}
			k, v, ok := strings.Cut(strings.TrimSpace(sub), ":")
			if ok && strings.TrimSpace(k) == family {
				return strings.TrimSpace(v), true
			}
		}
		return "", false
	}
	return "", false
}
