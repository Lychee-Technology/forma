package repoguard

import (
	"strings"
	"testing"
)

// editFence rewrites the n-th (0-based) ``` block of doc: its body becomes
// body, or the whole block, fences included, goes when drop is set.
func editFence(t *testing.T, doc string, n int, body string, drop bool) string {
	t.Helper()
	var out []string
	seen, inBlock, found := -1, false, false
	for _, line := range strings.Split(doc, "\n") {
		fence := strings.HasPrefix(line, "```")
		if fence && !inBlock {
			seen++
		}
		target := seen == n && (inBlock || fence)
		if fence {
			inBlock = !inBlock
		}
		switch {
		case !target:
			out = append(out, line)
		case drop:
			found = true
		case fence:
			out = append(out, line)
			if inBlock {
				found = true
				if body != "" {
					out = append(out, body)
				}
			}
		}
	}
	if !found {
		t.Fatalf("no fenced block %d", n)
	}
	return strings.Join(out, "\n")
}

// TestQuickStartDriftFixtures checks that the #877 Quick Start guard rejects
// the drift it exists for, not only that today's page passes.
func TestQuickStartDriftFixtures(t *testing.T) {
	index, readme := readDoc(t, docsIndex), readDoc(t, "README.md")
	cases := []struct {
		name          string
		index, readme string
		wantProblem   bool
	}{
		{"current pages", index, readme, false},
		{"step-by-step block cut to its last command", editFence(t, index, 1,
			"DB_PASSWORD=postgres SCHEMA_DIR=cmd/server/schemas ./build/server", false), readme, true},
		{"one-script block emptied", editFence(t, index, 0, "", false), readme, true},
		{"first-request block dropped", editFence(t, index, 2, "", true), readme, true},
		{"extra block added", strings.Replace(index, "## Learn More",
			"```bash\nmake build-all\n```\n\n## Learn More", 1), readme, true},
		{"README subsection renamed", index,
			strings.Replace(readme, "### First request", "### First requests", 1), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			problems := quickStartDrift(c.index, c.readme)
			if got := len(problems) > 0; got != c.wantProblem {
				t.Errorf("problems = %q, want any: %v", problems, c.wantProblem)
			}
		})
	}
}

// TestLinkProblemsFixtures checks the #877 link guard on targets it must
// refuse, including ones whose file exists outside docs/.
func TestLinkProblemsFixtures(t *testing.T) {
	blog := map[string]string{"/blog-series/": "test"}
	cases := []struct {
		name        string
		doc         string
		unbacked    map[string]string
		wantProblem bool
	}{
		{"current page", readDoc(t, docsIndex), unbackedSiteLinks, false},
		{"relative link with anchor", "[x](./schema-indexes.md#text-ranges)", nil, false},
		{"site-absolute link without .md", "[x](/schema-evolution)", nil, false},
		{"relative escape to the repository README", "[x](../README.md)", nil, true},
		{"site-absolute escape", "[x](/../README.md)", nil, true},
		{"escape that re-enters docs", "[x](../docs/index.md)", nil, true},
		{"missing file", "[x](./no-such-page.md)", nil, true},
		{"unbacked link without an exception", "[x](/blog-series/)", nil, true},
		{"unbacked link with an exception", "[x](/blog-series/)", blog, false},
		{"exception no longer linked", "[x](/schema-evolution)", blog, true},
		{"exception for a link that resolves", "[x](/schema-evolution)",
			map[string]string{"/schema-evolution": "test"}, true},
		{"foreign GitHub repository", "[x](https://github.com/ruoshui/forma)", nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			problems := linkProblems(c.doc, c.unbacked)
			if got := len(problems) > 0; got != c.wantProblem {
				t.Errorf("problems = %q, want any: %v", problems, c.wantProblem)
			}
		})
	}
}
