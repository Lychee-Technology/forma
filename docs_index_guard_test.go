package forma

import (
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
)

// docsIndex is the docs-site home page. It once carried its own Quick Start,
// which kept failing after the README's was fixed in #875 (#877), and links
// to a repository and a blog section that do not exist.
const docsIndex = "docs/index.md"

// TestDocsIndexQuickStartMatchesREADME pins #877: every code block of the
// home page's Quick Start must appear verbatim in the README's Quick Start,
// the one that is kept working. A fix to the README that the home page does
// not follow then fails here instead of shipping a failing first run.
func TestDocsIndexQuickStartMatchesREADME(t *testing.T) {
	index := markdownSection(t, readDoc(t, docsIndex), "## Quick Start")
	readme := markdownSection(t, readDoc(t, "README.md"), "## Quick Start")
	blocks := fencedBlocks(index)
	if len(blocks) == 0 {
		t.Fatalf("%s Quick Start has no fenced code block: the #877 guard has lost its target", docsIndex)
	}
	for _, block := range blocks {
		if !strings.Contains(readme, block) {
			t.Errorf("%s Quick Start block is not in README.md's Quick Start; copy it from the README instead (#877):\n%s", docsIndex, block)
		}
	}
}

var (
	githubRepoURL = regexp.MustCompile(`github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)`)
	// markdownLink captures an inline link's target; heroLink a VitePress
	// frontmatter action's link.
	markdownLink = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	heroLink     = regexp.MustCompile(`(?m)^\s*link:\s*(\S+)\s*$`)
)

// TestDocsIndexLinksResolve pins #877's link fixes: a GitHub repository URL
// names Lychee-Technology/forma, and every site-local link, in the hero
// actions or the body, reaches a file in docs/. External URLs other than
// GitHub's are not checked.
func TestDocsIndexLinksResolve(t *testing.T) {
	doc := readDoc(t, docsIndex)
	for _, m := range githubRepoURL.FindAllStringSubmatch(doc, -1) {
		if m[1] != "Lychee-Technology" || strings.TrimSuffix(m[2], ".git") != "forma" {
			t.Errorf("%s links to github.com/%s/%s; the repository is github.com/Lychee-Technology/forma", docsIndex, m[1], m[2])
		}
	}
	var targets []string
	for _, re := range []*regexp.Regexp{markdownLink, heroLink} {
		for _, m := range re.FindAllStringSubmatch(doc, -1) {
			targets = append(targets, m[1])
		}
	}
	for _, target := range targets {
		if strings.Contains(target, "://") || strings.HasPrefix(target, "#") {
			continue
		}
		if !siteLinkResolves(target) {
			t.Errorf("%s links to %q, which is no file under docs/", docsIndex, target)
		}
	}
}

// siteLinkResolves reports whether a docs-site link reaches a file under
// docs/: a relative path as written, or a site-absolute one as VitePress
// resolves it (with or without the .md suffix, or a directory's index.md).
func siteLinkResolves(target string) bool {
	target, _, _ = strings.Cut(target, "#")
	p := path.Join("docs", strings.TrimPrefix(target, "/"))
	for _, candidate := range []string{p, p + ".md", path.Join(p, "index.md")} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return true
		}
	}
	return false
}

func readDoc(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// markdownSection returns the lines from heading up to the next heading of
// the same or a higher level. Lines inside ``` blocks are never headings, so
// a shell comment does not end the section.
func markdownSection(t *testing.T, doc, heading string) string {
	t.Helper()
	level := strings.Index(heading, " ")
	lines := strings.Split(doc, "\n")
	start := -1
	for i, line := range lines {
		if line == heading {
			start = i
			break
		}
	}
	if start == -1 {
		t.Fatalf("no %q heading", heading)
	}
	end := len(lines)
	inBlock := false
	for i := start + 1; i < len(lines) && end == len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "```") {
			inBlock = !inBlock
			continue
		}
		hashes := len(line) - len(strings.TrimLeft(line, "#"))
		if !inBlock && hashes > 0 && hashes <= level && strings.HasPrefix(line[hashes:], " ") {
			end = i
		}
	}
	return strings.Join(lines[start:end], "\n")
}

// fencedBlocks returns the bodies of the ``` blocks in section.
func fencedBlocks(section string) []string {
	var blocks []string
	var body []string
	inBlock := false
	for _, line := range strings.Split(section, "\n") {
		if strings.HasPrefix(line, "```") {
			if inBlock {
				blocks = append(blocks, strings.Join(body, "\n"))
			}
			inBlock, body = !inBlock, nil
			continue
		}
		if inBlock {
			body = append(body, line)
		}
	}
	return blocks
}
