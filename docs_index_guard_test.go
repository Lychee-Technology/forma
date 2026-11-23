package forma

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
)

// docsIndex is the docs-site home page. It once carried its own Quick Start,
// which kept failing after the README's was fixed in #875 (#877), and links
// to a repository that does not exist.
const docsIndex = "docs/index.md"

// quickStartMirrors names, in order, the README Quick Start subsections whose
// first fenced block the home page's Quick Start repeats whole.
var quickStartMirrors = []string{
	"### Option A: one script",
	"### Option B: step by step",
	"### First request",
}

// TestDocsIndexQuickStartMatchesREADME pins #877: the home page's Quick Start
// holds exactly the README blocks named in quickStartMirrors, each byte for
// byte and in order. A fix to the README that the home page does not follow,
// a shortened or emptied copy, or a dropped one then fails here instead of
// shipping a failing first run.
func TestDocsIndexQuickStartMatchesREADME(t *testing.T) {
	for _, p := range quickStartDrift(readDoc(t, docsIndex), readDoc(t, "README.md")) {
		t.Errorf("%s: %s; copy the block from README.md's Quick Start (#877)", docsIndex, p)
	}
}

func quickStartDrift(index, readme string) []string {
	readmeQS, ok := markdownSection(readme, "## Quick Start")
	if !ok {
		return []string{"README.md has no \"## Quick Start\" section: the #877 guard has lost its source"}
	}
	var problems, want []string
	for _, heading := range quickStartMirrors {
		sub, ok := markdownSection(readmeQS, heading)
		blocks := fencedBlocks(sub)
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("README.md's Quick Start has no %q subsection", heading))
		case len(blocks) == 0 || strings.TrimSpace(blocks[0]) == "":
			problems = append(problems, fmt.Sprintf("README.md's %q has no non-empty fenced block", heading))
		default:
			want = append(want, blocks[0])
		}
	}
	if len(problems) > 0 {
		return problems
	}
	indexQS, ok := markdownSection(index, "## Quick Start")
	if !ok {
		return []string{"no \"## Quick Start\" section"}
	}
	got := fencedBlocks(indexQS)
	if len(got) != len(want) {
		problems = append(problems, fmt.Sprintf("Quick Start has %d fenced blocks; it mirrors %d README blocks (%s)",
			len(got), len(want), strings.Join(quickStartMirrors, ", ")))
	}
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			problems = append(problems, fmt.Sprintf("Quick Start block %d differs from the block under README.md's %q:\n--- home page\n%s\n--- README.md\n%s",
				i+1, quickStartMirrors[i], got[i], want[i]))
		}
	}
	return problems
}

var (
	githubRepoURL = regexp.MustCompile(`github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)`)
	// markdownLink captures an inline link's target; heroLink a VitePress
	// frontmatter action's link.
	markdownLink = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	heroLink     = regexp.MustCompile(`(?m)^\s*link:\s*(\S+)\s*$`)
)

// unbackedSiteLinks are site-local links the home page keeps although no file
// under docs/ backs them, each with the reason. Every entry must still be
// linked and still be unbacked, so an exception cannot outlive its cause.
var unbackedSiteLinks = map[string]string{
	// docs/blog-series/ was deleted in d030667, which shows only that this
	// repository no longer holds the series. Whether the deployed site still
	// serves it is unknown, and #877 leaves removing the link to the site
	// owner's confirmation.
	"/blog-series/": "awaiting the site owner's decision (#877)",
}

// TestDocsIndexLinksResolve pins #877's link fixes: a GitHub repository URL
// names Lychee-Technology/forma, and every site-local link, in the hero
// actions or the body, reaches a file in docs/ or is listed in
// unbackedSiteLinks. External URLs other than GitHub's are not checked.
func TestDocsIndexLinksResolve(t *testing.T) {
	for _, p := range linkProblems(readDoc(t, docsIndex), unbackedSiteLinks) {
		t.Errorf("%s: %s", docsIndex, p)
	}
}

func linkProblems(doc string, unbacked map[string]string) []string {
	var problems []string
	for _, m := range githubRepoURL.FindAllStringSubmatch(doc, -1) {
		if m[1] != "Lychee-Technology" || strings.TrimSuffix(m[2], ".git") != "forma" {
			problems = append(problems, fmt.Sprintf("links to github.com/%s/%s; the repository is github.com/Lychee-Technology/forma", m[1], m[2]))
		}
	}
	linked := map[string]bool{}
	for _, re := range []*regexp.Regexp{markdownLink, heroLink} {
		for _, m := range re.FindAllStringSubmatch(doc, -1) {
			target := m[1]
			if strings.Contains(target, "://") || strings.HasPrefix(target, "#") {
				continue
			}
			linked[target] = true
			if _, ok := unbacked[target]; ok {
				continue
			}
			if err := siteLinkResolves(target); err != nil {
				problems = append(problems, err.Error())
			}
		}
	}
	for target, reason := range unbacked {
		if !linked[target] {
			problems = append(problems, fmt.Sprintf("no longer links to %q; drop its unbackedSiteLinks entry (%s)", target, reason))
		} else if siteLinkResolves(target) == nil {
			problems = append(problems, fmt.Sprintf("%q now resolves under docs/; drop its unbackedSiteLinks entry (%s)", target, reason))
		}
	}
	return problems
}

// siteLinkResolves reports why a docs-site link does not reach a file under
// docs/, or nil when it does: a relative path as written, or a site-absolute
// one as VitePress resolves it (with or without the .md suffix, or a
// directory's index.md). A target that climbs out of docs/ is refused even
// when the file it names exists, or when the climb re-enters docs/, since the
// site serves docs/ as its root.
func siteLinkResolves(target string) error {
	target, _, _ = strings.Cut(target, "#")
	rel := path.Clean(strings.TrimPrefix(target, "/"))
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return fmt.Errorf("links to %q, which leaves docs/, the site root", target)
	}
	p := path.Join("docs", rel)
	for _, candidate := range []string{p, p + ".md", path.Join(p, "index.md")} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return nil
		}
	}
	return fmt.Errorf("links to %q, which is no file under docs/", target)
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
// the same or a higher level, and whether heading was found. Lines inside
// ``` blocks are never headings, so a shell comment does not end the section.
func markdownSection(doc, heading string) (string, bool) {
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
		return "", false
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
	return strings.Join(lines[start:end], "\n"), true
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
