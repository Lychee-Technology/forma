package publication

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// requiredPublicDocs are the public and operator docs that document the
// retired V1 surface most directly. Discovery must find each of them, so a
// walker that silently skips a tree fails instead of passing vacuously.
var requiredPublicDocs = []string{
	"README.md",
	"docs/index.md",
	"docs/publication-protocol-v2.md",
	"docs/federated-query/design.md",
	"docs/error-handling.md",
	"docs/manifest-reconcile.md",
	"docs/aurora-dsql.md",
	"docs/dsql-ministack.md",
	"docs/schema-evolution.md",
	"cmd/tools/README.md",
	"tests/e2e/README.md",
}

// nonPublicDocTrees hold markdown that is not product or operator
// documentation: tools/autoresearch is agent research scaffolding plus
// recorded benchmark run output, which quotes historical DuckDB errors
// (V1 level-3 globs included) verbatim.
var nonPublicDocTrees = map[string]bool{
	"tools/autoresearch": true,
}

// forbiddenDocTokens are retired V1 flags, env vars, APIs and glob patterns.
// A fenced code block never contains one; prose names one only inside a
// removal or historical note. A leading-dash name is a flag: it matches both
// Go `-flag` and POSIX `--flag` spellings, and only as a whole flag, so
// "read-time auto-repair" is not the `--repair` flag.
var forbiddenDocTokens = retiredDocTokens(
	"QuerySource.Fallback",
	"-glob-only",
	"-manifest-template",
	"-manifest-prefix",
	"-replace-delta",
	"-delta-prefix",
	"-repair",
	"-verify-stamps",
	"-allow-empty-manifest-schema",
	"-etag-retries",
	"-gc-grace",
	"DUCKDB_MANIFEST_TEMPLATE",
	"DUCKDB_MANIFEST_PREFIX",
	"DUCKDB_S3_PREFIX",
	"FLUSH_VISIBILITY_GRACE_MS",
	"/*.parquet",
)

// removalNoteRE marks a prose block as a removal or historical note: it says
// the thing is gone, refused or ignored, so a retired token it names is not
// an instruction. It deliberately omits imperatives such as "remove" or
// "delete", which can instruct.
var removalNoteRE = regexp.MustCompile(`(?i)\b(?:removed|removes|removal|deleted|deletes|deletion|retired|gone|went with|no longer|(?:has|have|had|takes|is|are) no|ignored|ignores|rejected|rejects|refused|refuses|obsolete|deprecated|banned|banning|bans|not defined)\b|不再|删除|拒绝|移除|废弃`)

// negatedMentionRE matches the text just before a mention that negates it,
// as in "and no `--repair` flag".
var negatedMentionRE = regexp.MustCompile("(?i)\\b(?:no|without)\\s+`?$")

var listItemRE = regexp.MustCompile(`^\s*(?:[-*+]|\d+[.)])\s+`)

// headingRE matches an ATX heading; a wrapped line that starts with an issue
// reference such as "#896 (" is not one.
var headingRE = regexp.MustCompile(`^\s{0,3}#{1,6}(?:\s|$)`)

type retiredDocToken struct {
	name string
	re   *regexp.Regexp // submatch 1 is the mention itself
}

func retiredDocTokens(names ...string) []retiredDocToken {
	tokens := make([]retiredDocToken, 0, len(names))
	for _, name := range names {
		pattern := "(" + regexp.QuoteMeta(name) + ")"
		if strings.HasPrefix(name, "-") {
			pattern = `(?:^|[^A-Za-z0-9_-])(--?` + regexp.QuoteMeta(name[1:]) + `)(?:[^A-Za-z0-9_-]|$)`
		}
		tokens = append(tokens, retiredDocToken{name: name, re: regexp.MustCompile(pattern)})
	}
	return tokens
}

type docLine struct {
	num  int
	text string
}

// proseBlock is one paragraph, list item, table row or heading. A list item
// also carries the paragraph that introduces its list ("These go:"), so the
// items of a list of removed things read as the note they belong to.
type proseBlock struct {
	lines   []docLine
	context string
}

func (b proseBlock) text() string {
	parts := make([]string, 0, len(b.lines)+1)
	parts = append(parts, b.context)
	for _, l := range b.lines {
		parts = append(parts, l.text)
	}
	return strings.Join(parts, " ")
}

// body joins b's own lines, without its list lead-in, and answers the byte
// offset at which each line starts in the result, so a clause can wrap.
func (b proseBlock) body() (string, []int) {
	var sb strings.Builder
	offsets := make([]int, len(b.lines))
	for i, l := range b.lines {
		if i > 0 {
			sb.WriteByte(' ')
		}
		offsets[i] = sb.Len()
		sb.WriteString(l.text)
	}
	return sb.String(), offsets
}

// publicDocs lists, sorted and slash-separated, every markdown file under
// root outside hidden, build, dependency and nonPublicDocTrees directories.
func publicDocs(root string) ([]string, error) {
	var docs []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk %s: %w", p, err)
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return fmt.Errorf("relativize %s against %s: %w", p, root, err)
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			name := d.Name()
			skip := strings.HasPrefix(name, ".") || name == "build" || name == "node_modules" || name == "vendor" || nonPublicDocTrees[rel]
			if p != root && skip {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(rel, ".md") {
			docs = append(docs, rel)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list the public docs under %s: %w", root, err)
	}
	sort.Strings(docs)
	return docs, nil
}

// legacyV1DocViolations answers, sorted, every retired V1 token in a fenced
// code block of relDocs, and every one in prose outside a removal or
// historical note.
func legacyV1DocViolations(root string, relDocs []string) ([]string, error) {
	var violations []string
	for _, rel := range relDocs {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return nil, fmt.Errorf("read public doc %s: %w", rel, err)
		}
		blocks, fenced := splitMarkdown(string(raw))
		for _, l := range fenced {
			for _, tok := range forbiddenDocTokens {
				if tok.re.MatchString(l.text) {
					violations = append(violations, fmt.Sprintf("%s:%d code block uses retired V1 token %q", rel, l.num, tok.name))
				}
			}
		}
		for _, b := range blocks {
			violations = append(violations, proseViolations(rel, b)...)
		}
	}
	sort.Strings(violations)
	return violations, nil
}

// proseViolations reports each line of b that names a retired token outside
// a removal or historical note. A block without a removal word is not a note.
// A note still refuses a mention whose own clause instructs the reader to use
// it, so "Run `--glob-only`; the old path was removed" fails: the removal word
// belongs to another clause than the instruction.
func proseViolations(rel string, b proseBlock) []string {
	blockIsNote := removalNoteRE.MatchString(b.text())
	body, offsets := b.body()
	var violations []string
	for i, l := range b.lines {
		for _, tok := range forbiddenDocTokens {
			for _, loc := range tok.re.FindAllStringSubmatchIndex(l.text, -1) {
				if negatedMentionRE.MatchString(l.text[:loc[2]]) {
					continue
				}
				if blockIsNote && !instructs(clauseAt(body, offsets[i]+loc[2])) {
					continue
				}
				violations = append(violations, fmt.Sprintf("%s:%d prose uses retired V1 token %q outside a removal or historical note", rel, l.num, tok.name))
				break
			}
		}
	}
	return violations
}

// directiveRE matches a verb that tells the reader to do something with what
// follows it. negationRE matches a clause that tells the reader not to.
var (
	directiveRE = regexp.MustCompile(`(?i)\b(?:run|use|pass|set|enable|specify|invoke|configure|execute|try)\b|运行|使用|传入|设置|启用|执行`)
	negationRE  = regexp.MustCompile(`(?i)\b(?:not|never|don't|cannot|can't)\b|不要|勿|禁止`)
	// clauseSepRE ends a clause: a semicolon, a table cell border, or a
	// sentence end. A period inside a name such as `cdc.go` is not one.
	clauseSepRE = regexp.MustCompile(`[;|。；]|[.!?](?:\s|$)`)
)

// instructs reports whether clause recommends what it names: a directive
// verb, with neither a removal word nor a negation in the same clause.
func instructs(clause string) bool {
	return directiveRE.MatchString(clause) && !removalNoteRE.MatchString(clause) && !negationRE.MatchString(clause)
}

// clauseAt answers the clause of text that holds byte offset pos.
func clauseAt(text string, pos int) string {
	start, end := 0, len(text)
	for _, loc := range clauseSepRE.FindAllStringIndex(text, -1) {
		if loc[0] < pos {
			start = loc[1]
			continue
		}
		end = loc[0]
		break
	}
	return text[start:end]
}

// splitMarkdown splits src into prose blocks and fenced code lines. A blank
// line, heading, table row, list item or fence ends a prose block.
func splitMarkdown(src string) ([]proseBlock, []docLine) {
	var blocks []proseBlock
	var fenced []docLine
	var cur *proseBlock
	inFence, listLead := false, ""
	flush := func() {
		if cur == nil {
			return
		}
		blocks = append(blocks, *cur)
		if cur.context == "" {
			// A paragraph ending in ":" introduces the list that follows it;
			// a list item keeps the lead of its list.
			listLead = ""
			if last := strings.TrimSpace(cur.lines[len(cur.lines)-1].text); strings.HasSuffix(last, ":") || strings.HasSuffix(last, "：") {
				listLead = cur.text()
			}
		}
		cur = nil
	}
	for idx, line := range strings.Split(src, "\n") {
		l := docLine{num: idx + 1, text: line}
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~"):
			flush()
			inFence, listLead = !inFence, ""
		case inFence:
			fenced = append(fenced, l)
		case trimmed == "":
			flush()
		case headingRE.MatchString(line) || strings.HasPrefix(trimmed, "|"):
			flush()
			blocks = append(blocks, proseBlock{lines: []docLine{l}})
			listLead = ""
		case listItemRE.MatchString(line):
			flush()
			cur = &proseBlock{lines: []docLine{l}, context: listLead}
		case cur == nil:
			cur = &proseBlock{lines: []docLine{l}}
		default:
			cur.lines = append(cur.lines, l)
		}
	}
	flush()
	return blocks, fenced
}

// TestLegacyV1DocGuardCatchesPlantedRegressions proves the doc half of
// TestNoLegacyV1PathsOrObsoleteFlagsRemain: discovery reaches public docs
// beyond the required set and skips non-public trees, fenced examples are
// always refused, and prose names a retired token only inside a removal or
// historical note.
func TestLegacyV1DocGuardCatchesPlantedRegressions(t *testing.T) {
	dir := t.TempDir()
	for rel, src := range map[string]string{
		"README.md": "Historical note: `--repair` was removed.\n\n```bash\nforma-tools cdc-flush -glob-only\nforma-tools manifest-reconcile --repair\n```\n",
		"cmd/tools/README.md": "Run `forma-tools cdc-flush --glob-only` to skip the manifest.\n\n" +
			"Under V1 an object was repairable:\n`manifest-reconcile --repair` appended it.\nV2 has no repair.\n\n" +
			"已删除的 V1 选项：`-manifest-template`。\n\nThere is no read-time auto-repair.\n\n" +
			"The server reads no template since\n#896 (`DUCKDB_MANIFEST_TEMPLATE` is ignored).\n",
		"docs/schema-evolution.md": "Recover with:\n\n```bash\nforma-tools manifest-reconcile --repair\n```\n",
		"docs/ops.md": "These are removed:\n\n- `--glob-only` in `cdc-flush`.\n- `DUCKDB_S3_PREFIX`.\n\n" +
			"Set these:\n\n- `--delta-prefix=s3://b/d` on `cdc-init`.\n\n" +
			"| `--gc`, `--repair` | Repair is removed |\n| `--repair` | re-adopts unlisted objects |\n\n" +
			"Reads take the generation, and no `--verify-stamps` pass.\n",
		// A removal word elsewhere in the block does not excuse an
		// instruction in another clause (lines 1, 4, 6 and 10); a clause that
		// removes, refuses or forbids what it names stays a note (12, 14, 16).
		"docs/mixed.md": "Run `cdc-flush --glob-only`; the old path was removed.\n\n" +
			"The old manifest cache was removed in #896.\nTo skip manifest checks, run `forma-tools cdc-flush --glob-only`.\n\n" +
			"| `cdc-flush` | V1 fallback was removed; pass `--glob-only` for fast exports |\n\n" +
			"These V1 notes cover removed paths:\n\n- Run `forma-tools manifest-reconcile --repair` to rebuild missing manifests.\n\n" +
			"Do not run `--repair`; it was removed in #891.\n\n" +
			"`--glob-only` was removed; use `--s3-prefix` instead.\n\n" +
			"Under V1 you would run the flush\nwith `--glob-only`, which #896 removed.\n",
		"tools/autoresearch/run/report.md":   "```\nread_parquet('s3://b/1/base/*.parquet')\n```\n",
		".autoforge/notes.md":                "Run `cdc-flush --glob-only`.\n",
		"tests/e2e/node_modules/x/README.md": "Run `cdc-flush --glob-only`.\n",
	} {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(src), 0o600))
	}

	docs, err := publicDocs(dir)
	require.NoError(t, err)
	require.Equal(t, []string{"README.md", "cmd/tools/README.md", "docs/mixed.md", "docs/ops.md", "docs/schema-evolution.md"}, docs)

	got, err := legacyV1DocViolations(dir, docs)
	require.NoError(t, err)
	require.Equal(t, []string{
		`README.md:4 code block uses retired V1 token "-glob-only"`,
		`README.md:5 code block uses retired V1 token "-repair"`,
		`cmd/tools/README.md:1 prose uses retired V1 token "-glob-only" outside a removal or historical note`,
		`docs/mixed.md:1 prose uses retired V1 token "-glob-only" outside a removal or historical note`,
		`docs/mixed.md:10 prose uses retired V1 token "-repair" outside a removal or historical note`,
		`docs/mixed.md:4 prose uses retired V1 token "-glob-only" outside a removal or historical note`,
		`docs/mixed.md:6 prose uses retired V1 token "-glob-only" outside a removal or historical note`,
		`docs/ops.md:11 prose uses retired V1 token "-repair" outside a removal or historical note`,
		`docs/ops.md:8 prose uses retired V1 token "-delta-prefix" outside a removal or historical note`,
		`docs/schema-evolution.md:4 code block uses retired V1 token "-repair"`,
	}, got)
}
