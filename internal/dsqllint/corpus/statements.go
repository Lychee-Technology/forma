package corpus

import (
	"regexp"
	"strings"
)

// dollarTag matches the opening tag of a dollar-quoted string: $$ or
// $name$. A $1 placeholder is not one.
var dollarTag = regexp.MustCompile(`^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$`)

// CountStatements answers how many executable statements text holds: the
// pieces between semicolons that keep something once comments and
// whitespace are removed. Semicolons and comment markers inside quoted
// strings, quoted identifiers and dollar-quoted bodies do not count. Text
// that is only comments, whitespace and semicolons holds none.
func CountStatements(text string) int {
	count := 0
	content := false
	for i := 0; i < len(text); {
		switch c := text[i]; {
		case strings.HasPrefix(text[i:], "--"):
			i = skipPast(text, i, "\n")
		case strings.HasPrefix(text[i:], "/*"):
			i = SkipBlockComment(text, i)
		case c == ';':
			if content {
				count++
			}
			content = false
			i++
		case c == '\'' || c == '"':
			content = true
			i = skipPast(text, i+1, string(c))
		case c == '$' && dollarTag.MatchString(text[i:]):
			tag := dollarTag.FindString(text[i:])
			content = true
			i = skipPast(text, i+len(tag), tag)
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		default:
			content = true
			i++
		}
	}
	if content {
		count++
	}
	return count
}

// SkipBlockComment answers the index just after the block comment that
// opens at text[from:], which starts with "/*", or len(text) when it never
// closes. PostgreSQL block comments nest, as its lexer reads them: each
// "/*" inside one opens another that needs its own "*/", so
// /* outer /* inner */ outer */ is one comment, not a comment followed by
// the text "outer */" (#740 review R11-F1). The scan's discovery and the
// gate's statement count both skip comments with it, so neither stops at
// the first "*/" of a nested comment.
func SkipBlockComment(text string, from int) int {
	depth := 0
	for i := from; i < len(text); {
		switch {
		case strings.HasPrefix(text[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(text[i:], "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(text)
}

// skipPast answers the index just after the first end at or after from, or
// the end of text. A doubled quote inside a quoted string is its escape and
// is skipped as one, which the next search does on its own.
func skipPast(text string, from int, end string) int {
	j := strings.Index(text[from:], end)
	if j < 0 {
		return len(text)
	}
	return from + j + len(end)
}
