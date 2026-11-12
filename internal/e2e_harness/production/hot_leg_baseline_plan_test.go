package production

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// The hot-leg latency baseline (#851) itself is e2e-tagged; the parts here
// need no cluster, so the unit suite checks them.

// baselineTemplates is the number of template rows written through the
// EntityManager.
const baselineTemplates = 500

// baselineHotSize is the fixed hot set: unflushed writes at every size, half
// updates of flushed rows and half creates.
const baselineHotSize = 1_000

// parseBaselineSizes parses FORMA_HOT_LEG_BASELINE_SIZES. The loader only
// grows the tables: it clones rows up to size - baselineHotSize/2, and the
// hot set's creates add the rest. Those creates land at every size, cloned
// or not, so each size must exceed the one before by at least that many
// rows, or the table would overshoot it. The first size leaves room for the
// templates and the hot set.
func parseBaselineSizes(raw string) ([]int64, error) {
	minSize := int64(baselineTemplates + baselineHotSize)
	step := int64(baselineHotSize / 2)
	var sizes []int64
	for _, field := range strings.Split(raw, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(field), 10, 64)
		if err != nil || n < minSize {
			return nil, fmt.Errorf("%q is not a size of at least %d", field, minSize)
		}
		if k := len(sizes); k > 0 && n-sizes[k-1] < step {
			return nil, fmt.Errorf("%d follows %d: each size must exceed the one before by at least %d, the hot set's creates", n, sizes[k-1], step)
		}
		sizes = append(sizes, n)
	}
	return sizes, nil
}

// hotLegCutoff returns the flush cutoff the statement's hot-leg predicate
// renders, flushed_at = 0 OR flushed_at >= cutoff: the change_log rows the
// statement keeps from PostgreSQL. Every occurrence must name the same one.
func hotLegCutoff(stmt string) (int64, error) {
	matches := regexp.MustCompile(`flushed_at = 0 OR [a-z_.]*flushed_at >= (\d+)\)`).FindAllStringSubmatch(stmt, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("the statement renders no literal flush cutoff")
	}
	cutoff, err := strconv.ParseInt(matches[0][1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse the flush cutoff %q: %w", matches[0][1], err)
	}
	for _, m := range matches[1:] {
		if m[1] != matches[0][1] {
			return 0, fmt.Errorf("the statement renders two flush cutoffs, %s and %s", matches[0][1], m[1])
		}
	}
	return cutoff, nil
}

// retargetHotLeg points every table the statement reads from the PostgreSQL
// catalog alias at schema instead of public. Nothing else in it changes.
func retargetHotLeg(stmt, alias, schema string) (string, error) {
	from := alias + `."public".`
	if !strings.Contains(stmt, from) {
		return "", fmt.Errorf("the statement reads nothing from %s", from)
	}
	return strings.ReplaceAll(stmt, from, alias+`."`+schema+`".`), nil
}

// baselineProfile is what one profiled run of a statement shows: the rows
// its scans returned, and its physical plan with the row counts left out.
type baselineProfile struct {
	postgresRows, parquetRows int64
	plan                      string
}

// profileNode is one operator of DuckDB's JSON profile.
type profileNode struct {
	Type             string         `json:"type"`
	IntermediateRows int64          `json:"intermediate_rows"`
	ExtraInfo        map[string]any `json:"extra_info"`
	Children         []profileNode  `json:"children"`
}

type profileDoc struct {
	Operator []profileNode `json:"operator"`
	Query    struct {
		SQL string `json:"sql"`
	} `json:"query"`
}

// summarizeProfile reads DuckDB's JSON profile of stmt. The postgres
// extension's scans name only the bare table, so a scan counts as PostgreSQL
// when stmt reads a table of that name from pgCatalog; DuckDB's own table
// scans name their catalog. A table scan of any other source fails the
// summary rather than be counted unseen.
func summarizeProfile(raw []byte, stmt, pgCatalog string) (baselineProfile, error) {
	var doc profileDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return baselineProfile{}, fmt.Errorf("decode the profile: %w", err)
	}
	// DuckDB records the statement with its whitespace trimmed.
	if strings.Join(strings.Fields(doc.Query.SQL), " ") != strings.Join(strings.Fields(stmt), " ") {
		return baselineProfile{}, fmt.Errorf("the profile is of another statement: %.80q", doc.Query.SQL)
	}
	pgTables := map[string]bool{}
	for _, m := range regexp.MustCompile(regexp.QuoteMeta(pgCatalog)+`\."[^"]+"\."([^"]+)"`).FindAllStringSubmatch(stmt, -1) {
		pgTables[m[1]] = true
	}
	var p baselineProfile
	var plan strings.Builder
	var walk func(nodes []profileNode, depth int) error
	walk = func(nodes []profileNode, depth int) error {
		for _, n := range nodes {
			if err := p.count(n, pgTables); err != nil {
				return err
			}
			plan.WriteString(planLine(n, depth))
			if err := walk(n.Children, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(doc.Operator, 0); err != nil {
		return baselineProfile{}, err
	}
	p.plan = plan.String()
	return p, nil
}

func (p *baselineProfile) count(n profileNode, pgTables map[string]bool) error {
	if n.Type != "TABLE_SCAN" {
		return nil
	}
	fn, _ := n.ExtraInfo["Function"].(string)
	table, _ := n.ExtraInfo["Table"].(string)
	switch {
	case fn == "READ_PARQUET" || fn == "PARQUET_SCAN":
		p.parquetRows += n.IntermediateRows
	case fn == "" && pgTables[table]:
		p.postgresRows += n.IntermediateRows
	default:
		return fmt.Errorf("table scan of an unclassified source: function %q, table %q", fn, table)
	}
	return nil
}

// planLine renders one operator of the plan: its type and every detail
// DuckDB shows but the cardinality estimate, which follows table statistics,
// not the plan's shape.
func planLine(n profileNode, depth int) string {
	var b strings.Builder
	b.WriteString(strings.Repeat("  ", depth) + n.Type)
	for _, k := range slices.Sorted(maps.Keys(n.ExtraInfo)) {
		if k == "Estimated Cardinality" {
			continue
		}
		v, _ := json.Marshal(n.ExtraInfo[k])
		fmt.Fprintf(&b, " %s=%s", k, v)
	}
	return b.String() + "\n"
}
