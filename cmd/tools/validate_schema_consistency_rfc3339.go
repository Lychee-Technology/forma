package main

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// checkRFC3339Images reports every image of a date/datetime bound with the
// rfc3339 encoding that is not the canonical image of its instant (#842),
// through the storage-domain census baseline-schema runs
// (widthaudit.DomainCensus). An image the read refuses fails every read of
// its row. Any other non-canonical image (an offset, a fraction of other
// than three digits, a whole-second image the migration has not rewritten)
// reads, but the PostgreSQL route compares its text and the DuckDB tiers
// its instant, so the two can answer differently on its row
// (docs/rfc3339-storage-contract.md, "Rows outside the contract"). Both are
// failures: after migrate-rfc3339 and the repair of the rows it lists, the
// check finds nothing.
func (v schemaConsistencyValidator) checkRFC3339Images(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	var targets []widthaudit.Target
	for _, t := range widthaudit.DomainTargets(cache) {
		if t.Column != "" && (t.Declared == forma.ValueTypeDate || t.Declared == forma.ValueTypeDateTime) {
			targets = append(targets, t)
		}
	}
	if len(targets) == 0 {
		return nil, nil
	}
	main := v.widths.entityMainTable
	tables := widthaudit.Tables{EntityMain: main, ChangeLog: v.widths.changeLogTable}
	findings, err := widthaudit.DomainCensus(ctx, v.pool, tables, targets)
	if err != nil {
		return nil, fmt.Errorf("rfc3339 image census: %w", err)
	}
	issues := make([]validationIssue, 0, len(findings))
	for _, f := range findings {
		if f.NonCanonical == "" {
			issues = append(issues, widthIssue("rfc3339 images the OLTP read refuses in "+main, f, severityError))
			continue
		}
		issue := widthIssue("rfc3339 images that are not canonical, on which the PostgreSQL and DuckDB routes can disagree, in "+main, f, severityError)
		issue.details += fmt.Sprintf(" repair=%q (%s)", f.NonCanonical, f.NonCanonical.Explain(f.Canonical))
		issues = append(issues, issue)
	}
	return issues, nil
}
