package main

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// checkBoundTextImages reports the entity_main images of the two encodings
// that keep a non-text type in a text column, through the storage-domain
// census baseline-schema runs (widthaudit.DomainCensus). An image the read
// refuses fails every OLTP read of its row, every update of it (an update
// parses each bound column of the row it merges into), and under the pgx
// hot leg, the default since #857, every federated page that holds the row
// while it is unflushed.
//
//   - A bool bound with bool_text must hold "1" or "0" (#404). NULL is an
//     unset bool and is not reported. The attach hot leg and the CDC export
//     read any other image as false, so no federated query failed on one
//     before #857.
//   - A date/datetime bound with rfc3339 must be the canonical image of its
//     instant (#842). Any other image the read parses (an offset, a fraction
//     of other than three digits, a whole-second image the migration has not
//     rewritten) reads, but the PostgreSQL route compares its text and the
//     DuckDB tiers its instant, so the two can answer differently on its row
//     (docs/rfc3339-storage-contract.md, "Rows outside the contract").
//
// All are failures: after migrate-rfc3339 and the repair of the rows the
// check lists, it finds nothing.
func (v schemaConsistencyValidator) checkBoundTextImages(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	var targets []widthaudit.Target
	for _, t := range widthaudit.DomainTargets(cache) {
		// DomainTargets binds a column only for bool_text and rfc3339.
		if t.Column != "" {
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
		return nil, fmt.Errorf("bool_text and rfc3339 image census: %w", err)
	}
	issues := make([]validationIssue, 0, len(findings))
	for _, f := range findings {
		switch {
		case f.Declared == forma.ValueTypeBool:
			issue := widthIssue("bool_text images the OLTP read refuses in "+main, f, severityError)
			issue.details += ` (bool_text holds "1", "0" or NULL)`
			issues = append(issues, issue)
		case f.NonCanonical == "":
			issues = append(issues, widthIssue("rfc3339 images the OLTP read refuses in "+main, f, severityError))
		default:
			issue := widthIssue("rfc3339 images that are not canonical, on which the PostgreSQL and DuckDB routes can disagree, in "+main, f, severityError)
			issue.details += fmt.Sprintf(" repair=%q (%s)", f.NonCanonical, f.NonCanonical.Explain(f.Canonical))
			issues = append(issues, issue)
		}
	}
	return issues, nil
}
