package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// UncoveredRow is one row_id in an orphan parquet whose newest uncovered
// version is not superseded by any manifest-listed file. Tombstone reports
// whether that version is a delete marker — re-appending an uncovered
// tombstone RESTORES a lost delete, while re-appending an uncovered live
// version of a Postgres-deleted row resurrects it.
type UncoveredRow struct {
	RowID     string
	Tombstone bool
}

// UncoveredRows returns, per row_id, the orphan parquet's versions that no
// listed file supersedes. Coverage is version-aware: a listed version with
// changed_at >= the orphan version's covers it (equal ties resolve
// base-wins in LWW, #183), so a same-row lost update — an orphan carrying a
// NEWER version than anything listed — still counts as uncovered. The
// manifest-reconcile tool (#203) builds its repair verdict on this: a
// row_id-only anti-join would misclassify lost updates as deletable
// leftovers.
func UncoveredRows(ctx context.Context, db *sql.DB, orphanURI string, listedURIs []string) ([]UncoveredRow, error) {
	if err := validateMergeURI(orphanURI); err != nil {
		return nil, fmt.Errorf("uncovered-rows orphan: %w", err)
	}
	notSuperseded := ""
	if len(listedURIs) > 0 {
		quoted := make([]string, 0, len(listedURIs))
		for _, uri := range listedURIs {
			if err := validateMergeURI(uri); err != nil {
				return nil, fmt.Errorf("uncovered-rows listed file: %w", err)
			}
			quoted = append(quoted, "'"+uri+"'")
		}
		notSuperseded = fmt.Sprintf(`
WHERE NOT EXISTS (
  SELECT 1 FROM read_parquet([%s], union_by_name=true) l
  WHERE l.row_id = o.row_id AND l.changed_at >= o.changed_at
)`, strings.Join(quoted, ", "))
	}
	// arg_max picks the newest uncovered version per row; its deleted_at
	// decides the tombstone flag.
	query := fmt.Sprintf(`
SELECT CAST(row_id AS VARCHAR) AS rid,
       (arg_max(COALESCE(deleted_at, 0), changed_at) > 0) AS tomb
FROM read_parquet('%s') o%s
GROUP BY row_id
ORDER BY rid`, orphanURI, notSuperseded)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query uncovered rows of %s: %w", orphanURI, err)
	}
	defer func() { _ = rows.Close() }()

	var uncovered []UncoveredRow
	for rows.Next() {
		var row UncoveredRow
		if err := rows.Scan(&row.RowID, &row.Tombstone); err != nil {
			return nil, fmt.Errorf("scan uncovered row of %s: %w", orphanURI, err)
		}
		uncovered = append(uncovered, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate uncovered rows of %s: %w", orphanURI, err)
	}
	return uncovered, nil
}
