package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// UncoveredRowIDs returns the distinct row_ids present in the orphan parquet
// that appear in none of the listed parquet files. The manifest-reconcile
// tool (#203) uses it to decide whether a delta-shaped orphan still carries
// rows the manifest-listed inventory does not cover: only such files are
// candidates for re-appending, everything else is a compaction leftover
// whose re-registration could resurrect rows whose tombstones the merge
// already dropped.
func UncoveredRowIDs(ctx context.Context, db *sql.DB, orphanURI string, listedURIs []string) ([]string, error) {
	if err := validateMergeURI(orphanURI); err != nil {
		return nil, fmt.Errorf("uncovered-rows orphan: %w", err)
	}
	query := fmt.Sprintf(
		"SELECT DISTINCT CAST(row_id AS VARCHAR) FROM read_parquet('%s') ORDER BY 1", orphanURI)
	if len(listedURIs) > 0 {
		quoted := make([]string, 0, len(listedURIs))
		for _, uri := range listedURIs {
			if err := validateMergeURI(uri); err != nil {
				return nil, fmt.Errorf("uncovered-rows listed file: %w", err)
			}
			quoted = append(quoted, "'"+uri+"'")
		}
		query = fmt.Sprintf(
			`SELECT DISTINCT CAST(o.row_id AS VARCHAR)
FROM read_parquet('%s') o
WHERE NOT EXISTS (
  SELECT 1 FROM read_parquet([%s], union_by_name=true) l WHERE l.row_id = o.row_id
)
ORDER BY 1`, orphanURI, strings.Join(quoted, ", "))
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query uncovered row ids of %s: %w", orphanURI, err)
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan uncovered row id of %s: %w", orphanURI, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate uncovered row ids of %s: %w", orphanURI, err)
	}
	return ids, nil
}
