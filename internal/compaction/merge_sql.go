package compaction

import (
	"fmt"
	"strings"
)

// mergeLWWOrderBy mirrors the federated read path's version fold EXACTLY
// (internal/sqlgen/advanced_query_template_duckdb.go, the `ranked` CTE):
// `ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC` over
// the parquet aliases ver_ts=changed_at, deleted_ts=deleted_at. Every cold
// file shares source_tier_priority=1, so within a parquet-only merge that
// term is constant and drops out. Since #274 both exporters encode live rows
// as deleted_at=0, so on an equal-ver_ts live/live tie deleted_at DESC is
// degenerate too: the copies are value-identical and the winner identity is
// unspecified — the contract is multiplicity + value, never scan order. A
// tombstone (deleted_at > 0) beating a live 0/NULL on the tie remains the
// hard delete-wins contract. NULLS LAST is DuckDB's default null order but is
// spelled out so the merge cannot diverge from the reader if a session ever
// reconfigures it: pre-#274 delta objects still carry NULL for live rows
// until compaction retires them, and 0-sorts-before-NULL keeps those legacy
// ties resolving the same way the reader does.
const mergeLWWOrderBy = "changed_at DESC, deleted_at DESC NULLS LAST, row_id ASC"

// defaultMergeCopyOptions matches the CDC exporters' parquet COPY options
// (internal/cdc/export_sql_builder.go buildParquetCopyOptions defaults).
const defaultMergeCopyOptions = "FORMAT PARQUET, PARQUET_VERSION V2, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 3"

// validateMergeURI rejects URIs that cannot be embedded in a single-quoted
// DuckDB literal. Production keys are prefix/schemaID/uuid-ish and never
// carry quotes; this guards against SQL breakage, not hostile input.
func validateMergeURI(uri string) error {
	if uri == "" {
		return fmt.Errorf("empty parquet URI")
	}
	if strings.ContainsAny(uri, `'";`) {
		return fmt.Errorf("parquet URI %q contains a quote or semicolon; cannot embed in DuckDB SQL", uri)
	}
	return nil
}

// buildMergeSQL renders the full-schema LWW merge: every source file's
// versions are folded per row_id with the read path's exact ordering, rn=1
// survivors drop tombstones entirely (safe ONLY because the source set is
// the schema's complete base+delta inventory — a partial merge could
// resurrect a row via an unmerged older version), and survivor deleted_at is
// normalized to 0, the canonical live encoding on every cold tier (#274).
// All remaining columns — changed_at above all (#210) — pass through
// verbatim.
func buildMergeSQL(sourceURIs []string, tmpURI, copyOptions string) (string, error) {
	if len(sourceURIs) == 0 {
		return "", fmt.Errorf("merge needs at least one source parquet URI")
	}
	if err := validateMergeURI(tmpURI); err != nil {
		return "", fmt.Errorf("tmp target: %w", err)
	}
	quoted := make([]string, 0, len(sourceURIs))
	for _, uri := range sourceURIs {
		if err := validateMergeURI(uri); err != nil {
			return "", fmt.Errorf("merge source: %w", err)
		}
		quoted = append(quoted, "'"+uri+"'")
	}
	if copyOptions == "" {
		copyOptions = defaultMergeCopyOptions
	}

	return fmt.Sprintf(`COPY (
  SELECT * EXCLUDE (_rn) REPLACE (COALESCE(deleted_at, 0) AS deleted_at)
  FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY row_id
      ORDER BY %s
    ) AS _rn
    FROM read_parquet([%s], union_by_name=true)
  )
  WHERE _rn = 1 AND (deleted_at IS NULL OR deleted_at = 0)
) TO '%s' (%s)`, mergeLWWOrderBy, strings.Join(quoted, ", "), tmpURI, copyOptions), nil
}

// buildMergeRowsInSQL counts the raw version rows across the source files
// (CompactionResult.RowsIn).
func buildMergeRowsInSQL(sourceURIs []string) (string, error) {
	if len(sourceURIs) == 0 {
		return "", fmt.Errorf("merge needs at least one source parquet URI")
	}
	quoted := make([]string, 0, len(sourceURIs))
	for _, uri := range sourceURIs {
		if err := validateMergeURI(uri); err != nil {
			return "", fmt.Errorf("rows-in source: %w", err)
		}
		quoted = append(quoted, "'"+uri+"'")
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM read_parquet([%s], union_by_name=true)", strings.Join(quoted, ", ")), nil
}

// buildMergeStatsSQL computes the merged file's manifest metadata. COALESCEs
// cover the zero-row merge of an all-tombstone schema: the entry is still
// written (RowCount=0) so the manifest never empties out — zero entries
// would send manifest-driven reads to the legacy glob fallback
// (internal/manifest/query_source.go), resurrecting undeleted leftovers.
func buildMergeStatsSQL(tmpURI string) (string, error) {
	if err := validateMergeURI(tmpURI); err != nil {
		return "", fmt.Errorf("stats target: %w", err)
	}
	return fmt.Sprintf(
		`SELECT COUNT(*),
       COALESCE(MIN(CAST(row_id AS VARCHAR)), ''),
       COALESCE(MAX(CAST(row_id AS VARCHAR)), ''),
       COALESCE(MIN(changed_at), 0),
       COALESCE(MAX(changed_at), 0)
FROM read_parquet('%s')`, tmpURI), nil
}
