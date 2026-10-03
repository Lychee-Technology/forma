// Package widthaudit finds stored values that do not fit the attribute's
// declared integer width, and says which tier disagreement each one can
// cause (#501). It reads eav_data.value_numeric for EAV-only attributes and
// the double_* columns of entity_main for a bigint bound to one (#618).
//
// #384 made the write funnel reject such values and moved the DuckDB
// projection of EAV-only smallint/integer to storage width DOUBLE. Rows that
// were written before that change still exist. A row exported before it has a
// parquet copy made by TRY_CAST(value_numeric AS INTEGER/SMALLINT): an
// out-of-range value became NULL and a non-integral one was rounded. Merging
// parquet never recovers either, so the federated route serves the damaged
// copy while the OLTP route reads the true value. Declared bigint still
// projects at BIGINT on every DuckDB leg, so its out-of-contract values
// diverge whether or not they were ever exported. Its contract is the range
// the float64 image eav_data keeps exactly, [-2^53, 2^53] (#590): a value
// stored past that was rounded on the write and reads back as the rounded
// value on every route, so the census reports it too. A bigint bound to a
// double_* column is stored as the same image under the same contract, so
// the census scans those columns as well (column_census.go).
package widthaudit

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/numutil"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// Querier is the read surface the census needs; *pgxpool.Pool satisfies it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Tables names the storage the census reads. EntityMain is needed only when
// a target is column-bound. ChangeLog may be empty for a deployment without
// CDC: nothing was ever exported there, so only the bigint class can be
// reported.
type Tables struct {
	EAV        string
	EntityMain string
	ChangeLog  string
}

// Target is one attribute whose stored value the census checks against an
// integer width: an EAV-only smallint/integer/bigint, or a bigint bound to a
// double_* column. For a list attribute, Declared is the items type, since
// each element is stored as its own eav_data row.
type Target struct {
	SchemaID   int16
	SchemaName string
	AttrID     int16
	AttrName   string
	Declared   forma.ValueType
	// Column is the double_* column of entity_main a column-bound bigint is
	// stored in. It is empty for an EAV-only target.
	Column string
}

// Finding is one stored value that does not fit its target's width: an
// eav_data row, or the target's column of an entity_main row. StoredValue is
// the NUMERIC text exactly as Postgres holds it for eav_data, and the float8
// in plain digits for a column (formatDoubleImage).
type Finding struct {
	Target
	RowID        uuid.UUID
	ArrayIndices string
	StoredValue  string
	// Pending means slot 0 of change_log is set: the dirty set routes reads
	// of the row to Postgres, and the next flush re-exports it.
	Pending bool
	// LastFlushedAt is the newest non-zero flushed_at of the row, or 0 when
	// the row was never exported.
	LastFlushedAt int64
}

// Targets lists every EAV-only smallint/integer/bigint attribute, including
// list attributes with such items, and every bigint bound to a double_*
// column, in (schema_id, attr_id) order. Other column-bound attributes are
// excluded: a smallint, integer or bigint column holds the value at its own
// integer width.
func Targets(cache *schemameta.MetadataCache) []Target {
	var targets []Target
	for _, schemaName := range cache.ListSchemas() {
		schemaID, ok := cache.GetSchemaID(schemaName)
		if !ok {
			continue
		}
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		for _, meta := range schemaCache {
			declared, column, ok := targetStorage(meta)
			if !ok {
				continue
			}
			targets = append(targets, Target{
				SchemaID: schemaID, SchemaName: schemaName,
				AttrID: meta.AttributeID, AttrName: meta.AttributeName, Declared: declared, Column: column,
			})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].SchemaID != targets[j].SchemaID {
			return targets[i].SchemaID < targets[j].SchemaID
		}
		return targets[i].AttrID < targets[j].AttrID
	})
	return targets
}

// targetStorage says whether meta is a census target, with the integer type
// its stored value is held to and, for a column-bound one, its column.
func targetStorage(meta forma.AttributeMetadata) (declared forma.ValueType, column string, ok bool) {
	if binding := meta.ColumnBinding; binding != nil {
		column = string(binding.ColumnName)
		return meta.ValueType, column, meta.ValueType == forma.ValueTypeBigInt && isDoubleColumn(column)
	}
	declared = meta.ValueType
	if declared == forma.ValueTypeList {
		declared = meta.EffectiveItemsType()
	}
	_, _, ok = integerBounds(declared)
	return declared, "", ok
}

// maxBigintImage is the largest magnitude the float64 image of a bigint
// keeps exactly; the write funnel admits exactly this range for eav_data and
// a double_* column (transform.checkBigintImageFit, #590), from the same
// constant.
const maxBigintImage = numutil.MaxExactFloat64Integer

// integerBounds is the inclusive range of an integer width as exact NUMERIC
// text. It matches the write funnel's fit rule (transform.checkIntegerFit
// for smallint/integer, transform.checkBigintImageFit for bigint), which
// also rejects non-integral values.
func integerBounds(vt forma.ValueType) (lo, hi string, ok bool) {
	switch vt {
	case forma.ValueTypeSmallInt:
		return strconv.Itoa(math.MinInt16), strconv.Itoa(math.MaxInt16), true
	case forma.ValueTypeInteger:
		return strconv.Itoa(math.MinInt32), strconv.Itoa(math.MaxInt32), true
	case forma.ValueTypeBigInt:
		return strconv.Itoa(-maxBigintImage), strconv.Itoa(maxBigintImage), true
	}
	return "", "", false
}

// changeLogJoin renders the join that supplies a finding's change_log facts
// as cl(pending, last_flushed_at), for the row the two SQL expressions
// identify. It is a LATERAL subquery keyed on the table's primary key, so
// only the rows the census keeps pay for the lookup.
func changeLogJoin(tables Tables, schemaID, rowID string) string {
	if tables.ChangeLog == "" {
		return "CROSS JOIN (SELECT FALSE AS pending, 0::bigint AS last_flushed_at) AS cl"
	}
	return fmt.Sprintf(`LEFT JOIN LATERAL (
  SELECT COALESCE(BOOL_OR(c.flushed_at = 0), FALSE) AS pending,
         COALESCE(MAX(c.flushed_at) FILTER (WHERE c.flushed_at > 0), 0) AS last_flushed_at
  FROM %s AS c
  WHERE c.schema_id = %s AND c.row_id = %s
) AS cl ON TRUE`, sqlutil.SanitizeIdentifier(tables.ChangeLog), schemaID, rowID)
}

// BuildCensusQuery renders the eav_data census over the given EAV-only
// targets. Each target binds (schema_id, attr_id, lo, hi) through a VALUES
// list.
func BuildCensusQuery(tables Tables, targets []Target) (string, []any) {
	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*4)
	for _, t := range targets {
		lo, hi, _ := integerBounds(t.Declared)
		n := len(args)
		values = append(values, fmt.Sprintf("($%d::smallint, $%d::smallint, $%d::numeric, $%d::numeric)", n+1, n+2, n+3, n+4))
		args = append(args, t.SchemaID, t.AttrID, lo, hi)
	}

	query := fmt.Sprintf(`SELECT e.schema_id, e.attr_id, e.row_id, e.array_indices, e.value_numeric::text,
  COALESCE(cl.pending, FALSE), COALESCE(cl.last_flushed_at, 0)
FROM %s AS e
JOIN (VALUES %s) AS w(schema_id, attr_id, lo, hi)
  ON e.schema_id = w.schema_id AND e.attr_id = w.attr_id
%s
WHERE e.value_numeric IS NOT NULL
  AND (e.value_numeric < w.lo OR e.value_numeric > w.hi OR e.value_numeric <> TRUNC(e.value_numeric))
ORDER BY e.schema_id, e.attr_id, e.row_id, e.array_indices`,
		sqlutil.SanitizeIdentifier(tables.EAV), strings.Join(values, ", "), changeLogJoin(tables, "e.schema_id", "e.row_id"))
	return query, args
}

// Census returns every stored value under the targets that does not fit the
// declared width: the eav_data findings first, then the entity_main ones.
// Each table is queried only when it has targets.
func Census(ctx context.Context, q Querier, tables Tables, targets []Target) ([]Finding, error) {
	var eavTargets, columnTargets []Target
	for _, t := range targets {
		if t.Column != "" {
			columnTargets = append(columnTargets, t)
			continue
		}
		eavTargets = append(eavTargets, t)
	}
	findings, err := censusEAV(ctx, q, tables, eavTargets)
	if err != nil {
		return nil, fmt.Errorf("EAV-only attributes: %w", err)
	}
	columnFindings, err := censusColumns(ctx, q, tables, columnTargets)
	if err != nil {
		return nil, fmt.Errorf("column-bound attributes: %w", err)
	}
	return append(findings, columnFindings...), nil
}

// targetIndex keys targets by (schema_id, attr_id) to resolve a census row.
func targetIndex(targets []Target) map[[2]int16]Target {
	byKey := make(map[[2]int16]Target, len(targets))
	for _, t := range targets {
		byKey[[2]int16{t.SchemaID, t.AttrID}] = t
	}
	return byKey
}

func censusEAV(ctx context.Context, q Querier, tables Tables, targets []Target) ([]Finding, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	byKey := targetIndex(targets)

	query, args := BuildCensusQuery(tables, targets)
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query integer-width census over %s: %w", tables.EAV, err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var f Finding
		var schemaID, attrID int16
		if err := rows.Scan(&schemaID, &attrID, &f.RowID, &f.ArrayIndices, &f.StoredValue, &f.Pending, &f.LastFlushedAt); err != nil {
			return nil, fmt.Errorf("scan integer-width census row from %s: %w", tables.EAV, err)
		}
		target, ok := byKey[[2]int16{schemaID, attrID}]
		if !ok {
			return nil, fmt.Errorf("integer-width census over %s returned schema_id=%d attr_id=%d, which is not a census target", tables.EAV, schemaID, attrID)
		}
		f.Target = target
		findings = append(findings, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate integer-width census over %s: %w", tables.EAV, err)
	}
	return findings, nil
}
