// Package widthaudit finds stored values that do not fit the attribute's
// declared integer width, and says what each one means for the routes
// (#501). It reads eav_data.value_numeric for EAV-only attributes and the
// entity_main column of an integer bound to a column wider than its
// declared type: a bigint in a double_* column (#618), a smallint or integer
// in a wider one (#708).
//
// #384 made the write funnel reject such values and moved the DuckDB
// projection of EAV-only smallint/integer to storage width DOUBLE. Rows that
// were written before that change still exist. The OLTP read refuses them
// (#729), so only rewriting the attribute repairs one. A row exported before
// #384 also has a parquet copy made by TRY_CAST(value_numeric AS
// INTEGER/SMALLINT): an out-of-range value became NULL and a non-integral one
// was rounded, and the federated route serves that copy until the rewrite's
// flush replaces it (Finding.ExportPredates). Declared bigint still
// projects at BIGINT on every DuckDB leg, so its out-of-contract values
// diverge whether or not they were ever exported. Its contract is the range
// the float64 image eav_data keeps exactly, [-2^53, 2^53] (#590): a value
// stored past that was rounded on the write and reads back as the rounded
// value on every route, so the census reports it too. A bigint bound to a
// double_* column is stored as the same image under the same contract, so
// the census scans those columns as well, and the wider columns a smallint or
// integer can be bound to (column_census.go).
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
// integer width: an EAV-only smallint/integer/bigint, or an integer bound to
// a column wider than its declared type (columnHoldsWider). For a list
// attribute, Declared is the items type, since each element is stored as its
// own eav_data row.
type Target struct {
	SchemaID   int16
	SchemaName string
	AttrID     int16
	AttrName   string
	Declared   forma.ValueType
	// Column is the entity_main column a column-bound target is stored in.
	// It is empty for an EAV-only target.
	Column string
}

// Finding is one stored value that does not fit its target's width, or,
// from DomainCensus, its type's domain: an eav_data row, or the target's
// column of an entity_main row. StoredValue is the NUMERIC text exactly as
// Postgres holds it for eav_data, the float8 in plain digits for a double_*
// column (formatDoubleImage), the exact digits for an integer_* or bigint_*
// column, and Go-quoted text for a value read from value_text or a text_*
// column.
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
// list attributes with such items, and every integer bound to a column wider
// than its declared type, in (schema_id, attr_id) order. Other column-bound
// attributes are excluded: a column as narrow as the declared type holds the
// value at its own width, and numeric has no integer width.
func Targets(cache *schemameta.MetadataCache) []Target {
	return collectTargets(cache, targetStorage, false)
}

// TargetsWithRetainedEAV lists Targets plus the eav_data rows each
// column-bound smallint/integer/bigint kept from before an EAV-to-bound
// promotion, held to the declared width (see collectTargets). The OLTP read
// decodes those rows under the declared type and refuses one outside it
// (#729), so the baseline audit and validate-schema-consistency both census
// them; a retained row's finding names no column and classifies as an
// eav_data value.
func TargetsWithRetainedEAV(cache *schemameta.MetadataCache) []Target {
	return collectTargets(cache, targetStorage, true)
}

// collectTargets lists, in (schema_id, attr_id) order, every attribute
// storage selects, held to the type it answers and stored in the column it
// names ("" for eav_data).
//
// With retainedEAV set, a column-bound attribute is also offered to storage
// as if it were unbound. An EAV-to-bound promotion leaves the eav_data rows
// written before it in place, and the whole-row read aggregates every
// eav_data row of the row and decodes each under the declared type, bound
// or not (internal.rowSelectWithAttributes, transform.fromPersistentRecord).
// Such an attribute can then yield two targets, its column first.
func collectTargets(cache *schemameta.MetadataCache, storage func(forma.AttributeMetadata) (forma.ValueType, string, bool), retainedEAV bool) []Target {
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
			views := []forma.AttributeMetadata{meta}
			if retainedEAV && meta.ColumnBinding != nil {
				unbound := meta
				unbound.ColumnBinding = nil
				views = append(views, unbound)
			}
			for _, view := range views {
				declared, column, ok := storage(view)
				if !ok {
					continue
				}
				targets = append(targets, Target{
					SchemaID: schemaID, SchemaName: schemaName,
					AttrID: meta.AttributeID, AttrName: meta.AttributeName, Declared: declared, Column: column,
				})
			}
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].SchemaID != targets[j].SchemaID {
			return targets[i].SchemaID < targets[j].SchemaID
		}
		if targets[i].AttrID != targets[j].AttrID {
			return targets[i].AttrID < targets[j].AttrID
		}
		return targets[i].Column > targets[j].Column
	})
	return targets
}

// targetStorage says whether meta is a census target, with the integer type
// its stored value is held to and, for a column-bound one, its column.
func targetStorage(meta forma.AttributeMetadata) (declared forma.ValueType, column string, ok bool) {
	if binding := meta.ColumnBinding; binding != nil {
		column = string(binding.ColumnName)
		return meta.ValueType, column, columnHoldsWider(meta.ValueType, column)
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
	query, args := BuildCensusQuery(tables, targets)
	return queryEAVFindings(ctx, q, tables.EAV, "integer-width census", query, args, targets)
}

// queryEAVFindings runs a census query over eav_data that selects (schema_id,
// attr_id, row_id, array_indices, value_numeric text, pending,
// last_flushed_at) and resolves each row to its target. census names the
// query in errors.
func queryEAVFindings(ctx context.Context, q Querier, table, census, query string, args []any, targets []Target) ([]Finding, error) {
	byKey := targetIndex(targets)
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query %s over %s: %w", census, table, err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var f Finding
		var schemaID, attrID int16
		if err := rows.Scan(&schemaID, &attrID, &f.RowID, &f.ArrayIndices, &f.StoredValue, &f.Pending, &f.LastFlushedAt); err != nil {
			return nil, fmt.Errorf("scan %s row from %s: %w", census, table, err)
		}
		target, ok := byKey[[2]int16{schemaID, attrID}]
		if !ok {
			return nil, fmt.Errorf("%s over %s returned schema_id=%d attr_id=%d, which is not a census target", census, table, schemaID, attrID)
		}
		f.Target = target
		findings = append(findings, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s over %s: %w", census, table, err)
	}
	return findings, nil
}
