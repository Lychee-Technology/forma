// Package widthaudit finds EAV rows whose stored value_numeric does not fit
// the attribute's declared integer width, and says which tier disagreement
// each one can cause (#501).
//
// #384 made the write funnel reject such values and moved the DuckDB
// projection of EAV-only smallint/integer to storage width DOUBLE. Rows that
// were written before that change still exist. A row exported before it has a
// parquet copy made by TRY_CAST(value_numeric AS INTEGER/SMALLINT): an
// out-of-range value became NULL and a non-integral one was rounded. Merging
// parquet never recovers either, so the federated route serves the damaged
// copy while the OLTP route reads the true value. Declared bigint still
// projects at BIGINT on every DuckDB leg, so its out-of-contract values
// diverge whether or not they were ever exported.
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
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// Querier is the read surface the census needs; *pgxpool.Pool satisfies it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Tables names the storage the census reads. ChangeLog may be empty for a
// deployment without CDC: nothing was ever exported there, so only the
// bigint class can be reported.
type Tables struct {
	EAV       string
	ChangeLog string
}

// Target is one EAV-only attribute whose declared type carries an integer
// width. For a list attribute, Declared is the items type, since each
// element is stored as its own eav_data row.
type Target struct {
	SchemaID   int16
	SchemaName string
	AttrID     int16
	AttrName   string
	Declared   forma.ValueType
}

// Finding is one eav_data row whose value does not fit its target's width.
// StoredValue is the NUMERIC text exactly as Postgres holds it.
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
// list attributes with such items, in (schema_id, attr_id) order. Attributes
// bound to a main column are excluded: their storage is the physical column,
// whose width is the declared one.
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
			if meta.Location() != forma.AttributeStorageLocationEAV {
				continue
			}
			declared := meta.ValueType
			if declared == forma.ValueTypeList {
				declared = meta.EffectiveItemsType()
			}
			if _, _, ok := integerBounds(declared); !ok {
				continue
			}
			targets = append(targets, Target{
				SchemaID: schemaID, SchemaName: schemaName,
				AttrID: meta.AttributeID, AttrName: meta.AttributeName, Declared: declared,
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

// integerBounds is the inclusive range of an integer width as exact NUMERIC
// text. It matches the write funnel's fit rule (transform.checkIntegerFit),
// which also rejects non-integral values.
func integerBounds(vt forma.ValueType) (lo, hi string, ok bool) {
	switch vt {
	case forma.ValueTypeSmallInt:
		return strconv.Itoa(math.MinInt16), strconv.Itoa(math.MaxInt16), true
	case forma.ValueTypeInteger:
		return strconv.Itoa(math.MinInt32), strconv.Itoa(math.MaxInt32), true
	case forma.ValueTypeBigInt:
		return strconv.FormatInt(math.MinInt64, 10), strconv.FormatInt(math.MaxInt64, 10), true
	}
	return "", "", false
}

// BuildCensusQuery renders the census over the given targets. Each target
// binds (schema_id, attr_id, lo, hi) through a VALUES list. The change_log
// facts come from a LATERAL subquery keyed on the table's primary key, so
// only the rows that are out of width pay for the lookup.
func BuildCensusQuery(tables Tables, targets []Target) (string, []any) {
	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*4)
	for _, t := range targets {
		lo, hi, _ := integerBounds(t.Declared)
		n := len(args)
		values = append(values, fmt.Sprintf("($%d::smallint, $%d::smallint, $%d::numeric, $%d::numeric)", n+1, n+2, n+3, n+4))
		args = append(args, t.SchemaID, t.AttrID, lo, hi)
	}

	changeLogJoin := "CROSS JOIN (SELECT FALSE AS pending, 0::bigint AS last_flushed_at) AS cl"
	if tables.ChangeLog != "" {
		changeLogJoin = fmt.Sprintf(`LEFT JOIN LATERAL (
  SELECT COALESCE(BOOL_OR(c.flushed_at = 0), FALSE) AS pending,
         COALESCE(MAX(c.flushed_at) FILTER (WHERE c.flushed_at > 0), 0) AS last_flushed_at
  FROM %s AS c
  WHERE c.schema_id = e.schema_id AND c.row_id = e.row_id
) AS cl ON TRUE`, sqlutil.SanitizeIdentifier(tables.ChangeLog))
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
		sqlutil.SanitizeIdentifier(tables.EAV), strings.Join(values, ", "), changeLogJoin)
	return query, args
}

// Census returns every eav_data row under the targets whose value does not
// fit the declared width. With no targets it issues no query.
func Census(ctx context.Context, q Querier, tables Tables, targets []Target) ([]Finding, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	byKey := make(map[[2]int16]Target, len(targets))
	for _, t := range targets {
		byKey[[2]int16{t.SchemaID, t.AttrID}] = t
	}

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
