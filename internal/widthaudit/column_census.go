package widthaudit

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// A column-bound integer whose column is wider than its declared type can
// hold an image that type cannot (#618, #708). A bigint bound to a double_*
// column is stored as its float64 image, under the contract eav_data holds
// an EAV-only bigint to (#590): integral and inside [-2^53, 2^53]. A
// smallint or integer bound to a wider column (integer to bigint_* or
// double_*, smallint to integer_*, bigint_* or double_*) is held to its own
// range, and the OLTP read refuses any other image (#708). The write funnel
// judges the declared type first, so a row written before it, or around it,
// is the only way such an image gets there. This census reports those rows.

// columnHoldsWider reports whether column is an entity_main column wider
// than declared, so that it can hold an image declared cannot: the bindings
// the census scans. The match is exact, as the writer's column allowlist is:
// a binding to any other name is not a place a value is stored
// (schemameta.ValidateColumnBinding refuses it, #557). A column as narrow as
// declared, or narrower, holds the value at its own width.
func columnHoldsWider(declared forma.ValueType, column string) bool {
	switch declared {
	case forma.ValueTypeBigInt:
		return slices.Contains(model.DoubleColumns, column)
	case forma.ValueTypeInteger:
		return slices.Contains(model.BigintColumns, column) || slices.Contains(model.DoubleColumns, column)
	case forma.ValueTypeSmallInt:
		return slices.Contains(model.IntegerColumns, column) || slices.Contains(model.BigintColumns, column) ||
			slices.Contains(model.DoubleColumns, column)
	}
	return false
}

// columnBounds is the inclusive range a column-bound target's image must lie
// in: the declared type's range, and for a bigint the ±2^53 its float64
// image keeps exactly. Every bound is a float64 integer, so comparing a
// double image against it is exact.
func columnBounds(declared forma.ValueType) (lo, hi int64) {
	switch declared {
	case forma.ValueTypeSmallInt:
		return math.MinInt16, math.MaxInt16
	case forma.ValueTypeInteger:
		return math.MinInt32, math.MaxInt32
	}
	return -maxBigintImage, maxBigintImage
}

// caseOverColumns renders a CASE picking the target's column out of columns,
// each read through cast ("" reads it as stored).
func caseOverColumns(columns []string, cast string) string {
	arms := make([]string, 0, len(columns))
	for _, col := range columns {
		arms = append(arms, fmt.Sprintf("WHEN '%s' THEN m.%s%s", col, sqlutil.SanitizeIdentifier(col), cast))
	}
	return "CASE w.col " + strings.Join(arms, " ") + " END"
}

// BuildColumnCensusQuery renders the entity_main census over the given
// column-bound targets. Each target binds (schema_id, attr_id, column, lo,
// hi) through a VALUES list, and a CASE over the candidate columns picks the
// target's column, so no identifier comes from schema metadata. A double_*
// column is read as float8 (image) and an integer_* or bigint_* column as
// bigint (exact): each is compared in its own type, so neither rounds.
//
// The double predicate keeps a fraction, a value outside [lo, hi], ±Infinity
// and NaN, which Postgres orders above every number. The comparison is exact:
// every bound is a float8 integer, and the column is a float8.
func BuildColumnCensusQuery(tables Tables, targets []Target) (string, []any) {
	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*5)
	for _, t := range targets {
		lo, hi := columnBounds(t.Declared)
		n := len(args)
		values = append(values, fmt.Sprintf("($%d::smallint, $%d::smallint, $%d::text, $%d::bigint, $%d::bigint)", n+1, n+2, n+3, n+4, n+5))
		args = append(args, t.SchemaID, t.AttrID, t.Column, lo, hi)
	}
	exact := make([]string, 0, len(model.IntegerColumns)+len(model.BigintColumns))
	exact = append(append(exact, model.IntegerColumns...), model.BigintColumns...)

	query := fmt.Sprintf(`SELECT m.ltbase_schema_id, w.attr_id, m.ltbase_row_id, v.image, v.exact,
  COALESCE(cl.pending, FALSE), COALESCE(cl.last_flushed_at, 0)
FROM %s AS m
JOIN (VALUES %s) AS w(schema_id, attr_id, col, lo, hi)
  ON m.ltbase_schema_id = w.schema_id
CROSS JOIN LATERAL (SELECT %s AS image, %s AS exact) AS v
%s
WHERE (v.image IS NOT NULL AND (v.image < w.lo::float8 OR v.image > w.hi::float8 OR v.image <> TRUNC(v.image)))
   OR (v.exact IS NOT NULL AND (v.exact < w.lo OR v.exact > w.hi))
ORDER BY m.ltbase_schema_id, w.attr_id, m.ltbase_row_id`,
		sqlutil.SanitizeIdentifier(tables.EntityMain), strings.Join(values, ", "),
		caseOverColumns(model.DoubleColumns, ""), caseOverColumns(exact, "::bigint"),
		changeLogJoin(tables, "m.ltbase_schema_id", "m.ltbase_row_id"))
	return query, args
}

// censusColumns returns every entity_main row whose column holds an image
// a column-bound target's declared type cannot. Targets without a table to
// read are an error: skipping them would report a clean census over rows
// nothing looked at.
func censusColumns(ctx context.Context, q Querier, tables Tables, targets []Target) ([]Finding, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	if tables.EntityMain == "" {
		t := targets[0]
		return nil, fmt.Errorf("no entity main table is configured to scan %d integer attribute(s) bound to a wider column (first: schema=%s attribute=%s column=%s)",
			len(targets), t.SchemaName, t.AttrName, t.Column)
	}
	byKey := targetIndex(targets)

	query, args := BuildColumnCensusQuery(tables, targets)
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query integer-width census over %s: %w", tables.EntityMain, err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var f Finding
		var schemaID, attrID int16
		var image *float64
		var exact *int64
		if err := rows.Scan(&schemaID, &attrID, &f.RowID, &image, &exact, &f.Pending, &f.LastFlushedAt); err != nil {
			return nil, fmt.Errorf("scan integer-width census row from %s: %w", tables.EntityMain, err)
		}
		target, ok := byKey[[2]int16{schemaID, attrID}]
		if !ok {
			return nil, fmt.Errorf("integer-width census over %s returned schema_id=%d attr_id=%d, which is not a census target", tables.EntityMain, schemaID, attrID)
		}
		f.Target = target
		switch {
		case image != nil:
			f.StoredValue = formatDoubleImage(*image)
		case exact != nil:
			f.StoredValue = strconv.FormatInt(*exact, 10)
		default:
			return nil, fmt.Errorf("integer-width census over %s returned row %s of schema_id=%d attr_id=%d with no value in column %s",
				tables.EntityMain, f.RowID, schemaID, attrID, target.Column)
		}
		findings = append(findings, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate integer-width census over %s: %w", tables.EntityMain, err)
	}
	return findings, nil
}

// formatDoubleImage prints a stored float8 the way the write funnel's
// refusal names an image (transform.formatFitValue): a whole number in exact
// plain digits, a fraction in its shortest round-trip form, and a magnitude
// from 1e21 in exponent form. The image is scanned as a float8 and printed
// here because Postgres's own casts are not that value: float8::numeric
// rounds to 15 significant digits. The non-finite values take the Postgres
// spelling.
func formatDoubleImage(image float64) string {
	switch {
	case math.IsNaN(image):
		return "NaN"
	case math.IsInf(image, 1):
		return "Infinity"
	case math.IsInf(image, -1):
		return "-Infinity"
	case math.Abs(image) >= 1e21:
		return strconv.FormatFloat(image, 'g', -1, 64)
	case image == math.Trunc(image):
		return strconv.FormatFloat(image, 'f', 0, 64)
	}
	return strconv.FormatFloat(image, 'f', -1, 64)
}
