package widthaudit

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// A bigint bound to a double_* column is stored as its float64 image, under
// the contract eav_data holds an EAV-only bigint to (#590): integral and
// inside [-2^53, 2^53]. The column itself admits any float8, so a row
// written before the funnel judged the image, or around it, can hold a value
// outside the contract. This census reports those rows (#618).

// isDoubleColumn reports whether name is a double_* column entity_main has.
// The match is exact, as the writer's column allowlist is: a binding to any
// other name is not a place a value is stored (schemameta.ValidateColumnBinding
// refuses it, #557).
func isDoubleColumn(name string) bool {
	return slices.Contains(model.DoubleColumns, name)
}

// BuildColumnCensusQuery renders the entity_main census over the given
// column-bound targets. Each target binds (schema_id, attr_id, column)
// through a VALUES list, and a CASE over the double_* columns picks the
// target's column, so no identifier comes from schema metadata.
//
// The predicate keeps a whole value past ±2^53, a fraction, a magnitude past
// int64, ±Infinity and NaN, which Postgres orders above every number. The
// comparison is exact: 2^53 is a float8, and the column is one.
func BuildColumnCensusQuery(tables Tables, targets []Target) (string, []any) {
	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*3)
	for _, t := range targets {
		n := len(args)
		values = append(values, fmt.Sprintf("($%d::smallint, $%d::smallint, $%d::text)", n+1, n+2, n+3))
		args = append(args, t.SchemaID, t.AttrID, t.Column)
	}

	arms := make([]string, 0, len(model.DoubleColumns))
	for _, col := range model.DoubleColumns {
		arms = append(arms, fmt.Sprintf("WHEN '%s' THEN m.%s", col, sqlutil.SanitizeIdentifier(col)))
	}

	query := fmt.Sprintf(`SELECT m.ltbase_schema_id, w.attr_id, m.ltbase_row_id, v.image,
  COALESCE(cl.pending, FALSE), COALESCE(cl.last_flushed_at, 0)
FROM %s AS m
JOIN (VALUES %s) AS w(schema_id, attr_id, col)
  ON m.ltbase_schema_id = w.schema_id
CROSS JOIN LATERAL (SELECT CASE w.col %s END AS image) AS v
%s
WHERE v.image IS NOT NULL
  AND (ABS(v.image) > %d::float8 OR v.image <> TRUNC(v.image))
ORDER BY m.ltbase_schema_id, w.attr_id, m.ltbase_row_id`,
		sqlutil.SanitizeIdentifier(tables.EntityMain), strings.Join(values, ", "), strings.Join(arms, " "),
		changeLogJoin(tables, "m.ltbase_schema_id", "m.ltbase_row_id"), int64(maxBigintImage))
	return query, args
}

// censusColumns returns every entity_main row whose double_* image of a
// column-bound bigint target is outside the contract. Targets without a
// table to read are an error: skipping them would report a clean census
// over rows nothing looked at.
func censusColumns(ctx context.Context, q Querier, tables Tables, targets []Target) ([]Finding, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	if tables.EntityMain == "" {
		t := targets[0]
		return nil, fmt.Errorf("no entity main table is configured to scan %d bigint attribute(s) bound to a double column (first: schema=%s attribute=%s column=%s)",
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
		var image float64
		if err := rows.Scan(&schemaID, &attrID, &f.RowID, &image, &f.Pending, &f.LastFlushedAt); err != nil {
			return nil, fmt.Errorf("scan integer-width census row from %s: %w", tables.EntityMain, err)
		}
		target, ok := byKey[[2]int16{schemaID, attrID}]
		if !ok {
			return nil, fmt.Errorf("integer-width census over %s returned schema_id=%d attr_id=%d, which is not a census target", tables.EntityMain, schemaID, attrID)
		}
		f.Target = target
		f.StoredValue = formatDoubleImage(image)
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
