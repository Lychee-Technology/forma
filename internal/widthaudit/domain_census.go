package widthaudit

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/iso8601"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/transform"
)

// The width and numeric censuses hold integer and numeric storage to their
// domains. Every other declared type has one too, and the OLTP read refuses
// an image outside it, so baseline-schema refuses one as well (#733). Each
// predicate below is the read's own rule:
//
//   - eav_data keeps text and uuid in value_text and every other type in
//     value_numeric; the read refuses a value in the other column
//     (transform.extractValueFromEAVRecord). That holds for every EAV-only
//     attribute, so each one is a target here.
//   - A uuid in value_text must parse (uuid.Parse, sqlutil.UUIDTextPattern).
//   - A bool in value_numeric must be finite (transform.finiteBoolInput),
//     which is numeric's domain: float64Overflow bounds it the same way.
//   - A date or datetime in value_numeric must be a whole number of epoch
//     millis within ±2^53 (transform.StoredDateImageRefusedSQL).
//   - A bool bound with bool_text holds "1" or "0"
//     (transform.boolFromBoolText).
//   - A date or datetime bound with iso8601 must parse as RFC3339
//     (iso8601.Parse). No SQL predicate states that rule (the day must exist
//     in its month), so every image is read and judged here.
//
// The other bindings hold only what their column type can: a bool_smallint
// reads any smallint as the nearest of 0/1, and a bigint_* date any int64.
// A list's elements are eav_data rows held to the items type.

// DomainTargets lists, in (schema_id, attr_id) order, every EAV-only
// attribute (a list held to its items type) and every bool bound with
// bool_text or date/datetime bound with iso8601.
func DomainTargets(cache *schemameta.MetadataCache) []Target {
	return collectTargets(cache, domainStorage)
}

func domainStorage(meta forma.AttributeMetadata) (declared forma.ValueType, column string, ok bool) {
	if binding := meta.ColumnBinding; binding != nil {
		switch {
		case meta.ValueType == forma.ValueTypeBool && binding.Encoding == forma.MainColumnEncodingBoolText,
			isDate(meta.ValueType) && binding.Encoding == forma.MainColumnEncodingISO8601:
			return meta.ValueType, string(binding.ColumnName), true
		}
		return "", "", false
	}
	declared = meta.ValueType
	if declared == forma.ValueTypeList {
		declared = meta.EffectiveItemsType()
	}
	switch declared {
	case forma.ValueTypeText, forma.ValueTypeUUID, forma.ValueTypeSmallInt, forma.ValueTypeInteger,
		forma.ValueTypeBigInt, forma.ValueTypeNumeric, forma.ValueTypeBool, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return declared, "", true
	}
	return "", "", false
}

func isDate(vt forma.ValueType) bool {
	return vt == forma.ValueTypeDate || vt == forma.ValueTypeDateTime
}

// storedInText reports whether eav_data keeps vt in value_text.
func storedInText(vt forma.ValueType) bool {
	return vt == forma.ValueTypeText || vt == forma.ValueTypeUUID
}

// BuildDomainCensusQuery renders the eav_data census over the given EAV-only
// targets: every row whose value sits in the other value column, or whose
// value is outside its type's domain.
func BuildDomainCensusQuery(tables Tables, targets []Target) (string, []any) {
	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*3+2)
	for _, t := range targets {
		n := len(args)
		values = append(values, fmt.Sprintf("($%d::smallint, $%d::smallint, $%d::text)", n+1, n+2, n+3))
		args = append(args, t.SchemaID, t.AttrID, string(t.Declared))
	}
	args = append(args, sqlutil.UUIDTextPattern, float64Overflow)
	uuidArg, overflowArg := len(args)-1, len(args)

	query := fmt.Sprintf(`SELECT e.schema_id, e.attr_id, e.row_id, e.array_indices, e.value_text, e.value_numeric::text,
  COALESCE(cl.pending, FALSE), COALESCE(cl.last_flushed_at, 0)
FROM %s AS e
JOIN (VALUES %s) AS w(schema_id, attr_id, declared)
  ON e.schema_id = w.schema_id AND e.attr_id = w.attr_id
%s
WHERE (CASE WHEN w.declared IN ('text', 'uuid') THEN e.value_numeric IS NOT NULL ELSE e.value_text IS NOT NULL END)
   OR (w.declared = 'uuid' AND e.value_text !~ $%d)
   OR (w.declared = 'bool' AND abs(e.value_numeric) >= $%d::numeric)
   OR (w.declared IN ('date', 'datetime') AND %s)
ORDER BY e.schema_id, e.attr_id, e.row_id, e.array_indices`,
		sqlutil.SanitizeIdentifier(tables.EAV), strings.Join(values, ", "),
		changeLogJoin(tables, "e.schema_id", "e.row_id"), uuidArg, overflowArg,
		transform.StoredDateImageRefusedSQL("e.value_numeric"))
	return query, args
}

// BuildDomainColumnCensusQuery renders the entity_main census over the
// given column-bound targets: every bool_text image other than "1" and
// "0", and every iso8601 image, which DomainCensus parses.
func BuildDomainColumnCensusQuery(tables Tables, targets []Target) (string, []any) {
	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*4)
	for _, t := range targets {
		n := len(args)
		values = append(values, fmt.Sprintf("($%d::smallint, $%d::smallint, $%d::text, $%d::text)", n+1, n+2, n+3, n+4))
		args = append(args, t.SchemaID, t.AttrID, t.Column, string(t.Declared))
	}

	query := fmt.Sprintf(`SELECT m.ltbase_schema_id, w.attr_id, m.ltbase_row_id, v.image,
  COALESCE(cl.pending, FALSE), COALESCE(cl.last_flushed_at, 0)
FROM %s AS m
JOIN (VALUES %s) AS w(schema_id, attr_id, col, declared)
  ON m.ltbase_schema_id = w.schema_id
CROSS JOIN LATERAL (SELECT %s AS image) AS v
%s
WHERE v.image IS NOT NULL AND (w.declared <> 'bool' OR v.image NOT IN ('1', '0'))
ORDER BY m.ltbase_schema_id, w.attr_id, m.ltbase_row_id`,
		sqlutil.SanitizeIdentifier(tables.EntityMain), strings.Join(values, ", "),
		caseOverColumns(model.TextColumns, ""), changeLogJoin(tables, "m.ltbase_schema_id", "m.ltbase_row_id"))
	return query, args
}

// DomainCensus returns every stored value under the targets that the read
// refuses: the eav_data findings first, then the entity_main ones. A value
// read from value_text, or from a text column, is quoted in StoredValue.
func DomainCensus(ctx context.Context, q Querier, tables Tables, targets []Target) ([]Finding, error) {
	var eavTargets, columnTargets []Target
	for _, t := range targets {
		if t.Column != "" {
			columnTargets = append(columnTargets, t)
			continue
		}
		eavTargets = append(eavTargets, t)
	}
	var findings []Finding
	if len(eavTargets) > 0 {
		query, args := BuildDomainCensusQuery(tables, eavTargets)
		eavFindings, err := queryDomainEAVFindings(ctx, q, tables.EAV, query, args, eavTargets)
		if err != nil {
			return nil, fmt.Errorf("EAV-only attributes: %w", err)
		}
		findings = eavFindings
	}
	if len(columnTargets) == 0 {
		return findings, nil
	}
	if tables.EntityMain == "" {
		t := columnTargets[0]
		return nil, fmt.Errorf("no entity main table is configured to scan %d bool_text or iso8601 attribute(s) (first: schema=%s attribute=%s column=%s)",
			len(columnTargets), t.SchemaName, t.AttrName, t.Column)
	}
	query, args := BuildDomainColumnCensusQuery(tables, columnTargets)
	columnFindings, err := queryDomainColumnFindings(ctx, q, tables.EntityMain, query, args, columnTargets)
	if err != nil {
		return nil, fmt.Errorf("bool_text and iso8601 attributes: %w", err)
	}
	return append(findings, columnFindings...), nil
}

const domainCensus = "storage-domain census"

// queryDomainEAVFindings resolves each eav_data row the query keeps. The
// stored value named is the one in the column the read refuses: the other
// value column when it is set, else the target's own.
func queryDomainEAVFindings(ctx context.Context, q Querier, table, query string, args []any, targets []Target) ([]Finding, error) {
	byKey := targetIndex(targets)
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query %s over %s: %w", domainCensus, table, err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var f Finding
		var schemaID, attrID int16
		var text, numeric *string
		if err := rows.Scan(&schemaID, &attrID, &f.RowID, &f.ArrayIndices, &text, &numeric, &f.Pending, &f.LastFlushedAt); err != nil {
			return nil, fmt.Errorf("scan %s row from %s: %w", domainCensus, table, err)
		}
		target, ok := byKey[[2]int16{schemaID, attrID}]
		if !ok {
			return nil, fmt.Errorf("%s over %s returned schema_id=%d attr_id=%d, which is not a census target", domainCensus, table, schemaID, attrID)
		}
		f.Target = target
		value, err := refusedEAVValue(target.Declared, text, numeric)
		if err != nil {
			return nil, fmt.Errorf("%s over %s returned row %s of schema_id=%d attr_id=%d: %w",
				domainCensus, table, f.RowID, schemaID, attrID, err)
		}
		f.StoredValue = value
		findings = append(findings, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s over %s: %w", domainCensus, table, err)
	}
	return findings, nil
}

// refusedEAVValue names the value the read refuses in a kept eav_data row:
// the one in the other value column when it is set, else the declared
// column's own. A value_text value is quoted.
func refusedEAVValue(declared forma.ValueType, text, numeric *string) (string, error) {
	quote := func(s *string) string { return strconv.Quote(*s) }
	plain := func(s *string) string { return *s }
	other, own := text, numeric
	otherForm, ownForm := quote, plain
	if storedInText(declared) {
		other, own = numeric, text
		otherForm, ownForm = plain, quote
	}
	switch {
	case other != nil:
		return otherForm(other), nil
	case own != nil:
		return ownForm(own), nil
	}
	return "", fmt.Errorf("no value in either value column")
}

// queryDomainColumnFindings resolves each entity_main row the query keeps,
// dropping an iso8601 image the read parses.
func queryDomainColumnFindings(ctx context.Context, q Querier, table, query string, args []any, targets []Target) ([]Finding, error) {
	byKey := targetIndex(targets)
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query %s over %s: %w", domainCensus, table, err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var f Finding
		var schemaID, attrID int16
		var image string
		if err := rows.Scan(&schemaID, &attrID, &f.RowID, &image, &f.Pending, &f.LastFlushedAt); err != nil {
			return nil, fmt.Errorf("scan %s row from %s: %w", domainCensus, table, err)
		}
		target, ok := byKey[[2]int16{schemaID, attrID}]
		if !ok {
			return nil, fmt.Errorf("%s over %s returned schema_id=%d attr_id=%d, which is not a census target", domainCensus, table, schemaID, attrID)
		}
		if isDate(target.Declared) {
			if _, err := iso8601.Parse(image); err == nil {
				continue
			}
		}
		f.Target = target
		f.StoredValue = strconv.Quote(image)
		findings = append(findings, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s over %s: %w", domainCensus, table, err)
	}
	return findings, nil
}
