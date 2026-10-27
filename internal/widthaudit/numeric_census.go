package widthaudit

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// numeric has no integer width, but its storage still has a domain: the
// finite float64 values. The write funnel refuses NaN and the infinities
// (transform.finiteForEAV, #322), and the OLTP read refuses a stored one
// (transform.numericFromEAVRecord, #592). eav_data keeps a numeric as
// NUMERIC, which can also hold NaN, ±Infinity and a magnitude past float64;
// the read parses that last one to an infinity (model.parseValueNumeric) and
// refuses it as well. A double_* column holds a float8, which can be NaN or
// infinite. The smallint_*, integer_* and bigint_* columns a numeric can also
// bind to hold only finite values. A row written before the funnel refused
// non-finite values, or around it, is the only way such a value gets there.
// baseline-schema refuses one (#733), as its parquet audit refuses a
// non-finite DOUBLE under numeric.

// float64Overflow is the smallest magnitude a float64 parse rounds to an
// infinity: MaxFloat64 plus half its last ulp, 2^1024 - 2^970. The tie
// itself rounds to even, which is 2^1024, so it overflows too.
var float64Overflow = new(big.Int).Sub(
	new(big.Int).Lsh(big.NewInt(1), 1024), new(big.Int).Lsh(big.NewInt(1), 970)).String()

// NumericTargets lists every numeric attribute whose storage can hold a value
// outside numeric's domain, in (schema_id, attr_id) order: an EAV-only one, a
// list with numeric items, one bound to a double_* column, and the eav_data
// rows any bound one kept from before its promotion (collectTargets).
func NumericTargets(cache *schemameta.MetadataCache) []Target {
	return collectTargets(cache, numericStorage, true)
}

func numericStorage(meta forma.AttributeMetadata) (declared forma.ValueType, column string, ok bool) {
	if binding := meta.ColumnBinding; binding != nil {
		column = string(binding.ColumnName)
		return meta.ValueType, column, meta.ValueType == forma.ValueTypeNumeric && slices.Contains(model.DoubleColumns, column)
	}
	declared = meta.ValueType
	if declared == forma.ValueTypeList {
		declared = meta.EffectiveItemsType()
	}
	return declared, "", declared == forma.ValueTypeNumeric
}

// BuildNumericCensusQuery renders the eav_data census over the given
// EAV-only numeric targets. Postgres orders NUMERIC NaN above every number,
// and abs maps -Infinity to Infinity, so one comparison against
// float64Overflow keeps NaN, both infinities and every finite magnitude a
// float64 parse overflows on.
func BuildNumericCensusQuery(tables Tables, targets []Target) (string, []any) {
	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*2+1)
	for _, t := range targets {
		n := len(args)
		values = append(values, fmt.Sprintf("($%d::smallint, $%d::smallint)", n+1, n+2))
		args = append(args, t.SchemaID, t.AttrID)
	}
	args = append(args, float64Overflow)

	query := fmt.Sprintf(`SELECT e.schema_id, e.attr_id, e.row_id, e.array_indices, e.value_numeric::text,
  COALESCE(cl.pending, FALSE), COALESCE(cl.last_flushed_at, 0)
FROM %s AS e
JOIN (VALUES %s) AS w(schema_id, attr_id)
  ON e.schema_id = w.schema_id AND e.attr_id = w.attr_id
%s
WHERE e.value_numeric IS NOT NULL
  AND abs(e.value_numeric) >= $%d::numeric
ORDER BY e.schema_id, e.attr_id, e.row_id, e.array_indices`,
		sqlutil.SanitizeIdentifier(tables.EAV), strings.Join(values, ", "),
		changeLogJoin(tables, "e.schema_id", "e.row_id"), len(args))
	return query, args
}

// BuildNumericColumnCensusQuery renders the entity_main census over the given
// numeric targets bound to a double_* column: every NaN or infinite image.
// It selects the shape BuildColumnCensusQuery does, with no exact value.
func BuildNumericColumnCensusQuery(tables Tables, targets []Target) (string, []any) {
	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*3)
	for _, t := range targets {
		n := len(args)
		values = append(values, fmt.Sprintf("($%d::smallint, $%d::smallint, $%d::text)", n+1, n+2, n+3))
		args = append(args, t.SchemaID, t.AttrID, t.Column)
	}

	query := fmt.Sprintf(`SELECT m.ltbase_schema_id, w.attr_id, m.ltbase_row_id, v.image, NULL::bigint,
  COALESCE(cl.pending, FALSE), COALESCE(cl.last_flushed_at, 0)
FROM %s AS m
JOIN (VALUES %s) AS w(schema_id, attr_id, col)
  ON m.ltbase_schema_id = w.schema_id
CROSS JOIN LATERAL (SELECT %s AS image) AS v
%s
WHERE v.image IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)
ORDER BY m.ltbase_schema_id, w.attr_id, m.ltbase_row_id`,
		sqlutil.SanitizeIdentifier(tables.EntityMain), strings.Join(values, ", "),
		caseOverColumns(model.DoubleColumns, ""), changeLogJoin(tables, "m.ltbase_schema_id", "m.ltbase_row_id"))
	return query, args
}

// NumericCensus returns every stored value under the numeric targets that is
// outside numeric's domain: the eav_data findings first, then the
// entity_main ones. Each table is queried only when it has targets.
func NumericCensus(ctx context.Context, q Querier, tables Tables, targets []Target) ([]Finding, error) {
	const census = "numeric-domain census"
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
		query, args := BuildNumericCensusQuery(tables, eavTargets)
		eavFindings, err := queryEAVFindings(ctx, q, tables.EAV, census, query, args, eavTargets)
		if err != nil {
			return nil, fmt.Errorf("EAV-only numeric attributes: %w", err)
		}
		findings = eavFindings
	}
	if len(columnTargets) == 0 {
		return findings, nil
	}
	if tables.EntityMain == "" {
		t := columnTargets[0]
		return nil, fmt.Errorf("no entity main table is configured to scan %d numeric attribute(s) bound to a double column (first: schema=%s attribute=%s column=%s)",
			len(columnTargets), t.SchemaName, t.AttrName, t.Column)
	}
	query, args := BuildNumericColumnCensusQuery(tables, columnTargets)
	columnFindings, err := queryColumnFindings(ctx, q, tables.EntityMain, census, query, args, columnTargets)
	if err != nil {
		return nil, fmt.Errorf("numeric attributes bound to a double column: %w", err)
	}
	return append(findings, columnFindings...), nil
}
