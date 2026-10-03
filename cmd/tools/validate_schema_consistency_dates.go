package main

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/numutil"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"
)

// maxFloat64ImageMillis is the largest magnitude of epoch millis the float64
// image in eav_data.value_numeric keeps exactly (2^53), as the census
// reports it; the predicate itself is transform's.
const maxFloat64ImageMillis = numutil.MaxExactFloat64Integer

// eavDateAttrIndex maps schema_id → attr_id → attribute name for every
// attribute whose value_numeric rows the read path decodes as epoch millis:
// an unbound date/datetime, or an unbound list of them. A bound one lives in
// entity_main and is not this census's concern.
type eavDateAttrIndex map[int16]map[int16]string

func buildEAVDateAttrIndex(cache *schemameta.MetadataCache) (eavDateAttrIndex, schemaNames) {
	dates := make(eavDateAttrIndex)
	names := make(schemaNames)
	for _, schemaName := range cache.ListSchemas() {
		schemaID, ok := cache.GetSchemaID(schemaName)
		if !ok {
			continue
		}
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		names[schemaID] = schemaName
		for _, meta := range schemaCache {
			if meta.ColumnBinding != nil || !isEAVDateAttribute(meta) {
				continue
			}
			if dates[schemaID] == nil {
				dates[schemaID] = make(map[int16]string)
			}
			dates[schemaID][meta.AttributeID] = meta.AttributeName
		}
	}
	return dates, names
}

func isEAVDateAttribute(meta forma.AttributeMetadata) bool {
	vt := meta.ValueType
	if vt == forma.ValueTypeList {
		vt = meta.EffectiveItemsType()
	}
	return vt == forma.ValueTypeDate || vt == forma.ValueTypeDateTime
}

// checkEAVDateImages reports eav_data rows whose value_numeric the read path
// refuses for a date/datetime attribute (#592): an image that is not a
// whole number (a fraction, NaN, ±Infinity), or one past 2^53, the range the
// float64 image keeps exactly. Such rows predate the write-side rule (or
// were edited by hand). After the upgrade a Postgres read of the row, and
// every update that does not name the attribute, is a consistency error
// until the operator rewrites the value; the DuckDB tiers read the image
// narrowed instead (#621), so this census is their only guard
// (docs/schema-consistency-migration.md). The predicate is the read's own
// rule (transform.StoredDateImageRefusedSQL), so it flags exactly the
// images the Postgres read refuses. The query is skipped when no schema has
// an attribute the census could name.
func (v schemaConsistencyValidator) checkEAVDateImages(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	dates, names := buildEAVDateAttrIndex(cache)
	if len(dates) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf(`
SELECT e.schema_id, e.attr_id, COUNT(*) AS record_count
FROM %s AS e
WHERE e.value_numeric IS NOT NULL
  AND %s
GROUP BY e.schema_id, e.attr_id
ORDER BY e.schema_id, e.attr_id`, quoteIdentifier(v.eavTable), transform.StoredDateImageRefusedSQL("e.value_numeric"))

	rows, err := v.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query date images outside the float64-exact range: %w", err)
	}
	defer rows.Close()

	var issues []validationIssue
	for rows.Next() {
		var schemaID, attrID int16
		var count int64
		if err := rows.Scan(&schemaID, &attrID, &count); err != nil {
			return nil, fmt.Errorf("scan date images outside the float64-exact range: %w", err)
		}
		// A numeric attribute may legitimately hold such an image, and a
		// bigint one is the integer-width census's (#590); only a
		// date/datetime hit is this check's.
		attrName, ok := dates[schemaID][attrID]
		if !ok {
			continue
		}
		issues = append(issues, validationIssue{
			category: "date/datetime images the read path refuses in " + v.eavTable,
			details: fmt.Sprintf("schema=%s schema_id=%d attr_id=%d attribute=%s rows=%d (value_numeric must be a whole number with |value| <= %d)",
				names[schemaID], schemaID, attrID, attrName, count, maxFloat64ImageMillis),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate date images outside the float64-exact range: %w", err)
	}
	return issues, nil
}
