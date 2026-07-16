package internal

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// QueryPersistentRecordsByAttrValues fetches full records whose attribute
// equals any of the given values through a single set-based anchor scan
// (#268). Relation enrichment previously expanded one equality condition per
// value into OR-of-N correlated EXISTS subqueries, which degenerated into a
// full EAV scan with N subplans per row.
func (r *DBPersistentRecordRepository) QueryPersistentRecordsByAttrValues(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	attr string,
	values []string,
	limit int,
) (*model.PersistentRecordPage, error) {
	if schemaID <= 0 {
		return nil, fmt.Errorf("failed to query records by attr values: schema id %d must be positive", schemaID)
	}
	if len(values) == 0 {
		return &model.PersistentRecordPage{CurrentPage: 1}, nil
	}
	if r.metadataCache == nil {
		return nil, fmt.Errorf("failed to query records by attr %q for schema %d: metadata cache is nil (expected a loaded schema metadata cache)", attr, schemaID)
	}
	cache, ok := r.metadataCache.GetSchemaCacheByID(schemaID)
	if !ok {
		return nil, fmt.Errorf("failed to query records by attr %q: schema %d not in metadata cache (expected a registered schema)", attr, schemaID)
	}
	meta, ok := cache[attr]
	if !ok {
		return nil, fmt.Errorf("failed to query records by attr %q: attribute not in metadata cache for schema %d (expected a registered attribute with a value type)", attr, schemaID)
	}

	clause, args, useMainTableAsAnchor, err := buildAttrValuesAnchor(attr, meta, values)
	if err != nil {
		return nil, fmt.Errorf("failed to query records by attr %q for schema %d: %w", attr, schemaID, err)
	}

	if limit <= 0 {
		limit = len(values)
	}

	records, totalRecords, err := r.runOptimizedQuery(
		ctx, tables, schemaID, clause, args, limit, 0, nil, useMainTableAsAnchor)
	if err != nil {
		return nil, fmt.Errorf("failed to query records by attr %q for schema_id %d: %w", attr, schemaID, err)
	}

	return &model.PersistentRecordPage{
		Records:      records,
		TotalRecords: totalRecords,
		TotalPages:   model.ComputeTotalPages(totalRecords, limit),
		CurrentPage:  1,
	}, nil
}

// buildAttrValuesAnchor renders the set-based anchor clause for one attribute
// against a value list. Placeholders start at $2: StreamOptimizedQuery binds
// $1 to the schema id.
func buildAttrValuesAnchor(attr string, meta forma.AttributeMetadata, values []string) (string, []any, bool, error) {
	if meta.ColumnBinding != nil {
		// Encoded bindings (unix-ms, bool-int, ...) need per-value transforms
		// this batch path does not perform; plain text columns bind directly.
		if meta.ColumnBinding.Encoding != "" || !isTextLikeValueType(meta.ValueType) {
			return "", nil, false, fmt.Errorf(
				"unsupported main-column binding for %s attribute %q: column %s with encoding %q (expected an unencoded text/uuid column for batch value lookup)",
				meta.ValueType, attr, meta.ColumnBinding.ColumnName, meta.ColumnBinding.Encoding)
		}
		clause := fmt.Sprintf("m.%s = ANY($2)", sanitizeIdentifier(string(meta.ColumnBinding.ColumnName)))
		return clause, []any{values}, true, nil
	}

	if isTextLikeValueType(meta.ValueType) {
		return "t.attr_id = $2 AND t.value_text = ANY($3)", []any{meta.AttributeID, values}, false, nil
	}

	switch meta.ValueType {
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		numeric := make([]float64, 0, len(values))
		for _, v := range values {
			parsed, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return "", nil, false, fmt.Errorf(
					"invalid value %q for %s attribute %q: not parseable as float64 (expected a decimal value_numeric operand): %w",
					v, meta.ValueType, attr, err)
			}
			numeric = append(numeric, parsed)
		}
		return "t.attr_id = $2 AND t.value_numeric = ANY($3)", []any{meta.AttributeID, numeric}, false, nil
	default:
		return "", nil, false, fmt.Errorf(
			"unsupported value_type %s for attribute %q: no batch value column (expected text, uuid, or a numeric family type)",
			meta.ValueType, attr)
	}
}

func isTextLikeValueType(vt forma.ValueType) bool {
	return vt == forma.ValueTypeText || vt == forma.ValueTypeUUID
}
