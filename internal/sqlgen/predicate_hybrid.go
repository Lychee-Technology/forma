package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
	"github.com/lychee-technology/forma/internal/model"
)

// normalizeHybridPayload converts a leaf for the hybrid condition builder.
// Routing mirrors the retired hybridConditionBuilder.resolveColumnName: a raw
// entity_main column (IsMainTableColumn) or a cache-bound attribute emits
// against the main table; everything else falls to the EAV EXISTS form. For
// the main branch the operator parse error is surfaced before the
// descriptor-validation error, matching the retired buildMainTableCondition
// ordering. The EAV branch reuses the strict-parse PgEav payload verbatim.
func normalizeHybridPayload(
	kv *forma.KvCondition,
	meta forma.AttributeMetadata,
	hasMeta bool,
	lenient conditionexpr.OperatorValue,
	lenientSQL conditionexpr.SQLOperatorResult,
	lenientSQLErr error,
) HybridLeafPayload {
	colName := hybridMainColumn(kv.Attr, meta, hasMeta)
	if colName == "" {
		return HybridLeafPayload{IsMain: false, Eav: normalizePgEavPayload(kv, meta, hasMeta)}
	}

	// Main-table branch. Operator parse error first (pre-#154 ordering).
	if lenientSQLErr != nil {
		return HybridLeafPayload{IsMain: true, Err: lenientSQLErr}
	}
	leafMeta, err := resolveHybridLeafMeta(colName, meta, hasMeta)
	if err != nil {
		return HybridLeafPayload{IsMain: true, Err: err}
	}
	if err := checkOperatorForType(kv.Attr, lenient.Operator, lenientSQL.SQLOperator, leafMeta.ValueType); err != nil {
		return HybridLeafPayload{IsMain: true, Err: err}
	}
	if p, ok, err := pgMainBoolPredicate(leafMeta, lenientSQL.SQLOperator, lenientSQL.Value, kv.Attr); err != nil {
		return HybridLeafPayload{IsMain: true, Err: err}
	} else if ok {
		return HybridLeafPayload{IsMain: true, MainColumn: colName, MainBool: &p}
	}
	value, err := ConvertPgMainValue(lenientSQL.Value, kv.Attr, leafMeta)
	if err != nil {
		return HybridLeafPayload{IsMain: true, Err: err}
	}
	return hybridMainComparison(colName, leafMeta.ValueType, lenientSQL.SQLOperator, value)
}

// hybridMainColumn is the entity_main column a hybrid leaf emits against:
// the raw column itself, the cache binding's column, or "" for the EAV form.
func hybridMainColumn(attr string, meta forma.AttributeMetadata, hasMeta bool) string {
	if model.IsMainTableColumn(attr) {
		return attr
	}
	if hasMeta && meta.ColumnBinding != nil {
		return string(meta.ColumnBinding.ColumnName)
	}
	return ""
}

// resolveHybridLeafMeta returns the attribute metadata driving value
// conversion for a main-table hybrid leaf. The physical column is always
// validated against the entity_main descriptors (preserving the pre-#140
// "unknown main table column" contract even for cache-bound attributes);
// cache-bound attributes that omit value_type derive it from the descriptor
// kind, and raw column references without metadata derive a minimal metadata.
func resolveHybridLeafMeta(colName string, meta forma.AttributeMetadata, hasMeta bool) (forma.AttributeMetadata, error) {
	desc := model.GetMainColumnDescriptor(colName)
	if desc == nil {
		return forma.AttributeMetadata{}, fmt.Errorf("unknown main table column: %s", colName)
	}
	if hasMeta {
		if meta.ValueType == "" {
			meta.ValueType = model.ColumnKindToValueType(desc.Kind)
		}
		return meta, nil
	}
	return forma.AttributeMetadata{ValueType: model.ColumnKindToValueType(desc.Kind)}, nil
}
