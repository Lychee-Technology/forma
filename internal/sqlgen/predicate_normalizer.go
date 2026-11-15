package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
)

// normalizeTargets selects which per-target payloads normalizePredicates
// computes, so standalone entrypoints do not pay for backends they never
// emit.
type normalizeTargets uint8

const (
	targetPgEav normalizeTargets = 1 << iota
	targetDuck
	targetHybrid

	targetsNone normalizeTargets = 0
	targetsAll                   = targetPgEav | targetDuck
)

// normalizePredicates converts a forma.Condition into the typed predicate
// tree consumed by walkPredicate. It is the parse-once layer of #143: every
// "op:value" parse, metadata lookup, operator whitelist check, and target
// value conversion happens here exactly once per leaf. Structural oddities
// (nil nodes, unknown condition types) are preserved as dedicated nodes so
// each walk style keeps its lazy, per-target semantics.
func normalizePredicates(cond forma.Condition, cache forma.SchemaAttributeCache, targets normalizeTargets) PredicateNode {
	if cond == nil {
		return predicateNilNode{}
	}
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		group := &PredicateGroup{Logic: c.Logic}
		for _, child := range c.Conditions {
			group.Children = append(group.Children, normalizePredicates(child, cache, targets))
		}
		return group
	case *forma.KvCondition:
		return normalizeLeaf(c, cache, targets)
	default:
		return predicateInvalidNode{err: fmt.Errorf("unsupported condition type %T", cond)}
	}
}

func normalizeLeaf(kv *forma.KvCondition, cache forma.SchemaAttributeCache, targets normalizeTargets) *PredicateLeaf {
	leaf := &PredicateLeaf{Attr: kv.Attr, Source: kv}

	meta, hasMeta := cache[kv.Attr]
	leaf.HasMeta = hasMeta
	leaf.Meta = meta

	lenient := conditionexpr.ParseOperatorValueLenient(kv.Value)
	leaf.Operator = PredicateOperator(conditionexpr.CanonicalOperator(lenient.Operator))
	if ext := classifyExtendedLeaf(kv.Attr, meta, hasMeta, lenient); ext.kind != extNone {
		applyExtendedLeaf(leaf, meta, hasMeta, ext, targets)
		return leaf
	}
	lenientSQL, lenientSQLErr := conditionexpr.ToSQLOperator(lenient.Operator, lenient.Value)
	if lenientSQLErr == nil {
		leaf.SQLOperator = lenientSQL.SQLOperator
		switch leaf.Operator {
		case PredicateOpStartsWith:
			leaf.Pattern = PatternKindPrefix
		case PredicateOpContains:
			leaf.Pattern = PatternKindContains
		}
	}

	if targets&targetPgEav != 0 {
		leaf.PgEav = normalizePgEavPayload(kv, meta, hasMeta)
	}
	if targets&targetDuck != 0 {
		leaf.Duck = normalizeDuckPayload(kv, meta, hasMeta, lenientSQL, lenientSQLErr)
	}
	if targets&targetHybrid != 0 {
		leaf.Hybrid = normalizeHybridPayload(kv, meta, hasMeta, lenient, lenientSQL, lenientSQLErr)
	}
	return leaf
}

// normalizePgEavPayload converts a leaf for the EAV EXISTS emitter: strict
// parse, EAV value-column selection, type conversion, and the text/bool
// operator whitelist.
func normalizePgEavPayload(kv *forma.KvCondition, meta forma.AttributeMetadata, hasMeta bool) PgEavLeafPayload {
	if !hasMeta {
		return PgEavLeafPayload{Err: forma.InvalidInputf("attribute not found in cache: %s", kv.Attr)}
	}

	operator, err := conditionexpr.ParseOperatorValueStrict(kv.Value)
	if err != nil {
		return PgEavLeafPayload{Err: err}
	}
	opStr := operator.Operator
	valStr := operator.Value

	// Operator, then operator against type, then operand: the order of the
	// entity_main forms, so a uuid LIKE pattern is refused for its operator
	// rather than as a malformed UUID.
	sqlOperator, err := conditionexpr.ToSQLOperator(opStr, valStr)
	if err != nil {
		return PgEavLeafPayload{Err: err}
	}
	sqlOp := sqlOperator.SQLOperator
	if err := checkOperatorForType(kv.Attr, opStr, sqlOp, meta.ValueType); err != nil {
		return PgEavLeafPayload{Err: err}
	}

	// The `unsupported value_type` default in parsePgEavValue deliberately stays
	// plain: that names the schema's declared type, not anything the caller sent.
	valueColumn, parsedValue, err := parsePgEavValue(kv.Attr, meta, valStr)
	if err != nil {
		return PgEavLeafPayload{Err: err}
	}
	if sqlOp == "LIKE" {
		parsedValue = sqlOperator.Value
	}

	return PgEavLeafPayload{
		AttrID:      meta.AttributeID,
		ValueColumn: valueColumn,
		SQLOp:       sqlOp,
		Value:       parsedValue,
		Truthy:      meta.ValueType == forma.ValueTypeBool,
		ByteOrder:   valueColumn == "value_text" && isRangeComparison(sqlOp),
	}
}

// parsePgEavValue picks the eav_data value column for the attribute's declared
// type and converts the literal into the Go value bound against it. Extracted
// from normalizePgEavPayload (#357) to keep that function under the size limit;
// behavior is unchanged, and the characterization matrix guards it.
func parsePgEavValue(attr string, meta forma.AttributeMetadata, valStr string) (valueColumn string, parsedValue any, err error) {
	switch meta.ValueType {
	case forma.ValueTypeText:
		v, err := textOperand(attr, valStr)
		if err != nil {
			return "", nil, err
		}
		return "value_text", v, nil

	case forma.ValueTypeUUID:
		v, err := uuidOperand(attr, valStr)
		if err != nil {
			return "", nil, err
		}
		return "value_text", v, nil

	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		v, ok, err := numericOperand(attr, valStr, meta.ValueType)
		if err != nil {
			return "", nil, err
		}
		if !ok {
			return "", nil, forma.InvalidInputf("invalid numeric value for '%s': %s", attr, valStr)
		}
		if exact, isInt := v.(int64); isInt {
			// bigint binds the exact int64 (#281, #357); the DOUBLE-width
			// classes narrow it through the same float64 funnel the stored
			// data took, so both engines compare the float64 image above
			// 2^53 (#384) — see NarrowEAVNumericOperand.
			return "value_numeric", NarrowEAVNumericOperand(meta.ValueType, exact), nil
		}
		return "value_numeric", v, nil

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		parsed, dateErr := parseDateValue(attr, valStr, meta)
		if dateErr != nil {
			return "", nil, fmt.Errorf("invalid date value for '%s': %w", attr, dateErr)
		}
		return "value_numeric", parsed, nil

	case forma.ValueTypeBool:
		// The bind is a Go bool compared against the BoolTruthiness (#404)
		// truthiness expression, not the raw column — the payload's Truthy
		// flag drives the emitters. The operand parse is the engine-shared
		// parseBoolOperand rule (#384 P2b).
		parsed, ok := parseBoolOperand(valStr)
		if !ok {
			return "", nil, forma.InvalidInputf("invalid boolean value for '%s': %s", attr, valStr)
		}
		return "value_numeric", parsed, nil

	default:
		return "", nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, attr)
	}
}

// NarrowEAVNumericOperand converts a parsed integral operand into the Go
// value the Postgres binders compare against value_numeric for the given
// DECLARED type (#384). integer/smallint/numeric EAV storage holds float64
// images (the write funnel narrows through numutil.Float64), so the operand
// takes the same narrowing: above 2^53 both engines then compare the float64
// image — the DuckDB leg's CAST(? AS DOUBLE) rounds identically — instead of
// Postgres comparing the exact integer while DuckDB matches its rounded
// neighbor. bigint keeps the exact int64 (#281/#357): its storage is
// BIGINT-backed on every tier. Shared by the EAV predicate normalizer and the
// batch attr-value anchor, which must reach the same verdict on the same
// literal (#355).
func NarrowEAVNumericOperand(vt forma.ValueType, v int64) any {
	if vt == forma.ValueTypeBigInt {
		return v
	}
	return float64(v)
}

// normalizeDuckPayload converts a leaf for the DuckDB emitter, keeping the
// no-metadata fallback inference (detectValueType) and the text/LIKE
// no-cast emission. An unregistered attribute keeps that lenient path only
// if its folded column is not a visible-CTE system or dedup column (#512):
// the fold is lossy, and an unchecked "created.at" would silently filter
// on created_at. Registered names were already checked at registration by
// ValidateParquetAttrColumns.
func normalizeDuckPayload(
	kv *forma.KvCondition,
	meta forma.AttributeMetadata,
	hasMeta bool,
	lenientSQL conditionexpr.SQLOperatorResult,
	lenientSQLErr error,
) DuckLeafPayload {
	if lenientSQLErr != nil {
		return DuckLeafPayload{Err: lenientSQLErr}
	}

	column := resolveDuckDBColumn(kv.Attr)
	if !hasMeta {
		if err := ValidateUnregisteredParquetAttrColumn(kv.Attr, column); err != nil {
			return DuckLeafPayload{Err: err}
		}
	}

	if lenientSQL.SQLOperator == "LIKE" {
		pattern, err := textOperand(kv.Attr, lenientSQL.Value)
		if err != nil {
			return DuckLeafPayload{Err: err}
		}
		return DuckLeafPayload{Column: column, SQLOp: lenientSQL.SQLOperator, TextLike: true, Param: pattern}
	}

	var valueType forma.ValueType
	if hasMeta {
		valueType = meta.ValueType
	} else {
		valueType = detectValueType(lenientSQL.Value)
	}

	if valueType == forma.ValueTypeText {
		text, err := textOperand(kv.Attr, lenientSQL.Value)
		if err != nil {
			return DuckLeafPayload{Err: err}
		}
		return DuckLeafPayload{Column: column, SQLOp: lenientSQL.SQLOperator, ValueType: valueType, TextLike: true, Param: text}
	}

	rawParam, err := parseDuckDBRawParam(lenientSQL.Value, kv.Attr, valueType)
	if err != nil {
		return DuckLeafPayload{Err: err}
	}
	if err := checkDuckRFC3339Operand(kv.Attr, lenientSQL.Value, rawParam, valueType, meta, hasMeta); err != nil {
		return DuckLeafPayload{Err: err}
	}
	sqlOp, rawParam, err := duckWholeNumberComparison(kv.Attr, valueType, lenientSQL.SQLOperator, rawParam)
	if err != nil {
		return DuckLeafPayload{Err: err}
	}
	param, err := ToDuckDBParam(rawParam, valueType)
	if err != nil {
		return DuckLeafPayload{Err: fmt.Errorf("to duckdb param: %w", err)}
	}
	return DuckLeafPayload{Column: column, SQLOp: sqlOp, ValueType: valueType, Param: param}
}

// checkDuckRFC3339Operand applies the rfc3339 four-digit-year rule to the
// DuckDB leaf's epoch-ms operand when the attribute is bound with that
// encoding. The DuckDB compare is exact, so without this the refusal the
// Postgres binders raise (parseDateValue → rfc3339FilterImage) would be "400
// on Postgres, exact compare on DuckDB" for the same literal (#588). Only a
// date/datetime operand is judged: parseDuckDBRawParam binds it as int64
// epoch millis, the shape rfc3339.Image takes.
func checkDuckRFC3339Operand(attr, literal string, rawParam any, valueType forma.ValueType, meta forma.AttributeMetadata, hasMeta bool) error {
	if valueType != forma.ValueTypeDate && valueType != forma.ValueTypeDateTime {
		return nil
	}
	if !hasMeta || meta.ColumnBinding == nil || meta.ColumnBinding.Encoding != forma.MainColumnEncodingRFC3339 {
		return nil
	}
	ms, ok := rawParam.(int64)
	if !ok {
		return fmt.Errorf("date operand for '%s' bound as %T, expected int64 epoch millis", attr, rawParam)
	}
	_, err := rfc3339FilterImage(attr, literal, ms, meta)
	return err
}
