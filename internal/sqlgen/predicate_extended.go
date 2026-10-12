package sqlgen

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
)

// The #646 leaf kinds: set membership (in / not_in), null tests (is_null /
// is_not_null) and the $now operand. Their operands are not scalar
// literals, so normalizeLeaf hands them to this file instead of the
// per-target scalar converters, which therefore never see them.
type extendedKind int

const (
	extNone extendedKind = iota
	extSet
	extNullTest
	extServerTime
)

// extendedLeaf is the target-independent parse of an extended leaf, done
// once. err is the caller-facing refusal every target reports, except the
// pg-main pushdown, which skips the leaves it cannot place.
type extendedLeaf struct {
	kind  extendedKind
	op    string // canonical logical operator
	sqlOp string
	raw   []string // list literals (extSet)
	elems []any    // raw coerced under the cache metadata (extSet, when known)
	now   conditionexpr.ServerTime
	err   error
}

func classifyExtendedLeaf(attr string, meta forma.AttributeMetadata, hasMeta bool, lenient conditionexpr.OperatorValue) extendedLeaf {
	op := conditionexpr.CanonicalOperator(lenient.Operator)
	switch op {
	case conditionexpr.OpIn, conditionexpr.OpNotIn:
		ext := extendedLeaf{kind: extSet, op: op, sqlOp: conditionexpr.SQLOpAny}
		if op == conditionexpr.OpNotIn {
			ext.sqlOp = conditionexpr.SQLOpAll
		}
		ext.raw, ext.err = conditionexpr.ParseListOperand(attr, op, lenient.Value)
		if ext.err == nil && hasMeta {
			ext.elems, ext.err = coerceListElements(attr, op, meta, ext.raw)
		}
		return ext
	case conditionexpr.OpIsNull, conditionexpr.OpIsNotNull:
		ext := extendedLeaf{kind: extNullTest, op: op, sqlOp: conditionexpr.SQLOpIsNull}
		if op == conditionexpr.OpIsNotNull {
			ext.sqlOp = conditionexpr.SQLOpIsNotNull
		}
		if lenient.Value != "true" {
			ext.err = forma.InvalidInputf("operator '%s' on attribute '%s' takes only the value true, got %q", op, attr, lenient.Value)
		}
		return ext
	}
	if isServerTimeLeaf(meta, hasMeta, lenient) {
		return classifyServerTimeLeaf(attr, op, meta, lenient.Value)
	}
	return extendedLeaf{}
}

func classifyServerTimeLeaf(attr, op string, meta forma.AttributeMetadata, value string) extendedLeaf {
	ext := extendedLeaf{kind: extServerTime, op: op}
	sql, _ := conditionexpr.ToSQLOperator(op, value)
	switch sql.SQLOperator {
	case "=", "!=", ">", ">=", "<", "<=":
		ext.sqlOp = sql.SQLOperator
		ext.now, ext.err = conditionexpr.ParseServerTime(attr, meta.ValueType, value)
	default:
		ext.err = forma.InvalidInputf(
			"operator '%s' does not accept the $now operand on %s attribute '%s' (expected equals, not_equals, gt, gte, lt or lte)",
			op, meta.ValueType, attr)
	}
	return ext
}

// applyExtendedLeaf fills the summary fields and the requested target
// payloads of an extended leaf. A null test on a bound column is the only
// extended leaf pg-main can push down: a set's array bind has no form in
// the pg_source pushdown (#646 D3), and $now is refused on that route.
func applyExtendedLeaf(leaf *PredicateLeaf, meta forma.AttributeMetadata, hasMeta bool, ext extendedLeaf, targets normalizeTargets) {
	leaf.SQLOperator = ext.sqlOp
	leaf.Pattern = PatternKindNone
	leaf.Pushable = ext.kind == extNullTest && hasMeta && meta.ColumnBinding != nil
	if targets&targetPgEav != 0 {
		leaf.PgEav = extendedPgEavPayload(leaf.Attr, meta, hasMeta, ext)
	}
	if targets&targetPgMain != 0 {
		leaf.PgMain = extendedPgMainPayload(leaf.Attr, meta, hasMeta, ext)
	}
	if targets&targetDuck != 0 {
		leaf.Duck = extendedDuckPayload(leaf.Attr, meta, hasMeta, ext)
	}
	if targets&targetHybrid != 0 {
		leaf.Hybrid = extendedHybridPayload(leaf.Attr, meta, hasMeta, ext)
	}
}

func extendedPgEavPayload(attr string, meta forma.AttributeMetadata, hasMeta bool, ext extendedLeaf) PgEavLeafPayload {
	if !hasMeta {
		return PgEavLeafPayload{Err: forma.InvalidInputf("attribute not found in cache: %s", attr)}
	}
	if ext.err != nil {
		return PgEavLeafPayload{Err: ext.err}
	}
	p := PgEavLeafPayload{AttrID: meta.AttributeID, ValueColumn: eavValueColumn(meta.ValueType)}
	switch ext.kind {
	case extNullTest:
		p.NullTest = ext.sqlOp
		p.List = meta.ValueType == forma.ValueTypeList
	case extSet:
		values, err := pgEavSetValues(attr, meta.ValueType, ext.elems)
		if err != nil {
			return PgEavLeafPayload{Err: err}
		}
		p.Set = &SetOperand{SQLOp: ext.sqlOp, Values: values}
		p.Truthy = meta.ValueType == forma.ValueTypeBool
	case extServerTime:
		p.SQLOp = ext.sqlOp
		p.Now = &ServerTimeOperand{OffsetMs: ext.now.OffsetMs, Date: meta.ValueType == forma.ValueTypeDate}
	}
	return p
}

func extendedPgMainPayload(attr string, meta forma.AttributeMetadata, hasMeta bool, ext extendedLeaf) PgMainLeafPayload {
	if !hasMeta || meta.ColumnBinding == nil {
		return PgMainLeafPayload{Skip: true}
	}
	if ext.kind == extServerTime {
		return PgMainLeafPayload{Err: serverTimeFederatedError(attr, meta.ValueType)}
	}
	if ext.err != nil {
		return PgMainLeafPayload{Err: ext.err}
	}
	if ext.kind == extSet {
		return PgMainLeafPayload{Skip: true}
	}
	return PgMainLeafPayload{Column: resolveMainTableColumn(attr, meta), NullTest: ext.sqlOp}
}

func extendedDuckPayload(attr string, meta forma.AttributeMetadata, hasMeta bool, ext extendedLeaf) DuckLeafPayload {
	if ext.kind == extServerTime {
		return DuckLeafPayload{Err: serverTimeFederatedError(attr, meta.ValueType)}
	}
	if ext.err != nil {
		return DuckLeafPayload{Err: ext.err}
	}
	column := resolveDuckDBColumn(attr)
	if !hasMeta {
		if err := ValidateUnregisteredParquetAttrColumn(attr, column); err != nil {
			return DuckLeafPayload{Err: err}
		}
	}
	if ext.kind == extNullTest {
		return DuckLeafPayload{Column: column, NullTest: ext.sqlOp}
	}
	if !hasMeta {
		// The list's element type comes from the declared value type.
		return DuckLeafPayload{Err: forma.InvalidInputf("attribute not found in cache: %s", attr)}
	}
	set, values, err := duckSetValues(ext.sqlOp, column, meta.ValueType, ext.elems)
	if err != nil {
		return DuckLeafPayload{Err: err}
	}
	return DuckLeafPayload{Column: column, SQLOp: ext.sqlOp, ValueType: meta.ValueType, Param: values, Set: &set}
}

// extendedHybridPayload routes like normalizeHybridPayload. A raw
// entity_main column carries no cache metadata, so its list is coerced
// under the descriptor-derived metadata instead.
func extendedHybridPayload(attr string, meta forma.AttributeMetadata, hasMeta bool, ext extendedLeaf) HybridLeafPayload {
	colName := hybridMainColumn(attr, meta, hasMeta)
	if colName == "" {
		return HybridLeafPayload{Eav: extendedPgEavPayload(attr, meta, hasMeta, ext)}
	}
	if ext.err != nil {
		return HybridLeafPayload{IsMain: true, Err: ext.err}
	}
	leafMeta, err := resolveHybridLeafMeta(colName, meta, hasMeta)
	if err != nil {
		return HybridLeafPayload{IsMain: true, Err: err}
	}
	p := HybridLeafPayload{IsMain: true, MainColumn: colName}
	switch ext.kind {
	case extNullTest:
		p.MainNullTest = ext.sqlOp
	case extServerTime:
		p.MainSQLOp = ext.sqlOp
		p.MainNow = &ServerTimeOperand{
			OffsetMs: ext.now.OffsetMs,
			Date:     leafMeta.ValueType == forma.ValueTypeDate,
			ISO8601:  leafMeta.ColumnBinding != nil && leafMeta.ColumnBinding.Encoding == forma.MainColumnEncodingISO8601,
		}
	case extSet:
		elems := ext.elems
		if !hasMeta || leafMeta.ValueType != meta.ValueType {
			if elems, err = coerceListElements(attr, ext.op, leafMeta, ext.raw); err != nil {
				return HybridLeafPayload{IsMain: true, Err: err}
			}
		}
		set, err := pgMainSetOperand(ext.sqlOp, colName, leafMeta, elems)
		if err != nil {
			return HybridLeafPayload{IsMain: true, Err: err}
		}
		p.MainSet = &set
	}
	return p
}
