package sqlgen

import (
	"github.com/lychee-technology/forma"
)

// PredicateNode is a node in the typed predicate tree produced by
// normalizePredicates (#143). The tree mirrors the forma.Condition AST
// one-to-one — composites become PredicateGroup, KvCondition leaves become
// PredicateLeaf, and nil / unknown-type nodes keep dedicated markers — so
// each walk style retains its distinct handling (error, skip, identity).
type PredicateNode interface{ isPredicateNode() }

// PredicateOperator is the canonical logical operator of a leaf, derived
// from the lenient "op:value" parse. Unknown tokens are kept verbatim; the
// per-target payloads carry any resulting error.
type PredicateOperator string

const (
	PredicateOpEquals     PredicateOperator = "equals"
	PredicateOpNotEquals  PredicateOperator = "not_equals"
	PredicateOpGt         PredicateOperator = "gt"
	PredicateOpGte        PredicateOperator = "gte"
	PredicateOpLt         PredicateOperator = "lt"
	PredicateOpLte        PredicateOperator = "lte"
	PredicateOpStartsWith PredicateOperator = "starts_with"
	PredicateOpContains   PredicateOperator = "contains"
	PredicateOpIn         PredicateOperator = "in"
	PredicateOpNotIn      PredicateOperator = "not_in"
	PredicateOpIsNull     PredicateOperator = "is_null"
	PredicateOpIsNotNull  PredicateOperator = "is_not_null"
)

// PatternKind classifies the LIKE wildcard normalization applied to a value.
type PatternKind int

const (
	PatternKindNone     PatternKind = iota
	PatternKindPrefix               // starts_with -> "value%"
	PatternKindContains             // contains -> "%value%"
)

// PredicateStorage says which physical storage a leaf resolves to.
type PredicateStorage int

const (
	PredicateStorageEAV PredicateStorage = iota
	PredicateStorageMain
)

// PredicateGroup mirrors a CompositeCondition.
type PredicateGroup struct {
	Logic    forma.Logic
	Children []PredicateNode
	// FullyPushable reports whether every leaf under this group can be
	// pushed to entity_main. The pg-main walker reads it to veto partial
	// OR branches without re-walking the subtree.
	FullyPushable bool
	// Source is the composite this group was normalized from, kept for the
	// legacy CompositeGuard contract.
	Source *forma.CompositeCondition
}

func (*PredicateGroup) isPredicateNode() {}

// PredicateLeaf is a KvCondition parsed exactly once: attribute metadata,
// operator normalization, and the converted values for all three emission
// targets. Target payloads are computed per the entrypoint's target set and
// keep their own error so strict/lenient parse policies stay per-target.
type PredicateLeaf struct {
	// Attr is the original attribute name; Source the leaf it came from.
	Attr   string
	Source *forma.KvCondition

	// HasMeta / Meta carry the schema cache lookup result (AttributeID,
	// ValueType, ColumnBinding).
	HasMeta bool
	Meta    forma.AttributeMetadata

	// Operator is the canonical lenient-parse operator; SQLOperator its SQL
	// form and Pattern the LIKE wildcarding, both zero when the operator is
	// unknown (the payloads carry the error).
	Operator    PredicateOperator
	SQLOperator string
	Pattern     PatternKind

	// Storage and Pushable summarize main-table placement: Storage is where
	// the leaf's value lives, Pushable whether pg-main may push it down.
	Storage  PredicateStorage
	Pushable bool

	PgEav  PgEavLeafPayload
	PgMain PgMainLeafPayload
	Duck   DuckLeafPayload
	Hybrid HybridLeafPayload
}

func (*PredicateLeaf) isPredicateNode() {}

// HybridLeafPayload carries the parse-once results the hybrid condition
// builder needs to emit either a main-table predicate or an EAV EXISTS
// subquery. Routing (IsMain) follows the hybrid column-resolution contract:
// raw entity_main columns and cache-bound columns emit against entity_main;
// every other attribute falls to the EAV EXISTS form. Placeholder assignment,
// identifier sanitization, table names, and the anchor alias remain the
// emitter's responsibility (they are request/builder state), mirroring the
// pg-main / pg-eav emitters.
type HybridLeafPayload struct {
	// Err is the main-branch resolution error (unsupported operator, unknown
	// main table column, or value conversion), surfaced verbatim at emit time
	// before any placeholder is consumed.
	Err    error
	IsMain bool

	// Main-table branch (lenient parse + descriptor-validated metadata).
	MainColumn string // physical column name, unsanitized
	MainSQLOp  string
	MainValue  any
	// MainCast is the type MainValue's placeholder is cast to; empty lets
	// the column type it. A numeric comparison against a whole-number
	// column binds an int64 cast to bigint (hybridMainComparison).
	MainCast string
	// MainBool replaces MainSQLOp/MainValue for a bool-encoded leaf under
	// = / != (#565): the emitter allocates Arity() placeholders, renders
	// it over them and binds Args(). Nil for every other main-branch leaf.
	MainBool *BoolMainPredicate
	// The #646 leaf kinds replace MainValue; at most one is set. MainNullTest
	// is SQLOpIsNull/SQLOpIsNotNull (no bind), MainSet the array bind of an
	// in / not_in leaf, MainNow the offset bind of a $now leaf compared under
	// MainSQLOp. RenderMain spells all of them.
	MainNullTest string
	MainSet      *SetOperand
	MainNow      *ServerTimeOperand

	// EAV branch — computed identically to PgEavLeafPayload (strict parse).
	Eav PgEavLeafPayload
}

// PgEavLeafPayload is the EAV EXISTS emission payload (strict parse policy).
type PgEavLeafPayload struct {
	Err         error
	AttrID      int16
	ValueColumn string
	SQLOp       string
	Value       any
	// Truthy marks a bool leaf: the comparison runs on the BoolTruthiness
	// image every DuckDB leg already derives, with Value a Go bool, so a
	// stored 2 answers the same on the OLTP route and the federated tiers
	// (#384; the spelling is the #404 read-side rule).
	Truthy bool
	// The #646 leaf kinds replace Value; at most one is set. NullTest
	// (SQLOpIsNull/SQLOpIsNotNull) makes the leaf a presence test with no
	// value bind; List marks a list attribute, whose empty-list marker row
	// counts as present (#204). Set is the array bind of an in / not_in
	// leaf; Now the offset bind of a $now leaf compared under SQLOp.
	NullTest string
	List     bool
	Set      *SetOperand
	Now      *ServerTimeOperand
}

// ComparisonLHS renders the left-hand side of the EAV EXISTS comparison for
// the payload's value column, qualified by the eav_data alias. Bool leaves
// compare the BoolTruthiness image instead of the raw column (#384).
func (p PgEavLeafPayload) ComparisonLHS(alias string) string {
	if p.Truthy {
		return BoolTruthiness(alias + "." + p.ValueColumn)
	}
	return alias + "." + p.ValueColumn
}

// PgMainLeafPayload is the entity_main pushdown payload (lenient parse
// policy). Skip marks leaves pg-main silently drops (unknown attribute or
// no column binding).
type PgMainLeafPayload struct {
	Err    error
	Skip   bool
	Column string
	SQLOp  string
	Value  any
	// Bool replaces SQLOp/Value for a bool-encoded leaf under = / != (#565):
	// the emitter allocates Arity() placeholders, renders it over them and
	// binds Args().
	Bool *BoolMainPredicate
	// NullTest (SQLOpIsNull/SQLOpIsNotNull) replaces SQLOp/Value for an
	// is_null / is_not_null leaf (#646): no bind, no placeholder.
	NullTest string
}

// DuckLeafPayload is the DuckDB emission payload (lenient parse policy).
// TextLike leaves bind the string value without a CAST wrapper.
type DuckLeafPayload struct {
	Err       error
	Column    string
	SQLOp     string
	ValueType forma.ValueType
	TextLike  bool
	Param     any
	// NullTest (SQLOpIsNull/SQLOpIsNotNull) renders "<col> IS [NOT] NULL"
	// with no bind (#646). Set renders an in / not_in leaf against Param,
	// which then holds the exactly-typed list the cast names.
	NullTest string
	Set      *DuckSetOperand
}

// predicateNilNode marks a nil condition (top-level or composite child);
// each style resolves it via its nilNode behavior.
type predicateNilNode struct{}

func (predicateNilNode) isPredicateNode() {}

// predicateInvalidNode preserves the walker's lazy "unsupported condition
// type" error for condition implementations the normalizer does not know.
type predicateInvalidNode struct{ err error }

func (predicateInvalidNode) isPredicateNode() {}
