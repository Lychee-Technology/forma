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
	// Truthy marks a bool leaf: the comparison runs on the value_numeric <> 0
	// truthiness every DuckDB leg already derives, with Value a Go bool, so a
	// stored 2 answers the same on the OLTP route and the federated tiers
	// (#384; the write-side truth table is #404).
	Truthy bool
}

// ComparisonLHS renders the left-hand side of the EAV EXISTS comparison for
// the payload's value column, qualified by the eav_data alias. Bool leaves
// compare the <> 0 truthiness instead of the raw column (#384).
func (p PgEavLeafPayload) ComparisonLHS(alias string) string {
	if p.Truthy {
		return "(" + alias + "." + p.ValueColumn + " <> 0)"
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
}

// predicateNilNode marks a nil condition (top-level or composite child);
// each style resolves it via its nilNode behavior.
type predicateNilNode struct{}

func (predicateNilNode) isPredicateNode() {}

// predicateInvalidNode preserves the walker's lazy "unsupported condition
// type" error for condition implementations the normalizer does not know.
type predicateInvalidNode struct{ err error }

func (predicateInvalidNode) isPredicateNode() {}
