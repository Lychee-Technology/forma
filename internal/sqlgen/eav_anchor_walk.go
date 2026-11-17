package sqlgen

import "github.com/lychee-technology/forma"

// EAVAnchorLeafEmitter renders the leaves of a condition anchored on
// eav_data: EmitTypedLeaf as a test of the entity the anchor row t belongs
// to, EmitRowLeaf as a test of the row t itself.
type EAVAnchorLeafEmitter interface {
	TypedLeafEmitter
	EmitRowLeaf(leaf *PredicateLeaf) (sql string, args []any, err error)
}

// WalkHybridEAVAnchor renders cond like WalkHybridCondition for the
// PostgreSQL route's eav_data anchor, which keeps an entity when some
// eav_data row t of it satisfies the clause (#881). Wherever the clause
// stays equivalent, leaves test t itself rather than an EXISTS over a
// second eav_data row correlated on t, so the anchor reads the rows the
// predicate selects instead of every eav_data row of the schema.
func WalkHybridEAVAnchor(cond forma.Condition, cache forma.SchemaAttributeCache, emit EAVAnchorLeafEmitter) (string, []any, error) {
	node := normalizePredicates(cond, cache, targetHybrid)
	leaves, _ := rowLeaves(node)
	return walkPredicate(node, hybridStyle, eavAnchorEmitter{emit: emit, onRow: leaves})
}

type eavAnchorEmitter struct {
	emit  EAVAnchorLeafEmitter
	onRow map[*PredicateLeaf]bool
}

func (e eavAnchorEmitter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	if e.onRow[leaf] {
		return e.emit.EmitRowLeaf(leaf)
	}
	return e.emit.EmitTypedLeaf(leaf)
}

// rowLeaves chooses the leaves of node to render on the anchor row t, and
// reports whether node, so rendered, holds for some row t of an entity
// exactly when node holds for the entity. Written R for node so rendered,
// the anchor's "some t with R(t)" must equal node:
//
//   - a leaf L that MatchesOnRow is EXISTS (x with P(x)); with R = P, some t
//     with P(t) is that EXISTS. is_null and main-table leaves are not.
//   - an and of A with the rest B, A so rendered: some t with A(t) and B is
//     (some t with A(t)) and B, since B tests the entity, not t: A and B.
//     One child is enough and the first that qualifies is taken; the others
//     keep their EXISTS.
//   - an or of children all so rendered: "some t" distributes over or, so
//     some t with A(t) or C(t) is A or C. One that does not qualify leaves
//     the whole or as it was.
//
// A node that does not qualify renders unchanged, and the anchor keeps the
// entities with an eav_data row that satisfy it: the pre-#881 answer, which
// an empty or (1=0) and an or with an is_null child need. Each leaf is its
// own pointer (normalizePredicates builds one per KvCondition), so the map
// names positions, not shared values.
func rowLeaves(node PredicateNode) (map[*PredicateLeaf]bool, bool) {
	switch n := node.(type) {
	case *PredicateLeaf:
		if n.Hybrid.IsMain || !n.Hybrid.Eav.MatchesOnRow() {
			return nil, false
		}
		return map[*PredicateLeaf]bool{n: true}, true
	case *PredicateGroup:
		return rowLeavesOfGroup(n)
	default:
		return nil, false
	}
}

func rowLeavesOfGroup(g *PredicateGroup) (map[*PredicateLeaf]bool, bool) {
	if len(g.Children) == 0 {
		return nil, false
	}
	switch g.Logic {
	case forma.LogicAnd:
		for _, child := range g.Children {
			if leaves, ok := rowLeaves(child); ok {
				return leaves, true
			}
		}
		return nil, false
	case forma.LogicOr:
		all := map[*PredicateLeaf]bool{}
		for _, child := range g.Children {
			leaves, ok := rowLeaves(child)
			if !ok {
				return nil, false
			}
			for leaf := range leaves {
				all[leaf] = true
			}
		}
		return all, true
	default:
		return nil, false
	}
}
