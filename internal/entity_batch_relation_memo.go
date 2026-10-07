package internal

// relationRootMemo answers a schema's relation root names once per batch.
//
// What it removes, exactly: for a schema that *does* declare relation roots,
// RelationRootNames allocates a slice and sorts it on every call
// (relation_index.go). When a batch's operations share such a schema, calling it
// per operation redoes that allocation and sort for every row, and the answer is
// the same every time.
//
// It removes nothing for a schema with no relation roots. That path returns nil
// after one map lookup — no allocation, no sort — so memoising it swaps one map
// lookup for another and is a wash. It is cached anyway, for the reason given at
// resolve.
//
// The memo is per batch rather than per manager on purpose: the index itself is
// fixed for the manager's lifetime, but a memo shared across concurrent batches
// would need a lock to be safe, and this one is confined to a single call stack
// and needs none.
//
// lookup is a field rather than a *RelationIndex so the caching itself is
// testable: the answers alone cannot distinguish a hit from a recomputation,
// since both produce the same list.
type relationRootMemo struct {
	lookup func(string) []string
	byName map[string][]string
}

// newRelationRootMemo memoises idx's relation roots. A nil index is fine —
// RelationRootNames is nil-receiver-safe, and a method value taken from a nil
// pointer receiver is legal Go, so the nil is simply carried into the call.
func newRelationRootMemo(idx *RelationIndex) *relationRootMemo {
	return &relationRootMemo{lookup: idx.RelationRootNames, byName: make(map[string][]string)}
}

// resolve answers schema's relation roots, computing them at most once.
//
// Keyed on presence, not on the value being non-nil. A schema with no relation
// roots answers nil, and presence-keying caches that nil like any other answer.
//
// The reason is uniformity, not speed: re-deriving a nil costs one map lookup,
// which is what the cache probe costs too, so nothing is saved there. What is
// gained is a single rule — every schema is looked up once, whatever it answers
// — instead of "cache the schemas with roots, re-derive the ones without". One
// rule is easier to hold, and it keeps resolve a function of the schema name
// alone for the batch's lifetime.
func (m *relationRootMemo) resolve(schema string) []string {
	if roots, computed := m.byName[schema]; computed {
		return roots
	}
	roots := m.lookup(schema)
	m.byName[schema] = roots
	return roots
}
