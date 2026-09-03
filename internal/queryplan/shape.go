// Package queryplan provides the plan-cache primitives for #142: a stable
// query-shape fingerprint, a composite cache key, and a concurrency-safe
// cache for compiled planning artifacts. Values, cursor positions, and dirty
// IDs never participate in a shape hash — they are bound per request.
package queryplan

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
	"github.com/lychee-technology/forma/internal/model"
)

// HashFederatedQueryShape fingerprints everything about a federated query
// that determines the compiled plan: the condition-tree structure (attribute
// names, canonical operators, and/or logic, original child order), sort keys,
// anchor choice, and pagination mode including keyset columns and direction.
// Filter operands, keyset cursor values, limit, and offset are excluded.
func HashFederatedQueryShape(q *model.FederatedAttributeQuery) (string, error) {
	h := sha256.New()
	write := func(parts ...string) {
		for _, p := range parts {
			_, _ = h.Write([]byte(p))
			_, _ = h.Write([]byte{0})
		}
	}
	write("federated-shape-v1")

	if q == nil {
		write("nil-query")
		return hex.EncodeToString(h.Sum(nil)), nil
	}

	if err := hashCondition(h, q.Condition); err != nil {
		return "", err
	}

	write("sort")
	for _, o := range q.AttributeOrders {
		write(strconv.Itoa(int(o.AttrID)), o.AttrName, o.ColumnName,
			string(o.StorageLocation), string(o.SortOrder), string(o.ValueType))
	}

	write("anchor", strconv.FormatBool(q.UseMainAsAnchor))
	// Only hasHot membership shapes the skeleton (#184): hot-excluded tiers
	// prune the pg_source CTE, while warm-vs-cold share the single flat
	// parquet glob (glob differences live in the scope hash). Mirrors
	// sqlgen.FederatedQueryHasHot — keep the two in lockstep.
	write("tiers", strconv.FormatBool(queryHasHot(q)))

	// IsActive, not a nil check: a non-nil zero-column cursor renders the
	// OFFSET form (sqlgen keys the clause off the same predicate), so hashing
	// it apart from a nil cursor split the cache on two spellings of one
	// skeleton (#381 item 9). Columns and Mode participate; Values do not.
	if q.KeysetCursor.IsActive() {
		write("pagination", "keyset", string(q.KeysetCursor.Mode))
		for _, col := range q.KeysetCursor.Columns {
			write(col.Attribute, string(col.Direction))
		}
	} else {
		write("pagination", "offset")
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// queryHasHot mirrors sqlgen.FederatedQueryHasHot (queryplan must not import
// sqlgen): empty PreferredTiers defaults to all tiers; otherwise hot must be
// named for the pg_source form to render.
func queryHasHot(q *model.FederatedAttributeQuery) bool {
	if len(q.PreferredTiers) == 0 {
		return true
	}
	for _, tier := range q.PreferredTiers {
		if tier == model.DataTierHot {
			return true
		}
	}
	return false
}

func hashCondition(h hash.Hash, cond forma.Condition) error {
	write := func(parts ...string) {
		for _, p := range parts {
			_, _ = h.Write([]byte(p))
			_, _ = h.Write([]byte{0})
		}
	}
	switch c := cond.(type) {
	case nil:
		write("cond-nil")
		return nil
	case *forma.CompositeCondition:
		if c == nil {
			write("cond-nil")
			return nil
		}
		// Original child order is preserved deliberately: reordering AND/OR
		// children would change arg ordering in the compiled fragments.
		write("composite", string(c.Logic), strconv.Itoa(len(c.Conditions)))
		for _, child := range c.Conditions {
			if err := hashCondition(h, child); err != nil {
				return err
			}
		}
		return nil
	case *forma.KvCondition:
		if c == nil {
			write("cond-nil")
			return nil
		}
		parsed := conditionexpr.ParseOperatorValueLenient(c.Value)
		write("kv", c.Attr, conditionexpr.CanonicalOperator(parsed.Operator))
		return nil
	default:
		return fmt.Errorf("shape hash: unsupported condition type %T", cond)
	}
}

// HashScopeParts fingerprints ordered scope components (table names,
// connection strings, template identity, inline pagination values) that must
// isolate cache entries but are not part of the query shape.
func HashScopeParts(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
