package transform

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

type shapeKind int

const (
	shapeScalar shapeKind = iota
	shapeList
	// shapeLegacyArray is a primitive array under element-typed metadata, as
	// attributes files declared one before #204.
	shapeLegacyArray
	shapeObject
	shapeObjectArray
)

// shapeNode is one member of a random schema.
type shapeNode struct {
	name     string
	kind     shapeKind
	policy   forma.RequiredPolicy
	children []*shapeNode
}

func randomShape(r *rand.Rand, name string, depth int) *shapeNode {
	node := &shapeNode{name: name, kind: shapeKind(r.Intn(5))}
	if depth >= 4 {
		node.kind = shapeScalar
	}
	switch node.kind {
	case shapeScalar:
		node.policy = []forma.RequiredPolicy{"", "", "", "", forma.RequiredPolicyAlways, forma.RequiredPolicyIfParentPresent}[r.Intn(6)]
	case shapeObject, shapeObjectArray:
		node.children = make([]*shapeNode, 1+r.Intn(3))
		for i := range node.children {
			node.children[i] = randomShape(r, string(rune('a'+i)), depth+1)
		}
	}
	return node
}

// declare adds the attributes of node, under ids in random order: the stored
// order of a row's records follows them.
func (n *shapeNode) declare(path []string, cache forma.SchemaAttributeCache, ids *[]int) {
	path = append(path[:len(path):len(path)], n.name)
	if len(n.children) > 0 {
		for _, child := range n.children {
			child.declare(path, cache, ids)
		}
		return
	}
	meta := forma.AttributeMetadata{
		AttributeName: strings.Join(path, "."), AttributeID: int16((*ids)[0]),
		ValueType: forma.ValueTypeText, RequiredPolicy: n.policy,
	}
	*ids = (*ids)[1:]
	if n.kind == shapeList {
		meta.ValueType = forma.ValueTypeList
	}
	cache[meta.AttributeName] = meta
}

// value returns a random value of the node's shape, and false for an object
// left without members.
func (n *shapeNode) value(r *rand.Rand) (any, bool) {
	text := func() any { return fmt.Sprintf("v%d", r.Intn(1000)) }
	object := func() map[string]any {
		members := make(map[string]any)
		for _, child := range n.children {
			if child.policy == "" && r.Intn(4) == 0 {
				continue
			}
			if value, ok := child.value(r); ok {
				members[child.name] = value
			}
		}
		return members
	}
	switch n.kind {
	case shapeScalar:
		return text(), true
	case shapeList, shapeLegacyArray:
		items := make([]any, r.Intn(3))
		for i := range items {
			items[i] = text()
		}
		return items, true
	case shapeObject:
		members := object()
		return members, len(members) > 0
	default:
		items := []any{object()}
		if r.Intn(2) == 0 {
			items = append(items, object())
		}
		return items, true
	}
}

// Over random schemas and the rows the writer stores for random documents of
// them, an update that addresses none of a row's records writes every one of
// them back or is refused: it never succeeds having dropped one (#619 review).
// The schemas nest scalars, lists, legacy text-typed arrays, objects and
// arrays of objects in each other, which reaches the shapes the rebuild
// cannot place (#623) without naming them.
//
// A refusal by the merge is stored state, never caller input. One by the
// write that follows deletes nothing either, so it is not judged here.
func TestMergeUpdateNeverDropsStoredRecords(t *testing.T) {
	r := rand.New(rand.NewSource(624))
	ctx := context.Background()
	written, refused := 0, 0
	for i := 0; i < 20000; i++ {
		roots := make([]*shapeNode, 1+r.Intn(3))
		cache := forma.SchemaAttributeCache{"zz": {AttributeName: "zz", AttributeID: 100, ValueType: forma.ValueTypeText}}
		ids := r.Perm(99)
		doc := make(map[string]any)
		for j := range roots {
			roots[j] = randomShape(r, string(rune('p'+j)), 1)
			roots[j].declare(nil, cache, &ids)
			if value, ok := roots[j].value(r); ok {
				doc[roots[j].name] = value
			}
		}
		tr := NewPersistentRecordTransformer(&stubSchemaRegistry{schemaID: 700, schemaName: "shapes", cache: cache})
		rowID := uuid.Must(uuid.NewV7())
		stored, err := tr.ToPersistentRecord(ctx, 700, rowID, doc)
		if err != nil {
			continue
		}

		merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"zz": "u"}))
		if err != nil {
			require.NotErrorIs(t, err, forma.ErrInvalidInput, "document %v", doc)
			refused++
			continue
		}
		updated, err := tr.ToPersistentRecord(ctx, 700, rowID, merged)
		if err != nil {
			continue
		}
		require.ElementsMatch(t, append(storedKeys(stored.OtherAttributes), "100[]=u"),
			storedKeys(updated.OtherAttributes), "document %v merged as %v", doc, merged)
		written++
	}
	require.Positive(t, written)
	require.Positive(t, refused)
	t.Logf("%d rows written back whole, %d updates refused", written, refused)
}
