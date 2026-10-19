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
// order of a row's records follows them. Each declares the arrays of objects
// above it as its array ancestors.
func (n *shapeNode) declare(path, arrays []string, cache forma.SchemaAttributeCache, ids *[]int) {
	path = append(path[:len(path):len(path)], n.name)
	if n.kind == shapeObjectArray {
		arrays = append(arrays[:len(arrays):len(arrays)], strings.Join(path, "."))
	}
	if len(n.children) > 0 {
		for _, child := range n.children {
			child.declare(path, arrays, cache, ids)
		}
		return
	}
	meta := forma.AttributeMetadata{
		AttributeName: strings.Join(path, "."), AttributeID: int16((*ids)[0]),
		ValueType: forma.ValueTypeText, RequiredPolicy: n.policy,
		ArrayAncestors: append([]string{}, arrays...),
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

// stored returns what the row the writer stores for value reads back as when
// every record is placed where it was written, and false when it stores
// nothing: an object without stored members is absent, an element of an
// array of objects that stores nothing reads as {} before one that does and
// is absent after the last, and [] stores a marker only at a list.
func (n *shapeNode) stored(value any) (any, bool) {
	switch n.kind {
	case shapeScalar:
		return value, true
	case shapeList:
		return value, true
	case shapeLegacyArray:
		return value, len(value.([]any)) > 0
	case shapeObject:
		members := n.storedMembers(value.(map[string]any))
		return members, len(members) > 0
	default:
		var elements []any
		last := -1
		for i, item := range value.([]any) {
			members := n.storedMembers(item.(map[string]any))
			elements = append(elements, members)
			if len(members) > 0 {
				last = i
			}
		}
		return elements[:last+1], last >= 0
	}
}

func (n *shapeNode) storedMembers(object map[string]any) map[string]any {
	members := make(map[string]any)
	for _, child := range n.children {
		if value, present := object[child.name]; present {
			if kept, ok := child.stored(value); ok {
				members[child.name] = kept
			}
		}
	}
	return members
}

// readsPerField reports whether the schema nests anything but a scalar
// inside an array of objects. Without array ancestry the rebuild reads such
// an array as an object of per-field arrays (#623), so a member some elements
// lack leaves a gap the write refuses or judges at the wrong level: a null in
// a primitive array, or a {} that a required member is missing from. A
// list's empty marker inside the array reads it per-field as well.
func readsPerField(roots []*shapeNode) bool {
	for _, root := range roots {
		if root.nestsInObjectArray(false) {
			return true
		}
	}
	return false
}

// nestsInObjectArray reports whether n, or anything beneath it, is a
// non-scalar inside an array of objects.
func (n *shapeNode) nestsInObjectArray(insideObjectArray bool) bool {
	if insideObjectArray && n.kind != shapeScalar {
		return true
	}
	for _, child := range n.children {
		if child.nestsInObjectArray(insideObjectArray || n.kind == shapeObjectArray) {
			return true
		}
	}
	return false
}

// Over random schemas and the rows the writer stores for random documents of
// them, an update that addresses none of a row's records writes every one of
// them back or is refused: it never succeeds having dropped one (#619 review).
// The schemas nest scalars, lists, legacy text-typed arrays, objects and
// arrays of objects in each other, which reaches every shape the rebuild
// places by array ancestry (#623) without naming them.
//
// With array ancestry declared, every such update is accepted and its write
// succeeds: each record is placed where it was written. Without it, the
// rebuild infers each record's array from the row and cannot place what is
// nested inside an array of objects; the merge refuses some rows, which is
// stored state, never caller input, and a schema the rebuild reads
// per-field may still be refused on the write that follows an accepted
// merge. That deletes nothing either, so it is counted, not judged.
func TestMergeUpdateNeverDropsStoredRecords(t *testing.T) {
	t.Run("array ancestry", func(t *testing.T) {
		written, refused, perField := checkMergeUpdateKeepsStoredRecords(t, true, true)
		require.Positive(t, written)
		require.Zero(t, refused, "an update over a row of a schema with array ancestry is never refused")
		require.Zero(t, perField)
	})
	t.Run("inferred", func(t *testing.T) {
		written, refused, perField := checkMergeUpdateKeepsStoredRecords(t, false, false)
		require.Positive(t, written)
		require.Positive(t, refused)
		t.Logf("%d rows written back whole, %d updates refused, %d per-field rows refused on write (#623)",
			written, refused, perField)
	})
	// A registry that no longer derives a schema's ancestry, as one whose
	// document gained another file's $ref does, reads rows written with it
	// by inference. Those rows differ only in their empty-list markers inside
	// an array of objects, which carry indices the inference cannot place,
	// so their rebuild fails.
	t.Run("ancestry lost after the write", func(t *testing.T) {
		written, refused, perField := checkMergeUpdateKeepsStoredRecords(t, true, false)
		require.Positive(t, written)
		require.Positive(t, refused)
		t.Logf("%d rows written back whole, %d updates refused, %d per-field rows refused on write (#623)",
			written, refused, perField)
	})
	// Rows written before #623 hold those markers with no indices, which a
	// reading by ancestry places in the object form of the array, or under
	// the literal key beside its elements, where the write stores them back.
	t.Run("ancestry gained after the write", func(t *testing.T) {
		written, refused, perField := checkMergeUpdateKeepsStoredRecords(t, false, true)
		require.Positive(t, written)
		require.Zero(t, refused, "an update over a row read with array ancestry is never refused")
		require.Zero(t, perField)
	})
}

func storedDocument(roots []*shapeNode, doc map[string]any) map[string]any {
	want := make(map[string]any)
	for _, root := range roots {
		if value, present := doc[root.name]; present {
			if kept, ok := root.stored(value); ok {
				want[root.name] = kept
			}
		}
	}
	return want
}

// shapesRegistry registers declared as is, or with no attribute declaring
// its array ancestry.
func shapesRegistry(declared forma.SchemaAttributeCache, ancestry bool) forma.SchemaRegistry {
	cache := make(forma.SchemaAttributeCache, len(declared))
	for name, meta := range declared {
		if !ancestry {
			meta.ArrayAncestors = nil
		}
		cache[name] = meta
	}
	return &stubSchemaRegistry{schemaID: 700, schemaName: "shapes", cache: cache}
}

// checkMergeUpdateKeepsStoredRecords stores each row through a registry
// with array ancestry or without (storeAncestry), and updates it through
// one with or without (updateAncestry).
func checkMergeUpdateKeepsStoredRecords(t *testing.T, storeAncestry, updateAncestry bool) (written, refused, perField int) {
	t.Helper()
	r := rand.New(rand.NewSource(624))
	ctx := context.Background()
	for i := 0; i < 20000; i++ {
		roots := make([]*shapeNode, 1+r.Intn(3))
		cache := forma.SchemaAttributeCache{"zz": {AttributeName: "zz", AttributeID: 100, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}}}
		ids := r.Perm(99)
		doc := make(map[string]any)
		for j := range roots {
			roots[j] = randomShape(r, string(rune('p'+j)), 1)
			roots[j].declare(nil, nil, cache, &ids)
			if value, ok := roots[j].value(r); ok {
				doc[roots[j].name] = value
			}
		}
		rowID := uuid.Must(uuid.NewV7())
		stored, err := NewPersistentRecordTransformer(shapesRegistry(cache, storeAncestry)).ToPersistentRecord(ctx, 700, rowID, doc)
		if err != nil {
			continue
		}
		tr := NewPersistentRecordTransformer(shapesRegistry(cache, updateAncestry))
		if storeAncestry && updateAncestry {
			read, err := tr.FromPersistentRecord(ctx, stored)
			require.NoError(t, err, "document %v", doc)
			require.Equal(t, storedDocument(roots, doc), read, "document %v", doc)
		}

		merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"zz": "u"}))
		if err != nil {
			require.NotErrorIs(t, err, forma.ErrInvalidInput, "document %v", doc)
			refused++
			continue
		}
		updated, err := tr.ToPersistentRecord(ctx, 700, rowID, merged)
		if err != nil {
			require.True(t, readsPerField(roots), "document %v merged as %v: %v", doc, merged, err)
			perField++
			continue
		}
		require.ElementsMatch(t, append(storedKeys(stored.OtherAttributes), "100[]=u"),
			storedKeys(updated.OtherAttributes), "document %v merged as %v", doc, merged)
		written++
	}
	return written, refused, perField
}
