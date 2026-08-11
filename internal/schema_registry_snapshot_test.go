package internal

import (
	"errors"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// mutatingSchemaRegistry answers a different document on every read of the same
// name, which is what forma.SchemaRegistry permits and a database-backed
// implementation can do without meaning to.
type mutatingSchemaRegistry struct {
	reads     int
	listCalls int
	docErr    error
}

func (r *mutatingSchemaRegistry) ListSchemas() []string {
	r.listCalls++
	return []string{"child"}
}

func (r *mutatingSchemaRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	r.reads++
	if r.docErr != nil {
		return 0, forma.JSONSchema{}, r.docErr
	}
	return 7, forma.JSONSchema{
		ID: 7, Name: name, Schema: fmt.Sprintf(`{"title":"read %d"}`, r.reads),
	}, nil
}

func (r *mutatingSchemaRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	r.reads++
	return "child", forma.JSONSchema{ID: 7, Name: "child", Schema: fmt.Sprintf(`{"title":"read %d"}`, r.reads)}, nil
}

func (r *mutatingSchemaRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 7, forma.SchemaAttributeCache{}, nil
}

func (r *mutatingSchemaRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "child", forma.SchemaAttributeCache{}, nil
}

// TestSnapshotSchemaDocumentsReadsEachNameOnce is the property the startup pair
// depends on: however many consumers read the snapshot, and by whichever
// accessor, they all see the bytes of one read.
func TestSnapshotSchemaDocumentsReadsEachNameOnce(t *testing.T) {
	registry := &mutatingSchemaRegistry{}

	snapshot := SnapshotSchemaDocuments(registry)

	require.Equal(t, 1, registry.reads, "the capture is one read per listed name")
	require.Equal(t, 1, registry.listCalls)

	_, first, err := snapshot.GetSchemaByName("child")
	require.NoError(t, err)
	_, second, err := snapshot.GetSchemaByName("child")
	require.NoError(t, err)
	_, byID, err := snapshot.GetSchemaByID(7)
	require.NoError(t, err)

	require.Equal(t, `{"title":"read 1"}`, first.Schema)
	require.Equal(t, first.Schema, second.Schema,
		"a second consumer must not be handed a second read's document")
	require.Equal(t, first.Schema, byID.Schema, "the id accessor answers from the same capture")
	require.Equal(t, 1, registry.reads, "no accessor may go back to the registry for a captured name")

	require.Equal(t, []string{"child"}, snapshot.ListSchemas())
	require.Equal(t, 1, registry.listCalls, "the name list is captured too")
}

// TestSnapshotSchemaDocumentsReplaysTheRegistrysFailure keeps each consumer's own
// failure handling intact. schemavalidate.New and LoadRelationIndex both report a
// registry that lists a name it cannot serve, with their own messages and the
// registry's cause underneath; a snapshot that swallowed or re-worded the error
// would change what an operator reads at startup.
func TestSnapshotSchemaDocumentsReplaysTheRegistrysFailure(t *testing.T) {
	cause := errors.New("backing store unavailable")
	registry := &mutatingSchemaRegistry{docErr: cause}

	snapshot := SnapshotSchemaDocuments(registry)

	for range 3 {
		_, _, err := snapshot.GetSchemaByName("child")
		require.ErrorIs(t, err, cause, "the captured error keeps its identity, not just its text")
	}
	require.Equal(t, 1, registry.reads)

	// A failed read carries no usable id, so nothing may be claimed for id 0.
	_, _, err := snapshot.GetSchemaByID(0)
	require.NoError(t, err, "id 0 was never captured, so the read falls through to the registry")
}

// TestSnapshotSchemaDocumentsIsNilForANilRegistry pins the typed-nil hazard: a
// boxed nil pointer would defeat LoadRelationIndex's own nil check and panic on
// the first accessor call.
func TestSnapshotSchemaDocumentsIsNilForANilRegistry(t *testing.T) {
	require.Nil(t, SnapshotSchemaDocuments(nil))

	idx, err := LoadRelationIndex(SnapshotSchemaDocuments(nil))
	require.NoError(t, err)
	require.NotNil(t, idx)
}

// TestSnapshotSchemaDocumentsServesTheSameDocumentsAsTheRegistry is the
// equivalence the whole change rests on: for a registry that does answer
// consistently — every shipped one — the snapshot is indistinguishable from the
// registry itself, so nothing about the guards' verdicts changes.
func TestSnapshotSchemaDocumentsServesTheSameDocumentsAsTheRegistry(t *testing.T) {
	registry := serveRelationFixture(t, "relation_required_root")

	direct, directErr := LoadRelationIndex(registry)
	viaSnapshot, snapshotErr := LoadRelationIndex(SnapshotSchemaDocuments(registry))

	require.Error(t, directErr)
	require.Error(t, snapshotErr)
	require.Equal(t, directErr.Error(), snapshotErr.Error())
	require.Nil(t, direct)
	require.Nil(t, viaSnapshot)

	ok := serveRelationFixture(t, "relation_ok_not")
	fromRegistry, err := LoadRelationIndex(ok)
	require.NoError(t, err)
	fromSnapshot, err := LoadRelationIndex(SnapshotSchemaDocuments(ok))
	require.NoError(t, err)
	require.Equal(t, fromRegistry.Relations("child"), fromSnapshot.Relations("child"))
}
