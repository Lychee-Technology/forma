package internal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// Fixtures for the unset operator's tests (#938) over testdata/unset_ops,
// whose unsetdoc schema holds every kind of target: bound and EAV scalars,
// lists, nested scalars and lists under an ordinary object, an array of
// objects, a system-column alias, unique keys, and the required policies of
// the validation matrix (name required_always, owner.id
// required_if_parent_present, memo required by the JSON Schema only).

const (
	unsetSchemaDir  = "testdata/unset_ops"
	unsetSchemaName = "unsetdoc"
)

// unsetMode is how the manager validates an update's merged document.
type unsetMode string

const (
	unsetStrict      unsetMode = "strict"
	unsetReportOnly  unsetMode = "report-only"
	unsetNoValidator unsetMode = "no validator"
)

var unsetModes = []unsetMode{unsetStrict, unsetReportOnly, unsetNoValidator}

// unsetHarness is a manager over unsetdoc with the mock repository.
type unsetHarness struct {
	ctx      context.Context
	em       forma.EntityManager
	repo     *mockPersistentRecordRepository
	tr       model.PersistentRecordTransformer
	schemaID int16
	metrics  *metricRecorder
}

func newUnsetHarness(t *testing.T, mode unsetMode) *unsetHarness {
	t.Helper()
	return newUnsetHarnessWith(t, mode, nil)
}

// newUnsetHarnessWith is newUnsetHarness with configure, when not nil,
// adjusting the manager's config before it is built.
func newUnsetHarnessWith(t *testing.T, mode unsetMode, configure func(*forma.Config)) *unsetHarness {
	t.Helper()
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(unsetSchemaDir)
	require.NoError(t, err)
	var validator *schemavalidate.Validator
	if mode != unsetNoValidator {
		validator, err = schemavalidate.New(registry, unsetSchemaDir)
		require.NoError(t, err)
	}
	schemaID, _, err := registry.GetSchemaByName(unsetSchemaName)
	require.NoError(t, err)

	h := &unsetHarness{ctx: context.Background(), repo: newMockPersistentRecordRepository(),
		tr: transform.NewPersistentRecordTransformer(registry), schemaID: schemaID, metrics: &metricRecorder{}}
	h.repo.clock = func(context.Context) (int64, error) { return condClockMs, nil }
	config := createTestConfig()
	config.Entity.SchemaDirectory = unsetSchemaDir
	config.Entity.ValidateUpdatesStrict = mode == unsetStrict
	config.Metrics.Emitter = h.metrics
	if configure != nil {
		configure(config)
	}
	h.em = mustNewEntityManager(t, h.tr, h.repo, nil, registry, config, validator)
	return h
}

// unsetBaseDoc is a valid unsetdoc row holding every unsettable target.
func unsetBaseDoc() map[string]any {
	return map[string]any{
		"name": "n", "memo": "m", "label": "L", "note": "N", "short": "ok",
		"tags": []any{"a", "b"}, "rank": int64(4), "counter": int64(3),
		"profile": map[string]any{"nick": "k", "aliases": []any{"x", "y"}, "title": "T"},
		"owner":   map[string]any{"id": "o1", "name": "Owner"},
	}
}

// create writes data through the manager, which validates it strictly.
func (h *unsetHarness) create(t *testing.T, data map[string]any) uuid.UUID {
	t.Helper()
	record, err := h.em.Create(h.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: unsetSchemaName}, Data: data})
	require.NoError(t, err)
	return record.RowID
}

// seed stores data underneath the manager, without validating it.
func (h *unsetHarness) seed(t *testing.T, data map[string]any) uuid.UUID {
	t.Helper()
	rowID := uuid.New()
	h.repo.storeRecord(buildPersistentRecord(t, h.tr, h.schemaID, rowID, data))
	return rowID
}

func (h *unsetHarness) update(rowID uuid.UUID, updates map[string]any, ops ...forma.UpdateOp) (*forma.DataRecord, error) {
	return h.em.Update(h.ctx, unsetOp(rowID, updates, ops...))
}

func (h *unsetHarness) record(rowID uuid.UUID) *model.PersistentRecord {
	return h.repo.records[h.schemaID][rowID]
}

func (h *unsetHarness) stored(t *testing.T, rowID uuid.UUID) map[string]any {
	t.Helper()
	attrs, err := h.tr.FromPersistentRecord(h.ctx, h.record(rowID))
	require.NoError(t, err)
	return attrs
}

func unsetOp(rowID uuid.UUID, updates map[string]any, ops ...forma.UpdateOp) *forma.EntityOperation {
	return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: unsetSchemaName, RowID: rowID},
		Updates: updates, UpdateOps: ops}
}

// withoutPath answers a deep copy of doc without the value at attr's dotted
// path, and without any object that leaves empty: the document an unset of
// attr makes of doc. It is the tests' own oracle, written apart from the
// merge's removeDocumentValue.
func withoutPath(doc map[string]any, attr string) map[string]any {
	out := copyMapDeep(doc)
	dropPath(out, strings.Split(attr, "."))
	return out
}

// dropPath deletes path from m and reports whether m is left empty.
func dropPath(m map[string]any, path []string) bool {
	if len(path) == 1 {
		delete(m, path[0])
		return len(m) == 0
	}
	child, ok := m[path[0]].(map[string]any)
	if ok && dropPath(child, path[1:]) {
		delete(m, path[0])
	}
	return len(m) == 0
}

// unsetLogical answers doc as its JSON round trip without the read-only
// created alias, which the store stamps: documents built in Go and documents
// read back then compare by their logical value, whatever Go types hold it.
func unsetLogical(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(doc)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out))
	delete(out, "created")
	return out
}
