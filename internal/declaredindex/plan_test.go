package declaredindex

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

func bound(col forma.MainColumn) forma.AttributeMetadata {
	return forma.AttributeMetadata{ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: col}}
}

// outbox is the issue's LTFlow shape: status → text_01, next_attempt_at →
// bigint_01, locked_until → bigint_02, partition_key → text_02, enqueued_at →
// bigint_03.
func outbox() SchemaDeclarations {
	return SchemaDeclarations{
		Schema: "outbox_message",
		Declarations: []forma.IndexDeclaration{
			{Name: "due", Attributes: []string{"status", "next_attempt_at"}},
			{Name: "lease", Attributes: []string{"status", "locked_until"}},
			{Name: "fifo", Attributes: []string{"partition_key", "enqueued_at"}},
		},
		Attributes: forma.SchemaAttributeCache{
			"status":          bound(forma.MainColumnText01),
			"partition_key":   bound(forma.MainColumnText02),
			"next_attempt_at": bound(forma.MainColumnBigint01),
			"locked_until":    bound(forma.MainColumnBigint02),
			"enqueued_at":     bound(forma.MainColumnBigint03),
			"payload":         {ValueType: forma.ValueTypeText},
		},
	}
}

func TestPlan_MapsEachDeclarationToItsColumnTuple(t *testing.T) {
	planned, err := Plan("entity_main", []SchemaDeclarations{outbox()})
	require.NoError(t, err)
	require.Len(t, planned, 3)

	byTuple := make(map[string]Index)
	for _, ix := range planned {
		byTuple[strings.Join(ix.Columns, ",")] = ix
		assert.Equal(t, "entity_main", ix.Table)
		assert.Equal(t, Name("entity_main", ix.Columns), ix.Name)
	}
	due := byTuple["ltbase_schema_id,text_01,bigint_01,ltbase_row_id"]
	assert.Equal(t, []Source{{Schema: "outbox_message", Index: "due"}}, due.Sources)
	lease := byTuple["ltbase_schema_id,text_01,bigint_02,ltbase_row_id"]
	assert.Equal(t, []Source{{Schema: "outbox_message", Index: "lease"}}, lease.Sources)
	fifo := byTuple["ltbase_schema_id,text_02,bigint_03,ltbase_row_id"]
	assert.Equal(t, []Source{{Schema: "outbox_message", Index: "fifo"}}, fifo.Sources)

	for i := 1; i < len(planned); i++ {
		assert.Less(t, planned[i-1].Name, planned[i].Name, "the plan is sorted by name")
	}
}

// The issue's Given: due over status → text_01 and next_attempt_at →
// bigint_01 builds (ltbase_schema_id, text_01, bigint_01, ltbase_row_id),
// non-partial, concurrently and idempotently. Golden text: an independent
// literal, never rebuilt from the renderer.
func TestCreateSQL_Golden(t *testing.T) {
	ix := Index{Table: "entity_main", Name: "entity_main_cx_0123456789abcdef",
		Columns: []string{"ltbase_schema_id", "text_01", "bigint_01", "ltbase_row_id"}}
	assert.Equal(t,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS "entity_main_cx_0123456789abcdef" ON "entity_main" (ltbase_schema_id, "text_01", "bigint_01", ltbase_row_id)`,
		ix.CreateSQL())

	qualified := Index{Table: "tenant.entity_main", Name: "tenant_entity_main_cx_0123456789abcdef",
		Columns: []string{"ltbase_schema_id", "uuid_01", "ltbase_row_id"}}
	assert.Equal(t,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS "tenant_entity_main_cx_0123456789abcdef" ON "tenant"."entity_main" (ltbase_schema_id, "uuid_01", ltbase_row_id)`,
		qualified.CreateSQL())
}

// Two schemas whose declarations bind the same column tuple share one physical
// index: ltbase_schema_id leads it, so neither reads the other's key range.
// Two declarations of one schema over the same tuple collapse the same way.
func TestPlan_DeduplicatesByColumnTuple(t *testing.T) {
	timers := SchemaDeclarations{
		Schema:       "timer",
		Declarations: []forma.IndexDeclaration{{Name: "due_at", Attributes: []string{"state", "fire_at"}}},
		Attributes: forma.SchemaAttributeCache{
			"state":   bound(forma.MainColumnText01),
			"fire_at": bound(forma.MainColumnBigint01),
		},
	}
	dup := outbox()
	dup.Declarations = append(dup.Declarations, forma.IndexDeclaration{Name: "due_again", Attributes: []string{"status", "next_attempt_at"}})

	planned, err := Plan("entity_main", []SchemaDeclarations{timers, dup})
	require.NoError(t, err)
	require.Len(t, planned, 3)

	var shared *Index
	for i := range planned {
		if strings.Join(planned[i].Columns, ",") == "ltbase_schema_id,text_01,bigint_01,ltbase_row_id" {
			shared = &planned[i]
		}
	}
	require.NotNil(t, shared)
	assert.Equal(t, []Source{
		{Schema: "outbox_message", Index: "due"},
		{Schema: "outbox_message", Index: "due_again"},
		{Schema: "timer", Index: "due_at"},
	}, shared.Sources)
	assert.Equal(t, "outbox_message.due, outbox_message.due_again, timer.due_at", FormatSources(shared.Sources))
}

func TestPlan_IndependentOfInputOrder(t *testing.T) {
	timers := SchemaDeclarations{
		Schema:       "timer",
		Declarations: []forma.IndexDeclaration{{Name: "due_at", Attributes: []string{"state"}}},
		Attributes:   forma.SchemaAttributeCache{"state": bound(forma.MainColumnText05)},
	}
	reordered := outbox()
	reordered.Declarations = []forma.IndexDeclaration{reordered.Declarations[2], reordered.Declarations[0], reordered.Declarations[1]}

	first, err := Plan("entity_main", []SchemaDeclarations{outbox(), timers})
	require.NoError(t, err)
	second, err := Plan("entity_main", []SchemaDeclarations{timers, reordered})
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestPlan_RefusesUnboundAttributeAndEmptyTable(t *testing.T) {
	eav := outbox()
	eav.Declarations = []forma.IndexDeclaration{{Name: "by_payload", Attributes: []string{"payload"}}}
	_, err := Plan("entity_main", []SchemaDeclarations{eav})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outbox_message.by_payload")
	assert.Contains(t, err.Error(), "attribute payload has no column binding")

	_, err = Plan(" ", []SchemaDeclarations{outbox()})
	require.Error(t, err)
}

// The name derives from the table and the column tuple only: the same tuple
// names the same index whichever schema or declaration asks for it, the order
// of the key is part of the tuple, and every name fits PostgreSQL's 63-byte
// identifier limit so a later run finds it under the name it planned.
func TestName_DerivedFromTableAndTuple(t *testing.T) {
	tuple := []string{"ltbase_schema_id", "text_01", "bigint_01", "ltbase_row_id"}
	swapped := []string{"ltbase_schema_id", "bigint_01", "text_01", "ltbase_row_id"}

	name := Name("entity_main", tuple)
	assert.Regexp(t, `^entity_main_cx_[0-9a-f]{16}$`, name)
	assert.Equal(t, name, Name("entity_main", append([]string(nil), tuple...)))
	assert.NotEqual(t, name, Name("entity_main", swapped))
	assert.Regexp(t, `^entity_main_dev_cx_[0-9a-f]{16}$`, Name("entity_main_dev", tuple))
	assert.Regexp(t, `^tenant_entity_main_cx_[0-9a-f]{16}$`, Name(`"tenant"."entity_main"`, tuple))

	long := strings.Repeat("t", 80)
	longName := Name(long, tuple)
	assert.LessOrEqual(t, len(longName), 63)
	assert.True(t, isDeclaredName(long, longName))
}

func TestIsDeclaredName(t *testing.T) {
	name := Name("entity_main", []string{"ltbase_schema_id", "text_01", "ltbase_row_id"})
	assert.True(t, isDeclaredName("entity_main", name))
	for _, other := range []string{
		"entity_main_text_01_idx",         // init-db's per-column index
		"entity_main_pkey",                // the primary key
		"entity_main_cx_0123",             // digest too short
		"entity_main_cx_0123456789abcdeg", // not hex
		"other_table_cx_0123456789abcdef", // another table's namespace
	} {
		assert.False(t, isDeclaredName("entity_main", other), other)
	}
}

// stubRegistry answers like a DB-mode file registry for one schema with a
// document and one with only an attributes file.
type stubRegistry struct {
	docs   map[string]forma.JSONSchema
	caches map[string]forma.SchemaAttributeCache
}

func (s stubRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	return 1, s.caches[name], nil
}

func (s stubRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", nil, forma.NotFoundf("unused")
}

func (s stubRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	doc, ok := s.docs[name]
	if !ok {
		return 0, forma.JSONSchema{}, forma.NotFoundf("schema data not found: %s", name)
	}
	return 1, doc, nil
}

func (s stubRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, forma.NotFoundf("unused")
}

func (s stubRegistry) ListSchemas() []string {
	return []string{"zeta_attrs_only", "outbox_message", "plain"}
}

func TestFromRegistry(t *testing.T) {
	ob := outbox()
	registry := stubRegistry{
		docs: map[string]forma.JSONSchema{
			"outbox_message": {Name: "outbox_message", Indexes: ob.Declarations},
			"plain":          {Name: "plain"},
		},
		caches: map[string]forma.SchemaAttributeCache{"outbox_message": ob.Attributes},
	}
	schemas, err := FromRegistry(registry)
	require.NoError(t, err)
	assert.Equal(t, []SchemaDeclarations{ob}, schemas, "a schema without a document or without declarations contributes nothing")

	none, err := FromRegistry(nil)
	require.NoError(t, err)
	assert.Nil(t, none)
}
