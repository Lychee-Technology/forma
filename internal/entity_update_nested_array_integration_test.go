package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/transform"
)

// Against Postgres, the row the writer stores for an array of objects nested
// inside another (#623) used to lose its record to an update of an unrelated
// attribute: the merge committed a row without it and replaceEAVAttributes
// deleted it (#619 review). With array ancestry declared the update writes
// it back. Without, the update is refused before anything is written, so
// eav_data holds the same rows afterwards. Either way the update that
// replaces the array commits.
func TestUnrelatedUpdateOverNestedArrayIntegration(t *testing.T) {
	for _, ancestry := range []bool{true, false} {
		t.Run(fmt.Sprintf("ancestry=%t", ancestry), func(t *testing.T) {
			checkUnrelatedUpdateOverNestedArray(t, ancestry)
		})
	}
}

// nestedArrayIntegration is an EntityManager over registry's schema writing
// to temporary tables in Postgres.
type nestedArrayIntegration struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	tables model.StorageTables
	tr     model.PersistentRecordTransformer
	repo   *DBPersistentRecordRepository
	em     forma.EntityManager
}

func newNestedArrayIntegration(t *testing.T, ctx context.Context, registry forma.SchemaRegistry) *nestedArrayIntegration {
	t.Helper()
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	_, attributes, err := registry.GetSchemaAttributeCacheByName("nested_array")
	require.NoError(t, err)
	metadata := schemameta.NewMetadataCache()
	require.NoError(t, metadata.RegisterSchema("nested_array", nestedArraySchema, attributes))
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := NewDBPersistentRecordRepository(pool, metadata)
	config := createTestConfig()
	config.Database.TableNames = forma.TableNames{EntityMain: tables.EntityMain, EAVData: tables.EAVData, ChangeLog: tables.ChangeLog}
	em := mustNewEntityManager(t, tr, repo, nil, registry, config, nil)
	return &nestedArrayIntegration{ctx: ctx, pool: pool, tables: tables, tr: tr, repo: repo, em: em}
}

// store writes data as row rowID through the writer.
func (e *nestedArrayIntegration) store(t *testing.T, rowID uuid.UUID, data map[string]any) {
	t.Helper()
	record, err := e.tr.ToPersistentRecord(e.ctx, nestedArraySchema, rowID, data)
	require.NoError(t, err)
	require.NoError(t, e.repo.InsertPersistentRecord(e.ctx, e.tables, record))
}

// storedRows names each eav_data row of rowID by attribute, array indices
// and text.
func (e *nestedArrayIntegration) storedRows(t *testing.T, rowID uuid.UUID) []string {
	t.Helper()
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(
		"SELECT attr_id || '[' || array_indices || ']=' || COALESCE(value_text, '<none>') FROM %s WHERE schema_id = $1 AND row_id = $2 ORDER BY attr_id, array_indices",
		sanitizeIdentifier(e.tables.EAVData)), nestedArraySchema, rowID)
	require.NoError(t, err)
	defer rows.Close()
	var stored []string
	for rows.Next() {
		var row string
		require.NoError(t, rows.Scan(&row))
		stored = append(stored, row)
	}
	require.NoError(t, rows.Err())
	return stored
}

func checkUnrelatedUpdateOverNestedArray(t *testing.T, ancestry bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := newNestedArrayIntegration(t, ctx, nestedArrayRegistry(ancestry))

	rowID := uuid.New()
	data := nestedArrayDocument("c")
	data["stage"] = "new"
	e.store(t, rowID, data)
	require.Equal(t, []string{"1[0,0]=c", "3[]=new"}, e.storedRows(t, rowID))

	_, err := e.em.Update(ctx, nestedArrayUpdate(rowID, map[string]any{"stage": "contacted"}))
	if ancestry {
		require.NoError(t, err)
		require.Equal(t, []string{"1[0,0]=c", "3[]=contacted"}, e.storedRows(t, rowID), "rows after the unrelated update")
		fetched, err := e.em.Get(ctx, &forma.QueryRequest{SchemaName: "nested_array", RowID: &rowID})
		require.NoError(t, err)
		require.Equal(t, nestedArrayDocument("c")["order"], fetched.Attributes["order"])
	} else {
		require.ErrorContains(t, err,
			"attrID=1 arrayIndices=0,0: stored text value of attribute 'order.items.lots.code' rebuilds at 'order.items.lots.code.code'")
		require.NotErrorIs(t, err, forma.ErrInvalidInput)
		require.Equal(t, []string{"1[0,0]=c", "3[]=new"}, e.storedRows(t, rowID), "rows after the refused update")
	}

	replacement := nestedArrayDocument("d")
	replacement["stage"] = "contacted"
	_, err = e.em.Update(ctx, nestedArrayUpdate(rowID, replacement))
	require.NoError(t, err)
	require.Equal(t, []string{"1[0,0]=d", "3[]=contacted"}, e.storedRows(t, rowID), "rows after the replacing update")
}

// Against Postgres, an empty list written through a literal dotted key that
// crosses the array of objects around it stores its marker with no indices.
// The read places it under that key, so an unrelated update writes its
// eav_data row back rather than delete it, and an update that rewrites the
// array with the list inside an element replaces it there.
func TestLiteralKeyMarkerBesideElementsIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := newNestedArrayIntegration(t, ctx, listInObjectArrayRegistry(true))
	rowID := uuid.New()
	e.store(t, rowID, map[string]any{"stage": "new", "order": map[string]any{
		"items": []any{map[string]any{"sku": "a"}}, "items.tags": []any{},
	}})
	require.Equal(t, []string{"3[]=new", "4[0]=a", "5[]=<none>"}, e.storedRows(t, rowID))

	_, err := e.em.Update(ctx, nestedArrayUpdate(rowID, map[string]any{"stage": "contacted"}))
	require.NoError(t, err)
	require.Equal(t, []string{"3[]=contacted", "4[0]=a", "5[]=<none>"}, e.storedRows(t, rowID), "rows after the unrelated update")
	fetched, err := e.em.Get(ctx, &forma.QueryRequest{SchemaName: "nested_array", RowID: &rowID})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"items": []any{map[string]any{"sku": "a"}}, "items.tags": []any{}}, fetched.Attributes["order"])
	projected, err := e.em.Get(ctx, &forma.QueryRequest{SchemaName: "nested_array", RowID: &rowID, Attrs: []string{"order.items.tags"}})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"order": map[string]any{"items.tags": []any{}}}, projected.Attributes, "projected Get")

	items := []any{map[string]any{"sku": "a", "tags": []any{}}}
	_, err = e.em.Update(ctx, nestedArrayUpdate(rowID, map[string]any{"order": map[string]any{"items": items}}))
	require.NoError(t, err)
	require.Equal(t, []string{"3[]=contacted", "4[0]=a", "5[0]=<none>"}, e.storedRows(t, rowID), "rows after the nested update")
	fetched, err = e.em.Get(ctx, &forma.QueryRequest{SchemaName: "nested_array", RowID: &rowID})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"items": items}, fetched.Attributes["order"])
}
