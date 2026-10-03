package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/transform"
)

// Against Postgres, the row the writer stores for an array of objects nested
// inside another (#623) used to lose its record to an update of an unrelated
// attribute: the merge committed a row without it and replaceEAVAttributes
// deleted it (#619 review). The update is now refused before anything is
// written, so eav_data holds the same rows afterwards, and the update that
// replaces the array still commits.
func TestUnrelatedUpdateOverNestedArrayIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	registry := nestedArrayRegistry()
	_, attributes, err := registry.GetSchemaAttributeCacheByName("nested_array")
	require.NoError(t, err)
	metadata := schemameta.NewMetadataCache()
	require.NoError(t, metadata.RegisterSchema("nested_array", nestedArraySchema, attributes))
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := NewDBPersistentRecordRepository(pool, metadata)
	config := createTestConfig()
	config.Database.TableNames = forma.TableNames{EntityMain: tables.EntityMain, EAVData: tables.EAVData, ChangeLog: tables.ChangeLog}
	em := mustNewEntityManager(t, tr, repo, nil, registry, config, nil)

	rowID := uuid.New()
	data := nestedArrayDocument("c")
	data["stage"] = "new"
	record, err := tr.ToPersistentRecord(ctx, nestedArraySchema, rowID, data)
	require.NoError(t, err)
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, record))

	storedRows := func() []string {
		t.Helper()
		rows, err := pool.Query(ctx, fmt.Sprintf(
			"SELECT attr_id || '[' || array_indices || ']=' || value_text FROM %s WHERE schema_id = $1 AND row_id = $2 ORDER BY attr_id, array_indices",
			sanitizeIdentifier(tables.EAVData)), nestedArraySchema, rowID)
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
	require.Equal(t, []string{"1[0,0]=c", "3[]=new"}, storedRows())

	_, err = em.Update(ctx, nestedArrayUpdate(rowID, map[string]any{"stage": "contacted"}))
	require.ErrorContains(t, err,
		"attrID=1 arrayIndices=0,0: stored text value of attribute 'order.items.lots.code' rebuilds at 'order.items.lots.code.code'")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
	require.Equal(t, []string{"1[0,0]=c", "3[]=new"}, storedRows(), "rows after the refused update")

	replacement := nestedArrayDocument("d")
	replacement["stage"] = "contacted"
	_, err = em.Update(ctx, nestedArrayUpdate(rowID, replacement))
	require.NoError(t, err)
	require.Equal(t, []string{"1[0,0]=d", "3[]=contacted"}, storedRows(), "rows after the replacing update")
}
