package internal

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Real-Postgres regressions for #457: the update path is a read-modify-write,
// so only a real database with real concurrent transactions can show whether
// it is guarded.

// concurrencyTestCache registers the attribute ids the EAV replace scope
// needs (#294): without a cache the repository refuses to scope its delete.
func concurrencyTestCache(t *testing.T, attrCount int) *schemameta.MetadataCache {
	t.Helper()
	cache := forma.SchemaAttributeCache{}
	for i := 1; i <= attrCount; i++ {
		name := fmt.Sprintf("attr_%02d", i)
		cache[name] = forma.AttributeMetadata{
			AttributeName: name,
			AttributeID:   int16(i),
			ValueType:     forma.ValueTypeText,
		}
	}
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("concurrency_schema", 1, cache))
	return mc
}

func attrValue(record *model.PersistentRecord, attrID int16) string {
	for _, attr := range record.OtherAttributes {
		if attr.AttrID == attrID && attr.ValueText != nil {
			return *attr.ValueText
		}
	}
	return ""
}

// TestMergePersistentRecordConcurrentDisjointUpdatesAllSurvive is #457's
// primary regression. Each writer sets one attribute nobody else touches and
// carries every other attribute it read forward — exactly what the service's
// merge does. Before the fix all writers merged onto the same pre-write
// snapshot and the last committer's delete-all-then-reinsert dropped the
// others' fields; the row ended with one attribute set instead of all eight.
func TestMergePersistentRecordConcurrentDisjointUpdatesAllSurvive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)

	const writers = 8
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, writers))

	rowID := uuid.New()
	seed := &model.PersistentRecord{SchemaID: 1, RowID: rowID}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, seed))

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			attrID := int16(i + 1)
			value := fmt.Sprintf("value-%02d", attrID)
			_, err := repo.MergePersistentRecord(ctx, tables, 1, rowID,
				func(_ context.Context, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
					if existing == nil {
						return nil, fmt.Errorf("merge base missing for %s", rowID)
					}
					// Carry every field read under the lock forward, then add
					// this writer's own — the service's merge in miniature.
					merged := &model.PersistentRecord{
						SchemaID:        1,
						RowID:           rowID,
						CreatedAt:       existing.CreatedAt,
						TextItems:       maps.Clone(existing.TextItems),
						OtherAttributes: append([]model.EAVRecord(nil), existing.OtherAttributes...),
					}
					if merged.TextItems == nil {
						merged.TextItems = map[string]string{}
					}
					merged.TextItems[fmt.Sprintf("text_%02d", attrID)] = value
					v := value
					merged.OtherAttributes = append(merged.OtherAttributes, model.EAVRecord{
						SchemaID: 1, RowID: rowID, AttrID: attrID, ValueText: &v,
					})
					return merged, nil
				})
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "writer %d failed", i)
	}

	final, err := repo.GetPersistentRecord(ctx, tables, 1, rowID)
	require.NoError(t, err)
	require.NotNil(t, final)

	for i := range writers {
		attrID := int16(i + 1)
		require.Equal(t, fmt.Sprintf("value-%02d", attrID), attrValue(final, attrID),
			"attribute %d was lost: a concurrent update replaced the whole document (#457)", attrID)
		require.Equal(t, fmt.Sprintf("value-%02d", attrID), final.TextItems[fmt.Sprintf("text_%02d", attrID)],
			"main column text_%02d was lost (#457)", attrID)
	}
}

// TestGetPersistentRecordIsNeverTorn is #457's torn-read regression. A writer
// flips a main column and an EAV attribute between two states that must
// always agree; a reader loops on GetPersistentRecord. The two-statement read
// this replaced could observe the main column from before a commit and the
// attribute from after it. The single-statement read cannot: one statement,
// one snapshot.
func TestGetPersistentRecordIsNeverTorn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, 1))

	rowID := uuid.New()
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, &model.PersistentRecord{SchemaID: 1, RowID: rowID}))

	write := func(state string) error {
		_, err := repo.MergePersistentRecord(ctx, tables, 1, rowID,
			func(_ context.Context, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
				v := state
				return &model.PersistentRecord{
					SchemaID:  1,
					RowID:     rowID,
					CreatedAt: existing.CreatedAt,
					TextItems: map[string]string{"text_01": state},
					OtherAttributes: []model.EAVRecord{
						{SchemaID: 1, RowID: rowID, AttrID: 1, ValueText: &v},
					},
				}, nil
			})
		return err
	}
	require.NoError(t, write("A"))

	done := make(chan struct{})
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeErr)
		states := []string{"A", "B"}
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := write(states[i%2]); err != nil {
				writeErr <- err
				return
			}
		}
	}()

	for range 300 {
		record, err := repo.GetPersistentRecord(ctx, tables, 1, rowID)
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Equal(t, record.TextItems["text_01"], attrValue(record, 1),
			"main column and EAV attribute came from different versions — the single-row read tore (#457)")
	}
	close(done)
	require.NoError(t, <-writeErr)
}
