package internal

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// newFileSchemaRegistryFromDir creates a schema registry that loads schemas from
// a directory without requiring a database connection. Schema IDs are auto-assigned
// based on alphabetical order of schema names.
//
// Parameters:
//   - schemaDir: Directory containing *_attributes.json files
func newFileSchemaRegistryFromDir(schemaDir string) (forma.SchemaRegistry, error) {
	return schemameta.NewFileSchemaRegistryFromDirectory(schemaDir)
}

// Mock repository for testing
type mockPersistentRecordRepository struct {
	records            map[int16]map[uuid.UUID]*model.PersistentRecord
	insertedRecords    []*model.PersistentRecord
	deleteCalls        int
	lastQuery          *model.PersistentRecordQuery
	queries            []*model.PersistentRecordQuery
	queryFunc          func(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error)
	byAttrValuesCalls  []mockByAttrValuesCall
	atomicInsertFailAt int
	atomicUpdateFailAt int
	atomicDeleteFailAt int
}

type mockFederatedQueryEngine struct {
	lastTables model.StorageTables
	lastQuery  *model.FederatedAttributeQuery
	lastOpts   *model.FederatedQueryOptions
	queryFunc  func(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error)
}

func newMockPersistentRecordRepository() *mockPersistentRecordRepository {
	return &mockPersistentRecordRepository{
		records: make(map[int16]map[uuid.UUID]*model.PersistentRecord),
	}
}

func (m *mockPersistentRecordRepository) storeRecord(record *model.PersistentRecord) {
	if record == nil {
		return
	}
	if m.records[record.SchemaID] == nil {
		m.records[record.SchemaID] = make(map[uuid.UUID]*model.PersistentRecord)
	}
	m.records[record.SchemaID][record.RowID] = record
}

func (m *mockPersistentRecordRepository) InsertPersistentRecord(ctx context.Context, tables model.StorageTables, record *model.PersistentRecord) error {
	m.insertedRecords = append(m.insertedRecords, record)
	m.storeRecord(record)
	return nil
}

func (m *mockPersistentRecordRepository) UpdatePersistentRecord(ctx context.Context, tables model.StorageTables, record *model.PersistentRecord) error {
	m.storeRecord(record)
	return nil
}

func (m *mockPersistentRecordRepository) DeletePersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID) error {
	m.deleteCalls++
	if schemaRecords, ok := m.records[schemaID]; ok {
		delete(schemaRecords, rowID)
	}
	return nil
}

func (m *mockPersistentRecordRepository) GetPersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID) (*model.PersistentRecord, error) {
	if schemaRecords, ok := m.records[schemaID]; ok {
		if record, ok := schemaRecords[rowID]; ok {
			return record, nil
		}
	}
	return nil, nil
}

func (m *mockPersistentRecordRepository) QueryPersistentRecords(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
	if m.lastQuery == nil {
		m.lastQuery = query
	}
	m.queries = append(m.queries, query)
	if m.queryFunc != nil {
		return m.queryFunc(ctx, query)
	}

	schemaRecords := m.records[query.SchemaID]
	rowIDs := make([]uuid.UUID, 0, len(schemaRecords))
	for id := range schemaRecords {
		rowIDs = append(rowIDs, id)
	}

	sort.Slice(rowIDs, func(i, j int) bool {
		return rowIDs[i].String() < rowIDs[j].String()
	})

	total := len(rowIDs)
	start := min(query.Offset, total)
	end := total
	if query.Limit > 0 && start+query.Limit < end {
		end = start + query.Limit
	}

	selected := make([]*model.PersistentRecord, 0, end-start)
	for _, id := range rowIDs[start:end] {
		selected = append(selected, schemaRecords[id])
	}

	var totalPages int
	if query.Limit > 0 && total > 0 {
		totalPages = int((int64(total) + int64(query.Limit) - 1) / int64(query.Limit))
	}

	currentPage := 1
	if query.Limit > 0 && query.Offset > 0 {
		currentPage = (query.Offset / query.Limit) + 1
	}

	return &model.PersistentRecordPage{
		Records:      selected,
		TotalRecords: int64(total),
		TotalPages:   totalPages,
		CurrentPage:  currentPage,
	}, nil
}

func (m *mockFederatedQueryEngine) Query(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
	if fq == nil {
		return nil, fmt.Errorf("federated query cannot be nil")
	}
	m.lastTables = tables
	m.lastQuery = fq
	m.lastOpts = opts
	if m.queryFunc != nil {
		return m.queryFunc(ctx, tables, fq, opts)
	}
	return &model.PersistentRecordPage{}, nil
}

func cloneRecordStore(src map[int16]map[uuid.UUID]*model.PersistentRecord) map[int16]map[uuid.UUID]*model.PersistentRecord {
	cloned := make(map[int16]map[uuid.UUID]*model.PersistentRecord, len(src))
	for schemaID, schemaRecords := range src {
		inner := make(map[uuid.UUID]*model.PersistentRecord, len(schemaRecords))
		maps.Copy(inner, schemaRecords)
		cloned[schemaID] = inner
	}
	return cloned
}

func (m *mockPersistentRecordRepository) BatchInsertPersistentRecords(ctx context.Context, tables model.StorageTables, records []*model.PersistentRecord) error {
	snapshot := cloneRecordStore(m.records)
	insertedCount := len(m.insertedRecords)

	for i, record := range records {
		if m.atomicInsertFailAt > 0 && i+1 == m.atomicInsertFailAt {
			m.records = snapshot
			m.insertedRecords = m.insertedRecords[:insertedCount]
			return fmt.Errorf("forced atomic insert failure at index %d", i)
		}
		if err := m.InsertPersistentRecord(ctx, tables, record); err != nil {
			m.records = snapshot
			m.insertedRecords = m.insertedRecords[:insertedCount]
			return err
		}
	}
	return nil
}

func (m *mockPersistentRecordRepository) BatchUpdatePersistentRecords(ctx context.Context, tables model.StorageTables, records []*model.PersistentRecord) error {
	snapshot := cloneRecordStore(m.records)

	for i, record := range records {
		if m.atomicUpdateFailAt > 0 && i+1 == m.atomicUpdateFailAt {
			m.records = snapshot
			return fmt.Errorf("forced atomic update failure at index %d", i)
		}
		if err := m.UpdatePersistentRecord(ctx, tables, record); err != nil {
			m.records = snapshot
			return err
		}
	}
	return nil
}

func (m *mockPersistentRecordRepository) BatchDeletePersistentRecords(ctx context.Context, tables model.StorageTables, keys []model.PersistentRecordKey) error {
	snapshot := cloneRecordStore(m.records)
	deleteCalls := m.deleteCalls

	for i, key := range keys {
		if m.atomicDeleteFailAt > 0 && i+1 == m.atomicDeleteFailAt {
			m.records = snapshot
			m.deleteCalls = deleteCalls
			return fmt.Errorf("forced atomic delete failure at index %d", i)
		}
		if err := m.DeletePersistentRecord(ctx, tables, key.SchemaID, key.RowID); err != nil {
			m.records = snapshot
			m.deleteCalls = deleteCalls
			return err
		}
	}
	return nil
}

func buildPersistentRecord(t *testing.T, transformer model.PersistentRecordTransformer, schemaID int16, rowID uuid.UUID, data map[string]any) *model.PersistentRecord {
	t.Helper()
	record, err := transformer.ToPersistentRecord(context.Background(), schemaID, rowID, data)
	if err != nil {
		t.Fatalf("failed to build persistent record: %v", err)
	}
	return record
}

// Helper function to create test config
func createTestConfig() *forma.Config {
	return &forma.Config{
		Query: forma.QueryConfig{
			DefaultPageSize: 50,
			MaxPageSize:     100,
		},
		Entity: forma.EntityConfig{
			SchemaDirectory: "../cmd/server/schemas",
		},
	}
}
