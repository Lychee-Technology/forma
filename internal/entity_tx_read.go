package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// Reads and row assertions inside a Tx (#650). Get and Query run the
// manager's own read services bound to the session's transaction, so they
// answer what EntityManager.Get and Query answer, plus the Tx's own writes.
// Every statement they send, relation enrichment's included, runs on the
// transaction; the bound query service has no federated engine, and a
// federated request is refused before any service is called.

func (s *txSession) Get(ctx context.Context, req *forma.QueryRequest, opts ...forma.ReadOption) (*forma.DataRecord, error) {
	var record *forma.DataRecord
	err := s.do(ctx, "Get", func(ctx context.Context) error {
		read, err := resolveTxRead("Get", req, opts)
		if err != nil {
			return err
		}
		if read.ForUpdate && req.SchemaName != "" && req.RowID != nil {
			// A missing row locks nothing but keeps the per-row lock; the
			// read below then answers the same ErrNotFound Get always does.
			if err := s.lockRow(ctx, req.SchemaName, *req.RowID, nil); err != nil && !errors.Is(err, forma.ErrNotFound) {
				return err
			}
		}
		record, err = s.crud.Get(ctx, req)
		return err
	})
	return record, err
}

func (s *txSession) Query(ctx context.Context, req *forma.QueryRequest, opts ...forma.ReadOption) (*forma.QueryResult, error) {
	var result *forma.QueryResult
	err := s.do(ctx, "Query", func(ctx context.Context) error {
		read, err := resolveTxRead("Query", req, opts)
		if err != nil {
			return err
		}
		if read.ForUpdate {
			return forma.InvalidInputf("Query does not accept ForUpdate: it locks no rows; " +
				"lock a row with Get and ForUpdate, or with AssertRow")
		}
		result, err = s.query.Query(ctx, req)
		return err
	})
	return result, err
}

func (s *txSession) AssertRow(ctx context.Context, schemaName string, rowID uuid.UUID, preconditions forma.Condition) error {
	return s.do(ctx, "AssertRow", func(ctx context.Context) error {
		return s.lockRow(ctx, schemaName, rowID, preconditions)
	})
}

// lockRow takes the write lock of one row and checks preconditions on it,
// writing nothing. The schema and the precondition are checked before any
// statement is sent.
func (s *txSession) lockRow(ctx context.Context, schemaName string, rowID uuid.UUID, preconditions forma.Condition) error {
	if schemaName == "" {
		return forma.InvalidInputf("schema name is required to lock a row")
	}
	if rowID == uuid.Nil {
		return forma.InvalidInputf("row ID is required to lock a row of %s", schemaName)
	}
	schemaID, cache, err := s.crud.registry.GetSchemaAttributeCacheByName(schemaName)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}
	if err := validatePrecondition(preconditions, schemaName, cache); err != nil {
		return fmt.Errorf("failed to validate assertion on %s/%s: %w", schemaName, rowID, err)
	}
	guard := model.RowGuard{Precondition: preconditions}
	if err := s.rows.LockPersistentRecord(ctx, s.crud.resolveTables(), schemaID, rowID, guard); err != nil {
		return fmt.Errorf("failed to lock %s/%s: %w", schemaName, rowID, err)
	}
	return nil
}

// resolveTxRead applies a read's options and refuses a federated request:
// a Tx reads the PostgreSQL hot tier, which holds every live row and its own
// uncommitted writes, and nothing else.
func resolveTxRead(method string, req *forma.QueryRequest, opts []forma.ReadOption) (forma.ReadOptions, error) {
	var read forma.ReadOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&read)
		}
	}
	if req == nil {
		return read, forma.InvalidInputf("query request cannot be nil")
	}
	if req.Federated != nil && req.Federated.Enabled {
		return read, forma.InvalidInputf("%s inside a transaction reads the PostgreSQL hot tier only; "+
			"send the request without federated.enabled", method)
	}
	return read, nil
}

// boundTo returns a copy of s that reads through repository and enriches
// through enrich, with no federated engine and no read budget of its own: a
// Tx session's query service (#650), bounded by the InTx deadline.
func (s *entityQueryService) boundTo(repository model.PersistentRecordRepository, enrich dataRecordEnricher) *entityQueryService {
	bound := *s
	bound.repository = repository
	bound.federatedQueryEngine = nil
	bound.enrichDataRecords = enrich
	bound.readBudget = 0
	return &bound
}

// boundTo returns a copy of s that looks up relation parents through
// repository: inside a Tx, the session's transaction.
func (s *entityRelationService) boundTo(repository model.PersistentRecordRepository) *entityRelationService {
	bound := *s
	bound.repository = repository
	return &bound
}
