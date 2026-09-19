package internal

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

func newLimitsTestManager(t *testing.T, config *forma.Config, repo *mockPersistentRecordRepository) forma.EntityManager {
	t.Helper()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	return mustNewEntityManager(t, transformer, repo, nil, registry, config, nil)
}

func visitCreateOps(n int) []forma.EntityOperation {
	ops := make([]forma.EntityOperation, n)
	for i := range ops {
		ops[i] = forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
			Type:             forma.OperationCreate,
			Data:             visitPayload("visit-limit"),
		}
	}
	return ops
}

// TestBatchOverMaxBatchSizeIsRefusedBeforeTheRepository pins #465: a batch
// larger than PerformanceConfig.MaxBatchSize is refused as caller input
// (ErrInvalidInput, a 400 at the HTTP boundary) before any operation reaches
// the repository, on every batch entry point and for both execution modes.
func TestBatchOverMaxBatchSizeIsRefusedBeforeTheRepository(t *testing.T) {
	config := createTestConfig()
	config.Performance.MaxBatchSize = 2
	repo := newMockPersistentRecordRepository()
	em := newLimitsTestManager(t, config, repo)

	for _, atomic := range []bool{false, true} {
		req := &forma.BatchOperation{Operations: visitCreateOps(3), Atomic: atomic}
		_, err := em.BatchCreate(context.Background(), req)
		if !errors.Is(err, forma.ErrInvalidInput) {
			t.Fatalf("atomic=%v: expected ErrInvalidInput, got %v", atomic, err)
		}
		if _, err := em.BatchUpdate(context.Background(), req); !errors.Is(err, forma.ErrInvalidInput) {
			t.Fatalf("atomic=%v: BatchUpdate expected ErrInvalidInput, got %v", atomic, err)
		}
		if _, err := em.BatchDelete(context.Background(), req); !errors.Is(err, forma.ErrInvalidInput) {
			t.Fatalf("atomic=%v: BatchDelete expected ErrInvalidInput, got %v", atomic, err)
		}
	}
	if len(repo.insertedRecords) != 0 || repo.batchUpdateCalls != 0 || repo.deleteCalls != 0 {
		t.Fatalf("an oversized batch reached the repository: inserts=%d updates=%d deletes=%d",
			len(repo.insertedRecords), repo.batchUpdateCalls, repo.deleteCalls)
	}

	// Exactly the cap is admitted.
	result, err := em.BatchCreate(context.Background(), &forma.BatchOperation{Operations: visitCreateOps(2)})
	if err != nil {
		t.Fatalf("a batch at the cap must be admitted: %v", err)
	}
	if len(result.Successful) != 2 {
		t.Fatalf("expected 2 successful creates, got %d (failed: %+v)", len(result.Successful), result.Failed)
	}
}

// TestZeroMaxBatchSizeLeavesBatchesUnbounded pins the zero-value contract a
// library embedder with a hand-built config relies on.
func TestZeroMaxBatchSizeLeavesBatchesUnbounded(t *testing.T) {
	config := createTestConfig()
	if config.Performance.MaxBatchSize != 0 {
		t.Fatalf("test config must leave MaxBatchSize unset, got %d", config.Performance.MaxBatchSize)
	}
	em := newLimitsTestManager(t, config, newMockPersistentRecordRepository())
	result, err := em.BatchCreate(context.Background(), &forma.BatchOperation{Operations: visitCreateOps(5)})
	if err != nil {
		t.Fatalf("unbounded batch failed: %v", err)
	}
	if len(result.Successful) != 5 {
		t.Fatalf("expected 5 successful creates, got %d", len(result.Successful))
	}
}

// TestPageOffsetRefusesOverflow pins the arithmetic guard (#465): a page whose
// offset does not fit in an int is caller input to refuse, never a negative
// offset handed to SQL generation.
func TestPageOffsetRefusesOverflow(t *testing.T) {
	if got, err := pageOffset(3, 10); err != nil || got != 20 {
		t.Fatalf("pageOffset(3, 10) = %d, %v; want 20, nil", got, err)
	}
	if got, err := pageOffset(1, 100); err != nil || got != 0 {
		t.Fatalf("pageOffset(1, 100) = %d, %v; want 0, nil", got, err)
	}
	last := math.MaxInt/100 + 1
	if got, err := pageOffset(last, 100); err != nil || got != (last-1)*100 {
		t.Fatalf("pageOffset(%d, 100) = %d, %v; want the largest fitting offset", last, got, err)
	}
	for _, page := range []int{last + 1, math.MaxInt} {
		got, err := pageOffset(page, 100)
		if !errors.Is(err, forma.ErrInvalidInput) {
			t.Fatalf("pageOffset(%d, 100) = %d, %v; want ErrInvalidInput", page, got, err)
		}
		if got < 0 {
			t.Fatalf("pageOffset(%d, 100) leaked a negative offset %d", page, got)
		}
	}
	if _, err := pageOffset(0, 100); !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("pageOffset(0, 100) must refuse a non-positive page, got %v", err)
	}
}

// TestQueryWithOverflowingPageIsRefusedBeforeTheRepository drives the guard
// through the manager: the request fails as ErrInvalidInput and the
// repository never sees a negative offset.
func TestQueryWithOverflowingPageIsRefusedBeforeTheRepository(t *testing.T) {
	repo := newMockPersistentRecordRepository()
	em := newLimitsTestManager(t, createTestConfig(), repo)

	_, err := em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: math.MaxInt, ItemsPerPage: 100,
	})
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
	if len(repo.queries) != 0 {
		t.Fatalf("an overflowing page reached the repository with offset %d", repo.queries[0].Offset)
	}

	_, err = em.CrossSchemaSearch(context.Background(), &forma.CrossSchemaRequest{
		SchemaNames: []string{"visit"}, SearchTerm: "x", Page: math.MaxInt, ItemsPerPage: 100,
	})
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("cross-schema: expected ErrInvalidInput, got %v", err)
	}
	if len(repo.queries) != 0 {
		t.Fatalf("an overflowing cross-schema page reached the repository")
	}
}

// TestQueryTimeoutCancelsABlockingRepository pins the read budget (#465): a
// repository that only returns when its context is done is released by
// QueryConfig.DefaultTimeout, and the caller sees context.DeadlineExceeded.
func TestQueryTimeoutCancelsABlockingRepository(t *testing.T) {
	config := createTestConfig()
	config.Query.DefaultTimeout = 50 * time.Millisecond
	repo := newMockPersistentRecordRepository()
	var sawDeadline bool
	repo.queryFunc = func(ctx context.Context, _ *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
		_, sawDeadline = ctx.Deadline()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	em := newLimitsTestManager(t, config, repo)

	start := time.Now()
	_, err := em.Query(context.Background(), &forma.QueryRequest{SchemaName: "visit"})
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if !sawDeadline {
		t.Fatalf("the repository context carried no deadline")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("query was not cancelled by the budget: took %s", elapsed)
	}
}

// TestTransactionTimeoutCancelsABlockingWrite pins the write side of #465:
// TransactionConfig.DefaultTimeout bounds the context every write hands the
// repository, on the single and the atomic batch path alike, and the failure
// surfaces as context.DeadlineExceeded (the 504 class at the HTTP boundary).
func TestTransactionTimeoutCancelsABlockingWrite(t *testing.T) {
	config := createTestConfig()
	config.Transaction.DefaultTimeout = 50 * time.Millisecond
	repo := newMockPersistentRecordRepository()
	var sawDeadline bool
	repo.insertFunc = func(ctx context.Context, _ *model.PersistentRecord) error {
		_, sawDeadline = ctx.Deadline()
		<-ctx.Done()
		return ctx.Err()
	}
	em := newLimitsTestManager(t, config, repo)

	writes := map[string]func() error{
		"create": func() error {
			_, err := em.Create(context.Background(), &visitCreateOps(1)[0])
			return err
		},
		"atomic batch create": func() error {
			_, err := em.BatchCreate(context.Background(), &forma.BatchOperation{Operations: visitCreateOps(2), Atomic: true})
			return err
		},
	}
	for name, write := range writes {
		sawDeadline = false
		start := time.Now()
		err := write()
		elapsed := time.Since(start)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s: expected context.DeadlineExceeded, got %v", name, err)
		}
		if !sawDeadline {
			t.Fatalf("%s: the repository context carried no deadline", name)
		}
		if elapsed > 5*time.Second {
			t.Fatalf("%s: write was not cancelled by the budget: took %s", name, elapsed)
		}
	}
}

// TestWithBudgetZeroLeavesTheContextUnbounded pins the "unset means no bound"
// half of the contract: a zero budget must not become an instant timeout.
func TestWithBudgetZeroLeavesTheContextUnbounded(t *testing.T) {
	ctx, cancel := withBudget(context.Background(), 0)
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatalf("a zero budget must not set a deadline")
	}
	bounded, cancel2 := withBudget(context.Background(), time.Minute)
	defer cancel2()
	if _, ok := bounded.Deadline(); !ok {
		t.Fatalf("a positive budget must set a deadline")
	}
	if got := budgetsFromConfig(nil); got != (requestBudgets{}) {
		t.Fatalf("nil config must yield zero budgets, got %+v", got)
	}
	got := budgetsFromConfig(forma.DefaultConfig(nil))
	if got.read != 30*time.Second || got.write != 30*time.Second || got.maxBatchSize != 1000 {
		t.Fatalf("default budgets = %+v; README documents 30s/30s/1000", got)
	}
}
