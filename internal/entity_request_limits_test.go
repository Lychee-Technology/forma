package internal

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/httpapi"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

func newLimitsTestManager(t *testing.T, config *forma.Config, repo *mockPersistentRecordRepository) forma.EntityManager {
	t.Helper()
	return newLimitsTestManagerWithEngine(t, config, repo, nil)
}

func newLimitsTestManagerWithEngine(t *testing.T, config *forma.Config, repo *mockPersistentRecordRepository, engine model.FederatedQueryEngine) forma.EntityManager {
	t.Helper()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	return mustNewEntityManager(t, transformer, repo, engine, registry, config, nil)
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
	if got, err := pageOffset(3, 10, 0); err != nil || got != 20 {
		t.Fatalf("pageOffset(3, 10) = %d, %v; want 20, nil", got, err)
	}
	if got, err := pageOffset(1, 100, 0); err != nil || got != 0 {
		t.Fatalf("pageOffset(1, 100) = %d, %v; want 0, nil", got, err)
	}
	last := math.MaxInt/100 + 1
	if got, err := pageOffset(last, 100, 0); err != nil || got != (last-1)*100 {
		t.Fatalf("pageOffset(%d, 100) = %d, %v; want the largest fitting offset", last, got, err)
	}
	for _, page := range []int{last + 1, math.MaxInt} {
		got, err := pageOffset(page, 100, 0)
		if !errors.Is(err, forma.ErrInvalidInput) {
			t.Fatalf("pageOffset(%d, 100) = %d, %v; want ErrInvalidInput", page, got, err)
		}
		if got < 0 {
			t.Fatalf("pageOffset(%d, 100) leaked a negative offset %d", page, got)
		}
	}
	if _, err := pageOffset(0, 100, 0); !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("pageOffset(0, 100) must refuse a non-positive page, got %v", err)
	}
}

// TestPageOffsetRefusesAWindowPastMaxRows pins the depth limit (#598): a page
// whose window ends exactly at maxRows is admitted, one that ends past it is
// refused with a published message naming the limit (even when its offset
// would overflow), and a non-positive maxRows leaves pagination unbounded.
func TestPageOffsetRefusesAWindowPastMaxRows(t *testing.T) {
	admitted := []struct{ page, itemsPerPage, maxRows, offset int }{
		{100, 100, 10000, 9900},  // ends exactly at the limit
		{3, 30, 100, 60},         // ends at 90, inside a limit the page size does not divide
		{1, 100, 100, 0},         // one full page is the smallest limit Validate allows
		{101, 100, 10100, 10000}, // tests/e2e/README.md: 10001 rows need a 10100 limit
	}
	for _, tc := range admitted {
		if got, err := pageOffset(tc.page, tc.itemsPerPage, tc.maxRows); err != nil || got != tc.offset {
			t.Fatalf("pageOffset(%d, %d, %d) = %d, %v; want %d, nil", tc.page, tc.itemsPerPage, tc.maxRows, got, err, tc.offset)
		}
	}

	refused := []struct{ page, itemsPerPage, maxRows int }{
		{101, 100, 10000},         // ends at 10100
		{4, 30, 100},              // ends at 120
		{101, 100, 10001},         // ends at 10100, though page 101 holds only row 10001
		{2, 100, 100},             // ends at 200
		{math.MaxInt, 100, 10000}, // the offset overflows; the limit still answers
	}
	for _, tc := range refused {
		got, err := pageOffset(tc.page, tc.itemsPerPage, tc.maxRows)
		if !errors.Is(err, forma.ErrInvalidInput) {
			t.Fatalf("pageOffset(%d, %d, %d) = %d, %v; want ErrInvalidInput", tc.page, tc.itemsPerPage, tc.maxRows, got, err)
		}
		requirePaginationLimitMessage(t, err, tc.maxRows)
	}

	// The issue's example walk, with no limit configured.
	for _, maxRows := range []int{0, -1} {
		if got, err := pageOffset(1_000_000, 1000, maxRows); err != nil || got != 999_999_000 {
			t.Fatalf("maxRows=%d must leave pagination unbounded, got %d, %v", maxRows, got, err)
		}
	}
}

// requirePaginationLimitMessage asserts err publishes the depth limit's
// message, the text the HTTP boundary answers a 400 with.
func requirePaginationLimitMessage(t *testing.T, err error, maxRows int) {
	t.Helper()
	msg, ok := forma.ResolvePublicMessage(err)
	want := fmt.Sprintf("exceeds the pagination limit of %d rows", maxRows)
	if !ok || !strings.Contains(msg, want) {
		t.Fatalf("published message %q (published=%v) does not name the limit: want %q", msg, ok, want)
	}
}

// TestQueryPastMaxRowsIsRefusedBeforeTheRepository drives the depth limit
// (#598) through the manager, on both routes of Query and on
// CrossSchemaSearch. The window is measured after the MaxPageSize clamp, in
// the rows the query would actually read, and a refused page reaches neither
// the repository nor the federated engine.
func TestQueryPastMaxRowsIsRefusedBeforeTheRepository(t *testing.T) {
	config := createTestConfig()
	config.Query.MaxRows = 1000
	repo := newMockPersistentRecordRepository()
	engine := &mockFederatedQueryEngine{}
	em := newLimitsTestManagerWithEngine(t, config, repo, engine)
	ctx := context.Background()
	query := func(page, itemsPerPage int, federated bool) error {
		req := &forma.QueryRequest{SchemaName: "visit", Page: page, ItemsPerPage: itemsPerPage}
		if federated {
			req.Federated = &forma.FederatedQueryRequest{Enabled: true}
		}
		_, err := em.Query(ctx, req)
		return err
	}
	search := func(page int) error {
		_, err := em.CrossSchemaSearch(ctx, &forma.CrossSchemaRequest{
			SchemaNames: []string{"visit"}, SearchTerm: "x", Page: page, ItemsPerPage: 100,
		})
		return err
	}

	refused := map[string]error{
		"query":                      query(11, 100, false),
		"query over the page cap":    query(11, 500, false), // clamps to 100 per page
		"federated query":            query(11, 100, true),
		"cross-schema search":        search(11),
		"overflowing federated page": query(math.MaxInt, 100, true),
	}
	for name, err := range refused {
		if !errors.Is(err, forma.ErrInvalidInput) {
			t.Fatalf("%s: expected ErrInvalidInput, got %v", name, err)
		}
		requirePaginationLimitMessage(t, err, 1000)
	}
	if len(repo.queries) != 0 || engine.lastQuery != nil {
		t.Fatalf("a page past the limit reached storage: repository queries=%d, federated=%v", len(repo.queries), engine.lastQuery != nil)
	}

	// The last page inside the limit is served at the offset it addresses, on
	// both routes, and so is a clamped page size whose window fits.
	for _, tc := range []struct {
		page, itemsPerPage int
		federated          bool
		offset             int
	}{{10, 100, false, 900}, {10, 500, false, 900}, {10, 100, true, 900}} {
		if err := query(tc.page, tc.itemsPerPage, tc.federated); err != nil {
			t.Fatalf("page %d (%d per page, federated=%v) is inside the limit: %v", tc.page, tc.itemsPerPage, tc.federated, err)
		}
	}
	if len(repo.queries) != 2 || repo.queries[0].Offset != 900 || repo.queries[1].Offset != 900 {
		t.Fatalf("expected two Postgres pages at offset 900, got %d queries", len(repo.queries))
	}
	if engine.lastQuery == nil || engine.lastQuery.Offset != 900 || engine.lastQuery.Limit != 100 {
		t.Fatalf("the federated page must reach the engine at offset 900, got %+v", engine.lastQuery)
	}
	if err := search(10); err != nil {
		t.Fatalf("the last cross-schema page inside the limit must be served: %v", err)
	}
}

// TestZeroMaxRowsLeavesPaginationUnbounded pins the zero-value contract a
// library embedder with a hand-built config relies on (#598): without a
// limit, a page goes as deep as its offset fits, as before #598.
func TestZeroMaxRowsLeavesPaginationUnbounded(t *testing.T) {
	config := createTestConfig()
	if config.Query.MaxRows != 0 {
		t.Fatalf("test config must leave MaxRows unset, got %d", config.Query.MaxRows)
	}
	repo := newMockPersistentRecordRepository()
	em := newLimitsTestManager(t, config, repo)

	if _, err := em.Query(context.Background(), &forma.QueryRequest{SchemaName: "visit", Page: 1_000_000, ItemsPerPage: 100}); err != nil {
		t.Fatalf("an unbounded deep page failed: %v", err)
	}
	if len(repo.queries) != 1 || repo.queries[0].Offset != 99_999_900 {
		t.Fatalf("the deep page must reach the repository at offset 99999900, got %d queries", len(repo.queries))
	}
	if _, err := em.CrossSchemaSearch(context.Background(), &forma.CrossSchemaRequest{
		SchemaNames: []string{"visit"}, SearchTerm: "x", Page: 1_000_000, ItemsPerPage: 100,
	}); err != nil {
		t.Fatalf("an unbounded deep cross-schema page failed: %v", err)
	}
}

// TestPastMaxRowsAnswers400NamingTheLimit is #598's acceptance shape at the
// HTTP boundary under the default limit: a page whose window ends past 10000
// rows answers 400 with a body naming the limit, on the GET query, the
// cross-schema search, and a federated advanced query, and nothing reaches
// the repository.
func TestPastMaxRowsAnswers400NamingTheLimit(t *testing.T) {
	config := createTestConfig()
	config.Query.MaxRows = forma.DefaultConfig(nil).Query.MaxRows
	repo := newMockPersistentRecordRepository()
	handler := httpapi.NewServer(newLimitsTestManager(t, config, repo), httpapi.Options{}).Handler()

	advanced := `{"schema_name":"visit","condition":{"l":"and","c":[]},"page":101,"items_per_page":100,"federated":{"enabled":true}}`
	requests := map[string]*http.Request{
		"query":                    httptest.NewRequest(http.MethodGet, "/api/v1/visit?page=101&items_per_page=100", nil),
		"search":                   httptest.NewRequest(http.MethodGet, "/api/v1/search?schemas=visit&q=x&page=101&items_per_page=100", nil),
		"federated advanced query": httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", strings.NewReader(advanced)),
	}
	for name, req := range requests {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d: %s", name, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "exceeds the pagination limit of 10000 rows") {
			t.Fatalf("%s: the body must name the limit, got %s", name, rec.Body.String())
		}
	}
	if len(repo.queries) != 0 {
		t.Fatalf("a page past the limit reached the repository %d times", len(repo.queries))
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
