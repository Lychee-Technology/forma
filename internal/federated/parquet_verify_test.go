package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type verifyFakeRows struct {
	rowsLeft  int
	nextCalls int
	streamErr error
}

func (r *verifyFakeRows) Next() bool {
	r.nextCalls++
	if r.rowsLeft > 0 {
		r.rowsLeft--
		return true
	}
	return false
}
func (r *verifyFakeRows) Scan(dest ...any) error { return nil }
func (r *verifyFakeRows) Err() error             { return r.streamErr }
func (r *verifyFakeRows) Close() error           { return nil }

// verifyFakeDuck fails Query for any SQL mentioning a failPath; midStream
// paths open fine and fail while iterating (the corruptMidFile class).
type verifyFakeDuck struct {
	failOpen    map[string]bool
	failStream  map[string]bool
	queries     []string
	healthyIter *verifyFakeRows // track the healthy path's iterator for assertion
}

func (d *verifyFakeDuck) Query(ctx context.Context, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	d.queries = append(d.queries, sqlStr)
	for p := range d.failOpen {
		if strings.Contains(sqlStr, p) {
			return nil, fmt.Errorf("IO Error: cannot open %s", p)
		}
	}
	for p := range d.failStream {
		if strings.Contains(sqlStr, p) {
			return &verifyFakeRows{rowsLeft: 0, streamErr: fmt.Errorf("corrupt page in %s", p)}, nil
		}
	}
	// Healthy path: return iterator with rowsLeft=3, store for later assertion
	d.healthyIter = &verifyFakeRows{rowsLeft: 3}
	return d.healthyIter, nil
}

func TestVerifyParquetPathsFlagsOpenAndStreamFailures(t *testing.T) {
	duck := &verifyFakeDuck{
		failOpen:   map[string]bool{"s3://b/trunc.parquet": true},
		failStream: map[string]bool{"s3://b/mid.parquet": true},
	}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/good.parquet", "s3://b/trunc.parquet", "s3://b/mid.parquet"})

	// Finding I1: Assert SQL shape — all queries must be full-drain (not metadata-only)
	for _, query := range duck.queries {
		if !strings.HasPrefix(query, "SELECT * FROM read_parquet(") {
			t.Fatalf("query must be full-drain SELECT *, got: %s", query)
		}
	}

	// Finding I1: Assert drain actually iterates rows to exhaustion
	if duck.healthyIter == nil {
		t.Fatal("healthy path iterator should have been created")
	}
	if duck.healthyIter.nextCalls != 4 {
		t.Fatalf("healthy path drain must call Next() 4 times (3 rows + final false), got %d", duck.healthyIter.nextCalls)
	}

	// Finding I2: Assert exact-set attribution (not just length)
	if len(corrupt) != 2 {
		t.Fatalf("corrupt set size = %d, want 2", len(corrupt))
	}
	corruptSet := make(map[string]bool)
	for _, p := range corrupt {
		corruptSet[p] = true
	}
	if !corruptSet["s3://b/trunc.parquet"] {
		t.Fatalf("corrupt set must contain trunc.parquet, got %v", corrupt)
	}
	if !corruptSet["s3://b/mid.parquet"] {
		t.Fatalf("corrupt set must contain mid.parquet, got %v", corrupt)
	}
	if corruptSet["s3://b/good.parquet"] {
		t.Fatalf("corrupt set must NOT contain good.parquet, got %v", corrupt)
	}
}

func TestVerifyParquetPathsSkipsUnverifiableEntries(t *testing.T) {
	duck := &verifyFakeDuck{}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/*.parquet", "s3://b/it's.parquet"})
	if corrupt != nil {
		t.Fatalf("glob/quote entries must be skipped, got %v", corrupt)
	}
	if len(duck.queries) != 0 {
		t.Fatalf("unverifiable entries must not be probed: %v", duck.queries)
	}
}

func TestVerifyParquetPathsCancelledContextVerifiesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/x.parquet": true}}
	if got := verifyParquetPaths(ctx, duck, []string{"s3://b/x.parquet"}); got != nil {
		t.Fatalf("cancelled verification must confirm nothing, got %v", got)
	}
}

func TestCorruptParquetRetryErrorChain(t *testing.T) {
	cause := fmt.Errorf("scan: %w: boom", ErrFederatedReadFailed)
	err := &corruptParquetRetryError{Corrupt: []string{"s3://b/bad.parquet"}, cause: cause}
	if !errors.Is(err, ErrFederatedReadFailed) {
		t.Fatal("retry error must keep the ErrFederatedReadFailed classification")
	}
	if !strings.Contains(err.Error(), "s3://b/bad.parquet") {
		t.Fatalf("retry error must name the corrupt objects: %v", err)
	}
}
