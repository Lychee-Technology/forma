package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type verifyFakeRows struct{ streamErr error }

func (r *verifyFakeRows) Next() bool             { return false }
func (r *verifyFakeRows) Scan(dest ...any) error { return nil }
func (r *verifyFakeRows) Err() error             { return r.streamErr }
func (r *verifyFakeRows) Close() error           { return nil }

// verifyFakeDuck fails Query for any SQL mentioning a failPath; midStream
// paths open fine and fail while iterating (the corruptMidFile class).
type verifyFakeDuck struct {
	failOpen   map[string]bool
	failStream map[string]bool
	queries    []string
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
			return &verifyFakeRows{streamErr: fmt.Errorf("corrupt page in %s", p)}, nil
		}
	}
	return &verifyFakeRows{}, nil
}

func TestVerifyParquetPathsFlagsOpenAndStreamFailures(t *testing.T) {
	duck := &verifyFakeDuck{
		failOpen:   map[string]bool{"s3://b/trunc.parquet": true},
		failStream: map[string]bool{"s3://b/mid.parquet": true},
	}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/good.parquet", "s3://b/trunc.parquet", "s3://b/mid.parquet"})
	if len(corrupt) != 2 {
		t.Fatalf("corrupt = %v, want trunc+mid", corrupt)
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
