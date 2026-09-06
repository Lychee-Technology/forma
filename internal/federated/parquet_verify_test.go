package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/sqlutil"
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
// failOpenOnce paths fail the next n opens and then read clean — the
// transient-blip class (#349 review R2-1).
type verifyFakeDuck struct {
	failOpen     map[string]bool
	failOpenOnce map[string]int
	failStream   map[string]bool
	queries      []string
	healthyIter  *verifyFakeRows // track the healthy path's iterator for assertion
}

func (d *verifyFakeDuck) Query(ctx context.Context, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	d.queries = append(d.queries, sqlStr)
	for p, n := range d.failOpenOnce {
		if n > 0 && strings.Contains(sqlStr, p) {
			d.failOpenOnce[p] = n - 1
			return nil, fmt.Errorf("IO Error: transient timeout on %s", p)
		}
	}
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

// TestVerifyParquetPathsTransientSingleFailureNotConfirmed pins the #349
// review R2-1 contract: one failed drain is inconclusive. A path whose first
// drain fails but whose immediate re-drain reads clean must NOT be confirmed
// corrupt — caching it would convert a transient object-level blip into a
// retention window of unmarked short answers while bypassing the breaker.
func TestVerifyParquetPathsTransientSingleFailureNotConfirmed(t *testing.T) {
	duck := &verifyFakeDuck{failOpenOnce: map[string]int{"s3://b/flaky.parquet": 1}}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/good.parquet", "s3://b/flaky.parquet"})
	if corrupt != nil {
		t.Fatalf("single-failure path must stay unconfirmed, got %v", corrupt)
	}
	// One drain for the healthy path, two for the flaky one (fail + clean).
	if len(duck.queries) != 3 {
		t.Fatalf("expected 3 drains (1 good + 2 flaky), got %d: %v", len(duck.queries), duck.queries)
	}
}

// TestVerifyParquetPathsPersistentFailureConfirmedOnSecondDrain pins the
// other half: deterministic corruption fails both consecutive drains and IS
// confirmed.
func TestVerifyParquetPathsPersistentFailureConfirmedOnSecondDrain(t *testing.T) {
	duck := &verifyFakeDuck{failOpenOnce: map[string]int{"s3://b/dead.parquet": 2}}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/good.parquet", "s3://b/dead.parquet"})
	if len(corrupt) != 1 || corrupt[0] != "s3://b/dead.parquet" {
		t.Fatalf("two consecutive failures must confirm, got %v", corrupt)
	}
}

func TestVerifyParquetPathsSkipsGlobEntries(t *testing.T) {
	duck := &verifyFakeDuck{}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/*.parquet", "s3://b/part-?.parquet", "s3://b/part-[0-9].parquet"})
	if corrupt != nil {
		t.Fatalf("glob entries must be skipped, got %v", corrupt)
	}
	if len(duck.queries) != 0 {
		t.Fatalf("glob entries must not be probed: %v", duck.queries)
	}
}

// TestVerifyParquetPathsVerifiesQuoteBearingPath pins #479: since #456 the
// drain escapes the path, so an object key that legitimately carries a quote
// (or a semicolon, or a double quote) is verifiable on its own and must be
// individually excludable rather than left to all-or-nothing behavior.
func TestVerifyParquetPathsVerifiesQuoteBearingPath(t *testing.T) {
	const quoted = "s3://b/it's;\"odd\".parquet"
	escaped := sqlutil.EscapeLiteral(quoted)
	duck := &verifyFakeDuck{failOpen: map[string]bool{escaped: true}}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/good.parquet", quoted})
	if len(corrupt) != 1 || corrupt[0] != quoted {
		t.Fatalf("quote-bearing path must be individually confirmed, got %v", corrupt)
	}
	// One drain for the healthy path, two for the corrupt one; every drain of
	// the quote-bearing path must render the quote doubled, never raw.
	if len(duck.queries) != 3 {
		t.Fatalf("expected 3 drains, got %d: %v", len(duck.queries), duck.queries)
	}
	for _, q := range duck.queries[1:] {
		if !strings.Contains(q, "read_parquet('"+escaped+"')") {
			t.Fatalf("quote-bearing path must render escaped, got: %s", q)
		}
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
