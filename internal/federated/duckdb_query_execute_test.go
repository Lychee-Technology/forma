package federated

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// execFakeSource stands in for the manifest-backed ParquetSource on the
// failure path only: failDuckDBScan never resolves paths (the scan already
// did), it only asks whether any scanned object is missing from storage.
type execFakeSource struct{ missing []string }

func (f *execFakeSource) Paths(ctx context.Context, schemaID int16) ([]string, map[string]map[string]string, error) {
	return nil, nil, nil // unused in these tests
}

func (f *execFakeSource) MissingIn(ctx context.Context, scanned []string) ([]string, error) {
	return f.missing, nil
}

// execFailQuery is the minimal query the failure path reads: only the schema
// ID reaches classification.
func execFailQuery(schemaID int16) *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: schemaID}}
}

func TestFailDuckDBScanConfirmedCorruptionSkipsBreakerAndRetries(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/bad.parquet": true}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute) // threshold 1: any RecordFailure opens it
	e := NewDBFederatedQueryEngine(nil, nil, duck, breaker, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{}))

	sc := scan{parquetPaths: []string{"s3://b/good.parquet", "s3://b/bad.parquet"}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: page corrupt", ErrFederatedReadFailed), "execute duckdb query")

	var retry *corruptParquetRetryError
	if !errors.As(err, &retry) {
		t.Fatalf("confirmed corruption must return the retryable error, got: %v", err)
	}
	if len(retry.Corrupt) != 1 || retry.Corrupt[0] != "s3://b/bad.parquet" {
		t.Fatalf("retry.Corrupt = %v", retry.Corrupt)
	}
	if breaker.IsOpen() {
		t.Fatal("confirmed per-file corruption must not feed the breaker (#251)")
	}
	if !errors.Is(err, ErrFederatedReadFailed) {
		t.Fatal("classification chain must survive for non-retrying callers")
	}
	kept, excluded := e.corruptPaths.Split(sc.parquetPaths)
	if len(excluded) != 1 || len(kept) != 1 {
		t.Fatalf("corrupt object must be cached for exclusion: kept=%v excluded=%v", kept, excluded)
	}
}

// TestFailDuckDBScanMissingObjectStaysInconsistentAndBreakerWorthy pins the
// ordering contract the brief marks "do not reorder": MissingIn classification
// must run BEFORE the corruption probe. The fake makes the scenario physical —
// an object deleted from storage cannot be opened, so verification WOULD call
// it corrupt if it ran first. That is what makes this test bite: reorder the
// two blocks in failDuckDBScan and a manifest-listed-but-deleted object gets
// cached, excluded and answered with a silently short page instead of the
// non-degradable ErrParquetSetInconsistent #187 scenario 2 exists to raise.
func TestFailDuckDBScanMissingObjectStaysInconsistentAndBreakerWorthy(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/7/x.parquet": true}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(nil, nil, duck, breaker, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{missing: []string{"7/x.parquet"}}))

	sc := scan{parquetPaths: []string{"s3://b/7/x.parquet", "s3://b/7/y.parquet"}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: no such file", ErrFederatedReadFailed), "execute duckdb query")

	if !errors.Is(err, ErrParquetSetInconsistent) {
		t.Fatalf("missing listed object must classify as inconsistency, got: %v", err)
	}
	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		t.Fatal("manifest inconsistency must never be retried as corruption")
	}
	if !breaker.IsOpen() {
		t.Fatal("inconsistency failure must still feed the breaker")
	}
}

// TestFailDuckDBScanTransientDrainErrorStaysOnFailurePath is the #349 review
// R2-1 regression: a drain that fails once and reads clean on the re-drain is
// a transient object fault, not corruption — it must take the ordinary
// RecordFailure path (threshold-1 breaker opens), cache nothing, and return a
// non-retryable error, exactly as before #251.
func TestFailDuckDBScanTransientDrainErrorStaysOnFailurePath(t *testing.T) {
	duck := &verifyFakeDuck{failOpenOnce: map[string]int{"s3://b/flaky.parquet": 1}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(nil, nil, duck, breaker, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{}))

	sc := scan{parquetPaths: []string{"s3://b/good.parquet", "s3://b/flaky.parquet"}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: read timeout", ErrFederatedReadFailed), "execute duckdb query")

	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		t.Fatal("a transient single-drain failure must not be confirmed as corruption")
	}
	if !breaker.IsOpen() {
		t.Fatal("an unconfirmed failure must keep ordinary breaker accounting")
	}
	kept, excluded := e.corruptPaths.Split(sc.parquetPaths)
	if len(excluded) != 0 || len(kept) != 2 {
		t.Fatalf("nothing may be cached for a transient failure: kept=%v excluded=%v", kept, excluded)
	}
}

func TestFailDuckDBScanAllPathsUnreadableIsEngineSickness(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{
		"s3://b/a.parquet": true, "s3://b/b.parquet": true,
	}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(nil, nil, duck, breaker, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{}))

	sc := scan{parquetPaths: []string{"s3://b/a.parquet", "s3://b/b.parquet"}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: io error", ErrFederatedReadFailed), "execute duckdb query")

	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		t.Fatal("every-object-unreadable is store/engine sickness, not per-file corruption")
	}
	if !breaker.IsOpen() {
		t.Fatal("store-wide failure must feed the breaker")
	}
}

func TestFailDuckDBScanHintPathsNeverVerified(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/bad.parquet": true}}
	e := NewDBFederatedQueryEngine(nil, nil, duck, nil, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{}))

	sc := scan{parquetPaths: []string{"s3://b/good.parquet", "s3://b/bad.parquet"}, pathsFromSource: false}
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: page corrupt", ErrFederatedReadFailed), "execute duckdb query")

	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		t.Fatal("operator-pinned hint sets keep all-or-nothing semantics")
	}
	if len(duck.queries) != 0 {
		t.Fatalf("hint-set failure must not trigger verification probes: %v", duck.queries)
	}
}
