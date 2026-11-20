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

// publishedScan is a failed scan over paths, the objects a generation of
// schema 7 lists.
func publishedScan(paths ...string) scan {
	gen := &PublishedGeneration{SchemaID: 7, Epoch: fakeSourceEpoch, Generation: fakeSourceGeneration, Paths: paths, Keys: paths}
	return scan{parquetPaths: paths, published: gen}
}

// execFailQuery is the minimal query the failure path reads: only the schema
// ID reaches classification.
func execFailQuery(schemaID int16) *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: schemaID}}
}

func TestFailDuckDBScanConfirmedCorruptionSkipsBreakerAndFails(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/bad.parquet": true}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute) // threshold 1: any RecordFailure opens it
	e := NewDBFederatedQueryEngine(nil, duck, breaker, forma.DuckDBConfig{}, nil, withParquetSource(&fakeParquetSource{}))

	sc := publishedScan("s3://b/good.parquet", "s3://b/bad.parquet")
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: page corrupt", ErrFederatedReadFailed), "execute duckdb query")

	var corrupt *CorruptParquetError
	if !errors.As(err, &corrupt) {
		t.Fatalf("confirmed corruption must return CorruptParquetError, got: %v", err)
	}
	if len(corrupt.Paths) != 1 || corrupt.Paths[0] != "s3://b/bad.parquet" || corrupt.SchemaID != 7 {
		t.Fatalf("corrupt = %+v", corrupt)
	}
	if _, retry := retryablePassFailure(err); retry {
		t.Fatal("confirmed corruption must not earn the remainder retry (#722)")
	}
	if breaker.IsOpen() {
		t.Fatal("confirmed per-file corruption must not feed the breaker (#251)")
	}
	if !degradableFederatedError(err) || !errors.Is(err, ErrFederatedReadFailed) {
		t.Fatal("the classification chain must survive, so degraded mode can absorb the failure")
	}
	if cached := e.corruptPaths.Cached(sc.parquetPaths); len(cached) != 1 {
		t.Fatalf("the corrupt object must be cached so later reads refuse without a scan: %v", cached)
	}
}

// TestFailDuckDBScanMissingObjectIsAPublicationInconsistency pins the
// ordering contract: the missing-object probe runs BEFORE the corruption
// probe. The fake makes the scenario physical: an object deleted from storage
// cannot be opened, so verification WOULD call it corrupt if it ran first.
// Reorder the two and a listed-but-deleted object gets cached as corrupt, and
// so degradable, instead of failing with the non-degradable
// ErrPublicationInconsistent #759 exists to raise. A lost object says nothing
// about DuckDB, so the breaker does not hear of it.
func TestFailDuckDBScanMissingObjectIsAPublicationInconsistency(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/7/x.parquet": true}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(nil, duck, breaker, forma.DuckDBConfig{}, nil, withParquetSource(&fakeParquetSource{missing: []string{"7/x.parquet"}}))

	sc := publishedScan("s3://b/7/x.parquet", "s3://b/7/y.parquet")
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: no such file", ErrFederatedReadFailed), "execute duckdb query")

	if !errors.Is(err, ErrPublicationInconsistent) || errors.Is(err, ErrFederatedReadFailed) {
		t.Fatalf("a missing listed object must classify as a publication inconsistency, never a failed read, got: %v", err)
	}
	var corrupt *CorruptParquetError
	if errors.As(err, &corrupt) {
		t.Fatal("a lost object must never be classified as corruption")
	}
	if len(duck.queries) != 0 {
		t.Fatalf("a confirmed loss must not trigger verification probes: %v", duck.queries)
	}
	if breaker.IsOpen() {
		t.Fatal("a lost object is not charged to the breaker")
	}
}

// TestFailDuckDBScanTransientDrainErrorStaysOnFailurePath is the #349 review
// R2-1 regression: a drain that fails once and reads clean on the re-drain is
// a transient object fault, not corruption — it must take the ordinary
// RecordFailure path (threshold-1 breaker opens), cache nothing, and return a
// plain read failure, exactly as before #251.
func TestFailDuckDBScanTransientDrainErrorStaysOnFailurePath(t *testing.T) {
	duck := &verifyFakeDuck{failOpenOnce: map[string]int{"s3://b/flaky.parquet": 1}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(nil, duck, breaker, forma.DuckDBConfig{}, nil, withParquetSource(&fakeParquetSource{}))

	sc := publishedScan("s3://b/good.parquet", "s3://b/flaky.parquet")
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: read timeout", ErrFederatedReadFailed), "execute duckdb query")

	var corrupt *CorruptParquetError
	if errors.As(err, &corrupt) {
		t.Fatal("a transient single-drain failure must not be confirmed as corruption")
	}
	if !breaker.IsOpen() {
		t.Fatal("an unconfirmed failure must keep ordinary breaker accounting")
	}
	if cached := e.corruptPaths.Cached(sc.parquetPaths); len(cached) != 0 {
		t.Fatalf("nothing may be cached for a transient failure: %v", cached)
	}
}

func TestFailDuckDBScanAllPathsUnreadableIsEngineSickness(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{
		"s3://b/a.parquet": true, "s3://b/b.parquet": true,
	}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(nil, duck, breaker, forma.DuckDBConfig{}, nil, withParquetSource(&fakeParquetSource{}))

	sc := publishedScan("s3://b/a.parquet", "s3://b/b.parquet")
	err := e.failDuckDBScan(context.Background(), execFailQuery(7), sc,
		fmt.Errorf("scan: %w: io error", ErrFederatedReadFailed), "execute duckdb query")

	var corrupt *CorruptParquetError
	if errors.As(err, &corrupt) {
		t.Fatal("every-object-unreadable is store/engine sickness, not per-file corruption")
	}
	// The #351 firewall: a sick store fails the guarded drain of every object,
	// so identification must decline it too. It can only do so because it
	// confirms the BARE drain succeeds before naming a path — delete that leg
	// and every object in a sick store is misattributed as schema-wrong.
	var viol *ParquetGuardViolationError
	if errors.As(err, &viol) {
		t.Fatalf("store/engine sickness must not be attributed to objects, got: %v", viol.Paths)
	}
	if !breaker.IsOpen() {
		t.Fatal("store-wide failure must feed the breaker")
	}
}
