package federated

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// fakeParquetSource is a PublicationReader for the engine tests that are not
// about the record: every schema's record is PUBLISHED at epoch 1, generation
// 3, and its generation lists paths, with stamps as the manifest's footer
// columns. pathsErr fails the manifest fetch as a store would (a failed read,
// not a consistency fault). MissingIn answers missing beside missingErr.
type fakeParquetSource struct {
	paths      []string
	stamps     map[string]map[string]string
	pathsErr   error
	missing    []string
	missingErr error
	// pathsCalls counts the manifest fetches, missingCalls the
	// missing-object probes, and lastScanned is the set the last probe was
	// asked about.
	pathsCalls   int
	missingCalls int
	lastScanned  []string
}

// fakeSourceEpoch and fakeSourceGeneration are the record fakeParquetSource
// answers.
const (
	fakeSourceEpoch      = 1
	fakeSourceGeneration = 3
)

func (f *fakeParquetSource) ReadRecord(_ context.Context, _ pgx.Tx, schemaID int16) (PublicationRecord, error) {
	return PublicationRecord{SchemaID: schemaID, Epoch: fakeSourceEpoch, Generation: fakeSourceGeneration, Published: true}, nil
}

func (f *fakeParquetSource) ResolveGeneration(_ context.Context, rec PublicationRecord) (PublishedGeneration, error) {
	f.pathsCalls++
	if f.pathsErr != nil {
		return PublishedGeneration{}, f.pathsErr
	}
	gen := PublishedGeneration{SchemaID: rec.SchemaID, Epoch: rec.Epoch, Generation: rec.Generation, Paths: f.paths, Columns: f.stamps}
	for _, p := range f.paths {
		gen.Keys = append(gen.Keys, filepath.Base(p))
	}
	return gen, nil
}

func (f *fakeParquetSource) MissingIn(_ context.Context, _ PublishedGeneration, scanned []string) ([]string, error) {
	f.missingCalls++
	f.lastScanned = scanned
	return f.missing, f.missingErr
}

// withParquetSource makes src the engine's publication reader, under the
// default pass deadline.
func withParquetSource(src *fakeParquetSource) EngineOption {
	return WithPublicationReader(src, 0)
}

func hybridDuckConfig() forma.DuckDBConfig {
	// Enables the hybrid engine with the "b" test bucket.
	return forma.DuckDBConfig{
		Enabled:  true,
		S3Bucket: "b",
		Routing:  forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid},
	}
}

// newParquetSourceTestEngine is a hybrid engine over duck whose publication
// reader is src.
func newParquetSourceTestEngine(t *testing.T, duck DuckDBQueryExecutor, src *fakeParquetSource) *DBFederatedQueryEngine {
	t.Helper()
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), withParquetSource(src), withFakeHotLeg(nil))
}

// newCachedRefusalTestEngine is an engine whose publication reader is src,
// over a DuckDB that answers the pre-read probes, with schema 7's metadata
// registered. The cached corrupt refusal runs the SQL build's refusals first
// (#722), so without metadata every refusal would be
// ErrSchemaMetadataCacheRequired.
func newCachedRefusalTestEngine(t *testing.T, src *fakeParquetSource) *DBFederatedQueryEngine {
	t.Helper()
	return NewDBFederatedQueryEngine(nil, &retryFakeDuck{passes: []retryPass{{}}}, nil,
		forma.DuckDBConfig{S3Bucket: "b"}, testMetadataCacheSchema7(t), withParquetSource(src), withFakeHotLeg(nil))
}

func exclusionTestQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7}}
}

var emptyScanTables = model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}

func allTierQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}
}

// countPlanSources counts the plan's sources read by engine.
func countPlanSources(plan *model.ExecutionPlan, engine string) int {
	n := 0
	for _, src := range plan.Sources {
		if src.Engine == engine {
			n++
		}
	}
	return n
}
