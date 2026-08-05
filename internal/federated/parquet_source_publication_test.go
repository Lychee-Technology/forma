package federated

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// The broken-hint 400 must publish the caller's own template and the reason,
// while text/template's parse prose stays operator-only (#313). This is the
// issue's motivating case: before typed publications this mixed chain
// collapsed to an opaque redacted body.
func TestParquetSource_InvalidHintPublishesTemplateWithoutRenderInternals(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/1/from-manifest.parquet"}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	q := coldTierQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://b/{{.Broken"}
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		q, &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.Error(t, err)

	var pub forma.PublicError
	require.True(t, errors.As(err, &pub), "broken hint publishes no client message: %v", err)
	require.Contains(t, pub.PublicMessage(), `"s3://b/{{.Broken"`)
	require.Contains(t, pub.PublicMessage(), "not renderable")
	require.NotContains(t, pub.PublicMessage(), "unclosed action",
		"text/template internals must stay operator-only")
	require.Contains(t, err.Error(), "unclosed action",
		"the operator copy must keep the render error")
	require.True(t, forma.HasOperatorDetail(err))
}
