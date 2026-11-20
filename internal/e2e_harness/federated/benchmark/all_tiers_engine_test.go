package benchmark

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

// recordingEngine records the federated query it was handed.
type recordingEngine struct {
	got *model.FederatedAttributeQuery
}

func (e *recordingEngine) Query(_ context.Context, _ model.StorageTables, fq *model.FederatedAttributeQuery, _ *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
	e.got = fq
	return &model.PersistentRecordPage{}, nil
}

// TestAllTiersEnginePinsTiers: the service path's engine declares all three
// tiers on every call, without touching the service's query. What the
// parquet tiers hold is the published generation, never a path the
// benchmark hands the engine.
func TestAllTiersEnginePinsTiers(t *testing.T) {
	inner := &recordingEngine{}
	engine := allTiersEngine{engine: inner}
	fq := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 3}}
	if _, err := engine.Query(context.Background(), model.StorageTables{}, fq, nil); err != nil {
		t.Fatalf("query: %v", err)
	}
	if inner.got == fq || fq.PreferredTiers != nil {
		t.Fatalf("expected the service's query left untouched, got %+v", fq)
	}
	if len(inner.got.PreferredTiers) != 3 || inner.got.SchemaID != 3 {
		t.Fatalf("expected all three tiers on schema 3, got %+v", inner.got)
	}
}
