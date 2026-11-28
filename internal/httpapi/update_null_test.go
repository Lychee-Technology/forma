package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"
)

// The HTTP update carries no unset operator (#938 leaves HTTP carriers to
// #655), and an explicit null stays what it was before: a refused value, not
// a clear. These tests drive a real EntityManager behind the handler, so the
// status is the manager's own refusal.

const unsetSchemaDir = "../testdata/unset_ops"

// storedRowRepository answers every merge with one stored unsetdoc row and
// counts the merges that wrote; the manager never needs anything else from
// it here.
type storedRowRepository struct {
	model.PersistentRecordRepository
	existing *model.PersistentRecord
	writes   int
}

func (r *storedRowRepository) MergePersistentRecord(ctx context.Context, _ model.StorageTables, _ int16, _ uuid.UUID, _ model.RowGuard, merge model.PersistentRecordMerge) (*model.PersistentRecord, error) {
	record, err := merge(ctx, r.existing, func(context.Context) (int64, error) { return 0, nil })
	if err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	r.writes++
	return record, nil
}

func TestHTTPUpdateSendingNullIsRefused(t *testing.T) {
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(unsetSchemaDir)
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	schemaID, _, err := registry.GetSchemaByName("unsetdoc")
	if err != nil {
		t.Fatalf("find unsetdoc: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	rowID := uuid.New()
	existing, err := transformer.ToPersistentRecord(context.Background(), schemaID, rowID, map[string]any{
		"name": "n", "memo": "m", "note": "N", "label": "L", "tags": []any{"a"}, "profile": map[string]any{"nick": "k"},
	})
	if err != nil {
		t.Fatalf("build the stored row: %v", err)
	}
	repo := &storedRowRepository{existing: existing}
	config := &forma.Config{
		Query:  forma.QueryConfig{DefaultPageSize: 50, MaxPageSize: 100},
		Entity: forma.EntityConfig{SchemaDirectory: unsetSchemaDir},
	}
	manager, err := internal.NewEntityManager(transformer, repo, nil, registry, config, nil)
	if err != nil {
		t.Fatalf("build the manager: %v", err)
	}
	server := &Server{manager: manager}

	for name, body := range map[string]string{
		"top-level":    `{"note": null}`,
		"bound":        `{"label": null}`,
		"nested":       `{"profile": {"nick": null}}`,
		"list element": `{"tags": ["a", null]}`,
	} {
		t.Run(name, func(t *testing.T) {
			url := fmt.Sprintf("/api/v1/unsetdoc/%s", rowID)
			req := httptest.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(body)))
			rec := httptest.NewRecorder()
			server.handleUpdate(rec, req)
			if rec.Code < 400 || rec.Code >= 500 {
				t.Fatalf("PUT %s answered %d, want a 4xx: %s", body, rec.Code, rec.Body.String())
			}
			if !bytes.Contains(rec.Body.Bytes(), []byte("cannot be set to null")) {
				t.Fatalf("PUT %s answered %s, want the null refusal", body, rec.Body.String())
			}
			if repo.writes != 0 {
				t.Fatalf("PUT %s wrote the row", body)
			}
		})
	}
}
