package internal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// newPatchedShippedSchemaHarness builds the same manager newShippedSchemaHarness
// does, over a copy of the shipped schema directory in which visit's attribute
// ledger has been rewritten by patch. The shipped ledgers carry no
// required_always beneath contactSnapshot — the #389 hazard is latent there —
// so the shape under test has to be declared on a copy.
func newPatchedShippedSchemaHarness(t *testing.T, patch func(ledger map[string]map[string]any)) shippedHarness {
	t.Helper()

	dir := t.TempDir()
	entries, err := os.ReadDir(shippedSchemaDir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		body, err := os.ReadFile(filepath.Join(shippedSchemaDir, entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, entry.Name()), body, 0o600))
	}

	ledgerPath := filepath.Join(dir, "visit_attributes.json")
	raw, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)
	var ledger map[string]map[string]any
	require.NoError(t, json.Unmarshal(raw, &ledger))
	patch(ledger)
	patched, err := json.Marshal(ledger)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(ledgerPath, patched, 0o600))

	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)

	validator, err := schemavalidate.New(registry, dir)
	require.NoError(t, err)

	visitSchemaID, _, err := registry.GetSchemaByName("visit")
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.SchemaDirectory = dir

	inner := transform.NewPersistentRecordTransformer(registry)
	spy := &writeSpy{inner: inner}
	repo := newMockPersistentRecordRepository()

	return shippedHarness{
		manager:       mustNewEntityManager(t, spy, repo, nil, registry, config, validator),
		spy:           spy,
		repo:          repo,
		transformer:   inner,
		visitSchemaID: visitSchemaID,
	}
}

// TestCreateSucceedsWithRequiredAlwaysBeneathRelationRoot is the #389
// acceptance test: a ledger declaring required_always on an attribute beneath
// a relation root must not turn every create into a 400.
//
// StripComputedFields removes the whole contactSnapshot subtree before
// validation, so the value the policy demands can never be present — sending it
// only gives the strip more to remove. The write path's input-side required
// check (transform.validateRequiredAttributesFromInput) therefore has to carve
// relation roots out the way the record-side check has since #315. The startup
// guard cannot catch this shape: it reads the JSON Schema document, and the
// policy lives in the attribute ledger.
func TestCreateSucceedsWithRequiredAlwaysBeneathRelationRoot(t *testing.T) {
	h := newPatchedShippedSchemaHarness(t, func(ledger map[string]map[string]any) {
		ledger["contactSnapshot.email"]["required_policy"] = string(forma.RequiredPolicyAlways)
	})

	_, err := h.manager.Create(context.Background(), createVisitOp(validVisit()))
	require.NoError(t, err,
		"a required_always beneath a relation root must not reject the stripped payload (#389)")

	require.Len(t, h.spy.seen, 1)
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot.email",
		"the relation subtree still never reaches storage")
}

// TestUpdateSucceedsWithRequiredAlwaysBeneathRelationRoot is the update half:
// Update merges the stored document with the caller's changes and runs the
// merged document through the same ToAttributes, so the carve-out has to hold
// there too — the seeded row never held contactSnapshot.email either, since
// the strip removed it on create.
func TestUpdateSucceedsWithRequiredAlwaysBeneathRelationRoot(t *testing.T) {
	h := newPatchedShippedSchemaHarness(t, func(ledger map[string]map[string]any) {
		ledger["contactSnapshot.email"]["required_policy"] = string(forma.RequiredPolicyAlways)
	})

	seed := validVisit()
	rowID := uuid.MustParse(seed["id"].(string))
	h.repo.storeRecord(buildPersistentRecord(t, h.transformer, h.visitSchemaID, rowID, seed))

	_, err := h.manager.Update(context.Background(), &forma.EntityOperation{
		Type:             forma.OperationUpdate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
		Updates:          map[string]any{"feedback": "ok", "contactSnapshot.email": "ada@example.com"},
	})
	require.NoError(t, err,
		"a required_always beneath a relation root must not reject the merged update document (#389)")

	require.Len(t, h.spy.seen, 1)
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot.email")
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot")
	require.Contains(t, h.spy.seen[0].keys, "feedback", "the rest of the update still lands")
}

// TestCreateStillRejectsMissingRequiredAlwaysOutsideRelationRoot is the control
// for the acceptance test above, on the same patched-ledger harness: the
// carve-out does not reach propertySnapshot, an ordinary nested object, whose
// required_always keeps answering the caller-facing 400.
func TestCreateStillRejectsMissingRequiredAlwaysOutsideRelationRoot(t *testing.T) {
	h := newPatchedShippedSchemaHarness(t, func(ledger map[string]map[string]any) {
		ledger["propertySnapshot.code"]["required_policy"] = string(forma.RequiredPolicyAlways)
	})

	_, err := h.manager.Create(context.Background(), createVisitOp(validVisit()))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.ErrorContains(t, err, "missing required attribute 'propertySnapshot.code'")
}
