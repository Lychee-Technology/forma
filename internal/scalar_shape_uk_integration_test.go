package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/httpapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of the scalar-shape invariant on ukaccount (#924),
// whose tenant is text bound to text_01 and a key component, email an EAV
// key component, note EAV text and seq and counter EAV integers, seq keyed.
// Before the rule, a report-only update giving one of them {} or [] kept a
// bound value, removed an EAV one and released its key entry; ["a@x"] stored
// element 0 under a scalar, or failed in the key planner with a redacted
// 500. Every update surface now refuses each container before it writes,
// under every validation mode.

// ukShapeRow is a row the refusals target, and a witness row each surface
// writes beside them.
type ukShapeRow struct {
	id, witness         uuid.UUID
	tenant, email, note string
}

// ukShapeSeq numbers the keyed seq of each row createUKShapeRow creates.
var ukShapeSeq atomic.Int64

func (e *txEnv) createUKShapeRow(t *testing.T, tag string) ukShapeRow {
	t.Helper()
	row := ukShapeRow{tenant: "t-" + tag, email: tag + "@x", note: "note-" + tag}
	data := ukAccountData(row.tenant, row.email, "h-"+tag)
	data["note"], data["seq"], data["counter"] = row.note, ukShapeSeq.Add(1), 1
	created, err := e.manager.Create(e.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	require.NoError(t, err)
	row.id = created.RowID
	witness, err := e.manager.Create(e.ctx, ukCreateOp(ukAccount, uuid.Nil, map[string]any{"note": "witness-" + tag}))
	require.NoError(t, err)
	row.witness = witness.RowID
	return row
}

// witnessWrite is a valid update of the witness row.
func (row ukShapeRow) witnessWrite() *forma.EntityOperation {
	return txUpdateOp(ukAccount, row.witness, map[string]any{"note": "witness-" + uuid.NewString()})
}

// shapeAttr is an attribute a refusal targets.
type shapeAttr struct {
	name  string
	vt    forma.ValueType
	keyed bool
}

var ukShapeAttrs = []shapeAttr{
	{"tenant", forma.ValueTypeText, true},
	{"email", forma.ValueTypeText, true},
	{"note", forma.ValueTypeText, false},
	{"seq", forma.ValueTypeInteger, true},
	{"counter", forma.ValueTypeInteger, false},
}

// shapeSurface sends updates to row through one update surface and asserts
// the scalar-shape refusal of attr, with the surface's own semantics: a Tx
// refused before writing stays usable and commits a later write; an atomic
// batch writes nothing, a best-effort batch fails only the invalid
// operation.
type shapeSurface struct {
	name string
	// keyed reports whether the surface may write a unique-key attribute at
	// all; Claim refuses it on its own (validateClaimMutation).
	keyed  bool
	refuse func(t *testing.T, env *txEnv, row ukShapeRow, updates map[string]any, attr shapeAttr)
}

// inTxRefused runs refused in a Tx, asserts its refusal, then writes the
// witness in the same Tx and requires the Tx to commit.
func inTxRefused(t *testing.T, env *txEnv, row ukShapeRow, attr shapeAttr, refused func(ctx context.Context, tx forma.Tx) error) {
	t.Helper()
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		requireScalarShapeRefusal(t, refused(ctx, tx), attr.name, attr.vt, "in the Tx")
		_, err := tx.Update(ctx, row.witnessWrite())
		return err
	}), "the refusal leaves the Tx usable, and it commits")
}

var ukShapeSurfaces = []shapeSurface{
	{"Update", true, func(t *testing.T, env *txEnv, row ukShapeRow, updates map[string]any, attr shapeAttr) {
		_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, row.id, updates))
		requireScalarShapeRefusal(t, err, attr.name, attr.vt, "Update")
	}},
	{"atomic BatchUpdate", true, func(t *testing.T, env *txEnv, row ukShapeRow, updates map[string]any, attr shapeAttr) {
		witness := env.storedState(t, ukAccount, row.witness)
		_, err := env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Atomic: true,
			Operations: []forma.EntityOperation{*row.witnessWrite(), *txUpdateOp(ukAccount, row.id, updates)}})
		requireScalarShapeRefusal(t, err, attr.name, attr.vt, "atomic BatchUpdate")
		assert.Contains(t, err.Error(), "operation[1]")
		env.requireUnchangedState(t, ukAccount, row.witness, witness, "the atomic batch's valid operation")
	}},
	{"best-effort BatchUpdate", true, func(t *testing.T, env *txEnv, row ukShapeRow, updates map[string]any, attr shapeAttr) {
		result, err := env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{
			Operations: []forma.EntityOperation{*row.witnessWrite(), *txUpdateOp(ukAccount, row.id, updates)}})
		require.NoError(t, err)
		require.Len(t, result.Successful, 1, "the valid operation is written")
		require.Len(t, result.Failed, 1)
		assert.Equal(t, row.id, result.Failed[0].Operation.RowID)
		assert.Equal(t, "UPDATE_FAILED", result.Failed[0].Code)
		assert.Contains(t, result.Failed[0].Error, "invalid value for attribute '"+attr.name+"'")
		assert.Contains(t, result.Failed[0].Error, "expected a scalar of type "+string(attr.vt))
	}},
	{"Tx.Update", true, func(t *testing.T, env *txEnv, row ukShapeRow, updates map[string]any, attr shapeAttr) {
		inTxRefused(t, env, row, attr, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Update(ctx, txUpdateOp(ukAccount, row.id, updates))
			return err
		})
	}},
	{"Tx.UpdateWhere", true, func(t *testing.T, env *txEnv, row ukShapeRow, updates map[string]any, attr shapeAttr) {
		inTxRefused(t, env, row, attr, func(ctx context.Context, tx forma.Tx) error {
			req := ukHandleWhere(row.note)
			req.Updates = updates
			_, err := tx.UpdateWhere(ctx, req)
			return err
		})
	}},
	{"Tx.Upsert update", true, func(t *testing.T, env *txEnv, row ukShapeRow, updates map[string]any, attr shapeAttr) {
		inTxRefused(t, env, row, attr, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{
				Create: *ukCreateOp(ukAccount, uuid.Nil, ukAccountData(row.tenant, row.email, "")),
				Target: forma.ConflictTarget{Key: "tenant_email"}, Action: forma.UpsertUpdate, Updates: updates})
			return err
		})
	}},
	{"Tx.Claim", false, func(t *testing.T, env *txEnv, row ukShapeRow, updates map[string]any, attr shapeAttr) {
		inTxRefused(t, env, row, attr, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: ukAccount, Limit: 1, Updates: updates,
				Condition: &forma.KvCondition{Attr: "note", Value: "equals:" + row.note}})
			return err
		})
	}},
}

// Every update surface refuses an object or an array at every scalar of
// ukaccount, bound or EAV, keyed or not, under every validation mode, and
// leaves the row, its change_log and the key table as they were.
func TestUpdateRefusesContainersAtScalarsOnEverySurface(t *testing.T) {
	base := setupUniqueKeyEnv(t)
	catalog := base.ukCatalog(t)
	for _, mode := range updateValidationModes {
		env := base.withUpdateValidation(t, mode, ukSchemaDir, catalog)
		t.Run(mode.name, func(t *testing.T) {
			for _, surface := range ukShapeSurfaces {
				row := env.createUKShapeRow(t, strings.NewReplacer(" ", "-", ".", "-").Replace(mode.name+"-"+surface.name))
				for _, attr := range ukShapeAttrs {
					if attr.keyed && !surface.keyed {
						continue
					}
					for kind, value := range scalarContainers() {
						t.Run(fmt.Sprintf("%s/%s/%s", surface.name, attr.name, kind), func(t *testing.T) {
							before := env.storedState(t, ukAccount, row.id)
							surface.refuse(t, env, row, map[string]any{attr.name: value()}, attr)
							env.requireUnchangedState(t, ukAccount, row.id, before, "refused update")
						})
					}
				}
			}
			env.assertKeyTableMatchesRows(t)
		})
	}
}

// The branches that write no row with the update's values succeed as before
// #772: an UpdateWhere or Claim taking no row, and an Upsert that creates.
func TestScalarShapeRefusalKeepsTheNoWriteBranches(t *testing.T) {
	base := setupUniqueKeyEnv(t)
	catalog := base.ukCatalog(t)
	for _, mode := range updateValidationModes {
		env := base.withUpdateValidation(t, mode, ukSchemaDir, catalog)
		t.Run(mode.name, func(t *testing.T) {
			bad := map[string]any{"note": map[string]any{}}
			tenant := "fresh-" + strings.ReplaceAll(mode.name, " ", "-")
			require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
				req := ukHandleWhere("matches-nothing")
				req.Updates = bad
				result, err := tx.UpdateWhere(ctx, req)
				require.NoError(t, err)
				assert.Empty(t, result.RowIDs)
				claimed, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: ukAccount, Limit: 1, Updates: bad,
					Condition: &forma.KvCondition{Attr: "note", Value: "equals:matches-nothing"}})
				require.NoError(t, err)
				assert.Empty(t, claimed.Records)
				upserted, err := tx.Upsert(ctx, &forma.UpsertRequest{
					Create: *ukCreateOp(ukAccount, uuid.Nil, ukAccountData(tenant, "fresh@x", "")),
					Target: forma.ConflictTarget{Key: "tenant_email"}, Action: forma.UpsertUpdate, Updates: bad})
				require.NoError(t, err)
				assert.Equal(t, forma.UpsertCreated, upserted.Outcome)
				return nil
			}))
			env.assertKeyTableMatchesRows(t)
		})
	}
}

// Valid scalar updates of every attribute still write, and a key component
// moved by one moves its entry (#682).
func TestScalarShapeRuleAdmitsScalarUpdates(t *testing.T) {
	base := setupUniqueKeyEnv(t)
	catalog := base.ukCatalog(t)
	for _, mode := range updateValidationModes {
		env := base.withUpdateValidation(t, mode, ukSchemaDir, catalog)
		t.Run(mode.name, func(t *testing.T) {
			row := env.createUKShapeRow(t, "valid-"+strings.ReplaceAll(mode.name, " ", "-"))
			updates := map[string]any{"tenant": row.tenant + "-moved", "email": "moved-" + row.email,
				"note": "moved", "seq": 900000 + len(mode.name), "counter": 2}
			record, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, row.id, updates))
			require.NoError(t, err)
			for attr, want := range updates {
				assert.EqualValues(t, want, record.Attributes[attr], attr)
			}
			env.assertKeyTableMatchesRows(t)
		})
	}
}

// The HTTP update route answers the refusal as a 400 with its published
// message, not a redacted 500.
func TestScalarShapeRefusalIs400OverHTTP(t *testing.T) {
	base := setupUniqueKeyEnv(t)
	catalog := base.ukCatalog(t)
	for _, mode := range updateValidationModes {
		env := base.withUpdateValidation(t, mode, ukSchemaDir, catalog)
		row := env.createUKShapeRow(t, "http-"+strings.ReplaceAll(mode.name, " ", "-"))
		server := httpapi.NewServer(env.manager, httpapi.Options{})
		for _, body := range []string{`{"tenant":{}}`, `{"email":["a@x"]}`, `{"note":[]}`} {
			t.Run(mode.name+"/"+body, func(t *testing.T) {
				before := env.storedState(t, ukAccount, row.id)
				req := httptest.NewRequest(http.MethodPut, "/api/v1/"+ukAccount+"/"+row.id.String(), strings.NewReader(body))
				rec := httptest.NewRecorder()
				server.Handler().ServeHTTP(rec, req)

				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				var resp httpapi.APIResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp.Error, "expected a scalar of type text")
				assert.Empty(t, resp.ErrorClass, "the refusal is not redacted")
				env.requireUnchangedState(t, ukAccount, row.id, before, "refused HTTP update")
			})
		}
	}
}
