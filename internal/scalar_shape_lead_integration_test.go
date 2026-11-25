package internal

import (
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of the scalar-shape invariant on the shipped lead
// schema (#924): required and optional scalars, at the top level, in an
// object in nested and dotted spelling, and in arrays of objects, whose
// array ancestry the file registry derives. Valid scalar, object, list,
// empty-list and array-of-objects updates still write.

const leadSchemaDir = "../cmd/server/schemas"

type leadShapeCase struct {
	name    string
	updates map[string]any
	attr    string
	vt      forma.ValueType
}

func refusedLeadUpdates() []leadShapeCase {
	cases := []leadShapeCase{}
	for kind, value := range scalarContainers() {
		cases = append(cases,
			leadShapeCase{"required top-level " + kind, map[string]any{"stage": value()}, "stage", forma.ValueTypeText},
			leadShapeCase{"required nested " + kind, map[string]any{"contact": map[string]any{"isAnonymous": value()}}, "contact.isAnonymous", forma.ValueTypeBool},
			leadShapeCase{"required dotted " + kind, map[string]any{"contact.isAnonymous": value()}, "contact.isAnonymous", forma.ValueTypeBool},
			leadShapeCase{"optional nested " + kind, map[string]any{"contact": map[string]any{"company": value()}}, "contact.company", forma.ValueTypeText},
			leadShapeCase{"optional dotted " + kind, map[string]any{"contact.annualIncome": value()}, "contact.annualIncome", forma.ValueTypeNumeric},
			leadShapeCase{"optional top-level " + kind, map[string]any{"score": value()}, "score", forma.ValueTypeNumeric},
			leadShapeCase{"array-of-objects member " + kind, map[string]any{"propertyInterests": []any{
				map[string]any{"propertyId": value(), "status": "viewed"}}}, "propertyInterests.propertyId", forma.ValueTypeText},
			leadShapeCase{"object in an array of objects " + kind, map[string]any{"propertyInterests": []any{
				map[string]any{"propertyId": "p1", "status": "viewed", "snapshot": map[string]any{"code": value()}}}},
				"propertyInterests.snapshot.code", forma.ValueTypeText},
			leadShapeCase{"array of objects in an object " + kind, map[string]any{"requirement": map[string]any{
				"areas": []any{map[string]any{"city": value()}}}}, "requirement.areas.city", forma.ValueTypeText},
		)
	}
	return cases
}

func TestLeadUpdateRefusesContainersAtScalars(t *testing.T) {
	base := setupTxEnv(t)
	for _, mode := range updateValidationModes {
		env := base.withUpdateValidation(t, mode, leadSchemaDir, nil)
		t.Run(mode.name, func(t *testing.T) {
			rowID := env.mustCreate(t, "lead", txLead("Ada"))
			for _, tc := range refusedLeadUpdates() {
				t.Run(tc.name, func(t *testing.T) {
					before := env.storedState(t, "lead", rowID)
					_, err := env.manager.Update(env.ctx, txUpdateOp("lead", rowID, tc.updates))
					requireScalarShapeRefusal(t, err, tc.attr, tc.vt, tc.name)
					env.requireUnchangedState(t, "lead", rowID, before, tc.name)
				})
			}
		})
	}
}

// Valid updates write and read back, each in its own shape.
func TestLeadUpdateAdmitsValidShapes(t *testing.T) {
	base := setupTxEnv(t)
	cases := []struct {
		name    string
		updates map[string]any
		path    string
		want    any
	}{
		{"scalar", map[string]any{"stage": "contacted"}, "stage", "contacted"},
		{"object", map[string]any{"contact": map[string]any{"company": "Acme", "isAnonymous": true}}, "contact.company", "Acme"},
		{"dotted scalar", map[string]any{"contact.name": "Bob"}, "contact.name", "Bob"},
		{"list", map[string]any{"tags": []any{"a", "b"}}, "tags", []any{"a", "b"}},
		{"empty list", map[string]any{"tags": []any{}}, "tags", []any{}},
		{"list in an object", map[string]any{"contact": map[string]any{"phones": []any{"1"}}}, "contact.phones", []any{"1"}},
		{"array of objects", map[string]any{"propertyInterests": []any{map[string]any{
			"propertyId": "p1", "status": "viewed", "snapshot": map[string]any{"code": "c"}}}},
			"propertyInterests", []any{map[string]any{"propertyId": "p1", "status": "viewed", "snapshot": map[string]any{"code": "c"}}}},
		{"array of objects in an object", map[string]any{"requirement": map[string]any{"areas": []any{map[string]any{"city": "Tokyo"}}}},
			"requirement.areas", []any{map[string]any{"city": "Tokyo"}}},
	}
	for _, mode := range updateValidationModes {
		env := base.withUpdateValidation(t, mode, leadSchemaDir, nil)
		t.Run(mode.name, func(t *testing.T) {
			rowID := env.mustCreate(t, "lead", txLead("Ada"))
			for _, tc := range cases {
				record, err := env.manager.Update(env.ctx, txUpdateOp("lead", rowID, tc.updates))
				require.NoError(t, err, tc.name)
				assert.Equal(t, tc.want, valueAtPath(record.Attributes, tc.path), "%s: answered", tc.name)
				assert.Equal(t, tc.want, valueAtPath(env.attrs(t, "lead", rowID), tc.path), "%s: stored", tc.name)
			}
		})
	}
}

// A row that holds a record under a scalar at an array index, which
// report-only validation used to admit from ["a@x"], reads back as an array.
// An update that does not address it writes it back as stored, as before;
// one that sends an array for it is refused; one that sends a scalar
// replaces it.
func TestUpdateKeepsAStoredContainerAtAScalar(t *testing.T) {
	base := setupUniqueKeyEnv(t)
	for _, mode := range updateValidationModes[1:] {
		env := base.withUpdateValidation(t, mode, ukSchemaDir, base.ukCatalog(t))
		t.Run(mode.name, func(t *testing.T) {
			rowID := env.mustCreateAccount(t, "t-legacy-"+mode.name, "", "")
			schemaID := ukSchemaIDs[ukAccount]
			_, err := env.postgresPool.Exec(env.ctx, fmt.Sprintf(
				"UPDATE %s SET array_indices = '0' WHERE schema_id = $1 AND row_id = $2 AND attr_id = 5",
				sanitizeIdentifier(env.tables.EAVData)), schemaID, rowID)
			require.NoError(t, err)
			require.Equal(t, []any{"n"}, env.attrs(t, ukAccount, rowID)["note"], "the legacy row reads note back as an array")

			_, err = env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, map[string]any{"counter": 5}))
			require.NoError(t, err, "an unrelated update writes the stored record back")
			assert.Equal(t, []any{"n"}, env.attrs(t, ukAccount, rowID)["note"])

			before := env.storedState(t, ukAccount, rowID)
			_, err = env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, map[string]any{"note": []any{"m"}}))
			requireScalarShapeRefusal(t, err, "note", forma.ValueTypeText, "an array for the legacy note")
			env.requireUnchangedState(t, ukAccount, rowID, before, "refused update of the legacy note")

			_, err = env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, map[string]any{"note": "fixed"}))
			require.NoError(t, err)
			assert.Equal(t, "fixed", env.attrs(t, ukAccount, rowID)["note"], "a scalar replaces the stored record")
		})
	}
}
