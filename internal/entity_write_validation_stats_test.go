package internal

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/stretchr/testify/require"
)

// classifyProseRegistry extends the package's shared validation stub with a
// dependentRequired clause, so one schema exercises every prose shape the
// classifier keys on.
type classifyProseRegistry struct{ validationRegistry }

const classifyProseSchemaJSON = `{
  "type": "object",
  "properties": {
    "name":  {"type": "string", "enum": ["open", "won"]},
    "email": {"type": "string"},
    "phone": {"type": "string"}
  },
  "required": ["name"],
  "dependentRequired": {"email": ["phone"]}
}`

func (classifyProseRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 100, forma.JSONSchema{ID: 100, Name: "test", Schema: classifyProseSchemaJSON}, nil
}

func (classifyProseRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "test", forma.JSONSchema{ID: 100, Name: "test", Schema: classifyProseSchemaJSON}, nil
}

// TestClassifyViolationPinsLibraryProse pins both halves of the prose contract
// classifyViolation keys on: jsonschema-go renders required and
// dependentRequired failures — and only those — with "missing properties".
// A library upgrade that changes either half turns this red and re-opens the
// classification (same pinning precedent as
// TestValidatorNamesTheMissingRootUnderOnlySomeApplicators).
func TestClassifyViolationPinsLibraryProse(t *testing.T) {
	validator, err := schemavalidate.New(classifyProseRegistry{}, t.TempDir())
	require.NoError(t, err)

	cases := []struct {
		name string
		doc  map[string]any
		kind string
	}{
		{"missing required property", map[string]any{}, violationKindRequired},
		{"dependentRequired counts as required", map[string]any{"name": "open", "email": "a@b.c"}, violationKindRequired},
		{"enum violation is a constraint", map[string]any{"name": "banana"}, violationKindConstraint},
		{"type violation is a constraint", map[string]any{"name": "open", "email": 7, "phone": "x"}, violationKindConstraint},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verr := validator.Validate(100, tc.doc)
			require.ErrorIs(t, verr, forma.ErrInvalidInput)
			require.Equal(t, tc.kind, classifyViolation(verr))
		})
	}
}
