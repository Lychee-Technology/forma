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

// TestReportOnlyStatsMilestones pins the milestone rule: the 1st accepted
// violation fires (the operator learns the schema has violations at all), then
// every 100th (the operator sees the trend without one line per write).
func TestReportOnlyStatsMilestones(t *testing.T) {
	stats := newReportOnlyStats()

	milestone, total, required, constraint := stats.record(7, violationKindConstraint)
	require.True(t, milestone, "the 1st violation must be a milestone")
	require.EqualValues(t, 1, total)
	require.EqualValues(t, 0, required)
	require.EqualValues(t, 1, constraint)

	for i := 2; i <= 99; i++ {
		milestone, _, _, _ = stats.record(7, violationKindRequired)
		require.False(t, milestone, "violation %d must not be a milestone", i)
	}

	milestone, total, required, constraint = stats.record(7, violationKindRequired)
	require.True(t, milestone, "the 100th violation must be a milestone")
	require.EqualValues(t, 100, total)
	require.EqualValues(t, 99, required)
	require.EqualValues(t, 1, constraint)
}

// TestReportOnlyStatsCountsPerSchema pins that milestones are per schema: the
// flip decision is per deployment but the repair work is per schema, and one
// noisy schema must not swallow another's first violation.
func TestReportOnlyStatsCountsPerSchema(t *testing.T) {
	stats := newReportOnlyStats()
	stats.record(1, violationKindConstraint)

	milestone, total, _, _ := stats.record(2, violationKindConstraint)

	require.True(t, milestone, "schema 2's first violation is its own milestone")
	require.EqualValues(t, 1, total)
}

// TestReportOnlyStatsNilReceiver pins that a manager constructed without stats
// (embedders and tests building services directly) skips milestones instead of
// panicking. Production wiring is pinned separately by
// TestReportOnlyUpdateLogsMilestoneOnFirstViolation.
func TestReportOnlyStatsNilReceiver(t *testing.T) {
	var stats *reportOnlyStats

	milestone, total, required, constraint := stats.record(1, violationKindConstraint)

	require.False(t, milestone)
	require.Zero(t, total)
	require.Zero(t, required)
	require.Zero(t, constraint)
}
