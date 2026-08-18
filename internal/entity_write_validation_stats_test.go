package internal

import (
	"context"
	"sync"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/telemetry"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

// TestReportOnlyKindIsClassifiedBeforeDecoration pins what the classifier is
// allowed to see: the validator's own error, and never the error after
// explainStrippedRelationRoots has decorated it.
//
// The hazard is not hypothetical. The diagnosis interpolates the schema's
// relation root names verbatim (%q), so a root named "missing properties" is by
// itself enough to make the decorated string satisfy the substring test
// classifyViolation keys on — the local demonstration below shows the same enum
// violation classifying both ways depending on which error is handed over. The
// root name is contrived, and that is the point: it turns "a future edit to this
// repo-owned prose would silently relabel every violation on every
// relation-declaring schema" into something a test holds today.
//
// It asserts at validateWritePayload rather than through a manager because that
// is the seam all four write paths share and the only place the ordering exists
// to get wrong. Classifying after the wrap turns this red: the emitted kind
// becomes "required".
func TestReportOnlyKindIsClassifiedBeforeDecoration(t *testing.T) {
	validator, err := schemavalidate.New(classifyProseRegistry{}, t.TempDir())
	require.NoError(t, err)
	payload := map[string]any{"name": "banana"}

	// The decoration really can flip the label; production is immune only
	// because it classifies first.
	verr := validator.Validate(100, payload)
	require.Error(t, verr)
	require.Equal(t, violationKindConstraint, classifyViolation(verr))
	require.Equal(t, violationKindRequired,
		classifyViolation(explainStrippedRelationRoots(verr, "test", []string{"missing properties"})),
		"the decorated error genuinely misclassifies, which is what the ordering protects against")

	var kinds []string
	telemetry.RegisterTelemetryEmitter(func(_ context.Context, _ string, labels map[string]string, _ any) {
		kinds = append(kinds, labels["kind"])
	})
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })
	core, logs := observer.New(zap.WarnLevel)
	t.Cleanup(zap.ReplaceGlobals(zap.New(core)))

	require.NoError(t, validateWritePayload(context.Background(), writeValidation{
		validator:     validator,
		schemaID:      100,
		schemaName:    "test",
		data:          payload,
		relationRoots: []string{"missing properties"},
		enforce:       false,
	}))

	require.Equal(t, []string{violationKindConstraint}, kinds,
		"an enum violation is a constraint however the diagnosis is worded")
	perWrite := logs.FilterMessageSnippet("violates the entity JSON schema").All()
	require.Len(t, perWrite, 1)
	fields := perWrite[0].ContextMap()
	require.Equal(t, violationKindConstraint, fields["kind"])
	// Vacuity guard: the assertion above says something only if the decoration
	// really joined the logged error and really does satisfy the classifier's
	// substring test.
	require.Contains(t, fields["error"], "missing properties")
}

// TestReportOnlyStatsRecordIsConcurrencySafe pins record against the way it is
// actually called: four write paths, one shared aggregate, no coordination
// above it. Two schemas are recorded from disjoint goroutine groups, so the
// lazy map insert races too and not only the counters.
//
// Removing the mutex fails this under -race (CI runs -race), and usually
// without it as well: the map insert becomes a "concurrent map writes" fatal
// and the increments lose updates, so the exact totals below stop holding.
func TestReportOnlyStatsRecordIsConcurrencySafe(t *testing.T) {
	const groupsPerSchema, recordsPerGoroutine = 4, 250
	stats := newReportOnlyStats()

	var wg sync.WaitGroup
	for _, schemaID := range []int16{1, 2} {
		for g := 0; g < groupsPerSchema; g++ {
			wg.Add(1)
			go func(schemaID int16) {
				defer wg.Done()
				for i := 0; i < recordsPerGoroutine; i++ {
					kind := violationKindConstraint
					if i%2 == 0 {
						kind = violationKindRequired
					}
					stats.record(schemaID, kind)
				}
			}(schemaID)
		}
	}
	wg.Wait()

	for _, schemaID := range []int16{1, 2} {
		counts := stats.schemas[schemaID]
		require.NotNil(t, counts, "schema %d must have been recorded", schemaID)
		require.EqualValues(t, groupsPerSchema*recordsPerGoroutine, counts.total,
			"schema %d lost or double-counted a violation", schemaID)
		require.EqualValues(t, groupsPerSchema*recordsPerGoroutine/2, counts.required)
		require.EqualValues(t, groupsPerSchema*recordsPerGoroutine/2, counts.constraint)
		require.EqualValues(t, counts.total, counts.required+counts.constraint,
			"the kinds must partition the total")
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
