package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEmitReportOnlyValidationViolation pins the #317 counter's wire shape:
// the name a scraping deployment keys its dashboard on, and the three labels
// the rollout question ("is it safe to flip VALIDATE_UPDATES_STRICT for this
// schema yet") is asked over.
func TestEmitReportOnlyValidationViolation(t *testing.T) {
	var gotName string
	var gotLabels map[string]string
	var gotValue any
	RegisterTelemetryEmitter(func(_ context.Context, name string, labels map[string]string, value any) {
		gotName, gotLabels, gotValue = name, labels, value
	})
	t.Cleanup(func() { RegisterTelemetryEmitter(nil) })

	EmitReportOnlyValidationViolation(context.Background(), 100, "lead", "required")

	require.Equal(t, "entity_report_only_validation_violation_total", gotName)
	require.Equal(t, map[string]string{
		"schema_id":   "100",
		"schema_name": "lead",
		"kind":        "required",
	}, gotLabels)
	require.Equal(t, int64(1), gotValue)
}
