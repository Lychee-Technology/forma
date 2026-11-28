package internal

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// Real-PostgreSQL tests of the unset operator's refusals and validation
// (#938): an intake refusal in a Tx names its target and sends no statement,
// so the Tx still commits; and the validation matrix's verdicts are what
// PostgreSQL commits, with report-only mode's existing log line and counter.

// unsetRefusal is an update intake refuses, and the text its published
// message holds.
type unsetRefusal struct {
	op   *forma.EntityOperation
	want string
}

// requireRefusalsLeaveTheTxUsable runs every refusal in one Tx and then
// commit, a valid update. Each refusal must be ErrInvalidInput whose
// published message holds its want, and send no statement: the Tx goes on
// to commit.
func (e *txEnv) requireRefusalsLeaveTheTxUsable(t *testing.T, refusals []unsetRefusal, commit *forma.EntityOperation) {
	t.Helper()
	var sent atomic.Int64
	previous := e.txr.statementFault
	e.txr.statementFault = func(sql string) error {
		sent.Add(1)
		if previous != nil {
			return previous(sql)
		}
		return nil
	}
	t.Cleanup(func() { e.txr.statementFault = previous })

	err := e.inTx(func(ctx context.Context, tx forma.Tx) error {
		for i, refusal := range refusals {
			before := sent.Load()
			_, err := tx.Update(ctx, refusal.op)
			if !assert.ErrorIs(t, err, forma.ErrInvalidInput, "refusal %d", i) {
				continue
			}
			message, published := forma.ResolvePublicMessage(err)
			assert.True(t, published, "refusal %d publishes its message: %v", i, err)
			assert.Contains(t, message, refusal.want, "refusal %d names its target", i)
			assert.Equal(t, before, sent.Load(), "refusal %d sent a statement", i)
		}
		before := sent.Load()
		if _, err := tx.Update(ctx, commit); err != nil {
			return fmt.Errorf("the update after the refusals: %w", err)
		}
		assert.Greater(t, sent.Load(), before, "the counted session is the one the Tx writes through")
		return nil
	})
	require.NoError(t, err, "the Tx commits after its refusals")
}

// TestUnsetRefusalsLeaveATxUsable pins that every intake refusal of unsetdoc,
// a refused target or an overlap, names its target and sends no statement:
// the Tx goes on to commit an unset, and the row is otherwise untouched.
func TestUnsetRefusalsLeaveATxUsable(t *testing.T) {
	env := setupUnsetDocPG(t)
	doc := unsetBaseDoc()
	doc["lines"] = []any{}
	rowID := env.mustCreate(t, unsetSchemaName, doc)
	before := env.snapshot(t, unsetSchemaName, rowID)
	op := func(updates map[string]any, ops ...forma.UpdateOp) *forma.EntityOperation {
		return unsetOp(rowID, updates, ops...)
	}
	env.requireRefusalsLeaveTheTxUsable(t, []unsetRefusal{
		{op(nil, forma.Unset("missing")), "unset names attribute 'missing'"},
		{op(nil, forma.Unset("created")), "attribute 'created', which is bound to system column 'ltbase_created_at'"},
		{op(nil, forma.Unset("profile")), "unset names 'profile', an object"},
		{op(nil, forma.Unset("items")), "unset names 'items', an array of objects"},
		{op(nil, forma.Unset("lines")), "unset names 'lines', an array of objects"},
		{op(nil, forma.Unset("items.sku")), "attribute 'items.sku' is inside the array of objects 'items'"},
		{op(nil, forma.Unset("tags[0]")), "unset names 'tags[0]'"},
		{op(nil, forma.Unset("$.note")), "unset names '$.note'"},
		{op(nil, forma.Unset("note"), forma.Unset("note")), "attribute 'note' has more than one update operator"},
		{op(map[string]any{"note": "x"}, forma.Unset("note")), "attribute 'note' appears in both updates"},
		{op(map[string]any{"profile": map[string]any{"nick": "x"}}, forma.Unset("profile.nick")), "attribute 'profile.nick' appears in both updates"},
		{op(map[string]any{"profile.nick": "x"}, forma.Unset("profile.nick")), "attribute 'profile.nick' appears in both updates"},
		{op(map[string]any{"profile": "flat"}, forma.Unset("profile.nick")), "attribute 'profile.nick' appears in both updates (as 'profile')"},
		{op(nil, forma.Increment("counter", 1), forma.Unset("counter")), "attribute 'counter' has more than one update operator"},
		{op(nil, forma.ServerTime("due", 0), forma.Unset("due")), "attribute 'due' has more than one update operator"},
	}, op(nil, forma.Unset("label")))

	after := env.snapshot(t, unsetSchemaName, rowID)
	assert.NotEqual(t, before.main, after.main, "the Tx committed the unset")
	assert.Nil(t, env.mainColumns(t, unsetSchemaName, rowID)["text_01"])
	assert.Equal(t, before.eav, after.eav, "no refused operation wrote EAV, the marker of lines included")
}

// TestUnsetRelationAndUnderivedRefusalsLeaveATxUsable covers the refusals
// unsetdoc cannot hold, over the shipped visit schema: an x-relation root and
// an attribute beneath it, and a nested attribute whose array ancestry the
// file registry does not derive (visit reaches its contact through another
// file's $ref, so none of visit's is derived).
func TestUnsetRelationAndUnderivedRefusalsLeaveATxUsable(t *testing.T) {
	env := setupTxEnv(t)
	visit := txVisit("scheduled")
	visit["feedback"] = "to clear"
	visit["propertySnapshot"] = map[string]any{"title": "kept"}
	rowID := env.mustCreate(t, "visit", visit)
	op := func(attr string) *forma.EntityOperation {
		return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
			Type: forma.OperationUpdate, UpdateOps: []forma.UpdateOp{forma.Unset(attr)}}
	}
	env.requireRefusalsLeaveTheTxUsable(t, []unsetRefusal{
		{op("contactSnapshot"), "attribute 'contactSnapshot' belongs to relation 'contactSnapshot'"},
		{op("contactSnapshot.birthday"), "attribute 'contactSnapshot.birthday' belongs to relation 'contactSnapshot'"},
		{op("propertySnapshot.title"), "unset names nested attribute 'propertySnapshot.title' of schema 'visit', whose array ancestry the registry did not derive"},
	}, op("feedback"))

	got := env.attrs(t, "visit", rowID)
	assert.NotContains(t, got, "feedback", "the Tx committed the unset")
	assert.Equal(t, map[string]any{"title": "kept"}, got["propertySnapshot"], "the refused nested target is kept")
}

// reportOnlyViolationLog is the message report-only validation logs for an
// accepted violation.
const reportOnlyViolationLog = "write payload violates the entity JSON schema; accepted because strict update validation is off"

// TestUnsetValidationMatrixCommits runs the validation matrix
// (unsetVerdictCases) against PostgreSQL in strict, report-only and
// no-validator modes, on both update routes: an accepted clear commits the
// resulting document, a refused one leaves the row as stored, and
// report-only mode logs and counts each violation it accepts as it does for
// any update.
func TestUnsetValidationMatrixCommits(t *testing.T) {
	env := setupUnsetDocPG(t)
	for _, mode := range updateValidationModes {
		recorder := &metricRecorder{}
		modeEnv := env.withUpdateValidationEmitting(t, mode, unsetSchemaDir, nil, recorder)
		for _, surface := range unsetSurfaces {
			for _, tc := range unsetVerdictCases() {
				t.Run(mode.name+"/"+surface.name+"/"+tc.name, func(t *testing.T) {
					rowID := env.mustCreate(t, unsetSchemaName, tc.base)
					before := env.snapshot(t, unsetSchemaName, rowID)
					recorder.events = nil
					core, logs := observer.New(zap.WarnLevel)
					t.Cleanup(zap.ReplaceGlobals(zap.New(core)))

					_, err := surface.update(modeEnv, unsetOp(rowID, tc.updates, tc.ops...))
					accept := tc.accept[unsetMode(mode.name)]
					requireUnsetVerdict(t, err, accept, tc.want)
					if accept {
						assert.Equal(t, unsetLogical(t, unsetVerdictResult(tc)), unsetLogical(t, env.attrs(t, unsetSchemaName, rowID)), "the resulting document is committed")
						assert.NotEqual(t, before.main, env.snapshot(t, unsetSchemaName, rowID).main)
					} else {
						assert.Equal(t, before, env.snapshot(t, unsetSchemaName, rowID), "a refused clear commits nothing")
					}
					requireReportOnlyEvidence(t, logs, recorder, rowID, mode.name == unsetReportOnlyName(), tc.reportOnlyKind)
				})
			}
		}
	}
}

// unsetReportOnlyName is the name updateValidationModes gives report-only
// mode, the unsetMode of the same name.
func unsetReportOnlyName() string { return string(unsetReportOnly) }

// unsetVerdictResult is the document tc's update makes of its base.
func unsetVerdictResult(tc unsetVerdictCase) map[string]any {
	doc := tc.base
	for _, op := range tc.ops {
		doc = withoutPath(doc, op.Attribute)
	}
	for attr, value := range tc.updates {
		doc[attr] = value
	}
	return doc
}

// requireReportOnlyEvidence asserts that report-only mode logged and counted
// exactly the one violation of kind it accepted on rowID, and that nothing
// was logged or counted otherwise.
func requireReportOnlyEvidence(t *testing.T, logs *observer.ObservedLogs, recorder *metricRecorder, rowID uuid.UUID, reportOnly bool, kind string) {
	t.Helper()
	var counted []string
	for _, event := range recorder.events {
		if event.Name == "entity_report_only_validation_violation_total" {
			counted = append(counted, event.Labels["kind"])
		}
	}
	entries := logs.FilterMessage(reportOnlyViolationLog).All()
	if !reportOnly || kind == "" {
		assert.Empty(t, counted, "no violation is counted")
		assert.Empty(t, entries, "no violation is logged")
		return
	}
	assert.Equal(t, []string{kind}, counted, "report-only counts the accepted violation as today")
	require.Len(t, entries, 1, "report-only logs the accepted violation once")
	fields := entries[0].ContextMap()
	assert.Equal(t, unsetSchemaName, fields["schemaName"])
	assert.Equal(t, rowID.String(), fmt.Sprint(fields["rowID"]))
	assert.Equal(t, kind, fields["kind"])
}
