package transform

import (
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// resetSkippedAttrLogMemo clears the process-wide memo so each test observes
// first-emission behavior regardless of what earlier tests in the package
// (e.g. TestAttributeConverterFromEAVRecords_SkipsUnknownAttributeIDs) fed it.
func resetSkippedAttrLogMemo() {
	skippedAttrLogSeen.mu.Lock()
	defer skippedAttrLogSeen.mu.Unlock()
	skippedAttrLogSeen.keys = make(map[string]struct{})
}

func skipLogRegistry(schemaID int16) *stubSchemaRegistry {
	return &stubSchemaRegistry{
		schemaID:   schemaID,
		schemaName: fmt.Sprintf("skip_log_schema_%d", schemaID),
		cache: forma.SchemaAttributeCache{
			"name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		},
	}
}

func skipLogRecords(schemaID int16, staleAttrID int16) []model.EAVRecord {
	rowID := uuid.Must(uuid.NewV7())
	known := "alive"
	stale := "mystery"
	return []model.EAVRecord{
		{SchemaID: schemaID, RowID: rowID, AttrID: 1, ValueText: &known},
		{SchemaID: schemaID, RowID: rowID, AttrID: staleAttrID, ValueText: &stale},
	}
}

// TestSkippedAttrLogInfoOncePerShape pins #343: the #294 tolerated-skip log is
// Info (not Warn — the state is supported, not an incident) and fires once per
// process per (schemaID, skipped-attrID-set), surviving converter churn — the
// read path builds a fresh converter per row (persistentRecordTransformer's
// newConverter), which is exactly why the memo must be package-level.
func TestSkippedAttrLogInfoOncePerShape(t *testing.T) {
	resetSkippedAttrLogMemo()
	registry := skipLogRegistry(441)

	core, logs := observer.New(zap.InfoLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)

	records := skipLogRecords(441, 999)
	// Fresh converter per call mirrors the per-row production pattern.
	for i := 0; i < 3; i++ {
		if _, err := NewAttributeConverter(registry).FromEAVRecords(records); err != nil {
			t.Fatalf("FromEAVRecords call %d: %v", i, err)
		}
	}

	entries := logs.FilterMessage(skippedAttrLogMessage).All()
	if len(entries) != 1 {
		t.Fatalf("got %d skip-log entries after 3 identical calls, want exactly 1 (memoized per shape)", len(entries))
	}
	if entries[0].Level != zapcore.InfoLevel {
		t.Fatalf("skip log level = %s, want Info: #294 defines the state as supported, Warn reads as an incident", entries[0].Level)
	}
	fields := entries[0].ContextMap()
	if fmt.Sprint(fields["attrIDs"]) != "[999]" {
		t.Fatalf("attrIDs field = %v, want [999]", fields["attrIDs"])
	}
	if fmt.Sprint(fields["schemaID"]) != "441" {
		t.Fatalf("schemaID field = %v, want 441", fields["schemaID"])
	}

	// A different skipped set on the same schema is a new shape: logged again.
	if _, err := NewAttributeConverter(registry).FromEAVRecords(skipLogRecords(441, 998)); err != nil {
		t.Fatalf("FromEAVRecords with second shape: %v", err)
	}
	if got := len(logs.FilterMessage(skippedAttrLogMessage).All()); got != 2 {
		t.Fatalf("got %d entries after a distinct skipped set, want 2 (memo keys on the set, not the schema)", got)
	}
}

// TestSkippedAttrLogConcurrentCallsEmitOnce pins the once-per-process contract
// under concurrency: parallel readers hitting the same shape must not race two
// emissions past the memo. CI's -race run exercises the lock discipline.
func TestSkippedAttrLogConcurrentCallsEmitOnce(t *testing.T) {
	resetSkippedAttrLogMemo()
	registry := skipLogRegistry(442)

	core, logs := observer.New(zap.InfoLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)

	records := skipLogRecords(442, 999)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := NewAttributeConverter(registry).FromEAVRecords(records); err != nil {
				t.Errorf("FromEAVRecords: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := len(logs.FilterMessage(skippedAttrLogMessage).All()); got != 1 {
		t.Fatalf("got %d skip-log entries from 8 concurrent calls, want exactly 1", got)
	}
}
