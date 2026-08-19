package transform

import (
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// skippedAttrLogMessage is a const so the tests pinning the once-per-shape
// contract filter on the exact string production emits.
const skippedAttrLogMessage = "skipped EAV records for attribute ids not in metadata cache (removed by schema evolution or retired; rows preserved, #294)"

// skippedAttrLogSeen dedupes the tolerated-skip log across the process (#343).
// FromEAVRecords runs once per row on the read path, and the skipped condition
// is a property of the schema shape, not of the row, so per-call logging turned
// a #294-supported state into per-row noise. Package-level deliberately:
// production builds a fresh AttributeConverter per row
// (persistentRecordTransformer.newConverter), so converter-scoped state would
// dedupe nothing. Keys are schemaID plus the sorted skipped attrID set; the
// population is bounded by registered schemas times the few dropped/retired
// sets each can accumulate, so the map needs no eviction.
var skippedAttrLogSeen = struct {
	mu   sync.Mutex
	keys map[string]struct{}
}{keys: make(map[string]struct{})}

// logSkippedAttrIDs emits the skip log at Info once per process per (schemaID,
// skipped attrID set). Info, not Warn: #294 defines the state as supported, and
// Warn reads as an incident to an on-call operator. ids must already be sorted
// so equal sets build equal keys; exampleRowID is the first row observed with
// this shape, recorded as a lookup pointer, not as the condition's scope.
func logSkippedAttrIDs(schemaID int16, exampleRowID uuid.UUID, ids []int16) {
	var key strings.Builder
	key.WriteString(strconv.Itoa(int(schemaID)))
	for _, id := range ids {
		key.WriteByte(':')
		key.WriteString(strconv.Itoa(int(id)))
	}

	skippedAttrLogSeen.mu.Lock()
	_, dup := skippedAttrLogSeen.keys[key.String()]
	if !dup {
		skippedAttrLogSeen.keys[key.String()] = struct{}{}
	}
	skippedAttrLogSeen.mu.Unlock()
	if dup {
		return
	}

	zap.S().Infow(skippedAttrLogMessage,
		"schemaID", schemaID, "exampleRowID", exampleRowID, "attrIDs", ids)
}
