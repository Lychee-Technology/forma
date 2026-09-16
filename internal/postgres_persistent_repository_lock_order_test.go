package internal

import (
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #554 ruling: every batch writer takes its per-row advisory locks in
// ascending (schemaID, rowID) order, so two overlapping batches of any kind
// cannot invert and deadlock. The sort key is the tuple, not the FNV lock
// key, so the order stays readable in tests and logs.

func TestSortedRowKeysOrdersBySchemaThenRowIDBytes(t *testing.T) {
	lowRow := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	midRow := uuid.MustParse("0fffffff-ffff-ffff-ffff-ffffffffffff")
	highRow := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")

	input := []model.PersistentRecordKey{
		{SchemaID: 2, RowID: lowRow},
		{SchemaID: 1, RowID: highRow},
		{SchemaID: 1, RowID: lowRow},
		{SchemaID: 1, RowID: midRow},
	}
	want := []model.PersistentRecordKey{
		{SchemaID: 1, RowID: lowRow},
		{SchemaID: 1, RowID: midRow},
		{SchemaID: 1, RowID: highRow},
		{SchemaID: 2, RowID: lowRow},
	}

	got := sortedRowKeys(input)
	assert.Equal(t, want, got)
}

func TestSortedRowKeysLeavesInputUntouched(t *testing.T) {
	a := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	b := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	input := []model.PersistentRecordKey{{SchemaID: 1, RowID: b}, {SchemaID: 1, RowID: a}}
	snapshot := append([]model.PersistentRecordKey(nil), input...)

	got := sortedRowKeys(input)

	require.Equal(t, snapshot, input, "the write loop depends on input order; sorting must copy")
	assert.Equal(t, []model.PersistentRecordKey{{SchemaID: 1, RowID: a}, {SchemaID: 1, RowID: b}}, got)
}

// The hashed lock key does NOT sort the same way as the tuple: this pins that
// the helper orders on the tuple, so a future change to rowVersionLockKey
// cannot silently change the lock order.
func TestSortedRowKeysIgnoresHashedLockKeyOrder(t *testing.T) {
	a := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	b := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	// Find a pair whose hash order is inverted relative to the tuple order.
	for rowVersionLockKey(1, a) < rowVersionLockKey(1, b) {
		b[15]++
		if b[15] == 0 {
			t.Skip("no inverted pair found in the probed range")
		}
	}
	got := sortedRowKeys([]model.PersistentRecordKey{{SchemaID: 1, RowID: b}, {SchemaID: 1, RowID: a}})
	assert.Equal(t, a, got[0].RowID, "tuple order, not hash order, must decide")
	assert.Equal(t, b, got[1].RowID)
}
