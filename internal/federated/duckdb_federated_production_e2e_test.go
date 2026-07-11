package federated

import "testing"

// The production three-tier suite (#177) in internal/e2e_harness/production
// supersedes these stubs with oracle-checked coverage; their removal is
// tracked by #196.

// TestStreamDuckDBFederatedQuery_GivenBaseAndHotVersions_WhenQueried_ThenLatestHotVersionWins
// is a stub whose scenario runs as TestThreeTierMerge/dirty_hot_shadows_cold.
func TestStreamDuckDBFederatedQuery_GivenBaseAndHotVersions_WhenQueried_ThenLatestHotVersionWins(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/dirty_hot_shadows_cold (internal/e2e_harness/production, #177); removal tracked by #196")
}

// TestStreamDuckDBFederatedQuery_GivenConflictingCreatedAndUpdatedAt_WhenQueried_ThenLatestUpdatedAtWins
// is a stub whose scenario runs as TestThreeTierMerge/changed_at_beats_tier_layout.
func TestStreamDuckDBFederatedQuery_GivenConflictingCreatedAndUpdatedAt_WhenQueried_ThenLatestUpdatedAtWins(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/changed_at_beats_tier_layout (internal/e2e_harness/production, #177); removal tracked by #196")
}

// TestStreamDuckDBFederatedQuery_GivenDirtyHotRow_WhenColdVersionExists_ThenColdVersionIsExcluded
// is a stub whose scenario runs as TestThreeTierMerge/dirty_hot_shadows_cold.
func TestStreamDuckDBFederatedQuery_GivenDirtyHotRow_WhenColdVersionExists_ThenColdVersionIsExcluded(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/dirty_hot_shadows_cold (internal/e2e_harness/production, #177); removal tracked by #196")
}

// TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotRows_WhenQueried_ThenAllNonOverlappingRowsAreReturned
// is a stub whose scenario runs as TestThreeTierMerge/union.
func TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotRows_WhenQueried_ThenAllNonOverlappingRowsAreReturned(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/union (internal/e2e_harness/production, #177); removal tracked by #196")
}

// TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotOverlap_WhenQueried_ThenNewestHotVersionWins
// is a stub whose scenario runs as TestThreeTierMerge/triple_overlap_hot_wins.
func TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotOverlap_WhenQueried_ThenNewestHotVersionWins(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/triple_overlap_hot_wins (internal/e2e_harness/production, #177); removal tracked by #196")
}

// TestStreamDuckDBFederatedQuery_GivenBaseAndDeltaRowsWithoutHotData_WhenQueried_ThenColdRowsAreReturned
// is a stub whose scenario runs as TestThreeTierMerge/changed_at_beats_tier_layout
// (base+delta, empty dirty set) and TestThreeTierIsolation (each parquet tier alone).
func TestStreamDuckDBFederatedQuery_GivenBaseAndDeltaRowsWithoutHotData_WhenQueried_ThenColdRowsAreReturned(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/changed_at_beats_tier_layout and TestThreeTierIsolation (internal/e2e_harness/production, #177); removal tracked by #196")
}

// TestStreamDuckDBFederatedQuery_GivenSoftDeletedRowsAcrossMixedTiers_WhenQueried_ThenDeletedRowsAreExcluded
// is a stub whose scenario runs as TestThreeTierSoftDelete.
func TestStreamDuckDBFederatedQuery_GivenSoftDeletedRowsAcrossMixedTiers_WhenQueried_ThenDeletedRowsAreExcluded(t *testing.T) {
	t.Skip("superseded by TestThreeTierSoftDelete (internal/e2e_harness/production, #177); removal tracked by #196")
}
