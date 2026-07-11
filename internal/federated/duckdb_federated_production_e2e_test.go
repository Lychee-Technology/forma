package federated

import "testing"

// The production three-tier suite (#177) in internal/e2e_harness/production
// supersedes these stubs with oracle-checked coverage; their removal is
// tracked by #196.

func TestStreamDuckDBFederatedQuery_GivenBaseAndHotVersions_WhenQueried_ThenLatestHotVersionWins(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/dirty_hot_shadows_cold (internal/e2e_harness/production, #177); removal tracked by #196")
}

func TestStreamDuckDBFederatedQuery_GivenConflictingCreatedAndUpdatedAt_WhenQueried_ThenLatestUpdatedAtWins(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/changed_at_beats_tier_layout (internal/e2e_harness/production, #177); removal tracked by #196")
}

func TestStreamDuckDBFederatedQuery_GivenDirtyHotRow_WhenColdVersionExists_ThenColdVersionIsExcluded(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/dirty_hot_shadows_cold (internal/e2e_harness/production, #177); removal tracked by #196")
}

func TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotRows_WhenQueried_ThenAllNonOverlappingRowsAreReturned(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/union (internal/e2e_harness/production, #177); removal tracked by #196")
}

func TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotOverlap_WhenQueried_ThenNewestHotVersionWins(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/triple_overlap_hot_wins (internal/e2e_harness/production, #177); removal tracked by #196")
}

func TestStreamDuckDBFederatedQuery_GivenBaseAndDeltaRowsWithoutHotData_WhenQueried_ThenColdRowsAreReturned(t *testing.T) {
	t.Skip("superseded by TestThreeTierMerge/changed_at_beats_tier_layout and TestThreeTierIsolation (internal/e2e_harness/production, #177); removal tracked by #196")
}

func TestStreamDuckDBFederatedQuery_GivenSoftDeletedRowsAcrossMixedTiers_WhenQueried_ThenDeletedRowsAreExcluded(t *testing.T) {
	t.Skip("superseded by TestThreeTierSoftDelete (internal/e2e_harness/production, #177); removal tracked by #196")
}
