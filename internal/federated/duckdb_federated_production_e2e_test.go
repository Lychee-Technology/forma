package federated

import "testing"

func TestStreamDuckDBFederatedQuery_GivenBaseAndHotVersions_WhenQueried_ThenLatestHotVersionWins(t *testing.T) {
	t.Skip("requires root symbols (NewDBPersistentRecordRepository, loadProductionFederatedMetadata); covered by root integration tests")
}

func TestStreamDuckDBFederatedQuery_GivenConflictingCreatedAndUpdatedAt_WhenQueried_ThenLatestUpdatedAtWins(t *testing.T) {
	t.Skip("requires root symbols; covered by root integration tests")
}

func TestStreamDuckDBFederatedQuery_GivenDirtyHotRow_WhenColdVersionExists_ThenColdVersionIsExcluded(t *testing.T) {
	t.Skip("requires root symbols; covered by root integration tests")
}

func TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotRows_WhenQueried_ThenAllNonOverlappingRowsAreReturned(t *testing.T) {
	t.Skip("requires root symbols; covered by root integration tests")
}

func TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotOverlap_WhenQueried_ThenNewestHotVersionWins(t *testing.T) {
	t.Skip("requires root symbols; covered by root integration tests")
}

func TestStreamDuckDBFederatedQuery_GivenBaseAndDeltaRowsWithoutHotData_WhenQueried_ThenColdRowsAreReturned(t *testing.T) {
	t.Skip("requires root symbols; covered by root integration tests")
}

func TestStreamDuckDBFederatedQuery_GivenSoftDeletedRowsAcrossMixedTiers_WhenQueried_ThenDeletedRowsAreExcluded(t *testing.T) {
	t.Skip("requires root symbols; covered by root integration tests")
}
