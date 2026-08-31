package benchmark

import (
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// #460: the benchmark generator assigns tradeTime FROM changed_at, and the
// federated reader used to alias changed_at into created_at — so ordering by
// created_at and ordering by tradeTime produced the same sequence by accident.
// Once a row's creation stamp became its FIRST version's changed_at, the two
// diverge for any row with overlapping versions.
//
// That matters because keyset and offset workloads cannot share an order: the
// engine supports keyset cursors on SYSTEM columns only, so a keyset page is
// ordered created_at DESC, row_id ASC while offset pages sort by tradeTime
// DESC. The oracle has to follow whichever the workload actually uses, or the
// hotspot-overlap distribution fails the row-id assertion deterministically.
//
// The CI smoke distribution is uniform (single-version rows, where the two
// orders still coincide), so this case needs its own coverage.

// overlappingVersionWinners builds two trade rows whose creation order is the
// REVERSE of their trade-time order — the shape a hotspot-overlap update
// produces, and the shape that makes the two orderings disagree:
//
//	older: created 100, updated to tradeTime 300  (the LWW winner)
//	newer: created 200, never updated             (tradeTime 200)
//
// By creation time: newer(200) then older(100).
// By trade time:    older(300) then newer(200).
func overlappingVersionWinners(t *testing.T) (records []GeneratedRecord, older, newer uuid.UUID) {
	t.Helper()
	older = uuid.MustParse("018f0000-0000-7000-8000-00000000000a")
	newer = uuid.MustParse("018f0000-0000-7000-8000-00000000000b")

	mk := func(rowID uuid.UUID, createdAt, tradeTime int64) GeneratedRecord {
		return GeneratedRecord{
			SchemaID:   SchemaIDTrade,
			SchemaName: "trade",
			RowID:      rowID,
			Version:    1,
			CreatedAt:  createdAt,
			ChangedAt:  tradeTime,
			Attributes: map[string]any{"tradeTime": strconv.FormatInt(tradeTime, 10)},
		}
	}
	return []GeneratedRecord{mk(older, 100, 300), mk(newer, 200, 200)}, older, newer
}

// TestKeysetWorkloadOracleOrdersByCreationStamp pins the keyset half: the
// oracle must rank by creation time, matching the created_at DESC cursor the
// engine actually pages with.
func TestKeysetWorkloadOracleOrdersByCreationStamp(t *testing.T) {
	records, older, newer := overlappingVersionWinners(t)

	sortExpectedRecordsForWorkload(records, WorkloadDefinition{
		TargetSchema:        "trade",
		UseKeysetPagination: true,
	})

	require.Equal(t, []uuid.UUID{newer, older},
		[]uuid.UUID{records[0].RowID, records[1].RowID},
		"a keyset workload pages by created_at DESC, so the later-CREATED row leads (#460)")
}

// TestOffsetWorkloadOracleStillOrdersByTradeTime pins the other half, so the
// keyset change cannot silently reorder the offset workloads that legitimately
// sort by tradeTime through AttributeOrders.
func TestOffsetWorkloadOracleStillOrdersByTradeTime(t *testing.T) {
	records, older, newer := overlappingVersionWinners(t)

	sortExpectedRecordsForWorkload(records, WorkloadDefinition{
		TargetSchema:        "trade",
		UseKeysetPagination: false,
	})

	require.Equal(t, []uuid.UUID{older, newer},
		[]uuid.UUID{records[0].RowID, records[1].RowID},
		"an offset workload sorts by tradeTime DESC, so the later-UPDATED row leads")
}

// TestKeysetAndOffsetOrdersDisagreeOnOverlappingVersions is the guard that
// keeps the two tests above meaningful. If a future change made creation time
// and trade time coincide again, both would pass while asserting nothing —
// exactly the state that hid this defect before #460.
func TestKeysetAndOffsetOrdersDisagreeOnOverlappingVersions(t *testing.T) {
	keyset, _, _ := overlappingVersionWinners(t)
	offset, _, _ := overlappingVersionWinners(t)

	sortExpectedRecordsForWorkload(keyset, WorkloadDefinition{TargetSchema: "trade", UseKeysetPagination: true})
	sortExpectedRecordsForWorkload(offset, WorkloadDefinition{TargetSchema: "trade", UseKeysetPagination: false})

	require.NotEqual(t, keyset[0].RowID, offset[0].RowID,
		"the fixture must actually separate creation time from trade time, "+
			"or these tests would pass vacuously the way the pre-#460 equivalence did")
}

// TestStampCreationTimesIsWhatSeparatesTheOrders ties the divergence back to
// its cause: SplitIntoTiers stamps every version of a row with its earliest
// ChangedAt, which is what makes created_at stop tracking the latest write.
func TestStampCreationTimesIsWhatSeparatesTheOrders(t *testing.T) {
	rowID := uuid.MustParse("018f0000-0000-7000-8000-00000000000c")
	records := []GeneratedRecord{
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: rowID, Version: 1, ChangedAt: 100},
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: rowID, Version: 2, ChangedAt: 300},
	}

	stampCreationTimes(records)

	for _, r := range records {
		require.Equal(t, int64(100), r.CreatedAt,
			"every version of a row carries the row's FIRST changed_at as its creation stamp (#460)")
	}
	require.NotEqual(t, records[1].CreatedAt, records[1].ChangedAt,
		"the winning version's creation stamp must differ from its version stamp, "+
			"which is precisely why the keyset and offset orders can disagree")
}
