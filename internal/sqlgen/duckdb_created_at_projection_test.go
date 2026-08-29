package sqlgen

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// #460: the parquet legs used to alias the LWW version stamp into the
// created_at slot (`changed_at AS created_at`) while the hot leg projected
// the real `m.ltbase_created_at`. The two UNION ALL legs therefore put
// different quantities in the same column: created_at was wrong for every
// parquet-winning row, and the default sort key changed value the moment a
// row was flushed. Both exporters have always written the true
// `ltbase_created_at` into parquet (cdc.exportModeSpec.baseSelectColumns),
// so the reader must project it.

// s3ProjectionItem returns the projection item at position idx of an
// S3SourceSelect fragment.
func s3ProjectionItem(t *testing.T, projection string, idx int) string {
	t.Helper()
	items := strings.Split(projection, ", ")
	require.Greater(t, len(items), idx, "projection %q has no item %d", projection, idx)
	return strings.TrimSpace(items[idx])
}

// TestSchemaProjection_S3CreatedAtIsTheCreationStamp pins the production
// parquet leg: slot 1 is the row's creation time, and the version stamp stays
// confined to ver_ts.
func TestSchemaProjection_S3CreatedAtIsTheCreationStamp(t *testing.T) {
	sp, err := BuildSchemaProjection(1, buildTestProjectionCache())
	require.NoError(t, err)

	require.Equal(t, "ltbase_created_at AS created_at", s3ProjectionItem(t, sp.S3SourceSelect, 1),
		"the parquet leg must project the exported creation stamp, not the LWW version stamp (#460)")
	require.Equal(t, "changed_at AS ver_ts", s3ProjectionItem(t, sp.S3SourceSelect, 2),
		"changed_at remains the version stamp — #460 narrows it, it does not move it")
	require.NotContains(t, sp.S3SourceSelect, "changed_at AS created_at",
		"the #460 alias must not survive anywhere in the parquet projection")
}

// TestSchemaProjection_HotAndS3CreatedAtAgree pins the cross-leg contract the
// UNION ALL depends on: both legs must name the same quantity in the
// created_at slot, so a row's default sort key does not change value when it
// crosses the flush boundary.
func TestSchemaProjection_HotAndS3CreatedAtAgree(t *testing.T) {
	sp, err := BuildSchemaProjection(1, buildTestProjectionCache())
	require.NoError(t, err)

	require.Contains(t, sp.PGSourceSelect, "m.ltbase_created_at AS created_at",
		"the hot leg reports the true creation time")
	require.Contains(t, sp.BuildPGSelectNoEAV(), "m.ltbase_created_at AS created_at",
		"the no-EAV hot leg reports the true creation time")
	require.Equal(t, "ltbase_created_at AS created_at", s3ProjectionItem(t, sp.S3SourceSelect, 1),
		"the parquet leg must report the same quantity as the hot leg (#460)")
}

// TestBenchmarkProjection_S3CreatedAtIsTheCreationStamp keeps the benchmark
// mirror in lockstep with the reader: leaving the alias behind would make
// benchmark numbers measure a contract nothing production-shaped uses.
func TestBenchmarkProjection_S3CreatedAtIsTheCreationStamp(t *testing.T) {
	for _, schemaID := range []int16{100, 101, 102} {
		proj := BuildBenchmarkProjections(schemaID)
		require.Equal(t, "ltbase_created_at AS created_at", s3ProjectionItem(t, proj.S3SourceSelect, 1),
			"benchmark schema %d parquet leg must project the creation stamp (#460)", schemaID)
		require.Contains(t, proj.PGSourceSelect, "m.ltbase_created_at AS created_at",
			"benchmark schema %d hot leg must report the same quantity", schemaID)

		require.Equal(t, "ltbase_created_at AS created_at",
			s3ProjectionItem(t, BuildBenchmarkS3Projection(schemaID), 1),
			"benchmark schema %d standalone S3 projection must match (#460)", schemaID)
	}
}
