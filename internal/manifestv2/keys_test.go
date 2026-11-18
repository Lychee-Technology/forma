package manifestv2

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLayoutSpellsTheProtocolKeys(t *testing.T) {
	id := uuid.MustParse("0190a000-0000-7000-8000-000000000001")
	for _, l := range []Layout{{Prefix: "data"}, {Prefix: "data/"}} {
		require.Equal(t, "data/100/_manifests/e3/g00000000000000000007-"+testToken.String()+".json", l.ManifestKey(100, 3, 7, testToken))
		require.Equal(t, "data/100/e3/delta/"+id.String()+".parquet", l.ParquetKey(100, 3, TierDelta, id))
	}
	require.Equal(t, "/100/e1/base/"+id.String()+".parquet", Layout{}.ParquetKey(100, 1, TierBase, id), "an empty prefix keeps the leading slash, as the V1 builders do")

	key, err := Layout{Prefix: "a/b"}.NewParquetKey(100, 2, TierBase)
	require.NoError(t, err)
	p, err := parseParquetKey(key)
	require.NoError(t, err)
	require.Equal(t, parquetKeyParts{layout: Layout{Prefix: "a/b"}, schemaID: 100, epoch: 2, tier: TierBase, id: p.id}, p)
	require.EqualValues(t, 7, p.id.Version())
	other, err := Layout{Prefix: "a/b"}.NewParquetKey(100, 2, TierBase)
	require.NoError(t, err)
	require.NotEqual(t, key, other, "every parquet key is fresh")
}

func TestParseManifestKeyReadsBackOnlyTheCanonicalSpelling(t *testing.T) {
	key := Layout{Prefix: "x/y"}.ManifestKey(100, 2, 9, testToken)
	p, err := parseManifestKey(key)
	require.NoError(t, err)
	require.Equal(t, manifestKeyParts{layout: Layout{Prefix: "x/y"}, schemaID: 100, epoch: 2, generation: 9, token: testToken}, p)

	tok := testToken.String()
	for _, bad := range []string{
		"x/100/_manifests/e2/g9-" + tok + ".json",                    // not zero-padded
		"x/100/_manifests/e2/g00000000000000000009-" + tok + ".JSON", // suffix
		"x/100/_manifests/e02/g00000000000000000009-" + tok + ".json",
		"x/100/_manifests/e0/g00000000000000000009-" + tok + ".json",
		"x/0100/_manifests/e2/g00000000000000000009-" + tok + ".json",
		"x/100/manifests/e2/g00000000000000000009-" + tok + ".json",
		"x/100/_manifests/e2/00000000000000000009-" + tok + ".json",
		"x/100/_manifests/e2/g00000000000000000009-" + uuid.MustParse(tok).URN() + ".json",
		"100/_manifests/e2/g00000000000000000009-" + tok + ".json", // no prefix segment
	} {
		_, err := parseManifestKey(bad)
		require.Error(t, err, bad)
	}
}

func TestParseParquetKeyRefusesWhatTheLayoutNeverWrites(t *testing.T) {
	v7 := "0190a000-0000-7000-8000-000000000001"
	for _, bad := range []string{
		"d/100/e1/hot/" + v7 + ".parquet",
		"d/100/e1/base/" + v7 + ".json",
		"d/100/e1/base/" + uuid.NewString() + ".parquet", // version 4
		"d/100/e1/base/0190A000-0000-7000-8000-000000000001.parquet",
		"d/100/1/base/" + v7 + ".parquet",
		"d/-1/e1/base/" + v7 + ".parquet",
		"100/e1/base/" + v7 + ".parquet",
	} {
		_, err := parseParquetKey(bad)
		require.Error(t, err, bad)
	}
}
