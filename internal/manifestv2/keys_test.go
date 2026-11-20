package manifestv2

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLayoutSpellsTheProtocolKeys(t *testing.T) {
	id := uuid.MustParse("0190a000-0000-7000-8000-000000000001")
	for _, l := range []Layout{{Prefix: "data"}, {Prefix: "data/"}, {Prefix: "data//"}} {
		require.Equal(t, "data/100/_manifests/e3/g00000000000000000007-"+testToken.String()+".json", l.ManifestKey(100, 3, 7, testToken))
		require.Equal(t, "data/100/e3/delta/"+id.String()+".parquet", l.ParquetKey(100, 3, TierDelta, id))
	}
	require.Equal(t, "/100/e1/base/"+id.String()+".parquet", Layout{}.ParquetKey(100, 1, TierBase, id), "an empty prefix keeps the leading slash, as the V1 builders do")
	require.Equal(t, "/100/e1/base/"+id.String()+".parquet", Layout{Prefix: "/"}.ParquetKey(100, 1, TierBase, id), "a prefix of slashes only is the empty prefix")

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

// TestEveryKeyALayoutBuildsReadsBackToItsCanonicalPrefix pins that the key
// builders and the parsers agree on every prefix: a key reads back to the
// prefix without its trailing slashes and is rebuilt byte for byte from it,
// and a key with a slash the canonical prefix drops is one no Layout builds.
func TestEveryKeyALayoutBuildsReadsBackToItsCanonicalPrefix(t *testing.T) {
	id := uuid.MustParse("0190a000-0000-7000-8000-000000000001")
	for prefix, canonical := range map[string]string{
		"data": "data", "data/": "data", "data//": "data", "data///": "data",
		"": "", "/": "", "//": "",
		"a/b": "a/b", "a/b/": "a/b", "a//b": "a//b", "/a": "/a", "/a/": "/a",
	} {
		l := Layout{Prefix: prefix}
		want := Layout{Prefix: canonical}

		pk := l.ParquetKey(100, 3, TierDelta, id)
		require.Equal(t, want.ParquetKey(100, 3, TierDelta, id), pk, prefix)
		p, err := parseParquetKey(pk)
		require.NoError(t, err, "prefix %q builds parquet key %q", prefix, pk)
		require.Equal(t, parquetKeyParts{layout: want, schemaID: 100, epoch: 3, tier: TierDelta, id: id}, p, prefix)
		require.Equal(t, pk, p.layout.ParquetKey(p.schemaID, p.epoch, p.tier, p.id), prefix)

		mk := l.ManifestKey(100, 3, 7, testToken)
		m, err := parseManifestKey(mk)
		require.NoError(t, err, "prefix %q builds manifest key %q", prefix, mk)
		require.Equal(t, manifestKeyParts{layout: want, schemaID: 100, epoch: 3, generation: 7, token: testToken}, m, prefix)
		require.Equal(t, mk, m.layout.ManifestKey(m.schemaID, m.epoch, m.generation, m.token), prefix)
	}

	for _, bad := range []string{
		"data//100/e3/delta/" + id.String() + ".parquet",
		"//100/e3/delta/" + id.String() + ".parquet",
	} {
		_, err := parseParquetKey(bad)
		require.Error(t, err, bad)
	}
	for _, bad := range []string{
		"data//100/_manifests/e3/g00000000000000000007-" + testToken.String() + ".json",
		"//100/_manifests/e3/g00000000000000000007-" + testToken.String() + ".json",
	} {
		_, err := parseManifestKey(bad)
		require.Error(t, err, bad)
	}
}

// TestLayoutHoldsOnlyManifestsUnderItsCanonicalPrefix: a layout holds a
// published manifest key under its prefix in any spelling of it, and refuses
// one under another prefix, a parent or a child of it included, with
// ErrForeignPrefix.
func TestLayoutHoldsOnlyManifestsUnderItsCanonicalPrefix(t *testing.T) {
	key := Layout{Prefix: "data"}.ManifestKey(100, 3, 7, testToken)
	for _, l := range []Layout{{Prefix: "data"}, {Prefix: "data/"}, {Prefix: "data//"}} {
		require.NoError(t, l.Holds(key), "prefix %q", l.Prefix)
	}
	for _, l := range []Layout{{}, {Prefix: "other"}, {Prefix: "data/sub"}, {Prefix: "x/data"}} {
		require.ErrorIs(t, l.Holds(key), ErrForeignPrefix, "prefix %q", l.Prefix)
	}
	require.NoError(t, Layout{}.Holds(Layout{}.ManifestKey(100, 3, 7, testToken)), "the empty prefix holds its own keys")
	err := Layout{Prefix: "data"}.Holds("data/100/not-a-manifest.json")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrForeignPrefix, "an unreadable key is not a prefix verdict")
}

func TestLayoutSchemaPrefixAndOwns(t *testing.T) {
	l := Layout{Prefix: "data/"}
	require.Equal(t, "data/100/", l.SchemaPrefix(100))

	manifestKey := l.ManifestKey(100, 2, 7, testToken)
	parquetKey, err := l.NewParquetKey(100, 2, TierBase)
	require.NoError(t, err)

	require.True(t, l.Owns(100, manifestKey))
	require.True(t, l.Owns(100, parquetKey))
	require.False(t, l.Owns(101, manifestKey), "other schema")
	require.False(t, l.Owns(101, parquetKey), "other schema")
	require.False(t, Layout{Prefix: "other"}.Owns(100, manifestKey), "other prefix")
	require.False(t, l.Owns(100, "data/100/manifest.json"), "V1 manifest")
	require.False(t, l.Owns(100, "data/100/_tmp/abc.parquet"), "V1 tmp object")
	require.False(t, l.Owns(100, "data/100/e2/base/not-a-uuid.parquet"), "malformed key")
}
