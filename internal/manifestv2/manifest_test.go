package manifestv2

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/stretchr/testify/require"
)

// goldenDigest is the SHA-256 of testdata/flush.golden.json. A change to the
// encoding changes every manifest a writer would stage and every digest
// PostgreSQL commits, so it must show up here first.
const goldenDigest = "4c9ff0fc4f429d3de79c61f144533cf880dece1cc2625956fd2a81f7db30cdf6"

func TestEncodeIsByteStableAndMatchesTheGolden(t *testing.T) {
	golden, err := os.ReadFile("testdata/flush.golden.json")
	require.NoError(t, err)
	m := testManifest(t)
	raw, digest, err := m.Encode()
	require.NoError(t, err)
	require.Equal(t, string(golden), string(raw))
	require.Equal(t, goldenDigest, digest)
	require.Equal(t, Digest(golden), digest, "the digest is the SHA-256 of exactly the stored bytes")
	require.Equal(t, testFile(t, TierDelta, 3).Key, m.Files[0].Key, "Encode sorts a copy and leaves the caller's file list alone")

	// Another file order and other map instances encode to the same bytes.
	reordered := testManifest(t)
	slices.Reverse(reordered.Files)
	reordered.Files[1].Columns = map[string]string{"name": "VARCHAR", "ltbase_row_id": "UUID", "ltbase_deleted_at": "BIGINT"}
	again, again256, err := reordered.Encode()
	require.NoError(t, err)
	require.Equal(t, raw, again)
	require.Equal(t, digest, again256)
}

// TestEncodedManifestIsWhatTheCollectorVerifies pins that what Encode writes
// passes publication.ManifestFiles, the collector's check, under the digest
// Encode answers, and that Verify reads the same document back.
func TestEncodedManifestIsWhatTheCollectorVerifies(t *testing.T) {
	m := testManifest(t)
	raw, ref := encoded(t, m)
	keys, err := publication.ManifestFiles(raw, 100, publication.Generation{
		Generation: 7, Epoch: 1, Kind: publication.KindFlush, ParentGeneration: 6,
		ManifestKey: ref.ManifestKey, ManifestSHA256: ref.ManifestSHA256, Token: testToken,
	})
	require.NoError(t, err)
	want := testManifest(t)
	want.Files = sortedFiles(want.Files)
	require.Equal(t, want.Keys(), keys)

	got, err := Verify(raw, ref)
	require.NoError(t, err)
	require.Equal(t, want, *got)
}

func TestEncodeShapesTheOptionalAndEmptyFields(t *testing.T) {
	init := testManifest(t)
	init.Kind, init.Files = publication.KindInit, nil
	raw, _, err := init.Encode()
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.JSONEq(t, `[]`, string(doc["files"]), "an empty generation lists files: [], never null")
	require.NotContains(t, doc, "input_generation")

	compact := testManifest(t)
	compact.Kind, compact.InputGeneration = publication.KindCompact, 5
	raw, _, err = compact.Encode()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.JSONEq(t, `5`, string(doc["input_generation"]))
}

func TestEncodeRefusesADocumentTheProtocolLacks(t *testing.T) {
	cases := map[string]func(*Manifest){
		"another format":               func(m *Manifest) { m.Format = "forma-manifest/1" },
		"no schema":                    func(m *Manifest) { m.SchemaID = 0 },
		"no epoch":                     func(m *Manifest) { m.Epoch = 0 },
		"no generation":                func(m *Manifest) { m.Generation = 0 },
		"a parent that is not earlier": func(m *Manifest) { m.ParentGeneration = 7 },
		"a negative parent":            func(m *Manifest) { m.ParentGeneration = -1 },
		"no lineage":                   func(m *Manifest) { m.Lineage = uuid.Nil },
		"no token":                     func(m *Manifest) { m.Token = uuid.Nil },
		"an unknown kind":              func(m *Manifest) { m.Kind = "merge" },
		"a flush with an input":        func(m *Manifest) { m.InputGeneration = 5 },
		"a compact without an input":   func(m *Manifest) { m.Kind = publication.KindCompact },
		"a compact with a later input": func(m *Manifest) { m.Kind, m.InputGeneration = publication.KindCompact, 7 },
		"an init listing a delta":      func(m *Manifest) { m.Kind = publication.KindInit },
		"a file listed twice":          func(m *Manifest) { m.Files = append(m.Files, m.Files[0]) },
		"a tier the key does not name": func(m *Manifest) { m.Files[0].Tier = TierBase },
		"a key of another schema":      func(m *Manifest) { m.Files[0].Key = testLayout.ParquetKey(101, 1, TierDelta, uuid.Must(uuid.NewV7())) },
		"a key of another epoch":       func(m *Manifest) { m.Files[0].Key = testLayout.ParquetKey(100, 2, TierDelta, uuid.Must(uuid.NewV7())) },
		"a key off the layout":         func(m *Manifest) { m.Files[0].Key = "data/100/delta/x.parquet" },
		"an empty file":                func(m *Manifest) { m.Files[0].SizeBytes = 0 },
		"no rows":                      func(m *Manifest) { m.Files[0].RowCount = 0 },
		"an uppercase digest": func(m *Manifest) {
			m.Files[0].SHA256 = "4BF5122F344554C53BDE2EBB8CD2B7E3D1600AD631C385A5D7CCE23C7785459A"
		},
		"a short digest":           func(m *Manifest) { m.Files[0].SHA256 = "abc" },
		"no row_id_min":            func(m *Manifest) { m.Files[0].RowIDMin = uuid.Nil },
		"a row range out of order": func(m *Manifest) { m.Files[0].RowIDMin, m.Files[0].RowIDMax = m.Files[0].RowIDMax, m.Files[0].RowIDMin },
		"no columns":               func(m *Manifest) { m.Files[0].Columns = nil },
		"a column without a type":  func(m *Manifest) { m.Files[0].Columns = map[string]string{"ltbase_row_id": ""} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := testManifest(t)
			mutate(&m)
			_, _, err := m.Encode()
			require.Error(t, err)
		})
	}
}
