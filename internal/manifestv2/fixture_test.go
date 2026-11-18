package manifestv2

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/stretchr/testify/require"
)

var (
	testLayout  = Layout{Prefix: "data"}
	testLineage = uuid.MustParse("0190a000-0000-7000-8000-00000000000a")
	testToken   = uuid.MustParse("0190a000-0000-7000-8000-00000000000b")
)

// testFile is a valid file of schema 100 epoch 1, named by the n-th fixed
// uuidv7, so its key and the bytes it encodes to never change.
func testFile(t *testing.T, tier Tier, n byte) File {
	t.Helper()
	id := uuid.MustParse("0190a000-0000-7000-8000-0000000000" + hexByte(n))
	return File{
		Tier: tier, Key: testLayout.ParquetKey(100, 1, tier, id), SizeBytes: 1024 * int64(n),
		SHA256:   Digest([]byte{n}),
		RowCount: 10 * int64(n),
		RowIDMin: uuid.MustParse("0190b000-0000-7000-8000-0000000000" + hexByte(n)),
		RowIDMax: uuid.MustParse("0190b000-0000-7000-8000-0000000001" + hexByte(n)),
		Columns:  map[string]string{"ltbase_row_id": "UUID", "ltbase_deleted_at": "BIGINT", "name": "VARCHAR"},
	}
}

func hexByte(n byte) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[n>>4], digits[n&0xf]})
}

// testManifest is generation 7 of schema 100, a flush over generation 6,
// listing a base file and two delta files out of key order.
func testManifest(t *testing.T) Manifest {
	t.Helper()
	return Manifest{
		Format: publication.ManifestFormat, SchemaID: 100, Epoch: 1, Generation: 7, ParentGeneration: 6,
		Lineage: testLineage, Token: testToken, Kind: publication.KindFlush,
		Files: []File{testFile(t, TierDelta, 3), testFile(t, TierBase, 1), testFile(t, TierDelta, 2)},
	}
}

// encoded answers m's bytes and the Ref PostgreSQL would hold for them.
func encoded(t *testing.T, m Manifest) ([]byte, Ref) {
	t.Helper()
	raw, digest, err := m.Encode()
	require.NoError(t, err)
	return raw, Ref{
		SchemaID: m.SchemaID, Epoch: m.Epoch, Generation: m.Generation, ParentGeneration: m.ParentGeneration,
		Token: m.Token, Lineage: m.Lineage, Kind: m.Kind,
		ManifestKey: testLayout.ManifestKey(m.SchemaID, m.Epoch, m.Generation, m.Token), ManifestSHA256: digest,
	}
}
