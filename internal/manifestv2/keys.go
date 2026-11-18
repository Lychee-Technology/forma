package manifestv2

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Tier is the parquet tier a file belongs to.
type Tier string

const (
	TierBase  Tier = "base"
	TierDelta Tier = "delta"
)

// manifestsDir is the directory, under a schema's prefix, that holds its
// manifest objects. A parquet key never contains it: the segment after the
// schema id is the epoch there.
const manifestsDir = "_manifests"

// Layout builds the object keys of one data prefix
// (docs/publication-protocol-v2.md, "Object keys"):
//
//	{prefix}/{schema_id}/_manifests/e{epoch}/g{generation:020d}-{token}.json
//	{prefix}/{schema_id}/e{epoch}/{base|delta}/{uuidv7}.parquet
//
// A trailing slash of Prefix is dropped, and an empty Prefix yields a key that
// starts with "/", as the V1 key builders do (internal/cdc). Correctness never
// depends on the layout, since a reader fetches the one key the record names
// and scans the keys its manifest lists. Verify still refuses a key that does
// not spell the identity it belongs to, which catches a manifest that lists
// another schema's or another epoch's object.
type Layout struct {
	Prefix string
}

func (l Layout) root() string { return strings.TrimSuffix(l.Prefix, "/") }

// ManifestKey is the key of a generation's manifest object. The token is in
// the key, so two writers that stage the same generation number never write
// the same object.
func (l Layout) ManifestKey(schemaID int16, epoch, generation int64, token uuid.UUID) string {
	return fmt.Sprintf("%s/%d/%s/e%d/g%020d-%s.json", l.root(), schemaID, manifestsDir, epoch, generation, token)
}

// ParquetKey is the key of a parquet object of the tier, named by id, which
// must be a version 7 UUID (NewParquetKey mints one).
func (l Layout) ParquetKey(schemaID int16, epoch int64, tier Tier, id uuid.UUID) string {
	return fmt.Sprintf("%s/%d/e%d/%s/%s.parquet", l.root(), schemaID, epoch, tier, id)
}

// NewParquetKey mints a fresh version 7 UUID and answers the parquet key it
// names. The UUID is never reused, so the key is one nobody has written.
func (l Layout) NewParquetKey(schemaID int16, epoch int64, tier Tier) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("mint the uuidv7 of a %s parquet key of schema %d epoch %d: %w", tier, schemaID, epoch, err)
	}
	return l.ParquetKey(schemaID, epoch, tier, id), nil
}

// manifestKeyParts is what a manifest key spells.
type manifestKeyParts struct {
	layout     Layout
	schemaID   int16
	epoch      int64
	generation int64
	token      uuid.UUID
}

// parseManifestKey reads a manifest key back. It accepts only the exact
// spelling ManifestKey builds (zero-padded generation, canonical token), so
// one identity has one key.
func parseManifestKey(key string) (manifestKeyParts, error) {
	parts := strings.Split(key, "/")
	n := len(parts)
	if n < 5 || parts[n-3] != manifestsDir {
		return manifestKeyParts{}, fmt.Errorf("manifest key %q is not {prefix}/{schema_id}/%s/e{epoch}/g{generation}-{token}.json", key, manifestsDir)
	}
	var p manifestKeyParts
	var err error
	p.layout = Layout{Prefix: strings.Join(parts[:n-4], "/")}
	if p.schemaID, err = parseSchemaID(parts[n-4]); err != nil {
		return manifestKeyParts{}, fmt.Errorf("manifest key %q: %w", key, err)
	}
	if p.epoch, err = parseEpoch(parts[n-2]); err != nil {
		return manifestKeyParts{}, fmt.Errorf("manifest key %q: %w", key, err)
	}
	name, ok := strings.CutSuffix(parts[n-1], ".json")
	gen, tok, cut := strings.Cut(strings.TrimPrefix(name, "g"), "-")
	if !ok || !cut || !strings.HasPrefix(name, "g") {
		return manifestKeyParts{}, fmt.Errorf("manifest key %q does not end in g{generation}-{token}.json", key)
	}
	if p.generation, err = strconv.ParseInt(gen, 10, 64); err != nil {
		return manifestKeyParts{}, fmt.Errorf("manifest key %q names generation %q: %w", key, gen, err)
	}
	if p.token, err = uuid.Parse(tok); err != nil {
		return manifestKeyParts{}, fmt.Errorf("manifest key %q names token %q: %w", key, tok, err)
	}
	if canonical := p.layout.ManifestKey(p.schemaID, p.epoch, p.generation, p.token); canonical != key {
		return manifestKeyParts{}, fmt.Errorf("manifest key %q is not spelled as its layout spells it, %q", key, canonical)
	}
	return p, nil
}

// parquetKeyParts is what a parquet key spells.
type parquetKeyParts struct {
	layout   Layout
	schemaID int16
	epoch    int64
	tier     Tier
	id       uuid.UUID
}

// parseParquetKey reads a parquet key back, accepting only the exact spelling
// ParquetKey builds over a version 7 UUID.
func parseParquetKey(key string) (parquetKeyParts, error) {
	parts := strings.Split(key, "/")
	n := len(parts)
	if n < 5 {
		return parquetKeyParts{}, fmt.Errorf("parquet key %q is not {prefix}/{schema_id}/e{epoch}/{tier}/{uuidv7}.parquet", key)
	}
	var p parquetKeyParts
	var err error
	p.layout = Layout{Prefix: strings.Join(parts[:n-4], "/")}
	if p.schemaID, err = parseSchemaID(parts[n-4]); err != nil {
		return parquetKeyParts{}, fmt.Errorf("parquet key %q: %w", key, err)
	}
	if p.epoch, err = parseEpoch(parts[n-3]); err != nil {
		return parquetKeyParts{}, fmt.Errorf("parquet key %q: %w", key, err)
	}
	p.tier = Tier(parts[n-2])
	if p.tier != TierBase && p.tier != TierDelta {
		return parquetKeyParts{}, fmt.Errorf("parquet key %q names tier %q, want %s or %s", key, p.tier, TierBase, TierDelta)
	}
	name, ok := strings.CutSuffix(parts[n-1], ".parquet")
	if !ok {
		return parquetKeyParts{}, fmt.Errorf("parquet key %q does not end in .parquet", key)
	}
	if p.id, err = uuid.Parse(name); err != nil || p.id.Version() != 7 {
		return parquetKeyParts{}, fmt.Errorf("parquet key %q is not named by a version 7 UUID", key)
	}
	if canonical := p.layout.ParquetKey(p.schemaID, p.epoch, p.tier, p.id); canonical != key {
		return parquetKeyParts{}, fmt.Errorf("parquet key %q is not spelled as its layout spells it, %q", key, canonical)
	}
	return p, nil
}

func parseSchemaID(s string) (int16, error) {
	id, err := strconv.ParseInt(s, 10, 16)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("schema id segment %q is not a positive SMALLINT", s)
	}
	return int16(id), nil
}

func parseEpoch(s string) (int64, error) {
	digits, ok := strings.CutPrefix(s, "e")
	epoch, err := strconv.ParseInt(digits, 10, 64)
	if !ok || err != nil || epoch <= 0 {
		return 0, fmt.Errorf("epoch segment %q is not e{epoch} with a positive epoch", s)
	}
	return epoch, nil
}
