package cdc

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/parquetcheck"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// An export writes its parquet object to a local scratch file first, and
// only a complete file is staged: describeExport reads back what it holds
// (rows, row id range, footer columns, size, content digest) and
// stageObject writes it once to its key with a conditional put
// (manifestv2.Store), so no key is ever overwritten and no object of the
// layout is ever partly written (docs/publication-protocol-v2.md,
// "Immutable manifest generations"). The digest is of the exact bytes the put
// sends.

// scratchFile is the local file one export writes, in a directory of its
// own that remove deletes.
type scratchFile struct {
	dir  string
	path string
}

func newScratchFile() (scratchFile, error) {
	dir, err := os.MkdirTemp("", "forma-cdc-export-")
	if err != nil {
		return scratchFile{}, fmt.Errorf("create the export scratch directory: %w", err)
	}
	return scratchFile{dir: dir, path: filepath.Join(dir, "export.parquet")}, nil
}

func (s scratchFile) remove() { _ = os.RemoveAll(s.dir) }

// exportedObject is what an export's parquet file holds.
type exportedObject struct {
	path      string
	sizeBytes int64
	sha256    string
	rowCount  int64
	// rowIDMin and rowIDMax are the least and greatest row ids in the order
	// of their canonical text, the order the manifest's row_id_min and
	// row_id_max take (manifestv2.File).
	rowIDMin, rowIDMax uuid.UUID
	columns            map[string]string
}

// statsSQL counts an exported file's rows and reads its row id range in text
// order.
const statsSQL = `SELECT count(*), min(CAST(row_id AS VARCHAR)), max(CAST(row_id AS VARCHAR)) FROM read_parquet('%s')`

// describeExport reads back the parquet file at path. A file with no row
// answers rowCount 0 and nothing else: it is never staged.
func (e *DuckExporter) describeExport(ctx context.Context, path string) (exportedObject, error) {
	obj := exportedObject{path: path}
	var lo, hi sql.NullString
	if err := e.DB.QueryRowContext(ctx, fmt.Sprintf(statsSQL, sqlutil.EscapeLiteral(path))).Scan(&obj.rowCount, &lo, &hi); err != nil {
		return exportedObject{}, fmt.Errorf("count the rows of the exported file %s: %w", path, err)
	}
	if obj.rowCount == 0 {
		return obj, nil
	}
	var err error
	if obj.rowIDMin, err = uuid.Parse(lo.String); err != nil {
		return exportedObject{}, fmt.Errorf("exported file %s: least row id %q: %w", path, lo.String, err)
	}
	if obj.rowIDMax, err = uuid.Parse(hi.String); err != nil {
		return exportedObject{}, fmt.Errorf("exported file %s: greatest row id %q: %w", path, hi.String, err)
	}
	if obj.columns, err = parquetcheck.DescribeColumns(ctx, e.DB, path); err != nil {
		return exportedObject{}, fmt.Errorf("read the footer columns of the exported file: %w", err)
	}
	if len(obj.columns) == 0 {
		return exportedObject{}, fmt.Errorf("exported file %s has no column in its footer", path)
	}
	if obj.sizeBytes, obj.sha256, err = digestFile(path); err != nil {
		return exportedObject{}, err
	}
	return obj, nil
}

// digestFile answers the size of the file at path and the SHA-256 of its
// bytes as 64 lowercase hex digits.
func digestFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", fmt.Errorf("open the exported file %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", fmt.Errorf("hash the exported file %s: %w", path, err)
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// stageObject writes obj once to key and answers its manifest entry. A key
// that already holds an object fails with manifestv2.ErrKeyExists and is left
// as it is; keys are fresh version 7 UUIDs, so the writer abandons the
// staging rather than retry the put as an overwrite.
func stageObject(ctx context.Context, store manifestv2.Store, key string, tier manifestv2.Tier, obj exportedObject) (manifestv2.File, error) {
	f, err := os.Open(obj.path)
	if err != nil {
		return manifestv2.File{}, fmt.Errorf("open the exported file %s: %w", obj.path, err)
	}
	defer f.Close()
	if err := store.Put(ctx, key, f); err != nil {
		return manifestv2.File{}, fmt.Errorf("stage the %s object %s: %w", tier, key, err)
	}
	return manifestv2.File{
		Tier: tier, Key: key, SizeBytes: obj.sizeBytes, SHA256: obj.sha256, RowCount: obj.rowCount,
		RowIDMin: obj.rowIDMin, RowIDMax: obj.rowIDMax, Columns: obj.columns,
	}, nil
}
