package federated

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lychee-technology/forma/internal/federatedv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/parquetcheck"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// manifestCacheEntries bounds the harness's cache of verified manifests, as
// the factory bounds the process-wide one.
const manifestCacheEntries = 1024

// SnapshotBeginner opens the snapshot the epoch pins are read in.
// *pgxpool.Pool satisfies it.
type SnapshotBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// PublicationReader builds the product's V2 publication reader
// (federatedv2.Source) over the harness's bucket, as the factory does
// (factory/publication_reader.go): the epochs of schemaIDs pinned in one
// snapshot of db read now, manifests fetched through one verifying cache the
// harness keeps for its lifetime, and objects addressed as s3:// URIs. Hand
// it to the engine with federated.WithPublicationReader.
func (h *FederatedTestHarness) PublicationReader(ctx context.Context, db SnapshotBeginner, schemaIDs []int16) (*federatedv2.Source, error) {
	cache, err := h.manifestCache()
	if err != nil {
		return nil, err
	}
	pins, err := loadEpochPins(ctx, db, h.tables.EntityMain, schemaIDs)
	if err != nil {
		return nil, err
	}
	src, err := federatedv2.New(federatedv2.Config{
		EntityMain: h.tables.EntityMain,
		Manifests:  cache,
		Epochs:     pins,
		ObjectURI:  federatedv2.S3URI(h.S3Bucket),
		Exists:     manifestv2.S3Exists(h.s3Client, h.S3Bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("build the harness's publication reader: %w", err)
	}
	return src, nil
}

// manifestCache answers the harness's manifest cache, built once.
func (h *FederatedTestHarness) manifestCache() (*manifestv2.Cache, error) {
	h.manifestsOnce.Do(func() {
		h.manifests, h.manifestsErr = manifestv2.NewCache(h.objectStore(), manifestCacheEntries)
	})
	if h.manifestsErr != nil {
		return nil, fmt.Errorf("build the harness's manifest cache: %w", h.manifestsErr)
	}
	return h.manifests, nil
}

// loadEpochPins reads the epoch pins of schemaIDs in one REPEATABLE READ READ
// ONLY snapshot.
func loadEpochPins(ctx context.Context, db SnapshotBeginner, entityMain string, schemaIDs []int16) (map[int16]int64, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("open the snapshot the publication epochs are pinned in: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	pins, err := federatedv2.LoadEpochPins(ctx, tx, entityMain, schemaIDs)
	if err != nil {
		return nil, fmt.Errorf("pin the publication epochs of %d schemas: %w", len(schemaIDs), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("end the snapshot the publication epochs were pinned in: %w", err)
	}
	return pins, nil
}

// parquetStatsSQL counts a local parquet file's rows and reads its row id
// range in text order, the order a manifest's row_id_min and row_id_max take.
const parquetStatsSQL = `SELECT count(*), min(CAST(row_id AS VARCHAR)), max(CAST(row_id AS VARCHAR)) FROM read_parquet('%s')`

// describeParquet reads back the local parquet file at path as a manifest
// entry, without its tier and key: rows, row id range, footer columns, size
// and content digest, as the product's exporter does.
func (h *FederatedTestHarness) describeParquet(ctx context.Context, path string) (manifestv2.File, error) {
	var f manifestv2.File
	var lo, hi sql.NullString
	if err := h.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(parquetStatsSQL, sqlutil.EscapeLiteral(path))).Scan(&f.RowCount, &lo, &hi); err != nil {
		return manifestv2.File{}, fmt.Errorf("count the rows of %s: %w", path, err)
	}
	var err error
	if f.RowIDMin, err = uuid.Parse(lo.String); err != nil {
		return manifestv2.File{}, fmt.Errorf("parquet %s: least row id %q: %w", path, lo.String, err)
	}
	if f.RowIDMax, err = uuid.Parse(hi.String); err != nil {
		return manifestv2.File{}, fmt.Errorf("parquet %s: greatest row id %q: %w", path, hi.String, err)
	}
	if f.Columns, err = parquetcheck.DescribeColumns(ctx, h.Duck.DB, path); err != nil {
		return manifestv2.File{}, fmt.Errorf("read the footer columns of %s: %w", path, err)
	}
	if f.SizeBytes, f.SHA256, err = digestFile(path); err != nil {
		return manifestv2.File{}, err
	}
	return f, nil
}

// digestFile answers the size of the file at path and the SHA-256 of its
// bytes as 64 lowercase hex digits.
func digestFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", fmt.Errorf("hash %s: %w", path, err)
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}
