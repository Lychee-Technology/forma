package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"go.uber.org/zap"
)

// GetChangeLogStats returns count and oldest changed_at for unflushed rows.
func GetChangeLogStats(ctx context.Context, db *sql.DB, table string, schemaID int16) (int64, int64, error) {
	if table == "" {
		table = "change_log"
	}
	query := fmt.Sprintf("SELECT COUNT(*), COALESCE(MIN(changed_at),0) FROM %s WHERE schema_id = $1 AND flushed_at = 0", sanitizeIdentifier(table))
	row := db.QueryRowContext(ctx, query, schemaID)
	var cnt int64
	var oldest int64
	if err := row.Scan(&cnt, &oldest); err != nil {
		return 0, 0, fmt.Errorf("get stats: %w", err)
	}
	return cnt, oldest, nil
}

// CopyTmpToFinal copies a parquet file from tmp key to final key and deletes tmp.
//
// CopySource names the tmp key as the exporter wrote it (#516): the object
// sits at s3://<bucket>/<tmpKey> byte for byte, so an empty data prefix yields
// a tmp key of "/1/_tmp/<uuid>.parquet" and a copy source of
// "<bucket>//1/_tmp/<uuid>.parquet". Trimming the leading slash here would
// name the sibling key "1/_tmp/..." that was never written and fail every
// promotion under an empty prefix.
//
// The header value is percent-encoded (encodeCopySourceKey): S3 URL-decodes
// x-amz-copy-source, and the SDK sets the header verbatim, so a key holding a
// reserved byte ("?", "#", "%", "+", whitespace, non-ASCII) would otherwise be
// misread or rejected. The encoding leaves "/" and unreserved characters
// alone, so the logical key, leading slash included, is unchanged, and every
// key the Build*Path helpers mint under an ordinary prefix encodes to itself.
// The destination Key is a plain SDK field and stays unencoded.
func CopyTmpToFinal(ctx context.Context, client S3ObjectClient, bucket, tmpKey, finalKey string, logger *zap.Logger) error {
	if client == nil {
		return fmt.Errorf("s3 client is nil")
	}
	src := bucket + "/" + encodeCopySourceKey(tmpKey)
	if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     &bucket,
		CopySource: &src,
		Key:        &finalKey,
	}); err != nil {
		// #226: the retry uses fresh UUIDv7 keys, so this tmp would be
		// unreachable garbage — best-effort delete before surfacing the copy
		// error. A failed cleanup is reclaimed by manifest-reconcile --gc.
		if _, delErr := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &tmpKey}); delErr != nil {
			logger.Sugar().Warnw("failed to delete tmp object after copy failure", "tmp_key", tmpKey, "err", delErr)
		}
		return fmt.Errorf("copy tmp->final: %w", err)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &tmpKey}); err != nil {
		logger.Sugar().Warnw("failed to delete tmp object", "err", err)
	}
	return nil
}

// encodeCopySourceKey percent-encodes an object key for the x-amz-copy-source
// header. S3 URL-decodes the header, so the key must be encoded on the way in
// (AWS SDK: "The value must be URL-encoded"). Unreserved characters (RFC 3986
// §2.3: ALPHA, DIGIT, "-", ".", "_", "~") and "/" pass through, which keeps an
// ordinary key byte-identical and a leading slash intact; every other byte is
// escaped as %XX. That deliberately covers "?" (S3 would read the rest as a
// versionId query), "+" (decoded as a space), "%" (decoded as an escape), and
// whitespace and non-ASCII bytes (invalid header content). url.PathEscape is
// not used because it escapes "/" and leaves "+" and other sub-delims alone.
func encodeCopySourceKey(key string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(key))
	for i := 0; i < len(key); i++ {
		c := key[i]
		if isCopySourceSafe(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0x0f])
	}
	return b.String()
}

func isCopySourceSafe(c byte) bool {
	switch {
	case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9':
		return true
	case c == '-', c == '.', c == '_', c == '~', c == '/':
		return true
	}
	return false
}

// DeleteObjectKey deletes one S3 object.
func DeleteObjectKey(ctx context.Context, client S3ObjectClient, bucket, key string) error {
	if client == nil {
		return fmt.Errorf("s3 client is nil")
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		return fmt.Errorf("delete object %s: %w", key, err)
	}
	return nil
}

// HeadObjectSize returns the byte size of an S3 object. Callers use it to
// populate manifest FileEntry.SizeBytes after a tmp->final copy; the size
// feeds compaction's promotion heuristic only, so callers should treat a
// failure as best-effort (log and keep 0) rather than failing the pipeline.
func HeadObjectSize(ctx context.Context, client S3ObjectClient, bucket, key string) (int64, error) {
	if client == nil {
		return 0, fmt.Errorf("s3 client is nil")
	}
	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return 0, fmt.Errorf("head object %s: %w", key, err)
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

// sanitizeIdentifier performs a minimal whitelist for table names.
func sanitizeIdentifier(name string) string {
	return sqlutil.SanitizeIdentifier(name)
}
