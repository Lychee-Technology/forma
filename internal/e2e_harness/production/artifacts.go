package production

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// maxParquetArtifacts caps how many parquet files get schema+sample dumps.
const maxParquetArtifacts = 20

// maxSampleRows caps sampled rows per parquet file.
const maxSampleRows = 20

// ArtifactsDir returns the run-specific artifact directory:
// <root>/.artifacts/e2e/<cluster.RunID>/<testName>/ where <root> is the
// repository root (or E2E_ARTIFACTS_DIR when set).
func (e *Env) ArtifactsDir() string {
	base := os.Getenv("E2E_ARTIFACTS_DIR")
	if base == "" {
		base = filepath.Join(repoRoot(), ".artifacts", "e2e")
	}
	return filepath.Join(base, e.Cluster.RunID, sanitizeName(e.T.Name()))
}

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	// internal/e2e_harness/production/artifacts.go -> repo root is 3 up.
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

// DumpArtifacts writes every diagnostic artifact for the Env and returns
// the directory path. It is called automatically when a test fails (or
// KEEP_E2E_ENV=1), before resource teardown; tests may also call it
// directly.
func (e *Env) DumpArtifacts(ctx context.Context) (string, error) {
	dir := e.ArtifactsDir()
	// Artifacts can reference external infrastructure details; keep them
	// readable by the invoking user only.
	if err := os.MkdirAll(filepath.Join(dir, "parquet"), 0o700); err != nil {
		return "", fmt.Errorf("create artifacts dir: %w", err)
	}

	steps := []struct {
		name string
		fn   func(context.Context, string) error
	}{
		{"run.json", e.dumpRunInfo},
		{"events.json", e.dumpEvents},
		{"change_log.json", e.dumpChangeLog},
		{"s3_listing.json", e.dumpS3Listing},
		{"manifests", e.dumpManifests},
		{"parquet", e.dumpParquet},
		{"queries", e.dumpQueries},
		{"diff.json", e.dumpDiff},
	}
	var errs []string
	for _, step := range steps {
		if err := step.fn(ctx, dir); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", step.name, err))
		}
	}
	if len(errs) > 0 {
		return dir, fmt.Errorf("artifact dump incomplete: %s", strings.Join(errs, "; "))
	}
	return dir, nil
}

// registerArtifactDump arranges the automatic dump. It must be registered
// AFTER resource teardown (t.Cleanup is LIFO) so the dump still sees the
// live database and containers.
func (e *Env) registerArtifactDump() {
	e.T.Cleanup(func() {
		if !e.T.Failed() && !KeepEnv() {
			return
		}
		dir, err := e.DumpArtifacts(context.Background())
		if err != nil {
			e.T.Logf("diagnostic artifact dump incomplete (%v); partial output in %s", err, dir)
			return
		}
		e.T.Logf("diagnostic artifacts: %s", dir)
	})
}

func writeJSONArtifact(dir, name string, payload any) error {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}

// unavailableArtifact is written in place of an artifact whose source
// resource was never provisioned, keeping the artifact set uniform.
func unavailableArtifact(reason string) map[string]any {
	return map[string]any{"unavailable": reason}
}

func (e *Env) dumpRunInfo(ctx context.Context, dir string) error {
	info := map[string]any{
		"run_id":       e.RunID,
		"seed":         e.Seed,
		"cluster_seed": e.Cluster.Seed,
		"test":         e.T.Name(),
		"database":     e.DBName,
		"pg_dsn":       e.redactedPGDSN(),
		"s3_bucket":    e.Cluster.Bucket,
		"s3_prefix":    e.S3Prefix,
		"s3_endpoint":  e.Cluster.S3Endpoint,
		"parquet_glob": e.ParquetGlob(),
		"git_sha":      gitSHA(ctx),
		"dumped_at":    time.Now().UTC().Format(time.RFC3339),
	}
	if e.provisionErr != nil {
		info["provision_error"] = e.provisionErr.Error()
	}
	return writeJSONArtifact(dir, "run.json", info)
}

func gitSHA(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func (e *Env) dumpEvents(_ context.Context, dir string) error {
	return writeJSONArtifact(dir, "events.json", e.events)
}

func (e *Env) dumpChangeLog(ctx context.Context, dir string) error {
	if e.Pool == nil {
		return writeJSONArtifact(dir, "change_log.json", unavailableArtifact("database was not provisioned"))
	}
	rows, err := e.Pool.Query(ctx,
		`SELECT schema_id, row_id, changed_at, COALESCE(deleted_at, 0), flushed_at
		 FROM change_log ORDER BY schema_id, row_id, flushed_at`)
	if err != nil {
		return fmt.Errorf("query change_log: %w", err)
	}
	defer rows.Close()

	var entries []map[string]any
	for rows.Next() {
		var schemaID int16
		var rowID [16]byte
		var changedAt, deletedAt, flushedAt int64
		if err := rows.Scan(&schemaID, &rowID, &changedAt, &deletedAt, &flushedAt); err != nil {
			return fmt.Errorf("scan change_log row: %w", err)
		}
		entries = append(entries, map[string]any{
			"schema_id":  schemaID,
			"row_id":     uuidString(rowID),
			"changed_at": changedAt,
			"deleted_at": deletedAt,
			"flushed_at": flushedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate change_log: %w", err)
	}
	return writeJSONArtifact(dir, "change_log.json", entries)
}

func uuidString(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (e *Env) dumpS3Listing(ctx context.Context, dir string) error {
	var listing []map[string]any
	var token *string
	for {
		out, err := e.Cluster.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(e.Cluster.Bucket),
			Prefix:            aws.String(e.S3Prefix + "/"),
			ContinuationToken: token,
		})
		if err != nil {
			return fmt.Errorf("list s3 objects: %w", err)
		}
		for _, obj := range out.Contents {
			entry := map[string]any{
				"key":  aws.ToString(obj.Key),
				"size": aws.ToInt64(obj.Size),
				"etag": aws.ToString(obj.ETag),
			}
			if obj.LastModified != nil {
				entry["last_modified"] = obj.LastModified.UTC().Format(time.RFC3339)
			}
			listing = append(listing, entry)
		}
		if out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return writeJSONArtifact(dir, "s3_listing.json", listing)
}

// dumpManifests stores the raw (unparsed) manifest bytes per schema.
func (e *Env) dumpManifests(ctx context.Context, dir string) error {
	if e.CDC.ManifestTemplate == "" {
		return nil
	}
	for _, ref := range DefaultSchemaFixtures() {
		key := fmt.Sprintf("%s/manifest/%d.json", e.S3Prefix, ref.ID)
		out, err := e.Cluster.S3.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(e.Cluster.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			continue // schema has no manifest yet
		}
		data, err := io.ReadAll(out.Body)
		_ = out.Body.Close()
		if err != nil {
			return fmt.Errorf("read manifest %s: %w", key, err)
		}
		name := fmt.Sprintf("manifest_%d.json", ref.ID)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	return nil
}

// dumpParquet writes DESCRIBE output and up to maxSampleRows sampled rows
// for every parquet object under the prefix (capped).
func (e *Env) dumpParquet(ctx context.Context, dir string) error {
	if e.Duck == nil {
		return nil // DuckDB was not provisioned; nothing to describe with.
	}
	keys, err := e.listS3Keys(ctx)
	if err != nil {
		return err
	}
	dumped := 0
	var errs []string
	for _, key := range keys {
		if !strings.HasSuffix(key, ".parquet") || strings.Contains(key, "/_tmp/") {
			continue
		}
		if dumped >= maxParquetArtifacts {
			break
		}
		if err := e.dumpOneParquet(ctx, dir, key); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", key, err))
			continue
		}
		dumped++
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func (e *Env) dumpOneParquet(ctx context.Context, dir, key string) error {
	path := fmt.Sprintf("s3://%s/%s", e.Cluster.Bucket, key)
	base := filepath.Join(dir, "parquet", sanitizeName(strings.TrimPrefix(key, e.S3Prefix+"/")))

	schema, err := e.duckRowsToMaps(ctx, fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", sqlutil.EscapeLiteral(path)), 0)
	if err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	if err := writeJSONArtifact(filepath.Dir(base), filepath.Base(base)+".schema.json", schema); err != nil {
		return err
	}

	sample, err := e.duckRowsToMaps(ctx, fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT %d", sqlutil.EscapeLiteral(path), maxSampleRows), maxSampleRows)
	if err != nil {
		return fmt.Errorf("sample: %w", err)
	}
	return writeJSONArtifact(filepath.Dir(base), filepath.Base(base)+".sample.json", sample)
}

// duckRowsToMaps runs a DuckDB query and renders rows as generic maps with
// JSON-safe values.
func (e *Env) duckRowsToMaps(ctx context.Context, query string, capRows int) ([]map[string]any, error) {
	rows, err := e.Duck.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("duckdb query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	var out []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = jsonSafeValue(values[i])
		}
		out = append(out, row)
		if capRows > 0 && len(out) >= capRows {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

func jsonSafeValue(v any) any {
	switch val := v.(type) {
	case nil:
		return nil
	case []byte:
		if len(val) == 16 {
			var b [16]byte
			copy(b[:], val)
			return uuidString(b)
		}
		return string(val)
	case time.Time:
		return val.UTC().Format(time.RFC3339Nano)
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}

// dumpQueries writes one query_<n>.json per executed Query: the spec, the
// translated FederatedAttributeQuery, the full execution plan (SQL, params,
// routing, timings), and the returned row IDs.
func (e *Env) dumpQueries(_ context.Context, dir string) error {
	for i, q := range e.queries {
		rowIDs := make([]string, 0, len(q.Records))
		for _, rec := range q.Records {
			rowIDs = append(rowIDs, rec.RowID.String())
		}
		payload := map[string]any{
			"spec":            q.Spec,
			"federated_query": q.FQ,
			"plan":            q.Plan,
			"total":           q.Total,
			"row_ids":         rowIDs,
		}
		if err := writeJSONArtifact(dir, fmt.Sprintf("query_%d.json", i+1), payload); err != nil {
			return err
		}
	}
	return nil
}

// dumpDiff always writes diff.json so the artifact contract is uniform:
// failures that never reached a query assertion (init/flush/provisioning)
// still produce a structured document instead of a missing file.
func (e *Env) dumpDiff(_ context.Context, dir string) error {
	if e.lastDiff == nil {
		return writeJSONArtifact(dir, "diff.json", map[string]any{
			"status":           "no_query_mismatch_recorded",
			"note":             "the failure happened outside AssertQueryMatches (e.g. provisioning, init, or flush); see run.json and the test log",
			"queries_executed": len(e.queries),
		})
	}
	return writeJSONArtifact(dir, "diff.json", e.lastDiff)
}
