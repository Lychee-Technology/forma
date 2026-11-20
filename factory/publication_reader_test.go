package factory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// stubS3Client fails every call and counts them: a reader that answers
// through it touched no storage.
type stubS3Client struct {
	mu    sync.Mutex
	calls int
}

func (c *stubS3Client) called() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *stubS3Client) fail(op string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return errors.New("stubS3Client." + op + " must not be called")
}

func (c *stubS3Client) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, c.fail("GetObject")
}

func (c *stubS3Client) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, c.fail("PutObject")
}

func (c *stubS3Client) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, c.fail("HeadObject")
}

// swapPublicationS3Client replaces the S3 client seam for the duration of a
// test.
func swapPublicationS3Client(t *testing.T, fn func(context.Context, forma.DuckDBConfig) (publicationS3Client, error)) {
	t.Helper()
	original := newPublicationS3Client
	newPublicationS3Client = fn
	t.Cleanup(func() { newPublicationS3Client = original })
}

// fakeReader stands in for the publication reader in the construction tests,
// which never run a pass.
type fakeReader struct{ federated.PublicationReader }

// duckDBReadConfig is a DuckDB configuration the factory accepts.
func duckDBReadConfig() forma.DuckDBConfig {
	return forma.DuckDBConfig{Enabled: true, DBPath: ":memory:", MaxConnections: 1, QueryTimeout: 30 * time.Second, S3Bucket: "bkt"}
}

// With DuckDB disabled no reader is built: the engine routes every query to
// PostgreSQL and runs no pass.
func TestNewEntityManagerWithConfig_Unit_NoPublicationReaderWhenDuckDBOff(t *testing.T) {
	t.Parallel()
	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	deps.newPublicationReader = func(context.Context, publicationReaderSpec) (federated.PublicationReader, error) {
		t.Fatal("the publication reader must not be built with DuckDB disabled")
		return nil, nil
	}

	em, err := newEntityManagerWithConfigContext(context.Background(), newUnitEntityManagerConfig(t), nil, deps)
	require.NoError(t, err)
	require.NotNil(t, em)
}

// With DuckDB enabled the reader is built from the DuckDB configuration, the
// loaded metadata and the schema-qualified entity_main table, whose name the
// publication tables derive from.
func TestNewEntityManagerWithConfig_Unit_PublicationReaderFromConfig(t *testing.T) {
	t.Parallel()
	meta := schemameta.NewMetadataCache()
	require.NoError(t, meta.RegisterSchema("orders", 7, forma.SchemaAttributeCache{}))
	deps := buildUnitEntityManagerDeps(meta)
	var got publicationReaderSpec
	deps.newDuckDBClient = func(context.Context, forma.DuckDBConfig) (*federated.DuckDBClient, error) { return nil, nil }
	deps.newPublicationReader = func(_ context.Context, spec publicationReaderSpec) (federated.PublicationReader, error) {
		got = spec
		return fakeReader{}, nil
	}
	config := newUnitEntityManagerConfig(t)
	config.DuckDB = duckDBReadConfig()

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)
	require.NoError(t, err)
	require.NotNil(t, em)
	assert.Equal(t, "bkt", got.duck.S3Bucket)
	assert.Equal(t, "public.entity_main", got.entityMain)
	assert.Same(t, meta, got.meta)
	assert.Nil(t, got.pool, "a nil pool reaches the reader as a nil interface")
}

// A reader that cannot be built fails startup, and the DuckDB client opened
// before it is closed rather than leaked (#250 PR review P2c).
func TestNewEntityManagerWithConfig_Unit_PublicationReaderErrorClosesDuckDBClient(t *testing.T) {
	t.Parallel()
	var built *federated.DuckDBClient
	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	deps.newDuckDBClient = func(ctx context.Context, cfg forma.DuckDBConfig) (*federated.DuckDBClient, error) {
		client, err := federated.NewDuckDBClientContext(ctx, cfg)
		built = client
		return client, err
	}
	deps.newPublicationReader = func(context.Context, publicationReaderSpec) (federated.PublicationReader, error) {
		return nil, errors.New("simulated pin failure")
	}
	config := newUnitEntityManagerConfig(t)
	config.DuckDB = duckDBReadConfig()

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)
	assert.Nil(t, em)
	require.ErrorContains(t, err, "simulated pin failure")
	require.ErrorContains(t, err, "publication reader")
	require.NotNil(t, built, "the test needs a real DuckDB client to observe")
	healthErr := built.HealthCheck(context.Background())
	require.Error(t, healthErr, "the duckdb client must be closed before the fatal return")
	assert.Contains(t, healthErr.Error(), "database is closed")
}

// DuckDB enabled without a bucket is refused before any I/O: every published
// generation the reader would scan is addressed in it.
func TestNewEntityManagerWithConfig_Unit_DuckDBWithoutBucketFailsBeforeIO(t *testing.T) {
	t.Parallel()
	for name, bucket := range map[string]string{"empty": "", "padded": " bkt"} {
		t.Run(name, func(t *testing.T) {
			deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
			deps.collectTables = func(context.Context, queryPool, string) ([]string, error) {
				t.Fatal("collectTables must not run before the parquet read configuration is validated")
				return nil, nil
			}
			config := newUnitEntityManagerConfig(t)
			config.DuckDB = duckDBReadConfig()
			config.DuckDB.S3Bucket = bucket

			em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)
			assert.Nil(t, em)
			require.ErrorContains(t, err, "invalid duckdb parquet read configuration")
			var cfgErr *forma.ConfigError
			require.ErrorAs(t, err, &cfgErr)
			assert.Equal(t, "duckdb.s3Bucket", cfgErr.Field)
		})
	}
}

// failingSnapshotPool refuses to open a snapshot.
type failingSnapshotPool struct{ err error }

func (p failingSnapshotPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, p.err
}

// The production reader refuses a missing pool, missing registry epochs, a
// snapshot it cannot open and an S3 client it cannot build, each naming the
// cause.
func TestNewPublicationReaderFailures(t *testing.T) {
	ctx := context.Background()
	_, err := newPublicationReader(ctx, publicationReaderSpec{duck: duckDBReadConfig(), entityMain: "entity_main"})
	require.ErrorContains(t, err, "needs a database pool")

	_, err = newPublicationReader(ctx, publicationReaderSpec{duck: duckDBReadConfig(), entityMain: "entity_main", pool: noSchemaPool{}})
	require.ErrorIs(t, err, ErrSchemaRegistryEpochsRequired, "no epochs read before the caller's registry loaded")

	none := map[int16]int64{}
	beginErr := errors.New("connection refused")
	_, err = newPublicationReader(ctx, publicationReaderSpec{duck: duckDBReadConfig(), entityMain: "entity_main", pool: failingSnapshotPool{beginErr}, registryEpochs: none})
	require.ErrorIs(t, err, beginErr)
	require.ErrorContains(t, err, "pinned")

	clientErr := errors.New("no credentials")
	swapPublicationS3Client(t, func(context.Context, forma.DuckDBConfig) (publicationS3Client, error) { return nil, clientErr })
	// With no schema to pin, only the S3 client seam can fail the reader.
	_, err = newPublicationReader(ctx, publicationReaderSpec{
		duck: duckDBReadConfig(), entityMain: "entity_main", pool: noSchemaPool{}, meta: schemameta.NewMetadataCache(), registryEpochs: none,
	})
	require.ErrorIs(t, err, clientErr)
}

// noSchemaPool opens a transaction that is never asked anything: with no
// schema to pin, loadEpochPins only begins and commits it.
type noSchemaPool struct{}

func (noSchemaPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return idleTx{}, nil }

// idleTx commits and rolls back, and panics on anything else.
type idleTx struct{ pgx.Tx }

func (idleTx) Commit(context.Context) error   { return nil }
func (idleTx) Rollback(context.Context) error { return nil }

func TestRegisteredSchemaIDsAreSorted(t *testing.T) {
	meta := schemameta.NewMetadataCache()
	for name, id := range map[string]int16{"c": 9, "a": 3, "b": 5} {
		require.NoError(t, meta.RegisterSchema(name, id, forma.SchemaAttributeCache{}))
	}
	assert.Equal(t, []int16{3, 5, 9}, registeredSchemaIDs(meta))
	assert.Nil(t, registeredSchemaIDs(nil))
}
