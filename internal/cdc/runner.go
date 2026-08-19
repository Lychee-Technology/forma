package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

var newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client {
	if endpoint != "" {
		return s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = &endpoint
			o.UsePathStyle = usePath
		})
	}
	return s3.NewFromConfig(cfg)
}

var newDuckExporterFn = NewDuckExporter

// Runner caches per-config S3 runtimes and DuckDB exporters across RunOnce
// calls. Each cache holds one entry per non-credential config group; a
// credential rotation (#329) replaces the group's entry, and a superseded
// DuckDB exporter is closed once no in-flight RunOnce holds it (#331) — so a
// long-lived Runner under STS rotation keeps exactly one exporter per group,
// not one per token. Two configs sharing a group but alternating *different*
// static credential triples therefore rebuild on every alternation: a rebuild
// cost, accepted in #331, never a correctness issue.
type Runner struct {
	logger        *zap.Logger
	mu            sync.Mutex
	s3Runtimes    map[s3RuntimeGroupKey]*cachedS3Runtime
	duckExporters map[duckExporterGroupKey]*cachedDuckExporter
}

type cachedS3Runtime struct {
	region          string
	credProvider    aws.CredentialsProvider
	client          *s3.Client
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

// credsEqual reports whether this entry was built from exactly this resolved
// static triple. The token is part of the signing identity the cached provider
// bakes in, so it participates in the match: omit it and a rotated
// AWS_SESSION_TOKEN under an unchanged pair keeps hitting the cached runtime,
// handing every caller a stale-token artifact (#329).
func (c *cachedS3Runtime) credsEqual(accessKeyID, secretAccessKey, sessionToken string) bool {
	return c.accessKeyID == accessKeyID &&
		c.secretAccessKey == secretAccessKey &&
		c.sessionToken == sessionToken
}

// s3RuntimeGroupKey identifies a cache slot by everything *except* the
// credential triple. Credentials live on the cached entry and are compared on
// lookup: a changed triple (e.g. a rotated AWS_SESSION_TOKEN, #329) rebuilds
// the runtime and replaces the superseded entry instead of minting a sibling
// key that strands the old entry for process lifetime (#331).
type s3RuntimeGroupKey struct {
	region   string
	endpoint string
	usePath  bool
}

// duckExporterGroupKey identifies a cache slot by everything *except* the
// credential triple. Credentials live on the cached entry and are compared on
// lookup, so a rotation replaces the slot's occupant instead of stranding it
// under a sibling key for process lifetime (#331).
type duckExporterGroupKey struct {
	dbPath   string
	threads  int
	memLimit string
	// The raw cfg region, not s3Runtime.region: the exporter is configured
	// from cfg alone, and an empty cfg.S3Region suppresses SET s3_region
	// entirely. Keying on the chain-resolved region claims a distinction
	// the exporter never makes, so two runs producing byte-identical
	// exporters would miss the cache (#329). It also cut the other way: an
	// empty-region cfg whose chain resolved to some region could collide
	// with an explicitly-configured cfg of that same region, two configs
	// that build *different* exporters, so the second silently reused the
	// first's — a latent wrong cache hit the raw cfg region rules out.
	region   string
	endpoint string
	useSSL   bool
	usePath  bool
}

// cachedDuckExporter is one cache slot's occupant. refs counts in-flight
// RunOnce frames currently holding the exporter; doomed marks an entry that
// has been superseded (or hard-closed via Close) and must not be handed out
// again. A doomed entry's DB closes when refs reaches zero — immediately at
// supersession if idle, else at the last release (#331).
type cachedDuckExporter struct {
	exporter        *DuckExporter
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	refs            int
	doomed          bool
}

// credsEqual: same rule as cachedS3Runtime.credsEqual — the exporter bakes
// the token into SET s3_session_token at construction, so an entry built from
// a different triple is a stale-token artifact, never a cache hit (#329).
func (c *cachedDuckExporter) credsEqual(accessKeyID, secretAccessKey, sessionToken string) bool {
	return c.accessKeyID == accessKeyID &&
		c.secretAccessKey == secretAccessKey &&
		c.sessionToken == sessionToken
}

func NewRunner(logger *zap.Logger) *Runner {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Runner{
		logger:        logger,
		s3Runtimes:    make(map[s3RuntimeGroupKey]*cachedS3Runtime),
		duckExporters: make(map[duckExporterGroupKey]*cachedDuckExporter),
	}
}

func (r *Runner) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	entries := make([]*cachedDuckExporter, 0, len(r.duckExporters))
	for _, entry := range r.duckExporters {
		// Hard shutdown: doomed here + idempotent sql.DB.Close means a release
		// arriving after Close is harmless.
		entry.doomed = true
		entries = append(entries, entry)
	}
	r.s3Runtimes = make(map[s3RuntimeGroupKey]*cachedS3Runtime)
	r.duckExporters = make(map[duckExporterGroupKey]*cachedDuckExporter)
	r.mu.Unlock()

	var errs []error
	for _, entry := range entries {
		if entry.exporter == nil || entry.exporter.DB == nil {
			continue
		}
		if err := entry.exporter.DB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Runner) RunOnce(ctx context.Context, cfg CDCConfig, s3Client S3ObjectClient, dryRun bool, schemaRegistry forma.SchemaRegistry) error {
	if schemaRegistry == nil {
		return fmt.Errorf("schema registry is required for CDC export")
	}

	cfg = cfg.WithDefaults()

	s3Runtime, err := r.getOrCreateS3Runtime(ctx, cfg)
	if err != nil {
		return fmt.Errorf("prepare s3 runtime: %w", err)
	}

	requireFullS3 := cfg.ManifestTemplate != ""
	clients, err := resolveS3Clients(s3Client, s3Runtime.client, requireFullS3)
	if err != nil {
		return fmt.Errorf("resolve s3 clients: %w", err)
	}

	db, pgPassword, err := setupPostgresConnection(ctx, cfg, s3Runtime.region, s3Runtime.credProvider, r.logger)
	if err != nil {
		return fmt.Errorf("setup postgres connection: %w", err)
	}
	defer db.Close()

	duck, releaseDuck, err := r.getOrCreateDuckExporter(ctx, cfg, s3Runtime)
	if err != nil {
		// getOrCreateDuckExporter carries the "new duck exporter:" prefix; this
		// layer adds the run-level step so the two never duplicate.
		return fmt.Errorf("prepare duck exporter: %w", err)
	}
	defer releaseDuck()

	flushCtx := newSchemaFlushContext(flushContextParams{
		cfg:            cfg,
		db:             db,
		duck:           duck,
		clients:        clients,
		pgPassword:     pgPassword,
		dryRun:         dryRun,
		logger:         r.logger,
		schemaRegistry: schemaRegistry,
	})

	schemaIDs, err := getUnflushedSchemaIDs(ctx, db, flushCtx.tableName)
	if err != nil {
		return fmt.Errorf("list schemas with unflushed rows: %w", err)
	}

	// Delegate to processSchemas so the Runner path runs the same pre-flight
	// (resolve every schema's attribute cache, abort the whole run before any
	// side effect if one is unresolvable) that populates flushCtx.attrCaches for
	// executeFlush. Duplicating the loop here silently skipped the pre-flight and
	// left attrCaches nil, so the exporter hard-errored on every schema (#193).
	return flushCtx.processSchemas(ctx, schemaIDs)
}

func (r *Runner) getOrCreateS3Runtime(ctx context.Context, cfg CDCConfig) (*cachedS3Runtime, error) {
	accessKeyID, secretAccessKey, sessionToken := ResolveStaticS3Credentials(cfg)

	key := s3RuntimeGroupKey{
		region:   cfg.S3Region,
		endpoint: cfg.S3Endpoint,
		usePath:  cfg.S3UsePath,
	}

	r.mu.Lock()
	cached := r.s3Runtimes[key]
	r.mu.Unlock()
	if cached != nil && cached.credsEqual(accessKeyID, secretAccessKey, sessionToken) {
		return cached, nil
	}

	var loadOpts []func(*config.LoadOptions) error
	if cfg.S3Region != "" {
		// WithRegion at load time (not a post-load overwrite) so
		// region-sensitive default-chain resolution — STS, SSO — sees the
		// configured region, and an unset region keeps whatever the chain
		// resolved instead of a hardcoded default (#302 parity, #326).
		loadOpts = append(loadOpts, config.WithRegion(cfg.S3Region))
	}
	awsCfg, err := loadAWSConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	if accessKeyID != "" {
		// The session token rides the source that supplied the pair (#329) —
		// dropping it signed temporary STS credentials as long-lived keys.
		awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken)
	}

	runtime := &cachedS3Runtime{
		region:          awsCfg.Region,
		credProvider:    awsCfg.Credentials,
		client:          newS3ClientFn(awsCfg, cfg.S3Endpoint, cfg.S3UsePath),
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		sessionToken:    sessionToken,
	}

	r.mu.Lock()
	if existing := r.s3Runtimes[key]; existing != nil && existing.credsEqual(accessKeyID, secretAccessKey, sessionToken) {
		r.mu.Unlock()
		return existing, nil
	}
	// Replacing a stale-credential entry needs no close: s3.Client holds no
	// OS resources, so dropping the map reference is the whole eviction (#331).
	r.s3Runtimes[key] = runtime
	r.mu.Unlock()

	return runtime, nil
}

func (r *Runner) getOrCreateDuckExporter(ctx context.Context, cfg CDCConfig, s3Runtime *cachedS3Runtime) (*DuckExporter, func(), error) {
	key := duckExporterGroupKey{
		dbPath:   cfg.DuckDBPath,
		threads:  cfg.DuckThreads,
		memLimit: cfg.DuckMemLimit,
		region:   cfg.S3Region,
		endpoint: cfg.S3Endpoint,
		useSSL:   cfg.S3UseSSL,
		usePath:  cfg.S3UsePath,
	}

	r.mu.Lock()
	if entry := r.duckExporters[key]; entry != nil && entry.credsEqual(s3Runtime.accessKeyID, s3Runtime.secretAccessKey, s3Runtime.sessionToken) {
		entry.refs++
		r.mu.Unlock()
		return entry.exporter, r.releaseFunc(entry), nil
	}
	r.mu.Unlock()

	// The cached triple, not a fresh resolve: one resolution keeps the SDK
	// provider and the DuckDB SET statements on the same credentials (#329).
	exporter, err := newDuckExporterFn(ctx, cfg, s3Runtime.accessKeyID, s3Runtime.secretAccessKey, s3Runtime.sessionToken, r.logger)
	if err != nil {
		return nil, nil, fmt.Errorf("new duck exporter: %w", err)
	}

	r.mu.Lock()
	if existing := r.duckExporters[key]; existing != nil && existing.credsEqual(s3Runtime.accessKeyID, s3Runtime.secretAccessKey, s3Runtime.sessionToken) {
		// Lost a same-credentials build race: ours is redundant.
		existing.refs++
		r.mu.Unlock()
		closeExporterDB(exporter, r.logger)
		return existing.exporter, r.releaseFunc(existing), nil
	}
	superseded := r.duckExporters[key]
	entry := &cachedDuckExporter{
		exporter:        exporter,
		accessKeyID:     s3Runtime.accessKeyID,
		secretAccessKey: s3Runtime.secretAccessKey,
		sessionToken:    s3Runtime.sessionToken,
		refs:            1,
	}
	r.duckExporters[key] = entry
	var toClose *DuckExporter
	if superseded != nil {
		superseded.doomed = true
		if superseded.refs == 0 {
			toClose = superseded.exporter
		}
	}
	r.mu.Unlock()
	// Outside the lock: DB.Close can block on an open connection.
	closeExporterDB(toClose, r.logger)
	return entry.exporter, r.releaseFunc(entry), nil
}

// releaseFunc hands a RunOnce frame its release hook: drop the hold, and if
// this was the last hold on a superseded entry, close its DB. Each hook must
// be called exactly once (#331).
func (r *Runner) releaseFunc(entry *cachedDuckExporter) func() {
	return func() {
		r.mu.Lock()
		entry.refs--
		shouldClose := entry.doomed && entry.refs == 0
		r.mu.Unlock()
		if shouldClose {
			closeExporterDB(entry.exporter, r.logger)
		}
	}
}

// closeExporterDB closes a doomed exporter's DuckDB handle. Failing the *new*
// run because an old handle failed to close would be wrong, so errors are
// logged, never returned. Nil-safe on both exporter and DB.
func closeExporterDB(exporter *DuckExporter, logger *zap.Logger) {
	if exporter == nil || exporter.DB == nil {
		return
	}
	if err := exporter.DB.Close(); err != nil {
		logger.Warn("close superseded duck exporter", zap.Error(err))
	}
}
