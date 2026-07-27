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
	"github.com/lychee-technology/forma/internal/manifest"
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

type Runner struct {
	logger        *zap.Logger
	mu            sync.Mutex
	s3Runtimes    map[s3RuntimeKey]*cachedS3Runtime
	duckExporters map[duckExporterKey]*DuckExporter
}

type cachedS3Runtime struct {
	region          string
	credProvider    aws.CredentialsProvider
	client          *s3.Client
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

type s3RuntimeKey struct {
	region      string
	endpoint    string
	usePath     bool
	accessKeyID string
	// The token is part of the signing identity the cached provider bakes in,
	// so it belongs in the key alongside the pair: omit it and a rotated
	// AWS_SESSION_TOKEN under an unchanged pair keeps hitting the cached
	// runtime, handing every caller a stale-token artifact (#329).
	secretAccessKey string
	sessionToken    string
}

type duckExporterKey struct {
	dbPath      string
	threads     int
	memLimit    string
	region      string
	endpoint    string
	useSSL      bool
	usePath     bool
	accessKeyID string
	// Same rule as s3RuntimeKey: the exporter bakes the token into
	// SET s3_session_token at construction, so a key without it returns a
	// stale-token exporter after a rotation (#329).
	secretAccessKey string
	sessionToken    string
}

func NewRunner(logger *zap.Logger) *Runner {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Runner{
		logger:        logger,
		s3Runtimes:    make(map[s3RuntimeKey]*cachedS3Runtime),
		duckExporters: make(map[duckExporterKey]*DuckExporter),
	}
}

func (r *Runner) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	exporters := make([]*DuckExporter, 0, len(r.duckExporters))
	for _, exporter := range r.duckExporters {
		exporters = append(exporters, exporter)
	}
	r.s3Runtimes = make(map[s3RuntimeKey]*cachedS3Runtime)
	r.duckExporters = make(map[duckExporterKey]*DuckExporter)
	r.mu.Unlock()

	var errs []error
	for _, exporter := range exporters {
		if exporter == nil || exporter.DB == nil {
			continue
		}
		if err := exporter.DB.Close(); err != nil {
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
	activeS3Client, activeFullS3Client, err := resolveS3Clients(s3Client, s3Runtime.client, requireFullS3)
	if err != nil {
		return fmt.Errorf("resolve s3 clients: %w", err)
	}

	var manifestStore manifest.Store
	var manifestResolver manifest.PathResolver
	if cfg.ManifestTemplate != "" {
		manifestStore = &manifest.S3Store{
			Client: activeFullS3Client,
			Bucket: cfg.S3Bucket,
		}
		manifestResolver = manifest.PathResolver{
			Prefix:       cfg.ManifestPrefix,
			PathTemplate: cfg.ManifestTemplate,
		}
	}

	db, pgPassword, err := setupPostgresConnection(ctx, cfg, s3Runtime.region, s3Runtime.credProvider, r.logger)
	if err != nil {
		return fmt.Errorf("setup postgres connection: %w", err)
	}
	defer db.Close()

	duck, err := r.getOrCreateDuckExporter(ctx, cfg, s3Runtime)
	if err != nil {
		// getOrCreateDuckExporter carries the "new duck exporter:" prefix; this
		// layer adds the run-level step so the two never duplicate.
		return fmt.Errorf("prepare duck exporter: %w", err)
	}

	tableName := cfg.ChangeLogTable
	if tableName == "" {
		tableName = "change_log"
	}

	schemaIDs, err := getUnflushedSchemaIDs(ctx, db, tableName)
	if err != nil {
		return err
	}

	flushCtx := &schemaFlushContext{
		db:               db,
		duck:             duck,
		s3Client:         activeS3Client,
		cfg:              cfg,
		tableName:        tableName,
		pgPassword:       pgPassword,
		dryRun:           dryRun,
		logger:           r.logger,
		schemaRegistry:   schemaRegistry,
		manifestStore:    manifestStore,
		manifestResolver: manifestResolver,
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

	key := s3RuntimeKey{
		region:          cfg.S3Region,
		endpoint:        cfg.S3Endpoint,
		usePath:         cfg.S3UsePath,
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		sessionToken:    sessionToken,
	}

	r.mu.Lock()
	cached := r.s3Runtimes[key]
	r.mu.Unlock()
	if cached != nil {
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
	if existing := r.s3Runtimes[key]; existing != nil {
		r.mu.Unlock()
		return existing, nil
	}
	r.s3Runtimes[key] = runtime
	r.mu.Unlock()

	return runtime, nil
}

func (r *Runner) getOrCreateDuckExporter(ctx context.Context, cfg CDCConfig, s3Runtime *cachedS3Runtime) (*DuckExporter, error) {
	key := duckExporterKey{
		dbPath:   cfg.DuckDBPath,
		threads:  cfg.DuckThreads,
		memLimit: cfg.DuckMemLimit,
		// The raw cfg region, not s3Runtime.region: the exporter is configured
		// from cfg alone, and an empty cfg.S3Region suppresses SET s3_region
		// entirely. Keying on the chain-resolved region claims a distinction
		// the exporter never makes, so two runs producing byte-identical
		// exporters would miss the cache (#329). It also cut the other way: an
		// empty-region cfg whose chain resolved to some region could collide
		// with an explicitly-configured cfg of that same region, two configs
		// that build *different* exporters, so the second silently reused the
		// first's — a latent wrong cache hit the raw cfg region rules out.
		region:      cfg.S3Region,
		endpoint:    cfg.S3Endpoint,
		useSSL:      cfg.S3UseSSL,
		usePath:     cfg.S3UsePath,
		accessKeyID: s3Runtime.accessKeyID,
		// The cached runtime's token, matching the triple handed to
		// newDuckExporterFn below (#329).
		secretAccessKey: s3Runtime.secretAccessKey,
		sessionToken:    s3Runtime.sessionToken,
	}

	r.mu.Lock()
	cached := r.duckExporters[key]
	r.mu.Unlock()
	if cached != nil {
		return cached, nil
	}

	// The cached triple, not a fresh resolve: one resolution keeps the SDK
	// provider and the DuckDB SET statements on the same credentials (#329).
	exporter, err := newDuckExporterFn(ctx, cfg, s3Runtime.accessKeyID, s3Runtime.secretAccessKey, s3Runtime.sessionToken, r.logger)
	if err != nil {
		return nil, fmt.Errorf("new duck exporter: %w", err)
	}

	r.mu.Lock()
	if existing := r.duckExporters[key]; existing != nil {
		r.mu.Unlock()
		if exporter.DB != nil {
			_ = exporter.DB.Close()
		}
		return existing, nil
	}
	r.duckExporters[key] = exporter
	r.mu.Unlock()

	return exporter, nil
}
