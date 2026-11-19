package main

import (
	"context"

	"github.com/lychee-technology/forma/internal/schemameta"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/cdc"
	"go.uber.org/zap"
)

var (
	toolLoggerFactoryDev    = zap.NewDevelopment
	toolLoggerFactoryProd   = zap.NewProduction
	toolPostgresPoolFn      = bootstrap.NewPostgresPoolFromConfigContext
	toolSchemaRegistryFn    = schemameta.NewFileSchemaRegistryContext
	toolLoadAWSConfigFn     = awsconfig.LoadDefaultConfig
	buildToolPostgresPoolFn = func(ctx context.Context, cfg forma.DatabaseConfig) (toolDBPool, error) {
		return toolPostgresPoolFn(ctx, cfg)
	}
)

// toolDBPool is the pool surface validate-schema-consistency needs: it only
// reads. *pgxpool.Pool and pgxmock satisfy it.
type toolDBPool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Close()
}

func buildToolLogger(dev bool) (*zap.Logger, error) {
	if dev {
		return toolLoggerFactoryDev()
	}
	return toolLoggerFactoryProd()
}

func buildToolPostgresPool(ctx context.Context, cfg forma.DatabaseConfig) (*pgxpool.Pool, error) {
	return toolPostgresPoolFn(ctx, cfg)
}

func buildSchemaConsistencyPool(ctx context.Context, cfg forma.DatabaseConfig) (toolDBPool, error) {
	return buildToolPostgresPoolFn(ctx, cfg)
}

func buildToolS3Client(ctx context.Context, region, endpoint string, usePath bool) (*s3.Client, error) {
	awsCfg, err := toolLoadAWSConfigFn(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}

	if endpoint != "" {
		return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = &endpoint
			o.UsePathStyle = usePath
		}), nil
	}

	return s3.NewFromConfig(awsCfg), nil
}

// toolCDCObjectClientFn builds the client cdc-flush and cdc-init stage their
// objects and manifests through. It is buildToolS3Client behind a seam, so a
// test can run either command's whole path against a bucket of its own.
var toolCDCObjectClientFn = func(ctx context.Context, region, endpoint string, usePath bool) (cdc.S3Client, error) {
	return buildToolS3Client(ctx, region, endpoint, usePath)
}

func buildToolSchemaRegistry(
	ctx context.Context,
	pool *pgxpool.Pool,
	table string,
	dir string,
) (forma.SchemaRegistry, error) {
	return toolSchemaRegistryFn(ctx, pool, table, dir)
}
