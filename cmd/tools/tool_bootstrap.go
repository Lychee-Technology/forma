package main

import (
	"context"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"go.uber.org/zap"
)

var (
	toolLoggerFactoryDev  = zap.NewDevelopment
	toolLoggerFactoryProd = zap.NewProduction
	toolPostgresPoolFn    = bootstrap.NewPostgresPoolFromConfigContext
	toolSchemaRegistryFn  = internal.NewFileSchemaRegistryContext
	toolLoadAWSConfigFn   = awsconfig.LoadDefaultConfig
)

func buildToolLogger(dev bool) (*zap.Logger, error) {
	if dev {
		return toolLoggerFactoryDev()
	}
	return toolLoggerFactoryProd()
}

func buildToolPostgresPool(ctx context.Context, cfg forma.DatabaseConfig) (*pgxpool.Pool, error) {
	return toolPostgresPoolFn(ctx, cfg)
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

func buildToolSchemaRegistry(
	ctx context.Context,
	pool *pgxpool.Pool,
	table string,
	dir string,
) (forma.SchemaRegistry, error) {
	return toolSchemaRegistryFn(ctx, pool, table, dir)
}
