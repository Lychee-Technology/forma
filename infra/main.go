// Package main is the entry point for the Pulumi infrastructure program.
// It provisions AWS infrastructure for the Forma application including:
// - Aurora DSQL serverless database cluster
// - S3 bucket for data lake storage
// - S3 bucket for Lambda deployment artifacts
// - IAM role and policies for Lambda execution
// - API Gateway HTTP API (without Lambda integration)
//
// Note: Lambda function and API Gateway integration are managed by the CD workflow,
// not by Pulumi. This allows Lambda to be deployed from GitHub Release artifacts
// without requiring infrastructure changes.
package main

import (
	"fmt"

	"github.com/lychee-technology/forma/infra/components"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		// Load configuration
		cfg := config.New(ctx, "forma-infra")
		env := cfg.Require("environment")

		// 1. Create S3 bucket for data lake
		storageOutput, err := components.NewStorage(ctx, env)
		if err != nil {
			return fmt.Errorf("failed to create S3 storage: %w", err)
		}

		// 2. Create Aurora DSQL serverless cluster
		// DSQL is fully serverless - no instance sizing, VPC, or passwords needed
		dbConfig := &components.DatabaseConfig{
			Environment: env,
		}

		dbOutput, err := components.NewDatabase(ctx, dbConfig)
		if err != nil {
			return fmt.Errorf("failed to create Aurora DSQL cluster: %w", err)
		}

		// 3. Create serverless infrastructure (S3 bucket, IAM role, API Gateway)
		// Note: Lambda function and integration are managed by the CD workflow
		serverlessConfig := &components.ServerlessConfig{
			Environment:    env,
			DSQLClusterArn: dbOutput.ClusterArn,
			S3Bucket:       storageOutput.BucketName,
		}

		serverlessOutput, err := components.NewServerless(ctx, serverlessConfig)
		if err != nil {
			return fmt.Errorf("failed to create serverless infrastructure: %w", err)
		}

		// Export outputs for use by CD workflow
		// Database outputs
		ctx.Export("environment", pulumi.String(env))
		ctx.Export("dsqlClusterEndpoint", dbOutput.ClusterEndpoint)
		ctx.Export("dsqlClusterArn", dbOutput.ClusterArn)
		ctx.Export("dsqlClusterID", dbOutput.ClusterID)

		// Storage outputs
		ctx.Export("s3BucketName", storageOutput.BucketName)
		ctx.Export("s3BucketArn", storageOutput.BucketArn)

		// Serverless infrastructure outputs (used by CD workflow to create Lambda)
		ctx.Export("lambdaDeploymentBucket", serverlessOutput.DeploymentBucketName)
		ctx.Export("lambdaRoleArn", serverlessOutput.LambdaRoleArn)
		ctx.Export("lambdaFunctionName", pulumi.String(serverlessOutput.LambdaFunctionName))
		ctx.Export("logGroupName", serverlessOutput.LogGroupName)
		ctx.Export("logGroupArn", serverlessOutput.LogGroupArn)

		// API Gateway outputs (integration created by CD workflow)
		ctx.Export("apiGatewayId", serverlessOutput.ApiGatewayID)
		ctx.Export("apiGatewayExecutionArn", serverlessOutput.ApiGatewayExecutionArn)
		ctx.Export("apiEndpoint", serverlessOutput.ApiEndpoint)

		return nil
	})
}
