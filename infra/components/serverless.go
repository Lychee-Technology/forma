package components

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/apigatewayv2"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/cloudwatch"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/s3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// ServerlessConfig holds configuration for creating serverless infrastructure.
// With Aurora DSQL, no VPC configuration is needed - DSQL is accessed via public endpoint.
type ServerlessConfig struct {
	Environment    string
	DSQLClusterArn pulumi.StringOutput // Aurora DSQL cluster ARN for IAM policy
	S3Bucket       pulumi.StringOutput // Data lake S3 bucket name
}

// ServerlessOutput contains the outputs from serverless infrastructure creation.
// Note: Lambda function, API Gateway integration, route, and permission are managed
// by the CD workflow, not Pulumi. This allows Lambda to be created/updated from
// GitHub Release artifacts without requiring Pulumi to manage the function code.
type ServerlessOutput struct {
	// DeploymentBucketName is the S3 bucket for Lambda deployment artifacts.
	DeploymentBucketName pulumi.StringOutput
	// LambdaRoleArn is the IAM role ARN that Lambda will use.
	LambdaRoleArn pulumi.StringOutput
	// LogGroupName is the CloudWatch Log Group name for Lambda logs.
	LogGroupName pulumi.StringOutput
	// LogGroupArn is the CloudWatch Log Group ARN for API Gateway access logs.
	LogGroupArn pulumi.StringOutput
	// ApiGatewayID is the API Gateway HTTP API ID.
	ApiGatewayID pulumi.StringOutput
	// ApiGatewayExecutionArn is needed for creating Lambda permission.
	ApiGatewayExecutionArn pulumi.StringOutput
	// ApiEndpoint is the API Gateway invoke URL (will return 404 until Lambda integration is created).
	ApiEndpoint pulumi.StringOutput
	// LambdaFunctionName is the expected Lambda function name (for CD workflow reference).
	LambdaFunctionName string
}

// NewServerless creates serverless infrastructure for the Forma API.
//
// This component creates:
//   - S3 bucket for Lambda deployment artifacts
//   - IAM role and policies for Lambda execution
//   - CloudWatch Log Group for Lambda logs
//   - API Gateway HTTP API with stage (but no Lambda integration)
//
// The following resources are managed by the CD workflow (not Pulumi):
//   - Lambda function (created/updated from GitHub Release artifacts)
//   - API Gateway Lambda integration
//   - API Gateway route
//   - Lambda invoke permission for API Gateway
//
// This separation allows:
//  1. Infrastructure to be created before any Lambda code exists
//  2. Lambda deployments to be done independently via CD workflow
//  3. Lambda code to come from GitHub Releases instead of being built during infra deployment
func NewServerless(ctx *pulumi.Context, config *ServerlessConfig) (*ServerlessOutput, error) {
	namePrefix := fmt.Sprintf("forma-%s", config.Environment)
	lambdaFunctionName := fmt.Sprintf("%s-api", namePrefix)

	// Create S3 bucket for Lambda deployment artifacts
	deploymentBucket, err := s3.NewBucket(ctx, namePrefix+"-lambda-deployments", &s3.BucketArgs{
		Bucket:       pulumi.String(namePrefix + "-lambda-deployments"),
		ForceDestroy: pulumi.Bool(true),
		Tags: pulumi.StringMap{
			"Name":        pulumi.String(namePrefix + "-lambda-deployments"),
			"Environment": pulumi.String(config.Environment),
			"Purpose":     pulumi.String("lambda-deployments"),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create deployment bucket: %w", err)
	}

	// Block public access on deployment bucket
	_, err = s3.NewBucketPublicAccessBlock(ctx, namePrefix+"-lambda-deployments-pab", &s3.BucketPublicAccessBlockArgs{
		Bucket:                deploymentBucket.ID(),
		BlockPublicAcls:       pulumi.Bool(true),
		BlockPublicPolicy:     pulumi.Bool(true),
		IgnorePublicAcls:      pulumi.Bool(true),
		RestrictPublicBuckets: pulumi.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure deployment bucket public access block: %w", err)
	}

	// Create IAM role for Lambda
	assumeRolePolicy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Action": "sts:AssumeRole",
			"Principal": {
				"Service": "lambda.amazonaws.com"
			},
			"Effect": "Allow"
		}]
	}`

	lambdaRole, err := iam.NewRole(ctx, namePrefix+"-lambda-role", &iam.RoleArgs{
		Name:             pulumi.String(namePrefix + "-lambda-role"),
		AssumeRolePolicy: pulumi.String(assumeRolePolicy),
		Tags: pulumi.StringMap{
			"Name":        pulumi.String(namePrefix + "-lambda-role"),
			"Environment": pulumi.String(config.Environment),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Lambda IAM role: %w", err)
	}

	// Attach basic Lambda execution policy (CloudWatch Logs)
	_, err = iam.NewRolePolicyAttachment(ctx, namePrefix+"-lambda-basic-execution", &iam.RolePolicyAttachmentArgs{
		Role:      lambdaRole.Name,
		PolicyArn: pulumi.String("arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to attach basic execution policy: %w", err)
	}

	// Create custom policy for Aurora DSQL access
	// Lambda needs dsql:DbConnectAdmin to generate auth tokens and connect
	dsqlPolicy := config.DSQLClusterArn.ApplyT(func(arn string) string {
		return fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": [
					"dsql:DbConnectAdmin"
				],
				"Resource": "%s"
			}]
		}`, arn)
	}).(pulumi.StringOutput)

	_, err = iam.NewRolePolicy(ctx, namePrefix+"-lambda-dsql-policy", &iam.RolePolicyArgs{
		Name:   pulumi.String(namePrefix + "-lambda-dsql-policy"),
		Role:   lambdaRole.ID(),
		Policy: dsqlPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create DSQL policy: %w", err)
	}

	// Create custom policy for S3 access (data lake bucket)
	s3Policy := config.S3Bucket.ApplyT(func(bucket string) string {
		return fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": [
					"s3:GetObject",
					"s3:PutObject",
					"s3:DeleteObject",
					"s3:ListBucket"
				],
				"Resource": [
					"arn:aws:s3:::%s",
					"arn:aws:s3:::%s/*"
				]
			}]
		}`, bucket, bucket)
	}).(pulumi.StringOutput)

	_, err = iam.NewRolePolicy(ctx, namePrefix+"-lambda-s3-policy", &iam.RolePolicyArgs{
		Name:   pulumi.String(namePrefix + "-lambda-s3-policy"),
		Role:   lambdaRole.ID(),
		Policy: s3Policy,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 policy: %w", err)
	}

	// Create CloudWatch Log Group for Lambda
	logGroup, err := cloudwatch.NewLogGroup(ctx, namePrefix+"-lambda-logs", &cloudwatch.LogGroupArgs{
		Name:            pulumi.Sprintf("/aws/lambda/%s", lambdaFunctionName),
		RetentionInDays: pulumi.Int(14),
		Tags: pulumi.StringMap{
			"Name":        pulumi.String(namePrefix + "-lambda-logs"),
			"Environment": pulumi.String(config.Environment),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create CloudWatch log group: %w", err)
	}

	// Create API Gateway HTTP API
	// Note: Integration and route will be created by CD workflow after Lambda is deployed
	api, err := apigatewayv2.NewApi(ctx, namePrefix+"-api-gw", &apigatewayv2.ApiArgs{
		Name:         pulumi.String(namePrefix + "-api"),
		ProtocolType: pulumi.String("HTTP"),
		Description:  pulumi.String(fmt.Sprintf("API Gateway for Forma %s", config.Environment)),
		CorsConfiguration: &apigatewayv2.ApiCorsConfigurationArgs{
			AllowHeaders: pulumi.StringArray{
				pulumi.String("Content-Type"),
				pulumi.String("Authorization"),
				pulumi.String("X-Amz-Date"),
				pulumi.String("X-Api-Key"),
				pulumi.String("X-Amz-Security-Token"),
			},
			AllowMethods: pulumi.StringArray{
				pulumi.String("GET"),
				pulumi.String("POST"),
				pulumi.String("PUT"),
				pulumi.String("DELETE"),
				pulumi.String("OPTIONS"),
			},
			AllowOrigins: pulumi.StringArray{
				pulumi.String("*"),
			},
			MaxAge: pulumi.Int(300),
		},
		Tags: pulumi.StringMap{
			"Name":        pulumi.String(namePrefix + "-api"),
			"Environment": pulumi.String(config.Environment),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create API Gateway: %w", err)
	}

	// Create stage with auto-deploy
	// Note: Access logs use the log group ARN, which is available before Lambda
	stage, err := apigatewayv2.NewStage(ctx, namePrefix+"-api-stage", &apigatewayv2.StageArgs{
		ApiId:      api.ID(),
		Name:       pulumi.String("$default"),
		AutoDeploy: pulumi.Bool(true),
		AccessLogSettings: &apigatewayv2.StageAccessLogSettingsArgs{
			DestinationArn: logGroup.Arn,
			Format:         pulumi.String(`{"requestId":"$context.requestId","ip":"$context.identity.sourceIp","requestTime":"$context.requestTime","httpMethod":"$context.httpMethod","routeKey":"$context.routeKey","status":"$context.status","protocol":"$context.protocol","responseLength":"$context.responseLength","integrationError":"$context.integrationErrorMessage"}`),
		},
		Tags: pulumi.StringMap{
			"Name":        pulumi.String(namePrefix + "-api-stage"),
			"Environment": pulumi.String(config.Environment),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create API Gateway stage: %w", err)
	}

	return &ServerlessOutput{
		DeploymentBucketName:   deploymentBucket.Bucket,
		LambdaRoleArn:          lambdaRole.Arn,
		LogGroupName:           logGroup.Name,
		LogGroupArn:            logGroup.Arn,
		ApiGatewayID:           api.ID().ToStringOutput(),
		ApiGatewayExecutionArn: api.ExecutionArn,
		ApiEndpoint:            stage.InvokeUrl,
		LambdaFunctionName:     lambdaFunctionName,
	}, nil
}
