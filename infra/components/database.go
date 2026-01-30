package components

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/dsql"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// DatabaseConfig holds configuration for creating an Aurora DSQL cluster.
type DatabaseConfig struct {
	Environment string
}

// DatabaseOutput contains the outputs from DSQL cluster creation.
type DatabaseOutput struct {
	ClusterEndpoint pulumi.StringOutput
	ClusterArn      pulumi.StringOutput
	ClusterID       pulumi.StringOutput
}

// NewDatabase creates an Aurora DSQL serverless cluster.
// Aurora DSQL is fully serverless - no instance sizing, VPC, or passwords needed.
// Authentication is done via IAM using temporary auth tokens.
func NewDatabase(ctx *pulumi.Context, config *DatabaseConfig) (*DatabaseOutput, error) {
	namePrefix := fmt.Sprintf("forma-%s", config.Environment)

	// Determine if this is a production environment
	isProduction := config.Environment == "prod"

	// Create Aurora DSQL cluster
	// DSQL is serverless - no need to specify instance class, storage, VPC, or subnets
	cluster, err := dsql.NewCluster(ctx, namePrefix+"-dsql", &dsql.ClusterArgs{
		// Enable deletion protection in production
		DeletionProtectionEnabled: pulumi.Bool(isProduction),
		Tags: pulumi.StringMap{
			"Name":        pulumi.String(namePrefix + "-dsql"),
			"Environment": pulumi.String(config.Environment),
			"ManagedBy":   pulumi.String("pulumi"),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Aurora DSQL cluster: %w", err)
	}

	// The cluster endpoint is constructed from the cluster identifier
	// Format: <cluster-id>.<region>.dsql.amazonaws.com
	clusterEndpoint := cluster.Identifier.ApplyT(func(id string) string {
		return fmt.Sprintf("%s.dsql.us-east-2.on.aws", id)
	}).(pulumi.StringOutput)

	return &DatabaseOutput{
		ClusterEndpoint: clusterEndpoint,
		ClusterArn:      cluster.Arn,
		ClusterID:       cluster.Identifier,
	}, nil
}
