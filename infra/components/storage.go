package components

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/s3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// StorageOutput contains the outputs from S3 storage creation.
type StorageOutput struct {
	BucketName pulumi.StringOutput
	BucketArn  pulumi.StringOutput
	BucketID   pulumi.StringOutput
}

// NewStorage creates an S3 bucket for the data lake.
func NewStorage(ctx *pulumi.Context, env string) (*StorageOutput, error) {
	namePrefix := fmt.Sprintf("forma-%s", env)

	// Create S3 bucket for data lake
	bucket, err := s3.NewBucket(ctx, namePrefix+"-data-lake", &s3.BucketArgs{
		Bucket:       pulumi.String(namePrefix + "-data-lake"),
		ForceDestroy: pulumi.Bool(env != "prod"), // Only allow force destroy in non-prod
		Tags: pulumi.StringMap{
			"Name":        pulumi.String(namePrefix + "-data-lake"),
			"Environment": pulumi.String(env),
			"Purpose":     pulumi.String("data-lake"),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 bucket: %w", err)
	}

	// Block all public access
	_, err = s3.NewBucketPublicAccessBlock(ctx, namePrefix+"-data-lake-pab", &s3.BucketPublicAccessBlockArgs{
		Bucket:                bucket.ID(),
		BlockPublicAcls:       pulumi.Bool(true),
		BlockPublicPolicy:     pulumi.Bool(true),
		IgnorePublicAcls:      pulumi.Bool(true),
		RestrictPublicBuckets: pulumi.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure public access block: %w", err)
	}

	// Enable versioning
	_, err = s3.NewBucketVersioningV2(ctx, namePrefix+"-data-lake-versioning", &s3.BucketVersioningV2Args{
		Bucket: bucket.ID(),
		VersioningConfiguration: &s3.BucketVersioningV2VersioningConfigurationArgs{
			Status: pulumi.String("Enabled"),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to enable bucket versioning: %w", err)
	}

	// Enable server-side encryption
	_, err = s3.NewBucketServerSideEncryptionConfigurationV2(ctx, namePrefix+"-data-lake-encryption", &s3.BucketServerSideEncryptionConfigurationV2Args{
		Bucket: bucket.ID(),
		Rules: s3.BucketServerSideEncryptionConfigurationV2RuleArray{
			&s3.BucketServerSideEncryptionConfigurationV2RuleArgs{
				ApplyServerSideEncryptionByDefault: &s3.BucketServerSideEncryptionConfigurationV2RuleApplyServerSideEncryptionByDefaultArgs{
					SseAlgorithm: pulumi.String("AES256"),
				},
				BucketKeyEnabled: pulumi.Bool(true),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure bucket encryption: %w", err)
	}

	// Configure lifecycle rules for cost optimization
	_, err = s3.NewBucketLifecycleConfigurationV2(ctx, namePrefix+"-data-lake-lifecycle", &s3.BucketLifecycleConfigurationV2Args{
		Bucket: bucket.ID(),
		Rules: s3.BucketLifecycleConfigurationV2RuleArray{
			// Move infrequent access data to IA tier after 30 days
			&s3.BucketLifecycleConfigurationV2RuleArgs{
				Id:     pulumi.String("transition-to-ia"),
				Status: pulumi.String("Enabled"),
				Filter: &s3.BucketLifecycleConfigurationV2RuleFilterArgs{
					Prefix: pulumi.String("data/"),
				},
				Transitions: s3.BucketLifecycleConfigurationV2RuleTransitionArray{
					&s3.BucketLifecycleConfigurationV2RuleTransitionArgs{
						Days:         pulumi.Int(30),
						StorageClass: pulumi.String("STANDARD_IA"),
					},
					&s3.BucketLifecycleConfigurationV2RuleTransitionArgs{
						Days:         pulumi.Int(90),
						StorageClass: pulumi.String("GLACIER"),
					},
				},
			},
			// Clean up incomplete multipart uploads
			&s3.BucketLifecycleConfigurationV2RuleArgs{
				Id:     pulumi.String("cleanup-incomplete-uploads"),
				Status: pulumi.String("Enabled"),
				AbortIncompleteMultipartUpload: &s3.BucketLifecycleConfigurationV2RuleAbortIncompleteMultipartUploadArgs{
					DaysAfterInitiation: pulumi.Int(7),
				},
			},
			// Delete old non-current versions
			&s3.BucketLifecycleConfigurationV2RuleArgs{
				Id:     pulumi.String("cleanup-old-versions"),
				Status: pulumi.String("Enabled"),
				NoncurrentVersionExpiration: &s3.BucketLifecycleConfigurationV2RuleNoncurrentVersionExpirationArgs{
					NoncurrentDays: pulumi.Int(30),
				},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure bucket lifecycle: %w", err)
	}

	return &StorageOutput{
		BucketName: bucket.ID().ToStringOutput(),
		BucketArn:  bucket.Arn,
		BucketID:   bucket.ID().ToStringOutput(),
	}, nil
}
