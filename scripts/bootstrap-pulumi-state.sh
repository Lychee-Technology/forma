#!/bin/bash
# Bootstrap script for Pulumi S3 state bucket
# This script creates the S3 bucket used to store Pulumi state files.
# Run this once before your first Pulumi deployment.

set -euo pipefail

# Configuration
AWS_REGION="${AWS_REGION:-us-east-2}"
BUCKET_NAME="${PULUMI_STATE_BUCKET:-forma-pulumi-state}"
ACCOUNT_ID=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Please run 'aws configure' first."
        exit 1
    fi
    
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    log_info "Using AWS Account: ${ACCOUNT_ID}"
    log_info "Region: ${AWS_REGION}"
}

# Create S3 bucket
create_bucket() {
    log_info "Creating S3 bucket: ${BUCKET_NAME}"
    
    # Check if bucket already exists
    if aws s3api head-bucket --bucket "${BUCKET_NAME}" 2>/dev/null; then
        log_warn "Bucket ${BUCKET_NAME} already exists. Skipping creation."
        return 0
    fi
    
    # Create bucket (different command for us-east-1)
    if [ "${AWS_REGION}" == "us-east-1" ]; then
        aws s3api create-bucket \
            --bucket "${BUCKET_NAME}" \
            --region "${AWS_REGION}"
    else
        aws s3api create-bucket \
            --bucket "${BUCKET_NAME}" \
            --region "${AWS_REGION}" \
            --create-bucket-configuration LocationConstraint="${AWS_REGION}"
    fi
    
    log_info "Bucket created successfully."
}

# Enable versioning
enable_versioning() {
    log_info "Enabling versioning on bucket..."
    
    aws s3api put-bucket-versioning \
        --bucket "${BUCKET_NAME}" \
        --versioning-configuration Status=Enabled
    
    log_info "Versioning enabled."
}

# Enable encryption
enable_encryption() {
    log_info "Enabling server-side encryption..."
    
    aws s3api put-bucket-encryption \
        --bucket "${BUCKET_NAME}" \
        --server-side-encryption-configuration '{
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                },
                "BucketKeyEnabled": true
            }]
        }'
    
    log_info "Encryption enabled."
}

# Block public access
block_public_access() {
    log_info "Blocking public access..."
    
    aws s3api put-public-access-block \
        --bucket "${BUCKET_NAME}" \
        --public-access-block-configuration '{
            "BlockPublicAcls": true,
            "IgnorePublicAcls": true,
            "BlockPublicPolicy": true,
            "RestrictPublicBuckets": true
        }'
    
    log_info "Public access blocked."
}

# Add bucket policy
add_bucket_policy() {
    log_info "Adding bucket policy..."
    
    POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "EnforceTLS",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}",
                "arn:aws:s3:::${BUCKET_NAME}/*"
            ],
            "Condition": {
                "Bool": {
                    "aws:SecureTransport": "false"
                }
            }
        }
    ]
}
EOF
)
    
    aws s3api put-bucket-policy \
        --bucket "${BUCKET_NAME}" \
        --policy "${POLICY}"
    
    log_info "Bucket policy added."
}

# Add tags
add_tags() {
    log_info "Adding tags..."
    
    aws s3api put-bucket-tagging \
        --bucket "${BUCKET_NAME}" \
        --tagging '{
            "TagSet": [
                {"Key": "Purpose", "Value": "pulumi-state"},
                {"Key": "ManagedBy", "Value": "bootstrap-script"},
                {"Key": "Project", "Value": "forma"}
            ]
        }'
    
    log_info "Tags added."
}

# Print summary
print_summary() {
    echo ""
    echo "=============================================="
    log_info "Pulumi state bucket setup complete!"
    echo "=============================================="
    echo ""
    echo "Bucket Name: ${BUCKET_NAME}"
    echo "Region: ${AWS_REGION}"
    echo "Account: ${ACCOUNT_ID}"
    echo ""
    echo "To use this bucket with Pulumi, run:"
    echo ""
    echo "  export PULUMI_STATE_BUCKET=${BUCKET_NAME}"
    echo "  pulumi login s3://${BUCKET_NAME}"
    echo ""
    echo "Or add to your GitHub repository secrets:"
    echo ""
    echo "  PULUMI_STATE_BUCKET=${BUCKET_NAME}"
    echo ""
    echo "=============================================="
}

# Main
main() {
    echo "=============================================="
    echo "  Pulumi S3 State Bucket Bootstrap Script"
    echo "=============================================="
    echo ""
    
    check_prerequisites
    create_bucket
    enable_versioning
    enable_encryption
    block_public_access
    add_bucket_policy
    add_tags
    print_summary
}

# Run main
main "$@"
