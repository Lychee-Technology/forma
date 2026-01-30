# CI/CD Infrastructure Design for Forma

## Overview

This document describes the CI/CD infrastructure design for Forma, a Go-based data management system using PostgreSQL + DuckDB.

| Aspect | Choice |
|--------|--------|
| IaC Tool | Pulumi (Go) |
| Database | AWS Aurora DSQL (serverless) |
| Compute | AWS Lambda (`provided.al2023`, ARM64) |
| API | API Gateway HTTP API (v2) |
| State Storage | S3 Backend |
| Environments | dev / prod |
| Region | us-east-2 |

## Current Architecture (No VPC)

The current implementation runs Lambda **outside of VPC** for simplicity and cost efficiency.

```
┌─────────────────────────────────────────────────────────────────┐
│                         AWS Cloud                               │
│                                                                 │
│   ┌───────────────┐       ┌──────────────────┐                 │
│   │ API Gateway   │──────▶│ Lambda (No VPC)  │                 │
│   │ HTTP API      │       │                  │                 │
│   └───────────────┘       └────────┬─────────┘                 │
│                                    │                            │
│                         IAM Auth   │  TLS                       │
│                         Token      │                            │
│                                    ▼                            │
│                          ┌──────────────────┐                  │
│                          │ Aurora DSQL      │                  │
│                          │ (Public Endpoint)│                  │
│                          │ *.dsql.*.on.aws  │                  │
│                          └──────────────────┘                  │
│                                                                 │
│   ┌──────────────────┐                                         │
│   │ S3 Data Lake     │                                         │
│   └──────────────────┘                                         │
└─────────────────────────────────────────────────────────────────┘
```

### Key Characteristics

- **No VPC required**: Aurora DSQL is accessed via public endpoint
- **IAM authentication**: No database passwords; uses temporary IAM auth tokens
- **TLS encryption**: All connections use `sslmode=require`
- **Zero networking costs**: No NAT Gateway, no VPC Endpoints

### Authentication Flow

1. Lambda loads AWS credentials from execution role
2. Generates IAM auth token using `dsql:DbConnectAdmin` permission
3. Connects to DSQL endpoint with token as password
4. Token valid for 15 minutes to 7 days

## Future Option: VPC + IPv6 Architecture

For organizations with strict security/compliance requirements mandating all traffic stay within private networks, a VPC-based architecture is possible.

### Key Discovery: Aurora DSQL Supports IPv6

Aurora DSQL supports **dual-stack (IPv4 + IPv6)** connections. This enables using **Egress-Only Internet Gateway** instead of NAT Gateway, significantly reducing costs.

Reference: [AWS DSQL Troubleshooting - NetworkUnreachable error](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/troubleshooting.html)

> "When a server supports dual-stack mode, these clients first resolve hostnames to both IPv4 and IPv6 addresses."

### Important Constraint: Lambda Requires Dual-Stack Subnets

**Lambda does NOT support IPv6-only subnets.** From [AWS Lambda VPC documentation](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html):

> "Lambda doesn't support outbound IPv6 connections for IPv6-only subnets in a VPC"

This means:
- Subnets must be **dual-stack** (both IPv4 and IPv6 CIDR blocks)
- Lambda uses IPv6 for outbound connections to DSQL
- The `Ipv6AllowedForDualStack` flag must be enabled on the Lambda function

### Proposed Architecture

```
                            ┌──────────────────────────────────────┐
                            │         AWS Lambda Service           │
                            │      (Lambda-managed VPC)            │
                            │                                      │
                            │  ┌────────────────────────────────┐  │
        ┌───────────────┐   │  │     Lambda Execution Env       │  │
        │ API Gateway   │   │  │  ┌──────────────────────────┐  │  │
Internet│ HTTP API      │───┼──┼─▶│   Your Lambda Function   │  │  │
        │               │   │  │  │                          │  │  │
        └───────────────┘   │  │  └────────────┬─────────────┘  │  │
                            │  │               │                 │  │
          AWS Internal      │  └───────────────┼─────────────────┘  │
          Invoke Path       │                  │                    │
                            └──────────────────┼────────────────────┘
                                               │
                                               │ Hyperplane ENI
                                               │ (managed by Lambda)
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Customer VPC (IPv6 enabled)                       │
│                                                                              │
│   ┌────────────────────────────────────────────────────────────────────┐     │
│   │                Private Subnet (Dual-Stack: IPv4 + IPv6)            │     │
│   │                                                                    │     │
│   │    AZ-a (10.0.1.0/24 + 2600:1f18::/64)                            │     │
│   │    ┌─────────────────┐                                             │     │
│   │    │ Hyperplane ENI  │◄── Lambda attaches here via ENI             │     │
│   │    │ (Lambda-managed)│                                             │     │
│   │    └────────┬────────┘                                             │     │
│   │             │                                                      │     │
│   │    AZ-b (10.0.2.0/24 + 2600:1f18:1::/64)                          │     │
│   │    ┌─────────────────┐                                             │     │
│   │    │ Hyperplane ENI  │                                             │     │
│   │    │ (Lambda-managed)│                                             │     │
│   │    └────────┬────────┘                                             │     │
│   │             │                                                      │     │
│   └─────────────┼──────────────────────────────────────────────────────┘     │
│                 │                                                            │
│                 │ IPv6 outbound (uses Egress-Only IGW)                       │
│                 ▼                                                            │
│   ┌─────────────────────────┐                                                │
│   │   Egress-Only IGW       │◄── FREE (IPv6 outbound only, no inbound)       │
│   │   (attached to VPC)     │                                                │
│   └─────────────┬───────────┘                                                │
│                 │                                                            │
└─────────────────┼────────────────────────────────────────────────────────────┘
                  │
                  │ IPv6 over public internet (TLS encrypted)
                  ▼
        ┌──────────────────┐
        │ Aurora DSQL      │
        │ (dual-stack)     │
        │ *.dsql.*.on.aws  │
        └──────────────────┘
```

### How API Gateway Invokes Lambda in a VPC

This is a common point of confusion. Here's how it actually works:

1. **Lambda runs in a Lambda-managed VPC**: Every Lambda function runs inside a VPC owned by AWS Lambda service. This is invisible to customers.

2. **API Gateway invokes Lambda through AWS internal network**: The invocation goes through Lambda's control plane, NOT through your customer VPC. No VPC Endpoint or VPC Link is needed.

3. **Customer VPC is for outbound access only**: When you configure a Lambda function with a VPC, you're giving it access to resources IN that VPC (or reachable from it), not changing how it receives invocations.

4. **Hyperplane ENI bridges the networks**: Lambda creates managed ENIs (Hyperplane ENIs) in your subnets. These ENIs allow Lambda to make outbound connections to your VPC resources.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Request Flow Diagram                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  [Client] ──HTTP──▶ [API Gateway] ──AWS Internal──▶ [Lambda Service]    │
│                                                            │            │
│                                                            │ Invoke     │
│                                                            ▼            │
│                                               [Lambda Execution Env]    │
│                                                            │            │
│                                                            │ Hyperplane │
│                                                            │ ENI        │
│                                                            ▼            │
│                                                    [Customer VPC]       │
│                                                            │            │
│                                                            │ IPv6       │
│                                                            ▼            │
│                                                  [Egress-Only IGW]      │
│                                                            │            │
│                                                            │ Internet   │
│                                                            ▼            │
│                                                    [Aurora DSQL]        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### What You DON'T Need

| Component | Required? | Reason |
|-----------|-----------|--------|
| VPC Endpoint for API Gateway | **NO** | API GW invokes Lambda through AWS internal network |
| VPC Link | **NO** | Only needed for API GW to access non-Lambda resources in VPC |
| NAT Gateway | **NO** | Using Egress-Only IGW with IPv6 instead |
| Internet Gateway | **NO** | Egress-Only IGW handles IPv6 outbound |
| Public Subnets | **NO** | Lambda doesn't need public IP; uses Hyperplane ENI |

### Cost Analysis

| Component | Current (No VPC) | VPC + IPv6 | Notes |
|-----------|------------------|------------|-------|
| NAT Gateway | $0 | $0 | Not needed with IPv6 |
| Egress-Only IGW | N/A | **FREE** | IPv6 outbound gateway |
| VPC | N/A | **FREE** | No charge for VPC itself |
| Subnets | N/A | **FREE** | No charge for subnets |
| Lambda ENI | N/A | **FREE** | No charge for VPC ENIs |
| VPC Endpoint (API GW) | N/A | **NOT NEEDED** | API GW invokes Lambda directly |
| **Total Additional Cost** | **$0** | **~$0** | |

**Comparison with NAT Gateway approach:**
```
NAT Gateway:        $32.40/month × 2 AZ = $64.80/month
Data processing:    $0.045/GB
───────────────────────────────────────────────────────
Total:              ~$65+/month (avoided with IPv6)
```

### Cold Start Impact

VPC-attached Lambda functions experience longer cold starts due to ENI attachment.

| Scenario | Cold Start Time | Notes |
|----------|-----------------|-------|
| No VPC | ~200-500ms | Current architecture |
| VPC (modern, post-2019) | ~500-1000ms | AWS optimized ENI attachment |
| VPC + Provisioned Concurrency | ~0ms | Pre-warmed instances |

**Mitigation Options:**

1. **Accept cold starts**: For most API workloads, 500ms-1s occasional latency is acceptable
2. **Provisioned Concurrency**: ~$0.015/GB-hour, eliminates cold starts entirely
3. **Keep Lambda warm**: Schedule periodic invocations (not recommended for production)

### Configuration Preferences (Recorded)

The following preferences were recorded for future implementation:

| Setting | Choice | Rationale |
|---------|--------|-----------|
| Subnet Type | **Dual-stack** | Lambda requires dual-stack; IPv6-only not supported |
| Availability Zones | **2 AZs** | Standard high availability |
| Cold Start Tolerance | **Acceptable** | No Provisioned Concurrency needed initially |
| IPv6 Outbound | **Enabled** | Set `Ipv6AllowedForDualStack=true` on Lambda |

### Implementation Checklist

When implementing VPC architecture, the following components are needed:

- [ ] Create VPC with both IPv4 and IPv6 CIDR blocks
- [ ] Create Egress-Only Internet Gateway (for IPv6 outbound)
- [ ] Create private subnets in 2 AZs (dual-stack: IPv4 + IPv6)
- [ ] Configure route tables:
  - `::/0` → Egress-Only IGW (IPv6 outbound)
  - No route for `0.0.0.0/0` (blocks IPv4 outbound to internet)
- [ ] Create security group for Lambda:
  - Outbound: TCP 5432 to `::/0` (DSQL over IPv6)
  - Outbound: TCP 443 to AWS service endpoints (if needed)
- [ ] Update Lambda function with:
  - `VpcConfig.SubnetIds` - private subnet IDs
  - `VpcConfig.SecurityGroupIds` - security group ID
  - `VpcConfig.Ipv6AllowedForDualStack = true`
- [ ] Attach `AWSLambdaVPCAccessExecutionRole` policy to Lambda role
- [ ] Update `infra/components/vpc.go` (new file)
- [ ] Update `infra/components/serverless.go` (add VPC config)
- [ ] Update `infra/main.go` (wire VPC component)
- [ ] Test IPv6 connectivity to DSQL

### Security Considerations

| Aspect | No VPC | VPC + IPv6 |
|--------|--------|------------|
| Network isolation | Public internet | Private subnets |
| Inbound traffic | Blocked (Lambda) | Blocked (no IGW) |
| Outbound traffic | Via AWS network | Via Egress-Only IGW (IPv6 only) |
| Authentication | IAM + TLS | IAM + TLS |
| Compliance | May not meet strict requirements | Meets network isolation requirements |

### Understanding Hyperplane ENIs

When Lambda is configured with a VPC, it uses **Hyperplane Elastic Network Interfaces (ENIs)** to connect to your VPC:

1. **Shared across functions**: ENIs are shared among functions using the same subnet + security group combination
2. **Managed by Lambda**: You don't create or manage these ENIs; Lambda handles lifecycle
3. **Support 65,000 connections**: Each ENI supports up to 65,000 concurrent connections
4. **Auto-scaling**: Lambda automatically scales ENIs based on traffic
5. **Idle cleanup**: If unused for 14 days, Lambda reclaims ENIs and function becomes `Inactive`

**Required IAM Permissions** (via `AWSLambdaVPCAccessExecutionRole`):
```json
{
  "Effect": "Allow",
  "Action": [
    "ec2:CreateNetworkInterface",
    "ec2:DescribeNetworkInterfaces",
    "ec2:DescribeSubnets",
    "ec2:DeleteNetworkInterface",
    "ec2:AssignPrivateIpAddresses",
    "ec2:UnassignPrivateIpAddresses"
  ],
  "Resource": "*"
}
```

## Key Technical Decisions

### Decision 1: Aurora DSQL over RDS PostgreSQL

**Chosen**: Aurora DSQL

| Factor | Aurora DSQL | RDS PostgreSQL |
|--------|-------------|----------------|
| Serverless | Fully serverless | Serverless v2 available |
| VPC Required | No | Yes |
| IAM Auth | Native | Supported |
| Maintenance | Zero | Patches, upgrades |
| Scaling | Automatic | Manual or auto-scaling |
| Cost Model | Pay per request | Pay for capacity |

### Decision 2: No VPC (Current)

**Chosen**: Lambda runs outside VPC

**Rationale**:
- Simpler architecture
- Faster cold starts
- Zero networking costs
- DSQL provides IAM auth + TLS encryption

**Trade-off**: Traffic goes over public internet (encrypted)

### Decision 3: Pulumi with Go

**Chosen**: Pulumi (Go) over Terraform

**Rationale**:
- Type safety with Go
- Native language loops and conditionals
- Better IDE support
- Consistent with application language

### Decision 4: S3 Backend for State

**Chosen**: S3 over Pulumi Cloud

**Rationale**:
- Full control over state
- No external dependencies
- Cost effective
- State encryption with KMS

## File Structure

```
forma/
├── cmd/lambda/
│   ├── main.go          # Lambda entry point with DSQL auth
│   ├── handlers.go      # HTTP handlers
│   └── utils.go         # Utility functions
├── infra/
│   ├── main.go          # Pulumi main entry point
│   ├── go.mod           # Infra dependencies
│   ├── Pulumi.yaml      # Project config
│   ├── Pulumi.dev.yaml  # Dev environment config
│   ├── Pulumi.prod.yaml # Prod environment config
│   └── components/
│       ├── database.go   # Aurora DSQL cluster
│       ├── serverless.go # Lambda + API Gateway
│       └── storage.go    # S3 data lake bucket
├── .github/workflows/
│   ├── ci.yml           # CI pipeline
│   ├── cd-dev.yml       # Deploy to dev
│   └── cd-prod.yml      # Deploy to prod
└── scripts/
    └── bootstrap-pulumi-state.sh  # Initialize S3 state bucket
```

## Environment Variables

### Lambda Runtime Environment

| Variable | Description | Example |
|----------|-------------|---------|
| `DSQL_ENDPOINT` | Aurora DSQL cluster endpoint | `abc123.dsql.us-east-2.on.aws` |
| `AWS_REGION` | AWS region for auth token | `us-east-2` |
| `S3_BUCKET` | Data lake bucket name | `forma-dev-data-lake` |
| `ENVIRONMENT` | Environment name | `dev` or `prod` |
| `LOG_LEVEL` | Logging verbosity | `info` |

### Pulumi Configuration

| Config Key | Description | Dev | Prod |
|------------|-------------|-----|------|
| `aws:region` | AWS region | us-east-2 | us-east-2 |
| `forma-infra:environment` | Environment | dev | prod |
| `forma-infra:lambdaMemory` | Lambda memory (MB) | 1024 | 2048 |
| `forma-infra:lambdaTimeout` | Lambda timeout (sec) | 30 | 60 |

## References

- [Aurora DSQL Documentation](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/what-is-aurora-dsql.html)
- [Aurora DSQL Troubleshooting](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/troubleshooting.html)
- [Lambda in VPC](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)
- [Egress-Only Internet Gateway](https://docs.aws.amazon.com/vpc/latest/userguide/egress-only-internet-gateway.html)
- [Pulumi AWS Provider](https://www.pulumi.com/registry/packages/aws/)
