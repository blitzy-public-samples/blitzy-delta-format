#
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -----------------------------------------------------------------------------
# infra/providers.tf
#
# Provider configuration for the net-new, additive AWS Glue 4.0 / PySpark /
# Delta Lake stored-procedure-chain replacement pipeline (AAP 0.5.1 Group F).
#
# This file ONLY configures the already-declared AWS provider and declares the
# shared account/partition data sources. The Terraform settings block
# (required_version, required_providers, and the partial S3 backend) lives in
# infra/versions.tf and is intentionally NOT repeated here. The hashicorp/archive
# provider (declared in versions.tf and used by infra/s3_objects.tf) needs no
# configuration block, so none is present.
#
# Convention anchor (read-only, never edited):
# benchmarks/infrastructure/aws/terraform/providers.tf, which is exactly
# `provider "aws" { region = var.region; default_tags { tags = var.tags } }`. We
# mirror that pattern but source the region from var.aws_region and the tags from
# local.common_tags.
#
# Credentials are intentionally NOT configured here (no assume_role / access-key
# blocks): per AAP 0.7.3 they are resolved at runtime via the execution role's
# IAM role assumption / the standard provider credential chain, never stored in
# source.
# -----------------------------------------------------------------------------

# AWS provider for every resource in this configuration. The region is sourced
# from var.aws_region (AWS_REGION) and is never hardcoded, so the same code base
# deploys unchanged across dev / nonprod / prod.
#
# default_tags propagates the five mandatory tags assembled in local.common_tags
# (Environment, Project, Owner, CostCenter, and the static ManagedBy = "terraform")
# to every taggable resource as a safety net for Gate 4 (mandatory tagging). Each
# taggable resource ALSO sets `tags = local.common_tags` explicitly for
# unambiguous, provable per-resource tagging; because both tag sources carry
# identical values, the aws ~> 5.0 provider resolves the overlap to the same
# result and produces no perpetual plan diff (Gate 4 drift check).
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

# Shared identity / partition data sources.
#
# These are consumed by infra/iam.tf to build least-privilege, fully-qualified
# resource ARNs with NO wildcard ("*") resources, which Gate 5 requires. Using the
# live account id and partition keeps the generated IAM policy ARNs exact and
# portable rather than guessed or wildcarded.

# Exposes the current AWS account id (data.aws_caller_identity.current.account_id),
# used to qualify ARNs such as the CloudWatch Logs log-group ARNs in infra/iam.tf.
data "aws_caller_identity" "current" {}

# Exposes the current AWS partition (data.aws_partition.current.partition) so ARNs
# are written as `arn:${data.aws_partition.current.partition}:...`, keeping them
# portable across the standard aws, aws-us-gov, and aws-cn partitions instead of
# hardcoding "aws".
data "aws_partition" "current" {}
