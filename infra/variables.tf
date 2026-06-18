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

# ---------------------------------------------------------------------------
# Input variables for the Delta Lake stored-procedure-chain replacement
# pipeline (AWS Glue 4.0 / PySpark / Delta Lake on S3 with S3DynamoDBLogStore).
#
# Values are supplied per environment by infra/envs/{dev,nonprod,prod}.tfvars.
# These variables are consumed by locals.tf (common_tags), providers.tf,
# glue_jobs.tf, iam.tf, dynamodb.tf, s3_objects.tf, and outputs.tf. Keep the
# variable names stable: the sibling .tf files reference them by name.
#
# Secrets/credentials are intentionally NOT declared here; sensitive values are
# resolved at runtime via IAM role assumption / parameter lookup, never stored
# in source or .tfvars. Remote-backend configuration lives in the per-env
# infra/envs/<env>-backend.hcl files, not as variables.
# ---------------------------------------------------------------------------

# ===========================================================================
# Tagging / identity
#
# These drive the five mandatory tags assembled into local.common_tags in
# infra/locals.tf (Environment, Project, Owner, CostCenter, ManagedBy). Gate 4
# requires all five tags on every resource. The fifth tag, ManagedBy, is the
# static literal "terraform" set in locals.tf and is therefore not a variable.
# ===========================================================================

variable "environment" {
  description = "Deployment environment token used in resource naming ({env}-...) and the Environment tag. Must be one of dev, nonprod, or prod."
  type        = string

  validation {
    condition     = contains(["dev", "nonprod", "prod"], var.environment)
    error_message = "The environment value must be one of: dev, nonprod, prod."
  }
}

variable "project" {
  description = "Project identifier used for the Project tag and as a naming prefix component."
  type        = string
  default     = "delta-sp-chain-replacement"
}

variable "owner" {
  description = "Owning team or individual for the Owner tag (organization-specific; no safe cross-environment default)."
  type        = string
}

variable "cost_center" {
  description = "Cost center accounting code for the CostCenter tag (organization-specific; no safe cross-environment default)."
  type        = string
}

# ===========================================================================
# Region
# ===========================================================================

variable "aws_region" {
  description = "AWS region in which all pipeline resources are managed (sourced from AWS_REGION). Used by the AWS provider, the DynamoDB region Spark conf, and region-scoped IAM ARNs."
  type        = string
}

# ===========================================================================
# S3 buckets
#
# All buckets are pre-existing and externally owned. This configuration only
# adds new prefix-scoped objects and IAM policy statements; it never creates or
# destroys these buckets.
# ===========================================================================

variable "delta_s3_bucket" {
  description = "Name of the S3 bucket (DELTA_S3_BUCKET) holding the Delta tables and the bad-record quarantine prefix."
  type        = string
}

variable "artifact_s3_bucket" {
  description = "Name of the S3 bucket (ARTIFACT_S3_BUCKET) holding the uploaded Delta JARs, the delta-spark wheel, the Glue job scripts, the lib/schemas zips, and the config YAMLs."
  type        = string
}

variable "mwaa_dag_s3_bucket" {
  description = "Name of the S3 bucket (MWAA_DAG_S3_BUCKET) backing the pre-provisioned MWAA environment's DAG folder; the pipeline DAGs are uploaded here."
  type        = string
}

variable "source_s3_bucket" {
  description = "Name of the S3 bucket containing the delimited source flat files (the bucket portion of PIPELINE_SOURCE_S3_PREFIX). Used for stage-0's --source_s3_path argument and for IAM read scoping."
  type        = string
}

variable "pipeline_source_s3_prefix" {
  # Key prefix only (e.g. incoming/finance/sp_chain_replacement). No leading or
  # trailing slash. Combined with source_s3_bucket to form the s3a:// source URI.
  description = "Key prefix within source_s3_bucket where the delimited source files land. No leading/trailing slash; combined with source_s3_bucket to form the s3a:// source URI."
  type        = string
}

# ===========================================================================
# AWS Glue runtime knobs
#
# The Glue version itself is pinned to "4.0" (Apache Spark 3.3.x, Python 3.10)
# directly in glue_jobs.tf and is intentionally not a variable.
# ===========================================================================

variable "glue_worker_type" {
  description = "Glue worker type for every job in the pipeline. One of G.1X, G.2X, G.4X, or G.8X."
  type        = string
  default     = "G.2X"

  validation {
    condition     = contains(["G.1X", "G.2X", "G.4X", "G.8X"], var.glue_worker_type)
    error_message = "The glue_worker_type value must be one of: G.1X, G.2X, G.4X, G.8X."
  }
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers allocated to each job in the pipeline."
  type        = number
  default     = 10
}

# ===========================================================================
# DynamoDB coordination table
#
# Backs io.delta.storage.S3DynamoDBLogStore for multi-cluster ACID commits.
# This is a net-new resource created by this configuration. The default RCU/WCU
# match the reference integration test (ReadCapacityUnits=5, WriteCapacityUnits=5).
# ===========================================================================

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB coordination table for S3DynamoDBLogStore (environment-specific). Fed to the spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName conf and the --ddb_table_name job argument."
  type        = string
}

variable "dynamodb_rcu" {
  description = "Provisioned read capacity units for the DynamoDB coordination table."
  type        = number
  default     = 5
}

variable "dynamodb_wcu" {
  description = "Provisioned write capacity units for the DynamoDB coordination table."
  type        = number
  default     = 5
}
