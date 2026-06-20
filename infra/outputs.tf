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
# infra/outputs.tf
#
# Root-module outputs for the net-new, additive AWS Glue 4.0 / PySpark + Delta
# Lake stored-procedure-chain replacement pipeline (AAP 0.5.1 Group F). These
# outputs publish the identifiers that downstream consumers need WITHOUT them
# having to re-derive any string:
#
#   * the validate/ harness (Gate 1 parity + Gate 5 security) reads an env-var
#     contract -- AWS_REGION, DELTA_S3_BUCKET, DELTA_DDB_TABLE_NAME,
#     GLUE_ROLE_ARN / GLUE_ROLE_NAME, and IAM_POLICY_JSON_PATH -- that operators
#     populate from these very outputs;
#   * operators wiring the pre-provisioned MWAA environment use mwaa_dag_bucket /
#     mwaa_dag_key to confirm where the DAG was deployed; and
#   * the MWAA DAG references the five glue_job_names when building one
#     GlueJobOperator per stage.
#
# STYLE: mirrors the read-only convention anchor
# benchmarks/infrastructure/aws/terraform/outputs.tf (`output "x" { value = ...}`),
# with a `description` added to every output for operator/self-documentation.
#
# CRITICAL (AAP 0.7.3 secrets policy): NO output exposes a secret or credential.
# Every value below is a resource name, ARN, S3 bucket/prefix, region, or the
# rendered least-privilege IAM policy JSON (which itself contains only ARNs and
# actions -- never a credential). Each value REFERENCES a real resource attribute
# (or an input var / computed local) so the output reflects the actually-created
# identifier rather than a re-derived guess.
#
# STABLE NAMES: the output names below are a CONTRACT. validate/test_parity.py
# and the operator runbooks key off them, so they must not be renamed.
#
# Minimal Change Mandate: this is a pure, additive CREATE; no existing repository
# file is modified.
# -----------------------------------------------------------------------------

# ===========================================================================
# (1) AWS Glue jobs (infra/glue_jobs.tf -> aws_glue_job.stage).
#
# aws_glue_job.stage is a for_each map keyed by the manifest `step` token, so
# the comprehensions below iterate that map. A `for` over a map yields elements
# ordered by the (sorted) key, so glue_job_names is deterministic across runs.
# ===========================================================================

output "glue_job_names" {
  description = "List of the AWS Glue job names created for the pipeline (one per manifest stage, ordered by step key). The MWAA DAG references these exact names when constructing each GlueJobOperator. Format: {env}-finance-sp-chain-replacement-{step}."
  value       = [for k, j in aws_glue_job.stage : j.name]
}

output "glue_job_arns" {
  description = "Map of pipeline step token => AWS Glue job ARN (e.g. \"stage-0-ingest\" => arn:<partition>:glue:<region>:<account>:job/<env>-finance-sp-chain-replacement-stage-0-ingest)."
  value       = { for k, j in aws_glue_job.stage : k => j.arn }
}

# ===========================================================================
# (2) IAM execution role + least-privilege policy (infra/iam.tf).
#
# glue_role_arn / glue_role_name drive the GLUE_ROLE_ARN / GLUE_ROLE_NAME env
# vars; Gate 5 (validate/test_parity.py) feeds glue_role_arn to
# `aws iam simulate-principal-policy` as the --policy-source-arn, and can write
# glue_policy_json to IAM_POLICY_JSON_PATH to assert there are no "*" resources.
# ===========================================================================

output "glue_role_arn" {
  description = "ARN of the least-privilege AWS Glue execution role (GLUE_ROLE_ARN env var). Gate 5 uses this as the --policy-source-arn for `aws iam simulate-principal-policy`."
  value       = aws_iam_role.glue.arn
}

output "glue_role_name" {
  description = "Name of the AWS Glue execution role (GLUE_ROLE_NAME env var consumed by the validate/ Gate 5 harness)."
  value       = aws_iam_role.glue.name
}

output "glue_policy_arn" {
  description = "ARN of the customer-managed least-privilege IAM policy attached to the Glue execution role."
  value       = aws_iam_policy.glue.arn
}

# The rendered least-privilege policy JSON. The Gate 5 harness can materialize it
# with `terraform output -raw glue_policy_json > "$IAM_POLICY_JSON_PATH"` and then
# assert no statement uses a "*" Resource (every statement in infra/iam.tf is
# scoped to a specific or path-scoped ARN). This document contains only ARNs and
# actions -- no credential -- so it is explicitly NOT sensitive; marking it
# sensitive would break `terraform output -raw` for the Gate 5 cross-check.
output "glue_policy_json" {
  description = "Rendered least-privilege IAM policy JSON for the Glue role. The Gate 5 harness writes it to IAM_POLICY_JSON_PATH (terraform output -raw glue_policy_json > policy.json) to assert there are no \"*\" Resource ARNs."
  value       = data.aws_iam_policy_document.glue.json
  sensitive   = false
}

# ===========================================================================
# (3) DynamoDB coordination table (infra/dynamodb.tf).
#
# Backs io.delta.storage.S3DynamoDBLogStore. dynamodb_table_name drives the
# DELTA_DDB_TABLE_NAME env var used by the validate/ harness.
# ===========================================================================

output "dynamodb_table_name" {
  description = "Name of the DynamoDB coordination table backing io.delta.storage.S3DynamoDBLogStore (DELTA_DDB_TABLE_NAME env var)."
  value       = aws_dynamodb_table.coordination.name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB coordination table used for the S3DynamoDBLogStore conditional-write commit protocol."
  value       = aws_dynamodb_table.coordination.arn
}

# ===========================================================================
# (4) S3 buckets and key prefixes (infra/variables.tf + infra/locals.tf +
#     infra/s3_objects.tf).
#
# The bucket names are passed through from the input variables (the buckets are
# pre-existing and externally owned); the prefix/key values come from the shared
# locals so they match exactly where infra/s3_objects.tf uploads each object.
# ===========================================================================

output "delta_bucket" {
  description = "Name of the S3 bucket holding the Delta tables and the bad-record quarantine prefix (DELTA_S3_BUCKET env var)."
  value       = var.delta_s3_bucket
}

output "artifact_bucket" {
  description = "Name of the S3 bucket holding the staged Delta JARs, the delta-spark wheel, the Glue job scripts, the lib/schemas zips, and the config YAMLs (ARTIFACT_S3_BUCKET)."
  value       = var.artifact_s3_bucket
}

output "artifact_code_prefix" {
  description = "Key prefix within artifact_bucket where the Glue job scripts and the lib.zip / schemas.zip packages are uploaded (matches infra/s3_objects.tf upload keys and the glue_jobs.tf script_location)."
  value       = local.code_prefix
}

output "mwaa_dag_bucket" {
  description = "Name of the S3 bucket backing the pre-provisioned MWAA environment's DAG folder, where the pipeline DAG is deployed (MWAA_DAG_S3_BUCKET)."
  value       = var.mwaa_dag_s3_bucket
}

output "mwaa_dag_key" {
  description = "Object key of the deployed pipeline DAG within mwaa_dag_bucket (matches the aws_s3_object.dag upload key in infra/s3_objects.tf)."
  value       = "${local.mwaa_dag_prefix}/sp_chain_replacement_dag.py"
}

# ===========================================================================
# (5) Region and naming (infra/variables.tf + infra/locals.tf).
# ===========================================================================

output "aws_region" {
  description = "AWS region in which all pipeline resources are managed (AWS_REGION env var)."
  value       = var.aws_region
}

output "name_prefix" {
  description = "The {env}-{domain}-{pipeline_name} resource-name stem shared by every pipeline resource (e.g. dev-finance-sp-chain-replacement); each Glue job name appends -{step} to it."
  value       = local.name_prefix
}
