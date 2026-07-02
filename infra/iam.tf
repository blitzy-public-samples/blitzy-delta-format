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
# infra/iam.tf
#
# The AWS Glue execution role and its LEAST-PRIVILEGE, customer-managed
# permissions policy for the net-new, additive AWS Glue 4.0 / PySpark + Delta
# Lake stored-procedure-chain replacement pipeline (AAP 0.5.1 Group F).
#
# GATE 5 (Security) — NON-NEGOTIABLE
# ----------------------------------
# The rendered IAM policy JSON MUST contain ZERO wildcard ("*") RESOURCE ARNs,
# and the role MUST pass `aws iam simulate-principal-policy` for the in-scope
# actions. validate/test_parity.py asserts this using the glue_role_arn /
# glue_role_name outputs (declared in infra/outputs.tf). To satisfy this:
#
#   * EVERY statement below scopes `resources` to SPECIFIC ARNs only — a single
#     bucket ARN, an object-key-prefix ARN (`.../<prefix>/*`), the one DynamoDB
#     coordination table ARN, or the `/aws-glue/*` log-group ARN. No statement
#     uses `resources = ["*"]`.
#   * Object-prefix ARNs that end in `/*` (e.g. `bucket/finance/...*`) and the
#     `log-group:/aws-glue/*` ARN are RESOURCE-PATH-SCOPED ARNs. They are NOT the
#     forbidden bare `Resource: "*"`; the `/*` is a key/path suffix WITHIN an
#     otherwise fully-qualified ARN, which is exactly how S3 object permissions
#     and CloudWatch Logs log-group permissions are expressed.
#   * `s3:ListBucket` is a BUCKET-level action, so its `resources` is the bucket
#     ARN (no `/*`); the listable key space is constrained with a `StringLike`
#     `s3:prefix` condition instead of widening the resource.
#   * We deliberately AUTHOR a customer-managed least-privilege policy rather than
#     attach the AWS-managed `AWSGlueServiceRole`, because that managed policy
#     contains `Resource: "*"` statements and would fail Gate 5.
#
# ARN PORTABILITY
# ---------------
# All ARNs are built as `arn:${data.aws_partition.current.partition}:...` so the
# configuration stays correct across the standard `aws`, `aws-us-gov`, and
# `aws-cn` partitions. The account id (for the region-scoped Logs ARN) comes from
# `data.aws_caller_identity.current.account_id`. Both data sources are declared
# in infra/providers.tf.
#
# TAGGING (Gate 4)
# ----------------
# `aws_iam_role` and `aws_iam_policy` are taggable and carry the five mandatory
# tags via `tags = local.common_tags` (assembled in infra/locals.tf). The
# `aws_iam_role_policy_attachment` resource is NOT taggable and therefore sets no
# tags.
#
# Minimal Change Mandate: this is a pure, additive CREATE; pre-existing S3
# buckets and the DynamoDB table are referenced by ARN only and never created or
# modified here.
# -----------------------------------------------------------------------------

# ===========================================================================
# (1) Assume-role trust policy.
#
# Restricts who may assume this role to the AWS Glue service principal ONLY.
# AWS Glue assumes this role when it runs each jobs/stage_*.py script
# (infra/glue_jobs.tf sets `role_arn = aws_iam_role.glue.arn`). No human, account,
# or other service principal is trusted.
# ===========================================================================
data "aws_iam_policy_document" "glue_assume" {
  statement {
    sid     = "GlueAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    # Service principal trust: only glue.amazonaws.com can assume the role.
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

# ===========================================================================
# (2) The Glue execution role.
#
# Assumed by every Glue job in the pipeline. Its name follows the manifest
# naming convention "{env}-{domain}-{pipeline_name}" via local.name_prefix
# (e.g. "dev-finance-sp-chain-replacement-glue-role").
# ===========================================================================
resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json

  # Five mandatory tags (Gate 4), assembled in infra/locals.tf.
  tags = local.common_tags
}

# ===========================================================================
# (3) Least-privilege permissions policy document.
#
# Every statement's `resources` is a SPECIFIC ARN (Gate 5). The statements grant
# exactly — and only — the access the pipeline needs:
#   * read the source flat files,
#   * read the staged JARs/wheel/code/config artifacts,
#   * read/write the Glue --TempDir scratch space,
#   * read/write the Delta tables and the bad-record quarantine prefix,
#   * run the S3DynamoDBLogStore conditional-write commit protocol against the
#     ONE coordination table, and
#   * emit continuous logs to the Glue CloudWatch Logs log group.
# ===========================================================================
data "aws_iam_policy_document" "glue" {

  # -------------------------------------------------------------------------
  # Source bucket — read the delimited source flat files (stage 0 input).
  # GetObject is scoped to the object-key prefix where files land.
  # -------------------------------------------------------------------------
  statement {
    sid     = "SourceBucketRead"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "arn:${data.aws_partition.current.partition}:s3:::${var.source_s3_bucket}/${var.pipeline_source_s3_prefix}/*",
    ]
  }

  # ListBucket is a bucket-level action: the resource is the bucket ARN (no
  # `/*`), and the listable key space is constrained to the source prefix via an
  # `s3:prefix` condition rather than by widening the resource.
  statement {
    sid       = "SourceBucketList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.source_s3_bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.pipeline_source_s3_prefix}/*"]
    }
  }

  # -------------------------------------------------------------------------
  # Artifact bucket — read the staged Delta JARs, the delta-spark wheel, the job
  # scripts, the lib/schemas zips, and the config YAMLs. Glue downloads these at
  # job start via --extra-jars, --additional-python-modules, --extra-py-files,
  # --extra-files, and script_location. All live under local.artifact_root.
  # -------------------------------------------------------------------------
  statement {
    sid     = "ArtifactBucketRead"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "arn:${data.aws_partition.current.partition}:s3:::${var.artifact_s3_bucket}/${local.artifact_root}/*",
    ]
  }

  statement {
    sid       = "ArtifactBucketList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.artifact_s3_bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.artifact_root}/*"]
    }
  }

  # Glue --TempDir scratch space (read/write/delete). Scoped to the temp prefix
  # only; the broader ArtifactBucketRead above grants read of all artifacts but
  # NOT write, so write access is confined here to the disposable temp prefix
  # (least privilege).
  statement {
    sid    = "ArtifactTempReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:s3:::${var.artifact_s3_bucket}/${local.temp_prefix}/*",
    ]
  }

  # -------------------------------------------------------------------------
  # Delta bucket — read/write the Delta tables (data files + _delta_log +
  # compaction) AND the bad-record quarantine prefix that stage 0 routes
  # malformed records to. Two specific object-prefix ARNs; never the whole
  # bucket.
  # -------------------------------------------------------------------------
  statement {
    sid    = "DeltaBucketReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:s3:::${var.delta_s3_bucket}/${local.delta_path_prefix}/*",
      "arn:${data.aws_partition.current.partition}:s3:::${var.delta_s3_bucket}/${local.quarantine_prefix}/*",
    ]
  }

  statement {
    sid       = "DeltaBucketList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.delta_s3_bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${local.delta_path_prefix}/*",
        "${local.quarantine_prefix}/*",
      ]
    }
  }

  # -------------------------------------------------------------------------
  # DynamoDB coordination table — the io.delta.storage.S3DynamoDBLogStore
  # conditional-write commit protocol. Every Delta commit is linearized through
  # a conditional write on the (tablePath, fileName) key of this ONE table
  # (storage-s3-dynamodb/integration_tests/dynamodb_logstore.py:L53,L77); a
  # failed conditional write raises and the job exits non-zero (no non-ACID
  # fallback). The resource is the exact table ARN — never a wildcard.
  # -------------------------------------------------------------------------
  statement {
    sid    = "DynamoDbCoordination"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
      "dynamodb:DescribeTable",
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
    ]
    resources = [aws_dynamodb_table.coordination.arn]
  }

  # -------------------------------------------------------------------------
  # CloudWatch Logs — Glue continuous logging. Scoped to the Glue log group
  # path ONLY. `/aws-glue/*` is a RESOURCE-PATH-SCOPED ARN (a log-group name
  # prefix within a fully-qualified region/account ARN), which satisfies Gate 5
  # ("no `*` RESOURCE"); it is NOT `Resource: "*"`.
  # -------------------------------------------------------------------------
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:AssociateKmsKey",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
    ]
  }

  # -------------------------------------------------------------------------
  # DELIBERATE EXCLUSION — cloudwatch:PutMetricData.
  #
  # The CloudWatch metrics API cannot be resource-scoped (PutMetricData only
  # supports a `cloudwatch:namespace` condition, never a resource ARN), so
  # granting it would require `Resource: "*"` and FAIL Gate 5. The mandated
  # six-field completion event (job name, Glue run id, input rows, output rows,
  # bad-record count, elapsed seconds) is emitted as structured stdout captured
  # by CloudWatch LOGS (via lib/logging_utils.py + Glue continuous logging),
  # NOT as a custom CloudWatch metric, so this permission is not required.
  #
  # DELIBERATE EXCLUSION — AWS Glue Data Catalog permissions (glue:Get*, etc.).
  #
  # The pipeline reads and writes Delta tables exclusively via S3 PATHS
  # (s3a://.../<table>) over the S3DynamoDBLogStore, not through the Glue Data
  # Catalog. Catalog permissions are therefore unnecessary and are omitted to
  # keep the policy least-privilege.
  # -------------------------------------------------------------------------
}

# ===========================================================================
# (4) Customer-managed policy + attachment.
#
# The rendered least-privilege document above is published as a managed policy
# and attached to the Glue execution role.
# ===========================================================================
resource "aws_iam_policy" "glue" {
  name   = "${local.name_prefix}-glue-policy"
  policy = data.aws_iam_policy_document.glue.json

  # Five mandatory tags (Gate 4).
  tags = local.common_tags
}

# Bind the least-privilege policy to the Glue execution role. The attachment
# resource is not taggable, so it carries no tags.
resource "aws_iam_role_policy_attachment" "glue" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue.arn
}
