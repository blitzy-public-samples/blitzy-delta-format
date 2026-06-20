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
# infra/dynamodb.tf
#
# Provisions the single net-new AWS resource that backs the Delta Lake
# multi-cluster ACID commit protocol for this pipeline: the DynamoDB
# coordination table consumed by io.delta.storage.S3DynamoDBLogStore
# (AAP 0.5.1 Group F; AAP 0.7.1 ACID convention).
#
# WHY THIS TABLE EXISTS
# ---------------------
# Amazon S3 provides no atomic "put-if-absent" primitive, so concurrent Delta
# writers cannot rely on S3 alone to guarantee a single winner for each
# _delta_log/N.json commit. The S3DynamoDBLogStore closes that gap: every Delta
# commit is first recorded in this DynamoDB table via a *conditional* write
# (mutual exclusion on the (tablePath, fileName) primary key). The conditional
# write is the linearization point that gives every Delta write its ACID
# guarantee. A failed conditional write surfaces as an exception in the Glue job
# and the job exits non-zero -- there is no non-ACID fallback path
# (AAP 0.7.1 ACID strictness).
#
# AUTHORITATIVE REFERENCE (read-only, never edited)
# -------------------------------------------------
# The exact table shape below is reproduced 1:1 from the repository's own
# integration test, which creates the coordination table with the AWS CLI:
#
#   storage-s3-dynamodb/integration_tests/dynamodb_logstore.py:L28-L42
#     aws dynamodb create-table
#       --attribute-definitions AttributeName=tablePath,AttributeType=S
#                               AttributeName=fileName,AttributeType=S
#       --key-schema            AttributeName=tablePath,KeyType=HASH
#                               AttributeName=fileName,KeyType=RANGE
#       --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
#     aws dynamodb update-time-to-live
#       --time-to-live-specification "Enabled=true, AttributeName=expireTime"
#
# The LogStore client hard-codes these key names/types and the expireTime TTL
# attribute, so they MUST match the reference EXACTLY; renaming or re-typing any
# of them breaks commit coordination at runtime.
#
# NOTE: this Delta coordination table is entirely SEPARATE from the Terraform
# state-lock DynamoDB table referenced by infra/envs/<env>-backend.hcl; the two
# serve unrelated purposes and never share a table.
# -----------------------------------------------------------------------------

# DynamoDB coordination table for io.delta.storage.S3DynamoDBLogStore.
#
# Every Delta write performed by the Glue jobs (jobs/stage_*.py) commits through
# this table; the table name is also handed to Spark as
# spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName (see infra/locals.tf
# local.delta_spark_conf and lib/spark_session.py) so the resource here and the
# runtime LogStore configuration always agree on the same table.
resource "aws_dynamodb_table" "coordination" {
  # Environment-specific table name (e.g. "prod-finance-sp-chain-replacement-
  # logstore"), supplied per environment via infra/envs/<env>.tfvars and passed
  # to the S3DynamoDBLogStore ddb.tableName Spark conf so writers and this
  # resource reference an identical table.
  name = var.dynamodb_table_name

  # Provisioned capacity, matching the reference create-table call
  # (dynamodb_logstore.py:L35 ReadCapacityUnits=5, WriteCapacityUnits=5). The
  # defaults of var.dynamodb_rcu / var.dynamodb_wcu are 5/5; PROVISIONED (not
  # PAY_PER_REQUEST) is required so the deployed capacity exactly mirrors the
  # reference and produces zero `terraform plan` drift (Gate 4).
  billing_mode   = "PROVISIONED"
  read_capacity  = var.dynamodb_rcu
  write_capacity = var.dynamodb_wcu

  # Composite primary key (dynamodb_logstore.py:L33-L34):
  #   tablePath = HASH  (partition key) -- the Delta table's storage path
  #   fileName  = RANGE (sort key)      -- the _delta_log/N.json being committed
  # Together they uniquely identify a single commit attempt; the conditional
  # write on this key is what enforces single-writer mutual exclusion.
  hash_key  = "tablePath"
  range_key = "fileName"

  # Attribute definition for the HASH key (dynamodb_logstore.py:L31).
  # Only key attributes are declared to DynamoDB; non-key item fields written by
  # the LogStore (e.g. tempPath, complete, expireTime) are schemaless.
  attribute {
    name = "tablePath"
    type = "S"
  }

  # Attribute definition for the RANGE key (dynamodb_logstore.py:L32).
  attribute {
    name = "fileName"
    type = "S"
  }

  # Time-to-live (dynamodb_logstore.py:L39-L42:
  # "Enabled=true, AttributeName=expireTime"). The LogStore stamps each
  # completed commit item with an expireTime epoch; DynamoDB lazily evicts
  # expired items so the coordination table self-prunes and does not grow
  # unbounded. The attribute name MUST be exactly "expireTime".
  ttl {
    attribute_name = "expireTime"
    enabled        = true
  }

  # The five mandatory tags assembled in infra/locals.tf
  # (Environment, Project, Owner, CostCenter, ManagedBy = "terraform"). Set
  # explicitly here in addition to the provider-level default_tags so this
  # resource is provably tagged for Gate 4; because both tag sources carry
  # identical values the aws ~> 5.0 provider resolves the overlap to the same
  # result and produces no perpetual plan diff.
  tags = local.common_tags
}
