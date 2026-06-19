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
# infra/envs/dev-backend.hcl
#
# DEV partial S3 remote-backend configuration. Supplies the concrete values for
# the empty `backend "s3" {}` block declared in the sibling infra/versions.tf.
#
# Used at init time:
#
#   terraform init -backend-config=envs/dev-backend.hcl
#
# This is a FLAT key/value partial-backend file ONLY: it contains the backend
# argument assignments and nothing else. It intentionally has NO `terraform {}`,
# NO `backend "s3" {}`, NO `provider {}`, and NO `resource {}` block - Terraform
# `-backend-config` files are flat `key = value` assignments that are merged into
# the backend block at init time.
#
# Backend authentication uses the runtime's IAM role / default credential chain;
# no static AWS credentials are embedded here (no long-lived keys, named profiles,
# or role-assumption settings) per AAP 0.7.3 (secrets management).
#
# NOTE: the remote state bucket and the state-lock DynamoDB table referenced below
# are OPERATIONAL PREREQUISITES - they are expected to pre-exist (or be created
# out-of-band by the platform/ops team) and are SEPARATE from the pipeline's own
# data resources (the DELTA / ARTIFACT / MWAA buckets and the S3DynamoDBLogStore
# coordination table provisioned by this Terraform module). Confirm the exact
# bucket name, lock-table name, and region with the platform team.
# -----------------------------------------------------------------------------

bucket         = "dev-finance-sp-chain-replacement-tfstate"   # Remote Terraform STATE bucket (ops/state bucket; separate from the pipeline data buckets in dev.tfvars).
key            = "sp-chain-replacement/dev/terraform.tfstate" # Env-specific state object path (key) within the state bucket.
region         = "us-east-1"                                  # AWS region hosting the state bucket and the state-lock table.
dynamodb_table = "dev-finance-sp-chain-replacement-tflock"    # Terraform STATE-LOCK table. DISTINCT from the S3DynamoDBLogStore coordination table (dev-finance-sp-chain-replacement-logstore, set as dynamodb_table_name in dev.tfvars). Do NOT reuse that value.
encrypt        = true                                         # Enforce server-side encryption (SSE) of the state object at rest.
