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
# infra/envs/prod.tfvars
#
# PROD per-environment input VALUES for the net-new, additive AWS Glue 4.0 /
# PySpark / Delta Lake stored-procedure-chain replacement pipeline (AAP Group F).
# Consumed at plan/apply time:
#
#   terraform plan  -var-file=envs/prod.tfvars
#   terraform apply -var-file=envs/prod.tfvars
#
# This file assigns VALUES ONLY. Every assignment below corresponds to a
# `variable` declared in the sibling, authoritative infra/variables.tf; it
# declares NO variables and provisions NO resources (no variable/resource/
# terraform/provider blocks). Per the Minimal Change Mandate it is a pure CREATE.
#
# The fifth mandatory tag, ManagedBy, is the static literal "terraform" assembled
# in infra/locals.tf and is intentionally NOT a variable, so it is not set here.
#
# Sensitive-value policy (AAP 0.7.3): this file holds only non-sensitive
# configuration values. No credentials of any kind are embedded here (no
# long-lived keys, named profiles, or role-assumption settings); authentication
# is resolved at runtime via the IAM role / default credential chain.
#
# PROD scale-up: relative to dev/nonprod, production raises Glue compute
# (glue_worker_type = "G.4X", glue_number_of_workers = 20) and DynamoDB
# coordination-table throughput (dynamodb_rcu = 25, dynamodb_wcu = 25, above the
# 5/5 reference baseline) for production commit/write volume, and uses a distinct
# prod cost_center.
#
# NOTE: the placeholder org-specific values below (the four S3 bucket names,
# owner, cost_center, and aws_region) follow the {env}-finance-sp-chain-replacement-*
# convention, and the prod RCU/WCU and worker sizing above are initial estimates;
# all of these should be CONFIRMED WITH THE PLATFORM TEAM before prod cutover.
# -----------------------------------------------------------------------------

# --- Tagging / identity (drive the five mandatory tags via infra/locals.tf; Gate 4) ---
environment = "prod"
project     = "delta-sp-chain-replacement"
owner       = "finance-data-engineering"
cost_center = "FIN-DATAENG-2001"

# --- AWS region (provider region; DynamoDB ddb.region Spark conf; region-scoped IAM ARNs) ---
aws_region = "us-east-1"

# --- Pre-existing S3 buckets (only new prefix-scoped objects/policies are added; never created here) ---
delta_s3_bucket    = "prod-finance-sp-chain-replacement-delta"
artifact_s3_bucket = "prod-finance-sp-chain-replacement-artifacts"
mwaa_dag_s3_bucket = "prod-finance-sp-chain-replacement-mwaa-dags"
source_s3_bucket   = "prod-finance-sp-chain-replacement-source"

# Source flat-file key prefix within source_s3_bucket (NO leading/trailing slash;
# combined as s3a://<source_s3_bucket>/<pipeline_source_s3_prefix> by infra/locals.tf).
pipeline_source_s3_prefix = "incoming/finance/sp_chain_replacement"

# --- AWS Glue runtime knobs (Glue version itself is pinned to 4.0 in infra/glue_jobs.tf; prod scale-up) ---
glue_worker_type       = "G.4X"
glue_number_of_workers = 20

# --- DynamoDB S3DynamoDBLogStore coordination table (env-specific; the LogStore table, NOT the state-lock table) ---
# RCU/WCU raised above the 5/5 reference baseline for prod commit throughput (tunable).
dynamodb_table_name = "prod-finance-sp-chain-replacement-logstore"
dynamodb_rcu        = 25
dynamodb_wcu        = 25
