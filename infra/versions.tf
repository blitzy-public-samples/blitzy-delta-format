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
# infra/versions.tf
#
# Terraform settings block for the net-new, additive AWS Glue 4.0 / Delta Lake
# pipeline (AAP 0.5.1 Group F). This file pins the Terraform CLI version,
# declares the required providers (AWS + archive), and declares an empty S3
# backend for partial backend configuration.
#
# Minimal Change Mandate: this is a pure, additive CREATE; no existing repository
# file is modified. The convention anchor (read-only, never edited) is
# benchmarks/infrastructure/aws/terraform/versions.tf; we deliberately deviate to
# the hashicorp/aws 5.x line per AAP 0.3.1.
#
# This file intentionally contains NO provider block (that lives in
# infra/providers.tf) and does NOT pin the Glue runtime version (that is pinned
# to "4.0" in infra/glue_jobs.tf).
# -----------------------------------------------------------------------------

terraform {
  # Modern Terraform is required for the language features used across the sibling
  # infra/*.tf files (for example yamldecode(), for_each, and optional object
  # type attributes).
  # NOTE: confirm the exact CLI version pin with the platform team.
  required_version = ">= 1.5.0"

  required_providers {
    # AAP 0.3.1 mandates the hashicorp/aws 5.x line (supports aws_glue_job for
    # Glue 4.0, aws_dynamodb_table TTL, and provider default_tags).
    # AAP 0.3.1: 5.x recommended; confirm exact pin with platform team.
    # (Repo benchmark uses ~> 4.15.1 for an unrelated module.)
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }

    # REQUIRED by infra/s3_objects.tf, which uses data "archive_file" to zip the
    # lib/ and schemas/ Python packages (preserving the package directory prefix)
    # before uploading them to ARTIFACT_S3_BUCKET for the Glue --extra-py-files
    # job argument.
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Partial backend configuration. The concrete bucket / key / region /
  # dynamodb_table / encrypt values are intentionally omitted here and supplied
  # per environment at init time via:
  #
  #   terraform init -backend-config=envs/<env>-backend.hcl
  #
  # (those per-environment .hcl files are authored under infra/envs/.) Keeping
  # the values out of this version-pinned source makes the state configuration
  # environment-specific and avoids committing environment coordinates.
  #
  # State locking uses a DynamoDB table declared in the backend .hcl file; that
  # state-lock table is SEPARATE from the io.delta.storage.S3DynamoDBLogStore
  # coordination table used by the Delta Lake jobs at runtime.
  backend "s3" {}
}
