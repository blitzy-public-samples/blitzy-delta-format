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
# infra/s3_objects.tf
#
# Uploads every RUNTIME artifact for the net-new, additive AWS Glue 4.0 /
# PySpark + Delta Lake stored-procedure-chain replacement pipeline
# (AAP 0.5.1 Group F) into the correct PRE-EXISTING S3 bucket via
# `aws_s3_object`. Two destination buckets, both externally owned and never
# created/modified here -- only new prefix-scoped objects are added:
#
#   ARTIFACT_S3_BUCKET (var.artifact_s3_bucket)
#     - the two staged Delta JARs        -> ${local.jar_prefix}/
#     - the staged delta-spark wheel      -> ${local.wheel_prefix}/
#     - the five Glue job scripts         -> ${local.code_prefix}/jobs/
#     - the zipped lib/ + schemas/ pkgs   -> ${local.code_prefix}/
#     - the manifest + source-contract    -> ${local.config_prefix}/
#
#   MWAA_DAG_S3_BUCKET (var.mwaa_dag_s3_bucket)
#     - the pipeline DAG                  -> ${local.mwaa_dag_prefix}/
#     - a manifest copy (DAG parse-time)  -> ${local.mwaa_dag_config_prefix}/
#
# WHY these uploads exist (AAP 0.1.2 / 0.3.2): the Delta artifacts are sourced
# EXCLUSIVELY from ARTIFACT_S3_BUCKET at Glue runtime -- public PyPI and public
# Maven Central are prohibited as runtime resolution paths -- so the wheel and
# JARs must be staged in S3 for `--additional-python-modules` / `--extra-jars`
# to resolve them. Likewise the job scripts, lib/schemas zips, and config YAMLs
# must be in S3 for Glue `script_location` / `--extra-py-files` / `--extra-files`.
#
# SINGLE SOURCE OF TRUTH: every object KEY is built from the prefix locals in
# infra/locals.tf (jar_prefix, wheel_prefix, code_prefix, config_prefix,
# mwaa_dag_prefix, mwaa_dag_config_prefix) and the staged-binary filename locals
# (delta_spark_jar, delta_storage_jar, delta_wheel). infra/glue_jobs.tf builds
# its Glue argument S3 URIs from the SAME locals, so the upload location and the
# runtime reference can never drift apart.
#
# DRIFT (Gate 4): every object sets `etag` to the md5 of its content
# (filemd5(...) for files on disk, the archive data source's output_md5 for the
# generated zips). Unchanged content therefore yields ZERO `terraform plan`
# drift after apply; changed content re-uploads automatically.
#
# TAGGING (Gate 4): every object carries `tags = local.common_tags` (the five
# mandatory tags Environment/Project/Owner/CostCenter/ManagedBy="terraform").
#
# DELIBERATELY NOT UPLOADED:
#   - artifacts/README.md  (provenance/checksum doc, not a runtime artifact)
#   - validate/*           (dev/test-only parity + security harness, never
#                           deployed to Glue or MWAA)
# ---------------------------------------------------------------------------

# ===========================================================================
# (A) Build the shared Python package zips (lib/ and schemas/).
#
# Glue resolves `import lib.<mod>` and `import schemas.<mod>` from the archives
# passed via `--extra-py-files`. For those imports to work, each zip MUST retain
# its package directory prefix (`lib/...`, `schemas/...`) and MUST NOT be
# flattened. The hashicorp/archive provider (declared in infra/versions.tf) is
# used with `dynamic "source"` blocks so every file is placed under its package
# path explicitly: `filename = "lib/${source.value}"`.
#
# fileset(..., "**") enumerates every file under the package directory (the
# packages are flat today, but "**" also future-proofs against sub-packages).
# The result is FILTERED to ".py" source files only via endswith(...): the Glue
# runtime needs only the Python modules, and -- critically -- `file(...)` below
# reads each entry as UTF-8 text, which FAILS on non-UTF-8 bytes. CPython byte
# caches (__pycache__/*.pyc), produced by any local import/test run, are not
# valid UTF-8 and previously broke `terraform validate`/`plan`; filtering to .py
# excludes them (and any other non-source files) deterministically without
# depending on a clean working tree.
# `file(...)` reads each retained module's UTF-8 text content into the archive.
#
# output_path writes into ${path.module}/.terraform-build/ -- a TRANSIENT build
# directory that should be git-ignored and is safe to delete between runs; it is
# recreated deterministically from source on every plan/apply. The archive's
# output_md5 (used as the S3 object etag below) is a stable function of the
# zipped content, so an unchanged package produces no plan drift (Gate 4).
# ===========================================================================

data "archive_file" "lib_zip" {
  type        = "zip"
  output_path = "${path.module}/.terraform-build/lib.zip"

  # One archive entry per .py file under ../lib, keyed under the "lib/" prefix so
  # `import lib.<module>` resolves at the Glue runtime. Non-source files (notably
  # __pycache__/*.pyc, which are not valid UTF-8) are excluded so `file(...)`
  # never tries to read non-UTF-8 bytes -- see the header note above.
  dynamic "source" {
    for_each = toset([for f in fileset("${path.module}/../lib", "**") : f if endswith(f, ".py")])
    content {
      content  = file("${path.module}/../lib/${source.value}")
      filename = "lib/${source.value}"
    }
  }
}

data "archive_file" "schemas_zip" {
  type        = "zip"
  output_path = "${path.module}/.terraform-build/schemas.zip"

  # One archive entry per .py file under ../schemas, keyed under the "schemas/"
  # prefix so `import schemas.<module>` resolves at the Glue runtime. Non-source
  # files (notably __pycache__/*.pyc, which are not valid UTF-8) are excluded so
  # `file(...)` never tries to read non-UTF-8 bytes -- see the header note above.
  dynamic "source" {
    for_each = toset([for f in fileset("${path.module}/../schemas", "**") : f if endswith(f, ".py")])
    content {
      content  = file("${path.module}/../schemas/${source.value}")
      filename = "schemas/${source.value}"
    }
  }
}

# ===========================================================================
# (B) Uploads to ARTIFACT_S3_BUCKET.
#
# Public PyPI / Maven are prohibited at runtime (AAP 0.1.2), so these objects
# are the ONLY resolution path for the Delta wheel, the Delta JARs, the job
# code, and the config YAMLs.
# ===========================================================================

# The three staged Delta JARs, loaded by Glue via `--extra-jars`:
#   - delta-spark_2.12-3.2.0.jar          (Delta Spark connector / DeltaCatalog)
#   - delta-storage-s3-dynamodb-3.2.0.jar (io.delta.storage.S3DynamoDBLogStore)
#   - delta-storage-3.2.0.jar             (transitive base: HadoopFileSystemLogStore,
#                                          CloseableIterator, internal.PathLock,
#                                          internal.FileNameUtils -- the classes the
#                                          S3DynamoDBLogStore base extends/uses)
# The third JAR is REQUIRED for runtime class-closure: with --datalake-formats
# omitted and public Maven prohibited (AAP 0.7.1), the S3 DynamoDB LogStore cannot
# class-load without its delta-storage base also on the classpath.
# Keyed under ${jar_prefix}; the exact filenames come from locals so they match
# the --extra-jars URIs built in infra/glue_jobs.tf.
resource "aws_s3_object" "jars" {
  for_each = toset([local.delta_spark_jar, local.delta_storage_jar, local.delta_storage_transitive_jar])

  bucket = var.artifact_s3_bucket
  key    = "${local.jar_prefix}/${each.value}"
  source = "${path.module}/../artifacts/${each.value}"
  etag   = filemd5("${path.module}/../artifacts/${each.value}")

  tags = local.common_tags
}

# The delta-spark Python wheel, resolved by Glue `--additional-python-modules`
# (the prohibited public-PyPI path is replaced by this S3-hosted wheel).
resource "aws_s3_object" "wheel" {
  bucket = var.artifact_s3_bucket
  key    = "${local.wheel_prefix}/${local.delta_wheel}"
  source = "${path.module}/../artifacts/${local.delta_wheel}"
  etag   = filemd5("${path.module}/../artifacts/${local.delta_wheel}")

  tags = local.common_tags
}

# The Glue job scripts (one PySpark entrypoint per stage). for_each over the
# actual *.py files keeps this in lock-step with the jobs/ directory: adding a
# stage script automatically uploads it. The object key is
# ${code_prefix}/jobs/<basename> -- which MUST equal the `script_location`
# referenced by each aws_glue_job in infra/glue_jobs.tf.
resource "aws_s3_object" "job_scripts" {
  for_each = fileset("${path.module}/../jobs", "*.py")

  bucket = var.artifact_s3_bucket
  key    = "${local.code_prefix}/jobs/${each.value}"
  source = "${path.module}/../jobs/${each.value}"
  etag   = filemd5("${path.module}/../jobs/${each.value}")

  tags = local.common_tags
}

# The zipped shared lib/ package (generated by data.archive_file.lib_zip),
# loaded via `--extra-py-files`. etag uses the archive's output_md5 so an
# unchanged package yields no drift.
resource "aws_s3_object" "lib_zip" {
  bucket = var.artifact_s3_bucket
  key    = "${local.code_prefix}/lib.zip"
  source = data.archive_file.lib_zip.output_path
  etag   = data.archive_file.lib_zip.output_md5

  tags = local.common_tags
}

# The zipped shared schemas/ package (generated by data.archive_file.schemas_zip),
# loaded via `--extra-py-files` alongside lib.zip.
resource "aws_s3_object" "schemas_zip" {
  bucket = var.artifact_s3_bucket
  key    = "${local.code_prefix}/schemas.zip"
  source = data.archive_file.schemas_zip.output_path
  etag   = data.archive_file.schemas_zip.output_md5

  tags = local.common_tags
}

# The two config YAMLs (authoritative pipeline manifest + the per-pipeline
# source contract). Glue localizes them next to the running script via
# `--extra-files`; lib/manifest.py and lib/source_contract.py then read them by
# their basenames (manifest_path / source_contract_path).
resource "aws_s3_object" "config" {
  for_each = toset(["pipeline_manifest.yaml", "sp_chain_replacement_source_contract.yaml"])

  bucket = var.artifact_s3_bucket
  key    = "${local.config_prefix}/${each.value}"
  source = "${path.module}/../config/${each.value}"
  etag   = filemd5("${path.module}/../config/${each.value}")

  tags = local.common_tags
}

# ===========================================================================
# (C) Uploads to MWAA_DAG_S3_BUCKET.
#
# The pre-provisioned MWAA environment itself is untouched; only objects under
# its DAG prefix are added. MWAA continuously syncs this prefix to its workers.
# ===========================================================================

# The pipeline DAG. MWAA imports it from ${mwaa_dag_prefix}/ in its DAG bucket.
resource "aws_s3_object" "dag" {
  bucket = var.mwaa_dag_s3_bucket
  key    = "${local.mwaa_dag_prefix}/sp_chain_replacement_dag.py"
  source = "${path.module}/../dags/sp_chain_replacement_dag.py"
  etag   = filemd5("${path.module}/../dags/sp_chain_replacement_dag.py")

  tags = local.common_tags
}

# A manifest copy co-deployed for DAG PARSE-TIME. The DAG reads the manifest at
# import time to build one GlueJobOperator per stage; because MWAA syncs the
# whole DAG-bucket prefix to the scheduler/workers, the manifest must sit
# alongside the DAG at dags/config/pipeline_manifest.yaml (the DAG's
# _resolve_manifest_path() looks for <dag_dir>/config/pipeline_manifest.yaml).
# This is a SECOND copy of the same file the ARTIFACT bucket holds for the Glue
# runtime -- the two destinations serve different consumers (Airflow vs Glue).
resource "aws_s3_object" "dag_manifest" {
  bucket = var.mwaa_dag_s3_bucket
  key    = "${local.mwaa_dag_config_prefix}/pipeline_manifest.yaml"
  source = "${path.module}/../config/pipeline_manifest.yaml"
  etag   = filemd5("${path.module}/../config/pipeline_manifest.yaml")

  tags = local.common_tags
}
