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
# infra/locals.tf
#
# Central computed values for the net-new, additive AWS Glue 4.0 / PySpark /
# Delta Lake stored-procedure-chain replacement pipeline (AAP 0.5.1 Group F).
#
# This is the SINGLE place that:
#   - assembles the five mandatory tags (Gate 4) into local.common_tags;
#   - decodes the authoritative pipeline manifest (config/pipeline_manifest.yaml)
#     EXACTLY ONCE so the rest of infra/ stays DRY and manifest-driven;
#   - derives resource naming from the manifest's resource_pattern; and
#   - computes every ARTIFACT_S3_BUCKET key prefix, staged-binary filename, and
#     Glue job-argument S3 URI.
#
# The prefix/URI locals below are the SINGLE SOURCE OF TRUTH shared by
# infra/s3_objects.tf (which uploads objects to these keys) and
# infra/glue_jobs.tf (which references the same keys as Glue argument URIs).
# Both sibling files reference these locals rather than re-deriving paths, so the
# upload location and the runtime reference can never drift apart.
#
# CRITICAL: the manifest is read at PLAN time from the LOCAL filesystem via
# path.module (../config/pipeline_manifest.yaml), NOT from S3. The S3 copies
# uploaded by s3_objects.tf exist only for the Glue runtime. No environment-
# specific or secret values appear here beyond what arrives through var.*.
# ---------------------------------------------------------------------------

locals {
  # -------------------------------------------------------------------------
  # (a) The five mandatory tags (Gate 4).
  #
  # This map is applied BOTH via the AWS provider `default_tags` (infra/
  # providers.tf) AND set explicitly as `tags = local.common_tags` on every
  # taggable resource, so every resource provably carries all five tags even if
  # a particular resource type ignores provider default_tags. Because the values
  # are identical in both places, the aws provider 5.x default_tags / resource
  # tags overlap resolves to the same value and produces NO perpetual plan diff.
  #
  # ManagedBy is the STATIC literal "terraform" (never sourced from a variable);
  # the other four tags come from the tagging/identity input variables.
  # -------------------------------------------------------------------------
  common_tags = {
    Environment = var.environment
    Project     = var.project
    Owner       = var.owner
    CostCenter  = var.cost_center
    ManagedBy   = "terraform"
  }

  # -------------------------------------------------------------------------
  # (b) Decode the pipeline manifest ONCE.
  #
  # config/pipeline_manifest.yaml is the authoritative, env-agnostic definition
  # of the pipeline (stored-procedure -> Glue-job map, stage order, write modes,
  # merge conditions). It is decoded here a single time and every derived value
  # below (and in the sibling .tf files) flows from this one read.
  # -------------------------------------------------------------------------
  manifest = yamldecode(file("${path.module}/../config/pipeline_manifest.yaml"))

  pipeline_id   = local.manifest.pipeline             # "sp_chain_replacement"
  domain        = local.manifest.naming.domain        # "finance"
  pipeline_name = local.manifest.naming.pipeline_name # "sp-chain-replacement"

  # Ordered list of the five stage maps (stage/step/type/job_script/reads/writes).
  stages = local.manifest.stages

  # for_each-ready map keyed by the UNIQUE `step` token (consumed by
  # infra/glue_jobs.tf to create one aws_glue_job per stage).
  stages_by_step = { for s in local.stages : s.step => s }

  delta_path_prefix    = local.manifest.defaults.delta_path_prefix    # "finance/sp_chain_replacement"
  quarantine_prefix    = local.manifest.defaults.quarantine_prefix    # "quarantine/finance/sp_chain_replacement"
  bad_record_threshold = local.manifest.defaults.bad_record_threshold # 0.0
  source_contract_rel  = local.manifest.source_contract               # "config/sp_chain_replacement_source_contract.yaml"

  # -------------------------------------------------------------------------
  # (c) Naming.
  #
  # Mirrors the manifest resource_pattern "{env}-{domain}-{pipeline_name}-{step}".
  # infra/glue_jobs.tf appends "-${each.value.step}" to build the full per-stage
  # resource name, e.g. "prod-finance-sp-chain-replacement-stage-0-ingest".
  # -------------------------------------------------------------------------
  name_prefix = "${var.environment}-${local.domain}-${local.pipeline_name}"

  # -------------------------------------------------------------------------
  # (d) ARTIFACT_S3_BUCKET key prefixes.
  #
  # The single source of truth for both the upload keys (s3_objects.tf) and the
  # Glue argument URIs (glue_jobs.tf). All artifacts live under a top prefix
  # named for the pipeline id.
  # -------------------------------------------------------------------------
  artifact_root = local.pipeline_id # top prefix in ARTIFACT bucket
  jar_prefix    = "${local.artifact_root}/jars"
  wheel_prefix  = "${local.artifact_root}/python"
  code_prefix   = "${local.artifact_root}/code"   # job scripts + lib.zip + schemas.zip
  config_prefix = "${local.artifact_root}/config" # manifest + source-contract for --extra-files
  temp_prefix   = "${local.artifact_root}/tmp"    # Glue --TempDir

  mwaa_dag_prefix        = "dags"        # DAG location in MWAA bucket
  mwaa_dag_config_prefix = "dags/config" # manifest co-deployed for DAG parse-time

  # -------------------------------------------------------------------------
  # (e) Exact staged binary filenames.
  #
  # These MUST match the artifacts/ siblings byte-for-byte: s3_objects.tf uploads
  # files with these names and glue_jobs.tf references the same names in its
  # --extra-jars / --additional-python-modules URIs.
  # -------------------------------------------------------------------------
  delta_spark_jar   = "delta-spark_2.12-3.2.0.jar"
  delta_storage_jar = "delta-storage-s3-dynamodb-3.2.0.jar"
  delta_wheel       = "delta_spark-3.2.0-py3-none-any.whl"

  # -------------------------------------------------------------------------
  # (f) S3 URIs for Glue job arguments (consumed by glue_jobs.tf
  #     default_arguments) plus the source/quarantine runtime roots.
  # -------------------------------------------------------------------------

  # --extra-jars: the two Delta JARs, comma-separated, from ARTIFACT_S3_BUCKET.
  extra_jars = join(",", [
    "s3://${var.artifact_s3_bucket}/${local.jar_prefix}/${local.delta_spark_jar}",
    "s3://${var.artifact_s3_bucket}/${local.jar_prefix}/${local.delta_storage_jar}",
  ])

  # --additional-python-modules: the delta-spark wheel from ARTIFACT_S3_BUCKET
  # (public PyPI is prohibited as a runtime resolution path).
  additional_python_modules = "s3://${var.artifact_s3_bucket}/${local.wheel_prefix}/${local.delta_wheel}"

  # --extra-py-files: the zipped shared lib/ and schemas/ packages.
  extra_py_files = join(",", [
    "s3://${var.artifact_s3_bucket}/${local.code_prefix}/lib.zip",
    "s3://${var.artifact_s3_bucket}/${local.code_prefix}/schemas.zip",
  ])

  # --extra-files: the manifest + source-contract YAMLs, localized next to the
  # job at runtime so lib/manifest.py and lib/source_contract.py can read them.
  extra_files = join(",", [
    "s3://${var.artifact_s3_bucket}/${local.config_prefix}/pipeline_manifest.yaml",
    "s3://${var.artifact_s3_bucket}/${local.config_prefix}/sp_chain_replacement_source_contract.yaml",
  ])

  # Localized basenames after --extra-files copies the YAMLs next to the script.
  manifest_basename        = "pipeline_manifest.yaml"
  source_contract_basename = "sp_chain_replacement_source_contract.yaml"

  # Glue --TempDir (must end with a trailing slash).
  glue_temp_dir = "s3://${var.artifact_s3_bucket}/${local.temp_prefix}/"

  # Stage-0 runtime roots. s3a:// (Hadoop S3A) is the scheme the Spark jobs use.
  source_s3_uri     = "s3a://${var.source_s3_bucket}/${var.pipeline_source_s3_prefix}" # stage-0 source root
  quarantine_s3_uri = "s3a://${var.delta_s3_bucket}/${local.quarantine_prefix}"        # stage-0 quarantine root

  # -------------------------------------------------------------------------
  # (g) The six mandatory Spark --conf LogStore keys.
  #
  # Mirrors lib.spark_session.delta_logstore_conf. AAP 0.7.1 mandates that
  # glue_jobs.tf ALSO pass these via --conf even though build_spark_session sets
  # them, because Glue's getOrCreate may attach to an already-started context.
  # These are the EXACT keys from the reference integration test
  # (storage-s3-dynamodb/integration_tests/dynamodb_logstore.py:L118-124).
  #
  # LogStore enforcement: BOTH spark.delta.logStore.s3.impl AND
  # spark.delta.logStore.s3a.impl are set to io.delta.storage.S3DynamoDBLogStore
  # (no other LogStore is permitted). The test-only s3n.impl key is deliberately
  # NOT included. The string is joined with " --conf " so glue_jobs.tf can prefix
  # a single leading "--conf " to form a complete argument list.
  # -------------------------------------------------------------------------
  delta_spark_conf = join(" --conf ", [
    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.s3.impl=io.delta.storage.S3DynamoDBLogStore",
    "spark.delta.logStore.s3a.impl=io.delta.storage.S3DynamoDBLogStore",
    "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName=${var.dynamodb_table_name}",
    "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=${var.aws_region}",
  ])
}
