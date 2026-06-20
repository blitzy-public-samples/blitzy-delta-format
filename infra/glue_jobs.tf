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
# infra/glue_jobs.tf
#
# Provisions one aws_glue_job per pipeline stage for the net-new, additive AWS
# Glue 4.0 / PySpark + Delta Lake stored-procedure-chain replacement pipeline
# (AAP 0.5.1 Group F). The stage set is MANIFEST-DRIVEN: local.stages_by_step
# (assembled in infra/locals.tf from config/pipeline_manifest.yaml) is a map
# keyed by the UNIQUE `step` token, so `for_each` creates exactly one Glue job
# per stored-procedure-replacement stage -- a strict 1:1 SP -> Glue-job mapping.
# Adding or removing a stage in the manifest automatically adds or removes the
# corresponding Glue job here; no job is ever hardcoded.
#
# The five stages (manifest `step` -> `job_script`):
#   stage-0-ingest                -> jobs/stage_0_ingest.py
#   stage-1-cleanse-transactions  -> jobs/stage_1_cleanse_transactions.py
#   stage-2-enrich-accounts       -> jobs/stage_2_enrich_accounts.py
#   stage-3-compute-balances      -> jobs/stage_3_compute_balances.py
#   stage-n-output                -> jobs/stage_n_output.py
#
# Each job's name follows the manifest pattern "{env}-{domain}-{pipeline_name}-
# {step}" via "${local.name_prefix}-${each.value.step}" (e.g.
# "prod-finance-sp-chain-replacement-stage-0-ingest"). The MWAA DAG
# (dags/sp_chain_replacement_dag.py) references these EXACT job names, so the
# naming here is a contract the orchestration layer depends on.
#
# RUNTIME PINS / ARTIFACT SOURCING (AAP 0.1.2 -- NON-NEGOTIABLE)
# -------------------------------------------------------------
#   * glue_version = "4.0" is HARDCODED (Apache Spark 3.3.x, Python 3.10) -- it
#     is explicitly NOT Glue 5.0 and is intentionally NOT a variable.
#   * The Delta wheel + JARs are resolved EXCLUSIVELY from ARTIFACT_S3_BUCKET via
#     --extra-jars / --additional-python-modules / --extra-py-files; public PyPI
#     and public Maven Central are prohibited as runtime resolution paths. We do
#     NOT set Glue's --datalake-formats=delta (see the in-resource comment).
#   * Every Delta commit is coordinated by io.delta.storage.S3DynamoDBLogStore;
#     the six mandatory Spark conf keys travel inside local.delta_spark_conf.
#
# TAGGING (Gate 4): every job carries `tags = local.common_tags` (the five
# mandatory tags Environment/Project/Owner/CostCenter/ManagedBy="terraform").
#
# SINGLE SOURCE OF TRUTH: all artifact S3 URIs (--extra-jars, the wheel, the
# lib/schemas zips, the config YAMLs, the --TempDir, and each script_location)
# are built from the prefix/URI locals in infra/locals.tf -- the SAME locals
# infra/s3_objects.tf uses to upload those objects -- so the upload location and
# the runtime reference can never drift apart.
#
# Minimal Change Mandate: this is a pure, additive CREATE; no existing
# repository file is modified.
# -----------------------------------------------------------------------------

# ===========================================================================
# (1) File-local Glue job-argument maps.
#
# Authored here (rather than in infra/locals.tf) so the aws_glue_job resource
# below stays readable while the heavy --conf / --extra-* values continue to be
# sourced from the shared locals in infra/locals.tf. Terraform merges `locals`
# blocks across every *.tf file in this module into one `local.*` namespace, so
# these names coexist with the locals declared in infra/locals.tf; they are
# NOT redeclared there.
# ===========================================================================
locals {
  # -------------------------------------------------------------------------
  # (a) Arguments applied to EVERY stage.
  #
  # The Delta artifact references below are the ONLY runtime resolution path for
  # the pinned Delta 3.2.0 wheel + JARs (public PyPI/Maven are prohibited,
  # AAP 0.1.2); all four point at objects uploaded to ARTIFACT_S3_BUCKET by
  # infra/s3_objects.tf using the very same infra/locals.tf values.
  #
  # The "--conf" value packs all SIX S3DynamoDBLogStore Spark keys into a single
  # argument, separated by " --conf " (local.delta_spark_conf is
  # join(" --conf ", [...])) -- the standard AWS Glue multi-conf convention,
  # where one --conf job argument whose value embeds further " --conf " tokens is
  # expanded by Glue into several distinct Spark --conf options. These keys
  # MIRROR lib.spark_session.delta_logstore_conf and are passed REDUNDANTLY here
  # (even though build_spark_session sets them in-process) because, per the
  # lib/spark_session.py docstring, Glue's getOrCreate may attach to an already-
  # started SparkContext whose conf was fixed before the job code ran; setting
  # them as job args guarantees the LogStore wiring is present regardless.
  #
  # The trailing custom args (--ddb_table_name, --aws_region, --delta_bucket,
  # --manifest_path, --run_date) are consumed by the jobs/stage_*.py scripts via
  # awsglue.utils.getResolvedOptions (see lib/job_args.py). --run_date defaults
  # to "" here; the DAG overrides it per run via the GlueJobOperator arguments.
  glue_common_args = {
    "--job-language"                     = "python"
    "--TempDir"                          = local.glue_temp_dir
    "--enable-continuous-cloudwatch-log" = "true"

    # Delta artifacts resolved EXCLUSIVELY from ARTIFACT_S3_BUCKET (no public
    # PyPI/Maven). --extra-py-files carries the zipped lib/ + schemas/ packages
    # so `import lib.<mod>` / `import schemas.<mod>` resolve at runtime;
    # --extra-files localizes the manifest + source-contract YAMLs next to the
    # script (read by their basenames via lib/manifest.py + lib/source_contract.py).
    "--extra-jars"                = local.extra_jars
    "--additional-python-modules" = local.additional_python_modules
    "--extra-py-files"            = local.extra_py_files
    "--extra-files"               = local.extra_files

    # Mandatory S3DynamoDBLogStore Spark wiring (AAP 0.7.1): six conf keys packed
    # into ONE --conf, separated by " --conf " (Glue multi-conf convention). Both
    # spark.delta.logStore.s3.impl AND spark.delta.logStore.s3a.impl are set to
    # io.delta.storage.S3DynamoDBLogStore inside local.delta_spark_conf; no other
    # LogStore is permitted.
    "--conf" = local.delta_spark_conf

    # Custom args consumed by jobs/stage_*.py via getResolvedOptions.
    "--ddb_table_name" = var.dynamodb_table_name
    "--aws_region"     = var.aws_region
    "--delta_bucket"   = var.delta_s3_bucket     # bucket NAME only (no scheme)
    "--manifest_path"  = local.manifest_basename # localized basename (--extra-files)
    "--run_date"       = ""                      # default; DAG overrides per run
  }

  # -------------------------------------------------------------------------
  # (b) Arguments applied ONLY to stage-0-ingest.
  #
  # The flat-file ingest stage additionally needs the source-file root, its
  # source-contract YAML (localized basename), the quarantine root for malformed
  # records, and the bad-record threshold above which it fails the job non-zero.
  # Non-stage-0 jobs neither request nor receive these (the merge() below is
  # gated on the step), keeping each job's argument surface minimal.
  # bad_record_threshold is numeric in the manifest; Glue default_arguments is a
  # map(string), so it is rendered with tostring().
  # -------------------------------------------------------------------------
  glue_stage0_args = {
    "--source_s3_path"       = local.source_s3_uri            # full s3a:// source root
    "--source_contract_path" = local.source_contract_basename # localized basename
    "--quarantine_s3_path"   = local.quarantine_s3_uri        # full s3a:// quarantine root
    "--bad_record_threshold" = tostring(local.bad_record_threshold)
  }
}

# ===========================================================================
# (2) One Spark ETL Glue job per manifest stage.
#
# for_each over local.stages_by_step yields one aws_glue_job per UNIQUE `step`
# (strict 1:1 SP -> job mapping). Each instance is addressable as
# aws_glue_job.stage["stage-0-ingest"], ...["stage-n-output"], etc.
# ===========================================================================
resource "aws_glue_job" "stage" {
  for_each = local.stages_by_step

  # Manifest naming contract: "{env}-{domain}-{pipeline_name}-{step}". The MWAA
  # DAG references these exact names when constructing each GlueJobOperator.
  name = "${local.name_prefix}-${each.value.step}"

  # Least-privilege Glue execution role (infra/iam.tf). Assumed by every stage.
  role_arn = aws_iam_role.glue.arn

  # AAP 0.1.2: Glue 4.0 ONLY (Apache Spark 3.3.x, Python 3.10) -- explicitly NOT
  # Glue 5.0. HARDCODED on purpose; this pin is a non-negotiable constraint and
  # is intentionally NOT exposed as a variable.
  glue_version = "4.0"

  # Worker sizing is environment-tunable; the Glue VERSION above is not.
  worker_type       = var.glue_worker_type # default G.2X
  number_of_workers = var.glue_number_of_workers

  # The pipeline is idempotent by design (staging stages overwrite; output
  # stage merges/overwrites), and any failure must surface immediately to the
  # orchestrator rather than being silently retried, so Glue-level retries are
  # disabled. The timeout is the 48h ceiling; real per-stage runtime is far
  # lower (Airflow enforces the operational SLA).
  max_retries = 0
  timeout     = 2880 # minutes

  command {
    name           = "glueetl" # Spark ETL job (NOT pythonshell)
    python_version = "3"

    # The script_location object KEY (${local.code_prefix}/jobs/<basename>) is
    # built from the SAME locals that infra/s3_objects.tf uses to upload each
    # jobs/*.py (aws_s3_object.job_scripts), so the reference and the upload key
    # are identical by construction. basename() drops the manifest's repo-
    # relative "jobs/" prefix, leaving just the file name under the code prefix.
    script_location = "s3://${var.artifact_s3_bucket}/${local.code_prefix}/jobs/${basename(each.value.job_script)}"
  }

  # Common args for every stage + the per-stage --step selector + (stage-0 only)
  # the ingest-specific args. --step tells the job which manifest entry to load;
  # JOB_NAME is resolved automatically by Glue and equals this resource `name`.
  # The DAG additionally passes --run_date (and --source_s3_path) at trigger
  # time; non-stage-0 jobs simply ignore --source_s3_path because they do not
  # request it in their getResolvedOptions call.
  default_arguments = merge(
    local.glue_common_args,
    { "--step" = each.value.step },
    each.value.step == "stage-0-ingest" ? local.glue_stage0_args : {},
  )

  # NOTE (AAP 0.1.2): we deliberately do NOT set --datalake-formats=delta. That
  # Glue magic argument would load Glue's OWN bundled Delta version, defeating
  # the requirement to run the pinned Delta 3.2.0 wheel + JARs staged in
  # ARTIFACT_S3_BUCKET. Delta is provided solely through --extra-jars /
  # --additional-python-modules above.
  #
  # NOTE (Gate 5): we deliberately do NOT enable --enable-metrics or
  # --enable-observability-metrics. Publishing custom CloudWatch metrics needs
  # cloudwatch:PutMetricData, which cannot be resource-scoped and would force a
  # "Resource": "*" statement, failing the no-wildcard IAM gate. The mandated
  # six-field completion event is emitted via structured stdout captured by
  # continuous CloudWatch LOGS (--enable-continuous-cloudwatch-log above +
  # lib/logging_utils.py), not as a custom metric.

  # Five mandatory tags (Gate 4), assembled in infra/locals.tf.
  tags = local.common_tags

  # Ensure all referenced objects exist in S3 and the role is ready before any
  # Glue job is created: the job scripts, the lib/schemas zips, the Delta JARs,
  # the wheel, and the config YAMLs (all in ARTIFACT_S3_BUCKET via
  # infra/s3_objects.tf), plus the least-privilege policy attachment
  # (infra/iam.tf) that grants the execution role its permissions.
  depends_on = [
    aws_s3_object.job_scripts,
    aws_s3_object.lib_zip,
    aws_s3_object.schemas_zip,
    aws_s3_object.jars,
    aws_s3_object.wheel,
    aws_s3_object.config,
    aws_iam_role_policy_attachment.glue,
  ]
}
