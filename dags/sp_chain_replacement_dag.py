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
"""Amazon MWAA DAG -- finance ``sp_chain_replacement`` pipeline.

Orchestrates the config-driven, multi-stage AWS Glue 4.0 / PySpark + Delta Lake
ETL pipeline that replaces the legacy finance SQL Server stored-procedure chain
on a strict 1:1 basis. One :class:`GlueJobOperator` task per manifest stage,
chained strictly sequentially with ``>>`` in ascending stage order, so the
stages run one-after-another exactly like the legacy chain.

The stage list, naming convention, and schedule are sourced from
``config/pipeline_manifest.yaml`` (the authoritative single source of truth).
The DAG never hardcodes the stage list; it derives the task graph from that
manifest at parse time. Each task references a **pre-existing AWS Glue job by
name** (created by ``infra/glue_jobs.tf`` using the same naming pattern) -- the
DAG only *triggers* jobs; it never creates, updates, or uploads job scripts.

Manual trigger ("Trigger DAG w/ config") accepts a JSON ``conf``::

    {"run_date": "2024-01-15",
     "source_s3_path": "s3://<source-bucket>/incoming/2024-01-15/"}

- ``run_date``       -> defaults to the run's logical date ({{ ds }}) when absent.
- ``source_s3_path`` -> defaults to "" (Stage 0's Glue job then falls back to its
  configured PIPELINE_SOURCE_S3_PREFIX default argument).

Deployment: uploaded to ``MWAA_DAG_S3_BUCKET`` by ``infra/s3_objects.tf``. The
pre-provisioned MWAA environment itself is untouched. The manifest must be
resolvable at parse time (see ``_resolve_manifest_path``); set the
``PIPELINE_MANIFEST_PATH`` env var or co-deploy the manifest next to this DAG.
If the manifest is absent the DAG raises a clear ``FileNotFoundError`` (which
surfaces as an Airflow Import Error) rather than silently running a stale
chain -- this is intentional, loud failure.

Notes for the platform team (documented here, not implemented elsewhere):
- The MWAA execution role must allow ``glue:StartJobRun`` and
  ``glue:GetJobRun(s)`` on the Glue jobs created by ``infra/glue_jobs.tf``.
- The MWAA task naming convention ``{env}-{domain}-{pipeline_name}-{step}`` is an
  AAP open item to confirm with the platform team; it is used verbatim until
  confirmed.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
import yaml

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# ---------------------------------------------------------------------------
# Manifest resolution + loading (the single source of truth)
# ---------------------------------------------------------------------------
# The DAG file lives under the MWAA DAGs prefix once deployed. The manifest may
# be co-located (e.g. ``dags/config/pipeline_manifest.yaml``) or pointed at via
# an environment variable so the platform team can relocate it without editing
# this DAG. Candidates are tried in priority order.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_MANIFEST_CANDIDATES = [
    os.environ.get("PIPELINE_MANIFEST_PATH"),
    os.path.join(_THIS_DIR, "config", "pipeline_manifest.yaml"),
    os.path.join(_THIS_DIR, "pipeline_manifest.yaml"),
    os.path.join(_THIS_DIR, os.pardir, "config", "pipeline_manifest.yaml"),
]


def _resolve_manifest_path() -> str:
    """Return the first existing manifest path from the candidate list.

    Raises:
        FileNotFoundError: when none of the candidate locations resolve. The
            error message lists every path searched so an operator can quickly
            see where to place the manifest or which env var to set. Raising
            here (at parse time) makes the failure loud and visible in the
            Airflow UI as an Import Error instead of producing a silent,
            stage-less DAG.
    """
    for candidate in _MANIFEST_CANDIDATES:
        if candidate and os.path.isfile(candidate):
            return candidate
    raise FileNotFoundError(
        "pipeline_manifest.yaml could not be located. Set the "
        "PIPELINE_MANIFEST_PATH environment variable or co-deploy the manifest "
        "next to this DAG. Searched: "
        + repr([candidate for candidate in _MANIFEST_CANDIDATES if candidate])
    )


def _load_manifest() -> dict:
    """Parse the pipeline manifest with PyYAML's safe loader.

    ``yaml.safe_load`` is used deliberately (never ``yaml.load``) so the
    parse-time read of operator-supplied YAML cannot execute arbitrary Python.
    """
    with open(_resolve_manifest_path(), "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _resolve_env() -> str:
    """Resolve the deployment environment token (the ``{env}`` placeholder).

    Each MWAA environment (dev / nonprod / prod) identifies itself so the same
    env-agnostic manifest and DAG can run everywhere. An OS environment variable
    is preferred to avoid a metadata-DB lookup during DAG parsing; an Airflow
    Variable is used as a fallback. Any failure degrades gracefully to ``dev``
    so environment resolution can never break DAG import.

    No bucket names, account IDs, ARNs, regions, or credentials are read or
    embedded here -- only the logical environment token.
    """
    env = os.environ.get("PIPELINE_ENV")
    if env:
        return env
    try:
        from airflow.models import Variable

        return Variable.get("pipeline_env", default_var="dev")
    except Exception:  # noqa: BLE001 - never let env resolution break DAG import
        return "dev"


# ---------------------------------------------------------------------------
# Parse the manifest once at module import (DAG parse) time.
# ---------------------------------------------------------------------------
_MANIFEST = _load_manifest()

_NAMING = _MANIFEST["naming"]
_DOMAIN = _NAMING["domain"]
_PIPELINE_NAME = _NAMING["pipeline_name"]
_RESOURCE_PATTERN = _NAMING["resource_pattern"]

_PIPELINE_ID = _MANIFEST["pipeline"]
_DESCRIPTION = _MANIFEST.get("description")

_SCHEDULE = _MANIFEST.get("schedule", {}) or {}
_CRON = _SCHEDULE.get("cron")
_TIMEZONE = _SCHEDULE.get("timezone", "UTC")
_CATCHUP = bool(_SCHEDULE.get("catchup", False))

# Build the stage list strictly from the manifest, ordered ascending by
# ``stage`` so the >> chain mirrors the legacy stored-procedure sequence. A
# malformed manifest with no stages is a loud, fail-fast error rather than a
# silently empty DAG.
_STAGES = sorted(_MANIFEST["stages"], key=lambda stage: stage["stage"])
if not _STAGES:
    raise ValueError(
        "pipeline_manifest.yaml declares no stages; refusing to build an empty "
        "DAG. Add at least one entry under 'stages'."
    )

# Environment token ({env}) resolved per MWAA environment at parse time.
_ENV = _resolve_env()

# Region is optional: when None, GlueJobOperator falls back to the AWS
# connection / MWAA environment region. Never hardcode a region literal.
_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

# Forwarded to EVERY Glue job run as run-time arguments (Jinja-templated from
# the DAG-run conf). Glue ``getResolvedOptions`` ignores arguments a given job
# does not declare, so it is safe to pass both to all stages; Stage 0 is the
# primary ``--source_s3_path`` consumer. ``run_date`` falls back to the run's
# logical date (``ds``); ``source_s3_path`` falls back to "" so the Stage 0
# Glue job uses its own configured PIPELINE_SOURCE_S3_PREFIX default. Airflow
# ``params`` are intentionally NOT declared, because params with empty-string
# defaults would populate ``conf`` and defeat the ``ds`` fallback.
_SCRIPT_ARGS = {
    "--run_date": "{{ dag_run.conf.get('run_date', ds) }}",
    "--source_s3_path": "{{ dag_run.conf.get('source_s3_path', '') }}",
}

# Re-running a stage is idempotent by design (staging tables overwrite, output
# tables merge/overwrite -- AAP Gate 3), so a single bounded retry is safe and
# does not violate ACID strictness.
_DEFAULT_ARGS = {
    "owner": _DOMAIN,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# DAG definition: one GlueJobOperator per stage, chained sequentially with >>.
# ---------------------------------------------------------------------------
with DAG(
    dag_id=_PIPELINE_ID,
    description=_DESCRIPTION,
    # Cron schedule from the manifest; the DAG also remains manually triggerable
    # ("Trigger DAG w/ config"). ``schedule_interval`` is broadly compatible
    # across MWAA Airflow 2.x; do not introduce a provider/Airflow pin.
    schedule_interval=_CRON,
    start_date=pendulum.datetime(2024, 1, 1, tz=_TIMEZONE),
    catchup=_CATCHUP,
    # Prevent two DAG runs from overwriting the same staging Delta tables
    # simultaneously (consistency with the ACID / overwrite design).
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=[_DOMAIN, "delta-lake", "glue", "etl", _PIPELINE_NAME],
    doc_md=__doc__,
) as dag:
    _previous_task = None
    for _stage in _STAGES:
        _step = _stage["step"]
        # BOTH the MWAA task naming convention AND the Terraform-created Glue
        # job name. Keeping ``task_id`` and ``job_name`` identical guarantees
        # the operator triggers the correct pre-existing job. Airflow task_ids
        # permit hyphens, so e.g. "prod-finance-sp-chain-replacement-stage-0-
        # ingest" is a valid task_id.
        _resource_name = _RESOURCE_PATTERN.format(
            env=_ENV,
            domain=_DOMAIN,
            pipeline_name=_PIPELINE_NAME,
            step=_step,
        )
        _task = GlueJobOperator(
            task_id=_resource_name,
            # References the Terraform-owned Glue job by name. No
            # script_location / iam_role_name / create_job_kwargs are set, so
            # the operator only triggers the existing job (never creates it).
            job_name=_resource_name,
            script_args=_SCRIPT_ARGS,
            region_name=_REGION,
            # Block until the Glue run finishes so the next stage starts only
            # after the current one succeeds -- true sequential execution.
            wait_for_completion=True,
        )
        if _previous_task is not None:
            # Strict sequential chaining with the >> operator; no parallelism,
            # no fan-out: stage_0 >> stage_1 >> ... >> stage_n.
            _previous_task >> _task
        _previous_task = _task
