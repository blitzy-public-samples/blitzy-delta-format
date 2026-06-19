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
"""Stage 0 (Ingestion) -- AWS Glue 4.0 / PySpark entrypoint for ``staging_raw``.

This is a standalone AWS Glue 4.0 (Apache Spark 3.3.x, Python 3.10, Scala 2.12)
``spark-submit`` script -- the first stage of a config-driven, multi-stage ETL
pipeline that replaces a legacy Informatica-orchestrated SQL Server
stored-procedure chain with Delta Lake tables on Amazon S3, committing through
``io.delta.storage.S3DynamoDBLogStore`` with full ACID guarantees. It is NOT part
of an importable package (there is intentionally no ``jobs/__init__.py``); Glue
places ``lib/`` and ``schemas/`` on ``PYTHONPATH`` at runtime, so this module uses
absolute ``from lib.x import ...`` / ``from schemas import ...`` imports.

Responsibility (in order)
-------------------------
#. Read the delimited flat files at ``--source_s3_path`` using the per-pipeline
   source contract (delimiter / quote / null token / header / encoding) and an
   explicit, all-``StringType`` read schema (plus the PERMISSIVE-mode
   ``_corrupt_record`` column) -- never inferred.
#. Validate / safe-cast every row against the explicit typed ``StructType`` for
   ``staging_raw`` (from the ``schemas/`` registry); split conforming rows from
   malformed ones.
#. Route malformed rows to a run-scoped quarantine S3 prefix (raw parquet) and
   count them.
#. FAIL the job with a non-zero exit when the bad-record rate exceeds
   ``--bad_record_threshold`` (default ``0.0`` => any bad record fails the job).
#. Write the conforming rows to the ``staging_raw`` Delta table in ``overwrite``
   mode (idempotent re-runs; Gate 3).
#. Emit the mandated six-field CloudWatch completion event exactly once.

Non-negotiable constraints encoded here (AAP s0.1.2 / s0.7.1)
-------------------------------------------------------------
* **LogStore = S3DynamoDBLogStore only.** The :class:`~pyspark.sql.SparkSession`
  is built exclusively via :func:`lib.spark_session.build_spark_session`; this
  file never constructs a session directly and never sets a LogStore config
  inline.
* **Schema-merge disabled on every write.** All Delta writes go through
  :mod:`lib.delta_io`, which keeps the write-time schema-merge flag pinned off;
  schema inference and write-time schema evolution are never enabled in this file.
* **Explicit schema only -- no inference.** The read schema is explicit and
  all-String; the typed target schema is an explicit ``StructType`` resolved from
  the ``schemas/`` registry.
* **ACID strictness -- no non-ACID fallback.** The read, schema validation, the
  bad-record threshold check, and the Delta write are NEVER wrapped in a
  swallowing / downgrading / retrying ``try``/``except``. The only ``try``/
  ``finally`` here stops the Spark session in ``finally`` WITHOUT suppressing the
  in-flight exception, so a failed DynamoDB conditional write (or a breached
  threshold) fails the Glue run non-zero.

Reconciliation note (PROMINENT)
-------------------------------
The schema registry key for ``staging_raw`` and its columns / types, the
source-contract ``expected_columns``, and this job must all be reconciled to one
coherent column model (the manifest / source-contract model is canonical) before
production cutover. As authored, ``staging_raw`` resolves via
``schemas.get_schema('staging_raw')`` (its registry key matches); transform /
output stages may require schema-registry-key reconciliation noted in their own
files. This job never hardcodes the column list: the read columns are derived from
``contract.expected_columns()`` and the target types from the ``staging_raw``
typed ``StructType``.

Job arguments (injected by ``infra/glue_jobs.tf``)
--------------------------------------------------
Required: ``JOB_NAME`` (Glue-provided), ``ddb_table_name``, ``aws_region``,
``delta_bucket`` (DELTA_S3_BUCKET, no scheme), ``manifest_path``, ``step`` (this
stage's unique manifest token, e.g. ``stage-0-ingest``), ``source_s3_path``
(``s3a://...``), ``source_contract_path``, ``quarantine_s3_path`` (``s3a://...``
root). Optional-with-default: ``bad_record_threshold`` (``"0.0"``), ``run_date``
(``""``).
"""

from __future__ import annotations

import os

from pyspark.sql.types import StringType, StructField, StructType

from lib.delta_io import count_rows, overwrite_delta
from lib.job_args import resolve_options
from lib.logging_utils import StageTimer, emit_completion_event, get_logger
from lib.manifest import Manifest, load_manifest
from lib.s3_paths import build_delta_table_uri, build_quarantine_uri
from lib.schema_validation import (
    enforce_bad_record_threshold,
    quarantine_bad_records,
    validate_against_schema,
)
from lib.source_contract import load_source_contract
from lib.spark_session import build_spark_session
from schemas import get_schema

# Name of the PERMISSIVE-mode corrupt-record column appended to the read schema.
# Must match the ``columnNameOfCorruptRecord`` emitted by the source contract's
# ``to_spark_csv_options()`` so unparseable lines surface here for quarantine
# instead of being silently dropped at read time.
_CORRUPT_COLUMN = "_corrupt_record"


def _resolve_glue_run_id() -> str:
    """Resolve the Glue job-run id best-effort (a CloudWatch log field only).

    Tries the optional ``--JOB_RUN_ID`` Glue argument first, then the
    ``JOB_RUN_ID`` environment variable, and finally falls back to the literal
    ``"unknown"``. The run id is purely a log field, so resolving it must NEVER
    fail the job; the broad ``except`` here is intentional and is deliberately
    OUTSIDE the ACID-critical read / validate / threshold / write path (which is
    never guarded by a swallowing handler).

    :returns: The resolved Glue run id, or ``"unknown"`` when unavailable.
    """
    try:
        resolved = resolve_options([], {"JOB_RUN_ID": ""})
        run_id = resolved.get("JOB_RUN_ID", "")
        if run_id:
            return run_id
    except Exception as exc:
        # Best-effort only: the run id is a non-essential log field, so any
        # failure to resolve it (for example ``awsglue`` being unavailable in a
        # local / test context) must NOT fail the job. This handler does not
        # guard any Delta / DynamoDB operation, so it preserves ACID strictness.
        # Record the benign reason at debug level (no no-op ``pass``) and fall
        # through to the environment / literal fallback below.
        get_logger("stage_0_ingest").debug(
            "Glue run id unresolved via resolve_options (%s: %s); falling back "
            "to the JOB_RUN_ID environment variable or 'unknown'.",
            type(exc).__name__,
            exc,
        )
    return os.environ.get("JOB_RUN_ID", "") or "unknown"


def _resolve_stage(manifest: Manifest, step: str) -> dict:
    """Return the manifest stage mapping whose ``step`` token equals ``step``.

    Topology is read from ``manifest.raw["stages"]`` (the full parsed YAML) so the
    job stays generic and manifest-driven rather than hardcoding stage metadata.

    :param manifest: The loaded :class:`lib.manifest.Manifest`.
    :param step: The unique stage token for THIS job (e.g. ``"stage-0-ingest"``).
    :returns: The raw stage mapping for ``step``.
    :raises ValueError: If no stage with ``step`` exists (lists available steps).
    """
    stages = manifest.raw.get("stages", [])
    for stage in stages:
        if stage.get("step") == step:
            return stage
    available = [s.get("step") for s in stages]
    raise ValueError(f"stage step {step!r} not found in manifest; available steps: {available}")


def _resolve_table(manifest: Manifest, name: str) -> dict:
    """Return the manifest table mapping whose ``name`` equals ``name``.

    Read from ``manifest.raw["tables"]`` so the table's relative ``path`` and
    optional ``partition_by`` stay config-driven.

    :param manifest: The loaded :class:`lib.manifest.Manifest`.
    :param name: The logical table name to resolve (e.g. ``"staging_raw"``).
    :returns: The raw table mapping for ``name``.
    :raises ValueError: If no table with ``name`` exists (lists available names).
    """
    tables = manifest.raw.get("tables", [])
    for table in tables:
        if table.get("name") == name:
            return table
    available = [t.get("name") for t in tables]
    raise ValueError(f"table {name!r} not found in manifest; available tables: {available}")


def _delta_path(delta_bucket: str, defaults: dict, table: dict) -> str:
    """Compose the fully qualified, validated ``s3a://`` Delta table location.

    Delegates to :func:`lib.s3_paths.build_delta_table_uri`, which composes
    ``s3a://{delta_bucket}/{defaults['delta_path_prefix']}/{table['path']}`` and
    centrally validates every piece: the bucket is checked against S3 naming
    rules (rejecting embedded schemes, slashes, backslashes, ``..`` and control
    characters), and the prefix / per-table path segments reject traversal
    tokens, embedded schemes, backslashes, control characters and empty /
    absolute segments (CWE-22 hardening). For legitimate manifest values the
    output is byte-identical to the prior slash-stripping composition, so the
    Delta layout is unchanged. ``delta_bucket`` is the scheme-less
    DELTA_S3_BUCKET job arg; the prefix and per-table path are relative,
    env-agnostic manifest values, keeping this composition portable across
    dev / nonprod / prod.

    :param delta_bucket: Destination bucket name, without an ``s3a://`` scheme.
    :param defaults: The manifest ``defaults`` mapping (uses ``delta_path_prefix``).
    :param table: The resolved table mapping (uses its relative ``path``).
    :returns: The fully qualified, validated ``s3a://`` location of the Delta table.
    :raises lib.s3_paths.S3PathError: If the bucket or any path segment is unsafe.
    """
    return build_delta_table_uri(
        delta_bucket,
        str(defaults.get("delta_path_prefix", "")),
        str(table.get("path", "")),
    )


def _string_read_schema(columns: list[str], corrupt_column: str = _CORRUPT_COLUMN) -> StructType:
    """Build an all-``StringType`` read schema plus the corrupt-record column.

    Every source column is declared as a nullable ``StringType`` and the
    PERMISSIVE-mode corrupt-record column is appended last (it MUST be declared in
    the read schema for Spark to populate it). Reading everything as String -- with
    NO inference -- defers all typing to
    :func:`lib.schema_validation.validate_against_schema`, which safe-casts each
    field to its target type. This honors the project-wide "no schema inference"
    mandate while still capturing structurally-unparseable rows in
    ``corrupt_column`` for quarantine instead of dropping them.

    :param columns: Ordered source column names (from ``contract.expected_columns()``).
    :param corrupt_column: Name of the appended corrupt-record column.
    :returns: The explicit all-String :class:`StructType` used by the CSV reader.
    """
    fields = [StructField(name, StringType(), True) for name in columns]
    fields.append(StructField(corrupt_column, StringType(), True))
    return StructType(fields)


def main() -> None:
    """Execute Stage 0 ingestion end to end.

    Resolves Glue arguments, builds the LogStore-wired :class:`SparkSession`,
    loads the manifest + source contract, reads the source files with an explicit
    all-String schema, validates / casts against the typed ``staging_raw`` schema,
    quarantines + counts malformed rows, enforces the fatal bad-record threshold,
    overwrite-writes ``staging_raw``, and emits the six-field completion event.

    The Spark session is stopped in a ``finally`` that does NOT suppress
    exceptions: any failure in the read / validate / threshold / write path
    (including a DynamoDB conditional-write failure surfaced by
    ``S3DynamoDBLogStore``, or a breached bad-record threshold) propagates so the
    Glue job exits non-zero (ACID strictness; the threshold is fatal).
    """
    args = resolve_options(
        required=[
            "JOB_NAME",
            "ddb_table_name",
            "aws_region",
            "delta_bucket",
            "manifest_path",
            "step",
            "source_s3_path",
            "source_contract_path",
            "quarantine_s3_path",
        ],
        optional={"bad_record_threshold": "0.0", "run_date": ""},
    )
    glue_run_id = _resolve_glue_run_id()
    logger = get_logger("stage_0_ingest")

    # SparkSession built ONLY via lib.spark_session (S3DynamoDBLogStore wiring plus
    # the Delta SQL extension / catalog). No master is passed -- Glue provides it.
    spark = build_spark_session(
        app_name=args["JOB_NAME"],
        ddb_table_name=args["ddb_table_name"],
        region=args["aws_region"],
    )
    try:
        # --- Resolve topology from the manifest (config-driven, never hardcoded).
        manifest = load_manifest(args["manifest_path"])
        defaults = manifest.raw["defaults"]
        stage = _resolve_stage(manifest, args["step"])
        # Stage 0 writes exactly one table (staging_raw); take the first entry.
        out_table_name = stage["writes"][0]
        out_table = _resolve_table(manifest, out_table_name)
        out_path = _delta_path(args["delta_bucket"], defaults, out_table)
        partition_by = out_table.get("partition_by")

        # --- Source contract -> explicit all-String read schema (no inference).
        contract = load_source_contract(args["source_contract_path"])
        read_schema = _string_read_schema(contract.expected_columns())

        # --- Explicit typed target schema for staging_raw (schemas/ registry).
        # The registry key equals the manifest table name, keeping the lookup
        # manifest-driven; the column list itself is never hardcoded here.
        typed_schema = get_schema(out_table_name)

        # --- Run-scoped quarantine path: <root>/<table>[/<run_date>], built and
        # validated centrally. ``build_quarantine_uri`` parses the quarantine
        # root as an s3a/s3 URI, validates ``out_table_name`` as a single
        # slash-free key component, and validates ``run_date`` as a strict
        # ``YYYY-MM-DD`` calendar date before appending it -- so an unsafe or
        # traversal run_date can never smuggle extra path levels (CWE-22).
        run_date = args.get("run_date", "")
        quarantine_path = build_quarantine_uri(
            args["quarantine_s3_path"], out_table_name, run_date
        )

        threshold = float(args["bad_record_threshold"])

        # Time the data work so the completion event's elapsed_seconds covers
        # read -> validate -> quarantine -> threshold -> write.
        with StageTimer() as timer:
            reader = spark.read.options(**contract.to_spark_csv_options())
            raw_df = reader.schema(read_schema).csv(args["source_s3_path"])
            # Structural failure (a column required by the typed schema is absent)
            # raises SchemaValidationError -- intentionally NOT caught (fatal).
            result = validate_against_schema(raw_df, typed_schema)
            # Route + count malformed rows to the raw-parquet quarantine sink.
            quarantine_bad_records(result.bad_df, quarantine_path)
            # Fatal gate: raises BadRecordThresholdExceeded on breach (NOT caught),
            # so the Glue job exits non-zero. Default threshold 0.0 => any bad row.
            enforce_bad_record_threshold(result.bad_count, result.total_count, threshold)
            # ACID overwrite via S3DynamoDBLogStore (mergeSchema=false inside);
            # overwrite gives Stage 0 its structural idempotency (Gate 3).
            overwrite_delta(result.valid_df, out_path, partition_by=partition_by)
            output_rows = count_rows(result.valid_df)

        # Exactly one six-field completion event, emitted on the success path only.
        emit_completion_event(
            job_name=args["JOB_NAME"],
            glue_run_id=glue_run_id,
            input_rows=result.total_count,
            output_rows=output_rows,
            bad_record_count=result.bad_count,
            elapsed_seconds=timer.elapsed,
            logger=logger,
            stage_id=args["step"],
            output_table=out_table_name,
        )
    finally:
        # Always release the session. A bare stop() in ``finally`` does NOT
        # suppress an in-flight exception, so ACID / threshold failures still
        # propagate and fail the Glue run non-zero (ACID strictness).
        spark.stop()


if __name__ == "__main__":
    main()
