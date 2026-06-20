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
"""Stage 1 (Transform) AWS Glue 4.0 / PySpark entrypoint -- ``dbo.usp_cleanse_transactions``.

This module is the **1:1 PySpark replacement** for the legacy SQL Server stored
procedure ``dbo.usp_cleanse_transactions`` (stage 1 of the config-driven pipeline
declared in ``config/pipeline_manifest.yaml``). It reads the prior stage's Delta
table ``staging_raw``, applies a deterministic row-level cleanse / normalization,
and writes the next Delta table ``staging_1_cleansed`` in ``overwrite`` mode so
re-running the same source / run id yields identical output with no row growth
(structural idempotency; Gate 3). Every Delta commit is coordinated through
``io.delta.storage.S3DynamoDBLogStore`` with full ACID guarantees.

This is a standalone Glue ``spark-submit`` script, NOT part of an importable
package (there is intentionally no ``jobs/__init__.py``); AWS Glue places ``lib/``
and ``schemas/`` on ``PYTHONPATH`` at runtime via ``--extra-py-files``, so this
module uses absolute ``from lib.x import ...`` / ``from schemas import ...`` imports.

Responsibility (in order)
-------------------------
#. Read the ``staging_raw`` Delta table written by Stage 0 (typed, already
   conformed to ``schemas.staging_raw``).
#. Apply :func:`cleanse_transactions`: trim string columns, uppercase the ISO-4217
   ``currency_code``, normalize ``debit_credit_indicator`` to a canonical ``D`` /
   ``C``, keep ``amount`` as an exact :class:`~pyspark.sql.types.DecimalType`
   (18, 2), stamp a ``cleansed_at`` audit timestamp, and emit a derived
   ``is_valid`` Boolean validity indicator. The stage is row-count-preserving:
   structurally-invalid rows (missing business keys) are flagged via ``is_valid``,
   never silently dropped.
#. Conform the result to the explicit ``StructType`` for ``staging_1_cleansed``
   (resolved from the ``schemas/`` registry) so the schema-locked write succeeds.
#. Overwrite-write ``staging_1_cleansed`` through :mod:`lib.delta_io`.
#. Emit the mandated six-field CloudWatch completion event exactly once.

Non-negotiable constraints encoded here (AAP s0.1.2 / s0.7.1)
-------------------------------------------------------------
* **LogStore = S3DynamoDBLogStore only.** The :class:`~pyspark.sql.SparkSession`
  is built exclusively via :func:`lib.spark_session.build_spark_session`; this
  file never constructs a session directly and never sets a LogStore config inline.
* **Schema-merge disabled on every write.** All Delta writes go through
  :mod:`lib.delta_io`, which keeps the write-time schema-merge flag pinned off;
  schema inference and write-time schema evolution are never enabled in this file.
* **Explicit schema only -- no inference.** The output schema is the explicit
  ``StructType`` resolved from the ``schemas/`` registry via
  :func:`schemas.get_schema`; the output is cast to it before writing.
* **ACID strictness -- no non-ACID fallback.** The Delta read and write are NEVER
  wrapped in a swallowing / downgrading / retrying ``try``/``except``. The only
  ``try``/``finally`` here stops the Spark session in ``finally`` WITHOUT
  suppressing the in-flight exception, so a failed DynamoDB conditional write
  (surfaced by ``S3DynamoDBLogStore``) fails the Glue run non-zero.
* **Structural idempotency.** ``overwrite`` replaces the table wholesale; the job
  never appends.

Reconciliation note (PROMINENT)
-------------------------------
The exact cleanse logic MUST be reconciled 1:1 with the real
``dbo.usp_cleanse_transactions`` definition before production cutover; the
operations in :func:`cleanse_transactions` are a representative finance template
(trim / case-fold / indicator-normalize / decimal-cast / validity-flagging), not
the verified procedure body. The ``schemas/`` registry key and columns for
``staging_1_cleansed`` must likewise be reconciled so that
``schemas.get_schema('staging_1_cleansed')`` resolves -- as authored it does (the
registry key matches the manifest table name), but the illustrative column model
must be confirmed against the procedure's true output. ``config/pipeline_manifest.yaml``
remains the canonical source of stage order, table names, and write modes; all I/O
here is manifest-driven, so reconciling the manifest / schemas requires no edit to
this job's control flow.

Job arguments (injected by ``infra/glue_jobs.tf``)
--------------------------------------------------
Required: ``JOB_NAME`` (Glue-provided), ``ddb_table_name``, ``aws_region``,
``delta_bucket`` (DELTA_S3_BUCKET, no scheme), ``manifest_path``, ``step`` (this
stage's unique manifest token, e.g. ``stage-1-cleanse-transactions``).
Optional-with-default: ``run_date`` (``""``).

Target runtime
--------------
AWS Glue 4.0 (Apache Spark 3.3.x, Python 3.10, Scala 2.12). Not Glue 5.0.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType, StructType

from lib.delta_io import count_rows, read_delta, write_delta
from lib.job_args import resolve_options
from lib.logging_utils import StageTimer, emit_completion_event, get_logger
from lib.manifest import Manifest, load_manifest
from lib.s3_paths import build_delta_table_uri
from lib.spark_session import build_spark_session
from schemas import get_schema

# Plain string columns whose only cleanse is whitespace trimming. ``currency_code``
# and ``debit_credit_indicator`` are intentionally excluded here because they get
# additional normalization (case-folding / canonical mapping) below.
_STRING_COLUMNS_TO_TRIM = (
    "gl_entry_id",
    "journal_id",
    "account_id",
    "cost_center",
    "source_system",
)

# Canonical debit / credit indicator vocabularies. Inbound feeds spell the
# indicator inconsistently; these map every accepted spelling to the single-letter
# canonical form. Any token outside both sets normalizes to NULL (the column is
# nullable in the target schema) rather than guessing a side.
_DEBIT_TOKENS = ("D", "DR", "DEBIT")
_CREDIT_TOKENS = ("C", "CR", "CREDIT")


def _resolve_glue_run_id() -> str:
    """Resolve the Glue job-run id best-effort (a CloudWatch log field only).

    Tries the optional ``--JOB_RUN_ID`` Glue argument first, then the
    ``JOB_RUN_ID`` environment variable, and finally falls back to the literal
    ``"unknown"``. The run id is purely a log field, so resolving it must NEVER
    fail the job; the broad ``except`` here is intentional and is deliberately
    OUTSIDE the ACID-critical read / write path (which is never guarded by a
    swallowing handler).

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
        get_logger("stage_1_cleanse_transactions").debug(
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
    :param step: The unique stage token for THIS job (e.g. ``"stage-1-cleanse-transactions"``).
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

    Read from ``manifest.raw["tables"]`` so each table's relative ``path`` and
    optional ``partition_by`` / ``write_mode`` stay config-driven.

    :param manifest: The loaded :class:`lib.manifest.Manifest`.
    :param name: The logical table name to resolve (e.g. ``"staging_1_cleansed"``).
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


def _conform_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Project ``df`` onto ``schema`` -- exact column names, order, and types.

    Selects exactly the fields declared by ``schema`` (in declaration order) and
    casts each to its declared :class:`~pyspark.sql.types.DataType`. This is what
    makes the schema-locked (``mergeSchema=false``) Delta write in :func:`main`
    succeed deterministically WITHOUT relying on schema inference: the written
    DataFrame's columns are guaranteed to match the explicit target ``StructType``
    resolved from the ``schemas/`` registry. A column required by ``schema`` but
    absent from ``df`` raises immediately (an ``AnalysisException``), which is the
    intended fail-fast behavior -- never silently dropped or merged.

    :param df: The transformed DataFrame to project.
    :param schema: The authoritative target :class:`StructType` (from
        :func:`schemas.get_schema`).
    :returns: ``df`` projected and cast to ``schema``.
    """
    projected = [
        F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields
    ]
    return df.select(*projected)


def cleanse_transactions(df: DataFrame) -> DataFrame:
    """Cleanse / normalize raw GL transactions at the ``gl_entry_id`` grain.

    The 1:1 PySpark replacement of ``dbo.usp_cleanse_transactions``. This is a
    PURE transformation (DataFrame in -> DataFrame out): it performs no I/O, builds
    no session, and reads no configuration, which keeps it trivially unit-testable.
    The operations are deterministic and inference-free, so re-running over the
    same input yields byte-identical output (idempotency; Gate 3):

    #. **Trim** leading / trailing whitespace on the plain string columns
       (``gl_entry_id``, ``journal_id``, ``account_id``, ``cost_center``,
       ``source_system``).
    #. **Currency** -- uppercase the trimmed ``currency_code`` to its canonical
       ISO-4217 form.
    #. **Debit / credit** -- map the trimmed, uppercased ``debit_credit_indicator``
       to the canonical ``"D"`` / ``"C"`` (accepting ``D``/``DR``/``DEBIT`` and
       ``C``/``CR``/``CREDIT``); any other token normalizes to ``NULL`` rather than
       guessing a side.
    #. **Amount** -- cast to an exact :class:`~pyspark.sql.types.DecimalType`
       (18, 2); never a floating-point type, so the Gate-1 parity hash stays
       byte-exact against the legacy baseline.
    #. **Posting date** -- defensively re-cast to an explicit ``DateType``.
    #. **Audit** -- stamp ``cleansed_at`` with the processing-time
       :func:`~pyspark.sql.functions.current_timestamp`.
    #. **Validity indicator** -- derive an ``is_valid`` Boolean column that is
       ``True`` only when every business key (``gl_entry_id``, ``account_id``,
       ``posting_date``, ``amount``) is non-null and the string keys are non-blank.
       Per the SP-replacement contract the indicator is **emitted as a column**
       (not used as a filter): rows are NOT silently dropped here, so the stage is
       row-count-preserving by construction and downstream consumers retain the
       explicit validity flag. The expression is total (each clause is an
       ``isNotNull`` / ``length`` / ``&`` that yields ``True`` or ``False`` but
       never ``NULL``), so ``is_valid`` is always populated and conforms to the
       non-nullable ``staging_1_cleansed.is_valid`` field. Because Stage 0 already
       validated ``staging_raw`` against its typed, non-nullable schema, in normal
       operation every row evaluates to ``True``; the column nonetheless makes the
       validity decision explicit and auditable.

    The returned DataFrame carries exactly the columns the ``staging_1_cleansed``
    schema declares (the ten ``staging_raw`` columns plus the derived ``is_valid``
    Boolean and ``cleansed_at``); :func:`main` additionally projects it through
    :func:`_conform_to_schema` to lock the column order and types before the
    schema-locked Delta write.

    Reconciliation note: this representative finance template MUST be reconciled 1:1
    with the real ``dbo.usp_cleanse_transactions`` body (including its exact
    ``is_valid`` derivation and any row-retention policy for invalid keys and
    unrecognized indicators) before production cutover. If the legacy procedure
    physically removes invalid rows rather than flagging them, that exclusion must
    be applied downstream (or here) to mirror the documented 1:1 behavior.

    :param df: The ``staging_raw`` DataFrame (typed per ``schemas.staging_raw``).
    :returns: The cleansed DataFrame (carrying the ``is_valid`` indicator), ready to
        be conformed to ``staging_1_cleansed``.
    """
    cleansed = df

    # (1) Trim whitespace on the plain string columns.
    for column_name in _STRING_COLUMNS_TO_TRIM:
        cleansed = cleansed.withColumn(column_name, F.trim(F.col(column_name)))

    # (2) Normalize currency_code to a trimmed, uppercased ISO-4217 token.
    cleansed = cleansed.withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))

    # (3) Normalize debit_credit_indicator to the canonical 'D' / 'C' (else NULL).
    normalized_indicator = F.upper(F.trim(F.col("debit_credit_indicator")))
    cleansed = cleansed.withColumn(
        "debit_credit_indicator",
        F.when(normalized_indicator.isin(*_DEBIT_TOKENS), F.lit("D"))
        .when(normalized_indicator.isin(*_CREDIT_TOKENS), F.lit("C"))
        .otherwise(F.lit(None)),
    )

    # (4) Keep amount as an exact DecimalType(18, 2) -- never a float.
    cleansed = cleansed.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))

    # (5) Keep posting_date as an explicit DateType (defensive re-cast).
    cleansed = cleansed.withColumn("posting_date", F.col("posting_date").cast("date"))

    # (6) Stamp the cleanse audit timestamp.
    cleansed = cleansed.withColumn("cleansed_at", F.current_timestamp())

    # (7) Derive the is_valid Boolean indicator and EMIT it as a column (do not
    #     filter). The expression is total -- each clause is an isNotNull / length
    #     comparison combined with ``&`` -- so it yields True/False but never NULL,
    #     satisfying the non-nullable staging_1_cleansed.is_valid field. Emitting
    #     (rather than dropping) keeps the stage row-count-preserving and surfaces
    #     the validity decision explicitly to downstream stages and parity checks.
    is_valid = (
        F.col("gl_entry_id").isNotNull()
        & (F.length(F.col("gl_entry_id")) > 0)
        & F.col("account_id").isNotNull()
        & (F.length(F.col("account_id")) > 0)
        & F.col("posting_date").isNotNull()
        & F.col("amount").isNotNull()
    )
    return cleansed.withColumn("is_valid", is_valid)


def main() -> None:
    """Execute Stage 1 (cleanse transactions) end to end.

    Resolves Glue arguments, builds the LogStore-wired :class:`SparkSession`, loads
    the manifest, resolves this stage's input / output tables and their Delta
    locations entirely from ``manifest.raw`` (config-driven, never hardcoded), reads
    ``staging_raw``, applies :func:`cleanse_transactions`, conforms the result to the
    explicit ``staging_1_cleansed`` schema, writes it through :mod:`lib.delta_io`
    using the manifest's ``write_mode`` (``overwrite``), and emits the mandated
    six-field completion event exactly once on the success path.

    The Spark session is stopped in a ``finally`` that does NOT suppress exceptions:
    any failure in the read / write path -- including a DynamoDB conditional-write
    failure surfaced by ``S3DynamoDBLogStore`` -- propagates so the Glue job exits
    non-zero (ACID strictness; there is no non-ACID fallback).
    """
    args = resolve_options(
        required=[
            "JOB_NAME",
            "ddb_table_name",
            "aws_region",
            "delta_bucket",
            "manifest_path",
            "step",
        ],
        optional={"run_date": ""},
    )
    glue_run_id = _resolve_glue_run_id()
    logger = get_logger("stage_1_cleanse_transactions")

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
        # A transform stage reads exactly one table and writes exactly one table.
        in_name = stage["reads"][0]
        out_name = stage["writes"][0]
        in_table = _resolve_table(manifest, in_name)
        out_table = _resolve_table(manifest, out_name)
        in_path = _delta_path(args["delta_bucket"], defaults, in_table)
        out_path = _delta_path(args["delta_bucket"], defaults, out_table)
        partition_by = out_table.get("partition_by")
        # The manifest -- not this file -- decides the write mode (overwrite here),
        # so idempotency for staging tables is enforced centrally.
        write_mode = out_table["write_mode"]

        # Explicit typed target schema for the output table (schemas/ registry). The
        # registry key equals the manifest table name, keeping the lookup
        # manifest-driven; the column list itself is never hardcoded in this job.
        target_schema = get_schema(out_name)

        # Time the data work so the completion event's elapsed_seconds covers
        # read -> cleanse -> conform -> write.
        with StageTimer() as timer:
            src_df = read_delta(spark, in_path)
            input_rows = count_rows(src_df)
            # Pure transform, then project / cast to the explicit target schema so
            # the schema-locked (mergeSchema=false) write succeeds without inference.
            out_df = cleanse_transactions(src_df)
            out_df = _conform_to_schema(out_df, target_schema)
            # ACID write via S3DynamoDBLogStore (mergeSchema=false inside lib.delta_io);
            # overwrite gives the stage its structural idempotency (Gate 3). Using
            # write_delta routes the manifest's write_mode rather than hardcoding it.
            write_delta(spark, out_df, out_path, write_mode, partition_by=partition_by)
            output_rows = count_rows(out_df)

        # Exactly one six-field completion event, emitted on the success path only.
        # Transforms have no bad-record routing, so bad_record_count is always 0.
        emit_completion_event(
            job_name=args["JOB_NAME"],
            glue_run_id=glue_run_id,
            input_rows=input_rows,
            output_rows=output_rows,
            bad_record_count=0,
            elapsed_seconds=timer.elapsed,
            logger=logger,
            stage_id=args["step"],
            output_table=out_name,
        )
    finally:
        # Always release the session. A bare stop() in ``finally`` does NOT suppress
        # an in-flight exception, so ACID failures still propagate and fail the Glue
        # run non-zero (ACID strictness).
        spark.stop()


if __name__ == "__main__":
    main()
