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
"""Stage 2 (Transform) AWS Glue 4.0 / PySpark entrypoint -- ``dbo.usp_enrich_accounts``.

This module is the **1:1 PySpark replacement** for the legacy SQL Server stored
procedure ``dbo.usp_enrich_accounts`` (stage 2 of the config-driven pipeline
declared in ``config/pipeline_manifest.yaml``). It reads the prior stage's Delta
table ``staging_1_cleansed``, applies a deterministic account-enrichment
transformation, and writes the next Delta table ``staging_2_enriched`` in
``overwrite`` mode so re-running the same source / run id yields identical output
with no row growth (structural idempotency; Gate 3). Every Delta commit is
coordinated through ``io.delta.storage.S3DynamoDBLogStore`` with full ACID
guarantees.

This is a standalone Glue ``spark-submit`` script, NOT part of an importable
package (there is intentionally no ``jobs/__init__.py``); AWS Glue places ``lib/``
and ``schemas/`` on ``PYTHONPATH`` at runtime via ``--extra-py-files``, so this
module uses absolute ``from lib.x import ...`` / ``from schemas import ...`` imports.
It mirrors the structure, shared helpers, and control flow of
``jobs/stage_1_cleanse_transactions.py`` so that every per-stage job stays uniform
and trivially reconcilable against its real stored procedure.

Responsibility (in order)
-------------------------
#. Read the ``staging_1_cleansed`` Delta table written by Stage 1 (typed, already
   conformed to ``schemas.staging_tables.STAGING_1_CLEANSED``).
#. Apply :func:`enrich_accounts`: derive an ``account_type`` from the account-id
   leading digit, derive a deterministic ``account_name``, normalize the
   ``cost_center``, carry the cleansed business keys forward, and stamp an
   ``enriched_at`` audit timestamp.
#. Conform the result to the explicit ``StructType`` for ``staging_2_enriched``
   (resolved from the ``schemas/`` registry) so the schema-locked write succeeds.
#. Overwrite-write ``staging_2_enriched`` through :mod:`lib.delta_io`.
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
The exact enrichment logic MUST be reconciled 1:1 with the real
``dbo.usp_enrich_accounts`` definition before production cutover; the operations
in :func:`enrich_accounts` are a representative finance template
(account-type classification by leading digit / deterministic account-name label /
cost-center normalization), not the verified procedure body. In particular, a real
enrichment commonly **joins a reference / account-dimension table** to source
``account_name`` / ``account_type`` rather than deriving them in-place -- wire that
join in here once the dimension source is confirmed. The
``signed_amount`` sign convention (negate ``amount`` when ``debit_credit_indicator``
is ``"C"``, keep it otherwise) is part of that broader template and **is emitted**
here as a ``DecimalType(18, 2)`` column, because the authoritative
``staging_2_enriched`` schema declares ``signed_amount`` as a NOT-NULL
``DecimalType(18, 2)`` field that Stage 3 consumes directly -- it sums this column
(``sum(signed_amount)``) into ``balance_amount``; reconcile this sign convention
1:1 against the procedure's exact debit / credit treatment. The ``schemas/``
registry key and columns for ``staging_2_enriched`` must
likewise be reconciled so that ``schemas.get_schema('staging_2_enriched')``
resolves -- as authored it does (the registry key matches the manifest table name),
but the illustrative column model must be confirmed against the procedure's true
output. ``config/pipeline_manifest.yaml`` remains the canonical source of stage
order, table names, and write modes; all I/O here is manifest-driven, so reconciling
the manifest / schemas requires no edit to this job's control flow.

Job arguments (injected by ``infra/glue_jobs.tf``)
--------------------------------------------------
Required: ``JOB_NAME`` (Glue-provided), ``ddb_table_name``, ``aws_region``,
``delta_bucket`` (DELTA_S3_BUCKET, no scheme), ``manifest_path``, ``step`` (this
stage's unique manifest token, e.g. ``stage-2-enrich-accounts``).
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

# Account-type labels derived from the leading digit of ``account_id``, following the
# conventional general-ledger chart-of-accounts ranges (1xxx assets, 2xxx liabilities,
# 3xxx equity, 4xxx revenue, 5xxx expenses). Centralized so the representative
# classification stays consistent and is trivial to reconcile against the real
# ``dbo.usp_enrich_accounts`` mapping (or an account-dimension join). The mapping is
# total: any leading character outside this set classifies as ``UNCLASSIFIED`` rather
# than producing a null account_type.
_ACCOUNT_TYPE_BY_LEADING_DIGIT = (
    ("1", "ASSET"),
    ("2", "LIABILITY"),
    ("3", "EQUITY"),
    ("4", "REVENUE"),
    ("5", "EXPENSE"),
)
_UNCLASSIFIED_ACCOUNT_TYPE = "UNCLASSIFIED"

# Canonical debit / credit indicator tokens (Stage 1 normalizes the raw indicator to
# exactly these). Used to derive ``signed_amount``: a credit ("C") is stored negative,
# every other side (a debit "D", or an unrecognized / NULL token) is stored positive.
# These MUST stay identical to the Stage 3 convention so the Stage 3 ``sum(signed_amount)``
# rollup yields byte-identical ``balance_amount`` values (Gate-1 parity).
_CREDIT_INDICATOR = "C"
# Exact monetary type for ``signed_amount`` -- DECIMAL(18, 2), never a floating-point
# type, matching ``amount`` and the ``staging_2_enriched.signed_amount`` schema field.
_MONEY_PRECISION = 18
_MONEY_SCALE = 2


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
        get_logger("stage_2_enrich_accounts").debug(
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
    :param step: The unique stage token for THIS job (e.g. ``"stage-2-enrich-accounts"``).
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
    :param name: The logical table name to resolve (e.g. ``"staging_2_enriched"``).
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


def enrich_accounts(df: DataFrame) -> DataFrame:
    """Enrich cleansed GL transactions with account attributes at the ``gl_entry_id`` grain.

    The 1:1 PySpark replacement of ``dbo.usp_enrich_accounts``. This is a PURE
    transformation (DataFrame in -> DataFrame out): it performs no I/O, builds no
    session, and reads no configuration, which keeps it trivially unit-testable.
    The operations are deterministic and inference-free, so re-running over the same
    input yields byte-identical output for every column except the processing-time
    ``enriched_at`` audit stamp (idempotency; Gate 3 compares the manifest's
    five-field business hash, which excludes ``enriched_at``):

    #. **Cost center** -- trim and uppercase ``cost_center`` to a canonical token.
    #. **Account type** -- derive ``account_type`` from the leading digit of the
       trimmed ``account_id`` using the conventional general-ledger chart-of-accounts
       ranges (``1`` asset, ``2`` liability, ``3`` equity, ``4`` revenue, ``5``
       expense); any other / absent leading character maps to ``"UNCLASSIFIED"`` so
       the column is total and never null.
    #. **Account name** -- derive a deterministic, human-readable ``account_name`` by
       joining the derived ``account_type`` with the trimmed ``account_id`` via
       :func:`~pyspark.sql.functions.concat_ws` (which skips null segments, so the
       label is well-defined even if a segment is unexpectedly null).
    #. **Signed amount** -- derive ``signed_amount`` by applying the debit / credit
       sign convention to ``amount``: a credit (canonical ``debit_credit_indicator``
       ``"C"``) is stored negative, while a debit (``"D"``) or any unrecognized /
       NULL side keeps the positive ``amount``. The result is cast to the exact
       ``DecimalType(18, 2)`` (never a float) so it conforms to the non-nullable
       ``staging_2_enriched.signed_amount`` field and stays byte-exact. This is the
       **single, authoritative** sign derivation for the pipeline: Stage 3 consumes
       this column directly (``sum(signed_amount)``) rather than re-deriving the sign,
       so the convention lives in exactly one place.
    #. **Audit** -- stamp ``enriched_at`` with the processing-time
       :func:`~pyspark.sql.functions.current_timestamp`.

    All other cleansed business keys (``gl_entry_id``, ``journal_id``, ``account_id``,
    ``posting_date``, ``amount``, ``currency_code``, ``debit_credit_indicator``,
    ``source_system``, ``load_ts``) are carried forward unchanged; the Stage-1
    ``cleansed_at`` audit column is intentionally dropped because the
    ``staging_2_enriched`` schema does not declare it. :func:`main` additionally
    projects the result through :func:`_conform_to_schema` to lock the exact column
    set, order, and types before the schema-locked (``mergeSchema=false``) Delta write.

    Reconciliation note: this representative finance template MUST be reconciled 1:1
    with the real ``dbo.usp_enrich_accounts`` body before production cutover. A real
    enrichment commonly **joins a reference / account-dimension table** to source
    ``account_name`` / ``account_type`` rather than deriving them from the account-id
    prefix; wire that join here once the dimension source is confirmed. The
    ``signed_amount`` sign convention (negate ``amount`` when ``debit_credit_indicator``
    is ``"C"``, otherwise keep it) must likewise be reconciled against the procedure's
    exact debit/credit treatment; it is emitted here as the authoritative signed value
    that Stage 3 sums into ``balance_amount``.

    :param df: The ``staging_1_cleansed`` DataFrame (typed per
        ``schemas.staging_tables.STAGING_1_CLEANSED``).
    :returns: The enriched DataFrame (carrying the derived ``signed_amount``), ready
        to be conformed to ``staging_2_enriched``.
    """
    enriched = df

    # (1) Normalize cost_center to a trimmed, uppercased canonical token.
    enriched = enriched.withColumn("cost_center", F.upper(F.trim(F.col("cost_center"))))

    # (2) Derive account_type from the leading digit of the trimmed account_id. The
    #     when/otherwise chain is built from the canonical mapping so the rule lives in
    #     exactly one place; the terminal otherwise makes the classification total
    #     (never null) for any leading character outside the mapped set.
    leading_digit = F.substring(F.trim(F.col("account_id")), 1, 1)
    account_type = F.lit(_UNCLASSIFIED_ACCOUNT_TYPE)
    for digit, type_label in reversed(_ACCOUNT_TYPE_BY_LEADING_DIGIT):
        account_type = F.when(leading_digit == F.lit(digit), F.lit(type_label)).otherwise(
            account_type
        )
    enriched = enriched.withColumn("account_type", account_type)

    # (3) Derive a deterministic, human-readable account_name from the derived
    #     account_type and the trimmed account_id. concat_ws skips null segments, so the
    #     label is well-defined even if a segment is unexpectedly null.
    enriched = enriched.withColumn(
        "account_name",
        F.concat_ws(" ", F.col("account_type"), F.trim(F.col("account_id"))),
    )

    # (4) Derive and EMIT signed_amount: a credit ("C") is stored negative, every
    #     other side (debit "D", or an unrecognized / NULL token) stays positive.
    #     ``coalesce(..., lit(False))`` collapses three-valued logic so a NULL
    #     indicator is treated as "not a credit" rather than propagating NULL into the
    #     sign decision. The cast to the exact DECIMAL(18, 2) (amount is already
    #     DECIMAL(18, 2), so +/- amount is lossless) conforms to the non-nullable
    #     staging_2_enriched.signed_amount field. This is the single authoritative
    #     sign derivation; Stage 3 consumes this column via sum(signed_amount) instead
    #     of re-deriving the sign, keeping the convention in exactly one place and
    #     guaranteeing identical balance_amount rollups (Gate-1 parity).
    indicator = F.upper(F.trim(F.col("debit_credit_indicator")))
    is_credit = F.coalesce(indicator == F.lit(_CREDIT_INDICATOR), F.lit(False))
    signed_amount = F.when(is_credit, -F.col("amount")).otherwise(F.col("amount"))
    enriched = enriched.withColumn(
        "signed_amount", signed_amount.cast(DecimalType(_MONEY_PRECISION, _MONEY_SCALE))
    )

    # (5) Stamp the enrichment audit timestamp.
    enriched = enriched.withColumn("enriched_at", F.current_timestamp())

    return enriched


def main() -> None:
    """Execute Stage 2 (enrich accounts) end to end.

    Resolves Glue arguments, builds the LogStore-wired :class:`SparkSession`, loads
    the manifest, resolves this stage's input / output tables and their Delta
    locations entirely from ``manifest.raw`` (config-driven, never hardcoded), reads
    ``staging_1_cleansed``, applies :func:`enrich_accounts`, conforms the result to
    the explicit ``staging_2_enriched`` schema, writes it through :mod:`lib.delta_io`
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
    logger = get_logger("stage_2_enrich_accounts")

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
        # read -> enrich -> conform -> write.
        with StageTimer() as timer:
            src_df = read_delta(spark, in_path)
            input_rows = count_rows(src_df)
            # Pure transform, then project / cast to the explicit target schema so
            # the schema-locked (mergeSchema=false) write succeeds without inference.
            out_df = enrich_accounts(src_df)
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
