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
"""Stage 3 (Transform) AWS Glue 4.0 / PySpark entrypoint -- ``dbo.usp_compute_balances``.

This module is the **1:1 PySpark replacement** for the legacy SQL Server stored
procedure ``dbo.usp_compute_balances`` (stage 3, the final transform stage, of
the config-driven pipeline declared in ``config/pipeline_manifest.yaml``). It is
the only stage that **changes grain**: it reads the prior stage's Delta table
``staging_2_enriched`` (one row per ``gl_entry_id``), rolls the general-ledger
entries up to one row per ``(account_id, posting_date)`` -- additionally split by
``currency_code`` and ``cost_center`` -- and writes the next Delta table
``staging_3_balances`` in ``overwrite`` mode. Overwriting a deterministic
aggregation makes the stage structurally idempotent: re-running the same source /
run id yields identical balances with no row growth (Gate 3). Every Delta commit
is coordinated through ``io.delta.storage.S3DynamoDBLogStore`` with full ACID
guarantees.

This is a standalone Glue ``spark-submit`` script, NOT part of an importable
package (there is intentionally no ``jobs/__init__.py``); AWS Glue places ``lib/``
and ``schemas/`` on ``PYTHONPATH`` at runtime via ``--extra-py-files``, so this
module uses absolute ``from lib.x import ...`` / ``from schemas import ...`` imports.
It mirrors the structure, shared helpers, and control flow of
``jobs/stage_1_cleanse_transactions.py`` and ``jobs/stage_2_enrich_accounts.py`` so
that every per-stage job stays uniform and trivially reconcilable against its real
stored procedure.

Responsibility (in order)
-------------------------
#. Read the ``staging_2_enriched`` Delta table written by Stage 2 (typed, already
   conformed to ``schemas.staging_tables.STAGING_2_ENRICHED``, at ``gl_entry_id``
   grain).
#. Apply :func:`compute_balances`: derive a signed amount per entry, aggregate to
   the ``(account_id, posting_date, currency_code, cost_center)`` grain producing a
   rolled-up ``balance_amount`` (net), ``debit_amount`` / ``credit_amount``
   subtotals, an ``entry_count``, carry the account attributes forward, and stamp a
   ``computed_at`` audit timestamp.
#. Conform the result to the explicit ``StructType`` for ``staging_3_balances``
   (resolved from the ``schemas/`` registry) so the schema-locked write succeeds.
#. Overwrite-write ``staging_3_balances`` through :mod:`lib.delta_io`.
#. Emit the mandated six-field CloudWatch completion event exactly once. The
   ``input_rows`` count is the entry-grain rows read; ``output_rows`` is the
   (smaller) count of aggregated balance rows written.

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
  :func:`schemas.get_schema`; the aggregated output is cast to it before writing.
* **Exact monetary arithmetic.** Every summed monetary column is cast explicitly to
  ``DecimalType(18, 2)`` (never a floating-point type) so the rolled-up balances are
  byte-exact and the Gate-1 five-field parity hash reproduces against the SQL Server
  baseline.
* **ACID strictness -- no non-ACID fallback.** The Delta read and write are NEVER
  wrapped in a swallowing / downgrading / retrying ``try``/``except``. The only
  ``try``/``finally`` here stops the Spark session in ``finally`` WITHOUT
  suppressing the in-flight exception, so a failed DynamoDB conditional write
  (surfaced by ``S3DynamoDBLogStore``) fails the Glue run non-zero.
* **Structural idempotency.** ``overwrite`` replaces the table wholesale; the job
  never appends. Because the aggregation is deterministic and order-independent,
  every business column is reproduced exactly across re-runs.

Reconciliation note (PROMINENT)
-------------------------------
The exact balance-computation logic MUST be reconciled 1:1 with the real
``dbo.usp_compute_balances`` definition before production cutover; the operations
in :func:`compute_balances` are a representative finance template (a signed-amount
sign convention, a net balance plus explicit-side debit / credit subtotals, and a
contributing-entry count), not the verified procedure body. In particular, the
signed-amount convention used here -- negate ``amount`` when the (Stage-1
canonicalized) ``debit_credit_indicator`` is ``"C"`` and keep it otherwise -- is the
same representative convention documented in ``jobs/stage_2_enrich_accounts.py``;
confirm the procedure's true sign rule, its grain (whether it splits by
``currency_code`` / ``cost_center`` or keys strictly on
``(account_id, posting_date)``), and whether a NULL / unknown side should be folded
into a subtotal (this template leaves a NULL-side entry out of both the debit and
credit subtotals, mirroring Stage 1's "unknown -> NULL, do not guess a side" rule,
while still counting it in the net ``balance_amount``). The ``schemas/`` registry
key and columns for ``staging_3_balances`` must likewise be reconciled so that
``schemas.get_schema('staging_3_balances')`` resolves -- as authored it does (the
registry key matches the manifest table name and ``schemas.staging_tables`` already
declares ``STAGING_3_BALANCES``), but the illustrative column model must be
confirmed against the procedure's true output. ``config/pipeline_manifest.yaml``
remains the canonical source of stage order, table names, and write modes; all I/O
here is manifest-driven, so reconciling the manifest / schemas requires no edit to
this job's control flow.

Job arguments (injected by ``infra/glue_jobs.tf``)
--------------------------------------------------
Required: ``JOB_NAME`` (Glue-provided), ``ddb_table_name``, ``aws_region``,
``delta_bucket`` (DELTA_S3_BUCKET, no scheme), ``manifest_path``, ``step`` (this
stage's unique manifest token, e.g. ``stage-3-compute-balances``).
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
from lib.spark_session import build_spark_session
from schemas import get_schema

# ---------------------------------------------------------------------------
# Monetary precision / scale for every summed currency-denominated column. Kept
# identical to ``schemas/staging_tables.py`` (DECIMAL(18, 2)) so the explicit casts
# applied to the aggregates here match the authoritative ``staging_3_balances``
# schema exactly -- floats are never used, keeping the rolled-up balances exact and
# the Gate-1 parity hash byte-reproducible.
# ---------------------------------------------------------------------------
_MONEY_PRECISION = 18
_MONEY_SCALE = 2

# Canonical single-letter debit / credit indicator values. Stage 1
# (``dbo.usp_cleanse_transactions``) already normalizes every accepted spelling to
# exactly ``"D"`` / ``"C"`` (or NULL for an unrecognized side), so this stage matches
# against the canonical forms only. Centralized so the sign convention and the
# subtotal split stay consistent and are trivial to reconcile against the real
# ``dbo.usp_compute_balances`` rule.
_DEBIT_INDICATOR = "D"
_CREDIT_INDICATOR = "C"


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
    except Exception:
        # Best-effort only: the run id is a non-essential log field, so any
        # failure to resolve it (for example ``awsglue`` being unavailable in a
        # local / test context) must NOT fail the job. This handler does not
        # guard any Delta / DynamoDB operation, so it preserves ACID strictness.
        pass
    return os.environ.get("JOB_RUN_ID", "") or "unknown"


def _resolve_stage(manifest: Manifest, step: str) -> dict:
    """Return the manifest stage mapping whose ``step`` token equals ``step``.

    Topology is read from ``manifest.raw["stages"]`` (the full parsed YAML) so the
    job stays generic and manifest-driven rather than hardcoding stage metadata.

    :param manifest: The loaded :class:`lib.manifest.Manifest`.
    :param step: The unique stage token for THIS job (e.g. ``"stage-3-compute-balances"``).
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
    :param name: The logical table name to resolve (e.g. ``"staging_3_balances"``).
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
    """Compose the fully qualified ``s3a://`` Delta table location.

    Built as ``s3a://{delta_bucket}/{defaults['delta_path_prefix']}/{table['path']}``
    with defensive slash-stripping so stray separators in the manifest never
    produce a doubled ``//``. ``delta_bucket`` is the scheme-less DELTA_S3_BUCKET
    job arg; the prefix and per-table path are relative, env-agnostic manifest
    values, keeping this composition portable across dev / nonprod / prod.

    :param delta_bucket: Destination bucket name, without an ``s3a://`` scheme.
    :param defaults: The manifest ``defaults`` mapping (uses ``delta_path_prefix``).
    :param table: The resolved table mapping (uses its relative ``path``).
    :returns: The fully qualified ``s3a://`` location of the Delta table.
    """
    bucket = delta_bucket.strip().strip("/")
    prefix = str(defaults.get("delta_path_prefix", "")).strip("/")
    rel_path = str(table.get("path", "")).strip("/")
    segments = [segment for segment in (prefix, rel_path) if segment]
    return f"s3a://{bucket}/" + "/".join(segments)


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


def compute_balances(df: DataFrame) -> DataFrame:
    """Aggregate enriched GL entries into per-account, per-date balances.

    The 1:1 PySpark replacement of ``dbo.usp_compute_balances`` and the only
    transform in the chain that **changes grain**: the input is at the
    ``gl_entry_id`` row level (``staging_2_enriched``) and the output is rolled up
    to one row per ``(account_id, posting_date, currency_code, cost_center)``. This
    is a PURE transformation (DataFrame in -> DataFrame out): it performs no I/O,
    builds no session, and reads no configuration, which keeps it trivially
    unit-testable. Every operation is deterministic and order-independent, so
    re-running over the same input yields byte-identical output for every business
    column (idempotency; Gate 3 compares the manifest's five-field business hash
    ``[account_id, posting_date, balance_amount, currency_code, cost_center]``,
    which excludes the processing-time ``computed_at`` audit stamp):

    #. **Signed amount** -- per entry, negate ``amount`` when the (Stage-1
       canonicalized) ``debit_credit_indicator`` is ``"C"`` (a credit) and keep it
       otherwise (a debit, or an unrecognized / NULL side); the indicator is
       defensively re-trimmed / upper-cased so a stray spelling cannot leak a wrong
       sign.
    #. **Net balance** -- ``balance_amount`` is the group sum of the signed amount,
       cast explicitly to ``DECIMAL(18, 2)`` so the rolled-up balance is exact.
    #. **Debit / credit subtotals** -- ``debit_amount`` / ``credit_amount`` are the
       group sums of ``amount`` restricted to the explicit ``"D"`` / ``"C"`` side
       respectively, each cast to ``DECIMAL(18, 2)``. A NULL / unknown side belongs
       to neither subtotal (mirroring Stage 1's "unknown -> NULL, do not guess a
       side" rule) and is NULL when a group has no rows on that side.
    #. **Entry count** -- ``entry_count`` is the number of contributing GL entries
       in the group (one input row == one entry, so a plain row count).
    #. **Carried account attributes** -- ``account_name`` / ``account_type`` are
       carried forward via :func:`~pyspark.sql.functions.max`; because Stage 2
       derives both deterministically from ``account_id`` they are constant within
       an account group, so ``max`` is an order-independent, deterministic way to
       surface that single value (it also tolerates an all-NULL group by yielding
       NULL). They feed the downstream ``dim_account_snapshot`` output table.
    #. **Audit** -- stamp ``computed_at`` with the processing-time
       :func:`~pyspark.sql.functions.current_timestamp`; this column is excluded
       from the parity hash, so its per-run value does not affect Gate 1 / Gate 3.

    :func:`main` additionally projects the result through :func:`_conform_to_schema`
    to lock the exact column set, order, and types of ``staging_3_balances`` before
    the schema-locked (``mergeSchema=false``) Delta write.

    Reconciliation note: this representative finance template MUST be reconciled 1:1
    with the real ``dbo.usp_compute_balances`` body before production cutover --
    confirm the sign convention, the aggregation grain, the subtotal treatment of a
    NULL side, and the entry-count definition against the procedure.

    :param df: The ``staging_2_enriched`` DataFrame (typed per
        ``schemas.staging_tables.STAGING_2_ENRICHED``, at ``gl_entry_id`` grain).
    :returns: The aggregated balances DataFrame, ready to be conformed to
        ``staging_3_balances``.
    """
    money = DecimalType(_MONEY_PRECISION, _MONEY_SCALE)

    # Defensive canonical read of the (already Stage-1-canonicalized) indicator.
    # ``coalesce(..., lit(False))`` collapses three-valued logic so a NULL indicator
    # is treated as "not a credit" / "not a debit" rather than propagating NULL into
    # the sign decision or the subtotal predicates.
    indicator = F.upper(F.trim(F.col("debit_credit_indicator")))
    is_credit = F.coalesce(indicator == F.lit(_CREDIT_INDICATOR), F.lit(False))
    is_debit = F.coalesce(indicator == F.lit(_DEBIT_INDICATOR), F.lit(False))

    # (1) Signed amount: a credit reduces the running balance; everything else (a
    #     debit, or an unrecognized / NULL side) keeps the amount positive. This is
    #     the representative sign convention documented in Stage 2.
    signed_amount = F.when(is_credit, -F.col("amount")).otherwise(F.col("amount"))
    # (3) Explicit-side subtotals: a NULL / unknown indicator contributes to neither
    #     subtotal (Stage 1 maps unknown tokens to NULL rather than guessing a side).
    #     ``when`` with no ``otherwise`` yields NULL off-side, and ``sum`` skips NULLs,
    #     so a group with no rows on a side aggregates to NULL (the column is nullable).
    debit_value = F.when(is_debit, F.col("amount"))
    credit_value = F.when(is_credit, F.col("amount"))

    # Aggregate to the (account_id, posting_date, currency_code, cost_center) grain.
    # Monetary sums are cast back to the exact DECIMAL(18, 2) target type (Spark widens
    # the precision of a decimal SUM), so no float ever enters a balance column.
    aggregated = df.groupBy("account_id", "posting_date", "currency_code", "cost_center").agg(
        F.sum(signed_amount).cast(money).alias("balance_amount"),
        F.sum(debit_value).cast(money).alias("debit_amount"),
        F.sum(credit_value).cast(money).alias("credit_amount"),
        # (4) One input row per GL entry, so the row count is the contributing-entry
        #     count; ``count(lit(1))`` is non-null and deterministic.
        F.count(F.lit(1)).alias("entry_count"),
        # (5) Carry the (account-constant) descriptive attributes forward.
        F.max(F.col("account_name")).alias("account_name"),
        F.max(F.col("account_type")).alias("account_type"),
    )

    # (6) Processing-time audit stamp; deliberately added AFTER the aggregation and
    #     excluded from the five-field parity hash, so its non-determinism never
    #     affects Gate 1 parity or Gate 3 idempotency.
    aggregated = aggregated.withColumn("computed_at", F.current_timestamp())

    return aggregated


def main() -> None:
    """Execute Stage 3 (compute balances) end to end.

    Resolves Glue arguments, builds the LogStore-wired :class:`SparkSession`, loads
    the manifest, resolves this stage's input / output tables and their Delta
    locations entirely from ``manifest.raw`` (config-driven, never hardcoded), reads
    ``staging_2_enriched``, applies :func:`compute_balances`, conforms the aggregated
    result to the explicit ``staging_3_balances`` schema, writes it through
    :mod:`lib.delta_io` using the manifest's ``write_mode`` (``overwrite``), and emits
    the mandated six-field completion event exactly once on the success path. The
    ``input_rows`` figure is the entry-grain rows read; ``output_rows`` is the
    (aggregated) balance rows written.

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
    logger = get_logger("stage_3_compute_balances")

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
        # read -> aggregate -> conform -> write.
        with StageTimer() as timer:
            src_df = read_delta(spark, in_path)
            input_rows = count_rows(src_df)
            # Pure aggregation, then project / cast to the explicit target schema so
            # the schema-locked (mergeSchema=false) write succeeds without inference.
            out_df = compute_balances(src_df)
            out_df = _conform_to_schema(out_df, target_schema)
            # ACID write via S3DynamoDBLogStore (mergeSchema=false inside lib.delta_io);
            # overwrite gives the stage its structural idempotency (Gate 3). Using
            # write_delta routes the manifest's write_mode rather than hardcoding it.
            write_delta(spark, out_df, out_path, write_mode, partition_by=partition_by)
            output_rows = count_rows(out_df)

        # Exactly one six-field completion event, emitted on the success path only.
        # Transforms have no bad-record routing, so bad_record_count is always 0.
        # input_rows = entry-grain rows read; output_rows = aggregated rows written.
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
