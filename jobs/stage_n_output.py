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
"""Stage N (Output) AWS Glue 4.0 / PySpark entrypoint -- ``dbo.usp_load_gl_outputs``.

This module is the **1:1 PySpark replacement** for the legacy SQL Server stored
procedure ``dbo.usp_load_gl_outputs`` (the terminal ``output`` stage -- stage 4 --
of the config-driven pipeline declared in ``config/pipeline_manifest.yaml``,
``step: stage-n-output``). Unlike the single-table transform stages, this stage
reads the prior stage's Delta table(s) and writes **one or more final output Delta
tables**, where **each output table's write mode is sourced from the manifest**:
either a ``merge`` upsert (with a manifest-defined merge condition) or an
``overwrite`` truncate-and-reload. Per the authoritative manifest this stage writes
two tables:

* ``fact_general_ledger`` -- ``merge`` upsert on the composite key
  ``(gl_entry_id, posting_date)``.
* ``dim_account_snapshot`` -- ``overwrite`` (truncate-and-reload).

Every Delta commit is coordinated through ``io.delta.storage.S3DynamoDBLogStore``
with full ACID guarantees. These output tables are the surface compared in
**Gate 1 (Parity)**: a row count plus a five-field hash must match the legacy
SQL Server baseline at >= 99.99% for 100% of tables.

This is a standalone Glue ``spark-submit`` script, NOT part of an importable
package (there is intentionally no ``jobs/__init__.py``); AWS Glue places ``lib/``
and ``schemas/`` on ``PYTHONPATH`` at runtime via ``--extra-py-files``, so this
module uses absolute ``from lib.x import ...`` / ``from schemas import ...`` imports.
It mirrors the structure, shared helpers, and control flow of the transform stages
(``jobs/stage_1_*.py`` .. ``jobs/stage_3_*.py``) so every per-stage job stays uniform
and trivially reconcilable against its real stored procedure.

Responsibility (in order)
-------------------------
#. Read EVERY Delta table named in this stage's manifest ``reads`` list (currently
   the ENTRY-GRAIN ``staging_2_enriched`` -- one row per GL entry), keyed by logical
   table name, so expanding ``reads`` in the manifest adds inputs with no change to
   this job's control flow.
#. For each table named in this stage's manifest ``writes`` list, invoke the
   registered builder (the 1:1 stored-procedure logic) to produce a DataFrame that
   conforms exactly to that table's explicit ``StructType`` (resolved from the
   ``schemas/`` registry).
#. Write each output through :mod:`lib.delta_io` using the manifest's per-table
   ``write_mode`` (``merge`` or ``overwrite``); a ``merge`` passes the manifest's
   merge condition after deterministic alias normalization (see
   :func:`_normalize_merge_condition`).
#. Emit the mandated six-field CloudWatch completion event exactly once for the
   whole stage (NOT once per table); per-table row counts ride along as optional
   ``**extra`` fields.

Non-negotiable constraints encoded here (AAP s0.1.2 / s0.7.1)
-------------------------------------------------------------
* **LogStore = S3DynamoDBLogStore only.** The :class:`~pyspark.sql.SparkSession`
  is built exclusively via :func:`lib.spark_session.build_spark_session`; this file
  never constructs a session directly and never sets a LogStore config inline.
* **Schema-merge disabled on every write.** All Delta writes (``merge`` AND
  ``overwrite``) go through :mod:`lib.delta_io`, which keeps the write-time
  schema-merge flag pinned off; this file never calls ``df.write.format("delta")``
  or hand-rolls a ``DeltaTable.merge``, and never enables ``mergeSchema=true`` /
  ``inferSchema`` / ``spark.databricks.delta.schema.autoMerge.enabled``.
* **Explicit schema only -- no inference.** Each output schema is the explicit
  ``StructType`` resolved from the ``schemas/`` registry via
  :func:`schemas.get_schema`; every builder's output is cast to it before writing.
* **ACID strictness -- no non-ACID fallback.** The Delta reads, the ``merge``, and
  the ``overwrite`` are NEVER wrapped in a swallowing / downgrading / retrying
  ``try``/``except``. The only ``try``/``finally`` here stops the Spark session in
  ``finally`` WITHOUT suppressing the in-flight exception, so a failed DynamoDB
  conditional write (surfaced by ``S3DynamoDBLogStore``) fails the Glue run non-zero.
* **Structural idempotency (Gate 3).** ``merge`` upserts on stable keys and
  ``overwrite`` replaces wholesale; the job never appends. Because every builder is
  a deterministic, order-independent transformation, re-running the same source /
  run id yields identical output and never grows row counts.

Reconciliation note (PROMINENT -- this stage has the most template gaps)
------------------------------------------------------------------------
``config/pipeline_manifest.yaml`` remains the canonical source of stage order, table
names, write modes, and merge conditions; all I/O here is manifest-driven, so the
items below are reconciled by editing the manifest / schemas (NOT this job's control
flow). The following MUST be reconciled before production cutover:

#. **1:1 logic.** The output builders (:func:`build_fact_general_ledger`,
   :func:`build_dim_account_snapshot`) now read entry-grain ``staging_2_enriched`` and
   are grain-correct by construction (the GL fact preserves true entry keys; the
   dimension selects each account's latest entry deterministically). What REMAINS to
   reconcile against the real ``dbo.usp_load_gl_outputs`` is any additional entry-level
   filter (e.g. posted-only) and whether the account dimension should instead be
   sourced from an authoritative account/reference table (see item 2).
#. **Output-stage ``reads`` vs. output grain (RESOLVED for grain).** The manifest now
   declares ``reads: [staging_2_enriched]`` (ENTRY grain, ``[gl_entry_id, ...]``), so
   ``fact_general_ledger`` preserves its true entry-grain ``(gl_entry_id, posting_date)``
   merge keys with NO synthetic-key collisions, and ``dim_account_snapshot`` derives a
   deterministic per-account latest-entry snapshot. If the real procedure sources the
   account dimension from a dedicated account/reference table rather than the latest
   transaction, add that table to this stage's ``reads`` -- because this job reads EVERY
   table in ``stage["reads"]`` generically, expanding ``reads`` adds the input with no
   edit to this job's control flow.
#. **Schemas registry keys/columns.** ``schemas.get_schema("fact_general_ledger")``
   and ``schemas.get_schema("dim_account_snapshot")`` must resolve and their columns
   must match the manifest -- as authored they do (``schemas.output_tables`` declares
   both), but the illustrative column model must be confirmed against the procedure.
#. **Merge-condition alias convention.** ``lib.delta_io.merge_delta`` aliases the
   target as ``t`` and the source as ``s``; the manifest condition is normalized to
   that convention here via :func:`_normalize_merge_condition` (the checked-in
   manifest already uses ``t``/``s``, so the normalization is a safe no-op today).
   Align the convention long-term (manifest and ``lib.delta_io`` agree on one).

Job arguments (injected by ``infra/glue_jobs.tf``)
--------------------------------------------------
Required: ``JOB_NAME`` (Glue-provided), ``ddb_table_name``, ``aws_region``,
``delta_bucket`` (DELTA_S3_BUCKET, no scheme), ``manifest_path``, ``step`` (this
stage's unique manifest token, ``stage-n-output``).
Optional-with-default: ``run_date`` (``""``; used for the ``load_run_id`` lineage,
falling back to the Glue run id when empty).

Target runtime
--------------
AWS Glue 4.0 (Apache Spark 3.3.x, Python 3.10, Scala 2.12). Not Glue 5.0.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import StructType

from lib.delta_io import count_rows, read_delta, write_delta
from lib.job_args import resolve_options
from lib.logging_utils import StageTimer, emit_completion_event, get_logger
from lib.manifest import Manifest, load_manifest
from lib.s3_paths import build_delta_table_uri
from lib.spark_session import build_spark_session
from schemas import get_schema

# ---------------------------------------------------------------------------
# Logical table names. Kept as module constants (never magic strings) so they are
# reconciled in exactly one place against ``config/pipeline_manifest.yaml`` and the
# ``schemas/`` registry keys. ``_ENRICHED_INPUT_TABLE`` is the single table the
# output stage reads -- it is the ENTRY-GRAIN ``staging_2_enriched`` table (one row
# per GL entry), NOT the aggregated ``staging_3_balances`` (account x date grain).
# Reading entry grain is what lets the GL fact preserve its true entry-level
# ``gl_entry_id`` / ``posting_date`` merge keys and per-entry ``amount`` (Gate-1
# parity; no synthetic-key collisions). The ``_FACT_*`` / ``_DIM_*`` names are both
# the manifest ``writes`` entries AND the ``schemas.get_schema`` registry keys.
# ---------------------------------------------------------------------------
_ENRICHED_INPUT_TABLE = "staging_2_enriched"
_FACT_GENERAL_LEDGER = "fact_general_ledger"
_DIM_ACCOUNT_SNAPSHOT = "dim_account_snapshot"


# ---------------------------------------------------------------------------
# Shared helpers (mirrored 1:1 from the transform stages for uniformity)
# ---------------------------------------------------------------------------
def _resolve_glue_run_id() -> str:
    """Resolve the Glue job-run id for the completion event, best-effort.

    Tries the ``--JOB_RUN_ID`` Glue argument first, then the ``JOB_RUN_ID``
    environment variable, and finally falls back to the literal ``"unknown"``. The
    run id is purely a log field (and a reasonable ``load_run_id`` fallback), so
    resolving it must NEVER fail the job; the broad ``except`` here is intentional
    and is deliberately OUTSIDE the ACID-critical read / write path (which is never
    guarded by a swallowing handler).

    :returns: The resolved Glue run id, or ``"unknown"`` when unavailable.
    """
    try:
        resolved = resolve_options([], {"JOB_RUN_ID": ""})
        run_id = resolved.get("JOB_RUN_ID", "")
        if run_id:
            return run_id
    except Exception as exc:
        # Best-effort only: the run id is a non-essential log field, so any failure
        # to resolve it (for example ``awsglue`` being unavailable in a local / test
        # context) must NOT fail the job. This handler does not guard any Delta /
        # DynamoDB operation, so it preserves ACID strictness. Record the benign
        # reason at debug level (no no-op ``pass``) and fall through to the
        # environment / literal fallback below.
        get_logger("stage_n_output").debug(
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
    :param step: The unique stage token for THIS job (e.g. ``"stage-n-output"``).
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

    Read from ``manifest.raw["tables"]`` so each table's relative ``path``, its
    ``write_mode``, optional ``partition_by``, and optional ``merge`` block stay
    config-driven.

    :param manifest: The loaded :class:`lib.manifest.Manifest`.
    :param name: The logical table name to resolve (e.g. ``"fact_general_ledger"``).
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

    For each field declared by ``schema`` (in declaration order): if ``df`` already
    has the column it is cast to the declared :class:`~pyspark.sql.types.DataType`;
    if the column is absent it is materialized as a typed ``NULL`` literal. This is
    what makes the schema-locked (``mergeSchema=false``) Delta write in :func:`main`
    succeed deterministically WITHOUT relying on schema inference: the written
    DataFrame's columns are guaranteed to match the explicit target ``StructType``
    resolved from the ``schemas/`` registry. Builders therefore only need to compute
    the business-meaningful columns; purely structural / not-yet-sourced columns
    (e.g. ``journal_id``, ``source_system``) are filled as typed NULLs here.

    :param df: The transformed DataFrame to project.
    :param schema: The authoritative target :class:`StructType` (from
        :func:`schemas.get_schema`).
    :returns: ``df`` projected and cast to ``schema``.
    """
    existing = set(df.columns)
    projected = [
        (
            F.col(field.name).cast(field.dataType).alias(field.name)
            if field.name in existing
            else F.lit(None).cast(field.dataType).alias(field.name)
        )
        for field in schema.fields
    ]
    return df.select(*projected)


def _normalize_merge_condition(cond: str) -> str:
    """Normalize a manifest merge condition to ``lib.delta_io``'s ``t``/``s`` aliases.

    ``lib.delta_io.merge_delta`` aliases the merge target as ``t`` and the source as
    ``s`` (``.alias("t")`` / ``.alias("s")``). A manifest may, by convention, write
    the condition with ``target.`` / ``source.`` alias prefixes; this helper rewrites
    those prefixes to ``t.`` / ``s.`` deterministically so the predicate matches the
    aliases ``lib.delta_io`` actually uses. The checked-in manifest already uses
    ``t``/``s``, so this is a safe no-op there; the rewrite keeps the job correct
    regardless of which convention a future manifest adopts.

    Cross-folder reconciliation item: long-term, align on a single alias convention
    across ``config/pipeline_manifest.yaml`` and ``lib.delta_io.merge_delta`` so this
    normalization can be retired.

    :param cond: The raw merge predicate from the manifest ``merge.condition``.
    :returns: The predicate with ``target.`` -> ``t.`` and ``source.`` -> ``s.``.
    """
    return cond.replace("target.", "t.").replace("source.", "s.")


def _with_lineage(df: DataFrame, load_run_id: str) -> DataFrame:
    """Append the ``load_run_id`` / ``load_timestamp`` lineage columns to ``df``.

    Adds ``load_run_id`` (a literal carrying the run identity) and ``load_timestamp``
    (the processing-time :func:`~pyspark.sql.functions.current_timestamp`). Both are
    added unconditionally; :func:`_conform_to_schema` then retains only those that the
    target output schema actually declares (both output tables declare both columns).
    ``load_timestamp`` is a per-run audit value and is deliberately excluded from
    every table's five-field parity hash and from the merge keys, so its
    non-determinism affects neither Gate 1 (parity) nor Gate 3 (idempotency / row
    count).

    :param df: The DataFrame to decorate.
    :param load_run_id: The resolved run identity (``run_date`` or the Glue run id).
    :returns: ``df`` with the two lineage columns appended.
    """
    return df.withColumn("load_run_id", F.lit(load_run_id)).withColumn(
        "load_timestamp", F.current_timestamp()
    )


def _require_input(inputs: dict[str, DataFrame], name: str) -> DataFrame:
    """Return the input DataFrame named ``name`` or raise a reconciliation-friendly error.

    Builders consume their upstream table(s) by logical name from the ``inputs`` map
    that :func:`main` populates from the stage's manifest ``reads`` list. If a builder
    needs a table the stage did not read, fail loudly with the available inputs so the
    fix (expanding the manifest ``reads`` -- RECONCILIATION item 2) is obvious.

    :param inputs: Mapping of logical table name -> read DataFrame.
    :param name: The required input table name.
    :returns: The DataFrame registered under ``name``.
    :raises ValueError: If ``name`` was not read by this stage.
    """
    if name not in inputs:
        available = sorted(inputs)
        raise ValueError(
            f"output builder requires input table {name!r} but this stage did not read "
            f"it; stage inputs are {available}. Expand the stage 'reads' list in "
            f"config/pipeline_manifest.yaml (RECONCILIATION item 2)."
        )
    return inputs[name]


# ---------------------------------------------------------------------------
# Output-table builders -- the representative 1:1 stored-procedure logic.
#
# Each builder is a PURE transformation (DataFrames in -> DataFrame out): it builds
# no Spark session, performs no I/O, and reads no configuration, which keeps it
# trivially unit-testable and deterministic. Every builder ends by conforming its
# result to the table's explicit ``StructType`` (resolved from the ``schemas/``
# registry) so the schema-locked (``mergeSchema=false``) write succeeds without
# inference. See the PROMINENT reconciliation note in the module docstring: these
# bodies are a representative finance template and MUST be reconciled 1:1 with the
# real ``dbo.usp_load_gl_outputs`` before production cutover.
# ---------------------------------------------------------------------------
def build_fact_general_ledger(inputs: dict[str, DataFrame], load_run_id: str) -> DataFrame:
    """Build the ``fact_general_ledger`` output rows at TRUE GL-entry grain.

    1:1 GL-fact portion of ``dbo.usp_load_gl_outputs``. The fact is an ENTRY-GRAIN
    table: one output row per source GL entry, keyed by the true entry-level
    ``(gl_entry_id, posting_date)`` merge key (the manifest merge condition), with the
    Gate-1 parity hash ``[gl_entry_id, account_id, posting_date, amount, currency_code]``.

    It reads the entry-grain ``staging_2_enriched`` table (one row per GL entry) and
    performs a straight projection that PRESERVES every true entry attribute -- it does
    NOT synthesize keys and does NOT roll up to a coarser grain:

    #. ``gl_entry_id`` -- the TRUE per-entry key carried straight from the source (the
       schema declares it NOT NULL; Stage 0/1 guarantee it is populated). Because it is
       the genuine entry key, distinct entries that merely share an ``account_id`` /
       ``posting_date`` (e.g. rows split by currency or cost-center) remain distinct
       rows and CANNOT collide on the ``(gl_entry_id, posting_date)`` merge key.
    #. ``amount`` -- the TRUE per-entry ``amount`` (exact ``DECIMAL(18, 2)``), NOT a
       rolled-up balance. The signed rollup lives in Stage 3's ``balance_amount``;
       the GL fact preserves entry-level amounts for entry-grain parity.
    #. ``debit_credit_indicator`` -- the TRUE per-entry side carried straight through
       (Stage 1 canonicalized it to ``"D"`` / ``"C"`` / NULL), NOT inferred from
       subtotals.
    #. ``journal_id`` / ``source_system`` -- the TRUE entry-level values carried from
       the source (entry grain makes these real columns rather than NULL fills).
    #. ``posting_date`` / ``account_id`` / ``currency_code`` / ``cost_center`` /
       ``account_name`` / ``account_type`` / ``load_ts`` -- carried straight through.

    The projection is deterministic and order-independent, so the manifest ``merge``
    upsert re-applies the same key set on every re-run and never grows the row count
    (Gate 3 idempotency). Lineage (``load_run_id`` / ``load_timestamp``) is appended via
    :func:`_with_lineage`. Every fact column is sourced from the entry-grain input, so
    :func:`_conform_to_schema` fills no business columns with NULL.

    Reconciliation note: confirm against the real ``dbo.usp_load_gl_outputs`` whether
    the GL fact applies any additional entry-level filter (e.g. posted-only) or column
    derivation; the grain and the true-key preservation are now correct by construction.

    :param inputs: Logical table name -> read DataFrame (from the stage ``reads``).
    :param load_run_id: The resolved run identity for lineage.
    :returns: A DataFrame conforming exactly to ``schemas.get_schema("fact_general_ledger")``.
    """
    enriched = _require_input(inputs, _ENRICHED_INPUT_TABLE)

    # Straight entry-grain projection: preserve the TRUE entry key, amount, side,
    # journal id and source system -- no synthesized keys, no aggregation. Distinct
    # entries therefore stay distinct, so the (gl_entry_id, posting_date) merge key
    # is collision-free.
    fact = enriched.select(
        F.col("gl_entry_id"),
        F.col("posting_date"),
        F.col("journal_id"),
        F.col("account_id"),
        F.col("amount"),
        F.col("currency_code"),
        F.col("debit_credit_indicator"),
        F.col("cost_center"),
        F.col("account_name"),
        F.col("account_type"),
        F.col("source_system"),
        F.col("load_ts"),
    )
    fact = _with_lineage(fact, load_run_id)
    return _conform_to_schema(fact, get_schema(_FACT_GENERAL_LEDGER))


def build_dim_account_snapshot(inputs: dict[str, DataFrame], load_run_id: str) -> DataFrame:
    """Build the ``dim_account_snapshot`` output rows at ACCOUNT grain.

    1:1 account-dimension portion of ``dbo.usp_load_gl_outputs``. The output is at
    ACCOUNT grain (natural key ``account_id``, one row per account) and its Gate-1
    parity hash is ``[account_id, account_name, account_type, cost_center,
    currency_code]``. It reads the entry-grain ``staging_2_enriched`` table and selects,
    for each account, the attributes of that account's LATEST GL entry -- a true
    point-in-time snapshot rather than an arbitrary aggregate of rolled-up balances.

    Determinism (Gate 3): the latest entry is chosen with a window
    ``row_number()`` over ``partitionBy(account_id)`` ordered by ``posting_date``
    descending then ``gl_entry_id`` descending, keeping ``row_number() == 1``. Because
    ``gl_entry_id`` is the unique entry key, the ordering is a TOTAL order within each
    account partition, so exactly one row is selected deterministically and the
    ``overwrite`` output is byte-identical across re-runs regardless of input ordering.
    ``snapshot_date`` is that latest entry's ``posting_date``. Every snapshot attribute
    (including ``source_system``) is sourced from the entry, so :func:`_conform_to_schema`
    fills no business column with NULL.

    Lineage (``load_run_id`` / ``load_timestamp``) is appended via :func:`_with_lineage`.

    Reconciliation note: confirm against the real ``dbo.usp_load_gl_outputs`` whether the
    account dimension is sourced from an authoritative account/reference table rather
    than the latest transactional entry; if so, add that reference table to the stage
    ``reads`` and source the snapshot from it. The account grain and deterministic
    selection are correct by construction.

    :param inputs: Logical table name -> read DataFrame (from the stage ``reads``).
    :param load_run_id: The resolved run identity for lineage.
    :returns: A DataFrame conforming exactly to ``schemas.get_schema("dim_account_snapshot")``.
    """
    enriched = _require_input(inputs, _ENRICHED_INPUT_TABLE)

    # Deterministic "latest entry per account" selection. orderBy(posting_date desc,
    # gl_entry_id desc) is a total order within each account partition (gl_entry_id is
    # the unique entry key), so row_number()==1 picks exactly one row deterministically.
    latest_entry = Window.partitionBy("account_id").orderBy(
        F.col("posting_date").desc(), F.col("gl_entry_id").desc()
    )
    ranked = enriched.withColumn("_rn", F.row_number().over(latest_entry))
    snapshot = ranked.where(F.col("_rn") == 1).select(
        F.col("account_id"),
        F.col("account_name"),
        F.col("account_type"),
        F.col("cost_center"),
        F.col("currency_code"),
        F.col("source_system"),
        F.col("posting_date").alias("snapshot_date"),
    )
    snapshot = _with_lineage(snapshot, load_run_id)
    return _conform_to_schema(snapshot, get_schema(_DIM_ACCOUNT_SNAPSHOT))


# Dispatch map: manifest output-table name -> its builder. ``main`` looks the builder
# up by the manifest ``writes`` entry so the write loop stays generic; a manifest
# output table with no registered builder fails loudly (prompting reconciliation).
BUILDERS = {
    _FACT_GENERAL_LEDGER: build_fact_general_ledger,
    _DIM_ACCOUNT_SNAPSHOT: build_dim_account_snapshot,
}


def main() -> None:
    """Execute Stage N (output) end to end.

    Resolves Glue arguments, builds the LogStore-wired :class:`SparkSession`, loads the
    manifest, and resolves this stage's inputs / outputs and their Delta locations
    entirely from ``manifest.raw`` (config-driven, never hardcoded). It reads EVERY
    table in the stage ``reads`` list, then loops the stage ``writes`` list and, for
    each output, builds the conforming DataFrame and writes it through
    :mod:`lib.delta_io` using that table's manifest ``write_mode`` (``merge`` passes a
    normalized merge condition; ``overwrite`` truncates and reloads). Exactly one
    six-field completion event is emitted on the success path, with per-table row
    counts attached as optional extras.

    The Spark session is stopped in a ``finally`` that does NOT suppress exceptions:
    any failure in a read / build / write -- including a DynamoDB conditional-write
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
    # Lineage run identity: prefer the explicit ``run_date``, else fall back to the
    # Glue run id (which itself never resolves empty -- it defaults to "unknown").
    load_run_id = args["run_date"] or glue_run_id
    logger = get_logger("stage_n_output")

    # SparkSession built ONLY via lib.spark_session (S3DynamoDBLogStore wiring plus the
    # Delta SQL extension / catalog). No master is passed -- Glue provides it.
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

        per_table_rows: dict[str, int] = {}
        # Time the data work so the completion event's elapsed_seconds covers
        # read -> build -> write for every input and output of the stage.
        with StageTimer() as timer:
            # Read EVERY declared input generically and key it by logical table name.
            # Expanding the manifest ``reads`` adds inputs with no control-flow change.
            inputs = {
                name: read_delta(
                    spark,
                    _delta_path(args["delta_bucket"], defaults, _resolve_table(manifest, name)),
                )
                for name in stage["reads"]
            }
            input_rows = sum(count_rows(df) for df in inputs.values())

            # Write each declared output, dispatching the per-table ``write_mode``
            # (``merge`` / ``overwrite``) straight from the manifest via lib.delta_io.
            for out_name in stage["writes"]:
                builder = BUILDERS.get(out_name)
                if builder is None:
                    raise ValueError(
                        f"manifest output table {out_name!r} has no registered builder in "
                        f"jobs/stage_n_output.py; registered builders are {sorted(BUILDERS)} "
                        f"(RECONCILIATION: add a builder or reconcile the manifest writes)."
                    )

                out_table = _resolve_table(manifest, out_name)
                out_path = _delta_path(args["delta_bucket"], defaults, out_table)
                write_mode = out_table["write_mode"]
                partition_by = out_table.get("partition_by")

                # Pure build, already conformed to the explicit target schema.
                out_df = builder(inputs, load_run_id)

                if write_mode == "merge":
                    merge_block = out_table.get("merge") or {}
                    raw_condition = merge_block.get("condition")
                    if not raw_condition:
                        raise ValueError(
                            f"output table {out_name!r} uses write_mode 'merge' but the "
                            f"manifest declares no 'merge.condition'"
                        )
                    merge_condition = _normalize_merge_condition(raw_condition)
                    # ACID upsert via S3DynamoDBLogStore (mergeSchema=false inside
                    # lib.delta_io); stable keys keep it idempotent (Gate 3).
                    write_delta(
                        spark,
                        out_df,
                        out_path,
                        write_mode,
                        partition_by=partition_by,
                        merge_condition=merge_condition,
                    )
                else:
                    # ACID wholesale overwrite (truncate-and-reload); idempotent.
                    write_delta(spark, out_df, out_path, write_mode, partition_by=partition_by)

                per_table_rows[out_name] = count_rows(out_df)

            output_rows = sum(per_table_rows.values())

        # Exactly ONE six-field completion event for the WHOLE stage (never one per
        # table), emitted on the success path only. The output stage has no bad-record
        # routing, so bad_record_count is always 0. Per-table row counts ride along as
        # optional ``**extra`` fields (Logs Insights visibility) and can never displace
        # any of the six mandated fields (emit_completion_event drops reserved-key
        # collisions).
        emit_completion_event(
            job_name=args["JOB_NAME"],
            glue_run_id=glue_run_id,
            input_rows=input_rows,
            output_rows=output_rows,
            bad_record_count=0,
            elapsed_seconds=timer.elapsed,
            logger=logger,
            stage_id=args["step"],
            output_tables=list(stage["writes"]),
            **{f"{name}_rows": rows for name, rows in per_table_rows.items()},
        )
    finally:
        # Always release the session. A bare stop() in ``finally`` does NOT suppress an
        # in-flight exception, so ACID failures still propagate and fail the Glue run
        # non-zero (ACID strictness).
        spark.stop()


if __name__ == "__main__":
    main()
