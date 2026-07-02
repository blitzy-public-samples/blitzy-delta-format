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
"""Explicit-``StructType`` conformance checking with bad-record quarantine.

This module is the schema-safety enforcement point for the AWS Glue 4.0 /
Apache Spark 3.3.x / Delta Lake ETL pipeline. It powers Stage 0
(``jobs/stage_0_ingest.py``) bad-record handling and underpins the
"explicit schema, never inferred" mandate that the whole workload depends on.

Responsibilities
----------------
Given a DataFrame and the **explicit** :class:`~pyspark.sql.types.StructType`
the rows are expected to satisfy, this module:

#. **Validates structure (fatal).** If a column required by the expected schema
   is absent from the DataFrame, the input file is the wrong shape and the job
   must fail fast -- :class:`SchemaValidationError` is raised.
#. **Detects row-level defects (quarantine).** Each remaining row is checked,
   without any schema inference, using deterministic safe-casts: a value that is
   non-null but fails to cast to its target type, a ``null`` landing in a
   non-nullable field, or a PERMISSIVE-mode ``_corrupt_record`` marker. Defective
   rows are split into a ``bad_df`` (tagged with a ``_validation_reason``) while
   the clean rows are cast to conform *exactly* to the expected schema.
#. **Quarantines + counts.** Defective rows are written to a quarantine S3
   prefix as raw **parquet** (a debug sink, never a Delta table) and counted --
   they are never silently dropped.
#. **Enforces the threshold (fatal).** When the bad-record rate exceeds the
   caller-supplied threshold (Stage 0 defaults it to ``0.0``),
   :class:`BadRecordThresholdExceeded` is raised so the Glue job exits non-zero.
   There is no swallow and no non-ACID fallback.

Design rules
------------
* **The expected ``StructType`` is passed in as a parameter.** This module never
  imports the ``schemas/`` package, which keeps ``lib`` decoupled and reusable
  across every pipeline.
* **Quarantine is a raw parquet sink, not Delta.** This module never imports
  ``lib.delta_io``; bad records are debug output, not a transactional table.
* **No schema inference.** Detection is cast-based
  (``col.isNotNull() & col.cast(type).isNull()``) -- the deterministic,
  inference-free way to find type-incompatible values while honoring the
  ``inferSchema`` / ``mergeSchema=true`` prohibition. ``valid_df`` is cast to
  match the expected schema so the downstream ``lib.delta_io.overwrite_delta``
  write succeeds with ``mergeSchema=false``.

Separating *structural* failure (missing columns -> raise) from *row-level*
failure (route to quarantine + threshold) is what lets a single malformed row be
tolerated/quarantined while a wrong-shaped file fails fast.

Stage 0 consumer flow
---------------------
The ``jobs/stage_0_ingest.py`` entrypoint wires the helpers together as::

    df = (
        spark.read.options(**source_contract.to_spark_csv_options())
        .schema(all_string_schema_with_corrupt_record)
        .csv(source_path)
    )
    result = validate_against_schema(df, STAGING_RAW)
    quarantine_bad_records(result.bad_df, quarantine_path)
    enforce_bad_record_threshold(
        result.bad_count, result.total_count, bad_record_threshold
    )
    overwrite_delta(result.valid_df, staging_raw_path)  # lib.delta_io
    emit_completion_event(                              # lib.logging_utils
        ...,
        input_rows=result.total_count,
        output_rows=result.valid_df.count(),
        bad_record_count=result.bad_count,
        ...,
    )

:func:`validate_and_quarantine` is provided as a single-call convenience facade
that performs validate -> quarantine -> threshold enforcement and emits the
mandated one-line bad-record summary; Stage 0 may call it instead of the three
primitives.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from lib.logging_utils import get_logger

# Module-level, idempotently configured stdout logger (Glue forwards stdout to
# CloudWatch). ``get_logger`` is safe to call at import time and returns the same
# handler-configured logger on repeated calls / re-imports on Spark executors.
_LOGGER = get_logger(__name__)

# Default name of the PERMISSIVE-mode column Spark populates with the raw text of
# any line it could not parse. Stage 0 reads CSV with this column appended so that
# unparseable lines surface here instead of being dropped at read time.
DEFAULT_CORRUPT_COLUMN: str = "_corrupt_record"

# Name of the transient column this module adds to tag *why* a row is bad. It is
# retained on ``bad_df`` (for debugging in quarantine) and dropped from
# ``valid_df`` (which is projected to exactly the expected schema).
VALIDATION_REASON_COLUMN: str = "_validation_reason"


class SchemaValidationError(ValueError):
    """Raised when the input DataFrame is *structurally* incompatible.

    This is the fatal, fail-fast signal that the source file is the wrong shape
    -- specifically, that one or more columns required by the expected
    :class:`~pyspark.sql.types.StructType` are entirely absent from the
    DataFrame. It is deliberately distinct from per-row defects (which are
    routed to quarantine and governed by the bad-record threshold): a
    wrong-shaped *file* cannot be partially salvaged and must abort the job.

    Subclasses :class:`ValueError` because a missing-column condition is an
    invalid-argument-style contract error.
    """


class BadRecordThresholdExceeded(RuntimeError):
    """Raised when the bad-record *rate* exceeds the configured threshold.

    Stage 0 defaults the threshold to ``0.0``, so by default any single bad
    record aborts the run. Raising this exception is what makes the Glue job
    exit non-zero; the condition is never swallowed and there is no non-ACID
    fallback path.

    Subclasses :class:`RuntimeError` because it reflects a runtime data-quality
    condition rather than a programming error.
    """


@dataclass
class ValidationResult:
    """Outcome of validating a DataFrame against an explicit ``StructType``.

    :ivar valid_df: Rows that conformed, projected to *exactly* the expected
        schema (each field cast to its target type; the corrupt-record and
        validation-reason helper columns dropped). Ready for a
        ``mergeSchema=false`` Delta overwrite.
    :ivar bad_df: Rows that failed validation, retaining all original columns
        plus the ``_validation_reason`` tag for debugging in quarantine.
    :ivar total_count: Total number of input rows (``valid + bad``).
    :ivar bad_count: Number of rows routed to ``bad_df``.
    """

    valid_df: DataFrame
    bad_df: DataFrame
    total_count: int
    bad_count: int


def validate_against_schema(
    df: DataFrame,
    expected: StructType,
    *,
    corrupt_column: str = DEFAULT_CORRUPT_COLUMN,
) -> ValidationResult:
    """Validate ``df`` against the explicit ``expected`` schema.

    Structural check (fatal): every field declared by ``expected`` must be
    present as a column on ``df`` (the ``corrupt_column``, if it happens to share
    a declared field name, is exempt). A missing required column raises
    :class:`SchemaValidationError` -- the wrong-shaped-file case.

    Row-level check (quarantine): a row is tagged bad when, for any field, a
    non-null source value fails to cast to the field's target type
    (``col.isNotNull() & col.cast(type).isNull()``), or a non-nullable field is
    null after casting, or the PERMISSIVE-mode ``corrupt_column`` is non-null.
    The first matching reason wins (via :func:`pyspark.sql.functions.coalesce`)
    and is recorded in the ``_validation_reason`` column.

    ``valid_df`` is the reason-free subset projected to exactly ``expected`` (each
    field cast and aliased), which both conforms the data to the explicit schema
    and drops the corrupt / reason helper columns. ``bad_df`` keeps every
    original column plus ``_validation_reason``.

    :param df: Input DataFrame, typically read with an all-``StringType`` schema
        plus the appended ``corrupt_column`` so casting is the sole type gate.
    :param expected: The explicit target :class:`~pyspark.sql.types.StructType`
        (supplied by the ``schemas/`` package by the caller, never imported here).
    :param corrupt_column: Name of the PERMISSIVE-mode corrupt-record column;
        when present on ``df`` a non-null value marks the row unparseable.
    :returns: A :class:`ValidationResult` carrying ``valid_df``, ``bad_df``, and
        the total / bad row counts.
    :raises SchemaValidationError: If any column required by ``expected`` is
        absent from ``df`` (structural / fatal failure).
    """
    present = set(df.columns)
    required = [f.name for f in expected.fields]
    missing = [c for c in required if c not in present and c != corrupt_column]
    if missing:
        raise SchemaValidationError(f"missing required columns: {missing}")

    # Build an ordered list of "reason" expressions; each is null unless its
    # specific defect matches, so coalesce() yields the first matching reason.
    reasons = []
    if corrupt_column in df.columns:
        reasons.append(
            F.when(F.col(corrupt_column).isNotNull(), F.lit("unparseable_record"))
        )
    for fld in expected.fields:
        casted = F.col(fld.name).cast(fld.dataType)
        # A non-null source value that cannot be cast to the target type is bad.
        bad_cast = F.col(fld.name).isNotNull() & casted.isNull()
        reasons.append(F.when(bad_cast, F.lit(f"cast_failed:{fld.name}")))
        if not fld.nullable:
            # A null (original or cast-induced) in a non-nullable field is bad.
            reasons.append(
                F.when(casted.isNull(), F.lit(f"null_in_non_nullable:{fld.name}"))
            )
    reason_col = F.coalesce(*reasons) if reasons else F.lit(None)

    marked = df.withColumn(VALIDATION_REASON_COLUMN, reason_col)
    bad_df = marked.filter(F.col(VALIDATION_REASON_COLUMN).isNotNull())
    valid_src = marked.filter(F.col(VALIDATION_REASON_COLUMN).isNull())
    # Project valid rows to EXACTLY the expected schema: cast + alias each field
    # (dropping corrupt_column and the reason column), enabling mergeSchema=false.
    select_exprs = [
        F.col(f.name).cast(f.dataType).alias(f.name) for f in expected.fields
    ]
    valid_df = valid_src.select(*select_exprs)

    total = df.count()
    bad = bad_df.count()
    _LOGGER.info(
        "schema validation complete: total_rows=%d valid_rows=%d bad_rows=%d",
        total,
        total - bad,
        bad,
    )
    return ValidationResult(
        valid_df=valid_df, bad_df=bad_df, total_count=total, bad_count=bad
    )


def quarantine_bad_records(
    bad_df: DataFrame,
    quarantine_path: str,
) -> int:
    """Write ``bad_df`` to the quarantine prefix and return its row count.

    Bad records are persisted as a **raw parquet debug sink**, in ``overwrite``
    mode -- so re-running a stage for the same source overwrites the prior
    quarantine rather than accumulating duplicates. The on-disk format is
    **hardcoded to parquet** and is deliberately **not** caller-selectable: the
    quarantine sink must never be a Delta (or any other transactional) table and
    never routes through ``lib.delta_io``. The ``_validation_reason`` column is
    preserved to aid offline debugging.

    The write is skipped when there are no bad records, so an empty quarantine
    directory is never created.

    :param bad_df: The defective-rows DataFrame from
        :func:`validate_against_schema` (retains ``_validation_reason``).
    :param quarantine_path: Fully qualified destination prefix (for example
        ``s3a://bucket/quarantine/<pipeline>/<run>``).
    :returns: The number of quarantined (bad) records.
    """
    count = bad_df.count()
    if count > 0:
        bad_df.write.format("parquet").mode("overwrite").save(quarantine_path)
        _LOGGER.info(
            "quarantined %d bad record(s) to %s (format=parquet)",
            count,
            quarantine_path,
        )
    else:
        _LOGGER.info(
            "no bad records to quarantine; skipped write to %s", quarantine_path
        )
    return count


def enforce_bad_record_threshold(
    bad_count: int, total_count: int, threshold: float
) -> None:
    """Fail the job when the bad-record rate exceeds ``threshold``.

    The rate is ``bad_count / total_count``, guarded so an empty input
    (``total_count == 0``) yields a rate of ``0.0`` and never trips a false
    failure. When the rate is strictly greater than ``threshold``,
    :class:`BadRecordThresholdExceeded` is raised so the Glue job exits non-zero.
    The breach is never swallowed and there is no non-ACID fallback.

    The threshold value itself is owned by the Stage-0 job argument
    (``BAD_RECORD_THRESHOLD``, default ``0.0``) and passed in here; it is not
    hard-coded in this module.

    :param bad_count: Number of bad records (from
        :func:`quarantine_bad_records` or :class:`ValidationResult`).
    :param total_count: Total number of input records.
    :param threshold: Maximum tolerated bad-record rate, in ``[0.0, 1.0]``.
    :raises BadRecordThresholdExceeded: If ``rate > threshold``.
    """
    rate = (bad_count / total_count) if total_count else 0.0
    if rate > threshold:
        raise BadRecordThresholdExceeded(
            f"bad-record rate {rate:.6f} exceeds threshold {threshold:.6f} "
            f"({bad_count}/{total_count})"
        )


def validate_and_quarantine(
    df: DataFrame,
    expected: StructType,
    quarantine_path: str,
    threshold: float,
    *,
    corrupt_column: str = DEFAULT_CORRUPT_COLUMN,
) -> ValidationResult:
    """Run the full validate -> quarantine -> threshold flow as one call.

    This convenience facade composes the three primitives in the exact order
    Stage 0 requires and emits the mandated **one-line bad-record summary**
    (total, bad, rate, quarantine path) immediately *before* threshold
    enforcement, giving operators a single CloudWatch line to query. It is
    behaviorally identical to calling :func:`validate_against_schema`,
    :func:`quarantine_bad_records`, and :func:`enforce_bad_record_threshold` in
    sequence, and returns the :class:`ValidationResult` so the caller can still
    access ``valid_df`` for the downstream Delta write.

    :param df: Input DataFrame to validate.
    :param expected: Explicit target :class:`~pyspark.sql.types.StructType`.
    :param quarantine_path: Destination prefix for the raw parquet bad-record
        sink.
    :param threshold: Maximum tolerated bad-record rate (Stage 0 default ``0.0``).
    :param corrupt_column: PERMISSIVE-mode corrupt-record column name.
    :returns: The :class:`ValidationResult` from :func:`validate_against_schema`.
    :raises SchemaValidationError: If a required column is missing (fatal).
    :raises BadRecordThresholdExceeded: If the bad-record rate exceeds
        ``threshold`` (job exits non-zero).
    """
    result = validate_against_schema(df, expected, corrupt_column=corrupt_column)
    quarantine_bad_records(result.bad_df, quarantine_path)
    rate = (result.bad_count / result.total_count) if result.total_count else 0.0
    # The mandated one-line summary, emitted BEFORE threshold enforcement so it
    # is present in CloudWatch even on the run that fails the job.
    _LOGGER.info(
        "bad-record summary: total=%d bad=%d rate=%.6f quarantine_path=%s",
        result.total_count,
        result.bad_count,
        rate,
        quarantine_path,
    )
    enforce_bad_record_threshold(result.bad_count, result.total_count, threshold)
    return result
