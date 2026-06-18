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
"""Centralized Delta Lake read / overwrite / merge helpers for the pipeline.

This module is the single choke point for every Delta Lake read and write
performed by the ``jobs/stage_*.py`` entrypoints. Routing all I/O through these
helpers guarantees that the pipeline's architectural rules are enforced
uniformly and cannot be accidentally bypassed by an individual stage:

* **Schema is locked (mergeSchema=false).** Every write path unconditionally
  sets ``.option("mergeSchema", "false")``. Table schemas are declared as
  explicit :class:`pyspark.sql.types.StructType` definitions in the ``schemas/``
  module; schema inference and schema evolution are prohibited.
  ``spark.databricks.delta.schema.autoMerge.enabled`` is never enabled here and
  must remain unset.
* **ACID strictness -- no non-ACID fallback.** Delta commits are coordinated by
  ``io.delta.storage.S3DynamoDBLogStore`` (configured at the
  :class:`~pyspark.sql.SparkSession` level, not in this module). A DynamoDB
  conditional-write failure raised during ``.save()`` or ``.execute()`` MUST
  propagate so the Glue job exits non-zero. These helpers therefore contain no
  ``try``/``except`` around writes or merges: exceptions bubble unchanged. There
  is no retry-into-silence and no fallback to a non-coordinated writer.
* **Idempotency.** ``overwrite`` replaces table contents wholesale and ``merge``
  upserts on stable keys, so re-running a stage for the same source date / run
  id yields identical output and never grows row counts (Gate 3).

The write pattern generalizes the canonical example from the repository's own
integration test
(``storage-s3-dynamodb/integration_tests/dynamodb_logstore.py``), which writes
``data.write.format("delta").mode("overwrite")...save(path)`` over an
``S3DynamoDBLogStore``-backed session, and adds the ``DeltaTable``-based merge
path used by the output stage.

The active :class:`~pyspark.sql.SparkSession` is always passed in as a parameter
so this module stays dependency-light (it deliberately does not import
``lib.spark_session``) and remains trivially testable against any Delta-enabled
session.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    """Return ``True`` if ``path`` already holds a valid Delta table.

    Wraps :meth:`delta.tables.DeltaTable.isDeltaTable`, which inspects the target
    location for a ``_delta_log`` transaction log. Used by :func:`merge_delta` to
    decide between an initial create and an upsert.

    :param spark: Active Delta-enabled Spark session.
    :param path: Fully qualified table location (e.g. ``s3a://bucket/prefix``).
    :returns: ``True`` when a Delta table exists at ``path``, else ``False``.
    """
    return DeltaTable.isDeltaTable(spark, path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read the Delta table at ``path`` into a :class:`~pyspark.sql.DataFrame`.

    Mirrors the integration test's ``spark.read.format("delta").load(path)``
    pattern. The read resolves against the LogStore configured on ``spark``.

    :param spark: Active Delta-enabled Spark session.
    :param path: Fully qualified Delta table location.
    :returns: A lazy DataFrame over the current table snapshot.
    """
    return spark.read.format("delta").load(path)


def overwrite_delta(
    df: DataFrame, path: str, *, partition_by: Sequence[str] | None = None
) -> None:
    """Overwrite the Delta table at ``path`` with the contents of ``df``.

    The schema is locked: ``mergeSchema`` is always ``"false"`` so an
    incompatible-schema write fails fast rather than silently evolving the table.
    ``overwrite`` replaces the table contents wholesale, which is what makes
    staging stages idempotent across re-runs (Gate 3).

    No ``try``/``except`` guards the ``.save()`` call: a LogStore
    conditional-write failure must propagate and fail the job (ACID strictness).

    :param df: DataFrame whose rows fully replace the target table.
    :param path: Fully qualified Delta table location.
    :param partition_by: Optional column names to partition by; ``partitionBy`` is
        applied only when a non-empty sequence is supplied.
    """
    writer = df.write.format("delta").mode("overwrite").option("mergeSchema", "false")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def merge_delta(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    *,
    condition: str,
    partition_by: Sequence[str] | None = None,
) -> None:
    """Upsert ``df`` into the Delta table at ``path`` using ``condition``.

    On the very first run the target table does not yet exist, so this performs
    an initial :func:`overwrite_delta` to create it (keeping the first run
    idempotent). On subsequent runs it executes a ``MERGE`` that updates matched
    rows and inserts unmatched ones, so re-running with the same input does not
    grow the row count (Gate 3).

    The ``.execute()`` call is intentionally unguarded: a DynamoDB
    conditional-write failure surfaced by ``S3DynamoDBLogStore`` must raise and
    exit the job non-zero -- there is no non-ACID fallback.

    :param spark: Active Delta-enabled Spark session.
    :param df: Source DataFrame to merge into the target.
    :param path: Fully qualified Delta table location.
    :param condition: Merge predicate joining target alias ``t`` to source alias
        ``s`` (e.g. ``"t.id = s.id"``), supplied by the pipeline manifest.
    :param partition_by: Optional partition columns used only on the initial
        create path.
    """
    if not delta_table_exists(spark, path):
        # Idempotent first run: create the table via a schema-locked overwrite.
        overwrite_delta(df, path, partition_by=partition_by)
        return
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_delta(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    write_mode: str,
    *,
    partition_by: Sequence[str] | None = None,
    merge_condition: str | None = None,
) -> None:
    """Dispatch a Delta write according to the manifest's ``write_mode``.

    This is the single entrypoint the ``jobs/stage_*.py`` scripts call with
    values taken straight from ``lib.manifest``. Centralizing dispatch here is
    what enforces the rule "staging = overwrite, output = merge/overwrite per
    table" -- individual stages never hand-roll a writer and therefore cannot
    accidentally enable ``mergeSchema`` or append.

    :param spark: Active Delta-enabled Spark session.
    :param df: DataFrame to write.
    :param path: Fully qualified Delta table location.
    :param write_mode: Either ``"overwrite"`` or ``"merge"``.
    :param partition_by: Optional partition columns.
    :param merge_condition: Required when ``write_mode == "merge"``; the merge
        predicate joining target alias ``t`` to source alias ``s``.
    :raises ValueError: If ``write_mode`` is ``"merge"`` without a
        ``merge_condition``, or if ``write_mode`` is not a supported value.
    """
    if write_mode == "overwrite":
        overwrite_delta(df, path, partition_by=partition_by)
    elif write_mode == "merge":
        if not merge_condition:
            raise ValueError("merge write_mode requires merge_condition")
        merge_delta(
            spark, df, path, condition=merge_condition, partition_by=partition_by
        )
    else:
        raise ValueError(f"unsupported write_mode: {write_mode!r}")


def count_rows(df: DataFrame) -> int:
    """Return the number of rows in ``df``.

    A single, shared place to compute the input/output row counts that callers
    feed to ``lib.logging_utils.emit_completion_event`` (the six-field CloudWatch
    completion event) and to the parity checks in ``validate/``.

    :param df: DataFrame to count.
    :returns: The exact row count via :meth:`pyspark.sql.DataFrame.count`.
    """
    return df.count()
