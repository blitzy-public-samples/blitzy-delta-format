#
# Copyright (2026) The Delta Lake Project Authors.
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

"""Foundational LogStore-wired :class:`~pyspark.sql.SparkSession` bootstrap.

This module is the single entry point every Glue job (``jobs/stage_*.py``) and the
``validate/`` reconciliation harness use to obtain a :class:`SparkSession` that is wired
for multi-cluster, ACID-safe Delta Lake writes on Amazon S3. It is the first building
block of the ``lib/`` shared library: every other job depends on a LogStore-wired session.

The configuration keys applied here mirror, on a 1:1 basis, the session built by the
repository's own integration test
``storage-s3-dynamodb/integration_tests/dynamodb_logstore.py`` (the ``SparkSession.builder``
block around lines 114-128). That test is the authoritative, in-repo proof that these exact
keys activate ``io.delta.storage.S3DynamoDBLogStore`` and the DeltaTable APIs. The test-only
knobs present there (``spark.delta.logStore.s3n.impl``, ``...S3DynamoDBLogStore.errorRates``,
``...provisionedThroughput.rcu``/``.wcu``, and the ``FailingS3DynamoDBLogStore`` class) are
deliberately omitted from this production module.

ACID coordination
------------------
All Delta commits are routed through ``io.delta.storage.S3DynamoDBLogStore`` ONLY. The store
performs a DynamoDB conditional put per commit, providing the mutual exclusion (and therefore
the ACID guarantees) required across concurrent Glue drivers writing to the same table on S3.
``S3SingleDriverLogStore`` -- or any other LogStore -- is prohibited, so this module hardcodes
the implementation class and exposes no parameter that could swap it. A failed conditional
write surfaces as an exception with no non-ACID fallback. :func:`build_spark_session`
additionally rejects any ``extra_conf`` that attempts to override either LogStore key,
enforcing the rule even against caller misuse.

Both ``spark.delta.logStore.s3.impl`` AND ``spark.delta.logStore.s3a.impl`` are set, because
Spark/Hadoop may resolve an S3 location through either the ``s3://`` or the ``s3a://``
filesystem scheme depending on configuration; pinning only one would leave a non-ACID gap for
paths resolved through the other scheme.

Delta SQL extension + catalog
-----------------------------
``spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`` and
``spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`` are also
applied; this pair activates the ``DeltaTable`` Python/SQL APIs that ``lib/delta_io.py`` relies
on for ``merge`` upserts.

Glue startup nuance
-------------------
``spark.sql.extensions`` and ``spark.sql.catalog.spark_catalog`` are SQL-extension/catalog
configs that must be present at ``SparkContext`` *startup*. Inside an AWS Glue job the
``SparkContext`` is created by the Glue runtime before user code runs, so ``getOrCreate()`` may
attach to an already-started context and the two configs above would then be ignored.
Therefore ``infra/glue_jobs.tf`` MUST ALSO pass them as job ``--conf`` arguments
(``--conf spark.sql.extensions=...`` and ``--conf spark.sql.catalog.spark_catalog=...``). This
module still applies them so that local/``validate/`` runs work correctly and so that the
canonical key set lives in exactly one place (see :func:`delta_logstore_conf`).

Target runtime
--------------
AWS Glue 4.0 (Apache Spark 3.3.x, Python 3.10, Scala 2.12). Not Glue 5.0.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Implementation-class values (mandated, hardcoded -- never made configurable).
# ---------------------------------------------------------------------------
#: Delta's Spark SQL extension; activates Delta SQL parsing and analysis rules.
DELTA_SQL_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
#: Delta catalog implementation backing spark_catalog; enables the DeltaTable APIs.
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
#: The ONLY permitted LogStore; provides multi-cluster ACID commits via DynamoDB.
S3_DYNAMODB_LOG_STORE = "io.delta.storage.S3DynamoDBLogStore"

# ---------------------------------------------------------------------------
# Authoritative Spark configuration keys. This is the single source of truth
# for the key names; validate/ imports delta_logstore_conf() and
# infra/glue_jobs.tf mirrors these same keys as Glue job --conf arguments.
# ---------------------------------------------------------------------------
SQL_EXTENSIONS_KEY = "spark.sql.extensions"
SQL_CATALOG_KEY = "spark.sql.catalog.spark_catalog"
LOGSTORE_S3_IMPL_KEY = "spark.delta.logStore.s3.impl"
LOGSTORE_S3A_IMPL_KEY = "spark.delta.logStore.s3a.impl"
DDB_TABLE_NAME_KEY = "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName"
DDB_REGION_KEY = "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region"

#: LogStore implementation keys guarded against override in build_spark_session().
_LOGSTORE_IMPL_KEYS = (LOGSTORE_S3_IMPL_KEY, LOGSTORE_S3A_IMPL_KEY)

__all__ = [
    "DELTA_SQL_EXTENSION",
    "DELTA_CATALOG",
    "S3_DYNAMODB_LOG_STORE",
    "SQL_EXTENSIONS_KEY",
    "SQL_CATALOG_KEY",
    "LOGSTORE_S3_IMPL_KEY",
    "LOGSTORE_S3A_IMPL_KEY",
    "DDB_TABLE_NAME_KEY",
    "DDB_REGION_KEY",
    "delta_logstore_conf",
    "build_spark_session",
]


def delta_logstore_conf(ddb_table_name: str, region: str) -> dict[str, str]:
    """Return the authoritative Spark configuration for S3DynamoDBLogStore Delta writes.

    The returned mapping contains exactly six entries -- the canonical key set that wires a
    :class:`SparkSession` for multi-cluster, ACID-safe Delta Lake commits on S3:

    #. ``spark.sql.extensions`` -> :data:`DELTA_SQL_EXTENSION`
    #. ``spark.sql.catalog.spark_catalog`` -> :data:`DELTA_CATALOG`
    #. ``spark.delta.logStore.s3.impl`` -> :data:`S3_DYNAMODB_LOG_STORE`
    #. ``spark.delta.logStore.s3a.impl`` -> :data:`S3_DYNAMODB_LOG_STORE`
    #. ``spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName`` -> ``ddb_table_name``
    #. ``spark.io.delta.storage.S3DynamoDBLogStore.ddb.region`` -> ``region``

    This function is the single source of truth for the key set: ``validate/`` imports it to
    build read sessions, and ``infra/glue_jobs.tf`` mirrors the same keys as Glue ``--conf``
    job arguments. Both LogStore implementation keys are intentionally pinned to the same
    value so that S3 paths resolved through either the ``s3://`` or ``s3a://`` scheme commit
    through DynamoDB-coordinated, ACID-safe writes.

    Args:
        ddb_table_name: Name of the DynamoDB table that coordinates Delta commits. The table
            is provisioned by ``infra/dynamodb.tf`` (partition key ``tablePath``, sort key
            ``fileName``). Must be a non-empty string.
        region: AWS region of the DynamoDB coordination table (for example ``us-east-1``).
            Must be a non-empty string.

    Returns:
        A ``dict[str, str]`` with exactly the six configuration pairs described above.

    Raises:
        ValueError: If ``ddb_table_name`` or ``region`` is empty or ``None``.
    """
    if not ddb_table_name:
        raise ValueError("ddb_table_name is required for S3DynamoDBLogStore coordination")
    if not region:
        raise ValueError("region is required for S3DynamoDBLogStore coordination")
    return {
        SQL_EXTENSIONS_KEY: DELTA_SQL_EXTENSION,
        SQL_CATALOG_KEY: DELTA_CATALOG,
        LOGSTORE_S3_IMPL_KEY: S3_DYNAMODB_LOG_STORE,
        LOGSTORE_S3A_IMPL_KEY: S3_DYNAMODB_LOG_STORE,
        DDB_TABLE_NAME_KEY: ddb_table_name,
        DDB_REGION_KEY: region,
    }


def build_spark_session(
    app_name: str,
    ddb_table_name: str,
    region: str,
    *,
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Build (or attach to) a :class:`SparkSession` wired for ACID Delta writes on S3.

    The session is configured with :func:`delta_logstore_conf`, so every Delta commit it makes
    is coordinated through ``io.delta.storage.S3DynamoDBLogStore`` and a DynamoDB conditional
    write. There is no non-ACID fallback and no way to select a different LogStore.

    Args:
        app_name: Spark application name (shown in the Spark UI and driver logs). Must be a
            non-empty string.
        ddb_table_name: DynamoDB coordination table name (see :func:`delta_logstore_conf`).
        region: AWS region of the DynamoDB coordination table (see :func:`delta_logstore_conf`).
        master: Optional Spark master URL (for example ``local[*]``). Provide it for local or
            ``validate/`` runs. In an AWS Glue job, omit it so the Glue-managed context's
            master is used.
        extra_conf: Optional additional Spark configuration to merge on top of the canonical
            Delta configuration. It may NOT override either LogStore implementation key to any
            value other than :data:`S3_DYNAMODB_LOG_STORE`; attempting to do so raises
            ``ValueError``. Any other keys (and re-asserting the LogStore keys to the same
            value) are accepted.

    Returns:
        A configured :class:`SparkSession` obtained via ``builder.getOrCreate()``.

    Raises:
        ValueError: If ``app_name`` is empty; if ``ddb_table_name`` or ``region`` is empty
            (propagated from :func:`delta_logstore_conf`); or if ``extra_conf`` tries to point
            a LogStore implementation key at anything other than :data:`S3_DYNAMODB_LOG_STORE`.

    Example:
        >>> spark = build_spark_session(
        ...     "stage-0-ingest", "delta-logstore-prod", "us-east-1", master="local[*]"
        ... )
        >>> spark.conf.get("spark.delta.logStore.s3a.impl")
        'io.delta.storage.S3DynamoDBLogStore'
    """
    if not app_name:
        raise ValueError("app_name is required to build a SparkSession")

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    conf = delta_logstore_conf(ddb_table_name, region)
    if extra_conf:
        for key in _LOGSTORE_IMPL_KEYS:
            if key in extra_conf and extra_conf[key] != S3_DYNAMODB_LOG_STORE:
                raise ValueError(
                    "LogStore override prohibited: S3DynamoDBLogStore only "
                    f"(offending key: {key})"
                )
        conf.update(extra_conf)

    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
