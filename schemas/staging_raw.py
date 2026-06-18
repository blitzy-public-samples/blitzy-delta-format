#
# Copyright (2021) The Delta Lake Project Authors.
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

"""Explicit PySpark ``StructType`` for the ``staging_raw`` raw-ingest Delta table.

Purpose
-------
This module declares the canonical, first-class ``StructType`` describing the
post-parse *raw landing* Delta table named ``staging_raw``. That table is produced
by ``jobs/stage_0_ingest.py`` (Stage 0 ingestion) and is the conformance target
consumed by ``lib/schema_validation.py``.

It deliberately supersedes the ad-hoc DDL-string schema demonstrated in the
repository's own LogStore integration test (a literal ``"id: int, a: int"`` handed
to ``createDataFrame``) with a reusable, strongly typed, version-controlled schema
object that every Stage 0 reader and writer shares.

Schema safety
-------------
Schema safety is a core pillar of this AWS Glue 4.0 / PySpark workload. Every
schema is declared explicitly as a ``StructType`` so that schema inference is never
relied upon and every downstream Delta write keeps the write-time schema-merge flag
pinned to ``false``. ``lib/schema_validation.py`` uses :data:`STAGING_RAW_SCHEMA`
to perform field-by-field conformance checking and to route and count
non-conforming ("bad") records; Stage 0 fails the job with a non-zero exit when the
bad-record rate exceeds ``BAD_RECORD_THRESHOLD``.

Monetary fields use ``DecimalType`` (never a floating-point type) so the Gate-1
parity 5-field hash is byte-exact against the legacy SQL Server baseline. Every
``StructField`` declares an explicit ``nullable`` flag: business / natural keys are
non-nullable, while optional attributes are nullable.

Reconciliation directive
------------------------
These columns are an illustrative template representing a finance
stored-procedure-chain replacement. Before production cutover, reconcile the column
names, Spark types, and nullability with the authoritative per-pipeline source
contract (``config/<pipeline>_source_contract`` -- the YAML document declaring the
expected columns, delimiter, quote, null string, header flag, and encoding) and the
pipeline manifest (``config/pipeline_manifest`` -- the YAML document mapping stored
procedures to Glue jobs and defining stage order and write modes).

Purity
------
This is a pure data-structure module. It builds no Spark session, performs no I/O,
network, file, or AWS calls, and reads no external configuration. Importing it is
instantaneous and free of side effects beyond defining the two module-level
constants below.

Public API
----------
``STAGING_RAW_TABLE_NAME``
    Canonical table-name constant (``"staging_raw"``).
``STAGING_RAW_SCHEMA``
    Explicit ``StructType`` for the raw-ingest table.

Both names are intentionally stable: ``lib/schema_validation.py`` and
``jobs/stage_0_ingest.py`` import them as
``from schemas.staging_raw import STAGING_RAW_SCHEMA, STAGING_RAW_TABLE_NAME`` and
``schemas/__init__.py`` re-exports them.
"""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

__all__ = ["STAGING_RAW_TABLE_NAME", "STAGING_RAW_SCHEMA"]


# Canonical name of the raw-ingest Delta table written by ``jobs/stage_0_ingest.py``.
STAGING_RAW_TABLE_NAME: str = "staging_raw"


# Explicit, version-controlled schema for the ``staging_raw`` table. The column set
# is a representative finance template (see the module docstring's reconciliation
# directive). Field order is significant: it mirrors the expected source-contract
# column order so that conformance checking and parity hashing stay deterministic.
# Monetary values use ``DecimalType`` (never a float) for byte-exact parity hashing,
# and every field declares an explicit ``nullable`` flag.
STAGING_RAW_SCHEMA: StructType = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),    # natural key
        StructField("account_id", StringType(), nullable=False),        # account natural key
        StructField("customer_id", StringType(), nullable=True),        # optional customer ref
        StructField("transaction_date", DateType(), nullable=False),    # business date
        StructField("posting_date", DateType(), nullable=True),         # posting date (optional)
        StructField("amount", DecimalType(18, 2), nullable=False),      # money -> DecimalType
        StructField("currency_code", StringType(), nullable=True),      # ISO 4217, 3 chars
        StructField("transaction_type", StringType(), nullable=True),   # DEBIT / CREDIT
        StructField("merchant_name", StringType(), nullable=True),      # merchant label
        StructField("description", StringType(), nullable=True),        # free-text memo
        StructField("branch_code", StringType(), nullable=True),        # branch identifier
        StructField("status_code", StringType(), nullable=True),        # record status
        StructField("source_system", StringType(), nullable=True),      # source feed provenance
    ]
)
