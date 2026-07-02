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

"""Explicit PySpark ``StructType`` for the ``staging_raw`` raw-ingest Delta table.

Purpose
-------
This module declares the canonical, first-class ``StructType`` describing the
post-parse *raw landing* Delta table named ``staging_raw``. That table is produced
by ``jobs/stage_0_ingest.py`` (Stage 0 ingestion) and is the conformance target
consumed by ``lib/schema_validation.py``.

Authoritative reconciliation
-----------------------------
This schema is the authoritative, reconciled column model for Stage 0. Its field
names and order are field-for-field identical to the ``expected_columns`` list in
``config/sp_chain_replacement_source_contract.yaml`` (the per-pipeline source
contract), and its symbol name (:data:`STAGING_RAW`) matches the
``schema.symbol`` referenced by the ``staging_raw`` entry in
``config/pipeline_manifest.yaml``. The source contract carries column *names and
order only*; the authoritative column *types* and nullability live here.

The ten columns model a finance general-ledger feed: a GL entry identifier, its
journal, the posting account, the posting date, the signed monetary amount, the
currency, a debit/credit indicator, a cost center, the originating source system,
and a load timestamp.

Schema safety
-------------
Schema safety is a core pillar of this AWS Glue 4.0 / PySpark workload. The schema
is declared explicitly as a ``StructType`` so that schema inference is never relied
upon and every downstream Delta write keeps the write-time schema-merge flag pinned
to ``false``. ``lib/schema_validation.py`` uses :data:`STAGING_RAW` to perform
field-by-field conformance checking and to route and count non-conforming ("bad")
records; Stage 0 fails the job with a non-zero exit when the bad-record rate
exceeds ``BAD_RECORD_THRESHOLD``.

The monetary ``amount`` field uses ``DecimalType(18, 2)`` (never a floating-point
type) so the Gate-1 parity 5-field hash is byte-exact against the legacy SQL Server
baseline. Every ``StructField`` declares an explicit ``nullable`` flag: the four
financial-identity anchors that downstream stages key, merge, and hash on
(``gl_entry_id``, ``account_id``, ``posting_date``, ``amount``) are non-nullable,
while the remaining descriptive / audit attributes are nullable.

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
``STAGING_RAW``
    Explicit ``StructType`` for the raw-ingest table (the symbol the manifest
    references for the ``staging_raw`` table).

Both names are intentionally stable: ``lib/schema_validation.py`` and
``jobs/stage_0_ingest.py`` resolve them as
``from schemas.staging_raw import STAGING_RAW, STAGING_RAW_TABLE_NAME`` (or via the
manifest's ``schema.module``/``schema.symbol`` reference), and
``schemas/__init__.py`` re-exports them.
"""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

__all__ = ["STAGING_RAW_TABLE_NAME", "STAGING_RAW"]


# Canonical name of the raw-ingest Delta table written by ``jobs/stage_0_ingest.py``.
STAGING_RAW_TABLE_NAME: str = "staging_raw"


# Explicit, version-controlled schema for the ``staging_raw`` table. Field order is
# significant: it mirrors the ``expected_columns`` order of the source contract
# (``config/sp_chain_replacement_source_contract.yaml``) field-for-field so that
# conformance checking and parity hashing stay deterministic. The monetary value
# uses ``DecimalType`` (never a float) for byte-exact parity hashing, and every
# field declares an explicit ``nullable`` flag.
STAGING_RAW: StructType = StructType(
    [
        # --- Financial-identity anchors (non-nullable): keyed / merged / hashed downstream ---
        StructField("gl_entry_id", StringType(), nullable=False),            # GL entry natural key
        StructField("journal_id", StringType(), nullable=True),             # owning journal reference
        StructField("account_id", StringType(), nullable=False),            # posting account natural key
        StructField("posting_date", DateType(), nullable=False),            # GL posting date
        StructField("amount", DecimalType(18, 2), nullable=False),          # signed money -> DecimalType
        # --- Descriptive / provenance attributes (nullable) ---
        StructField("currency_code", StringType(), nullable=True),          # ISO 4217 currency
        StructField("debit_credit_indicator", StringType(), nullable=True),  # 'D' / 'C'
        StructField("cost_center", StringType(), nullable=True),            # cost-center attribution
        StructField("source_system", StringType(), nullable=True),          # source feed provenance
        StructField("load_ts", TimestampType(), nullable=True),             # source load timestamp
    ]
)
