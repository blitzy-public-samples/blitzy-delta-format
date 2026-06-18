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

"""Explicit ``StructType`` definitions for the intermediate ``staging_<n>`` Delta tables.

Purpose
-------
This module declares **one explicit** :class:`~pyspark.sql.types.StructType`
**per intermediate staging table** produced by the transformation stages of the
AWS Glue 4.0 / PySpark ETL pipeline that replaces the legacy SQL Server
stored-procedure chain on a strict 1:1 basis (one Glue job per stored
procedure). Each transformation stage (``jobs/stage_1_<sp>.py`` …
``jobs/stage_{N-1}_<sp>.py``) reads the prior stage's Delta table and writes the
next intermediate ``staging_<n>`` table in ``overwrite`` mode.

The schemas are exposed through :data:`STAGING_SCHEMAS`, a registry keyed by
table name, so that a job can resolve its output schema purely from the table
name declared in the pipeline manifest. Adding a stage later therefore means
adding one registry entry (and its ``STAGING_<n>_SCHEMA`` definition) with **no
change to any consumer code**.

Reconciliation directive
------------------------
The number of staging stages ``N`` and each table's columns are **authoritative
in the pipeline manifest** (``config/pipeline_manifest``). The two tables defined
here (:data:`STAGING_1_SCHEMA`, :data:`STAGING_2_SCHEMA`) are an **illustrative
finance template** that demonstrates the two common intermediate shapes — a
cleansed, row-level table and an account-by-month aggregate. Add or rename
``STAGING_<n>_SCHEMA`` definitions and their registry keys to match the
manifest's stored-procedure-to-job map (one intermediate table per stored
procedure) **before production cutover**.

Schema-safety and purity guarantees
-----------------------------------
* Every column is declared with an explicit
  :class:`~pyspark.sql.types.StructField`; schema inference is never used and
  automatic schema evolution / column merging is disabled on every downstream
  Delta write.
* Every field declares an explicit ``nullable`` flag: key / mandatory columns
  are non-nullable (``False``); optional columns are nullable (``True``).
* Monetary amounts use :class:`~pyspark.sql.types.DecimalType` (precision 18,
  scale 2) — never a floating-point type — to preserve exact financial values.
* This is a **pure** definition module: it performs no I/O, creates no Spark
  session, reads no configuration, and produces no top-level side effects. It is
  safe to import from Glue jobs, the schema-validation library, and test
  harnesses alike.

Column lineage
--------------
``staging_1`` is derivable directly from ``schemas/staging_raw.py`` (raw
delimited records, typed and normalized at row granularity), and the aggregated
``staging_2`` feeds ``schemas/output_tables.py``. Keep these definitions
coherent with both neighbours when the manifest is reconciled.
"""

from typing import Dict

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Monetary precision/scale shared by every currency-denominated column.
# Centralised so all financial columns stay consistent and exact (no floats):
# DECIMAL(18, 2) preserves values up to 9,999,999,999,999,999.99.
# ---------------------------------------------------------------------------
_MONEY_PRECISION = 18
_MONEY_SCALE = 2


# ---------------------------------------------------------------------------
# staging_1 — cleansed, row-level output of a "cleanse / normalize" stored
# procedure. Derivable 1:1 from ``schemas/staging_raw.py``: raw delimited
# records are typed, normalized, and flagged for validity at row granularity.
# ---------------------------------------------------------------------------
STAGING_1_SCHEMA: StructType = StructType(
    [
        # --- Keys / mandatory lineage columns (non-nullable) ---
        StructField("transaction_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        # --- Optional descriptive / foreign-key columns ---
        StructField("customer_id", StringType(), nullable=True),
        # --- Event dates: the transaction date is mandatory; posting may lag ---
        StructField("transaction_date", DateType(), nullable=False),
        StructField("posting_date", DateType(), nullable=True),
        # --- Monetary value: exact decimal, mandatory ---
        StructField(
            "amount", DecimalType(_MONEY_PRECISION, _MONEY_SCALE), nullable=False
        ),
        StructField("currency_code", StringType(), nullable=True),
        StructField("transaction_type", StringType(), nullable=True),
        StructField("merchant_name", StringType(), nullable=True),
        StructField("normalized_description", StringType(), nullable=True),
        # --- Validity flag set by the cleanse step (mandatory) ---
        StructField("is_valid", BooleanType(), nullable=False),
        # --- Audit timestamp of when the row was cleansed ---
        StructField("cleansed_at", TimestampType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# staging_2 — account-by-month aggregated output of an "enrich / aggregate"
# stored procedure. Feeds ``schemas/output_tables.py``: one row per
# (account_id, activity_month) carrying rolled-up debit / credit / net amounts
# and activity counts.
# ---------------------------------------------------------------------------
STAGING_2_SCHEMA: StructType = StructType(
    [
        # --- Grouping keys ---
        StructField("account_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        # --- Aggregation period as "YYYY-MM" (mandatory grouping key) ---
        StructField("activity_month", StringType(), nullable=False),
        # --- Rolled-up monetary aggregates (exact decimal; may be null when no
        #     debit / credit activity exists for the period) ---
        StructField(
            "total_debit_amount",
            DecimalType(_MONEY_PRECISION, _MONEY_SCALE),
            nullable=True,
        ),
        StructField(
            "total_credit_amount",
            DecimalType(_MONEY_PRECISION, _MONEY_SCALE),
            nullable=True,
        ),
        StructField(
            "net_amount", DecimalType(_MONEY_PRECISION, _MONEY_SCALE), nullable=True
        ),
        # --- Activity counts: total transactions is mandatory (>= 0) ---
        StructField("transaction_count", LongType(), nullable=False),
        StructField("distinct_merchant_count", IntegerType(), nullable=True),
        # --- First / last transaction dates within the period ---
        StructField("first_transaction_date", DateType(), nullable=True),
        StructField("last_transaction_date", DateType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Registry: intermediate staging table name -> explicit StructType.
#
# Jobs resolve their output schema by the table name declared in the pipeline
# manifest, so extending the chain is a one-line addition here (plus the new
# ``STAGING_<n>_SCHEMA`` above) with zero consumer-code changes.
# ``schemas/__init__.py`` re-exports this mapping and merges it into the unified
# ``ALL_SCHEMAS`` registry.
# ---------------------------------------------------------------------------
STAGING_SCHEMAS: Dict[str, StructType] = {
    "staging_1": STAGING_1_SCHEMA,
    "staging_2": STAGING_2_SCHEMA,
}


def get_staging_schema(table_name: str) -> StructType:
    """Return the explicit :class:`StructType` for an intermediate staging table.

    This is a pure, side-effect-free lookup against :data:`STAGING_SCHEMAS`.
    Jobs call it to obtain the authoritative output schema for the
    ``staging_<n>`` table named in the pipeline manifest.

    Args:
        table_name: The intermediate staging table name, e.g. ``"staging_1"``.

    Returns:
        The :class:`StructType` registered for ``table_name``.

    Raises:
        KeyError: If ``table_name`` is not a registered staging table. The error
            message enumerates the known table names
            (``sorted(STAGING_SCHEMAS)``) so that a manifest reference to an
            undefined table fails loudly and clearly rather than silently.
    """
    if table_name not in STAGING_SCHEMAS:
        known = sorted(STAGING_SCHEMAS)
        raise KeyError(
            "Unknown staging table name {!r}; known staging tables are: {}".format(
                table_name, known
            )
        )
    return STAGING_SCHEMAS[table_name]


__all__ = [
    "STAGING_1_SCHEMA",
    "STAGING_2_SCHEMA",
    "STAGING_SCHEMAS",
    "get_staging_schema",
]
