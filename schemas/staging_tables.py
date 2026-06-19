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
name declared in the pipeline manifest. Each symbol name also matches the
``schema.symbol`` the manifest references for that table.

Authoritative reconciliation
-----------------------------
The number of staging stages ``N``, each table's name, and its columns are
authoritative in the pipeline manifest (``config/pipeline_manifest.yaml``). The
three tables defined here are reconciled 1:1 to that manifest's staging catalog:

* :data:`STAGING_1_CLEANSED` (table ``staging_1_cleansed``) — the cleansed,
  row-level output of ``dbo.usp_cleanse_transactions``: the raw GL feed typed and
  normalized at row granularity, plus a cleanse audit timestamp.
* :data:`STAGING_2_ENRICHED` (table ``staging_2_enriched``) — the enriched output
  of ``dbo.usp_enrich_accounts``: the cleansed rows decorated with account
  attributes (``account_name``, ``account_type``) consumed by the output stage.
* :data:`STAGING_3_BALANCES` (table ``staging_3_balances``) — the balances output
  of ``dbo.usp_compute_balances``: per ``(account_id, posting_date)`` rolled-up
  balance / debit / credit amounts and an entry count.

Each table carries every column the manifest's ``parity.hash_columns`` and
``parity.key_columns`` reference for it, so the Gate-1 parity hash and key joins
resolve against a real column.

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
``staging_1_cleansed`` is derivable directly from ``schemas/staging_raw.py``;
``staging_2_enriched`` adds the account attributes that ``staging_3_balances`` and
``schemas/output_tables.py`` consume. Keep these definitions coherent with both
neighbours when the manifest is changed.
"""

from typing import Dict

from pyspark.sql.types import (
    DateType,
    DecimalType,
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
# staging_1_cleansed — cleansed, row-level output of the "cleanse transactions"
# stored procedure (``dbo.usp_cleanse_transactions``). Derivable 1:1 from
# ``schemas/staging_raw.py``: raw delimited GL records, typed and normalized at
# row granularity, plus a cleanse audit timestamp. Manifest hash columns:
# (gl_entry_id, account_id, posting_date, amount, currency_code); key: gl_entry_id.
# ---------------------------------------------------------------------------
STAGING_1_CLEANSED: StructType = StructType(
    [
        # --- Financial-identity anchors (non-nullable) ---
        StructField("gl_entry_id", StringType(), nullable=False),
        StructField("journal_id", StringType(), nullable=True),
        StructField("account_id", StringType(), nullable=False),
        StructField("posting_date", DateType(), nullable=False),
        StructField(
            "amount", DecimalType(_MONEY_PRECISION, _MONEY_SCALE), nullable=False
        ),
        # --- Descriptive / provenance attributes ---
        StructField("currency_code", StringType(), nullable=True),
        StructField("debit_credit_indicator", StringType(), nullable=True),
        StructField("cost_center", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("load_ts", TimestampType(), nullable=True),
        # --- Audit timestamp of when the row was cleansed ---
        StructField("cleansed_at", TimestampType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# staging_2_enriched — enriched output of the "enrich accounts" stored procedure
# (``dbo.usp_enrich_accounts``). The cleansed rows decorated with account master
# attributes (``account_name``, ``account_type``) that ``staging_3_balances`` and
# the final ``dim_account_snapshot`` consume. Manifest hash columns:
# (gl_entry_id, account_id, posting_date, amount, currency_code); key: gl_entry_id.
# ---------------------------------------------------------------------------
STAGING_2_ENRICHED: StructType = StructType(
    [
        # --- Financial-identity anchors (non-nullable) ---
        StructField("gl_entry_id", StringType(), nullable=False),
        StructField("journal_id", StringType(), nullable=True),
        StructField("account_id", StringType(), nullable=False),
        StructField("posting_date", DateType(), nullable=False),
        StructField(
            "amount", DecimalType(_MONEY_PRECISION, _MONEY_SCALE), nullable=False
        ),
        # --- Descriptive / provenance attributes ---
        StructField("currency_code", StringType(), nullable=True),
        StructField("debit_credit_indicator", StringType(), nullable=True),
        StructField("cost_center", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("load_ts", TimestampType(), nullable=True),
        # --- Account master attributes added by enrichment ---
        StructField("account_name", StringType(), nullable=True),
        StructField("account_type", StringType(), nullable=True),
        # --- Audit timestamp of when the row was enriched ---
        StructField("enriched_at", TimestampType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# staging_3_balances — balances output of the "compute balances" stored procedure
# (``dbo.usp_compute_balances``): one row per ``(account_id, posting_date)`` with
# the rolled-up closing balance, debit / credit subtotals, and an entry count.
# Manifest hash columns: (account_id, posting_date, balance_amount, currency_code,
# cost_center); composite key: (account_id, posting_date).
# ---------------------------------------------------------------------------
STAGING_3_BALANCES: StructType = StructType(
    [
        # --- Composite key (non-nullable) ---
        StructField("account_id", StringType(), nullable=False),
        StructField("posting_date", DateType(), nullable=False),
        # --- Attribution / currency (part of the parity hash) ---
        StructField("cost_center", StringType(), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        # --- Computed closing balance (exact decimal, mandatory) ---
        StructField(
            "balance_amount",
            DecimalType(_MONEY_PRECISION, _MONEY_SCALE),
            nullable=False,
        ),
        # --- Carried account attributes (feed dim_account_snapshot) ---
        StructField("account_name", StringType(), nullable=True),
        StructField("account_type", StringType(), nullable=True),
        # --- Rolled-up debit / credit subtotals (may be null when no activity) ---
        StructField(
            "debit_amount", DecimalType(_MONEY_PRECISION, _MONEY_SCALE), nullable=True
        ),
        StructField(
            "credit_amount", DecimalType(_MONEY_PRECISION, _MONEY_SCALE), nullable=True
        ),
        # --- Count of contributing GL entries (mandatory, >= 0) ---
        StructField("entry_count", LongType(), nullable=False),
        # --- Audit timestamp of when the balance was computed ---
        StructField("computed_at", TimestampType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Registry: intermediate staging table name -> explicit StructType.
#
# Keys MUST match the staging table names declared in the pipeline manifest
# (``config/pipeline_manifest.yaml``). Jobs resolve their output schema by the
# manifest table name, so extending the chain is a one-line addition here (plus
# the new ``STAGING_<n>_*`` definition above) with zero consumer-code changes.
# ``schemas/__init__.py`` re-exports this mapping and merges it into the unified
# ``ALL_SCHEMAS`` registry.
# ---------------------------------------------------------------------------
STAGING_SCHEMAS: Dict[str, StructType] = {
    "staging_1_cleansed": STAGING_1_CLEANSED,
    "staging_2_enriched": STAGING_2_ENRICHED,
    "staging_3_balances": STAGING_3_BALANCES,
}


def get_staging_schema(table_name: str) -> StructType:
    """Return the explicit :class:`StructType` for an intermediate staging table.

    This is a pure, side-effect-free lookup against :data:`STAGING_SCHEMAS`.
    Jobs call it to obtain the authoritative output schema for the
    ``staging_<n>`` table named in the pipeline manifest.

    Args:
        table_name: The intermediate staging table name, e.g.
            ``"staging_1_cleansed"``.

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
    "STAGING_1_CLEANSED",
    "STAGING_2_ENRICHED",
    "STAGING_3_BALANCES",
    "STAGING_SCHEMAS",
    "get_staging_schema",
]
