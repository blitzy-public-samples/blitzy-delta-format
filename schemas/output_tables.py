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

"""Explicit ``StructType`` schemas for the pipeline's final output Delta tables.

This module declares exactly one explicit
:class:`~pyspark.sql.types.StructType` per *final output* Delta table produced
by the terminal stage of the AWS Glue / PySpark ETL workload
(``jobs/stage_n_output.py``). The schemas are exposed through a registry keyed
by table name (:data:`OUTPUT_SCHEMAS`) and a pure lookup helper
(:func:`get_output_schema`). Downstream consumers -- the output-stage job,
``lib/schema_validation.py``, and ``validate/reconciliation.py`` -- import
these definitions to enforce a fixed write contract and to compute the parity
hash. The package initializer (``schemas/__init__.py``) re-exports
:data:`OUTPUT_SCHEMAS` and folds it into the repository-wide ``ALL_SCHEMAS``
registry.

Reconciliation directive
-------------------------
The authoritative set of output tables, their columns, each table's write mode
(``merge`` upsert vs. ``overwrite`` truncate-and-reload), and the merge
conditions are defined in the YAML manifest at ``config/pipeline_manifest``
(parsed at runtime by ``lib/manifest.py``). The two tables defined here --
``fact_account_monthly_summary`` (a ``merge``/upsert target) and
``dim_customer`` (an ``overwrite`` truncate-and-reload target) -- are an
*illustrative finance template*. Align their names, columns, and key fields
with the manifest before production cutover.

Parity note (Gate 1)
--------------------
These tables are the surface compared in Gate 1 (Parity): a row count plus a
five-field hash must match the legacy SQL Server baseline at >= 99.99% for
100% of tables. The five-field hash is computed in
``validate/reconciliation.py`` from a stable subset of these columns, so
reproducible, byte-exact hashing depends on fixed, explicit column types. In
particular, every monetary column is a fixed-precision
:class:`~pyspark.sql.types.DecimalType` (never a floating-point type), keeping
the hash deterministic across runs, clusters, and engines.

Schema-safety and purity
------------------------
* Every schema is an explicit ``StructType``/``StructField`` tree; schema
  inference is prohibited and schema-merge is disabled on every Delta write
  (the write layer always supplies an explicit schema).
* Every ``StructField`` sets an explicit ``nullable`` flag. Business / merge
  key columns are ``nullable=False``; derived measures and audit columns are
  ``nullable=True``.
* This is a pure, side-effect-free module. It performs no I/O, constructs no
  Spark session, reads no configuration, and issues no AWS SDK calls.
  Importing it only builds in-memory type objects, so it is safe to import
  from any job, shared library, or test.
"""

from typing import Dict

from pyspark.sql.types import (
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
# Final output table schemas
#
# Each definition below is a 1:1 contract for a final output Delta table. The
# column set, ordering, types, and ``nullable`` flags are deliberately fixed so
# that writes are deterministic and the Gate 1 parity hash is reproducible.
# ---------------------------------------------------------------------------

# ``fact_account_monthly_summary`` -- monthly per-account rollup fact table.
#
# Write mode: ``merge`` (upsert). The manifest-defined merge key is the
# composite ``(account_id, activity_month)``; both key columns are therefore
# declared ``nullable=False`` so a NULL key can never silently match or
# duplicate a row during the upsert. Column lineage: this table is derived from
# the aggregated ``staging_2`` table (see ``schemas/staging_tables.py``).
FACT_ACCOUNT_MONTHLY_SUMMARY_SCHEMA: StructType = StructType(
    [
        # --- Merge / business keys (manifest merge key: account_id + activity_month) ---
        StructField("account_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        # "YYYY-MM" calendar-month bucket the summary belongs to.
        StructField("activity_month", StringType(), nullable=False),
        # --- Monetary measures: fixed-precision Decimal for reproducible hashing ---
        StructField("total_debit_amount", DecimalType(18, 2), nullable=True),
        StructField("total_credit_amount", DecimalType(18, 2), nullable=True),
        StructField("net_amount", DecimalType(18, 2), nullable=True),
        # --- Count measures ---
        StructField("transaction_count", LongType(), nullable=False),
        StructField("distinct_merchant_count", IntegerType(), nullable=True),
        # --- Activity window ---
        StructField("first_transaction_date", DateType(), nullable=True),
        StructField("last_transaction_date", DateType(), nullable=True),
        # --- Lineage / audit ---
        StructField("load_run_id", StringType(), nullable=True),
        StructField("load_timestamp", TimestampType(), nullable=True),
    ]
)

# ``dim_customer`` -- customer dimension table.
#
# Write mode: ``overwrite`` (truncate-and-reload). ``customer_id`` is the
# natural business key and is declared ``nullable=False``; the remaining
# columns are derived aggregates and audit metadata.
DIM_CUSTOMER_SCHEMA: StructType = StructType(
    [
        # --- Business key ---
        StructField("customer_id", StringType(), nullable=False),
        # --- Aggregate measures ---
        StructField("account_count", IntegerType(), nullable=True),
        StructField("total_lifetime_net_amount", DecimalType(18, 2), nullable=True),
        # --- Activity window ---
        StructField("first_seen_date", DateType(), nullable=True),
        StructField("last_seen_date", DateType(), nullable=True),
        # --- Lineage / audit ---
        StructField("load_run_id", StringType(), nullable=True),
        StructField("load_timestamp", TimestampType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Registry + lookup helper
# ---------------------------------------------------------------------------

# Authoritative registry mapping each final output table name to its explicit
# schema. Keys MUST match the output-table names declared in the manifest at
# ``config/pipeline_manifest``. Re-exported by ``schemas/__init__.py``.
OUTPUT_SCHEMAS: Dict[str, StructType] = {
    "fact_account_monthly_summary": FACT_ACCOUNT_MONTHLY_SUMMARY_SCHEMA,
    "dim_customer": DIM_CUSTOMER_SCHEMA,
}


def get_output_schema(table_name: str) -> StructType:
    """Return the explicit ``StructType`` registered for ``table_name``.

    This is a pure, in-memory lookup against :data:`OUTPUT_SCHEMAS`; it performs
    no I/O and has no side effects. The exact object stored in the registry is
    returned (object identity is preserved), so callers may safely compare the
    result with ``is`` and rely on it being the singleton schema instance.

    Args:
        table_name: Logical output-table name exactly as keyed in
            :data:`OUTPUT_SCHEMAS` (for example, ``"dim_customer"``).

    Returns:
        The :class:`~pyspark.sql.types.StructType` registered for the table.

    Raises:
        KeyError: If ``table_name`` is not a registered output table. The error
            message enumerates the known table names (``sorted(OUTPUT_SCHEMAS)``)
            so a misconfiguration is immediately actionable.
    """
    try:
        return OUTPUT_SCHEMAS[table_name]
    except KeyError:
        known = ", ".join(sorted(OUTPUT_SCHEMAS))
        raise KeyError(
            f"Unknown output table {table_name!r}; registered output tables are: [{known}]"
        ) from None


__all__ = [
    "FACT_ACCOUNT_MONTHLY_SUMMARY_SCHEMA",
    "DIM_CUSTOMER_SCHEMA",
    "OUTPUT_SCHEMAS",
    "get_output_schema",
]
