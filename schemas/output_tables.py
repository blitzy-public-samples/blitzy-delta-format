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

Authoritative reconciliation
-----------------------------
The set of output tables, their columns, each table's write mode (``merge``
upsert vs. ``overwrite`` truncate-and-reload), and the merge conditions are
defined in the YAML manifest at ``config/pipeline_manifest.yaml`` (parsed at
runtime by ``lib/manifest.py``). The two tables defined here are reconciled 1:1
to that manifest's output catalog:

* :data:`FACT_GENERAL_LEDGER` (table ``fact_general_ledger``) — the GL fact at
  entry grain, written with ``merge`` (upsert) on the composite key
  ``(gl_entry_id, posting_date)``. Both merge-key columns are declared
  ``nullable=False`` so a NULL key can never silently match or duplicate a row
  during the upsert.
* :data:`DIM_ACCOUNT_SNAPSHOT` (table ``dim_account_snapshot``) — the account
  dimension snapshot, written with ``overwrite`` (truncate-and-reload).
  ``account_id`` is the natural key and is declared ``nullable=False``.

Each symbol name matches the ``schema.symbol`` the manifest references for that
table, and each table carries every column the manifest's
``parity.hash_columns`` and ``parity.key_columns`` reference for it.

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

# ``fact_general_ledger`` -- GL fact table at entry grain.
#
# Write mode: ``merge`` (upsert). The manifest-defined merge key is the composite
# ``(gl_entry_id, posting_date)``; BOTH key columns are therefore declared
# ``nullable=False`` so a NULL key can never silently match or duplicate a row
# during the upsert. Manifest hash columns: (gl_entry_id, account_id,
# posting_date, amount, currency_code). Column lineage: derived from the
# ``staging_3_balances`` / enriched GL rows (see ``schemas/staging_tables.py``).
FACT_GENERAL_LEDGER: StructType = StructType(
    [
        # --- Merge / business keys (manifest merge key: gl_entry_id + posting_date) ---
        StructField("gl_entry_id", StringType(), nullable=False),
        StructField("posting_date", DateType(), nullable=False),
        # --- Identity / account anchors ---
        StructField("journal_id", StringType(), nullable=True),
        StructField("account_id", StringType(), nullable=False),
        # --- Monetary measure: fixed-precision Decimal for reproducible hashing ---
        StructField("amount", DecimalType(18, 2), nullable=False),
        # --- Descriptive / provenance attributes ---
        StructField("currency_code", StringType(), nullable=True),
        StructField("debit_credit_indicator", StringType(), nullable=True),
        StructField("cost_center", StringType(), nullable=True),
        StructField("account_name", StringType(), nullable=True),
        StructField("account_type", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("load_ts", TimestampType(), nullable=True),
        # --- Lineage / audit ---
        StructField("load_run_id", StringType(), nullable=True),
        StructField("load_timestamp", TimestampType(), nullable=True),
    ]
)

# ``dim_account_snapshot`` -- account dimension snapshot.
#
# Write mode: ``overwrite`` (truncate-and-reload). ``account_id`` is the natural
# business key and is declared ``nullable=False``. Manifest hash columns:
# (account_id, account_name, account_type, cost_center, currency_code).
DIM_ACCOUNT_SNAPSHOT: StructType = StructType(
    [
        # --- Business key ---
        StructField("account_id", StringType(), nullable=False),
        # --- Account master attributes (part of the parity hash) ---
        StructField("account_name", StringType(), nullable=True),
        StructField("account_type", StringType(), nullable=True),
        StructField("cost_center", StringType(), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        # --- Provenance / snapshot window ---
        StructField("source_system", StringType(), nullable=True),
        StructField("snapshot_date", DateType(), nullable=True),
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
# ``config/pipeline_manifest.yaml``. Re-exported by ``schemas/__init__.py``.
OUTPUT_SCHEMAS: Dict[str, StructType] = {
    "fact_general_ledger": FACT_GENERAL_LEDGER,
    "dim_account_snapshot": DIM_ACCOUNT_SNAPSHOT,
}


def get_output_schema(table_name: str) -> StructType:
    """Return the explicit ``StructType`` registered for ``table_name``.

    This is a pure, in-memory lookup against :data:`OUTPUT_SCHEMAS`; it performs
    no I/O and has no side effects. The exact object stored in the registry is
    returned (object identity is preserved), so callers may safely compare the
    result with ``is`` and rely on it being the singleton schema instance.

    Args:
        table_name: Logical output-table name exactly as keyed in
            :data:`OUTPUT_SCHEMAS` (for example, ``"dim_account_snapshot"``).

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
    "FACT_GENERAL_LEDGER",
    "DIM_ACCOUNT_SNAPSHOT",
    "OUTPUT_SCHEMAS",
    "get_output_schema",
]
