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

"""``schemas`` -- explicit PySpark ``StructType`` registry for every Delta table.

Purpose
-------
This package holds the canonical, version-controlled
:class:`~pyspark.sql.types.StructType` definition for **every** Delta table that
the config-driven AWS Glue 4.0 (Apache Spark 3.3.x / Python 3.10) ETL workload
reads or writes -- the raw landing table, every intermediate ``staging_<n>``
table, and every final output table. Each schema lives in a dedicated sibling
module; this package initializer is the single, convenient *facade* that
re-exports their public API and folds all of them into one unified, table-name
-> ``StructType`` registry.

Schema-safety convention
------------------------
Schema safety is a core pillar of this workload: every table schema is declared
*explicitly* as a ``StructType`` so that schema inference is never relied upon
and write-time schema evolution / column merging is disabled on every Delta
write. Declaring schemas up front is what lets ``lib/schema_validation.py``
perform field-by-field conformance checking, route and count non-conforming
("bad") records, and lets Stage 0 fail the job with a non-zero exit when the
bad-record rate exceeds its threshold.

Config-driven lookup entrypoints
--------------------------------
:func:`get_schema` and :data:`ALL_SCHEMAS` are the preferred entrypoints for
config-driven lookups. A job (or ``lib/schema_validation.py``) takes a table
name from the pipeline manifest and calls ``schemas.get_schema(name)`` without
needing to know which sibling module defines that table. This facade therefore
decouples consumers from the package's internal module layout: a table can be
moved between modules, or a new staging stage added, with no change to any
consumer call site.

Reconciliation directive
------------------------
The table names and column sets exposed here are an *illustrative finance
template*. The authoritative set of tables, their columns, stage order, and
per-table write modes are defined in the pipeline manifest
(``config/pipeline_manifest``) and the per-pipeline source contract
(``config/<pipeline>_source_contract``). Reconcile every schema -- names, Spark
types, and ``nullable`` flags -- with those documents before production cutover.

Purity (leaf dependency)
------------------------
This is a pure, side-effect-free package. It builds no Spark session, performs
no file / network / AWS SDK calls, and reads no external configuration. The only
import-time effect is constructing the in-memory :data:`ALL_SCHEMAS` dictionary
(and validating it for duplicate keys). To prevent import cycles, ``schemas`` is
a strict *leaf*: it imports only from its own sibling modules plus the standard
library and ``pyspark.sql.types`` -- never from ``jobs/``, ``lib/``, or
``config/`` (those packages depend on ``schemas``, not the reverse).

Public API
----------
Re-exported from the sibling modules:

* :data:`STAGING_RAW_SCHEMA`, :data:`STAGING_RAW_TABLE_NAME`
  -- from ``schemas.staging_raw``.
* :data:`STAGING_SCHEMAS`, :func:`get_staging_schema`
  -- from ``schemas.staging_tables``.
* :data:`OUTPUT_SCHEMAS`, :func:`get_output_schema`
  -- from ``schemas.output_tables``.

Added by this facade:

* :data:`ALL_SCHEMAS` -- the unified ``{table_name: StructType}`` registry.
* :func:`get_schema` -- the unified, table-name resolver over
  :data:`ALL_SCHEMAS`.
"""

from typing import Dict, Mapping, Tuple

from pyspark.sql.types import StructType

# Intra-package (sibling) imports. ``schemas`` is a leaf dependency, so these are
# the ONLY first-party imports permitted here. Absolute intra-package form is
# used for clarity; relative form (``from .staging_raw import ...``) would be
# equivalent.
from schemas.staging_raw import STAGING_RAW_SCHEMA, STAGING_RAW_TABLE_NAME
from schemas.staging_tables import STAGING_SCHEMAS, get_staging_schema
from schemas.output_tables import OUTPUT_SCHEMAS, get_output_schema

# Package version, consistent with the sibling ``lib`` and ``validate`` package
# markers. Intentionally not part of :data:`__all__` (a dunder, not public API).
__version__ = "0.1.0"


def _merge_schema_sources(
    *sources: Tuple[str, Mapping[str, StructType]],
) -> Dict[str, StructType]:
    """Merge the per-module schema registries into one, rejecting duplicate keys.

    Each positional argument is a ``(source_label, mapping)`` pair, where
    ``mapping`` maps a Delta table name to its explicit
    :class:`~pyspark.sql.types.StructType`. The merge is order-preserving and
    fails loudly -- rather than silently overwriting -- if the same table name is
    contributed by more than one source, which would otherwise mask a schema
    definition bug. Every value is also asserted to be a ``StructType`` so a
    malformed registry is caught at import time instead of at job runtime.

    Args:
        *sources: ``(source_label, mapping)`` pairs to merge, applied in order.

    Returns:
        A new ``dict`` mapping each unique table name to its ``StructType``.

    Raises:
        TypeError: If any mapping value is not a ``StructType``.
        ValueError: If the same table name appears in more than one source.
    """
    merged: Dict[str, StructType] = {}
    origin: Dict[str, str] = {}
    for source_label, mapping in sources:
        for table_name, schema in mapping.items():
            if not isinstance(schema, StructType):
                raise TypeError(
                    "Schema for table {!r} from {!r} must be a pyspark "
                    "StructType, got {!r}.".format(
                        table_name, source_label, type(schema).__name__
                    )
                )
            if table_name in merged:
                raise ValueError(
                    "Duplicate Delta table schema key {!r}: defined in both {!r} "
                    "and {!r}. Every table name in the unified schema registry "
                    "must be unique.".format(
                        table_name, origin[table_name], source_label
                    )
                )
            merged[table_name] = schema
            origin[table_name] = source_label
    return merged


# ---------------------------------------------------------------------------
# Unified registry: every Delta table name -> its explicit ``StructType``.
#
# Built from the three sibling sources (raw landing table, intermediate staging
# tables, final output tables). Collision detection guarantees the three key
# spaces stay disjoint; a duplicate name fails the import immediately.
# ---------------------------------------------------------------------------
ALL_SCHEMAS: Dict[str, StructType] = _merge_schema_sources(
    ("schemas.staging_raw", {STAGING_RAW_TABLE_NAME: STAGING_RAW_SCHEMA}),
    ("schemas.staging_tables", STAGING_SCHEMAS),
    ("schemas.output_tables", OUTPUT_SCHEMAS),
)


def get_schema(table_name: str) -> StructType:
    """Return the explicit ``StructType`` registered for ``table_name``.

    This is the single, config-driven resolver that jobs and
    ``lib/schema_validation.py`` should prefer: pass the table name exactly as it
    appears in the pipeline manifest and receive its authoritative schema,
    regardless of which sibling module defines it. The lookup is a pure,
    side-effect-free read of :data:`ALL_SCHEMAS`, and the exact registered object
    is returned (object identity is preserved), so callers may compare the result
    with ``is`` against the module-level schema constants.

    Args:
        table_name: Logical Delta table name exactly as keyed in
            :data:`ALL_SCHEMAS` (for example, ``"staging_raw"``,
            ``"staging_1"``, or ``"dim_customer"``).

    Returns:
        The :class:`~pyspark.sql.types.StructType` registered for the table.

    Raises:
        KeyError: If ``table_name`` is not a registered table. The error message
            enumerates the known table names (``sorted(ALL_SCHEMAS)``) so a
            manifest reference to an undefined table fails loudly and clearly
            rather than silently.
    """
    try:
        return ALL_SCHEMAS[table_name]
    except KeyError:
        known = ", ".join(sorted(ALL_SCHEMAS))
        raise KeyError(
            "Unknown Delta table name {!r}; registered tables are: [{}]".format(
                table_name, known
            )
        ) from None


__all__ = [
    # Re-exported from schemas.staging_raw
    "STAGING_RAW_SCHEMA",
    "STAGING_RAW_TABLE_NAME",
    # Re-exported from schemas.staging_tables
    "STAGING_SCHEMAS",
    "get_staging_schema",
    # Re-exported from schemas.output_tables
    "OUTPUT_SCHEMAS",
    "get_output_schema",
    # Added by this facade
    "ALL_SCHEMAS",
    "get_schema",
]
