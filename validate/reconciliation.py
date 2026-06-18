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

"""Parity and 5-field-hash reconciliation helpers (Gate 1 validation harness).

This module provides the *pure*, reusable PySpark helper functions that
``validate/test_parity.py`` (Gate 1) uses to compare each deployed Delta output
table against its sampled legacy SQL Server baseline. The business-acceptance
criterion is end-to-end functional parity of **>= 99.99%** (``0.9999``) for
**100% of staging and final tables** versus the legacy baseline.

Methodology
-----------
Two independent parity signals are computed per table and **both** must clear
the configured threshold (default ``0.9999`` == 99.99%):

1. **Row-count parity** -- ``min(actual, baseline) / max(actual, baseline)``.
   A symmetric ratio in ``[0.0, 1.0]`` that is ``1.0`` when both sides have the
   same number of rows (including the degenerate both-empty case).

2. **5-field hash parity** -- a deterministic SHA-256 fingerprint is built from
   *exactly five* manifest-declared columns
   (``config/pipeline_manifest.yaml`` -> ``tables[].parity.hash_columns``).
   The two sides are aligned with a FULL OUTER JOIN on the table's key columns
   and the fraction of rows whose hashes match is reported. This is
   intentionally stricter than a naive intersection ratio: a row that is
   *missing on either side*, *extra on either side*, or whose hash *differs*
   all count against parity.

Purity contract
---------------
This module is intentionally *pure*. Every function accepts already-loaded
Spark :class:`~pyspark.sql.DataFrame` objects plus plain Python values, and
returns plain Python values or :class:`~pyspark.sql.DataFrame` objects. It
performs **no** I/O: it does not build a ``SparkSession``, it does not read or
write Delta tables or files, it does not call ``boto3``, and it does not read
environment variables. Construction of the ``SparkSession`` and the reading of
the Delta/baseline tables are owned by ``validate/conftest.py`` and
``validate/test_parity.py``.

Lazy-import expectation
-----------------------
``pyspark`` is imported at the top of this module because this is a genuine
PySpark module. The Gate 5 (security / IAM) checks in ``test_parity.py`` are
``boto3``-only and must still run when ``pyspark`` is not installed; therefore
callers import THIS module *lazily, inside the Spark-gated tests*. As a result
no defensive ``try/except`` is required around the ``pyspark`` import here --
test collection never imports this module at module scope.

Schema-safety convention
------------------------
Consistent with the feature's schema-safety rules, columns are referenced
explicitly and never inferred. :func:`add_row_hash` raises :class:`ValueError`
when the hash-column count is not exactly five or when a requested column is
absent, so schema drift surfaces as a hard failure rather than a silent
mismatch.
"""

from typing import Sequence, Mapping, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

#: Documented *fallback* parity threshold. The authoritative threshold is the
#: manifest value (``config/pipeline_manifest.yaml`` -> ``defaults.parity_threshold``)
#: and MUST be passed explicitly by callers. This constant exists only so the
#: helpers have a sensible, self-documenting default and is deliberately *not*
#: used to mask a missing manifest value.
DEFAULT_PARITY_THRESHOLD: float = 0.9999

#: Delimiter joined between the five hash columns by ``concat_ws``. A two-char
#: token reduces the (already negligible) chance of accidental field-boundary
#: collisions versus a single character that might also appear in data.
HASH_SEPARATOR: str = "||"

#: Sentinel substituted for SQL ``NULL`` *before* concatenation. ``concat_ws``
#: silently *drops* ``NULL`` arguments, which would misalign subsequent columns
#: and make two structurally different rows hash identically. Coalescing every
#: column to this non-printable sentinel keeps the column positions stable and
#: the fingerprint deterministic and null-safe.
NULL_SENTINEL: str = "\u0000"

#: The parity hash must be computed over EXACTLY five columns. This invariant
#: comes from the manifest contract (every ``tables[].parity.hash_columns`` is a
#: five-element list) and is enforced by :func:`add_row_hash`.
EXPECTED_HASH_FIELD_COUNT: int = 5

#: Default name of the hash column produced by :func:`add_row_hash`.
ROW_HASH_COL: str = "row_hash"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def row_count(df: DataFrame) -> int:
    """Return the total number of rows in ``df``.

    Thin, documented wrapper around :meth:`pyspark.sql.DataFrame.count` so the
    parity helpers express row counting through a single, mockable seam.

    Parameters
    ----------
    df:
        An already-loaded Spark ``DataFrame`` (this module never reads it).

    Returns
    -------
    int
        ``df.count()`` -- the materialized row count.
    """
    return df.count()


def add_row_hash(
    df: DataFrame,
    hash_columns: Sequence[str],
    alias: str = ROW_HASH_COL,
) -> DataFrame:
    """Append a deterministic SHA-256 fingerprint column built from five columns.

    The fingerprint is computed, in the *given column order*, as::

        sha2(concat_ws("||", coalesce(cast(c1 as string), NUL),
                              ..., coalesce(cast(c5 as string), NUL)), 256)

    Casting each column to ``string`` and coalescing ``NULL`` to a stable
    sentinel makes the hash deterministic and null-safe; ``sha2(..., 256)``
    yields a stable, reproducible 256-bit fingerprint of the five fields.

    Parameters
    ----------
    df:
        The DataFrame to fingerprint.
    hash_columns:
        Exactly :data:`EXPECTED_HASH_FIELD_COUNT` (5) column names, in the order
        they should participate in the hash. Order is significant -- the same
        five columns in a different order produce a different fingerprint.
    alias:
        Name of the appended hash column. Defaults to :data:`ROW_HASH_COL`.

    Returns
    -------
    DataFrame
        ``df`` with one additional column named ``alias`` holding the hash.

    Raises
    ------
    ValueError
        If ``hash_columns`` does not contain exactly five names, or if any
        requested column is absent from ``df`` (schema-safety: drift fails hard
        rather than silently producing a mismatched fingerprint).
    """
    # Enforce the "exactly five fields" manifest invariant.
    if len(hash_columns) != EXPECTED_HASH_FIELD_COUNT:
        raise ValueError(
            "add_row_hash requires exactly "
            f"{EXPECTED_HASH_FIELD_COUNT} hash columns, got "
            f"{len(hash_columns)}: {list(hash_columns)!r}"
        )

    # Explicit-column enforcement: never infer, never silently skip. Surface
    # schema drift as a hard, descriptive failure listing the missing columns.
    available = set(df.columns)
    missing = [c for c in hash_columns if c not in available]
    if missing:
        raise ValueError(
            "add_row_hash: hash columns not present in DataFrame: "
            f"{missing!r}; available columns: {list(df.columns)!r}"
        )

    # Build the projection in the caller-supplied order. Each column is cast to
    # string and NULL is replaced by the sentinel so concat_ws cannot drop a
    # value and misalign the remaining fields.
    cols = [
        F.coalesce(F.col(c).cast("string"), F.lit(NULL_SENTINEL))
        for c in hash_columns
    ]
    return df.withColumn(alias, F.sha2(F.concat_ws(HASH_SEPARATOR, *cols), 256))


def count_parity(actual_count: int, baseline_count: int) -> float:
    """Return the symmetric row-count parity ratio in ``[0.0, 1.0]``.

    Defined as ``min / max`` so the result is order-independent and ``1.0`` when
    the two counts are equal. Both degenerate empty cases return ``1.0`` (two
    empty tables are in perfect parity).

    Parameters
    ----------
    actual_count:
        Row count of the deployed/actual table.
    baseline_count:
        Row count of the legacy baseline table.

    Returns
    -------
    float
        ``1.0`` when both counts are zero (or, defensively, when the maximum is
        zero); otherwise ``min(actual, baseline) / max(actual, baseline)``.
    """
    # Both sides empty -> perfect parity.
    if actual_count == 0 and baseline_count == 0:
        return 1.0
    # Defensive guard against division-by-zero (only reachable if a count is
    # non-positive); two empty sides are already handled above.
    if max(actual_count, baseline_count) == 0:
        return 1.0
    return min(actual_count, baseline_count) / max(actual_count, baseline_count)


def hash_parity(
    actual_df: DataFrame,
    baseline_df: DataFrame,
    key_columns: Sequence[str],
    hash_columns: Sequence[str],
) -> dict:
    """Compute strict 5-field-hash parity via a FULL OUTER JOIN on the keys.

    Both sides are fingerprinted with :func:`add_row_hash` (which enforces the
    five-column invariant and explicit-column rule) and then aligned on
    ``key_columns`` with a full outer join. The reported parity is the fraction
    of joined rows whose fingerprints are present on *both* sides and equal.

    This is intentionally stricter than a naive intersection ratio: a row that
    is missing on the actual side, extra on the actual side, or whose value
    differs all reduce ``matched`` while still contributing to ``total`` -- so
    every kind of divergence counts against parity.

    Parameters
    ----------
    actual_df:
        The deployed/actual table DataFrame.
    baseline_df:
        The legacy baseline table DataFrame.
    key_columns:
        Columns that uniquely identify a row and on which the two sides are
        joined. Per the manifest design these are expected to be unique on each
        side; duplicates are intentionally **not** silently de-duplicated here
        (doing so would mask a data-contract violation) -- they would instead
        inflate ``total`` and depress parity, which is the desired signal.
    hash_columns:
        Exactly five columns forming the value fingerprint (see
        :func:`add_row_hash`).

    Returns
    -------
    dict
        ``{"matched": int, "total": int, "parity": float}`` where ``parity`` is
        ``1.0`` when ``total`` is zero, else ``matched / total``.
    """
    keys = list(key_columns)

    # Fingerprint each side and keep only the join keys + that side's hash.
    # NOTE: key_columns are expected to be unique per side per the manifest
    # design; we deliberately do not call .dropDuplicates() so that any
    # duplicate-key data-contract violation is reflected in the parity number
    # rather than being silently masked.
    a = add_row_hash(actual_df, hash_columns, "a_hash").select(*keys, "a_hash")
    b = add_row_hash(baseline_df, hash_columns, "b_hash").select(*keys, "b_hash")

    joined = a.join(b, on=keys, how="full_outer")

    # A row "matches" only when both fingerprints are present and identical.
    matched = joined.filter(
        F.col("a_hash").isNotNull()
        & F.col("b_hash").isNotNull()
        & (F.col("a_hash") == F.col("b_hash"))
    ).count()

    # Distinct union of keys across both sides.
    total = joined.count()

    parity = 1.0 if total == 0 else matched / total
    return {"matched": matched, "total": total, "parity": parity}


def table_parity_report(
    table_name: str,
    actual_df: DataFrame,
    baseline_df: DataFrame,
    key_columns: Sequence[str],
    hash_columns: Sequence[str],
    threshold: float = DEFAULT_PARITY_THRESHOLD,
) -> dict:
    """Build the full Gate 1 parity report for a single table.

    Computes both parity signals and decides pass/fail using a
    greater-than-OR-EQUAL comparison against ``threshold``. The ``>=`` boundary
    is critical: exactly one mismatch in 10,000 rows yields a parity of
    ``0.9999`` which **must pass** at the ``0.9999`` threshold.

    Parameters
    ----------
    table_name:
        Logical name of the table under comparison (for the report/log line).
    actual_df:
        The deployed/actual table DataFrame.
    baseline_df:
        The legacy baseline table DataFrame.
    key_columns:
        Join-key columns (see :func:`hash_parity`).
    hash_columns:
        Exactly five fingerprint columns (see :func:`add_row_hash`).
    threshold:
        Minimum acceptable parity for BOTH signals. Defaults to
        :data:`DEFAULT_PARITY_THRESHOLD` but should be supplied from the
        manifest (``defaults.parity_threshold``) by callers.

    Returns
    -------
    dict
        A report with keys ``table_name``, ``actual_count``, ``baseline_count``,
        ``count_parity``, ``hash_parity``, ``matched``, ``total``,
        ``threshold`` and ``passed``.
    """
    actual_count = row_count(actual_df)
    baseline_count = row_count(baseline_df)

    cp = count_parity(actual_count, baseline_count)

    hp_detail = hash_parity(actual_df, baseline_df, key_columns, hash_columns)
    hp = hp_detail["parity"]

    # Boundary-correct gate: BOTH signals must be at least the threshold.
    passed = (cp >= threshold) and (hp >= threshold)

    return {
        "table_name": table_name,
        "actual_count": actual_count,
        "baseline_count": baseline_count,
        "count_parity": cp,
        "hash_parity": hp,
        "matched": hp_detail["matched"],
        "total": hp_detail["total"],
        "threshold": threshold,
        "passed": passed,
    }


def format_report(report: Mapping[str, Any]) -> str:
    """Render a parity report as a single, self-explanatory log/assertion line.

    The returned string is what ``test_parity.py`` passes as the message of its
    ``assert report["passed"], format_report(report)`` so a failing gate is
    immediately diagnosable from the test output.

    Parameters
    ----------
    report:
        A mapping as produced by :func:`table_parity_report`.

    Returns
    -------
    str
        e.g. ``"table=orders count_parity=1.000000 hash_parity=0.999900
        threshold=0.9999 actual=10000 baseline=10000 matched=9999/10000 PASS"``.
    """
    status = "PASS" if report["passed"] else "FAIL"
    return (
        f"table={report['table_name']} "
        f"count_parity={report['count_parity']:.6f} "
        f"hash_parity={report['hash_parity']:.6f} "
        f"threshold={report['threshold']} "
        f"actual={report['actual_count']} "
        f"baseline={report['baseline_count']} "
        f"matched={report['matched']}/{report['total']} "
        f"{status}"
    )


def all_passed(reports: Sequence[Mapping[str, Any]]) -> bool:
    """Return ``True`` only when *every* report passed.

    Convenience aggregate for the Gate 1 requirement that **100%** of staging
    and final tables clear the parity threshold.

    Parameters
    ----------
    reports:
        An iterable of report mappings (see :func:`table_parity_report`).

    Returns
    -------
    bool
        ``all(r["passed"] for r in reports)`` -- vacuously ``True`` for an empty
        sequence.
    """
    return all(r["passed"] for r in reports)
