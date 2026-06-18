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
"""Per-pipeline source-contract loader for the Glue/PySpark Delta pipeline.

This module parses the per-pipeline *source contract* YAML
(``config/<pipeline>_source_contract.yaml``) that declares the on-disk layout of
the delimited flat files ingested by ``jobs/stage_0_ingest.py``, and turns it
into the exact set of Spark CSV ``DataFrameReader`` options used to read those
files deterministically.

The module is intentionally **pure Python + PyYAML** -- it imports no
``pyspark``, ``delta`` or ``awsglue`` symbols. ``to_spark_csv_options`` returns a
plain ``dict[str, str]`` that the Stage-0 job passes straight through to
``spark.read.options(**opts)``. Keeping the contract free of Spark imports lets
it be unit-tested and reused without a Spark/JVM runtime.

Schema-safety mandate (AAP s0.1.2 / s0.7.1)
-------------------------------------------
Schema inference is **prohibited** project-wide. ``to_spark_csv_options`` always
emits ``inferSchema = "false"`` and there is deliberately no switch to enable it.
Stage 0 supplies an explicit ``pyspark.sql.types.StructType`` (declared in the
``schemas/`` module); the reader runs in ``PERMISSIVE`` mode with a
``_corrupt_record`` column so malformed rows are *captured* (for the quarantine
path) rather than silently dropped.

Supported YAML keys
--------------------
- ``delimiter`` (required, str): field separator, e.g. ``"|"``, ``","`` or a tab.
- ``quote_char`` (str): quote character; defaults to ``"``.
- ``null_string`` (str): literal token treated as SQL ``NULL``; defaults to ``""``.
- ``header`` (bool): whether the first line holds column names; defaults to ``True``.
- ``encoding`` (str): file character encoding; defaults to ``"UTF-8"``.
- ``columns`` (list): expected column names, in order; defaults to ``[]``.
- ``escape`` (str, optional): escape character for quoted fields; omitted if unset.
- ``multiline`` (bool): allow records spanning multiple lines; defaults to ``False``.

Example contract (``config/finance_source_contract.yaml``)::

    # Pipe-delimited extract, header row present, "NULL" is the null token.
    delimiter: "|"
    quote_char: "\""
    null_string: "NULL"
    header: true
    encoding: "UTF-8"
    columns:
      - account_id
      - posting_date
      - amount
      - currency
      - description
    # escape: "\\"        # uncomment for backslash-escaped quoted fields
    # multiline: false    # uncomment/enable for embedded newlines in fields
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import yaml


class SourceContractError(ValueError):
    """Raised when a source-contract YAML document is missing or malformed.

    Subclasses :class:`ValueError` so callers may catch it either specifically
    or via the broader ``ValueError`` category.
    """


@dataclass(frozen=True)
class SourceContract:
    """Immutable description of a delimited flat-file source.

    Instances are produced by :func:`load_source_contract` and consumed by
    ``jobs/stage_0_ingest.py``. The dataclass is ``frozen`` so a contract cannot
    be mutated after loading, which keeps ingestion behaviour reproducible across
    a job run.

    Attributes:
        delimiter: Field separator character (required).
        quote_char: Character used to quote fields. Defaults to ``"``.
        null_string: Literal token interpreted as SQL ``NULL``. Defaults to ``""``.
        header: ``True`` when the first record contains column names.
        encoding: Character encoding of the source files. Defaults to ``"UTF-8"``.
        columns: Ordered list of expected column names. Defaults to an empty list.
        escape: Optional escape character for quoted fields; ``None`` when unset.
        multiline: ``True`` to allow records that span multiple physical lines.
    """

    delimiter: str
    quote_char: str = '"'
    null_string: str = ""
    header: bool = True
    encoding: str = "UTF-8"
    columns: list = field(default_factory=list)
    escape: Optional[str] = None
    multiline: bool = False

    def to_spark_csv_options(self) -> dict[str, str]:
        """Return the Spark CSV ``DataFrameReader`` options for this contract.

        Every value is a *string*, matching the Spark ``DataFrameReader.option``
        contract -- ``header``/``inferSchema``/``multiLine`` are the lowercase
        strings ``"true"``/``"false"``, never Python booleans.

        ``inferSchema`` is hardcoded to ``"false"`` and ``mode`` to
        ``"PERMISSIVE"`` with ``columnNameOfCorruptRecord = "_corrupt_record"``:
        schema inference is prohibited project-wide, and unparseable rows must be
        captured for quarantine rather than dropped. ``escape`` and ``multiLine``
        are emitted only when the contract supplies them.

        Returns:
            A ``dict[str, str]`` suitable for ``spark.read.options(**opts)``.
        """
        opts: dict[str, str] = {
            "sep": self.delimiter,
            "quote": self.quote_char,
            "nullValue": self.null_string,
            "header": "true" if self.header else "false",
            "encoding": self.encoding,
            "mode": "PERMISSIVE",
            "inferSchema": "false",
            "columnNameOfCorruptRecord": "_corrupt_record",
        }
        if self.escape is not None:
            opts["escape"] = self.escape
        if self.multiline:
            opts["multiLine"] = "true"
        return opts

    def expected_columns(self) -> list[str]:
        """Return the ordered list of expected column names.

        Stage 0 uses this both to build an all-``StringType`` read schema (so
        ingestion stays deterministic with inference disabled) and to assert the
        parsed column count matches the contract.

        Returns:
            A new list containing the contract's ``columns`` (a defensive copy,
            so callers cannot mutate the frozen instance's internal state).
        """
        return list(self.columns)


def load_source_contract(path: str) -> SourceContract:
    """Load and validate a source-contract YAML file into a ``SourceContract``.

    Args:
        path: Filesystem path to the ``<pipeline>_source_contract.yaml`` file.

    Returns:
        A populated, immutable :class:`SourceContract`.

    The parse options may live either under a nested ``source:`` mapping (the
    per-pipeline contract layout, e.g. ``source.delimiter``/``source.quote``/
    ``source.null_value``) or directly at the document root (a flat mapping).
    Both layouts are accepted; the nested ``source:`` block takes precedence when
    present. Column names + order are read from ``expected_columns`` (the contract
    layout), with ``columns`` accepted as an alias.

    Raises:
        SourceContractError: If the document root is not a mapping, the required
            ``delimiter`` key is absent from both the ``source:`` block and the
            root, or the column list is present but not a list.
    """
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise SourceContractError("source contract root must be a mapping")
    # Format/parse options live under a nested ``source:`` mapping when present;
    # otherwise fall back to the document root so a flat mapping is also accepted.
    src = raw.get("source")
    if not isinstance(src, dict):
        src = raw
    # Column names + order live under ``expected_columns``; accept ``columns`` too.
    cols = raw.get("expected_columns", raw.get("columns", []))
    if not isinstance(cols, list):
        raise SourceContractError("'expected_columns' must be a list")
    if "delimiter" not in src:
        raise SourceContractError("source contract requires 'delimiter'")
    return SourceContract(
        delimiter=src["delimiter"],
        quote_char=src.get("quote", src.get("quote_char", '"')),
        null_string=src.get("null_value", src.get("null_string", "")),
        header=bool(src.get("header", True)),
        encoding=src.get("encoding", "UTF-8"),
        columns=cols,
        escape=src.get("escape"),
        multiline=bool(src.get("multiline", False)),
    )
