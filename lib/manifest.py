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
"""Pipeline manifest loader — the authoritative stored-procedure -> Glue-job map.

This module parses and validates ``config/pipeline_manifest.yaml``, the *single
source of truth* for the Glue/PySpark/Delta pipeline that replaces the legacy
Informatica-orchestrated SQL Server stored-procedure chain. The manifest is the
only place the pipeline topology lives, so the DAG (``dags/<pipeline>_dag.py``)
and the per-stage jobs (``jobs/stage_*.py``) can be reconstructed deterministically
without hardcoding any stage order, write mode, or merge condition.

The loader is intentionally **pure Python + PyYAML** — it must remain importable
at Amazon MWAA DAG-parse time and inside the lightweight ``validate/`` harness, so
it deliberately avoids any ``pyspark``/``delta``/``awsglue`` imports.

Authoritative YAML shape
------------------------
The loader models the checked-in manifest exactly. The document carries a scalar
``pipeline`` id, a ``naming`` block (``domain``/``pipeline_name``/
``resource_pattern`` feeding the MWAA task-naming convention
``{env}-{domain}-{pipeline_name}-{step}``), an optional ``manifest_version``,
``description``, ``schedule``, ``defaults`` and ``source_contract`` keys, a
top-level ``tables`` catalog, and an ordered ``stages`` chain.

Each ``tables[]`` entry has ``name``, ``kind`` (``staging`` | ``output``),
``path`` (relative), a ``schema`` reference (``module`` + ``symbol`` pointing at the
explicit ``StructType`` in ``schemas/``), a ``write_mode`` (``overwrite`` |
``merge``), an optional ``merge`` block (required when ``write_mode == "merge"``,
carrying ``keys`` and a ``condition`` whose aliases are ``t``/``s`` to match
``lib.delta_io.merge_delta``), and a ``parity`` block (``baseline`` +
``key_columns`` + exactly five ``hash_columns`` for the Gate-1 5-field hash).

Each ``stages[]`` entry has an integer ``stage`` (contiguous from 0), a unique
``step`` token (Terraform ``for_each`` key + MWAA task-id suffix), a ``type``
(``ingest`` | ``transform`` | ``output``), a ``job_script``, the
``replaces_stored_procedure`` it replaces 1:1 (or ``null`` for ingest), ``reads``
and ``writes`` lists of ``tables[].name``, and an optional stage-level
``bad_record_threshold``. Exactly one stage is of type ``output``.

Example (abridged)::

    pipeline: sp_chain_replacement
    naming:
      domain: finance
      pipeline_name: sp-chain-replacement
      resource_pattern: "{env}-{domain}-{pipeline_name}-{step}"
    tables:
      - name: staging_raw
        kind: staging
        path: "staging/staging_raw"
        schema: { module: "schemas.staging_raw", symbol: "STAGING_RAW" }
        write_mode: overwrite
        parity:
          baseline: "dbo.Staging_Raw"
          key_columns: [gl_entry_id]
          hash_columns: [gl_entry_id, account_id, posting_date, amount, currency_code]
    stages:
      - stage: 0
        step: "stage-0-ingest"
        type: ingest
        job_script: "jobs/stage_0_ingest.py"
        replaces_stored_procedure: null
        reads: []
        writes: [staging_raw]

Any structural problem with the manifest raises :class:`ManifestError` (a
:class:`ValueError` subclass); the loader never silently defaults invalid input.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import yaml

__all__ = [
    "ManifestError",
    "SchemaRef",
    "MergeSpec",
    "ParitySpec",
    "TableSpec",
    "StageSpec",
    "NamingSpec",
    "Manifest",
    "load_manifest",
    "VALID_WRITE_MODES",
    "VALID_STAGE_TYPES",
    "VALID_TABLE_KINDS",
    "HASH_COLUMN_COUNT",
]

# Delta write modes permitted on any table. ``merge`` performs a conditional
# upsert and therefore always requires a ``merge.condition``; ``overwrite``
# truncates and reloads. ``append`` is intentionally not permitted (idempotency).
VALID_WRITE_MODES = {"overwrite", "merge"}

# The three stage roles. ``ingest`` reads flat files into the raw table;
# ``transform`` replays one stored procedure 1:1; ``output`` writes the final
# Delta table(s). The pipeline must contain exactly one ``output`` stage.
VALID_STAGE_TYPES = {"ingest", "transform", "output"}

# Logical table kinds in the manifest ``tables`` catalog.
VALID_TABLE_KINDS = {"staging", "output"}

# The mandated 5-field parity hash (Gate 1): every table's ``parity.hash_columns``
# must enumerate exactly this many columns.
HASH_COLUMN_COUNT = 5


class ManifestError(ValueError):
    """Raised for any structural problem in the pipeline manifest.

    Subclasses :class:`ValueError` so callers can catch it either specifically
    (``except ManifestError``) or as part of broader value-validation handling.
    """


# ---------------------------------------------------------------------------
# Validation helpers (pure, no side effects)
# ---------------------------------------------------------------------------
def _require_mapping(value: object, *, context: str) -> dict:
    """Return ``value`` if it is a mapping, else raise :class:`ManifestError`."""
    if not isinstance(value, dict):
        raise ManifestError(
            f"{context} must be a mapping, got {type(value).__name__}"
        )
    return value


def _require_nonempty_str(value: object, *, context: str) -> str:
    """Return ``value`` if it is a non-empty string, else raise."""
    if not isinstance(value, str) or not value:
        raise ManifestError(f"{context} must be a non-empty string, got {value!r}")
    return value


def _optional_str(value: object, *, context: str) -> Optional[str]:
    """Return ``None`` or a string ``value``, else raise."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ManifestError(f"{context} must be a string when present, got {value!r}")
    return value


def _string_list(value: object, *, context: str) -> List[str]:
    """Return ``value`` as a list of strings (``None`` -> ``[]``), else raise."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise ManifestError(f"{context} must be a list when present, got {type(value).__name__}")
    for item in value:
        if not isinstance(item, str) or not item:
            raise ManifestError(f"{context} must contain only non-empty strings, got {item!r}")
    return list(value)


# ---------------------------------------------------------------------------
# Typed manifest model
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SchemaRef:
    """Reference to an explicit ``StructType`` defined in the ``schemas/`` package.

    Attributes:
        module: Importable module path, e.g. ``"schemas.staging_raw"``.
        symbol: Module-level symbol name, e.g. ``"STAGING_RAW"``.
    """

    module: str
    symbol: str


@dataclass(frozen=True)
class MergeSpec:
    """The ``merge`` block for an output table written with ``write_mode: merge``.

    Attributes:
        keys: Business-key columns the upsert matches on.
        condition: SQL merge predicate joining target alias ``t`` to source alias
            ``s`` (e.g. ``"t.id = s.id"``), consumed verbatim by
            ``lib.delta_io.merge_delta``.
        when_matched: Optional matched-action token (e.g. ``"update_all"``).
        when_not_matched: Optional not-matched-action token (e.g. ``"insert_all"``).
    """

    keys: List[str]
    condition: str
    when_matched: Optional[str] = None
    when_not_matched: Optional[str] = None


@dataclass(frozen=True)
class ParitySpec:
    """The Gate-1 parity block for a table.

    Attributes:
        baseline: Legacy SQL Server baseline object name (e.g. ``"dbo.FactGeneralLedger"``).
        key_columns: Key columns used to align rows against the baseline.
        hash_columns: Exactly :data:`HASH_COLUMN_COUNT` columns for the 5-field hash.
    """

    baseline: str
    key_columns: List[str]
    hash_columns: List[str]


@dataclass(frozen=True)
class TableSpec:
    """A single logical Delta table from the manifest ``tables`` catalog.

    Attributes:
        name: Logical table name (also the schema-registry key).
        kind: One of :data:`VALID_TABLE_KINDS`.
        path: Relative path under ``defaults.delta_path_prefix``.
        schema: The explicit-``StructType`` reference (:class:`SchemaRef`).
        write_mode: One of :data:`VALID_WRITE_MODES`.
        merge: The :class:`MergeSpec` when ``write_mode == "merge"``, else ``None``.
        parity: The :class:`ParitySpec` (present for every table in this pipeline).
    """

    name: str
    kind: str
    path: str
    schema: SchemaRef
    write_mode: str
    merge: Optional[MergeSpec] = None
    parity: Optional[ParitySpec] = None


@dataclass(frozen=True)
class StageSpec:
    """A single pipeline stage (one Glue job) parsed from the manifest.

    Attributes:
        stage: Zero-based contiguous position in the strictly sequential chain.
        step: Unique stage token (Terraform ``for_each`` key + MWAA task-id suffix).
        type: One of :data:`VALID_STAGE_TYPES`.
        job_script: Path to the PySpark entrypoint, e.g. ``jobs/stage_0_ingest.py``.
        replaces_stored_procedure: Legacy SQL Server stored procedure replicated
            1:1 (``None`` for the ingest stage).
        reads: List of ``tables[].name`` this stage reads.
        writes: List of ``tables[].name`` this stage writes.
        bad_record_threshold: Optional stage-level override of
            ``defaults.bad_record_threshold``.
    """

    stage: int
    step: str
    type: str
    job_script: str
    replaces_stored_procedure: Optional[str] = None
    reads: List[str] = field(default_factory=list)
    writes: List[str] = field(default_factory=list)
    bad_record_threshold: Optional[float] = None


@dataclass(frozen=True)
class NamingSpec:
    """The manifest ``naming`` block driving AWS resource / MWAA task naming.

    Attributes:
        domain: ``PIPELINE_DOMAIN`` token (e.g. ``"finance"``).
        pipeline_name: AWS/MWAA resource token (hyphens, e.g. ``"sp-chain-replacement"``).
        resource_pattern: Naming pattern (``env`` is injected at runtime).
    """

    domain: str
    pipeline_name: str
    resource_pattern: str


@dataclass(frozen=True)
class Manifest:
    """A fully validated pipeline manifest.

    Attributes:
        pipeline: Scalar logical pipeline id (e.g. ``"sp_chain_replacement"``).
        naming: The :class:`NamingSpec` resource/task-naming block.
        tables: List of :class:`TableSpec` (in declaration order).
        stages: List of :class:`StageSpec` (in declaration order).
        manifest_version: Manifest shape version (``None`` if not declared).
        description: Free-text description (``None`` if not declared).
        schedule: The raw ``schedule`` mapping (``{}`` if not declared).
        defaults: The raw ``defaults`` mapping (``{}`` if not declared).
        source_contract: Path to the Stage-0 source contract (``None`` if absent).
        raw: The raw parsed mapping, retained for callers needing extra keys.
    """

    pipeline: str
    naming: NamingSpec
    tables: List[TableSpec]
    stages: List[StageSpec]
    manifest_version: Optional[int] = None
    description: Optional[str] = None
    schedule: dict = field(default_factory=dict)
    defaults: dict = field(default_factory=dict)
    source_contract: Optional[str] = None
    raw: dict = field(default_factory=dict)

    # --- Stage accessors -------------------------------------------------
    def ordered_stages(self) -> List[StageSpec]:
        """Return the stages sorted by their zero-based ``stage`` index."""
        return sorted(self.stages, key=lambda s: s.stage)

    def get_stage(self, step: str) -> StageSpec:
        """Return the stage whose ``step`` token matches ``step``.

        Raises:
            ManifestError: If no stage with that ``step`` exists.
        """
        for stage in self.stages:
            if stage.step == step:
                return stage
        raise ManifestError(f"stage step not found: {step!r}")

    def transform_stages(self) -> List[StageSpec]:
        """Return the ``transform`` stages in execution order."""
        return [s for s in self.ordered_stages() if s.type == "transform"]

    @property
    def output_stage(self) -> StageSpec:
        """Return the single ``output`` stage.

        Raises:
            ManifestError: If there is not exactly one stage of type ``output``.
        """
        matches = [s for s in self.stages if s.type == "output"]
        if len(matches) != 1:
            raise ManifestError(
                f"manifest must declare exactly one 'output' stage, found {len(matches)}"
            )
        return matches[0]

    # --- Table accessors -------------------------------------------------
    def table_names(self) -> List[str]:
        """Return all logical table names in declaration order."""
        return [t.name for t in self.tables]

    def get_table(self, name: str) -> TableSpec:
        """Return the table whose ``name`` matches ``name``.

        Raises:
            ManifestError: If no table with that name exists.
        """
        for table in self.tables:
            if table.name == name:
                return table
        raise ManifestError(f"table not found: {name!r}")

    def staging_tables(self) -> List[TableSpec]:
        """Return all ``kind == "staging"`` tables in declaration order."""
        return [t for t in self.tables if t.kind == "staging"]

    def output_tables(self) -> List[TableSpec]:
        """Return all ``kind == "output"`` tables in declaration order."""
        return [t for t in self.tables if t.kind == "output"]


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------
def _build_schema_ref(value: object, *, context: str) -> SchemaRef:
    """Build a :class:`SchemaRef` from a ``{module, symbol}`` mapping."""
    d = _require_mapping(value, context=f"{context} 'schema'")
    module = _require_nonempty_str(d.get("module"), context=f"{context} 'schema.module'")
    symbol = _require_nonempty_str(d.get("symbol"), context=f"{context} 'schema.symbol'")
    return SchemaRef(module=module, symbol=symbol)


def _build_merge(value: object, *, context: str) -> MergeSpec:
    """Build a :class:`MergeSpec`, requiring a non-empty ``condition``."""
    d = _require_mapping(value, context=f"{context} 'merge'")
    condition = _require_nonempty_str(d.get("condition"), context=f"{context} 'merge.condition'")
    keys = _string_list(d.get("keys"), context=f"{context} 'merge.keys'")
    if not keys:
        raise ManifestError(f"{context} 'merge.keys' must be a non-empty list")
    return MergeSpec(
        keys=keys,
        condition=condition,
        when_matched=_optional_str(d.get("when_matched"), context=f"{context} 'merge.when_matched'"),
        when_not_matched=_optional_str(
            d.get("when_not_matched"), context=f"{context} 'merge.when_not_matched'"
        ),
    )


def _build_parity(value: object, *, context: str) -> ParitySpec:
    """Build a :class:`ParitySpec`, requiring exactly five ``hash_columns``."""
    d = _require_mapping(value, context=f"{context} 'parity'")
    baseline = _require_nonempty_str(d.get("baseline"), context=f"{context} 'parity.baseline'")
    key_columns = _string_list(d.get("key_columns"), context=f"{context} 'parity.key_columns'")
    if not key_columns:
        raise ManifestError(f"{context} 'parity.key_columns' must be a non-empty list")
    hash_columns = _string_list(d.get("hash_columns"), context=f"{context} 'parity.hash_columns'")
    if len(hash_columns) != HASH_COLUMN_COUNT:
        raise ManifestError(
            f"{context} 'parity.hash_columns' must list exactly {HASH_COLUMN_COUNT} "
            f"columns (the mandated 5-field hash), got {len(hash_columns)}"
        )
    return ParitySpec(baseline=baseline, key_columns=key_columns, hash_columns=hash_columns)


def _build_table(d: object) -> TableSpec:
    """Build and validate a single :class:`TableSpec` from a mapping.

    Raises:
        ManifestError: On any missing/invalid field.
    """
    d = _require_mapping(d, context="table entry")
    name = _require_nonempty_str(d.get("name"), context="table 'name'")
    context = f"table {name!r}"

    kind = d.get("kind")
    if kind not in VALID_TABLE_KINDS:
        raise ManifestError(
            f"{context}: 'kind' must be one of {sorted(VALID_TABLE_KINDS)}, got {kind!r}"
        )

    path = _require_nonempty_str(d.get("path"), context=f"{context} 'path'")
    schema = _build_schema_ref(d.get("schema"), context=context)

    write_mode = d.get("write_mode")
    if write_mode not in VALID_WRITE_MODES:
        raise ManifestError(
            f"{context}: 'write_mode' must be one of {sorted(VALID_WRITE_MODES)}, got {write_mode!r}"
        )

    raw_merge = d.get("merge")
    merge: Optional[MergeSpec] = None
    if write_mode == "merge":
        if raw_merge is None:
            raise ManifestError(f"{context}: 'write_mode' 'merge' requires a 'merge' block")
        merge = _build_merge(raw_merge, context=context)
    elif raw_merge is not None:
        raise ManifestError(
            f"{context}: 'merge' block is only valid when 'write_mode' is 'merge'"
        )

    raw_parity = d.get("parity")
    parity = _build_parity(raw_parity, context=context) if raw_parity is not None else None

    return TableSpec(
        name=name,
        kind=kind,
        path=path,
        schema=schema,
        write_mode=write_mode,
        merge=merge,
        parity=parity,
    )


def _build_stage(d: object) -> StageSpec:
    """Build and validate a single :class:`StageSpec` from a mapping.

    Raises:
        ManifestError: On any missing/invalid field.
    """
    d = _require_mapping(d, context="stage entry")

    # ``bool`` is a subclass of ``int``; reject it explicitly so a YAML ``true``
    # is never silently accepted as stage 1.
    stage_idx = d.get("stage")
    if isinstance(stage_idx, bool) or not isinstance(stage_idx, int):
        raise ManifestError(f"stage 'stage' must be an integer, got {stage_idx!r}")

    step = _require_nonempty_str(d.get("step"), context=f"stage {stage_idx} 'step'")
    context = f"stage {step!r}"

    stage_type = d.get("type")
    if stage_type not in VALID_STAGE_TYPES:
        raise ManifestError(
            f"{context}: 'type' must be one of {sorted(VALID_STAGE_TYPES)}, got {stage_type!r}"
        )

    job_script = _require_nonempty_str(d.get("job_script"), context=f"{context} 'job_script'")

    bad_threshold = d.get("bad_record_threshold")
    if bad_threshold is not None and (
        isinstance(bad_threshold, bool) or not isinstance(bad_threshold, (int, float))
    ):
        raise ManifestError(f"{context}: 'bad_record_threshold' must be a number when present")

    return StageSpec(
        stage=stage_idx,
        step=step,
        type=stage_type,
        job_script=job_script,
        replaces_stored_procedure=_optional_str(
            d.get("replaces_stored_procedure"),
            context=f"{context} 'replaces_stored_procedure'",
        ),
        reads=_string_list(d.get("reads"), context=f"{context} 'reads'"),
        writes=_string_list(d.get("writes"), context=f"{context} 'writes'"),
        bad_record_threshold=(float(bad_threshold) if bad_threshold is not None else None),
    )


def load_manifest(path: str) -> Manifest:
    """Load, parse, and fully validate the pipeline manifest at ``path``.

    Validation steps:
        1. Parse the YAML and require the document root to be a mapping.
        2. Require a non-empty scalar ``pipeline`` id and a ``naming`` block with
           non-empty ``domain``/``pipeline_name``/``resource_pattern``.
        3. Require a non-empty ``tables`` list; validate each table
           (``kind``/``path``/``schema``/``write_mode``/``merge``/``parity``) and
           require unique table names.
        4. Require a non-empty ``stages`` list; validate each stage
           (``stage``/``step``/``type``/``job_script``/``reads``/``writes``) and
           require unique ``step`` tokens.
        5. Require stage ``stage`` indices to be contiguous from 0 (no gaps/dupes),
           exactly one ``output`` stage, and every ``reads``/``writes`` entry to
           reference a declared table name.

    Args:
        path: Filesystem path to ``pipeline_manifest.yaml``.

    Returns:
        A validated :class:`Manifest` exposing typed accessors over ``stages``
        and ``tables``.

    Raises:
        ManifestError: On any structural problem (including malformed YAML).
    """
    try:
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ManifestError(f"failed to parse YAML manifest at {path!r}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ManifestError(
            f"manifest root must be a mapping, got {type(raw).__name__} from {path!r}"
        )

    pipeline = _require_nonempty_str(raw.get("pipeline"), context="manifest 'pipeline'")

    naming_d = _require_mapping(raw.get("naming"), context="manifest 'naming'")
    naming = NamingSpec(
        domain=_require_nonempty_str(naming_d.get("domain"), context="'naming.domain'"),
        pipeline_name=_require_nonempty_str(
            naming_d.get("pipeline_name"), context="'naming.pipeline_name'"
        ),
        resource_pattern=_require_nonempty_str(
            naming_d.get("resource_pattern"), context="'naming.resource_pattern'"
        ),
    )

    manifest_version = raw.get("manifest_version")
    if manifest_version is not None and (
        isinstance(manifest_version, bool) or not isinstance(manifest_version, int)
    ):
        raise ManifestError(
            f"'manifest_version' must be an integer when present, got {manifest_version!r}"
        )

    raw_tables = raw.get("tables")
    if not isinstance(raw_tables, list) or not raw_tables:
        raise ManifestError("manifest requires a non-empty 'tables' list")
    tables = [_build_table(t) for t in raw_tables]

    table_names = [t.name for t in tables]
    duplicate_tables = sorted({n for n in table_names if table_names.count(n) > 1})
    if duplicate_tables:
        raise ManifestError(f"duplicate table name(s): {duplicate_tables}")
    table_name_set = set(table_names)

    raw_stages = raw.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise ManifestError("manifest requires a non-empty 'stages' list")
    stages = [_build_stage(s) for s in raw_stages]

    steps = [s.step for s in stages]
    duplicate_steps = sorted({s for s in steps if steps.count(s) > 1})
    if duplicate_steps:
        raise ManifestError(f"duplicate stage step(s): {duplicate_steps}")

    indices = sorted(s.stage for s in stages)
    if indices != list(range(len(indices))):
        raise ManifestError(
            f"stage 'stage' indices must be contiguous starting at 0 "
            f"(0..{len(indices) - 1}); got {indices}"
        )

    output_stages = [s for s in stages if s.type == "output"]
    if len(output_stages) != 1:
        raise ManifestError(
            f"manifest must declare exactly one 'output' stage, found {len(output_stages)}"
        )

    # Every reads/writes reference must resolve to a declared table.
    for stage in stages:
        for ref in stage.reads + stage.writes:
            if ref not in table_name_set:
                raise ManifestError(
                    f"stage {stage.step!r} references unknown table {ref!r}; "
                    f"declared tables are {sorted(table_name_set)}"
                )

    return Manifest(
        pipeline=pipeline,
        naming=naming,
        tables=tables,
        stages=stages,
        manifest_version=manifest_version,
        description=_optional_str(raw.get("description"), context="manifest 'description'"),
        schedule=raw.get("schedule") or {},
        defaults=raw.get("defaults") or {},
        source_contract=_optional_str(
            raw.get("source_contract"), context="manifest 'source_contract'"
        ),
        raw=raw,
    )
